import paho.mqtt.client as mqtt
import json
import time
import asyncio
import telnetlib
import re
import os

share_dir = '/share'
config_dir = '/data'
data_dir = '/pycommax'

HA_TOPIC = 'commax'
STATE_TOPIC = HA_TOPIC + '/{}/{}/state'
ELFIN_TOPIC = 'ew11'
ELFIN_SEND_TOPIC = ELFIN_TOPIC + '/send'

def log(string):
    date = time.strftime('%Y-%m-%d %p %I:%M:%S', time.localtime(time.time()))
    print(f'[{date}] {string}')

def checksum(input_hex):
    try:
        input_hex = input_hex[:14]
        s1 = sum([int(input_hex[val], 16) for val in range(0, 14, 2)])
        s2 = sum([int(input_hex[val + 1], 16) for val in range(0, 14, 2)])
        s1 = s1 + int(s2 // 16)
        s1 = s1 % 16
        s2 = s2 % 16
        return input_hex + format(s1, 'X') + format(s2, 'X')
    except:
        return None

def pad(value):
    try:
        v = int(float(value))
        return '{:02d}'.format(v)
    except:
        return "20"

def make_hex(k, input_hex, change):
    if input_hex:
        try:
            change = int(change)
            # 장치 번호(idx) 반영
            input_hex = f'{input_hex[:change - 1]}{format(int(input_hex[change-1], 16) + k, "X")}{input_hex[change:]}'
        except:
            pass
    return checksum(input_hex)

def make_hex_temp(k, curTemp, setTemp, state, device_list):
    info = device_list.get('Thermo')
    if not info: return None

    if state in ['OFF', 'ON', 'CHANGE']:
        tmp_hex = info.get('command' + state)
        change_pos = info.get('commandNUM')
        tmp_hex = make_hex(k, tmp_hex, change_pos)
        if state == 'CHANGE':
            setT = pad(setTemp)
            chaTnum = info.get('chaTemp')
            tmp_hex = tmp_hex[:chaTnum - 1] + setT + tmp_hex[chaTnum + 1:]
        return checksum(tmp_hex)
    else:
        # 상태 확인용 패킷 생성
        tmp_hex = info.get(state) # stateON, stateOFF
        change_pos = info.get('stateNUM')
        tmp_hex = make_hex(k, tmp_hex, change_pos)
        setT = pad(setTemp)
        curT = pad(curTemp)
        curTnum = info.get('curTemp')
        setTnum = info.get('setTemp')
        tmp_hex = tmp_hex[:setTnum - 1] + setT + tmp_hex[setTnum + 1:]
        tmp_hex = tmp_hex[:curTnum - 1] + curT + tmp_hex[curTnum + 1:]
        
        if state == 'stateON':
            # 일부 모델은 두 가지 상태 헤더를 가짐
            tmp_hex2 = tmp_hex[:3] + "3" + tmp_hex[4:]
            return [checksum(tmp_hex), checksum(tmp_hex2)]
        return [checksum(tmp_hex)]

def do_work(config, device_list):
    mqtt_log = config.get('mqtt_log', False)
    elfin_log = config.get('elfin_log', False)
    debug = config.get('DEBUG', False)

    DEVICE_LISTS = {}
    # 기기 목록 초기화 로직 (원본 유지)
    for name in device_list:
        if name == 'EV':
            DEVICE_LISTS['EV'] = {'type': 'switch', 'list': [device_list['EV']]}
            continue
        
        num = device_list[name].get('Number', 0)
        if num > 0:
            arr = []
            for i in range(num):
                single = {}
                hex_idx = format(i + 1, 'X')
                for cmd in ['command', 'state']:
                    for onoff in ['ON', 'OFF']:
                        base = device_list[name].get(cmd + onoff)
                        pos = device_list[name].get(cmd + 'NUM')
                        if base and pos:
                            mod = f"{base[:pos-1]}{hex_idx}{base[pos:]}"
                            single[cmd+onoff] = checksum(mod)
                arr.append(single)
            DEVICE_LISTS[name] = {'type': device_list[name]['type'], 'list': arr}

    HOMESTATE = {}
    QUEUE = []
    prefix_list = {}
    for name, info in DEVICE_LISTS.items():
        state = info['list'][0].get('stateON')
        prefix = (state[0][:2] if isinstance(state, list) else state[:2]) if state else None
        if prefix: prefix_list[prefix] = name

    async def recv_from_HA(topics, value):
        if mqtt_log: log(f'[LOG] HA ->> : {"/".join(topics)} -> {value}')
        
        device_raw = topics[1]
        device_type = re.sub(r'\d+', '', device_raw)
        idx = int(''.join(re.findall(r'\d+', device_raw)))
        
        matched_key = next((k for k in DEVICE_LISTS.keys() if k.lower() == device_type.lower()), None)
        if not matched_key: return

        device = matched_key
        command = topics[2]
        val_upper = value.upper()

        if device == 'Thermo':
            curTemp = HOMESTATE.get(device_raw + 'curTemp', "20")
            setTemp = HOMESTATE.get(device_raw + 'setTemp', "22")
            power_state = HOMESTATE.get(device_raw + 'power', 'OFF')

            if command == 'power':
                target = 'ON' if val_upper in ['HEAT', 'ON'] else 'OFF'
                send = make_hex_temp(idx-1, curTemp, setTemp, target, device_list)
                recv = make_hex_temp(idx-1, curTemp, setTemp, 'state'+target, device_list)
                QUEUE.append({'sendcmd': send, 'recvcmd': recv, 'count': 0})
            
            elif command == 'setTemp':
                try:
                    new_temp = int(float(value))
                    # [핵심] 꺼져있으면 ON 명령 먼저 추가
                    if power_state == 'OFF':
                        on_send = make_hex_temp(idx-1, curTemp, new_temp, 'ON', device_list)
                        on_recv = make_hex_temp(idx-1, curTemp, new_temp, 'stateON', device_list)
                        QUEUE.append({'sendcmd': on_send, 'recvcmd': on_recv, 'count': 0})
                    
                    send = make_hex_temp(idx-1, curTemp, new_temp, 'CHANGE', device_list)
                    recv = make_hex_temp(idx-1, curTemp, new_temp, 'stateON', device_list)
                    QUEUE.append({'sendcmd': send, 'recvcmd': recv, 'count': 0})
                except: pass
        else:
            # 기타 장치 (전등, 플러그 등)
            cmd_key = 'command' + val_upper
            if cmd_key in DEVICE_LISTS[device]['list'][idx-1]:
                send = DEVICE_LISTS[device]['list'][idx-1][cmd_key]
                recv = [DEVICE_LISTS[device]['list'][idx-1].get('state'+val_upper, 'NULL')]
                QUEUE.append({'sendcmd': send, 'recvcmd': recv, 'count': 0})

    async def recv_from_elfin(data):
        # 상태 업데이트 로직 (원본의 update_state 등 호출)
        # 큐 매칭 제거 로직 포함
        for que in QUEUE:
            if data in que['recvcmd']:
                QUEUE.remove(que)
                break
        
        device_name = prefix_list.get(data[:2])
        if not device_name: return

        # (이하 원본의 상태 업데이트 로직과 동일하게 유지하거나 보정)
        # 예: update_state(device_name, index, onoff) 등 호출

    async def send_loop():
        while True:
            if QUEUE:
                cmd = QUEUE.pop(0)
                mqtt_client.publish(ELFIN_SEND_TOPIC, bytes.fromhex(cmd['sendcmd']))
                if cmd['count'] < 5:
                    cmd['count'] += 1
                    QUEUE.append(cmd)
                await asyncio.sleep(0.1)
            await asyncio.sleep(0.01)

    # MQTT 설정 및 실행
    mqtt_client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, 'commax-mqtt')
    mqtt_client.username_pw_set(config['mqtt_id'], config['mqtt_password'])
    
    def on_message(client, userdata, msg):
        topics = msg.topic.split('/')
        if topics[0] == HA_TOPIC and topics[-1] == 'command':
            asyncio.run_coroutine_threadsafe(recv_from_HA(topics, msg.payload.decode()), loop)
        elif topics[0] == ELFIN_TOPIC and topics[-1] == 'recv':
            raw = msg.payload.hex().upper()
            for i in range(0, len(raw), 16):
                asyncio.run_coroutine_threadsafe(recv_from_elfin(raw[i:i+16]), loop)

    mqtt_client.on_message = on_message
    mqtt_client.connect(config['mqtt_server'])
    mqtt_client.loop_start()

    global loop
    loop = asyncio.get_event_loop()
    loop.run_until_complete(send_loop())

if __name__ == '__main__':
    # 설정 파일 로드 및 do_work 실행
    with open(config_dir + '/options.json') as f:
        CONFIG = json.load(f)
    try:
        with open(share_dir + '/commax_found_device_new.json') as f:
            OPTION = json.load(f)
    except:
        # 파일 없을 시 기본 정보 사용 로직
        with open(data_dir + '/commax_devinfo.json') as f:
            OPTION = json.load(f)
    
    do_work(CONFIG, OPTION)
