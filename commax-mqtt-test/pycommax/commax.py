import paho.mqtt.client as mqtt
import json
import time
import asyncio
import telnetlib
import re

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
    return

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

def find_device(config):
    with open(data_dir + '/commax_devinfo.json') as file:
        dev_info = json.load(file)
    statePrefix = {dev_info[name]['stateON'][:2]: name for name in dev_info if dev_info[name].get('stateON')}
    device_num = {name: 0 for name in statePrefix.values()}
    collect_data = {name: set() for name in statePrefix.values()}

    target_time = time.time() + 20

    def on_connect(client, userdata, flags, rc, properties=None):
        nonlocal target_time
        target_time = time.time() + 20
        if rc == 0:
            log("Connected to MQTT broker..")
            log("Find devices for 20s..")
            client.subscribe(f'{ELFIN_TOPIC}/#', 0)
        else:
            log(f"Connection failed with code {rc}")

    def on_message(client, userdata, msg):
        raw_data = msg.payload.hex().upper()
        for k in range(0, len(raw_data), 16):
            data = raw_data[k:k + 16]
            if data == checksum(data) and data[:2] in statePrefix:
                name = statePrefix[data[:2]]
                collect_data[name].add(data)
                if dev_info[name].get('stateNUM'):
                    device_num[name] = max([device_num[name], int(data[int(dev_info[name]['stateNUM']) - 1])])
                else:
                    device_num[name] = 1

    mqtt_client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION1, 'commax-mqtt-finder')
    mqtt_client.username_pw_set(config['mqtt_id'], config['mqtt_password'])
    mqtt_client.on_connect = on_connect
    mqtt_client.on_message = on_message
    mqtt_client.connect(config['mqtt_server'])
    mqtt_client.loop_start()

    while time.time() < target_time:
        time.sleep(1)

    mqtt_client.loop_stop()

    log('기기 탐색 완료...')
    for name in collect_data:
        dev_info[name]['Number'] = device_num[name]
    
    with open(share_dir + '/commax_found_device.json', 'w', encoding='utf-8') as make_file:
        json.dump(dev_info, make_file, indent="\t")
    return dev_info

def do_work(config, device_list):
    debug = config.get('DEBUG', False)
    mqtt_log = config.get('mqtt_log', False)
    elfin_log = config.get('elfin_log', False)
    find_signal = config.get('save_unregistered_signal', False)

    def pad(value):
        try:
            val = int(float(value))
            return format(val, '02X')
        except:
            return '16'

    def make_hex(k, input_hex, change):
        if input_hex:
            try:
                change = int(change)
                # 기기 번호 인덱스 반영 (기본값에 k를 더함)
                base_idx = int(input_hex[change - 1], 16)
                new_idx = format(base_idx + k, 'X')
                input_hex = f'{input_hex[:change - 1]}{new_idx}{input_hex[change:]}'
            except:
                pass
        return checksum(input_hex)

    def make_hex_temp(k, curTemp, setTemp, state):
        # HA 명령 보정
        if state == 'heat': state = 'ON'
        if state == 'setTemp': state = 'CHANGE'
        state = state.upper()

        if state in ['OFF', 'ON', 'CHANGE']:
            tmp_hex = device_list['Thermo'].get('command' + state)
            change = device_list['Thermo'].get('commandNUM')
            
            if not tmp_hex: return None
            tmp_hex = make_hex(k, tmp_hex, change)
            
            if state in ['CHANGE', 'ON']:
                try:
                    setT = pad(setTemp)
                    chaTnum = device_list['Thermo'].get('chaTemp')
                    if chaTnum:
                        tmp_hex = tmp_hex[:chaTnum - 1] + setT + tmp_hex[chaTnum + 1:]
                except:
                    pass
            return checksum(tmp_hex)
        return None

    def make_device_info(dev_name, device_list):
        num = device_list[dev_name].get('Number', 0)
        if num > 0:
            arr = []
            for i in range(num):
                single_device = {}
                if dev_name.lower() == 'fan':
                    for key in ['commandOFF', 'commandON', 'stateOFF']:
                        single_device[key] = device_list[dev_name].get(key)
                    single_device['commandCHANGE'] = device_list[dev_name].get('commandCHANGE', [])
                    stateON = device_list[dev_name].get('stateON')
                    single_device['stateON'] = stateON if isinstance(stateON, list) else [stateON]
                elif dev_name.lower() in ['lightbreaker', 'gas']:
                    for key in ['commandON', 'commandOFF', 'stateON', 'stateOFF']:
                        single_device[key] = device_list[dev_name].get(key)
                else:
                    hex_index = format(i + 1, 'X')
                    for cmd in ['command', 'state']:
                        for onoff in ['ON', 'OFF']:
                            base_hex = device_list[dev_name].get(cmd + onoff)
                            change_pos = device_list[dev_name].get(cmd + 'NUM')
                            if base_hex and change_pos:
                                mod_hex = f"{base_hex[:change_pos - 1]}{hex_index}{base_hex[change_pos:]}"
                                single_device[cmd + onoff] = checksum(mod_hex)
                arr.append(single_device)
            return {'type': device_list[dev_name]['type'], 'list': arr}
        return None

    DEVICE_LISTS = {}
    for name in device_list:
        info = make_device_info(name, device_list)
        if info: DEVICE_LISTS[name] = info
    
    if 'EV' in device_list:
        DEVICE_LISTS['EV'] = {'type': 'switch', 'list': [device_list['EV']]}

    prefix_list = {}
    for name in DEVICE_LISTS:
        sample_state = DEVICE_LISTS[name]['list'][0].get('stateON')
        if sample_state:
            prefix = sample_state[0][:2] if isinstance(sample_state, list) else sample_state[:2]
            prefix_list[prefix] = name

    HOMESTATE = {}
    QUEUE = []
    COLLECTDATA = {'LastRecv': time.time_ns(), 'EVtime': time.time()}

    async def recv_from_HA(topics, value):
        if mqtt_log: log(f'[LOG] HA ->> : {"/".join(topics)} -> {value}')
        
        device_raw = topics[1]
        device_type = re.sub(r'\d+', '', device_raw)
        idx = int(''.join(re.findall(r'\d+', device_raw)))
        
        # 기기 타입 매칭 (대소문자 무시)
        matched_key = next((k for k in DEVICE_LISTS if k.lower() == device_type.lower()), None)
        if not matched_key: return

        key = device_raw + topics[2]
        val_upper = value.upper()

        if matched_key == 'Thermo':
            curTemp = HOMESTATE.get(device_raw + 'curTemp', '20')
            setTemp = HOMESTATE.get(device_raw + 'setTemp', '22')
            
            if topics[2] == 'power':
                cmd = 'ON' if value.lower() == 'heat' else 'OFF'
                sendcmd = make_hex_temp(idx - 1, curTemp, setTemp, cmd)
                if sendcmd: QUEUE.append({'sendcmd': sendcmd, 'recvcmd': [], 'count': 0})
            elif topics[2] == 'setTemp':
                sendcmd = make_hex_temp(idx - 1, curTemp, value, 'CHANGE')
                if sendcmd: QUEUE.append({'sendcmd': sendcmd, 'recvcmd': [], 'count': 0})

        elif matched_key == 'Fan':
            if topics[2] == 'power':
                sendcmd = DEVICE_LISTS['Fan']['list'][idx-1].get('command' + val_upper)
                if sendcmd: QUEUE.append({'sendcmd': sendcmd, 'recvcmd': [], 'count': 0})
            elif topics[2] == 'speed':
                try:
                    speed_idx = max(0, min(2, int(value) - 1))
                    sendcmd = DEVICE_LISTS['Fan']['list'][idx-1]['commandCHANGE'][speed_idx]
                    if sendcmd: QUEUE.append({'sendcmd': sendcmd, 'recvcmd': [], 'count': 0})
                except: pass
        else:
            sendcmd = DEVICE_LISTS[matched_key]['list'][idx-1].get('command' + val_upper)
            if sendcmd: QUEUE.append({'sendcmd': sendcmd, 'recvcmd': [], 'count': 0})

    async def slice_raw_data(raw_data):
        for k in range(0, len(raw_data), 16):
            data = raw_data[k:k + 16]
            if data == checksum(data):
                await recv_from_elfin(data)

    async def recv_from_elfin(data):
        COLLECTDATA['LastRecv'] = time.time_ns()
        device_name = prefix_list.get(data[:2])
        if not device_name: return

        if device_name == 'Thermo':
            curTnum, setTnum = device_list['Thermo']['curTemp'], device_list['Thermo']['setTemp']
            curT, setT = data[curTnum-1:curTnum+1], data[setTnum-1:setTnum+1]
            onoff = 'ON' if int(data[device_list['Thermo']['stateONOFFNUM']-1]) > 0 else 'OFF'
            idx = int(data[device_list['Thermo']['stateNUM']-1]) - 1
            await update_state('Thermo', idx, onoff)
            await update_temperature(idx, curT, setT)
        elif device_name == 'Fan':
            # 팬 상태 처리 로직...
            pass
        else:
            # 기타 기기 상태 갱신...
            pass

    async def update_state(device, idx, onoff):
        deviceID = f"{device}{idx+1}"
        key = deviceID + 'power'
        ha_val = 'heat' if (device == 'Thermo' and onoff == 'ON') else onoff
        if ha_val != HOMESTATE.get(key):
            HOMESTATE[key] = ha_val
            mqtt_client.publish(STATE_TOPIC.format(deviceID, 'power'), ha_val.encode())

    async def update_temperature(idx, curT, setT):
        deviceID = f"Thermo{idx+1}"
        for s, v in [('curTemp', curT), ('setTemp', setT)]:
            if v != HOMESTATE.get(deviceID + s):
                HOMESTATE[deviceID + s] = v
                mqtt_client.publish(STATE_TOPIC.format(deviceID, s), str(int(v, 16)).encode())

    def on_connect(client, userdata, flags, rc, properties=None):
        if rc == 0:
            log("MQTT Connected..")
            client.subscribe([(HA_TOPIC + '/#', 0), (ELFIN_TOPIC + '/recv', 0)])
            # Discovery 설정 생략 (기존 코드와 동일)

    def on_message(client, userdata, msg):
        topics = msg.topic.split('/')
        try:
            if topics[0] == HA_TOPIC and topics[-1] == 'command':
                asyncio.run(recv_from_HA(topics, msg.payload.decode('utf-8')))
            elif topics[0] == ELFIN_TOPIC and topics[-1] == 'recv':
                asyncio.run(slice_raw_data(msg.payload.hex().upper()))
        except: pass

    async def send_to_elfin():
        while True:
            if QUEUE and (time.time_ns() - COLLECTDATA['LastRecv'] > 100000000):
                q = QUEUE.pop(0)
                mqtt_client.publish(ELFIN_SEND_TOPIC, bytes.fromhex(q['sendcmd']))
                if q['count'] < 3:
                    q['count'] += 1
                    QUEUE.append(q)
            await asyncio.sleep(0.1)

    mqtt_client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, 'commax-mqtt-main')
    mqtt_client.username_pw_set(config['mqtt_id'], config['mqtt_password'])
    mqtt_client.on_connect = on_connect
    mqtt_client.on_message = on_message
    mqtt_client.connect(config['mqtt_server'])
    mqtt_client.loop_start()

    asyncio.run(send_to_elfin())

if __name__ == '__main__':
    with open(config_dir + '/options.json') as f:
        CONFIG = json.load(f)
    try:
        with open(share_dir + '/commax_found_device_new.json') as f:
            OPTION = json.load(f)
    except:
        OPTION = find_device(CONFIG)
    
    do_work(CONFIG, OPTION)
