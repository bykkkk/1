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
        userdata = time.time() + 20
        if rc == 0:
            log("Connected to MQTT broker..")
            log("Find devices for 20s..")
            client.subscribe(f'{ELFIN_TOPIC}/#', 0)
        else:
            errcode = {1: 'Connection refused - incorrect protocol version',
                       2: 'Connection refused - invalid client identifier',
                       3: 'Connection refused - server unavailable',
                       4: 'Connection refused - bad username or password',
                       5: 'Connection refused - not authorised'}
            log(errcode[rc])

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

    mqtt_client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION1, 'commax-mqtt')
    mqtt_client.username_pw_set(config['mqtt_id'], config['mqtt_password'])
    mqtt_client.on_connect = on_connect
    mqtt_client.on_message = on_message
    mqtt_client.connect_async(config['mqtt_server'])
    mqtt_client.user_data_set(target_time)
    mqtt_client.loop_start()

    while time.time() < target_time:
        pass

    mqtt_client.loop_stop()

    log('다음의 데이터를 찾았습니다...')
    log('======================================')
    for name in collect_data:
        collect_data[name] = sorted(collect_data[name])
        dev_info[name]['Number'] = device_num[name]
        log('DEVICE: {}'.format(name))
        log('Packets: {}'.format(collect_data[name]))
        log('-------------------')
    log('======================================')
    log('기기의 숫자만 변경하였습니다. 상태 패킷은 직접 수정하여야 합니다.')
    with open(share_dir + '/commax_found_device.json', 'w', encoding='utf-8') as make_file:
        json.dump(dev_info, make_file, indent="\t")
        log('기기리스트 저장 중 : /share/commax_found_device.json')
    return dev_info

def do_work(config, device_list):
    debug = config['DEBUG']
    mqtt_log = config['mqtt_log']
    elfin_log = config['elfin_log']
    find_signal = config['save_unregistered_signal']

    def pad(value):
        value = int(value)
        return '0' + str(value) if value < 10 else str(value)

    def make_hex(k, input_hex, change):
        if input_hex:
            try:
                change = int(change)
                input_hex = f'{input_hex[:change - 1]}{int(input_hex[change - 1]) + k}{input_hex[change:]}'
            except:
                pass
        return checksum(input_hex)

    def make_hex_temp(k, curTemp, setTemp, state):
        if state in ['OFF', 'ON', 'CHANGE']:
            tmp_hex = device_list['Thermo'].get('command' + state)
            change = device_list['Thermo'].get('commandNUM')
            tmp_hex = make_hex(k, tmp_hex, change)
            if state == 'CHANGE':
                setT = pad(setTemp)
                chaTnum = device_list['Thermo'].get('chaTemp')
                tmp_hex = tmp_hex[:chaTnum - 1] + setT + tmp_hex[chaTnum + 1:]
            return checksum(tmp_hex)
        else:
            tmp_hex = device_list['Thermo'].get(state)
            change = device_list['Thermo'].get('stateNUM')
            tmp_hex = make_hex(k, tmp_hex, change)
            setT = pad(setTemp)
            curT = pad(curTemp)
            curTnum = device_list['Thermo'].get('curTemp')
            setTnum = device_list['Thermo'].get('setTemp')
            tmp_hex = tmp_hex[:setTnum - 1] + setT + tmp_hex[setTnum + 1:]
            tmp_hex = tmp_hex[:curTnum - 1] + curT + tmp_hex[curTnum + 1:]
            if state == 'stateOFF':
                return checksum(tmp_hex)
            elif state == 'stateON':
                tmp_hex2 = tmp_hex[:3] + str(3) + tmp_hex[4:]
                return [checksum(tmp_hex), checksum(tmp_hex2)]
            else:
                return None

    def make_device_info(dev_name, device_list):
        num = device_list[dev_name].get('Number', 0)
        if num > 0:
            arr = []
            for i in range(num):
                single_device = {}
                if dev_name.lower() == 'fan':
                    single_device['commandOFF'] = device_list[dev_name].get('commandOFF')
                    single_device['commandON'] = device_list[dev_name].get('commandON')
                    single_device['commandCHANGE'] = device_list[dev_name].get('commandCHANGE', [])
                    stateON = device_list[dev_name].get('stateON')
                    single_device['stateON'] = stateON if isinstance(stateON, list) else [stateON]
                    single_device['stateOFF'] = device_list[dev_name].get('stateOFF')
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
        else:
            return None

    DEVICE_LISTS = {}
    for name in device_list:
        device_info = make_device_info(name, device_list)
        if device_info:
            DEVICE_LISTS[name] = device_info
    if 'EV' in device_list:
        DEVICE_LISTS['EV'] = {
            'type': 'switch',
            'list': [device_list['EV']]
        }
    prefix_list = {}
    log('----------------------')
    log('등록된 기기 목록 DEVICE_LISTS..')
    log('----------------------')
    for name in DEVICE_LISTS:
        state = DEVICE_LISTS[name]['list'][0].get('stateON')
        if state:
            prefix = state[0][:2] if isinstance(state, list) else state[:2]
            prefix_list[prefix] = name
        log(f'{name}: {DEVICE_LISTS[name]["type"]}')
        log(f' >>>> {DEVICE_LISTS[name]["list"]}')
    log('----------------------')

    if 'EV' in device_list and 'stateON' in device_list['EV']:
        ev_prefix = device_list['EV']['stateON'][:2]
        prefix_list[ev_prefix] = 'EV'

    HOMESTATE = {}
    QUEUE = []
    COLLECTDATA = {'cond': find_signal, 'data': set(), 'EVtime': time.time(), 'LastRecv': time.time_ns()}
    if find_signal:
        log('[LOG] 50개의 신호를 수집 중..')

    def map_percent_to_index(percent):
        return max(0, min(2, percent - 1))

    async def recv_from_HA(topics, value):
        if mqtt_log:
            log('[LOG] HA ->> : {} -> {}'.format('/'.join(topics), value))

        device_raw = topics[1]  # 예: "Fan1"
        device = re.sub(r'\d+', '', device_raw)  # 예: "Fan"
        idx = int(''.join(re.findall(r'\d+', topics[1])))  # "Fan1" → 1

    
        matched_device_key = None
        for dev in DEVICE_LISTS.keys():
            if dev.lower() == device.lower():
                matched_device_key = dev
                break

        if not matched_device_key:
            if debug:
                log(f"[DEBUG] DEVICE_LISTS에 {device} 없음. 현재 키: {list(DEVICE_LISTS.keys())}")
            return

        device = matched_device_key  # 올바른 키로 교체
        key = topics[1] + topics[2]
        value = value.lower()

        cur_state = HOMESTATE.get(key)
        if cur_state and value.upper() == cur_state:
            if debug:
                log('[DEBUG] {} is already set: {}'.format(key, value))
            return

        if device == 'Thermo':
            curTemp = HOMESTATE.get(topics[1] + 'curTemp')
            setTemp = HOMESTATE.get(topics[1] + 'setTemp')
            if topics[2] == 'power':
                sendcmd = make_hex_temp(idx - 1, curTemp, setTemp, value.upper())
                recvcmd = [make_hex_temp(idx - 1, curTemp, setTemp, 'state' + value.upper())]
                if sendcmd:
                    QUEUE.append({'sendcmd': sendcmd, 'recvcmd': recvcmd, 'count': 0})
                    if debug:
                        log('[DEBUG] Queued ::: sendcmd: {}, recvcmd: {}'.format(sendcmd, recvcmd))
            elif topics[2] == 'setTemp':
                try:
                    value = int(float(value))
                    if value != int(setTemp):
                        setTemp = value
                        sendcmd = make_hex_temp(idx - 1, curTemp, setTemp, 'CHANGE')
                        recvcmd = [make_hex_temp(idx - 1, curTemp, setTemp, 'stateON')]
                        if sendcmd:
                            QUEUE.append({'sendcmd': sendcmd, 'recvcmd': recvcmd, 'count': 0})
                            if debug:
                                log('[DEBUG] Queued ::: sendcmd: {}, recvcmd: {}'.format(sendcmd, recvcmd))
                except ValueError:
                    log(f"[WARNING] Invalid temperature value: {value}")
        elif device == 'Fan':
            if topics[2] == 'power':
                sendcmd = DEVICE_LISTS[device]['list'][idx-1].get('command' + value.upper())
                recvcmd = [DEVICE_LISTS[device]['list'][idx-1].get('state' + value.upper())]
                if sendcmd:
                    QUEUE.append({'sendcmd': sendcmd, 'recvcmd': recvcmd, 'count': 0})
                    if debug:
                        log('[DEBUG] Queued ::: sendcmd: {}, recvcmd: {}'.format(sendcmd, recvcmd))

            elif topics[2] == 'speed':
                try:
                    log(f"[DEBUG] 받은 speed value: {value} (type: {type(value)})")
                    percent = int(value)  # ← 이제 확실히 1~3임을 알고 있음
                  
                    if percent == 0:
                        # 전원 끄기 신호로 처리
                        sendcmd = DEVICE_LISTS[device]['list'][idx-1].get('commandOFF')
                        recvcmd = [DEVICE_LISTS[device]['list'][idx-1].get('stateOFF')]
                        QUEUE.append({'sendcmd': sendcmd, 'recvcmd': recvcmd, 'count': 0})
                        if debug:
                            log(f"[DEBUG] 퍼센트 0% 수신 → 전원 OFF 처리")
                    else:
                        index = map_percent_to_index(percent)
                        sendcmd = DEVICE_LISTS[device]['list'][idx-1]['commandCHANGE'][index]
                        recvcmd = [DEVICE_LISTS[device]['list'][idx-1]['stateON'][index]]
                        QUEUE.append({'sendcmd': sendcmd, 'recvcmd': recvcmd, 'count': 0})
                        if debug:
                            log(f"[DEBUG] Speed set (1~3): {percent} → index {index} → send: {sendcmd}")

                except Exception as e:
                    log(f"[ERROR] 팬 speed 처리 실패: {value} → {e}")
                else:
                    log(f"[WARNING] 알 수 없는 팬 속도 요청: {value}")


        else:
            sendcmd = DEVICE_LISTS[device]['list'][idx-1].get('command' + value.upper())
            if sendcmd:
                recvcmd = [DEVICE_LISTS[device]['list'][idx-1].get('state' + value.upper(), 'NULL')]
                QUEUE.append({'sendcmd': sendcmd, 'recvcmd': recvcmd, 'count': 0})
                if debug:
                    log('[DEBUG] Queued ::: sendcmd: {}, recvcmd: {}'.format(sendcmd, recvcmd))
            else:
                if debug:
                    log('[DEBUG] There is no command for {}'.format('/'.join(topics)))         
        
        
        if mqtt_log:
            log('[LOG] HA ->> : {} -> {}'.format('/'.join(topics), value))

        device = re.sub(r'\d+', '', topics[1]).lower()

        if device in DEVICE_LISTS:
            key = topics[1] + topics[2]
            idx = int(''.join(re.findall('\d', topics[1])))
            cur_state = HOMESTATE.get(key)
            value = 'ON' if value == 'heat' else value.upper()
            if cur_state:
                if value == cur_state:
                    if debug:
                        log('[DEBUG] {} is already set: {}'.format(key, value))
                else:
                    if device == 'Thermo':
                        curTemp = HOMESTATE.get(topics[1] + 'curTemp')
                        setTemp = HOMESTATE.get(topics[1] + 'setTemp')
                        if topics[2] == 'power':
                            sendcmd = make_hex_temp(idx - 1, curTemp, setTemp, value)
                            recvcmd = [make_hex_temp(idx - 1, curTemp, setTemp, 'state' + value)]
                            if sendcmd:
                                QUEUE.append({'sendcmd': sendcmd, 'recvcmd': recvcmd, 'count': 0})
                                if debug:
                                    log('[DEBUG] Queued ::: sendcmd: {}, recvcmd: {}'.format(sendcmd, recvcmd))
                        elif topics[2] == 'setTemp':
                            value = int(float(value))
                            if value == int(setTemp):
                                if debug:
                                    log('[DEBUG] {} is already set: {}'.format(topics[1], value))
                            else:
                                setTemp = value
                                sendcmd = make_hex_temp(idx - 1, curTemp, setTemp, 'CHANGE')
                                recvcmd = [make_hex_temp(idx - 1, curTemp, setTemp, 'stateON')]
                                if sendcmd:
                                    QUEUE.append({'sendcmd': sendcmd, 'recvcmd': recvcmd, 'count': 0})
                                    if debug:
                                        log('[DEBUG] Queued ::: sendcmd: {}, recvcmd: {}'.format(sendcmd, recvcmd))

                    elif device == 'Fan':
                        if topics[2] == 'power':
                            sendcmd = DEVICE_LISTS[device]['list'][idx-1].get('command' + value)
                            recvcmd = [DEVICE_LISTS[device]['list'][idx-1].get('state' + value)]
                            QUEUE.append({'sendcmd': sendcmd, 'recvcmd': recvcmd, 'count': 0})
                            if debug:
                                log('[DEBUG] Queued ::: sendcmd: {}, recvcmd: {}'.format(sendcmd, recvcmd))
                        elif topics[2] == 'speed':
                             speed_list = ['low', 'medium', 'high']
                             value = value.lower()
                             if value in speed_list:
                                 index = speed_list.index(value)
                                 try:
                                     sendcmd = DEVICE_LISTS[device]['list'][idx-1]['commandCHANGE'][index]
                                     recvcmd = [DEVICE_LISTS[device]['list'][idx-1]['stateON'][index]]
                                     QUEUE.append({'sendcmd': sendcmd, 'recvcmd': recvcmd, 'count': 0})
                                     if debug:
                                         log('[DEBUG] Fan speed change queued: {} => {}'.format(sendcmd, recvcmd))
                                 except Exception as e:
                                     log(f"[ERROR] 팬 속도 변경 실패: {e}")
                             else:
                                 log(f"[WARNING] 알 수 없는 팬 속도 요청: {value}")
                    else:
                        sendcmd = DEVICE_LISTS[device]['list'][idx-1].get('command' + value)
                        if sendcmd:
                            recvcmd = [DEVICE_LISTS[device]['list'][idx-1].get('state' + value, 'NULL')]
                            QUEUE.append({'sendcmd': sendcmd, 'recvcmd': recvcmd, 'count': 0})
                            if debug:
                                log('[DEBUG] Queued ::: sendcmd: {}, recvcmd: {}'.format(sendcmd, recvcmd))
                        else:
                            if debug:
                                log('[DEBUG] There is no command for {}'.format('/'.join(topics)))
            else:
                if debug:
                    log('[DEBUG] There is no command about {}'.format('/'.join(topics)))
        else:
            if debug:
                log('[DEBUG] There is no command for {}'.format('/'.join(topics)))

    async def slice_raw_data(raw_data):
        if elfin_log:
            log('[SIGNAL] receved: {}'.format(raw_data))
        cors = [recv_from_elfin(raw_data[k:k + 16]) for k in range(0, len(raw_data), 16) if raw_data[k:k + 16] == checksum(raw_data[k:k + 16])]
        await asyncio.gather(*cors)

    async def recv_from_elfin(data):
        COLLECTDATA['LastRecv'] = time.time_ns()
        if data:
            if HOMESTATE.get('EV1power') == 'ON':
                if COLLECTDATA['EVtime'] < time.time():
                    await update_state('EV', 0, 'OFF')
            for que in QUEUE:
                if data in que['recvcmd']:
                    QUEUE.remove(que)
                    if debug:
                        log('[DEBUG] Found matched hex: {}. Delete a queue: {}'.format(data, que))
                    break

            device_name = prefix_list.get(data[:2])
            if device_name == 'Thermo':
                curTnum = device_list['Thermo']['curTemp']
                setTnum = device_list['Thermo']['setTemp']
                curT = data[curTnum - 1:curTnum + 1]
                setT = data[setTnum - 1:setTnum + 1]
                onoffNUM = device_list['Thermo']['stateONOFFNUM']
                staNUM = device_list['Thermo']['stateNUM']
                index = int(data[staNUM - 1]) - 1
                onoff = 'ON' if int(data[onoffNUM - 1]) > 0 else 'OFF'
                await update_state(device_name, index, onoff)
                await update_temperature(index, curT, setT)
            elif device_name == 'Fan':
                stateON_list = DEVICE_LISTS['Fan']['list'][0].get('stateON', [])
                idx = 0  # Fan1
                
                if data in stateON_list:
                    speed = stateON_list.index(data)  # 0: High, 1: Medium, 2: Low
                    await update_state('Fan', idx, 'ON')
                    await update_fan(idx, speed)

                    # 🔵 풍속 텍스트 MQTT 발행
                    speed_texts = ['강', '중', '약']
                    speed_text = speed_texts[speed]
                    wind_topic = f"{HA_TOPIC}/Fan{idx+1}/wind_text/state"
                    mqtt_client.publish(wind_topic, speed_text.encode(), retain=True)
                    log(f"[DEBUG] 풍속 상태 MQTT 발행: {wind_topic} -> {speed_text}")

                    log(f"[DEBUG] 수신된 패킷: {data} → 속도: {['high', 'medium', 'low'][speed]}")

                elif data == DEVICE_LISTS['Fan']['list'][0].get('stateOFF'):
                    await update_state('Fan', idx, 'OFF')

                    # 🔵 꺼짐 상태도 MQTT로 발행
                    wind_topic = f"{HA_TOPIC}/Fan{idx+1}/wind_text/state"
                    mqtt_client.publish(wind_topic, "꺼짐".encode(), retain=True)
                    log(f"[DEBUG] 풍속 상태 MQTT 발행: {wind_topic} -> 꺼짐")

                else:
                    log(f"[WARNING] <{device_name}> 기기의 신호를 찾음: {data}")
                    log('[WARNING] 기기목록에 등록되지 않는 패킷입니다. JSON 파일을 확인하세요..')
            elif device_name == 'Outlet':
                staNUM = device_list['Outlet']['stateNUM']
                index = int(data[staNUM - 1]) - 1

                for onoff in ['OFF', 'ON']:
                    if data.startswith(DEVICE_LISTS[device_name]['list'][index]['state' + onoff][:8]):
                        await update_state(device_name, index, onoff)
                        if onoff == 'ON':
                            await update_outlet_value(index, data[10:14])
                        else:
                            await update_outlet_value(index, 0)
            elif device_name == 'EV':
                val = int(data[4:6], 16)
                BF = device_list['EV']['BasementFloor']
                val = str(int(val) - BF + 1) if val >= BF else 'B' + str(BF - int(val))
                await update_state('EV', 0, 'ON')
                await update_ev_value(0, val)
                COLLECTDATA['EVtime'] = time.time() + 3
            else:
                num = len(DEVICE_LISTS[device_name]['list'])
                state = [DEVICE_LISTS[device_name]['list'][k]['stateOFF'] for k in range(num)] + [
                    DEVICE_LISTS[device_name]['list'][k]['stateON'] for k in range(num)]
                if data in state:
                    index = state.index(data)
                    onoff, index = ['OFF', index] if index < num else ['ON', index - num]
                    await update_state(device_name, index, onoff)
                else:
                    log(f"[WARNING] <{device_name}> 기기의 신호를 찾음: {data}")
                    log('[WARNING] 기기목록에 등록되지 않는 패킷입니다. JSON 파일을 확인하세요..')

    async def update_state(device, idx, onoff):
        state = 'power'
        deviceID = device + str(idx + 1)
        key = deviceID + state

        if onoff != HOMESTATE.get(key):
            HOMESTATE[key] = onoff
            topic = STATE_TOPIC.format(deviceID, state)
            mqtt_client.publish(topic, onoff.encode())
            if mqtt_log:
                log('[LOG] ->> HA : {} >> {}'.format(topic, onoff))
        else:
            if debug:
                log('[DEBUG] {} is already set: {}'.format(deviceID, onoff))
        return

    async def update_fan(idx, speed):
        deviceID = 'Fan' + str(idx + 1)
        speed_list = ['low', 'medium', 'high']

        if isinstance(speed, int) and 0 <= speed < len(speed_list):
            # 프리셋 문자열 발행 (원하면 유지 가능)
            speed_str = speed_list[speed]
            preset_topic = STATE_TOPIC.format(deviceID, 'preset_mode')
            mqtt_client.publish(preset_topic, speed_str.encode())  # retain 제거됨

            log(f'[DEBUG] 프리셋 발행: {preset_topic} -> {speed_str}')
            if mqtt_log:
                log(f'[LOG] ->> HA : {preset_topic} >> {speed_str}')

            # 퍼센트 발행 (retain ✅ 유지)
            percent_map = {0: 100, 1: 67, 2: 33}
            percent_value = percent_map.get(speed, 33)
            percent_topic = f"{HA_TOPIC}/Fan{idx+1}/percentage/state"
            mqtt_client.publish(percent_topic, str(percent_value).encode(), retain=True)

            log(f'[DEBUG] 퍼센트 발행: {percent_topic} -> {percent_value}')
            if mqtt_log:
                log(f'[LOG] ->> HA : {percent_topic} >> {percent_value}')

    async def update_temperature(idx, curTemp, setTemp):
        deviceID = 'Thermo' + str(idx + 1)
        temperature = {'curTemp': pad(curTemp), 'setTemp': pad(setTemp)}
        for state in temperature:
            key = deviceID + state
            val = temperature[state]
            if val != HOMESTATE.get(key):
                HOMESTATE[key] = val
                topic = STATE_TOPIC.format(deviceID, state)
                mqtt_client.publish(topic, val.encode())
                if mqtt_log:
                    log('[LOG] ->> HA : {} -> {}'.format(topic, val))
            else:
                if debug:
                    log('[DEBUG] {} is already set: {}'.format(key, val))
        return

    async def update_outlet_value(idx, val):
        deviceID = 'Outlet' + str(idx + 1)
        try:
            val = '%.1f' % float(int(val) / 10)
            topic = STATE_TOPIC.format(deviceID, 'watt')
            mqtt_client.publish(topic, val.encode())
            if debug:
                log('[LOG] ->> HA : {} -> {}'.format(topic, val))
        except:
            pass

    async def update_ev_value(idx, val):
        deviceID = 'EV' + str(idx + 1)
        topic = STATE_TOPIC.format(deviceID, 'floor')
        mqtt_client.publish(topic, val.encode())
        if debug:
            log('[LOG] ->> HA : {} -> {}'.format(topic, val))

    def on_connect(client, userdata, flags, rc,  properties=None):
        if rc == 0:
            log("MQTT 접속 중..")
            client.subscribe([(HA_TOPIC + '/#', 0), (ELFIN_TOPIC + '/recv', 0), (ELFIN_TOPIC + '/send', 1)])
            if 'EV' in DEVICE_LISTS:
                asyncio.run(update_state('EV', 0, 'OFF'))
            for device in DEVICE_LISTS:
                for idx in range(len(DEVICE_LISTS[device]['list'])):
                    config_topic = f'homeassistant/{DEVICE_LISTS[device]["type"]}/commax_{device.lower()}{idx + 1}/config'
                    if DEVICE_LISTS[device]["type"] == "climate":
                        payload = {
                            "device": {
                                "identifiers": "commax",
                                "name": "코맥스 월패드",
                                "manufacturer": "commax",
                            },
                            "device_class": DEVICE_LISTS[device]["type"],
                            "name": f'{device}{idx+1}',
                            "object_id": f'commax_{device.lower()}{idx + 1}',
                            "unique_id": f'commax_{device.lower()}{idx + 1}',
                            "entity_category": 'config',
                            "mode_cmd_t": f"{HA_TOPIC}/{device}{idx+1}/power/command",
                            "mode_stat_t": f"{HA_TOPIC}/{device}{idx+1}/power/state",
                            "temp_cmd_t": f"{HA_TOPIC}/{device}{idx+1}/setTemp/command",
                            "temp_stat_t": f"{HA_TOPIC}/{device}{idx+1}/setTemp/state",
                            "curr_temp_t": f"{HA_TOPIC}/{device}{idx+1}/curTemp/state",
                            "min_temp":"10",
                            "max_temp":"30",
                            "temp_step":"1",
                            "modes":["off", "heat"],
                            "mode_state_template": "{% set modes = {'OFF': 'off', 'ON': 'heat'} %} {{modes[value] if value in modes.keys() else 'off'}}"
                        }
                    elif DEVICE_LISTS[device]["type"] == "fan":
                        config_topic = f'homeassistant/fan/commax_{device.lower()}{idx + 1}/config'
                        payload = {
                            "device": {
                                "identifiers": "commax",
                                "name": "코맥스 월패드",
                                "manufacturer": "commax",
                            },
                            "name": f"{device}{idx+1}",
                            "unique_id": f"commax_{device.lower()}{idx + 1}",
                            "command_topic": f"{HA_TOPIC}/{device}{idx+1}/power/command",
                            "state_topic": f"{HA_TOPIC}/{device}{idx+1}/power/state",
                            "percentage_command_topic": f"{HA_TOPIC}/{device}{idx+1}/speed/command",
                            "percentage_state_topic": f"{HA_TOPIC}/{device}{idx+1}/percentage/state",  # ✅ 퍼센트 전용
                            "preset_mode_state_topic": f"{HA_TOPIC}/{device}{idx+1}/speed/state",       # ✅ 문자열용 (선택)
                            "preset_modes": ["low", "medium", "high"],
                            "speed_range_min": 1,
                            "speed_range_max": 3,
                            "payload_on": "ON",
                            "payload_off": "OFF"
                        }
                        mqtt_client.publish(config_topic, json.dumps(payload))
                    else:
                        payload = {
                            "device": {
                                "identifiers": "commax",
                                "name": "코맥스 월패드",
                                "manufacturer": "commax",
                            },
                            "~": f'{HA_TOPIC}/{device}{idx + 1}/power',
                            "device_class": DEVICE_LISTS[device]["type"],
                            "name": f'{device}{idx+1}',
                            "object_id": f'commax_{device.lower()}{idx + 1}',
                            "unique_id": f'commax_{device.lower()}{idx + 1}',
                            "cmd_t": "~/command",
                            "stat_t": "~/state"}
                        if device == "Outlet":
                            payload["device_class"] = 'outlet'
                            payload["entity_category"] = 'diagnostic'

                    mqtt_client.publish(config_topic, json.dumps(payload))
                    if device == "Outlet":
                        config_topic = f'homeassistant/sensor/commax_{device}{idx + 1}_watt/config'
                        payload = {
                            "device": {
                                "identifiers": "commax",
                                "name": "코맥스 월패드",
                                "manufacturer": "commax",
                            },
                            "device_class": 'energy',
                            "name": f'{device}{idx + 1} Watt',
                            "object_id": f'commax_{device.lower()}{idx + 1}_watt',
                            "unique_id": f'commax_{device.lower()}{idx + 1}_watt',
                            "entity_category": 'diagnostic',
                            "stat_t": f'{HA_TOPIC}/{device}{idx + 1}/watt/state',
                            "unit_of_measurement": "W"
                        }
                        mqtt_client.publish(config_topic, json.dumps(payload))
        else:
            errcode = {1: 'Connection refused - incorrect protocol version',
                       2: 'Connection refused - invalid client identifier',
                       3: 'Connection refused - server unavailable',
                       4: 'Connection refused - bad username or password',
                       5: 'Connection refused - not authorised'}
            log(errcode[rc])

    def on_message(client, userdata, msg):
        topics = msg.topic.split('/')
        try:
            if topics[0] == HA_TOPIC and topics[-1] == 'command':
                asyncio.run(recv_from_HA(topics, msg.payload.decode('utf-8')))
            elif topics[0] == ELFIN_TOPIC and topics[-1] == 'recv':
                asyncio.run(slice_raw_data(msg.payload.hex().upper()))
        except:
            pass

    async def send_to_elfin():
        while True:
            try:
                if time.time_ns() - COLLECTDATA['LastRecv'] > 10000000000:
                    log('[WARNING] 10초간 신호를 받지 못했습니다. ew11 기기를 재시작합니다.')
                    try:
                        elfin_id = config['elfin_id']
                        elfin_password = config['elfin_password']
                        elfin_server = config['elfin_server']

                        ew11 = telnetlib.Telnet(elfin_server)
                        ew11.read_until(b"login:")
                        ew11.write(elfin_id.encode('utf-8') + b'\n')
                        ew11.read_until(b"password:")
                        ew11.write(elfin_password.encode('utf-8') + b'\n')
                        ew11.write('Restart'.encode('utf-8') + b'\n')
                        await asyncio.sleep(10)
                    except:
                        log('[WARNING] 기기 재시작 오류! 기기 상태를 확인하세요.')
                    COLLECTDATA['LastRecv'] = time.time_ns()
                elif time.time_ns() - COLLECTDATA['LastRecv'] > 100000000:
                    if QUEUE:
                        send_data = QUEUE.pop(0)
                        if elfin_log:
                            log('[SIGNAL] 신호 전송: {}'.format(send_data))
                        mqtt_client.publish(ELFIN_SEND_TOPIC, bytes.fromhex(send_data['sendcmd']))
                        if send_data['count'] < 5:
                            send_data['count'] += 1
                            QUEUE.append(send_data)
                        else:
                            if elfin_log:
                                log('[SIGNAL] Send over 5 times. Delete queue: {}'.format(send_data))
            except Exception as err:
                log('[ERROR] send_to_elfin(): {}'.format(err))
                return True
            await asyncio.sleep(0.01)

    mqtt_client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, 'commax-mqtt')
    mqtt_client.username_pw_set(config['mqtt_id'], config['mqtt_password'])
    mqtt_client.on_connect = on_connect
    mqtt_client.on_message = on_message
    mqtt_client.connect_async(config['mqtt_server'])
    mqtt_client.loop_start()

    loop = asyncio.get_event_loop()
    while True:
        loop.run_until_complete(send_to_elfin())
    loop.close()
    mqtt_client.loop_stop()

if __name__ == '__main__':
    with open(config_dir + '/options.json') as file:
        CONFIG = json.load(file)
    try:
        with open(share_dir + '/commax_found_device_new.json') as file:
            log('기기 정보 파일을 찾음: /share/commax_found_device_new.json')
            OPTION = json.load(file)
    except IOError:
        log('기기 정보 파일이 없습니다.: /share/commax_found_device_new.json')
        OPTION = find_device(CONFIG)
    while True:
        do_work(CONFIG, OPTION)
