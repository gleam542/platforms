from Crypto.Cipher import AES
from functools import wraps
from urllib import parse
import werdsazxc
import platforms
import requests
import operator
import hashlib
import logging
import socket
import base64
import copy
import html
import json
import time
import re
logger = logging.getLogger('robot')


class EncryptSession(requests.Session):
    def __init__(self, secret_key):
        super().__init__()
        self.key = hashlib.md5(secret_key.encode('utf-8')).hexdigest().encode()

    def pad(self, s):
        return s +(AES.block_size - len(s)% AES.block_size)* chr(AES.block_size - len(s)% AES.block_size).encode('utf-8')

    def unpad(self, s):
        return s[0:-ord(s[-1])]

    def encrypt(self, data):
        if data is None:
            return data
        if type(data) in (dict, list):
            data = json.dumps(data, ensure_ascii=False).encode('utf-8')
        elif type(data) == str:
            data = data.encode('utf-8')
        cryptor = AES.new(self.key, AES.MODE_CBC, self.key[:16])
        data = cryptor.encrypt(self.pad(data))
        data = base64.b64encode(data)
        return data

    def decrypt(self, data):
        cryptor = AES.new(self.key, AES.MODE_CBC, self.key[:16])
        data = base64.b64decode(data)
        data = cryptor.decrypt(data).decode()
        data = self.unpad(data)
        return data

    def post(self, url, data=None, **kwargs):
        data = copy.deepcopy(data)
        # 加密
        if data and data.get('data'):
            data['data']['data'] = self.encrypt(data['data'].get('data', '')).decode()
            data = parse.quote(json.dumps(data))
            # logger.info(f'PR6后台[加密完成]>>{data}')

        # 呼叫API
        result = super().post(url, data={'JDATA': data}, **kwargs)
        # 移除BOM
        content = re.sub(b'^\xef\xbb\xbf', b'', result.content)
        # Json解析
        try:
            result.js = json.loads(content)
        except json.JSONDecodeError as e:
            logger.error(f'PR6后台[解析失敗]>>{result.content}')
        except UnicodeDecodeError as e:
            logger.error(f'PR6后台[編碼異常]>>{result.content}')
        # data、errcode 解密
        if hasattr(result, 'js'):
            if type(result.js) == dict:
                if type(result.js.get('data')) == str:
                    try:
                        result.js['data'] = self.decrypt(result.js['data'])
                        result.js['data'] = html.unescape(result.js['data'])
                        result.js['data'] = json.loads(result.js['data'])
                    except Exception as e:
                        logger.error(f'PR6后台[資料解密失敗]>>{result.content}')
                if type(result.js.get('errcode')) == str:
                    try:
                        result.js['errcode'] = self.decrypt(result.js['errcode'])
                        result.js['errcode'] = json.loads(result.js['errcode'])
                    except Exception as e:
                        logger.error(f'PR6后台[錯誤訊息解密失敗]>>{result.content}')
                        result.js['errcode'] = None

        return result


def nested_getattr(obj, attr_path, **kwargs):
    if kwargs.get('default') is not None:
        try:
            return operator.attrgetter(attr_path)(obj)
        except AttributeError as e:
            return kwargs.get('default')
    else:
        return operator.attrgetter(attr_path)(obj)


# 呼叫PR6后台
def backend_api(act, url, item, retry_times, cf, **kwargs):
    '''
    與PR6后台行資料交換, 並解析狀態碼、Json格式後回傳
    Args:
        item: 要傳入PR6后台的參數內容
    Return:
        result: 以下兩者皆成功為true(其餘為false):
                PR6后台回傳為true
                機器人解析json成功
        error: PR6后台回傳為false時為PR6后台回傳錯誤訊息
               機器人解析json失敗、解密失敗時、程序報錯時, 為機器人Exception訊息
        data: PR6后台回傳API目標內容
    '''
    global session
    session = globals().get('session')
    if not session or getattr(session, 'secret_key', None) != cf['secret_key']:
        session = EncryptSession(cf['secret_key'])

    # 參數內容調整
    url = url + 'bkd.sys.php'
    item = {
        'token': '' if act == '登入PR6后台' else cf['token'],
        'data': item,
    }
    # 紀錄參數
    # logger.debug(f'PR6后台[{act}]>>>網址: {url}')
    logger.info(f'PR6后台[{act}]>>>參數: {item}')

    i = 0
    while i < retry_times:
        try:
            req = session.post(url, data=item, timeout=cf['timeout'])
            logger.debug(f'PR6后台[{act}]>>>返回: {req.status_code} {req.content}')

            if req.status_code == 504:
                return {
                    'result': False,
                    'error': f'PR6后台[{act}]: PR6后台回应504错误, 请与开发团队联系',
                    'data': ''
                }
            if req.status_code == 403:
                return {
                    'result': False,
                    'error': f'PR6后台[{act}]: PR6后台回应403错误，请确认PR6后台是否綁定機器人IP白名单。',
                    'data': ''
                }
            if req.status_code == 404:
                return {
                    'result': False,
                    'error': f'PR6后台[{act}]: PR6后台回应404错误, 请确认PR6后台网址是否有误，再次点选按钮启动机器人',
                    'data': ''
                }
            if req.status_code != 200:
                return {
                    'result': False,
                    'error': f'PR6后台[{act}]: PR6后台回应状态码异常({req.status_code}), 请确认设定是否正确',
                    'data': ''
                }
            if hasattr(req, 'js'):
                if type(req.js) != dict:
                    return {
                        'result': False,
                        'error': f'PR6后台[{act}]: 回传内容错误，请联系开发团队。',
                        'RawStatusCode': req.status_code,
                        'RawContent': req.content
                    }
                if type(req.js.get('data')) not in (list, dict):
                    return {
                        'result': False,
                        'error': f'PR6后台[{act}]: 密文解析错误, 请检查密钥设定是否与PR6后台一致',
                        'RawStatusCode': req.status_code,
                        'RawContent': req.content
                    }
            # 嘗試json解析, 讓流程走到JsonDecodeError
            if not hasattr(req, 'js'):
                json.loads(req.content)
            # 發生連線逾時自動嘗試重連
            if '连线逾时' in req.js['error'] and cf['need_bk_otp'] is False:
                login_api(cf)
                item['token'] = cf['token']
                i += 1
                continue
            # 試圖回傳詳細錯誤內容
            if req.js.get('errcode'):
                result = {
                    'result': bool(req.js['result']),
                    'error': f"PR6后台[{act}]: {req.js.get('errcode')}",
                    'data': req.js.get('data', [])
                }
            else:
                # 一般回傳
                result = {
                    'result': bool(req.js['result']),
                    'error': f"PR6后台[{act}]: {req.js['error'][0]}" if req.js['error'] else '',
                    'data': req.js.get('data', [])
                }
            if '连线逾时' in result['error']:
                result['error'] = result['error'] + '，請重新登入'
            if '机器人帐号已在其他地方登入' in result['error']:
                result['error'] = result['error'] + '，請重新登入'
            return result

        except requests.exceptions.MissingSchema as e:
            return {
                'result': False,
                'error': '通讯协定(http或https)未输入'
            }
        except requests.exceptions.InvalidSchema as e:
            return {
                'result': False,
                'error': '通讯协定(http或https)无法解析, 请检察PR6后台域名设定是否正确'
            }
        except requests.exceptions.InvalidURL as e:
            return {
                'result': False,
                'error': '[PR6后台]无法解析, 请检察PR6后台设定是否正确'
            }
        except requests.exceptions.TooManyRedirects as e:
            return {
                'result': False,
                'error': f'[PR6后台]连线发生重导向超过上限, 请检查PR6后台是否输入正确',
            }
        except json.JSONDecodeError as e:
            # 設定異常狀態訊息
            if f'PR6后台[{act}]>>解析错误' not in cf['error_msg']:
                cf['error_msg'].append(f'PR6后台[{act}]>>解析错误')
            # 紀錄檔紀錄訊息
            logger.info(f'---解析错误({e.__class__.__name__})({i+1}/{retry_times})---------------------')
            time.sleep(1)
            i += 1
            if i >= retry_times:
                return {
                    'result': False,
                    'error': f'PR6后台[{act}]: 解析错误>>重试超过上限',
                    'data': []
                }
        except (UnicodeEncodeError, UnicodeDecodeError) as e:
            # 設定異常狀態訊息
            if f'PR6后台[{act}]>>编码异常' not in cf['error_msg']:
                cf['error_msg'].append(f'PR6后台[{act}]>>编码异常')
            # 紀錄檔紀錄訊息
            logger.info(f'---编码异常({e.__class__.__name__})({i+1}/{retry_times})---------------------')
            logger.debug(req.content)
            time.sleep(1)
            i += 1
            if i >= retry_times:
                return {
                    'result': False,
                    'error': f'PR6后台[{act}]: 编码异常>>重试超过上限',
                    'data': []
                }
        except (requests.exceptions.ConnectionError, requests.exceptions.Timeout) as e:
            # 設定異常狀態訊息
            if f'PR6后台[{act}]>>连线异常' not in cf['error_msg']:
                cf['error_msg'].append(f'PR6后台[{act}]>>连线异常')
            # 紀錄檔紀錄訊息
            logger.info(f'---连线异常({e.__class__.__name__})({i+1}/{retry_times})---------------------')
            time.sleep(1)
            i += 1
            if i >= retry_times:
                return {
                    'result': False,
                    'error': f'PR6后台[{act}]: 连线异常>>重试超过上限',
                    'data': []
                }
        except Exception as e:
            werdsazxc.log_trackback()
            return {
                'result': False,
                'error': f'{e.__class__.__name__}：{e}，请寻求开发人员的帮助(Exception)。'.replace("'", "").replace('"','')
            }
    return {
        'result': False,
        'error': f'PR6后台[{act}]: 重试超过上限',
        'data': []
    }

#取得PR6後台時間
def get_time_api(act, url, item, retry_times, cf, **kwargs):
    '''
    與PR6后台行資料交換, 並解析狀態碼、Json格式後回傳
    Args:
        item: 要傳入PR6后台的參數內容
    Return:
        result: 以下兩者皆成功為true(其餘為false):
                PR6后台回傳為true
                機器人解析json成功
        error: PR6后台回傳為false時為PR6后台回傳錯誤訊息
               機器人解析json失敗、解密失敗時、程序報錯時, 為機器人Exception訊息
        data: PR6后台回傳API目標內容
    '''
    global session
    session = globals().get('session')
    if not session or getattr(session, 'secret_key', None) != cf['secret_key']:
        session = EncryptSession(cf['secret_key'])
    logger.info(f'session = {cf}')
    # 參數內容調整
    url = url + 'bkd.sys.php'
    item = {"mod": "sys", "app": "login", "func": "time"}
    item = {
        'token': '' if act == '登入PR6后台' else cf['token'],
        'data': item,
    }
    # 紀錄參數
    # logger.debug(f'PR6后台[{act}]>>>網址: {url}')
    logger.info(f'PR6后台[{act}]>>>參數: {item}')

    i = 0
    while i < retry_times:
        try:
            req = session.post(url, data=item, timeout=cf['timeout'])
            logger.debug(f'PR6后台[{act}]>>>返回: {req.status_code} {req.content}')

            if req.status_code == 504:
                return {
                    'result': False,
                    'error': f'PR6后台[{act}]: PR6后台回应504错误, 请与开发团队联系',
                    'data': ''
                }
            if req.status_code == 403:
                return {
                    'result': False,
                    'error': f'PR6后台[{act}]: PR6后台回应403错误，请确认PR6后台是否綁定機器人IP白名单。',
                    'data': ''
                }
            if req.status_code == 404:
                return {
                    'result': False,
                    'error': f'PR6后台[{act}]: PR6后台回应404错误, 请确认PR6后台网址是否有误，再次点选按钮启动机器人',
                    'data': ''
                }
            if req.status_code != 200:
                return {
                    'result': False,
                    'error': f'PR6后台[{act}]: PR6后台回应状态码异常({req.status_code}), 请确认设定是否正确',
                    'data': ''
                }
            if hasattr(req, 'js'):
                if type(req.js) != dict:
                    return {
                        'result': False,
                        'error': f'PR6后台[{act}]: 回传内容错误，请联系开发团队。',
                        'RawStatusCode': req.status_code,
                        'RawContent': req.content
                    }
                if type(req.js.get('data')) not in (list, dict):
                    return {
                        'result': False,
                        'error': f'PR6后台[{act}]: 密文解析错误, 请检查密钥设定是否与PR6后台一致',
                        'RawStatusCode': req.status_code,
                        'RawContent': req.content
                    }
            # 嘗試json解析, 讓流程走到JsonDecodeError
            if not hasattr(req, 'js'):
                json.loads(req.content)
            # 發生連線逾時自動嘗試重連
            if '连线逾时' in req.js['error'] and cf['need_bk_otp'] is False:
                login_api(cf)
                item['token'] = cf['token']
                i += 1
                continue
            # 試圖回傳詳細錯誤內容
            if req.js.get('errcode'):
                result = {
                    'result': bool(req.js['result']),
                    'error': f"PR6后台[{act}]: {req.js.get('errcode')}",
                    'data': req.js.get('data', [])
                }
            else:
                # 一般回傳
                result = {
                    'result': bool(req.js['result']),
                    'error': f"PR6后台[{act}]: {req.js['error'][0]}" if req.js['error'] else '',
                    'data': req.js.get('data', [])
                }
            if '连线逾时' in result['error']:
                result['error'] = result['error'] + '，請重新登入'
            if '机器人帐号已在其他地方登入' in result['error']:
                result['error'] = result['error'] + '，請重新登入'
            logger.info(f'backend_api的result{result}')
            return result

        except requests.exceptions.MissingSchema as e:
            return {
                'result': False,
                'error': '通讯协定(http或https)未输入'
            }
        except requests.exceptions.InvalidSchema as e:
            return {
                'result': False,
                'error': '通讯协定(http或https)无法解析, 请检察PR6后台域名设定是否正确'
            }
        except requests.exceptions.InvalidURL as e:
            return {
                'result': False,
                'error': '[PR6后台]无法解析, 请检察PR6后台设定是否正确'
            }
        except requests.exceptions.TooManyRedirects as e:
            return {
                'result': False,
                'error': f'[PR6后台]连线发生重导向超过上限, 请检查PR6后台是否输入正确',
            }
        except json.JSONDecodeError as e:
            # 設定異常狀態訊息
            if f'PR6后台[{act}]>>解析错误' not in cf['error_msg']:
                cf['error_msg'].append(f'PR6后台[{act}]>>解析错误')
            # 紀錄檔紀錄訊息
            logger.info(f'---解析错误({e.__class__.__name__})({i+1}/{retry_times})---------------------')
            time.sleep(1)
            i += 1
            if i >= retry_times:
                return {
                    'result': False,
                    'error': f'PR6后台[{act}]: 解析错误>>重试超过上限',
                    'data': []
                }
        except (UnicodeEncodeError, UnicodeDecodeError) as e:
            # 設定異常狀態訊息
            if f'PR6后台[{act}]>>编码异常' not in cf['error_msg']:
                cf['error_msg'].append(f'PR6后台[{act}]>>编码异常')
            # 紀錄檔紀錄訊息
            logger.info(f'---编码异常({e.__class__.__name__})({i+1}/{retry_times})---------------------')
            logger.debug(req.content)
            time.sleep(1)
            i += 1
            if i >= retry_times:
                return {
                    'result': False,
                    'error': f'PR6后台[{act}]: 编码异常>>重试超过上限',
                    'data': []
                }
        except (requests.exceptions.ConnectionError, requests.exceptions.Timeout) as e:
            # 設定異常狀態訊息
            if f'PR6后台[{act}]>>连线异常' not in cf['error_msg']:
                cf['error_msg'].append(f'PR6后台[{act}]>>连线异常')
            # 紀錄檔紀錄訊息
            logger.info(f'---连线异常({e.__class__.__name__})({i+1}/{retry_times})---------------------')
            time.sleep(1)
            i += 1
            if i >= retry_times:
                return {
                    'result': False,
                    'error': f'PR6后台[{act}]: 连线异常>>重试超过上限',
                    'data': []
                }
        except Exception as e:
            werdsazxc.log_trackback()
            return {
                'result': False,
                'error': f'{e.__class__.__name__}：{e}，请寻求开发人员的帮助(Exception)。'.replace("'", "").replace('"','')
            }
    return {
        'result': False,
        'error': f'PR6后台[{act}]: 重试超过上限',
        'data': []
    }

# 登入平台
def login_platform(platform, url, acc, pw, otp, timeout, cf):
    '''
    登入平台, 並解析各錯誤內容進行動作.
    Args:
        platform: 平台物件, 具有login方法
        cf: 機器人設置內容
    Return:
        <str>: 程式執行發生異常, 需要中止機器人並跳出錯誤視窗
        False: 連線發生異常, 需要中止機器人, 但是不需跳出錯誤視窗
        "登入成功": 登入已經完成, 可以準備執行任務
    '''
    result1 = platform.login(
        url=url,
        acc=acc,
        pw=pw,
        otp=otp,
        timeout=timeout,
        cf=cf,
    )
    # 清空 otp 內容
    cf['backend_otp'] = ''

    # 連線異常時, 設定連線為false, 回傳為false
    if result1["ErrorCode"] == platforms.CODE_DICT['CONNECTION_CODE']['code']:
        cf['connect'] = False
        return result1['ErrorMessage']

    # 程式異常時, 回傳錯誤訊息, 用於跳出視窗
    if result1['ErrorCode'] != platforms.CODE_DICT['SUCCESS_CODE']['code']:
        return result1['ErrorMessage']

    # 登入成功
    return '登入成功'


# 子系统清单
def get_list(cf):
    '''
    子系统清单, 獲取子系统清单.
    Args:
        cf: 機器人設置內容
    Return:
        <str>: 程式執行發生異常, 需要中止機器人並跳出錯誤視窗
        False: 連線發生異常, 需要中止機器人, 但是不需跳出錯誤視窗
        "登入成功": 登入已經完成, 可以準備執行任務
    '''
    source = {
        'mod': 'sys',
        'app': 'robot',
        'func': 'list',
        'version': cf['api_version'],
    }
    result = backend_api(
        act='子系统清单',
        url=cf['api'],
        item=source,
        retry_times=cf['retry_times'],
        cf=cf
    )
    logger.info(f'PR6后台[子系统清单]>>>解析: {result}')

    # 連線異常時, 設定連線為false, 回傳為false
    if '重试超过上限' in result["error"]:
        cf['connect'] = False
        return result["error"]

    # 程式異常時, 回傳錯誤訊息, 用於跳出視窗
    elif '密文解析错误' in result.get('error'):
        return '密文解析错误，请检查机器人密钥与PR6后台密钥是否匹配'
    elif '504错误' in result.get('error'):
        return '与PR6后台连线异常(504)，请与开发团队联系'
    elif '404错误' in result.get('error'):
        return '请确认PR6后台网址是否有误或者網路是否正常後，再次点选按钮启动机器人'
    elif not result['result']:
        return result['error']


    # 加上充值選項, 使畫面可以設定
    platform = getattr(platforms, cf['platform'].lower(), None)
    cf['game_setting'] = werdsazxc.Dict({
        mod: {
            'name': name,
            'mod': mod,
            'func': func,
            'extra': {
                k: v['default']
                for k, v in nested_getattr(
                    platform.mission,
                    f'{name}.Meta.extra',
                    default={}
                ).items()
            },
            'monitor': cf['game_setting'].get(mod, {}).get('monitor', 0),
            'deposit': cf['game_setting'].get(mod, {}).get('deposit', 0)
        }
        for mod, func, name in result['data']
    })

    return result


# 登出PR6后台
def logout_api(cf):
    '''
    登出PR6后台, 並解析各錯誤內容進行動作.
    Args:
        cf: 機器人設置內容
    Return:
        <str>: 程式執行發生異常, 需要中止機器人並跳出錯誤視窗
        False: 連線發生異常, 需要中止機器人, 但是不需跳出錯誤視窗
        "登入成功": 登入已經完成, 可以準備執行任務
    '''
    source = {
        'mod': 'sys',
        'app': 'robot',
        'func': 'logout',
        'version': cf['api_version'],
    }
    result = backend_api(
        act='登出PR6后台',
        url=cf['api'],
        item=source,
        retry_times=1,
        cf=cf
    )
    logger.info(f'PR6后台[登出PR6后台]>>>解析: {result}')


# PR6后台
def login_api(cf):
    '''
    PR6后台, 並解析各錯誤內容進行動作.
    Args:
        cf: 機器人設置內容
    Return:
        <str>: 程式執行發生異常, 需要中止機器人並跳出錯誤視窗
        False: 連線發生異常, 需要中止機器人, 但是不需跳出錯誤視窗
        "登入成功": 登入已經完成, 可以準備執行任務
    '''
    source = {
        'mod': 'sys',
        'app': 'robot',
        'func': 'login',
        'version': cf['api_version'],
        'username': cf['robot_act'],
        'pasw': cf['robot_pw'],
        'secret': cf['bk_otp']
    }
    result = backend_api(
        act='登入PR6后台',
        url=cf['api'],
        item=source,
        retry_times=1,
        cf=cf
    )
    logger.info(f'PR6后台[登入PR6后台]>>>解析: {result}')
    # 連線異常時, 設定連線為false, 回傳為false
    if '重试超过上限' in result["error"]:
        cf['connect'] = False
        return result["error"]

    # 程式異常時, 回傳錯誤訊息, 用於跳出視窗
    elif '密文解析错误' in result.get('error'):
        return '密文解析错误，请检查机器人密钥与PR6后台密钥是否匹配'
    elif '504错误' in result.get('error'):
        return '与PR6后台连线异常(504)，请与开发团队联系'
    elif '404错误' in result.get('error'):
        return '请确认PR6后台网址是否有误或者網路是否正常後，再次点选按钮启动机器人'
    elif not result['result']:
        return result['error']

    cf['token'] = result["data"]["token"]
    return '登入成功'


# 延長liveTime
def keep_connect(cf):
    '''
    延長liveTime時間.
    Args:
        cf: 機器人設置內容
    Return:
        <str>: 程式執行發生異常, 需要中止機器人並跳出錯誤視窗
        False: 連線發生異常, 需要中止機器人, 但是不需跳出錯誤視窗
        "登入成功": 登入已經完成, 可以準備執行任務
    '''
    source = {
        'mod': 'sys',
        'app': 'login',
        'func': 'connect',
        'version': cf['api_version'],
    }
    result = backend_api(
        act='保持登入狀態',
        url=cf['api'],
        item=source,
        retry_times=1,
        cf=cf
    )
    logger.info(f'PR6后台[保持登入狀態]>>>解析: {result}')


# 索取任務
def get_task(mod, cf):
    '''
    索取任務, 並解析各錯誤內容進行動作.
    Args:
        cf: 機器人設置內容
    Return:
        True: PR6后台返回data為空, 代表目前沒有需要執行的任務
        False: 連線發生異常, 需要中止機器人, 但是不需跳出錯誤視窗
               (當自動重連, 會嘗試百度網站, 可連線時會再次進入迴圈)
        "登入成功": 登入已經完成, 可以準備執行任務
        "其他字串": 程式執行發生異常, 需要中止機器人並跳出錯誤視窗
    '''
    platform = getattr(platforms, cf['platform'].lower())
    func = getattr(platform.mission, mod['func'], None)
    source = {
        'mod': mod['mod'],
        'app': 'robot',
        'func': 'task',
        'version': cf['api_version'],
        'chkbbin': int(mod['monitor'] > 0),
        'chkpoint': int(mod['deposit'] > 0),
    }
    if func is None:
        source['liveTime'] = 60
    else:
        source['liveTime'] = getattr(func.Meta, 'liveTime', 60)

    item = backend_api(
        act='索取任務',
        url=cf['api'],
        item=source,
        retry_times=cf['retry_times'],
        cf=cf
    )
    logger.info(f'PR6后台[索取任務]>>>解析: {item}')

    if '重试超过上限' in item.get('error'):
        cf['connect'] = False
        return item['error']
    elif '密文解析错误' in item.get('error'):
        return '密文解析错误，请检查机器人密钥与PR6后台密钥是否匹配'
    elif '504错误' in item.get('error'):
        return '与PR6后台连线异常(504)，请与开发团队联系'
    elif '404错误' in item.get('error'):
        return '请确认PR6后台网址是否有误或者網路是否正常後，再次点选按钮启动机器人'
    elif item.get('result') is False:
        return item["error"]

    # data 為 空陣列 或 空字典, 代表沒資料要處理
    if item['data'] in ([], {}):
        return True

    if item["data"]["Action"] not in ('chkbbin', 'chkpoint'):
        return f'PR6后台回应栏位异常, 任务目标指向错误, 请与开发团队联系'

    return item


# 回報任務
def return_task(mod, action, result, cf):
    '''
    回報任務, 並解析各錯誤內容進行動作.
    Args:
        result: 任務執行結果
        cf: 機器人設置內容
    Return:
        False: 連線發生異常, 需要中止機器人, 但是不需跳出錯誤視窗
               (當自動重連, 會嘗試百度網站, 可連線時會再次進入迴圈)
        "登入成功": 登入已經完成, 可以準備執行任務
        "其他字串": 程式執行發生異常, 需要中止機器人並跳出錯誤視窗
    '''
    platform = getattr(platforms, cf['platform'].lower())
    func = getattr(platform.mission, mod['func'], None)
    result = {
        'mod': mod['mod'],
        'app': 'robot',
        'func': 'chkres',
        'version': cf['api_version'],
        'data': dict(result, **{'Action': action})
    }
    if func is None:
        result['liveTime'] = 60
    else:
        result['liveTime'] = getattr(func.Meta, 'liveTime', 60)

    end = backend_api(
        act='回報任務',
        url=cf['api'],
        item=result,
        retry_times=cf['retry_times'],
        cf=cf
    )
    logger.info(f'PR6后台[回報任務]>>>解析: {end}')

    if '重试超过上限' in end.get("error", ''):
        cf['connect'] = False
        return end['error']
    elif end.get('error').endswith('重试超过上限'):
        cf['connect'] = False
        return False
    elif end.get('error') == '密文解析错误':
        return '密文解析错误，请检查机器人密钥与PR6后台密钥是否匹配'
    elif '504错误' in end.get('error'):
        return '与PR6后台连线异常(504)，请与开发团队联系'
    elif '404错误' in end.get('error'):
        return '请确认PR6后台网址是否有误或者網路是否正常後，再次点选按钮启动机器人'
    elif end.get('result') is False:
        return end['error']

    return True


# 訂單執行進度回報
def return_schedule(cf, mod_key, **kwargs):
    '''
    訂單執行進度回報
    Args:
    Returns:
    '''
    source = {
        'mod': mod_key,
        'app': 'robot',
        'func': 'status',
        'version': cf['api_version'],
        'liveTime': 60,
        'data': kwargs
    }
    result = backend_api(
        act='訂單執行進度回報',
        url=cf['api'],
        item=source,
        retry_times=1,
        cf=cf
    )
    logger.info(f'PR6后台[訂單執行進度回報]>>>解析: {result}')

# 連線檢查使用
def canConnect(cf):
    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
        try:
            s.connect((cf['test_dns'], 80))
            return s.getsockname()[0]
        except Exception as e:
            logger.info(f'檢查連線異常: {e.__class__.__name__}, {e}')
            if '检查连线异常' not in cf['error_msg']:
                cf['error_msg'].append('检查连线异常')
            return ''

def myip():
    try:
        resp = requests.get('https://api.ipify.org', timeout=1)
        if resp.status_code != 200:
            return ''
        return f'({resp.text})'
    except Exception as e:
        logger.debug(f'{e.__class__.__name__} {e}')
        return ''
