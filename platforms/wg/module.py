import hashlib
import random
import datetime
import json
import logging
from sys import platform
import requests
from platforms.config import CODE_DICT as config
from .utils import (
    log_info,
    default_headers,
    catch_exception,
    NullError
)
import hashlib

logger = logging.getLogger('robot')


session = requests.Session()
session.login = False


@log_info
@catch_exception
def login(cf: dict, url: str, acc: str, pw: str, otp: str, timeout: tuple=(60), endpoints: str='auth/oauth/token', **kwargs) -> dict:
    '''WG 登入'''
    #清空cookies
    session.login = False
    session.cookies.clear()
    # 檢查帳密
    if not acc.isalnum():
        return {
            'IsSuccess': False,
            'ErrorCode': config['ACC_CODE']['code'],
            'ErrorMessage': config['ACC_CODE']['msg'],
        }
    if not pw.isalnum():
        return {
            'IsSuccess': False,
            'ErrorCode': config['ACC_CODE']['code'],
            'ErrorMessage': config['ACC_CODE']['msg'],
        }

    #定義變數
    source = {
        'code': 'ewp5',
        'gaCode': otp,
        'grant_type': 'password',
        'password': hashlib.sha256(pw.encode()).hexdigest(),
        'randomStr': ''.join([str(random.choice(range(0,10))) for i in range(4)])  + str(int(datetime.datetime.now().timestamp()*1000)),
        'scope': 'server',
        'username': acc,   
    }
    # 登入
    default_headers.update({
        'authorization': 'Basic cGlnOnBpZw==',
        'content-type': 'application/json',
        'language': 'zh',
    })
    resp = session.post(url + endpoints, data=json.dumps(source), headers=default_headers, timeout=30+timeout, verify=False)
    resp.encoding = 'utf-8-sig'
    rmk ='''
    {
        'access_token': '54ca1ea0-3abb-4d04-a5ec-c1c6296507af',
        'token_type': 'bearer',
        'refresh_token': 'd44036b4-ac0e-4de6-a423-acd1cf3ae6e9',
        'expires_in': 44399,
        'scope': 'server',
        'user_status': 1,
        'backInfos': [{
            'unitId': '128',
            'unitName': '银河',
            'loginBackType': 3,
            'belongType': 1,
            'belongId': 1531168034517381122,
            'belongName': 'ERA',
            'blocId': 49,
            'blocName': 'HK集团',
            'proxyMode': 0,
            'languageCode': 'zh',
            'timeZone': 'UTC +08:00',
            'hourOffset': 8,
            'minuteOffset': 0,
            'secondOffset': 0,
            'roleIds': [1548102979552616450],
            'dockingTag': False,
            'unitStatus': 0,
            'currencyCode': 'CNY',
            'currencySign': '￥',
            'gameRate': '1.00',
            'canLogin': True,
            'childNodes': [],
            'openClub': True
        }],
        'blocIds': [],
        'companyIds': [],
        'pwStatus': 1,
        'currentBackType': 3,
        'frozen_status': 0,
        'license': 'made by universe',
        'roleIds': [1548102979552616450],
        'user_id': 1548103101455867905,
        'blocId': 49,
        'unitId': '128',
        'technologyTag': False,
        'site_codes': ['128'],
        'username': 'twwg1',
        'blocName': 'HK集团'
    }
    '''
    try:
        content = json.loads(resp.text)
    except:
        return {
            'IsSuccess': False,
            'ErrorCode': config.JSON_ERROR_CODE.code,
            'ErrorMessage': config.JSON_ERROR_CODE.msg.format(platform=cf.platform),
            'RawStatusCode': resp.status_code,
            'RawContent': resp.content
        }   
    if content.get('access_token'):
        session.token = content['access_token']
        session.sitecode = content['site_codes'][0]
        default_headers['authorization'] = f'bearer {session.token}'
        default_headers['sitecode'] = session.sitecode
        session.login = True
        session.url = url
        session.acc = acc
        session.pw = pw
        logger.info(f'cookies: {session.cookies.get_dict()}')
        return {
            'IsSuccess' : True,
            'ErrorCode': config.SUCCESS_CODE.code,
            'ErrorMessage': config.SUCCESS_CODE.msg,
            'RawStatusCode': resp.status_code,
            'RawContent': resp.content
        }
    else:
        return {
            'IsSuccess' : False,
            'ErrorCode': config.SIGN_OUT_CODE.code,
            'ErrorMessage': f"【{cf.platform}】回傳內容：{content.get('msg') if content.get('msg') else content.get('data')}",
            'RawStatusCode': resp.status_code,
            'RawContent': resp.content
        }


@log_info
@catch_exception
def activation_token(cf: dict, url: str, timeout: tuple=(60), endpoints: str='api/admin/check/heartbeat', **kwargs) -> dict:
    '''WG 保持連線'''
    resp = session.get(url+endpoints, headers=default_headers, timeout=30+timeout, verify=False)
    resp.encoding = 'utf-8-sig'
    rmk = '''
    {
        'code': 0,
        'msg': 'success',
        'data': True,
        'timestamp': 1658722349045,
        'success': True
    }
    '''
    if resp.status_code >= 500 and resp.status_code < 600:
        return  {
            'IsSuccess' : False,
            'ErrorCode': config.CONNECTION_CODE.code,
            'ErrorMessage': f"【{cf.platform}】回傳內容：{f'status_code:{resp.status_code}'}",
            'RawStatusCode': resp.status_code,
            'RawContent': resp.content
        }
    elif resp.status_code != 200:
        return {
                'IsSuccess' : False,
                'ErrorCode': config.HTML_STATUS_CODE.code,
                'ErrorMessage': config.HTML_STATUS_CODE.msg.format(platform=cf.platform, status_code=resp.status_code),
                'RawStatusCode': resp.status_code,
                'RawContent': resp.content
        }
    try:
        content = json.loads(resp.text)
        if content.get('code') == 401:  # 被登出
            return {
                'IsSuccess' : False,
                'ErrorCode': config.SIGN_OUT_CODE.code,
                'ErrorMessage': config.SIGN_OUT_CODE.msg.format(platform=cf.platform),
                'RawStatusCode': resp.status_code,
                'RawContent': resp.content
            }
    except:
        return {
            'IsSuccess': False,
            'ErrorCode': config.JSON_ERROR_CODE.code,
            'ErrorMessage': config.JSON_ERROR_CODE.msg.format(platform=cf.platform),
            'RawStatusCode': resp.status_code,
            'RawContent': resp.content
        }                
    if content.get('success', False):
        return {
            'IsSuccess' : True,
            'ErrorCode': config.SUCCESS_CODE.code,
            'ErrorMessage': config.SUCCESS_CODE.msg,
            'RawStatusCode': resp.status_code,
            'RawContent': resp.content
        }
    else:
        return {
            'IsSuccess' : False,
            'ErrorCode': config.SIGN_OUT_CODE.code,
            'ErrorMessage': f"【{cf.platform}】回傳內容：{content.get('msg')}",
            'RawStatusCode': resp.status_code,
            'RawContent': resp.content
        }        


@log_info
@catch_exception
def member_userMember_allUser(cf: dict, url: str, params:dict, timeout: tuple=(60), endpoints: str='api/member/userMember/allUser', selectTimeKey=0, **kwargs) -> dict:
    '''WG 查詢會員層級 | 登錄會員【会员管理 > 所有会员】'''
    params['selectTimeKey'] = selectTimeKey
    params['current'] = 1
    params['size'] = 20
    resp = session.get(url+endpoints, params=params, headers=default_headers, timeout=30+timeout, verify=False)
    resp.encoding = 'utf-8-sig'
    rmk = '''
    {'code': 0,
    'msg': 'success',
    'data': {
        'result': 0,
        'data': [{
            'useridx': 22520721,
            'username': 'woyaoyinqian888',
            'realname': '',
            'nickname': '齐泰',
            'regpkgid': 999999,
            'regpkgidName': '后台添加',
            'parentPlatformId': 'bdzy1',
            'registerTime': 1637561327,
            'vipLevel': 1,
            'vipLevelName': 'VIP1',
            'memberLevel': 1,
            'memberLevelName': '新会员层级',
            'totalBalance': 0.0,
            'totalDeposit': 0.0,
            'totalWithdraw': 0.0,
            'depositWithdrawDiff': 0.0,
            'loginIp': '登录IP未知',
            'loginOsType': 0,
            'loginTime': '登录时间未知',
            'loginArea': '/-',
            'isProagent': False,
            'isProagentBlocked': False,
            'verifyType': 1,
            'onlineStatus': 2,
            'iconType': None,
            'hasNoLogin': True
            }],
    'total': 5},
    'timestamp': 1659500882061,
    'success': True}
    '''
    try:
        content = json.loads(resp.text)
        if content.get('code') == 401:  # 被登出
            return {
                'IsSuccess' : False,
                'ErrorCode': config.SIGN_OUT_CODE.code,
                'ErrorMessage': config.SIGN_OUT_CODE.msg.format(platform=cf.platform),
                'RawStatusCode': resp.status_code,
                'RawContent': resp.content
            }
    except:
        return {
            'IsSuccess': False,
            'ErrorCode': config.JSON_ERROR_CODE.code,
            'ErrorMessage': config.JSON_ERROR_CODE.msg.format(platform=cf.platform),
            'RawStatusCode': resp.status_code,
            'RawContent': resp.content
        }    
    if content.get('success', False):
        return {
            'IsSuccess': True,
            'ErrorCode': config.SUCCESS_CODE.code,
            'ErrorMessage': config.SUCCESS_CODE.msg,
            'Data': content,
            'RawStatusCode': resp.status_code,
            'RawContent': resp.content
        }
    else:
        return {
            'IsSuccess': False,
            'ErrorCode': config.HTML_STATUS_CODE.code,
            'ErrorMessage': f"【{cf.platform}】回傳內容：{content.get('msg')}",
            'RawStatusCode': resp.status_code,
            'RawContent': resp.content
            }        


@log_info
@catch_exception
def user_account_info(cf: dict, url: str, params:dict, timeout: tuple=(60), endpoints: str='api/member/userMember/user_account/info', **kwargs) -> dict:
    '''WG 查詢會員詳情【会员管理 > 所有会员 > 详情 > 会员信息】'''
    resp = session.get(url+endpoints, params=params, headers=default_headers, timeout=30+timeout, verify=False)
    resp.encoding = 'utf-8-sig'
    rmk = '''
        {'idx': 256268,
        'platform_id': '888zxc',
        'nickname': '涵荷姑娘',
        'parent_platform_id': 'systemuser',
        'vip_level': 4,
        'member_level': 14,
        'order_count': 172,
        'withdraw_count': 14,
        'total_deposit': 100794.69,
        'total_withdraw': 107550.0,
        'game_gold': 0.55,
        'bank_balance': 0.0,
        'total_locked': 0.0,
        'pkg_id': 0,
        'record_ip': '127.0.0.1',
        'register_area': '未知',
        'register_time': 1635912385,
        'login_os_type': 3,
        'os_type': None,
        'online_status': 0,
        'last_logout_time': 1663903105,
        'remark': '',
        'realname': '刘后平',
        'profit': -32658.43,
        'register_device_id': '',
        'user_type': 2,
        'mobile_phone': '',
        'bind_awards': '',
        'user_status': 1,
        'black_status': None,
        'last_login_time': 1663903105,
        'login_ip': '183.137.2.36',
        'login_id': 'fcd00e90-fc2f-4a73-9d29-b4b80dbc7ca6',
        'user_question': '',
        'gender': 1,
        'birthday': 0,
        'totalBet': 414.6,
        'validBet': 414.59,
        'allValidBet': 916411.2,
        'username': '78404905',
        'portrait_id': 'https://x9xko9-128.oss-accelerate.aliyuncs.com/siteadmin/upload/img/1557005346891964418.png',
        'login_pkg_id': 0,
        'max_deposit_amount': 84130.0,
        'level_locked': 1,
        'platform_type': '',
        'promote_id': '78404905',
        'promote_last_level_id': 0,
        'commission_status': 1,
        'account_type': 2,
        'last_login_area': 'China/Zhejiang-Lipu',
        'mobile_phone_time': 1658745949,
        'promote_last_level_parmas': '',
        'jpush_id': '',
        'qrcode_id': '',
        'regpkgid': 999999,
        'parent_platform_useridx': '97822087',
        'wechat_id': '',
        'agent_mode': 12345,
        'parent_proagent_id': 0,
        'regtype': '0',
        'total_money': 0.0,
        'pkg_name': '后台添加',
        'bank_profit': 0,
        'user_question_text': '',
        'user_sms_pass': False,
        'is_proagent': False,
        'is_proagent_blocked': False,
        'verify_type': 1,
        'areaCode': '',
        'user_remark': None,
        'active_reward': 1199.0,
        'task_reward': 45.0,
        'achievement_reward': 132.0,
        'user_tag': '',
        'not_login_day': 0,
        'vip_name': 'VIP4',
        'level_name': '白金会员',
        'login_pkg_name': None,
        'login_en_name': None,
        'login_pkg_type': None,
        'reg_type': None,
        'black_idx': None,
        'tag': [],
        'returngold_setting_name': '实时返水3.0%!无(MISSING)上限',
        'status_remark': '',
        'mode_name': '无限极差',
        'thirdAccount': '-',
        'isGoogleAuth': False,
        'email': '',
        'whatsapp': '',
        'facebook': '',
        'telegram': '',
        'facebookDisableEdit': False,
        'cpf': ''}
    '''
    try:
        content = json.loads(resp.text)
        if content.get('code') == 401:  # 被登出
            return {
                'IsSuccess' : False,
                'ErrorCode': config.SIGN_OUT_CODE.code,
                'ErrorMessage': config.SIGN_OUT_CODE.msg.format(platform=cf.platform),
                'RawStatusCode': resp.status_code,
                'RawContent': resp.content
            }
    except:
        return {
            'IsSuccess': False,
            'ErrorCode': config.JSON_ERROR_CODE.code,
            'ErrorMessage': config.JSON_ERROR_CODE.msg.format(platform=cf.platform),
            'RawStatusCode': resp.status_code,
            'RawContent': resp.content
        }    
    if content.get('success', False):
        return {
            'IsSuccess': True,
            'ErrorCode': config.SUCCESS_CODE.code,
            'ErrorMessage': config.SUCCESS_CODE.msg,
            'Data': content,
            'RawStatusCode': resp.status_code,
            'RawContent': resp.content
        }
    else:
        return {
            'IsSuccess': False,
            'ErrorCode': config.HTML_STATUS_CODE.code,
            'ErrorMessage': f"【{cf.platform}】回傳內容：{content.get('msg')}",
            'RawStatusCode': resp.status_code,
            'RawContent': resp.content
            }        


@log_info
@catch_exception
def active_reward_add(cf, url: str, data: dict, timeout: tuple=(60), endpoints: str='api/active/active/reward/add', **kwargs) -> dict:
    '''WG 進行充值【优惠中心 > 活动中心 > 活动列表（活动名称→派发奖励）】'''
    data['category'] = 4
    data['passwd'] = hashlib.sha256(session.pw.encode()).hexdigest()
    data['periodDay'] = 0
    data['receiveType'] = 1
    data['rewardType'] = 1
    try:
        resp = session.post(url+endpoints, data=json.dumps(data), headers=default_headers, timeout=30+timeout, verify=False)
        resp.encoding = 'utf-8-sig'
        rmk = '''
        {
            'code': 0,
            'msg': 'success',
            'data': None,
            'timestamp': 1658481842897,
            'success': True
        }
        '''
        if resp.status_code != 200:
            return {
                    'IsSuccess' : False,
                    'ErrorCode': config.HTML_STATUS_CODE.code,
                    'ErrorMessage': f'{resp.status_code}异常, 请确认{cf.platform}确认是否到帐',
                    'RawStatusCode': resp.status_code,
                    'RawContent': resp.content
            }
        try:
            content = json.loads(resp.text)
            if content.get('code') == 401:  # 被登出
                return {
                    'IsSuccess' : False,
                    'ErrorCode': config.SIGN_OUT_CODE.code,
                    'ErrorMessage': config.SIGN_OUT_CODE.msg.format(platform=cf.platform),
                    'RawStatusCode': resp.status_code,
                    'RawContent': resp.content
                }
        except:
            return {
                'IsSuccess': False,
                'ErrorCode': config.JSON_ERROR_CODE.code,
                'ErrorMessage': config.JSON_ERROR_CODE.msg.format(platform=cf.platform),
                'RawStatusCode': resp.status_code,
                'RawContent': resp.content
            }    
        if content.get('success', False):
            return {
                'IsSuccess': True,
                'ErrorCode': config.SUCCESS_CODE.code,
                'ErrorMessage': config.SUCCESS_CODE.msg,
                'Data': content,
                'RawStatusCode': resp.status_code,
                'RawContent': resp.content
            }
        else:
            return {
                    'IsSuccess' : False,
                    'ErrorCode': config.DEPOSIT_FAIL.code,
                    'ErrorMessage': config.DEPOSIT_FAIL.msg.format(platform=cf.platform, msg=content.get('msg', '')),
                    'RawStatusCode': resp.status_code,
                    'RawContent': resp.content
            }
    except (requests.exceptions.ConnectionError,requests.exceptions.Timeout):
        return  {
            'IsSuccess': False,
            'ErrorCode': config.IGNORE_CODE.code,
            'ErrorMessage': config.IGNORE_CODE.msg,
            # 'RawStatusCode': resp.status_code,
            # 'RawContent': resp.content
        }


@log_info
@catch_exception
def active_reward_check(cf, url: str, data: dict, timeout: tuple=(60), endpoints: str='api/active/active/reward/check', **kwargs) -> dict:
    '''WG 通過充值【优惠中心 > 活动中心 > 派发审核】'''
    passwd_hash=hashlib.sha256()
    if cf.get('ask_api','') == 'chkpoint.php':
        passwd=cf.get('pw','')
    else:
        passwd=cf.get('backend_password','')
    passwd_hash.update(passwd.encode("utf-8"))
    data.update({
        'checkStatus': 1,
        'passwd' : str(passwd_hash.hexdigest())
    })  
    resp = session.post(url+endpoints, data=json.dumps(data), headers=default_headers, timeout=30+timeout, verify=False)
    resp.encoding = 'utf-8-sig'
    rmk = '''
    {
        'code': 0,
        'msg': 'success',
        'data': {'check_status': None, 'reward_remark': None},
        'timestamp': 1658481332901,
        'success': True
    }
     '''
    try:
        content = json.loads(resp.text)
        if content.get('code') == 401:  # 被登出
            return {
                'IsSuccess' : False,
                'ErrorCode': config.SIGN_OUT_CODE.code,
                'ErrorMessage': config.SIGN_OUT_CODE.msg.format(platform=cf.platform),
                'RawStatusCode': resp.status_code,
                'RawContent': resp.content
            }
    except:
        return {
            'IsSuccess': False,
            'ErrorCode': config.JSON_ERROR_CODE.code,
            'ErrorMessage': config.JSON_ERROR_CODE.msg.format(platform=cf.platform),
            'RawStatusCode': resp.status_code,
            'RawContent': resp.content
        } 
    if content.get('success', False):
        return {
                'IsSuccess': True,
                'ErrorCode': config.SUCCESS_CODE.code,
                'ErrorMessage': config.SUCCESS_CODE.msg,
                'Data': content,
                'RawStatusCode': resp.status_code,
                'RawContent': resp.content
                }
    else:
        return {
            'IsSuccess': False,
            'ErrorCode': config.HTML_STATUS_CODE.code,
            'ErrorMessage': f"【{cf.platform}】回傳內容：{content.get('msg')}",
            'RawStatusCode': resp.status_code,
            'RawContent': resp.content
            }


@log_info
@catch_exception
def api_active_page(cf, url: str, params: dict={
        'status': -1,
        'current': 1,
        'size': 1000,
    }, timeout: tuple=(60), endpoints: str='api/active/active/page', **kwargs) -> dict:
    '''WG 充值ID_活動名稱對照表【优惠中心 > 活动中心 > 活动列表】'''
    resp = session.get(url+endpoints, params=params, headers=default_headers, timeout=30+timeout, verify=False)
    resp.encoding = 'utf-8-sig'
    rmk = '''
    {
        'code': 0,
        'msg': 'success',
        'data': {'records': [{'id': 36,
            'name': '系统升级公告',
            'category': 1,
            'categoryText': '综合活动',
            'startTime': 1658419200,
            'endTime': 1893427199,
            'status': 1,
            'activeSwitch': 1,
            'weigh': 49,
            'ruleText': None,
            'audit': 1,
            'giveType': 1,
            'type': 12,
            'typeText': '自定义',
            'userText': '新会员层级,过渡层,15体验金,套利刷水层级,审核会员,停用会员层级,存款会员,三方测试,优质会员,老会员',
            'oneClick': None,
            'userLevel': '1,2,11,12,13,10001,10002,10003,10004,10005',
            'isPushMsg': 0,
            'isShowTime': 2,
            'isClientShow': None,
            'bindType': None,
            'startShowTime': 0,
            'endShowTime': 0,
            'createUser': 'djlucky',
            'pubUser': None,
            'sortNumber': None,
            'bet': None,
            'agentMode': '',
            'updateTime': 1658480884,
            'isDefaultLang': None,
            'content': None,
            'imgId': None}],    
        'current': 1,
        'size': 1000,
        'pages': 1,
        'total': 32},
        'timestamp': 1658731113742,
        'success': True
    }     
    '''
    try:
        content = json.loads(resp.text)
        if content.get('code') == 401:  # 被登出
            return {
                'IsSuccess' : False,
                'ErrorCode': config.SIGN_OUT_CODE.code,
                'ErrorMessage': config.SIGN_OUT_CODE.msg.format(platform=cf.platform),
                'RawStatusCode': resp.status_code,
                'RawContent': resp.content
            }
    except:
        return {
            'IsSuccess': False,
            'ErrorCode': config.JSON_ERROR_CODE.code,
            'ErrorMessage': config.JSON_ERROR_CODE.msg.format(platform=cf.platform),
            'RawStatusCode': resp.status_code,
            'RawContent': resp.content
        } 
    if content.get('success', False):
        return {
                'IsSuccess': True,
                'ErrorCode': config.SUCCESS_CODE.code,
                'ErrorMessage': config.SUCCESS_CODE.msg,
                'Data': content,
                'RawStatusCode': resp.status_code,
                'RawContent': resp.content
                }
    else:
        return {
            'IsSuccess': False,
            'ErrorCode': config.HTML_STATUS_CODE.code,
            'ErrorMessage': f"【{cf.platform}】回傳內容：{content.get('msg')}",
            'RawStatusCode': resp.status_code,
            'RawContent': resp.content
            }


@log_info
@catch_exception
def active_reward_records(cf, url: str, data: dict, timeout: tuple=(60), endpoints: str='api/active/active/reward/records', **kwargs) -> dict:
    '''WG 確認充值狀態【优惠中心 > 活动中心 > 派发审核】'''
    startTime = (datetime.datetime.now() - datetime.timedelta(days=1)).strftime('%Y-%m-%d') + ' 00:00:00'  #★取1天內充值單子
    startTime = int(datetime.datetime.strptime(startTime, '%Y-%m-%d %H:%M:%S').timestamp())
    endTime = int(datetime.datetime.now().timestamp())
    data.update({
        'timeType': 1,
        'type': 1,
        'status': -1,
        'startTime': startTime,
        'endTime': endTime,    
        'current': 1,
        'size': 1000, 
        'page': 1,
        'requestType': 0,   
    })    
    resp = session.post(url+endpoints, data=json.dumps(data), headers=default_headers, timeout=30+timeout, verify=False)
    resp.encoding = 'utf-8-sig'
    rmk = '''
    {
        'code': 0,
        'msg': 'success',
        'data': {'page': 1,
        'total': 13,
        'record': [{'activeName': '幸运老虎机',
            'userName': 'chongzhi123',
            'amount': 0.02,
            'audit': 1,
            'rewardType': 1,
            'periodDay': 0,
            'receiveType': 1,
            'orderId': '6109330000274752',
            'checkStatus': 2,
            'rewardRemark': '奖励说明',
            'createUser': 'twwg1',
            'checkUser': 'admin5',
            'createTime': 1658481842,
            'checkTime': 1658665246,
            'frontRemark': '前台备注',
            'backRemark': '后台备注'}]},
        'timestamp': 1658731407239,
        'success': True
    }    
    '''    
    try:
        content = json.loads(resp.text)
        if content.get('code') == 401:  # 被登出
            return {
                'IsSuccess' : False,
                'ErrorCode': config.SIGN_OUT_CODE.code,
                'ErrorMessage': config.SIGN_OUT_CODE.msg.format(platform=cf.platform),
                'RawStatusCode': resp.status_code,
                'RawContent': resp.content
            }
    except:
        return {
            'IsSuccess': False,
            'ErrorCode': config.JSON_ERROR_CODE.code,
            'ErrorMessage': config.JSON_ERROR_CODE.msg.format(platform=cf.platform),
            'RawStatusCode': resp.status_code,
            'RawContent': resp.content
        }   
    if content.get('success', False):
        return {
                'IsSuccess': True,
                'ErrorCode': config.SUCCESS_CODE.code,
                'ErrorMessage': config.SUCCESS_CODE.msg,
                'Data': content,
                'RawStatusCode': resp.status_code,
                'RawContent': resp.content
                }
    else:
        return {
            'IsSuccess': False,
            'ErrorCode': config.HTML_STATUS_CODE.code,
            'ErrorMessage': f"【{cf.platform}】回傳內容：{content.get('msg')}",
            'RawStatusCode': resp.status_code,
            'RawContent': resp.content
            }


@log_info
@catch_exception
def active_reward_batchDetail(cf, url: str, data: dict, timeout: tuple=(60), endpoints: str='api/active/active/reward/batchDetail', **kwargs) -> dict:
    '''WG 確認充值狀態【优惠中心 > 活动中心 > 派发审核 > 活动名称 > 查看详情 >会员账号】'''
    data.update({
    "checkStatus": -1,
    "receiveStatus": -1,
    "current": 1,
    "size": 20,
    "page": 1
    })  
    resp = session.post(url+endpoints, data=json.dumps(data), headers=default_headers, timeout=30+timeout, verify=False)
    resp.encoding = 'utf-8-sig'
    rmk = '''
    {
    "code": 0,
    "msg": "success",
    "data": {
        "page": 1,
        "total": 88,
        "template": 1,
        "record": [
            {
                "id": 392124,
                "activeId": 6,
                "userIdx": 30569144,
                "userName": "chongzhi123",
                "orderId": "6110170139628235",
                "orderNo": "6110170139632946",
                "amount": 0.35,
                "audit": 1,
                "frontRemark": "1",
                "backRemark": "1",
                "rewardType": 1,
                "receiveType": 1,
                "periodDay": 0,
                "checkStatus": 1,
                "receiveStatus": 3,
                "createUser": "twwg3",
                "createTime": 1665758244,
                "checkUser": "twwg3",
                "checkTime": 1665758250,
                "receiveTime": 1665758250,
                "requestType": 0,
                "requestRemark": ""
            },
                    ],
        "questions": null
    },
    "timestamp": 1665758690695,
    "success": true
    }  
    '''    
    try:
        content = json.loads(resp.text)
        if content.get('code') == 401:  # 被登出
            return {
                'IsSuccess' : False,
                'ErrorCode': config.SIGN_OUT_CODE.code,
                'ErrorMessage': config.SIGN_OUT_CODE.msg.format(platform=cf.platform),
                'RawStatusCode': resp.status_code,
                'RawContent': resp.content
            }
    except:
        return {
            'IsSuccess': False,
            'ErrorCode': config.JSON_ERROR_CODE.code,
            'ErrorMessage': config.JSON_ERROR_CODE.msg.format(platform=cf.platform),
            'RawStatusCode': resp.status_code,
            'RawContent': resp.content
        }   
    if content.get('success', False):
        return {
                'IsSuccess': True,
                'ErrorCode': config.SUCCESS_CODE.code,
                'ErrorMessage': config.SUCCESS_CODE.msg,
                'Data': content,
                'RawStatusCode': resp.status_code,
                'RawContent': resp.content
                }
    else:
        return {
            'IsSuccess': False,
            'ErrorCode': config.HTML_STATUS_CODE.code,
            'ErrorMessage': f"【{cf.platform}】回傳內容：{content.get('msg')}",
            'RawStatusCode': resp.status_code,
            'RawContent': resp.content
            }


@log_info
@catch_exception
def playerBankDetails_all(cf, url: str, params: dict, timeout: tuple=(60), endpoints: str='playerBankDetails/all', **kwargs) -> dict:
    '''WG 查詢綁定銀行卡【会员管理 > 会员列表 > 会员资料 > 检测银行完整资讯】'''
    resp = session.get(url+endpoints, params=params, headers=default_headers, timeout=30+timeout, verify=False)
    # {"draw":1,"recordsFiltered":1,"recordsTotal":1,"data":[{"id":"129305","bank_name":"\u4e2d\u56fd\u5de5\u5546\u94f6\u884c","branch_name":"\u5317\u4eac\u652f\u884c","account_name":"\u5927\u9646","account_number":"6222333344445555666","province_id":"\u5317\u4eac\u5e02","city_id":"\u5317\u4eac\u5e02","default_bank":"0","allowed_bank_id":"1","image_url":"https:\/\/rtw002.com\/bo\/bank\/bank_icon.png","image_name":"bank_icon.png","icon":"bank_icon.png"}]}
    if resp.status_code != 200:
        raise NullError(f'status_code={resp.status_code}')
    content = json.loads(resp.text)
    if 'data' not in content:
        return {
            'IsSuccess': False,
            'ErrorCode': config.JSON_ERROR_CODE.code,
            'ErrorMessage': config.JSON_ERROR_CODE.msg.format(platform=cf.platform),
            'Data': resp.text,
            'RawStatusCode': resp.status_code,
            'RawContent': resp.content
        }
    return {
            'IsSuccess': True,
            'ErrorCode': config.SUCCESS_CODE.code,
            'ErrorMessage': config.SUCCESS_CODE.msg,
            'Data': content,
            'RawStatusCode': resp.status_code,
            'RawContent': resp.content
        }


@log_info
@catch_exception
def ipEnquiryRecords(cf, url: str, params: dict, timeout: tuple=(60), endpoints: str='ipAddressMonitoring/ipEnquiryRecords', **kwargs) -> dict:
    '''WG 查詢IP【风控管理 > IP查询】'''
    resp = session.get(url+endpoints, params=params, headers=default_headers, timeout=30+timeout, verify=False)
    # {"status":"success","message":"get_success","errorcode":"","showalert":"false","data":[{"player_id":"170244301","time":"1624263038","type":"login","ip":"113.26.43.243","location":"\u5d14\u5bb6\u5d16 ,\u5c71\u897f ,\u4e2d\u56fd ","domain":"www.2277442.com","browser":"Chrome","device":"h5","os":"Android x32","username":"yyx8324056","affiliate":"-","affiliate_id":"0","register_ip":"223.104.197.69"},{"player_id":"170244301","time":"1624260916","type":"login","ip":"113.26.43.243","location":"\u5d14\u5bb6\u5d16 ,\u5c71\u897f ,\u4e2d\u56fd ","domain":"www.2266442.com","browser":"Chrome","device":"h5","os":"Android x32","username":"yyx8324056","affiliate":"-","affiliate_id":"0","register_ip":"223.104.197.69"},{"player_id":"170244301","time":"1624260791","type":"login","ip":"113.26.43.243","location":"\u5d14\u5bb6\u5d16 ,\u5c71\u897f ,\u4e2d\u56fd ","domain":"www.appjer0u2.xyz","browser":"","device":"app","os":"Android - 10","username":"yyx8324056","affiliate":"-","affiliate_id":"0","register_ip":"223.104.197.69"},{"player_id":"170244301","time":"1624243651","type":"login","ip":"223.104.14.58","location":"\u5317\u4eac ,\u5317\u4eac\u5e02 ,\u4e2d\u56fd ","domain":"www.appjer0u2.xyz","browser":"","device":"app","os":"Android - 10","username":"yyx8324056","affiliate":"-","affiliate_id":"0","register_ip":"223.104.197.69"},{"player_id":"170244301","time":"1624199998","type":"login","ip":"117.136.91.216","location":"\u5317\u4eac ,\u5317\u4eac\u5e02 ,\u4e2d\u56fd ","domain":"www.appjer0u2.xyz","browser":"","device":"app","os":"Android - 10","username":"yyx8324056","affiliate":"-","affiliate_id":"0","register_ip":"223.104.197.69"},{"player_id":"170244301","time":"1624198558","type":"login","ip":"117.136.4.159","location":"\u6c89\u9633\u5e02 ,\u8fbd\u5b81 ,\u4e2d\u56fd ","domain":"www.appjer0u2.xyz","browser":"","device":"app","os":"Android - 10","username":"yyx8324056","affiliate":"-","affiliate_id":"0","register_ip":"223.104.197.69"},{"player_id":"170244301","time":"1624179115","type":"login","ip":"117.136.4.131","location":"\u6c89\u9633\u5e02 ,\u8fbd\u5b81 ,\u4e2d\u56fd ","domain":"www.appjer0u2.xyz","browser":"","device":"app","os":"Android - 10","username":"yyx8324056","affiliate":"-","affiliate_id":"0","register_ip":"223.104.197.69"},{"player_id":"170244301","time":"1624168714","type":"login","ip":"117.136.4.131","location":"\u6c89\u9633\u5e02 ,\u8fbd\u5b81 ,\u4e2d\u56fd ","domain":"www.appjer0u2.xyz","browser":"","device":"app","os":"Android - 10","username":"yyx8324056","affiliate":"-","affiliate_id":"0","register_ip":"223.104.197.69"},{"player_id":"170244301","time":"1624156627","type":"login","ip":"117.136.4.189","location":"\u6c89\u9633\u5e02 ,\u8fbd\u5b81 ,\u4e2d\u56fd ","domain":"mobileapp.rtw002appbu.com","browser":"","device":"app","os":"Android - 10","username":"yyx8324056","affiliate":"-","affiliate_id":"0","register_ip":"223.104.197.69"}],"recordsTotal":"9","recordsFiltered":"9"}
    if resp.status_code != 200:
        raise NullError(f'status_code={resp.status_code}')
    content = json.loads(resp.text)
    if 'data' not in content:
        return {
            'IsSuccess': False,
            'ErrorCode': config.JSON_ERROR_CODE.code,
            'ErrorMessage': config.JSON_ERROR_CODE.msg.format(platform=cf.platform),
            'Data': resp.text,
            'RawStatusCode': resp.status_code,
            'RawContent': resp.content
        }
    return {
            'IsSuccess': True,
            'ErrorCode': config.SUCCESS_CODE.code,
            'ErrorMessage': config.SUCCESS_CODE.msg,
            'Data': content,
            'RawStatusCode': resp.status_code,
            'RawContent': resp.content
        }


@log_info
@catch_exception
def creditsTotalOptimized(cf, url: str, data: dict, timeout: tuple=(60), endpoints: str='report/getMemberPlayerWalletCreditsTotalOptimized', **kwargs) -> dict:
    '''WG 查詢累積存款金額&次數【报表管理 > 会员报表】'''
    data['rf_cs_rForm_'] = session.rf_cs_rForm
    resp = session.post(url+endpoints, data=data, headers=default_headers, timeout=30+timeout, verify=False)
    # {"total_deposit":8481.93,"total_deposit_count":83,"total_withdrawal":10846,"total_actual_withdrawal":10846,"total_withdrawal_count":55,"total_promo":1082.103,"total_rebate":129.46,"total_adjusted_amount":0,"total_adjusted_amount_count":0}
    if resp.status_code != 200:
        raise NullError(f'status_code={resp.status_code}')
    try:
        content = json.loads(resp.text)
        return {
                'IsSuccess': True,
                'ErrorCode': config.SUCCESS_CODE.code,
                'ErrorMessage': config.SUCCESS_CODE.msg,
                'Data': content,
                'RawStatusCode': resp.status_code,
                'RawContent': resp.content
            }
    except:
        return {
            'IsSuccess': False,
            'ErrorCode': config.JSON_ERROR_CODE.code,
            'ErrorMessage': config.JSON_ERROR_CODE.msg.format(platform=cf.platform),
            'Data': resp.text,
            'RawStatusCode': resp.status_code,
            'RawContent': resp.content
        }            


@log_info
@catch_exception
def report_v2_betting(cf, url: str, params: dict, timeout: tuple=(60), endpoints: str='api/report/report/v2/betting', **kwargs) -> dict:
    '''WG 取得注單內容【报表统计 > 投注记录 > 投注明细】'''
    params['queryType'] = 1
    params['current'] = 1
    params['size'] = 20
    resp = session.get(url+endpoints, params=params, headers=default_headers, timeout=30+timeout, verify=False)
    resp.encoding = 'utf-8-sig'
    rmk = '''
    {'code': 0,
    'msg': 'success',
    'data': {'current_page': 1,
    'per_page': 1,
    'total': 1,
    'total_all_bet': '8.00',
    'total_valid_bet': '8.00',
    'total_net_profit': '-8.00',
    'data': [{'record_id': 'FJ163_3005_1659587940234_705700',
        'order_no': '128-35B5-50157530-3332S1',
        'round_id': '128-35B5-50157530-3332',
        'account_name': 'b131488',
        'account': '96744126',
        'third_user_name': '96744126',
        'platform_id': 13,
        'platform_name': '銀河',
        'game_category_id': 3,
        'game_category_name': '电子',
        'game_id': 3005,
        'game_name': '多福多财',
        'bet_time': '1659587930',
        'settle_time': '1659587930',
        'all_bet': '8.00',
        'valid_bet': '8.00',
        'net_profit': '-8.00',
        'after_balance': 689.0,
        'settle_status': 2,
        'settle_status_desc': '已结算',
        'register_source': '后台添加',
        'winlost_time': '1659587970'}]},
    'timestamp': 1659588123233,
    'success': True}
    '''
    try:
        content = json.loads(resp.text)
        if content.get('code') == 401:  # 被登出
            return {
                'IsSuccess' : False,
                'ErrorCode': config.SIGN_OUT_CODE.code,
                'ErrorMessage': config.SIGN_OUT_CODE.msg.format(platform=cf.platform),
                'RawStatusCode': resp.status_code,
                'RawContent': resp.content
            }
    except:
        return {
            'IsSuccess': False,
            'ErrorCode': config.JSON_ERROR_CODE.code,
            'ErrorMessage': config.JSON_ERROR_CODE.msg.format(platform=cf.platform),
            'RawStatusCode': resp.status_code,
            'RawContent': resp.content
        }    
    if content.get('success', False):
        return {
            'IsSuccess': True,
            'ErrorCode': config.SUCCESS_CODE.code,
            'ErrorMessage': config.SUCCESS_CODE.msg,
            'Data': content,
            'RawStatusCode': resp.status_code,
            'RawContent': resp.content
        }
    else:
        return {
            'IsSuccess': False,
            'ErrorCode': config.HTML_STATUS_CODE.code,
            'ErrorMessage': f"【{cf.platform}】回傳內容：{content.get('msg')}",
            'RawStatusCode': resp.status_code,
            'RawContent': resp.content
            }        

@log_info
@catch_exception
def report_v2_bettingStat(cf, url: str, params: dict, timeout: tuple=(60), endpoints: str='api/report/report/v2/bettingStat', **kwargs) -> dict:
    '''WG 取得注單內容【报表统计 > 投注记录 > 投注明细】抓取total_all_bet、total_valid_bet、total_net_profit(原report_v2_betting抓不到)'''
    params['queryType'] = 1
    params['current'] = 1
    params['size'] = 20
    resp = session.get(url+endpoints, params=params, headers=default_headers, timeout=30+timeout, verify=False)
    resp.encoding = 'utf-8-sig'
    rmk = '''
    {'code': 0,
    'msg': 'success',
    'data': {'current_page': 1,
    'per_page': 1,
    'total': 1,
    'total_all_bet': '8.00',
    'total_valid_bet': '8.00',
    'total_net_profit': '-8.00',
    'data': []},
    'timestamp': 1659588123233,
    'success': True}
    '''
    try:
        content = json.loads(resp.text)
        if content.get('code') == 401:  # 被登出
            return {
                'IsSuccess' : False,
                'ErrorCode': config.SIGN_OUT_CODE.code,
                'ErrorMessage': config.SIGN_OUT_CODE.msg.format(platform=cf.platform),
                'RawStatusCode': resp.status_code,
                'RawContent': resp.content
            }
    except:
        return {
            'IsSuccess': False,
            'ErrorCode': config.JSON_ERROR_CODE.code,
            'ErrorMessage': config.JSON_ERROR_CODE.msg.format(platform=cf.platform),
            'RawStatusCode': resp.status_code,
            'RawContent': resp.content
        }    
    if content.get('success', False):
        return {
            'IsSuccess': True,
            'ErrorCode': config.SUCCESS_CODE.code,
            'ErrorMessage': config.SUCCESS_CODE.msg,
            'Data': content,
            'RawStatusCode': resp.status_code,
            'RawContent': resp.content
        }
    else:
        return {
            'IsSuccess': False,
            'ErrorCode': config.HTML_STATUS_CODE.code,
            'ErrorMessage': f"【{cf.platform}】回傳內容：{content.get('msg')}",
            'RawStatusCode': resp.status_code,
            'RawContent': resp.content
            } 



@log_info
@catch_exception
def v2_betting_detail(cf, url: str, params: dict={}, timeout: tuple=(10, 20), endpoints: str='api/report/report/v2/betting/detail', **kwargs) -> dict:
    '''WG 查注單詳情【报表统计 > 投注记录 > 投注明细 > 详情】'''
    resp = session.get(url + endpoints, params=params, verify=False, timeout=timeout, headers=default_headers)
    resp.encoding = 'utf-8-sig'
    try:
        content = json.loads(resp.text)
        if content.get('code') == 401:  # 被登出
            return {
                'IsSuccess' : False,
                'ErrorCode': config.SIGN_OUT_CODE.code,
                'ErrorMessage': config.SIGN_OUT_CODE.msg.format(platform=cf.platform),
                'RawStatusCode': resp.status_code,
                'RawContent': resp.content
            }
    except:
        return {
            'IsSuccess': False,
            'ErrorCode': config.JSON_ERROR_CODE.code,
            'ErrorMessage': config.JSON_ERROR_CODE.msg.format(platform=cf.platform),
            'RawStatusCode': resp.status_code,
            'RawContent': resp.content
        }   
    if content.get('success', False):
        return {
            'IsSuccess': True,
            'ErrorCode': config.SUCCESS_CODE.code,
            'ErrorMessage': config.SUCCESS_CODE.msg,
            'Data': content,
            'RawStatusCode': resp.status_code,
            'RawContent': resp.content
        }
    else:
        return {
            'IsSuccess': False,
            'ErrorCode': config.HTML_STATUS_CODE.code,
            'ErrorMessage': f"【{cf.platform}】回傳內容：{content.get('msg')}",
            'RawStatusCode': resp.status_code,
            'RawContent': resp.content
            }   


@log_info
@catch_exception
def pg_bet_history(cf, params: dict={}, data: dict={}, url: str='https://public-api.pgjazz.com/', timeout: tuple=(10, 20),
    headers: dict={'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/104.0.0.0 Safari/537.36'}, 
    endpoints: str='web-api/operator-proxy/v1/History/GetBetHistory', **kwargs) -> dict:
    '''PG電子 注單詳細內容'''
    resp = session.post(url + endpoints, params=params, data=data, headers=headers, verify=False, timeout=timeout)
    resp.encoding = 'utf-8-sig'        
    if resp.status_code != 200:
        return {
            'IsSuccess': False,
            'ErrorCode': config['SIGN_OUT_CODE']['code'],
            'ErrorMessage': config['SIGN_OUT_CODE']['msg'].format(platform=cf.platform),
            'RawStatusCode': resp.status_code,
            'RawContent': resp.content
        }
    try:
        content = json.loads(resp.text)
    except:
        return {
            'IsSuccess': False,
            'ErrorCode': config.JSON_ERROR_CODE.code,
            'ErrorMessage': config.JSON_ERROR_CODE.msg.format(platform=cf.platform),
            'RawStatusCode': resp.status_code,
            'RawContent': resp.content
        }   
    err = content.get('err', {})
    if err and err.get('msg', '') == 'Invalid operator session':
        logger.info('(X)PG电子显示：Invalid operator session(2001)')
        raise requests.ConnectionError()
    return {
        'IsSuccess': True,
        'ErrorCode': config['SUCCESS_CODE']['code'],
        'ErrorMessage': config['SUCCESS_CODE']['msg'],
        'Data': content,
        'RawStatusCode': resp.status_code,
        'RawContent': resp.content
    }


@log_info
@catch_exception
def cq9_bet_history(cf, params: dict={}, data: dict={}, url: str='https://detail.liulijing520.com/', timeout: tuple=(10, 20),
    headers: dict={}, endpoints: str='odh5/api/inquire/v1/db/wager', **kwargs) -> dict:
    '''CQ9電子 注單詳細內容'''
    resp = session.get(url + endpoints, params=params, data=data, headers=headers, verify=False, timeout=timeout)
    resp.encoding = 'utf-8-sig'        
    if resp.status_code != 200:
        return {
            'IsSuccess': False,
            'ErrorCode': config['SIGN_OUT_CODE']['code'],
            'ErrorMessage': config['SIGN_OUT_CODE']['msg'].format(platform=cf.platform),
            'RawStatusCode': resp.status_code,
            'RawContent': resp.content
        }
    try:
        content = json.loads(resp.text)
    except:
        return {
            'IsSuccess': False,
            'ErrorCode': config.JSON_ERROR_CODE.code,
            'ErrorMessage': config.JSON_ERROR_CODE.msg.format(platform=cf.platform),
            'RawStatusCode': resp.status_code,
            'RawContent': resp.content
        }   

    return {
        'IsSuccess': True,
        'ErrorCode': config['SUCCESS_CODE']['code'],
        'ErrorMessage': config['SUCCESS_CODE']['msg'],
        'Data': content,
        'RawStatusCode': resp.status_code,
        'RawContent': resp.content
    }


@log_info
@catch_exception
def jdb_bet_history(cf, params: dict={}, data: dict={}, url: str='https://playerapi247.jdb199.com/', timeout: tuple=(10, 20),
    headers: dict={}, endpoints: str='history/gameGroupType', **kwargs) -> dict:
    '''JDB電子 注單詳細內容'''
    resp = session.get(url + endpoints, params=params, data=data, headers=headers, verify=False, timeout=timeout)
    resp.encoding = 'utf-8-sig'        
    if resp.status_code != 200:
        return {
            'IsSuccess': False,
            'ErrorCode': config['SIGN_OUT_CODE']['code'],
            'ErrorMessage': config['SIGN_OUT_CODE']['msg'].format(platform=cf.platform),
            'RawStatusCode': resp.status_code,
            'RawContent': resp.content
        }
    try:
        content = json.loads(resp.text)
    except:
        return {
            'IsSuccess': False,
            'ErrorCode': config.JSON_ERROR_CODE.code,
            'ErrorMessage': config.JSON_ERROR_CODE.msg.format(platform=cf.platform),
            'RawStatusCode': resp.status_code,
            'RawContent': resp.content
        }   

    return {
        'IsSuccess': True,
        'ErrorCode': config['SUCCESS_CODE']['code'],
        'ErrorMessage': config['SUCCESS_CODE']['msg'],
        'Data': content,
        'RawStatusCode': resp.status_code,
        'RawContent': resp.content
    }


@log_info
@catch_exception
def searchGroup(cf, url: str, params: dict={}, headers=default_headers,
    timeout: tuple=(60), endpoints: str='playerGroup/searchGroup', **kwargs) -> dict:
    '''WG 查詢層級列表【会员管理 > 会员层级】'''
    resp = session.get(url+endpoints, params=params, headers=headers, timeout=30+timeout, verify=False)
    # {"results":[{"id":"15","name":"\u4e09\u65b9\u6d4b\u8bd5\u5c42\u7ea7"},{"id":"3","name":"\u4f18\u8d28\u4f1a\u5458"},{"id":"16","name":"\u4f53\u9a8c\u91d1"},{"id":"8","name":"\u505c\u7528\u5c42\u7ea7"},{"id":"12","name":"\u5ba1\u6838\u4e2d"},{"id":"1","name":"\u65b0\u4f1a\u5458"},{"id":"4","name":"\u65e0\u8fd4\u6c34\u65e0\u4f18\u60e0"},{"id":"2","name":"\u6709\u6548\u4f1a\u5458"},{"id":"5","name":"\u8001\u4f1a\u5458"},{"id":"14","name":"\u8fc7\u6e21\u5c42"}],"pagination":{"more":true}}
    if resp.status_code != 200:
        raise NullError(f'status_code={resp.status_code}')
    content = json.loads(resp.text)
    if 'results' not in content:
        return {
            'IsSuccess': False,
            'ErrorCode': config.JSON_ERROR_CODE.code,
            'ErrorMessage': config.JSON_ERROR_CODE.msg.format(platform=cf.platform),
            'Data': resp.text,
            'RawStatusCode': resp.status_code,
            'RawContent': resp.content
        }
    return {
            'IsSuccess': True,
            'ErrorCode': config.SUCCESS_CODE.code,
            'ErrorMessage': config.SUCCESS_CODE.msg,
            'Data': content,
            'RawStatusCode': resp.status_code,
            'RawContent': resp.content
        }


@log_info
@catch_exception
def updateBulklist(cf, url: str, data: dict, headers=default_headers,
                   timeout: tuple=(60), endpoints: str='memberList/updateBulklist', **kwargs) -> dict:
    '''WG 移動層級【会员管理 > 会员列表 > 会员资料 > 修改会员等级】'''
    resp = session.post(url+endpoints, data=data, headers=headers, timeout=30+timeout, verify=False)
    # {"success":true,"code":"","message":"Bulk update success.","data":[]}
    if resp.status_code != 200:
        raise NullError(f'status_code={resp.status_code}')
    try:
        content = json.loads(resp.text)
        if content['success']:
            return {
                    'IsSuccess': True,
                    'ErrorCode': config.SUCCESS_CODE.code,
                    'ErrorMessage': config.SUCCESS_CODE.msg,
                    'Data': content,
                    'RawStatusCode': resp.status_code,
                    'RawContent': resp.content
                }
        else:
            return {
                'IsSuccess': False,
                'ErrorCode': config.LayerError.code,
                'ErrorMessage': config.LayerError.msg.format(platform=cf.platform, msg=content['message']),
                'Data': content,
                'RawStatusCode': resp.status_code,
                'RawContent': resp.content
            }                
    except:
        return {
            'IsSuccess': False,
            'ErrorCode': config.JSON_ERROR_CODE.code,
            'ErrorMessage': config.JSON_ERROR_CODE.msg.format(platform=cf.platform),
            'Data': resp.text,
            'RawStatusCode': resp.status_code,
            'RawContent': resp.content
        }


@log_info
@catch_exception
def operation_message_add(cf, url: str, data: dict, headers=default_headers, timeout: tuple=(60), endpoints: str='api/ops/operation/message/add', **kwargs) -> dict:    
    '''WG 推播通知(1/3)：新增【运营管理 > 消息管理 > 通知消息 > 新增】'''
    resp = session.post(url+endpoints, data=json.dumps(data), headers=headers, timeout=30+timeout, verify=False)
    resp.encoding = 'utf-8-sig'
    # {'code': 0, 'msg': 'success', 'data': True, 'timestamp': 1663829080831, 'success': True}
    # {'code': 4000132, 'msg': '请传递正确的参数', 'data': None, 'timestamp': 1663829187869, 'success': False}
    try:
        content = json.loads(resp.text)
        if content.get('code') == 401:  # 被登出
            return {
                'IsSuccess' : False,
                'ErrorCode': config.SIGN_OUT_CODE.code,
                'ErrorMessage': config.SIGN_OUT_CODE.msg.format(platform=cf.platform),
                'RawStatusCode': resp.status_code,
                'RawContent': resp.content
            }
    except:
        return {
            'IsSuccess': False,
            'ErrorCode': config.JSON_ERROR_CODE.code,
            'ErrorMessage': config.JSON_ERROR_CODE.msg.format(platform=cf.platform),
            'RawStatusCode': resp.status_code,
            'RawContent': resp.content
        }   
    if content.get('success', False):
        return {
            'IsSuccess': True,
            'ErrorCode': config.SUCCESS_CODE.code,
            'ErrorMessage': config.SUCCESS_CODE.msg,
            'Data': content,
            'RawStatusCode': resp.status_code,
            'RawContent': resp.content
        }
    else:
        return {
            'IsSuccess': False,
            'ErrorCode': config.HTML_STATUS_CODE.code,
            'ErrorMessage': f"【{cf.platform}】回傳內容：{content.get('msg')}",
            'RawStatusCode': resp.status_code,
            'RawContent': resp.content
            }   


@log_info
@catch_exception
def operation_message_index(cf, url: str, params: dict, headers=default_headers, timeout: tuple=(60), endpoints: str='api/ops/operation/message/index', **kwargs) -> dict:    
    '''WG 推播通知(2/3)：待審核【运营管理 > 消息管理 > 通知消息】'''
    resp = session.get(url+endpoints, params=params, headers=headers, timeout=30+timeout, verify=False)
    resp.encoding = 'utf-8-sig'
    # {'code': 0, 'msg': 'success', 'data': {'total': 1, 'current': 1, 'pages': 1, 'size': 20, 'records': [{'agent_id': 0, 'UserCount': 1, 'title': '標題lalala8', 'type': 1, 'content': '<p>顯示內容lalala8</p>', 'operator': 'twwg1', 'jpush': 0, 'Send': 1, 'publish_operator': '', 'Read': 0, 'username_ids': '30569144', 'publish_time': 0, 'id': 618, 'agent_mode': '', 'contentType': 0, 'publish_status': 0, 'operate_time': 1663829494, 'UsernameNames': 'chongzhi123', 'ReceiverType': 2, 'CreateTime': 1663829494, 'end_time': 0, 'weight': 0, 'message_id': 0, 'content_origin': '顯示內容lalala8', 'level_ids': '', 'start_time': 0, 'send_time': 1663829494, 'vip_ids': '', 'interval': 0, 'contentList': None, 'status': 1}], 'now': 1663829498}, 'timestamp': 1663829498876, 'success': True}
    # {'code': 4000132, 'msg': '请传递正确的参数', 'data': None, 'timestamp': 1663829092728, 'success': False}
    try:
        content = json.loads(resp.text)
        if content.get('code') == 401:  # 被登出
            return {
                'IsSuccess' : False,
                'ErrorCode': config.SIGN_OUT_CODE.code,
                'ErrorMessage': config.SIGN_OUT_CODE.msg.format(platform=cf.platform),
                'RawStatusCode': resp.status_code,
                'RawContent': resp.content
            }
    except:
        return {
            'IsSuccess': False,
            'ErrorCode': config.JSON_ERROR_CODE.code,
            'ErrorMessage': config.JSON_ERROR_CODE.msg.format(platform=cf.platform),
            'RawStatusCode': resp.status_code,
            'RawContent': resp.content
        }   
    if content.get('success', False):
        return {
            'IsSuccess': True,
            'ErrorCode': config.SUCCESS_CODE.code,
            'ErrorMessage': config.SUCCESS_CODE.msg,
            'Data': content,
            'RawStatusCode': resp.status_code,
            'RawContent': resp.content
        }
    else:
        return {
            'IsSuccess': False,
            'ErrorCode': config.HTML_STATUS_CODE.code,
            'ErrorMessage': f"【{cf.platform}】回傳內容：{content.get('msg')}",
            'RawStatusCode': resp.status_code,
            'RawContent': resp.content
            }   


@log_info
@catch_exception
def operation_message_publish(cf, url: str, data: dict, headers=default_headers, timeout: tuple=(60), endpoints: str='api/ops/operation/message/publish', **kwargs) -> dict:    
    '''WG 推播通知(3/3)：審核發布【运营管理 > 消息管理 > 通知消息】'''
    resp = session.post(url+endpoints, data=json.dumps(data), headers=headers, timeout=30+timeout, verify=False)
    resp.encoding = 'utf-8-sig'
    # {'code': 0, 'msg': 'success', 'data': True, 'timestamp': 1663830586603, 'success': True}
    # {'code': 999, 'msg': 'record not found', 'data': None, 'timestamp': 1663830523498, 'success': False}
    try:
        content = json.loads(resp.text)
        if content.get('code') == 401:  # 被登出
            return {
                'IsSuccess' : False,
                'ErrorCode': config.SIGN_OUT_CODE.code,
                'ErrorMessage': config.SIGN_OUT_CODE.msg.format(platform=cf.platform),
                'RawStatusCode': resp.status_code,
                'RawContent': resp.content
            }
    except:
        return {
            'IsSuccess': False,
            'ErrorCode': config.JSON_ERROR_CODE.code,
            'ErrorMessage': config.JSON_ERROR_CODE.msg.format(platform=cf.platform),
            'RawStatusCode': resp.status_code,
            'RawContent': resp.content
        }   
    if content.get('success', False):
        return {
            'IsSuccess': True,
            'ErrorCode': config.SUCCESS_CODE.code,
            'ErrorMessage': config.SUCCESS_CODE.msg,
            'Data': content,
            'RawStatusCode': resp.status_code,
            'RawContent': resp.content
        }
    else:
        return {
            'IsSuccess': False,
            'ErrorCode': config.HTML_STATUS_CODE.code,
            'ErrorMessage': f"【{cf.platform}】回傳內容：{content.get('msg')}",
            'RawStatusCode': resp.status_code,
            'RawContent': resp.content
            }   


@log_info
@catch_exception
def accountEnquiryRecords(cf, url: str, params: dict, headers=default_headers,
                   timeout: tuple=(60), endpoints: str='ipAddressMonitoring/accountEnquiryRecords', **kwargs) -> dict:
    '''WG 查詢會員登錄【风控管理 > 帐号查询】'''

    resp = session.get(url+endpoints, params=params, headers=headers, timeout=30+timeout, verify=False)
    # {"success":true,"code":"S0189","message":"新增成功","data":[]}
    # {"success":true,"code":"","message":"Bulk update success.","data":[]}
    if resp.status_code != 200:
        raise NullError(f'status_code={resp.status_code}')
    try:
        content = json.loads(resp.text)
        if content['status']:
            return {
                    'IsSuccess': True,
                    'ErrorCode': config.SUCCESS_CODE.code,
                    'ErrorMessage': config.SUCCESS_CODE.msg,
                    'Data': content,
                    'RawStatusCode': resp.status_code,
                    'RawContent': resp.content
                }
        else:
            return {
                'IsSuccess': False,
                'ErrorCode': config.LayerError.code,
                'ErrorMessage': config.LayerError.msg.format(platform=cf.platform, msg=content['message']),
                'Data': content,
                'RawStatusCode': resp.status_code,
                'RawContent': resp.content
            }                
    except:
        return {
            'IsSuccess': False,
            'ErrorCode': config.JSON_ERROR_CODE.code,
            'ErrorMessage': config.JSON_ERROR_CODE.msg.format(platform=cf.platform),
            'Data': resp.text,
            'RawStatusCode': resp.status_code,
            'RawContent': resp.content
        }        