from . import CODE_DICT as config
from urllib import parse
import requests_html
import werdsazxc
import datetime
import requests
import logging
import json
import re
from .utils import (
    log_info,
    alert_pattern,
    default_headers,
    catch_exception,
    NotSignError,
)
logger = logging.getLogger('robot')

session = requests_html.HTMLSession()
session.cookies.set('langcode', 'zh-cn')
session.cookies.set('langx', 'zh-cn')
session.cookies.set('lang', 'zh-cn')
session.login = False


@log_info
@catch_exception
def login(cf: dict, url: str, acc: str, pw: str, otp: str,
          timeout: tuple = (3, 5), endpoints: str = 'hex/login/v2', **kwargs) -> dict:
    '''BBIN 登入
    Args:
        url : BBIN後台網址，範例格式 'https://jsj888.33443356.com/'
        acc : BBIN使用者帳號
        pw : BBIN使用者密碼
        otp : BBIN動態驗證碼
    Returns:{
        'IsSuccess': 是(登入成功/布林值)否(登入失敗/布林值),
        'ErrorCode': 錯誤代碼(如果前述布林值為False才有內容/文字格式)
        'ErrorMessage': 錯誤原因(如果前述布林值為False才有內容/文字格式)
        'RawStatusCode': BBIN原始回傳狀態碼
        'RawContent': BBIN原始回傳內容
    }'''
    # 清空cookies
    session.login = False
    session.cookies.clear()

    # 更新headers
    default_headers['origin'] = url[:-1]

    # 【第一階段登入】
    resp = session.post(
        url + endpoints,
        data=json.dumps({
            'username': acc,
            'password': pw,
            'otp': otp
        }),
        headers={
            'referer': url + 'vi/login',
            **default_headers
        },
        verify=False,
        timeout=timeout
    )
    # 檢查狀態碼為成功
    if resp.status_code != 200:
        return {
            'IsSuccess': False,
            'ErrorCode': config['HTML_STATUS_CODE']['code'],
            'ErrorMessage': config['HTML_STATUS_CODE']['msg'].format(
                status_code=resp.status_code, platform=cf.platform),
            'RawStatusCode': resp.status_code,
            'RawContent': resp.content
        }
    # 檢查是否有彈跳視窗
    match = alert_pattern.search(resp.text)
    if match:
        return {
            'IsSuccess': False,
            'ErrorCode': config['HTML_CONTENT_CODE']['code'],
            'ErrorMessage': f'{cf["platform"]} 显示：{match.group()}',
            'RawStatusCode': resp.status_code,
            'RawContent': resp.content
        }
    # 解析網站回應, 失敗直接結束
    try:
        result = resp.json()
    except json.JSONDecodeError as e:
        return {
            'IsSuccess': False,
            'ErrorCode': config['JSON_ERROR_CODE']['code'],
            'ErrorMessage': '{platform}网页回应异常，请联系{platform}'.format(
                platform=cf.platform
            ),
            'RawStatusCode': resp.status_code,
            'RawContent': resp.content
        }
    # 平台回應失敗
    if result.get('message'):
        if result['message'] in ['block ip', 'No service']:
            message = f'{cf["platform"]} 显示：IP未在服務的範圍'
        elif result['message'] == 'Your username or password is invalid':
            message = f'{cf["platform"]} 显示：会员帐号或密码输入错误，请重新输入'
        elif result['message'] == 'In maintenance':
            message = f'{cf["platform"]} 显示：网站进行系统维护中'
        else:
            message = f'{cf["platform"]} 显示：{result["message"]}'
        return {
            'IsSuccess': False,
            'ErrorCode': config.ACC_CODE.code,
            'ErrorMessage': message,
            'RawStatusCode': resp.status_code,
            'RawContent': resp.content
        }
    # 檢查密碼是否過期
    if result.get('data', {}).get('expire') == True:
        return {
            'IsSuccess': False,
            'ErrorCode': config.ACC_CODE.code,
            'ErrorMessage': f'{cf["platform"]} 显示：您一个月没有更新密码了,请更新密码',
            'RawStatusCode': resp.status_code,
            'RawContent': resp.content
        }
    # 檢查result是True
    if result['result'] is not True:
        return {
            'IsSuccess': False,
            'ErrorCode': config.EXCEPTION_CODE.code,
            'ErrorMessage': config.EXCEPTION_CODE.msg.format(platform=cf.platform),
            'RawStatusCode': resp.status_code,
            'RawContent': resp.content
        }
    # 檢查登入方式
    if result['data']['login_result'] == 1:
        pass
    elif result['data']['login_result'] == 27:
        # 檢查是否有取得必須欄位
        if (set(['token', 'device_id', 'session_id', 'user', 'ws_url']) - resp.json()['data'].keys() or
                set(['id']) - resp.json()['data']['user'].keys()
        ):
            return {
                'IsSuccess': False,
                'ErrorCode': config.HTML_CONTENT_CODE.code,
                'ErrorMessage': config.HTML_CONTENT_CODE.msg.format(platform=cf.platform),
                'RawStatusCode': resp.status_code,
                'RawContent': resp.content
            }
        # 【第二階段登入】
        resp = session.post(
            url + endpoints,
            data=json.dumps({
                'username': acc,
                'password': pw,
                'ub_auth_token': otp,
                'verify_type': "ubauth",
                'auth_token': result['data']['token'],
                'device_id': result['data']['device_id'],
                'sid': result['data']['session_id'],
                'uid': result['data']['user']['id'],
                'ws_url': result['data']['ws_url']
            }),
            headers={
                'referer': url + 'vi/login',
                **default_headers
            },
            verify=False,
            timeout=timeout
        )
    else:
        return {
            'IsSuccess': False,
            'ErrorCode': config.EXCEPTION_CODE.code,
            'ErrorMessage': '机器人仅支援BBIN Ubauth验证登入，你的BBIN帐号非Ubauth验证，请至BBIN后台重新设置后，再重启机器人。',
            'RawStatusCode': resp.status_code,
            'RawContent': resp.content
        }

    # 解析網站回應, 失敗直接結束
    try:
        result = resp.json()
    except json.JSONDecodeError as e:
        return {
            'IsSuccess': False,
            'ErrorCode': config['JSON_ERROR_CODE']['code'],
            'ErrorMessage': '{platform}网页回应异常，请联系{platform}'.format(
                platform=cf.platform
            ),
            'RawStatusCode': resp.status_code,
            'RawContent': resp.content
        }
    # 檢查錯誤訊息
    if result.get('message'):
        if result['message'] in ['UBAuth error', 'UBAuth is invalid', 'ub_auth_token is invalid']:
            return {
                'IsSuccess': False,
                'ErrorCode': config.OTP_CODE.code,
                'ErrorMessage': config.OTP_CODE.msg,
                'RawStatusCode': resp.status_code,
                'RawContent': resp.content
            }
        else:
            return {
                'IsSuccess': False,
                'ErrorCode': config.OTP_CODE.code,
                'ErrorMessage': f"{cf['platform']} 显示：{result['message']}",
                'RawStatusCode': resp.status_code,
                'RawContent': resp.content
            }
    # 【登入成功】
    session.cookies.set('sid', result['data']['session_id'])
    session.cookies.set('langcode', 'zh-cn')
    session.cookies.set('langx', 'zh-cn')
    session.cookies.set('lang', 'zh-cn')
    session.url = url
    session.acc = acc
    session.pw = pw
    session.login = True
    logger.info(f'cookies: {session.cookies.get_dict()}')
    return {
        'IsSuccess': True,
        'ErrorCode': config.SUCCESS_CODE.code,
        'ErrorMessage': '',
        'RawStatusCode': resp.status_code,
        'RawContent': resp.content
    }


@log_info
@catch_exception
def user_list(cf, url: str, params: dict, headers: dict = {},
              timeout: tuple = (10, 20), endpoints: str = 'user/list', **kwargs) -> dict:
    '''BBIN 會員批次查詢功能'''

    resp = session.get(url=url + endpoints, params=params, timeout=timeout, verify=False)
    # 被登出
    if 'window.open' in resp.text:
        raise NotSignError('帐号已其他地方登入, 请重新登入')
    # 被登出2
    if 'System Error:#0000 ' in resp.text:
        raise NotSignError('帐号已其他地方登入, 请重新登入')
    # 被登出3
    if '請重新登入' in resp.text:
        raise NotSignError('帐号已其他地方登入, 请重新登入')
    # 被登出4
    if resp.status_code == 401:
        raise NotSignError('请重新登入, 謝謝')
    # 檢查權限不足
    if resp.status_code == 403:
        logger.info('respone status code: 403')
        return {
            'IsSuccess': False,
            'ErrorCode': config.PERMISSION_CODE.code,
            'ErrorMessage': config.PERMISSION_CODE.msg.format(platform=cf.platform),
            'RawStatusCode': resp.status_code,
            'RawContent': resp.content
        }
    # 檢查狀態碼
    if resp.status_code != 200:
        return {
            'IsSuccess': False,
            'ErrorCode': config['HTML_STATUS_CODE']['code'],
            'ErrorMessage': config['HTML_STATUS_CODE']['msg'].format(
                status_code=resp.status_code, platform=cf.platform),
            'RawStatusCode': resp.status_code,
            'RawContent': resp.content
        }
    # 檢查彈出視窗
    match = alert_pattern.search(resp.text)
    if match:
        if match.group() == '此会员十秒内已有执行人工存入(防止重复入款)，请稍后入款!':
            return {
                'IsSuccess': False,
                'ErrorCode': config.REPEAT_DEPOSIT.code,
                'ErrorMessage': config.REPEAT_DEPOSIT.msg.format(platform=cf.platform, msg=match.group()),
                'Data': {},
                'RawStatusCode': resp.status_code,
                'RawContent': resp.content
            }
        if '所輸入的金額數量無法由您的權限做設定' in match.group():
            return {
                'IsSuccess': False,
                'ErrorCode': config.PERMISSION_CODE.code,
                'ErrorMessage': config.DEPOSIT_FAIL.msg.format(platform=cf.platform, msg=match.group()),
                'RawStatusCode': resp.status_code,
                'RawContent': resp.content
            }
        return {
            'IsSuccess': False,
            'ErrorCode': config.DEPOSIT_FAIL.code,
            'ErrorMessage': config.DEPOSIT_FAIL.msg.format(platform=cf.platform, msg=match.group()),
            'RawStatusCode': resp.status_code,
            'RawContent': resp.content
        }
    # 檢查內容
    table = resp.html.find('div.tab-content table', first=True)
    if table is None:
        raise NotSignError('帐号已其他地方登入, 请重新登入')
    columns = [th.text for th in table.find('thead > tr > th')]
    userids = [
        re.search(
            '/user/(?P<userid>\d+)/detail_info',
            tr.find('a', first=True).attrs.get('href')
        ).group('userid')
        for tr in table.find('tbody > tr')
    ]
    data = [
        dict(zip(columns, map(lambda td: td.text, tr.find('td'))))
        for tr in table.find('tbody > tr')
    ]
    data = [
        {'userid': userid, **{k: v for k, v in d.items() if k != ''}}
        for d, userid in zip(data, userids)
    ]
    data = {d['帐号']: d for d in data}
    return {
        'IsSuccess': True,
        'ErrorCode': config.SUCCESS_CODE.code,
        'ErrorMessage': config.SUCCESS_CODE.msg,
        'Data': data,
        'RawStatusCode': resp.status_code,
        'RawContent': resp.content
    }


@log_info
@catch_exception
def agv3_cl(cf, url: str, params: dict, data: dict, headers: dict = {},
            timeout: tuple = (10, 20), endpoints: str = 'agv3/cl/', **kwargs) -> dict:
    '''BBIN 帐号管理、现金系统
    Args:
        url : (必填) BBIN後台網址，範例格式 'https://jsj888.33443356.com/'
        params : (必填) BBIN API 網址參數, 使用字典傳入, 內容參考下方
        data :  (必填) 查詢內容, 使用字典傳入, 內容參考下方
        headers : BBIN API 表頭, 使用字典傳入, 預設為: {
            'content-type': 'application/json;charset=UTF-8',
            'x-requested-with': 'XMLHttpRequest',
            # 登入時會添加
            'origin': 'https://jsj888.33443356.com'
        }
    Returns:{
        'IsSuccess': 是(登入成功/布林值)否(登入失敗/布林值),
        'ErrorCode': 錯誤代碼(如果前述布林值為False才有內容/文字格式)
        'ErrorMessage': 錯誤原因(如果前述布林值為False才有內容/文字格式)
        'Data': {內容參考下方}
        'RawStatusCode': BBIN原始回傳狀態碼
        'RawContent': BBIN原始回傳內容
    }
    >>> 查詢會員層級(帐号管理->层级管理->会员查询)
        Args:
            params: {'module': 'Level', 'method': 'searchMemList'}
            data: {'Users': 'zhanglangge'}
        Returns:
            Data: {
                'status': 'ok' or 'error',
                'notfound': [],
                'user_list': [{
                    'parent_name': 'djsjt168',
                    'user_id': '620301765',
                    'user_name': 'zhanglangge',
                    'create_time': '2019-10-23 06:02:39',
                    'deposit_count': 3,
                    'withdrawal_count': 0,
                    'level_id': 15116,
                    'locked': False,
                    'deposit_total': '3',
                    'deposit_max': '1',
                    'withdrawal_total': 0
                }],
                'select': [{
                    {'level_id': 13539, 'alias': '未分層', 'user_count': 25979},
                    {'level_id': 15116, 'alias': '测试支付', 'user_count': 7},
                }]
            }
    >>> 充值前查詢會員(现金系统->现金系统->人工线上存提->人工存入)
        Args:
            params: {'module': 'Deposit', 'method': 'query', 'sid': ''}
            data: {'search_name': 'zhanglangge'},
        Returns:
            # 查不到時回應 {'LoginName': None}
            # 查的到時回應如下
            Data: {
                # 充值需要的內容
                'user_id': 620301765,
                'HallID': 3820316,
                'CHK_ID': '1c7c6bd622716a2d1466bc739f224f9a',
                'date': '2021-03-26 04:10:49 PM'
                # 以下為暫時沒用到的內容
                'user_name': 'zhanglangge',
                'Balance_BB': '392.0000',
                'JsAuditValue': 1,
                'SpLimit': 1,
                'SpRate': 0.5,
                'SpMax': 0,
                'DailySpMax': 0,
                'AbLimit': 10,
                'AbRate': 0,
                'AbMax': 0,
                'ComplexAudit': 'Y',
                'ComplexAuditValue': 1,
                'NormalityAudit': 'Y',
                'NormalityAuditValue': 100,
                'DailyAbsorbMax_Manual': 0,
                'AbTotal': 0,
                'SpTotal': 0,
                'Currency': 'RMB',
                'LoginName': 'zhanglangge',
            }
    >>> 充值(现金系统->现金系统->人工线上存提->人工存入->确定)
        Args:
            params: {'module': 'Deposit', 'method': 'deposit', 'sid': ''}
            data: {
                'user_id': 620301765,
                'hallid': 3820316,
                'CHK_ID': 'a55097f59c3065402a9f48de5fe6c518',
                'user_name': 'zhanglangge',
                'date': '2021-03-26 04:19:36 PM',
                'currency': 'RMB',
                'abamount_limit': '0',
                'amount': float(1),  # 充值金額
                'amount_memo': '充值測試',
                'ComplexAuditCheck': '1',
                'complex': float(1) * 1,  # 打碼量
                'CommissionCheck': 'Y',
                'DepositItem': 'ARD8'
            }
        Returns:
            Data: {} # 充值回傳為self.location.href
    >>> 層級調整(帐号管理->层级管理->会员查询->分层)
        Args:
            params: {'module': 'Level', 'method': 'searchMemListSet'}
            data: {'Change[0][user_id]': '855417666', 'Change[0][target]': '17741'}
        Returns:
            {status: "ok"}
    '''
    logger.info(params)
    # 【充值】不進行retry, 失敗要直接return
    if params['module'] == 'Deposit' and params['method'] == 'deposit':
        try:
            resp = session.post(url + endpoints, params=params, data=data, verify=False, timeout=timeout,
                                headers=headers)
        except Exception as e:
            werdsazxc.log_trackback()
            return {
                'IsSuccess': False,
                'ErrorCode': config.IGNORE_CODE.code,
                'ErrorMessage': config.IGNORE_CODE.msg
            }
    else:
        resp = session.post(url + endpoints, params=params, data=data, verify=False, timeout=timeout, headers=headers)

    # 被登出
    if 'window.open' in resp.text:
        raise NotSignError('帐号已其他地方登入, 请重新登入')
    # 被登出2
    if 'System Error:#0000 ' in resp.text:
        raise NotSignError('帐号已其他地方登入, 请重新登入')
    # 被登出3
    if '請重新登入' in resp.text:
        raise NotSignError('帐号已其他地方登入, 请重新登入')
    # 被登出4
    if resp.status_code == 401:
        raise NotSignError('请重新登入, 謝謝')
    # 檢查權限不足
    if resp.status_code == 403:
        logger.info('respone status code: 403')
        return {
            'IsSuccess': False,
            'ErrorCode': config.PERMISSION_CODE.code,
            'ErrorMessage': config.PERMISSION_CODE.msg.format(platform=cf.platform),
            'RawStatusCode': resp.status_code,
            'RawContent': resp.content
        }
    # 檢查狀態碼
    if resp.status_code != 200:
        return {
            'IsSuccess': False,
            'ErrorCode': config['HTML_STATUS_CODE']['code'],
            'ErrorMessage': config['HTML_STATUS_CODE']['msg'].format(
                status_code=resp.status_code, platform=cf.platform),
            'RawStatusCode': resp.status_code,
            'RawContent': resp.content
        }
    # 檢查彈出視窗
    match = alert_pattern.search(resp.text)
    if match:
        if match.group() == '此会员十秒内已有执行人工存入(防止重复入款)，请稍后入款!':
            return {
                'IsSuccess': False,
                'ErrorCode': config.REPEAT_DEPOSIT.code,
                'ErrorMessage': config.REPEAT_DEPOSIT.msg.format(platform=cf.platform, msg=match.group()),
                'Data': {},
                'RawStatusCode': resp.status_code,
                'RawContent': resp.content
            }
        if '所輸入的金額數量無法由您的權限做設定' in match.group():
            return {
                'IsSuccess': False,
                'ErrorCode': config.PERMISSION_CODE.code,
                'ErrorMessage': config.DEPOSIT_FAIL.msg.format(platform=cf.platform, msg=match.group()),
                'RawStatusCode': resp.status_code,
                'RawContent': resp.content
            }
        return {
            'IsSuccess': False,
            'ErrorCode': config.DEPOSIT_FAIL.code,
            'ErrorMessage': config.DEPOSIT_FAIL.msg.format(platform=cf.platform, msg=match.group()),
            'RawStatusCode': resp.status_code,
            'RawContent': resp.content
        }
    # 充值功能成功時回傳非json格式
    if params['module'] == 'Deposit' and params['method'] == 'deposit':
        return {
            'IsSuccess': True,
            'ErrorCode': config.SUCCESS_CODE.code,
            'ErrorMessage': config.SUCCESS_CODE.msg,
            'Data': {},
            'RawStatusCode': resp.status_code,
            'RawContent': resp.content
        }
    # 檢查json回傳
    if resp.json().get('status', 'ok') not in ['ok', 'Y']:
        success = False
        code = config.DEPOSIT_FAIL.code
        msg = resp.json().get('message') or resp.json().get('msg')
        message = config.DEPOSIT_FAIL.msg.format(platform=cf.platform, msg=msg)
    elif resp.json().get('INFO', {}).get('error'):
        success = False
        code = config.DEPOSIT_FAIL.code
        msg = resp.json().get('INFO', {}).get('error')
        message = config.DEPOSIT_FAIL.msg.format(platform=cf.platform, msg=msg)
    else:
        success = True
        code = config.SUCCESS_CODE.code
        message = config.SUCCESS_CODE.msg
    # 其餘回傳
    return {
        'IsSuccess': success,
        'ErrorCode': code,
        'ErrorMessage': message,
        'Data': resp.json(),
        'RawStatusCode': resp.status_code,
        'RawContent': resp.content
    }


@log_info
@catch_exception
def cash_system_search(cf, url: str, data: dict, headers: dict = {},
    timeout: tuple = (10, 20), endpoints: str = 'agv3/callduck/api/cash-system/search',**kwargs) -> dict:
    '''BB现金系统
    Args:
        url : (必填) BBIN後台網址，範例格式 'https://jsj888.33443356.com/'
        data :  (必填) 查詢內容, 使用字典傳入, 內容參考下方
        headers : BBIN API 表頭, 使用字典傳入, 預設為: {}
    Returns:{
        'IsSuccess': 是(登入成功/布林值)否(登入失敗/布林值),
        'ErrorCode': 錯誤代碼(如果前述布林值為False才有內容/文字格式)
        'ErrorMessage': 錯誤原因(如果前述布林值為False才有內容/文字格式)
        'RawStatusCode': BBIN原始回傳狀態碼
        'RawContent': BBIN原始回傳內容
        'Data': {
            'status': 'Y',
            'code': '',
            'message': '',
            'data': {
                'cashEntryList': [
                    {
                        'id': '97192338759',
                        'username': 'zjl333',
                        'dealCategory': '活动优惠',
                        'amount': '7.34',
                        'balance': '8.18',
                        'currency': 'RMB',
                        'dealDate': '2021-08-16 09:14:05',
                        'serialNumber': '',
                        'memo': {'type': 'text', 'content': '刮刮乐'}
                    },
                    ...
                ],
                'balanceAmount': '4.85',
                'total': '29.91',
                'pageNum': 1,
                'pageCount': 500,
                'page': 1,
                'totalCount': '5'
            }
        }
    }
    '''
    logger.info(data)
    resp = session.post(url=url + endpoints, data=data, timeout=timeout, verify=False)

    # 檢查狀態碼
    if resp.status_code != 200:
        return {
            'IsSuccess': False,
            'ErrorCode': config['HTML_STATUS_CODE']['code'],
            'ErrorMessage': config['HTML_STATUS_CODE']['msg'].format(
                status_code=resp.status_code, platform=cf.platform),
            'RawStatusCode': resp.status_code,
            'RawContent': resp.content
        }

    js = resp.json()
    # 權限不足
    if js['code'] == 111130009:
        return {
            'IsSuccess': False,
            'ErrorCode': config.PERMISSION_CODE.code,
            'ErrorMessage': config.PERMISSION_CODE.msg.format(platform=cf.platform),
            'RawStatusCode': resp.status_code,
            'RawContent': resp.content
        }

    # 被登出, BBIN回傳此code, 訊息為系统繁忙，请稍后再试
    if js['code'] == 150330001:
        raise NotSignError('帐号已其他地方登入, 请重新登入')

    # 其餘未知回傳
    if js['status'] != 'Y':
        return {
            'IsSuccess': False,
            'ErrorCode': config['HTML_CONTENT_CODE']['code'],
            'ErrorMessage': f'{cf["platform"]} 显示：{js["message"]}',
            'RawStatusCode': resp.status_code,
            'RawContent': resp.content
        }

    # 成功回傳
    return {
        'IsSuccess': True,
        'ErrorCode': config.SUCCESS_CODE.code,
        'ErrorMessage': config.SUCCESS_CODE.msg,
        'Data': js,
        'RawStatusCode': resp.status_code,
        'RawContent': resp.content
    }


@log_info(log_args=False, log_result=False)
@catch_exception
def game_betrecord_search(cf, url: str, params: dict, headers: dict = {'Accept-Encoding': ''},
                          timeout: tuple = (10, 20), endpoints: str = 'game/betrecord_search/kind5', **kwargs) -> dict:
    '''BBIN 注單查詢
    Args:
        url : (必填) BBIN後台網址，範例格式 'https://jsj888.33443356.com/'
        params : (必填) BBIN API 網址參數, 使用字典傳入
        headers : BBIN API 表頭, 使用字典傳入, 預設為: {
            'content-type': 'application/json;charset=UTF-8',
            'x-requested-with': 'XMLHttpRequest',
            # 登入時會添加
            'origin': 'https://jsj888.33443356.com'
        }
    Returns:{
        'IsSuccess': 是(登入成功/布林值)否(登入失敗/布林值),
        'ErrorCode': 錯誤代碼(如果前述布林值為False才有內容/文字格式)
        'ErrorMessage': 錯誤原因(如果前述布林值為False才有內容/文字格式)
        'RawStatusCode': BBIN原始回傳狀態碼
        'RawContent': BBIN原始回傳內容
    }'''
    headers = headers or default_headers
    resp = session.get(url + endpoints, params=params, verify=False, timeout=timeout, headers=headers)
    # 被登出
    if 'window.open' in resp.text:
        raise NotSignError('帐号已其他地方登入, 请重新登入')
    # 被登出2
    if 'System Error:#0000 ' in resp.text:
        raise NotSignError('帐号已其他地方登入, 请重新登入')
    # 被登出3
    if '請重新登入' in resp.text:
        raise NotSignError('帐号已其他地方登入, 请重新登入')
    # 被登出4
    if resp.status_code == 401:
        raise NotSignError('请重新登入, 謝謝')
    # 檢查權限不足
    if resp.status_code == 403:
        logger.info('respone status code: 403')
        return {
            'IsSuccess': False,
            'ErrorCode': config.PERMISSION_CODE.code,
            'ErrorMessage': config.PERMISSION_CODE.msg.format(platform=cf.platform),
            'RawStatusCode': resp.status_code,
            'RawContent': resp.content
        }
    # 檢查狀態碼
    if resp.status_code != 200:
        return {
            'IsSuccess': False,
            'ErrorCode': config['HTML_STATUS_CODE']['code'],
            'ErrorMessage': config['HTML_STATUS_CODE']['msg'].format(
                status_code=resp.status_code, platform=cf.platform),
            'RawStatusCode': resp.status_code,
            'RawContent': resp.content
        }
    # 檢查彈出視窗
    match = alert_pattern.search(resp.text)
    if match:
        return {
            'IsSuccess': False,
            'ErrorCode': config['HTML_CONTENT_CODE']['code'],
            'ErrorMessage': f'{cf["platform"]} 显示：{match.group()}',
            'RawStatusCode': resp.status_code,
            'RawContent': resp.content
        }

    html = requests_html.HTML(html=resp.text)

    # 撈取所有分類
    match = re.search('(?<=var amenu \= \').+(?=\';)', resp.text)
    if match:
        amenu = json.loads(match.group())
    else:
        amenu = {}
    # 撈取遊戲
    games = {opt.text: opt.attrs.get('value') for opt in html.find('#gametypelist > option')}
    # 撈取畫面頁數
    total_page = max([1] + [int(a.text) for a in html.find('.pagination > li:last-child')])
    # 撈取所有訂單
    columns = [th.text for th in html.find('table thead tr th:not([colspan])')]
    html.find('table tbody tr td:nth-child(1) a')
    records = []
    for tr in html.find('table tbody tr'):
        r = dict(zip(columns, [td.text for td in tr.find('td')]))
        a = tr.find('td:nth-child(1) > a', first=True)
        if a:
            r['url'] = a.attrs.get('href')
        else:
            r['url'] = ''
        # 點擊內容網址
        r['wagers_detail_url'] = tr.find('input')[0].attrs.get('value', '') if tr.find('input') else ''
        records.append(r)

    summary = {}
    for tr in html.find('table tfoot tr'):
        r = dict(zip(columns, [td.text for td in tr.find('th')]))
        if '小计' in tr.text:
            summary['小计'] = r
        elif '总计' in tr.text:
            summary['总计'] = r
        else:
            logger.warning(f'網頁可能改版(summary): {r}')

    return {
        'IsSuccess': True,
        'ErrorCode': config.SUCCESS_CODE.code,
        'ErrorMessage': config.SUCCESS_CODE.msg,
        'Data': {
            'amenu': amenu,
            'records': records,
            'games': games,
            'summary': summary,
            'total_page': total_page,
        },
        'RawStatusCode': resp.status_code,
        'RawContent': resp.content
    }

@log_info
@catch_exception
def betrecord_betrecord_url(cf, url: str, params: dict, headers: dict = {},
    timeout: tuple = (10, 20), endpoints: str = 'game/betrecord/betrecord_url',**kwargs) -> dict:
    '''BBIN 注單詳細內容
    Args:
        url : (必填) BBIN後台網址，範例格式 'https://jsj888.33443356.com/'
        params : (必填) BBIN API 網址參數, 使用字典傳入, 範例格式 {
            'gamekind': '58',                    # PG電子
            'userid': '613862401',               # 會員ID
            'wagersid': '1377118144690225152',   # 住單號
            'SearchData': 'MemberBets'           # 預設(查詢注單)
        }
        headers : BBIN API 表頭, 使用字典傳入, 預設為: {
            'content-type': 'application/json;charset=UTF-8',
            'x-requested-with': 'XMLHttpRequest',
            # 登入時會添加
            'origin': 'https://jsj888.33443356.com'
        }
    Returns:{
        'IsSuccess': 是(登入成功/布林值)否(登入失敗/布林值),
        'ErrorCode': 錯誤代碼(如果前述布林值為False才有內容/文字格式)
        'ErrorMessage': 錯誤原因(如果前述布林值為False才有內容/文字格式)
        'RawStatusCode': BBIN原始回傳狀態碼
        'RawContent': BBIN原始回傳內容
    }'''

    resp = session.get(url + endpoints, params=params, headers=headers, timeout=timeout, verify=False)
    # 被登出
    if 'window.open' in resp.text:
        raise NotSignError('帐号已其他地方登入, 请重新登入')
    # 被登出2
    if 'System Error:#0000 ' in resp.text:
        raise NotSignError('帐号已其他地方登入, 请重新登入')
    # 被登出3
    if '請重新登入' in resp.text:
        raise NotSignError('帐号已其他地方登入, 请重新登入')
    # 被登出4
    if resp.status_code == 401:
        raise NotSignError('请重新登入, 謝謝')
    # 檢查權限不足
    if resp.status_code == 403:
        logger.info('respone status code: 403')
        return {
            'IsSuccess': False,
            'ErrorCode': config.PERMISSION_CODE.code,
            'ErrorMessage': config.PERMISSION_CODE.msg.format(platform=cf.platform),
            'RawStatusCode': resp.status_code,
            'RawContent': resp.content
        }
    # 檢查狀態碼
    if resp.status_code != 200:
        return {
            'IsSuccess': False,
            'ErrorCode': config['HTML_STATUS_CODE']['code'],
            'ErrorMessage': config['HTML_STATUS_CODE']['msg'].format(
                status_code=resp.status_code, platform=cf.platform),
            'RawStatusCode': resp.status_code,
            'RawContent': resp.content
        }

    data = resp.json()

    # 檢查API回傳內容
    if data['error_code'] != 0:
        return {
            'IsSuccess': False,
            'ErrorCode': config['HTML_CONTENT_CODE']['code'],
            'ErrorMessage': f'{cf["platform"]} 显示：{data["error_message"]}({data["error_code"]})',
            'RawStatusCode': resp.status_code,
            'RawContent': resp.content
        }

    return {
        'IsSuccess': True,
        'ErrorCode': config['SUCCESS_CODE']['code'],
        'ErrorMessage': config['SUCCESS_CODE']['msg'],
        'Data': data,
        'RawStatusCode': resp.status_code,
        'RawContent': resp.content
    }


@log_info
@catch_exception
def deposit_and_withdraw_info(cf, url: str, params: dict = {}, headers: dict = {},
    timeout: tuple = (10, 20),endpoints: str = 'hex/user/772332969/deposit_and_withdraw/info', **kwargs) -> dict:
    '''BBIN 會員累積充值金額、次數
    Args:
        url : (必填) BBIN後台網址，範例格式 'https://jsj888.33443356.com/'
        params : (必填) BBIN API 網址參數, 使用字典傳入, 範例格式 {
        }
        headers : BBIN API 表頭, 使用字典傳入, 預設為: {
            'permname': 'UserDetailInfo',
            # referer必須與url中的userId相同
            'referer': 'https://wyma.1629yl.com/vi/user/772332969/detail_info'
        }
    Returns:{
        'IsSuccess': 是(登入成功/布林值)否(登入失敗/布林值),
        'ErrorCode': 錯誤代碼(如果前述布林值為False才有內容/文字格式)
        'ErrorMessage': 錯誤原因(如果前述布林值為False才有內容/文字格式)
        'RawStatusCode': BBIN原始回傳狀態碼
        'RawContent': BBIN原始回傳內容
        'Data': {
            'user_id': 會員的ID，可用於比對是否正確，例：772332969
            'username': 會員的帳號，可用於比對是否正確，例：sai888
            'deposit_count': 累積存款次數，型別為int，例：625
            'withdrawal_count': 累積提款次數，型別為int，例：150
            'deposit_amount': 累積存款金額，型別為str，例：'155171.79'
            'withdrawal_amount': 累積提款金額，型別為str，例：'121692.00'
        }
    }'''
    logger.info(f'準備查詢會員累積充值次數')
    resp = session.get(url + endpoints, params=params, headers=headers, timeout=timeout, verify=False)
    data = resp.json()
    if data['result'] is False:
        return {
            'IsSuccess': False,
            'ErrorCode': config.PARAMETER_ERROR.code,
            'ErrorMessage': f'{cf["platform"]} 显示：{data["message"]}',
            'RawStatusCode': resp.status_code,
            'RawContent': resp.content
        }
    return {
        'IsSuccess': True,
        'ErrorCode': config.SUCCESS_CODE.code,
        'ErrorMessage': config.SUCCESS_CODE.msg,
        'RawStatusCode': resp.status_code,
        'RawContent': resp.content,
        'Data': data['data']
    }


@log_info
@catch_exception
def users_bank_account(cf, url: str, params: dict, headers: dict = {},
                       timeout: tuple = (10, 20), endpoints: str = 'hex/users/bank_account', **kwargs) -> dict:
    '''BBIN 會員綁定銀行
    Args:
        url : (必填) BBIN後台網址，範例格式 'https://jsj888.33443356.com/'
        params : (必填) BBIN API 網址參數, 使用字典傳入, 範例格式 {
            'users[]': [772332969]      # 查詢的帳號ID (必須與headers中的referer相同)
        }
        headers : BBIN API 表頭, 使用字典傳入, 預設為: {
            'permname': 'UserDetailInfo',
            # referer必須與params中的userId相同
            'referer': 'https://wyma.1629yl.com/vi/user/772332969/detail_info'
        }
    Returns:{
        'IsSuccess': 是(登入成功/布林值)否(登入失敗/布林值),
        'ErrorCode': 錯誤代碼(如果前述布林值為False才有內容/文字格式)
        'ErrorMessage': 錯誤原因(如果前述布林值為False才有內容/文字格式)
        'RawStatusCode': BBIN原始回傳狀態碼
        'RawContent': BBIN原始回傳內容
    }'''
    resp = session.get(url + endpoints, params=params, headers=headers, verify=False)
    data = resp.json()
    if data['result'] is False:
        return {
            'IsSuccess': False,
            'ErrorCode': config.PARAMETER_ERROR.code,
            'ErrorMessage': f'{cf["platform"]} 显示：{data["message"]}',
            'RawStatusCode': resp.status_code,
            'RawContent': resp.content
        }
    return {
        'IsSuccess': True,
        'ErrorCode': config.SUCCESS_CODE.code,
        'ErrorMessage': config.SUCCESS_CODE.msg,
        'RawStatusCode': resp.status_code,
        'RawContent': resp.content,
        'Data': {str(d['user_id']): d for d in data['data']}
    }


@log_info
@catch_exception
def users_detail(cf, url: str, params: dict = {}, headers: dict = {},
                 timeout: tuple = (10, 20), endpoints: str = 'hex/user/772332969/detail', **kwargs) -> dict:
    '''BBIN 會員真實姓名
    Args:
        url : (必填) BBIN後台網址，範例格式 'https://jsj888.33443356.com/'
        params : (必填) BBIN API 網址參數, 使用字典傳入, 範例格式 {}
        headers : BBIN API 表頭, 使用字典傳入, 預設為: {
            'permname': 'UserDetailInfo',
            # 需要與endpoints中的userID相同
            'referer': 'https://wyma.1629yl.com/vi/user/772332969/detail_info'
        }
    Returns:{
        'IsSuccess': 是(登入成功/布林值)否(登入失敗/布林值),
        'ErrorCode': 錯誤代碼(如果前述布林值為False才有內容/文字格式)
        'ErrorMessage': 錯誤原因(如果前述布林值為False才有內容/文字格式)
        'RawStatusCode': BBIN原始回傳狀態碼
        'RawContent': BBIN原始回傳內容
    }'''
    resp = session.get(url + endpoints, params=params, headers=headers, verify=False)
    data = resp.json()
    if data['result'] is False:
        return {
            'IsSuccess': False,
            'ErrorCode': config.PARAMETER_ERROR.code,
            'ErrorMessage': f'{cf["platform"]} 显示：{data["message"]}',
            'RawStatusCode': resp.status_code,
            'RawContent': resp.content
        }
    return {
        'IsSuccess': True,
        'ErrorCode': config.SUCCESS_CODE.code,
        'ErrorMessage': config.SUCCESS_CODE.msg,
        'RawStatusCode': resp.status_code,
        'RawContent': resp.content,
        'Data': data['data']
    }


@log_info
@catch_exception
def users_detail_permission(cf, url: str, params: dict = {}, headers: dict = {},
    timeout: tuple = (10, 20), endpoints: str = 'hex/session', **kwargs) -> dict:
    '''BBIN 會員詳細資料 權限查詢'''
    resp = session.get(url + endpoints, params=params, headers=headers, verify=False)
    data = resp.json()
    if data['result'] is False:
        return {
            'IsSuccess': False,
            'ErrorCode': config.PARAMETER_ERROR.code,
            'ErrorMessage': f'{cf["platform"]} 显示：{data["message"]}',
            'RawStatusCode': resp.status_code,
            'RawContent': resp.content
        }
    return {
        'IsSuccess': True,
        'ErrorCode': config.SUCCESS_CODE.code,
        'ErrorMessage': config.SUCCESS_CODE.msg,
        'RawStatusCode': resp.status_code,
        'RawContent': resp.content,
        'Data': data['data']
    }


@log_info
@catch_exception
def login_record_info_auto(cf, url: str, params: dict, headers: dict = {},
                           timeout: tuple = (10, 20), endpoints: str = 'user/login_record_info/auto', **kwargs) -> dict:
    '''BBIN 自動稽核
    Args:
        url : (必填) BBIN後台網址，範例格式 'https://jsj888.33443356.com/'
        params : (必填) BBIN API 網址參數, 使用字典傳入, 範例格式 {
            'AccName': 'zdg71274',          # 帳號名稱
            'StartDate': '2021-04-10',      # 搜尋起始日期
            'EndDate': '2021-06-08',        # 搜尋結束日期
            'show': '100',                  # 顯示筆數
            'page': 1,                      # 搜尋頁面
        }
        headers : BBIN API 表頭, 使用字典傳入, 預設為: {}
    Returns:{
        'IsSuccess': 是(登入成功/布林值)否(登入失敗/布林值),
        'ErrorCode': 錯誤代碼(如果前述布林值為False才有內容/文字格式)
        'ErrorMessage': 錯誤原因(如果前述布林值為False才有內容/文字格式)
        'RawStatusCode': BBIN原始回傳狀態碼
        'RawContent': BBIN原始回傳內容
    }'''
    resp = session.get(url + endpoints, params=params, headers=headers, timeout=timeout, verify=False)
    # 捕捉權限未開啟
    if resp.text.strip() == '没有开放此功能权限':
        return {
            'IsSuccess': False,
            'ErrorCode': config.PERMISSION_CODE.code,
            'ErrorMessage': config.PERMISSION_CODE.msg.format(platform=cf.platform),
            'RawStatusCode': resp.status_code,
            'RawContent': resp.content
        }
    # 捕捉被登出
    alert_main = resp.html.find('.alert-main', first=True)
    if alert_main:
        return {
            'IsSuccess': False,
            'ErrorCode': config.SIGN_OUT_CODE.code,
            'ErrorMessage': config.SIGN_OUT_CODE.msg.format(platform=cf.platform),
            'RawStatusCode': resp.status_code,
            'RawContent': resp.content
        }
    # 捕捉日期錯誤
    alert_danger = resp.html.find('div.alert.alert-danger.alert-minpadding', first=True)
    alert_warning = resp.html.find('div.alert.alert-warning', first=True)
    table = resp.html.find('table', first=True)
    if not table:
        total_page = 1
        content = []
    else:
        columns = [th.text for th in table.find('thead > tr > th')]
        total_page = resp.html.find('ul.pagination > li:last-child', first=True)
        total_page = int(total_page.text) if total_page else 1
        content = [
            {
                col: td.text
                for col, td in zip(columns, tr.find('td'))
            }
            for tr in table.find('tbody > tr')
        ]
    return {
        'IsSuccess': True,
        'ErrorCode': config.SUCCESS_CODE.code,
        'ErrorMessage': config.SUCCESS_CODE.msg,
        'RawStatusCode': resp.status_code,
        'RawContent': resp.content,
        'Data': {
            'alert_danger': alert_danger.text if alert_danger else '',
            'alert_warning': alert_warning.text if alert_warning else '',
            'total_page': total_page,
            'content': content
        }
    }



@log_info(log_args=False, log_result=False)
@catch_exception
def login_record_info_mobile(cf, url: str, params: dict, headers: dict = {},
    timeout: tuple = (10, 20), endpoints: str = 'user/login_record_info/mobile',**kwargs) -> dict:
    '''BBIN 登入纪录
    Args:
        url : (必填) BBIN後台網址，範例格式 'https://jsj888.33443356.com/'
        params : (必填) BBIN API 網址參數, 使用字典傳入, 範例格式 {
            'Role': '1',                 # 查詢層級, 0:全部, 7:廳主, 5:大股東, 4:股東, 3:總代理, 2:代理商, 1:會員
            'LoginSource': 'APP',        # 登入來源, APP:手機App, MOBILEWEB:手機網頁板
            'Username': '',              # 帳號, 不輸入則查詢全部
            'StartDate': '2021-09-16',   # 時間區間-起始日期
            'EndDate': '2021-09-16',     # 時間區間-結束日期
            'show': '100',               # 每頁筆數, 必須要輸入 10, 20, 50, 100
        }
        headers : BBIN API 表頭, 使用字典傳入, 預設為: {}
    Returns:{
        'IsSuccess': 是(登入成功/布林值)否(登入失敗/布林值),
        'ErrorCode': 錯誤代碼(如果前述布林值為False才有內容/文字格式)
        'ErrorMessage': 錯誤原因(如果前述布林值為False才有內容/文字格式)
        'RawStatusCode': BBIN原始回傳狀態碼
        'RawContent': BBIN原始回傳內容
    }'''
    resp = session.get(url + endpoints, params=params, headers=headers, timeout=timeout, verify=False)
    # 捕捉權限未開啟
    if resp.text.strip() == '没有开放此功能权限':
        return {
            'IsSuccess': False,
            'ErrorCode': config.PERMISSION_CODE.code,
            'ErrorMessage': config.PERMISSION_CODE.msg.format(platform=cf.platform),
            'RawStatusCode': resp.status_code,
            'RawContent': resp.content
        }
    # 捕捉被登出
    alert_main = resp.html.find('.alert-main', first=True)
    if alert_main:
        return {
            'IsSuccess': False,
            'ErrorCode': config.SIGN_OUT_CODE.code,
            'ErrorMessage': config.SIGN_OUT_CODE.msg.format(platform=cf.platform),
            'RawStatusCode': resp.status_code,
            'RawContent': resp.content
        }
    # 捕捉警告訊息
    alert_danger = resp.html.find('div.alert.alert-danger.alert-minpadding', first=True)
    alert_danger = alert_danger.text if alert_danger else ''
    alert_warning = resp.html.find('div.alert.alert-warning', first=True)
    alert_warning = alert_warning.text if alert_warning else ''
    # 讀取總頁數
    total_page = resp.html.find('ul.pagination > li:last-child', first=True)
    total_page = int(total_page.text) if total_page else 1
    # 讀取查詢結果
    columns = [th.text for th in resp.html.find('table th')]
    content = [
        dict(zip(columns, [td.text for td in tr.find('td')]))
        for tr in resp.html.find('table > tbody > tr')
    ]
    return {
        'IsSuccess': True,
        'ErrorCode': config.SUCCESS_CODE.code,
        'ErrorMessage': config.SUCCESS_CODE.msg,
        'RawStatusCode': resp.status_code,
        'RawContent': resp.content,
        'Data': {
            'alert_danger': alert_danger,
            'alert_warning': alert_warning,
            'total_page': total_page,
            'content': content
        }
    }


@log_info
@catch_exception
def login_record_info_mobile2(cf, url: str, params: dict, headers: dict = {},
    timeout: tuple = (10, 20), endpoints: str = 'user/login_record_info/mobile',**kwargs) -> dict:
    '''BBIN 登入纪录
    Args:
        url : (必填) BBIN後台網址，範例格式 'https://jsj888.33443356.com/'
        params : (必填) BBIN API 網址參數, 使用字典傳入, 範例格式 {
            'Role': '1',                 # 查詢層級, 0:全部, 7:廳主, 5:大股東, 4:股東, 3:總代理, 2:代理商, 1:會員
            'LoginSource': 'APP',        # 登入來源, APP:手機App, MOBILEWEB:手機網頁板
            'Username': '',              # 帳號, 不輸入則查詢全部
            'StartDate': '2021-09-16',   # 時間區間-起始日期
            'EndDate': '2021-09-16',     # 時間區間-結束日期
            'show': '100',               # 每頁筆數, 必須要輸入 10, 20, 50, 100
        }
        headers : BBIN API 表頭, 使用字典傳入, 預設為: {}
    Returns:{
        'IsSuccess': 是(登入成功/布林值)否(登入失敗/布林值),
        'ErrorCode': 錯誤代碼(如果前述布林值為False才有內容/文字格式)
        'ErrorMessage': 錯誤原因(如果前述布林值為False才有內容/文字格式)
        'RawStatusCode': BBIN原始回傳狀態碼
        'RawContent': BBIN原始回傳內容
    }'''
    resp = session.get(url + endpoints, params=params, headers=headers, timeout=timeout, verify=False)
    # 捕捉權限未開啟
    if resp.text.strip() == '没有开放此功能权限':
        return {
            'IsSuccess': False,
            'ErrorCode': config.PERMISSION_CODE.code,
            'ErrorMessage': config.PERMISSION_CODE.msg.format(platform=cf.platform),
            'RawStatusCode': resp.status_code,
            'RawContent': resp.content
        }
    # 捕捉被登出
    alert_main = resp.html.find('.alert-main', first=True)
    if alert_main:
        return {
            'IsSuccess': False,
            'ErrorCode': config.SIGN_OUT_CODE.code,
            'ErrorMessage': config.SIGN_OUT_CODE.msg.format(platform=cf.platform),
            'RawStatusCode': resp.status_code,
            'RawContent': resp.content
        }
    # 捕捉警告訊息
    alert_danger = resp.html.find('div.alert.alert-danger.alert-minpadding', first=True)
    alert_danger = alert_danger.text if alert_danger else ''
    alert_warning = resp.html.find('div.alert.alert-warning', first=True)
    alert_warning = alert_warning.text if alert_warning else ''
    # 讀取總頁數
    total_page = resp.html.find('ul.pagination > li:last-child', first=True)
    total_page = int(total_page.text) if total_page else 1
    # 讀取查詢結果
    columns = [th.text for th in resp.html.find('table th')]
    content = [
        dict(zip(columns, [td.text for td in tr.find('td')]))
        for tr in resp.html.find('table > tbody > tr')
    ]
    return {
        'IsSuccess': True,
        'ErrorCode': config.SUCCESS_CODE.code,
        'ErrorMessage': config.SUCCESS_CODE.msg,
        'RawStatusCode': resp.status_code,
        'RawContent': resp.content,
        'Data': {
            'alert_danger': alert_danger,
            'alert_warning': alert_warning,
            'total_page': total_page,
            'content': content
        }
    }


@log_info
@catch_exception
def user_msg_add(cf, url: str, params: dict = {}, data: dict = {}, headers: dict = {},
                 timeout: tuple = (10, 20), endpoints: str = 'agv3/callduck/api/user-msg/add', **kwargs) -> dict:
    '''BBIN 新增一般訊息
    Args:
        url : (必填) BBIN後台網址，範例格式 'https://jsj888.33443356.com/'
        params : BBIN API 網址參數, 使用字典傳入
        data : (必填) 查詢內容, 使用字典傳入, 範例格式 {
            'hallId': '',                       # 未知, BBIN本身即帶空值
            'type': '1',                        # 发布对象, 0:體系,2:層級
            'replySwitch': 'N',                 # 开放会员回覆
            'startTime': '2021-09-22 00:00:00', # 发布时间(美东)-開始日期
            'endTime': '2021-09-23 23:59:00',   # 发布时间(美东)-結束日期
            'title': '{"zh-tw":"測試主題","zh-cn":"測試主題","en":"測試主題","vi":"","ko":""}',     # 主題
            'content': '{"zh-tw":"測試內容","zh-cn":"測試內容","en":"測試內容","vi":"","ko":""}',   # 內容
        }
        headers : BBIN API 表頭, 使用字典傳入, 預設為: {}
    Returns:{
        'IsSuccess': 是(登入成功/布林值)否(登入失敗/布林值),
        'ErrorCode': 錯誤代碼(如果前述布林值為False才有內容/文字格式)
        'ErrorMessage': 錯誤原因(如果前述布林值為False才有內容/文字格式)
        'RawStatusCode': BBIN原始回傳狀態碼
        'RawContent': BBIN原始回傳內容
        'Data': {
            "status": "Y",
            "code": "",
            "message": "",
            "data": {
                "status": 1,
                "type": 1,
                "msgId": 148838,
                "startTime": "2021-09-22 00:00:00",
                "endTime": "2021-09-23 23:59:00",
                "title": {"en":"test","ko":"","vi":"","zh-cn":"test","zh-tw":"test"},
                "content": {"en":"test","ko":"","vi":"","zh-cn":"test","zh-tw":"test"},
                "urlTitle": [],
                "url": "",
                "replySwitch": "N",
                "userNameList": [],
                "recipient": []
            }
        }
    }'''
    resp = session.post(url + endpoints, params=params, data=data, headers=headers, timeout=timeout, verify=False)

    # 被登出
    if 'window.open' in resp.text:
        raise NotSignError('帐号已其他地方登入, 请重新登入')
    # 被登出2
    if 'System Error:#0000 ' in resp.text:
        raise NotSignError('帐号已其他地方登入, 请重新登入')
    # 被登出3
    if '請重新登入' in resp.text:
        raise NotSignError('帐号已其他地方登入, 请重新登入')
    # 被登出4
    if resp.status_code == 401:
        raise NotSignError('请重新登入, 謝謝')
    # 檢查權限不足
    if resp.status_code == 403:
        logger.info('respone status code: 403')
        return {
            'IsSuccess': False,
            'ErrorCode': config.PERMISSION_CODE.code,
            'ErrorMessage': config.PERMISSION_CODE.msg.format(platform=cf.platform),
            'RawStatusCode': resp.status_code,
            'RawContent': resp.content
        }
    # 檢查狀態碼
    if resp.status_code != 200:
        return {
            'IsSuccess': False,
            'ErrorCode': config['HTML_STATUS_CODE']['code'],
            'ErrorMessage': config['HTML_STATUS_CODE']['msg'].format(
                status_code=resp.status_code, platform=cf.platform),
            'RawStatusCode': resp.status_code,
            'RawContent': resp.content
        }
    # 檢查彈出視窗
    match = alert_pattern.search(resp.text)
    if match:
        return {
            'IsSuccess': False,
            'ErrorCode': config.DEPOSIT_FAIL.code,
            'ErrorMessage': config.DEPOSIT_FAIL.msg.format(platform=cf.platform, msg=match.group()),
            'RawStatusCode': resp.status_code,
            'RawContent': resp.content
        }

    # 檢查json回傳
    if resp.json().get('status') != 'Y':
        success = False
        code = config.UserMessageError.code
        msg = f"{resp.json().get('message', '')}({resp.json().get('code', '')})"
        message = config.UserMessageError.msg.format(platform=cf.platform, msg=msg)
    else:
        success = True
        code = config.SUCCESS_CODE.code
        message = config.SUCCESS_CODE.msg
    # 回傳
    return {
        'IsSuccess': success,
        'ErrorCode': code,
        'ErrorMessage': message,
        'Data': resp.json(),
        'RawStatusCode': resp.status_code,
        'RawContent': resp.content
    }


@log_info
@catch_exception
def user_msg_send(cf, url: str, params: dict = {}, data: dict = {}, headers: dict = {},
                  timeout: tuple = (10, 20), endpoints: str = 'agv3/callduck/api/user-msg/send', **kwargs) -> dict:
    '''BBIN 新增一般訊息
    Args:
        url : (必填) BBIN後台網址，範例格式 'https://jsj888.33443356.com/'
        params : BBIN API 網址參數, 使用字典傳入
        data : (必填) 查詢內容, 使用字典傳入, 範例格式 {
            'hallId': '',
            'msgId': '148838',
            'recipient': '{"1":[721604757],"2":[],"3":[],"4":[],"5":[],"6":[],"7":[]}',
        }
        headers : BBIN API 表頭, 使用字典傳入, 預設為: {}
    Returns:{
        'IsSuccess': 是(登入成功/布林值)否(登入失敗/布林值),
        'ErrorCode': 錯誤代碼(如果前述布林值為False才有內容/文字格式)
        'ErrorMessage': 錯誤原因(如果前述布林值為False才有內容/文字格式)
        'RawStatusCode': BBIN原始回傳狀態碼
        'RawContent': BBIN原始回傳內容
    }'''
    resp = session.post(url + endpoints, params=params, data=data, headers=headers, timeout=timeout, verify=False)

    # 被登出
    if 'window.open' in resp.text:
        raise NotSignError('帐号已其他地方登入, 请重新登入')
    # 被登出2
    if 'System Error:#0000 ' in resp.text:
        raise NotSignError('帐号已其他地方登入, 请重新登入')
    # 被登出3
    if '請重新登入' in resp.text:
        raise NotSignError('帐号已其他地方登入, 请重新登入')
    # 被登出4
    if resp.status_code == 401:
        raise NotSignError('请重新登入, 謝謝')
    # 檢查權限不足
    if resp.status_code == 403:
        logger.info('respone status code: 403')
        return {
            'IsSuccess': False,
            'ErrorCode': config.PERMISSION_CODE.code,
            'ErrorMessage': config.PERMISSION_CODE.msg.format(platform=cf.platform),
            'RawStatusCode': resp.status_code,
            'RawContent': resp.content
        }
    # 檢查狀態碼
    if resp.status_code != 200:
        return {
            'IsSuccess': False,
            'ErrorCode': config['HTML_STATUS_CODE']['code'],
            'ErrorMessage': config['HTML_STATUS_CODE']['msg'].format(
                status_code=resp.status_code, platform=cf.platform),
            'RawStatusCode': resp.status_code,
            'RawContent': resp.content
        }
    # 檢查彈出視窗
    match = alert_pattern.search(resp.text)
    if match:
        return {
            'IsSuccess': False,
            'ErrorCode': config.DEPOSIT_FAIL.code,
            'ErrorMessage': config.DEPOSIT_FAIL.msg.format(platform=cf.platform, msg=match.group()),
            'RawStatusCode': resp.status_code,
            'RawContent': resp.content
        }

    # 檢查json回傳
    if resp.json().get('status') != 'Y':
        success = False
        code = config.UserMessageError.code
        msg = f"{resp.json().get('message', '')}({resp.json().get('code', '')})"
        message = config.UserMessageError.msg.format(platform=cf.platform, msg=msg)
    else:
        success = True
        code = config.SUCCESS_CODE.code
        message = config.SUCCESS_CODE.msg
    # 回傳
    return {
        'IsSuccess': success,
        'ErrorCode': code,
        'ErrorMessage': message,
        'Data': resp.json(),
        'RawStatusCode': resp.status_code,
        'RawContent': resp.content
    }


@log_info
@catch_exception
def bbin_bet_history(cf, params: dict, url: str, timeout: tuple = (10, 20),
                     headers: dict = {}, endpoints: str = 'game/wagers_detail', **kwargs) -> dict:
    '''BB电子 注單詳細內容'''
    title = []
    content = []
    resp = session.get(url + endpoints, params=params, headers=headers, verify=False, timeout=timeout)
    # 被登出
    if 'window.open' in resp.text:
        raise NotSignError('帐号已其他地方登入, 请重新登入')
    # 被登出2
    if 'System Error:#0000 ' in resp.text:
        raise NotSignError('帐号已其他地方登入, 请重新登入')
    # 被登出3
    if '請重新登入' in resp.text:
        raise NotSignError('帐号已其他地方登入, 请重新登入')
    # 被登出4
    if resp.status_code == 401:
        raise NotSignError('请重新登入, 謝謝')
    # 檢查權限不足
    if resp.status_code == 403:
        logger.info('respone status code: 403')
        return {
            'IsSuccess': False,
            'ErrorCode': config.PERMISSION_CODE.code,
            'ErrorMessage': config.PERMISSION_CODE.msg.format(platform=cf.platform),
            'RawStatusCode': resp.status_code,
            'RawContent': resp.content
        }
    # 檢查狀態碼
    if resp.status_code != 200:
        return {
            'IsSuccess': False,
            'ErrorCode': config['HTML_STATUS_CODE']['code'],
            'ErrorMessage': config['HTML_STATUS_CODE']['msg'].format(
                status_code=resp.status_code, platform=cf.platform),
            'RawStatusCode': resp.status_code,
            'RawContent': resp.content
        }
    # 檢查彈出視窗
    match = alert_pattern.search(resp.text)
    if match:
        return {
            'IsSuccess': False,
            'ErrorCode': config['HTML_CONTENT_CODE']['code'],
            'ErrorMessage': f'{cf["platform"]} 显示：{match.group()}',
            'RawStatusCode': resp.status_code,
            'RawContent': resp.content
        }
    html = requests_html.HTML(html=resp.text)

    tbody = html.find('table[class="table table-hover table-bordered info"] > tbody')
    if not tbody:
        return {
            'IsSuccess': False,
            'ErrorCode': config.HTML_CONTENT_CODE.code,
            'ErrorMessage': config.HTML_CONTENT_CODE.msg.format(platform=cf.platform),
            'Data': '',
            'RawStatusCode': resp.status_code,
            'RawContent': resp.content
        }
    for tr in tbody[0].find('tr'):
        if len(tr.find('td')) == 2:
            title.append([td.text for td in tr.find('td')][0])
            content.append([td.text for td in tr.find('td')][1])
        else:
            return {
                'IsSuccess': False,
                'ErrorCode': config.HTML_CONTENT_CODE.code,
                'ErrorMessage': config.HTML_CONTENT_CODE.msg.format(platform=cf.platform),
                'Data': '',
                'RawStatusCode': resp.status_code,
                'RawContent': resp.content
            }

    # tr = html.find('table[class="table table-hover table-bordered info"] tbody > tr')
    # error_html_content = [td for td in tr if len(td.find('td')) != 2]
    # if error_html_content:
    #     return {
    #             'IsSuccess': False,
    #             'ErrorCode': config.HTML_CONTENT_CODE.code,
    #             'ErrorMessage': config.HTML_CONTENT_CODE.msg.format(platform=cf.platform),
    #             'Data': '',
    #             'RawStatusCode': resp.status_code,
    #             'RawContent': resp.content
    #         }
    # title = [td.find('td')[0].text for td in tr]
    # content = [td.find('td')[1].text for td in tr]
    # if len(title) != len(content):
    #     return {
    #             'IsSuccess': False,
    #             'ErrorCode': config.HTML_CONTENT_CODE.code,
    #             'ErrorMessage': config.HTML_CONTENT_CODE.msg.format(platform=cf.platform),
    #             'Data': '',
    #             'RawStatusCode': resp.status_code,
    #             'RawContent': resp.content
    #         }

    result = dict(zip(title, content))
    result['FreeGame'] = int('FreeGame' in resp.text)
    return {
        'IsSuccess': True,
        'ErrorCode': config.SUCCESS_CODE.code,
        'ErrorMessage': config.SUCCESS_CODE.msg,
        'Data': result,
        'RawStatusCode': resp.status_code,
        'RawContent': resp.content
    }

@log_info
@catch_exception
def pg_bet_history(cf, params: dict, data: dict, url: str = 'https://public-api.pgjazz.com/', timeout: tuple = (10, 20),
    headers: dict = {}, endpoints: str = 'web-api/operator-proxy/v1/History/GetBetHistory',**kwargs) -> dict:
    '''PG電子 注單詳細內容'''
    resp = session.post(url + endpoints, params=params, data=data, headers=headers, verify=False, timeout=timeout)
    if resp.status_code != 200:
        return {
            'IsSuccess': False,
            'ErrorCode': config['SIGN_OUT_CODE']['code'],
            'ErrorMessage': config['SIGN_OUT_CODE']['msg'].format(platform=cf.platform),
            'RawStatusCode': resp.status_code,
            'RawContent': resp.content
        }
    err = resp.json().get('err', {})
    if err and err.get('msg', '') == 'Invalid operator session':
        logger.info('PG电子显示：Invalid operator session(2001)')
        raise requests.ConnectionError()

    return {
        'IsSuccess': True,
        'ErrorCode': config['SUCCESS_CODE']['code'],
        'ErrorMessage': config['SUCCESS_CODE']['msg'],
        'Data': resp.json(),
        'RawStatusCode': resp.status_code,
        'RawContent': resp.content
    }


@log_info
@catch_exception
def cq9_bet_history(cf, data: dict = {}, params: dict = {}, url: str = 'https://detail.liulijing520.com/',timeout: tuple = (10, 20),
                    headers: dict = {}, endpoints: str = 'odh5/api/inquire/v1/db/wager', **kwargs) -> dict:
    '''CQ9電子 注單詳細內容'''
    resp = session.get(url + endpoints, data=data, params=params, headers=headers, verify=False, timeout=timeout)
    if resp.status_code != 200:
        return {
            'IsSuccess': False,
            'ErrorCode': config['SIGN_OUT_CODE']['code'],
            'ErrorMessage': config['SIGN_OUT_CODE']['msg'].format(platform=cf.platform),
            'RawStatusCode': resp.status_code,
            'RawContent': resp.content
        }

    return {
        'IsSuccess': True,
        'ErrorCode': config['SUCCESS_CODE']['code'],
        'ErrorMessage': config['SUCCESS_CODE']['msg'],
        'Data': resp.json(),
        'RawStatusCode': resp.status_code,
        'RawContent': resp.content
    }


@log_info
@catch_exception
# def jdb_bet_history(cf, data: dict, params: dict={}, url: str='https://asapi.jdb199.com/', timeout: tuple=(10, 20),
def jdb_bet_history(cf, data: dict, params: dict = {}, url: str = 'https://playerapi247.jdb199.com/',timeout: tuple = (10, 20),
    headers: dict = {},
    # endpoints: str='api/runDao',
    # endpoints: str='history/slot',
    endpoints: str = 'history/gameGroupType',
    **kwargs) -> dict:
    '''JDB電子 注單詳細內容'''
    # resp = session.post(url + endpoints, data=data, params=params, headers=headers, verify=False, timeout=timeout)
    # 保留後續若需要spin_data內資料內容使用以下方式解碼
    # import lzstring
    # resp.json()['data']['gamehistory']['spin_data'] = json.loads(
    #     lzstring.LZString().decompressFromBase64(
    #         resp.json()['data']['gamehistory']['spin_data']
    #     )
    # )
    resp = session.get(url + endpoints, data=data, params=params, headers=headers, verify=False, timeout=timeout)

    if resp.status_code != 200:
        return {
            'IsSuccess': False,
            'ErrorCode': config['SIGN_OUT_CODE']['code'],
            'ErrorMessage': config['SIGN_OUT_CODE']['msg'].format(platform=cf.platform),
            'RawStatusCode': resp.status_code,
            'RawContent': resp.content
        }

    return {
        'IsSuccess': True,
        'ErrorCode': config['SUCCESS_CODE']['code'],
        'ErrorMessage': config['SUCCESS_CODE']['msg'],
        'Data': resp.json(),
        'RawStatusCode': resp.status_code,
        'RawContent': resp.content
    }


# VIPLEVEL批次查詢
@log_info
@catch_exception
def users_vip_level(cf, url: str, VIPcategory = '版本1.5以前', params: dict = {}, headers: dict = {},
    timeout: tuple = (10, 20), endpoints: str = 'almond/api/vip/statistics/user/all/level',**kwargs) -> dict:
    '''BBIN VIPVEL查詢
    Args:
        url : (必填) BBIN後台網址，範例格式 'https://jsj888.33443356.com/'
        params : (必填) BBIN API 網址參數, 使用字典傳入, 範例格式 {
        }
        headers : BBIN API 表頭, 使用字典傳入, 預設為: {
            'permname': 'UserDetailInfo',
            # referer必須與url中的userId相同
            'referer': 'https://wyma.1629yl.com/vi/user/772332969/detail_info'
        }
    Returns:{
        'IsSuccess': 是(登入成功/布林值)否(登入失敗/布林值),
        'ErrorCode': 錯誤代碼(如果前述布林值為False才有內容/文字格式)
        'ErrorMessage': 錯誤原因(如果前述布林值為False才有內容/文字格式)
        'RawStatusCode': BBIN原始回傳狀態碼
        'RawContent': BBIN原始回傳內容
        'Data': {
            '會員帳號': {'VipLevel':ViP等級}
            '會員帳號': {'VipLevel':{終身:ViP等級,贵宾会:ViP等級,真人:ViP等級}}
        }
    }'''

    resp = session.get(url=url + endpoints, params=params, timeout=timeout, verify=False)
    # 被登出
    if 'window.open' in resp.text:
        raise NotSignError('帐号已其他地方登入, 请重新登入')
    # 被登出2
    if 'System Error:#0000 ' in resp.text:
        raise NotSignError('帐号已其他地方登入, 请重新登入')
    # 被登出3
    if '請重新登入' in resp.text:
        raise NotSignError('帐号已其他地方登入, 请重新登入')
    # 被登出4
    if resp.status_code == 401:
        raise NotSignError('请重新登入, 謝謝')
    # 檢查權限不足
    if resp.status_code == 403:
        logger.info('respone status code: 403')
        return {
            'IsSuccess': False,
            'ErrorCode': config.PERMISSION_CODE.code,
            'ErrorMessage': config.PERMISSION_CODE.msg.format(platform=cf.platform),
            'RawStatusCode': resp.status_code,
            'RawContent': resp.content
        }
    # 檢查狀態碼
    if resp.status_code != 200:
        return {
            'IsSuccess': False,
            'ErrorCode': config['HTML_STATUS_CODE']['code'],
            'ErrorMessage': config['HTML_STATUS_CODE']['msg'].format(
                status_code=resp.status_code, platform=cf.platform),
            'RawStatusCode': resp.status_code,
            'RawContent': resp.content
        }
    respdata = resp.json()
    data = {}
    if respdata['status'] != 'Y':
        return {
            'IsSuccess': False,
            'ErrorCode': config.HTML_CONTENT_CODE,
            'ErrorMessage': f'{cf["platform"]} 显示：{respdata["message"]}。{config.HTML_CONTENT_CODE.msg.format(platform=cf.platform)}',
            'RawStatusCode': resp.status_code,
            'RawContent': resp.content
        }
    logger.info(f'VIPcategory:{VIPcategory}')

    # 判別網站的VIP儲存類型
    if VIPcategory != '版本1.5以前':
        if VIPcategory:
            logger.info(f'PR6系統導入vip等級{VIPcategory}')
        else:
            VIPcategory = '终身累计打码'
            logger.info(f'PR6系統預設vip等級為终身累计打码')
    else:  # 舊的判斷版本1.5前沒有VIPcategory參數
        if 'bwin888' in url:
            VIPcategory = '贵宾会VIP等级'
        elif 'yin188' in url:
            VIPcategory = '终身累计打码'
        elif 'la357' in url:
            VIPcategory = 'VIP贵宾会等级'
        elif 'xj188' in url:
            VIPcategory = 'VIP天峰优越会等级'
        elif 'by001' in url:
            VIPcategory = 'VIP优越会'
        else:
            VIPcategory = '终身累计打码'

        logger.info(f"1.5版本以前--抓取VIP等级:{VIPcategory}")

    for member in respdata['data']['memberList']:
        for vip in member['vipList']:
            if vip['categoryName'] == VIPcategory:
                data[member['username']] = {}
                data[member['username']]['VipLevel'] = vip['vipName']
        if not member['username'] in data:
            data[member['username']] = {}
            data[member['username']]['VipLevel'] = '--'
    return {
        'IsSuccess': True,
        'ErrorCode': config.SUCCESS_CODE.code,
        'ErrorMessage': config.SUCCESS_CODE.msg,
        'RawStatusCode': resp.status_code,
        'RawContent': resp.content,
        'Data': data
    }