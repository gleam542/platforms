import datetime
import json
import logging
import re
from sys import platform
import requests
from bs4 import BeautifulSoup
import werdsazxc
from platforms.config import CODE_DICT as config
from .utils import (
    log_info,
    default_headers,
    catch_exception,
    NullError
)

logger = logging.getLogger('robot')


session = requests.Session()
session.login = False

@log_info
@catch_exception
def token(cf: dict, url: str, timeout: tuple=(60), **kwargs) -> dict:
    '''CD 取得token'''
    global session
    while True:
    # 取得token
        resp = session.get(url, headers=default_headers, timeout=30+timeout, verify=False)
        if 'var token' not in resp.text:
            try:
                r = re.search('(?<=document.cookie=").*(?=;path=/")', resp.text)
                newCookie = r.group()
                logger.info(f'●二次登入：{newCookie}')
                a, b = newCookie.split('=')
                session.cookies.set(a, b)        
                continue 
            except:
                return {
                    'IsSuccess': False,
                    'ErrorCode': config.CONNECTION_CODE.code,
                    'ErrorMessage':config.CONNECTION_CODE.msg,
                    'RawStatusCode': resp.status_code,
                    'RawContent': resp.content
                }                
        else:
            break

    if resp.status_code == 403:
        soup = BeautifulSoup(resp.text, 'lxml')
        if soup.find('title').text in ['IP 禁止','IP Block']:
            return {
                'IsSuccess': False,
                'ErrorCode': config.IP_CODE.code,
                'ErrorMessage':config.IP_CODE.msg.format(platform=cf.platform),
                'RawStatusCode': resp.status_code,
                'RawContent': resp.content
            }
        else:
            return {
                'IsSuccess': False,
                'ErrorCode': config.HTML_STATUS_CODE.code,
                'ErrorMessage':config.HTML_STATUS_CODE.msg.format(platform=cf.platform, status_code=resp.status_code),
                'RawStatusCode': resp.status_code,
                'RawContent': resp.content
            }
            
    elif resp.status_code != 200:
        return {
            'IsSuccess': False,
            'ErrorCode': config.HTML_STATUS_CODE.code,
            'ErrorMessage':config.HTML_STATUS_CODE.msg.format(platform=cf.platform, status_code=resp.status_code),
            'RawStatusCode': resp.status_code,
            'RawContent': resp.content
        }
    soup = BeautifulSoup(resp.text, 'lxml')
    if soup.find('title').text in ['IP 禁止','IP Block']:
        return {
            'IsSuccess': False,
            'ErrorCode': config.IP_CODE.code,
            'ErrorMessage':config.IP_CODE.msg.format(platform=cf.platform),
            'RawStatusCode': resp.status_code,
            'RawContent': resp.content
        }
    if soup.find_all('script'):
        a = soup.find_all('script')
        b = [str(i) for i in a if 'var token' in str(i)][0].split('\n')
        c = [i for i in b if 'var token' in i][0]
        session.rf_cs_rForm = c.split("var token = '")[1].split("';")[0]
        logger.info(f'●session.rf_cs_rForm: {session.rf_cs_rForm}')

        return {
            'IsSuccess' : True,
            'ErrorCode': config.SUCCESS_CODE.code,
            'ErrorMessage': config.SUCCESS_CODE.msg,
            'RawStatusCode': resp.status_code,
            'RawContent': resp.content
        }
    else:
        return {
            'IsSuccess': False,
            'ErrorCode': config.HTML_STATUS_CODE.code,
            'ErrorMessage':config.HTML_STATUS_CODE.msg.format(platform=cf.platform, status_code=resp.status_code),
            'RawStatusCode': resp.status_code,
            'RawContent': resp.content
        }

@log_info
@catch_exception
def login(cf: dict, url: str, acc: str, pw: str, otp: str, timeout: tuple=(60), endpoints: str='index/login', **kwargs) -> dict:
    '''CD 登入'''

    #清空cookies
    session.login = False
    session.cookies.clear()
    resp = token(cf, url, timeout)
    if not resp['IsSuccess']:
        return resp
    # 檢查帳密
    if not acc.isalnum():
        return {
            'IsSuccess': False,
            'ErrorCode': config['ACC_CODE']['code'],
            'ErrorMessage': config['ACC_CODE']['msg'],
        }
    # if not pw.isalnum():
    #     return {
    #         'IsSuccess': False,
    #         'ErrorCode': config['ACC_CODE']['code'],
    #         'ErrorMessage': config['ACC_CODE']['msg'],
    #    }

    #定義變數
    source = {
        'initAccLogin': 'true',
        'userEmail': acc,
        'userPassword': pw,
        'userlang': 'zh_hans',
        'totp': otp,
        'rf_cs_rForm_': session.rf_cs_rForm,
        'rememberMe': 'false',
    }

    # 登入
    resp = session.post(url + endpoints, data=source, headers=default_headers, timeout=30+timeout, verify=False)
    if resp.status_code != 200:
        return {
                'IsSuccess': False,
                'ErrorCode': config.HTML_STATUS_CODE.code,
                'ErrorMessage':config.HTML_STATUS_CODE.msg.format(platform=cf.platform, status_code=resp.status_code),
                'RawStatusCode': resp.status_code,
                'RawContent': resp.content
            }
    content = json.loads(resp.text)
    if not content.get('message'):
        return {
            'IsSuccess' : False,
            'ErrorCode': config.JSON_ERROR_CODE.code,
            'ErrorMessage': config.JSON_ERROR_CODE.msg.format(platform=cf.platform),
            'RawStatusCode': resp.status_code,
            'RawContent': resp.content
        }
    if content.get('message') in ['登录成功', '登陆成功'] or content.get('status') in ['success']:
        default_headers['x-csrf-token'] = session.rf_cs_rForm
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
            'ErrorCode': config.ACC_CODE.code,
            'ErrorMessage': f"【{cf.platform}】回傳內容：{content.get('message')}",
            'RawStatusCode': resp.status_code,
            'RawContent': resp.content
        }

@log_info
@catch_exception
def activation_token(cf: dict, url: str, timeout: tuple=(60), endpoints: str='home', **kwargs) -> dict:
    '''CD 保持連線'''
    resp = session.get(url+endpoints, headers=default_headers, timeout=30+timeout, verify=False)

    if resp.status_code >= 500 and resp.status_code < 600:
        return  {
            'IsSuccess' : False,
            'ErrorCode': config.CONNECTION_CODE.code,
            'ErrorMessage': f"【{cf.platform}】回傳內容：{f'status_code:{resp.status_code}'}",
            'RawStatusCode': resp.status_code,
            'RawContent': resp.content
        }
    elif resp.status_code != 200:
        try:
            # 預設狀態碼「301」
            r = re.search('(?<=document.cookie=").*(?=;path=/")', resp.text)
            newCookie = r.group()
            logger.info(f'●保持連線：{newCookie}')
            a, b = newCookie.split('=')
            session.cookies.set(a, b)   
        except:
            return  {
                'IsSuccess' : False,
                'ErrorCode': config.IP_CODE.code if resp.status_code == 403 else config.HTML_STATUS_CODE.code,
                'ErrorMessage': config.IP_CODE.msg.format(platform=cf.platform) if resp.status_code == 403 else f'{resp.status_code}异常, 请确认{cf.platform}网址、帐号、密码后再次尝试',
                'RawStatusCode': resp.status_code,
                'RawContent': resp.content
            }
    r = re.search('(?<=id="admin_id" value=").*(?="/>)', resp.text)
    if r:
        session.admin_id = r.group()
    soup = BeautifulSoup(resp.text,'lxml')
    if soup.find('title').text in ['IP 禁止','IP Block']:
        return {
            'IsSuccess': False,
            'ErrorCode': config.IP_CODE.code,
            'ErrorMessage':config.IP_CODE.msg.format(platform=cf.platform),
            'RawStatusCode': resp.status_code,
            'RawContent': resp.content
        }
    req = soup.find('p',{'style':'text-align: center;font-size: 25px;color: black'})
    if req and req.text == '游戏平台管理':
        return {
            'IsSuccess' : False,
            'ErrorCode': config.SIGN_OUT_CODE.code,
            'ErrorMessage': f'{cf.platform}状态登出,请重新登入',
            'RawStatusCode': resp.status_code,
            'RawContent': resp.content
        }
    else:
        return {
            'IsSuccess' : True,
            'ErrorCode': config.SUCCESS_CODE.code,
            'ErrorMessage': config.SUCCESS_CODE.msg,
            'RawStatusCode': resp.status_code,
            'RawContent': resp.content
        }

@log_info
@catch_exception
# def searchusername(cf: dict, url: str, params:dict, timeout: tuple=(60), endpoints: str='memberList/searchUsername', **kwargs) -> dict:
def searchusername(cf: dict, url: str, data:dict, timeout: tuple=(60), endpoints: str='memberList/processPasteUsername', **kwargs) -> dict:
    '''CD 查詢會員ID【会员管理 > 会员列表】'''
    data['rf_cs_rForm_'] = session.rf_cs_rForm
    # resp = session.get(url+endpoints, params=params, headers=default_headers, timeout=30+timeout, verify=False)
    resp = session.post(url+endpoints, data=data, headers=default_headers, timeout=30+timeout, verify=False)
    # [{"id":"170000127","username":"dlvip888"}]
    if resp.status_code != 200:
        raise NullError(f'status_code={resp.status_code}')

    if resp.text == 'null':
        raise NullError('搜尋會員回傳NULL')
    content = {'results': json.loads(resp.text)}
    return  {
            'IsSuccess' : True,
            'ErrorCode': config.SUCCESS_CODE.code,
            'ErrorMessage': config.SUCCESS_CODE.msg,
            'Data':content,
            'RawStatusCode': resp.status_code,
            'RawContent': resp.content
            }

@log_info
@catch_exception
def depositsave(cf, url: str, data: dict, timeout: tuple=(60), endpoints: str='batchAdjust/depositSave', **kwargs) -> dict:
    '''CD 充值【会员管理 > 会员列表 > 用户名 > 会员资料 > 人工存入】'''
    data['rf_cs_rForm_'] = session.rf_cs_rForm
    data['admin_password'] = session.pw
    try:
        resp = session.post(url+endpoints, data=data, headers=default_headers, timeout=30+timeout, verify=False)
        if resp.status_code != 200:
            return {
                    'IsSuccess' : False,
                    'ErrorCode': config.HTML_STATUS_CODE.code,
                    'ErrorMessage': f'{resp.status_code}异常, 请确认{cf.platform}确认是否到帐',
                    'RawStatusCode': resp.status_code,
                    'RawContent': resp.content
            }
        content = json.loads(resp.text)
        return {
            'IsSuccess': True,
            'ErrorCode': config.SUCCESS_CODE.code,
            'ErrorMessage': config.SUCCESS_CODE.msg,
            'Data': content,
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
def creditflowmanagementdata(cf, url: str, params: dict, timeout: tuple=(60), endpoints: str='report/creditFlowManagementData', **kwargs) -> dict:
    '''CD 確認充值狀態【报表管理 > 额度管理】'''
    resp = session.get(url+endpoints, params=params, headers=default_headers, timeout=30+timeout, verify=False)
    if resp.status_code != 200:
        raise NullError(f'status_code={resp.status_code}')
    content = json.loads(resp.text)
    if content.get('data') is None:
        return {
            'IsSuccess': False,
            'ErrorCode': config.SIGN_OUT_CODE.code,
            'ErrorMessage': config.SIGN_OUT_CODE.msg.format(platform=cf.platform),
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
def memberlist_all(cf, url: str, data: dict, timeout: tuple=(60), endpoints: str='memberList/all', **kwargs) -> dict:
    '''CD 查詢會員層級、等級【会员管理 > 会员列表】'''
    data['rf_cs_rForm_'] = session.rf_cs_rForm
    resp = session.post(url+endpoints, data=data, headers=default_headers, timeout=30+timeout, verify=False)
    # {"draw":2,"recordsFiltered":1,"players":"170000127","data":[{"id":"170000127","affiliate_name":"hga","username":"dlvip888","full_name":"\u5927\u9646","mobile_number":"13800138888","member_level":"\u8001\u4f1a\u5458","vip_level":"\u94dc\u5361","status":"1","created_at":"1604136736","wallet_bal":"104936.8970","game_wallet_bal":"2.4100","affiliate_id":"170000122","last_login_time":"1624205381","rebate_bal":"0.00","sub_vip_level":[{"group_id":"3","vip_level_id":"34","vip_name":"VIP0","group_name":"\u7535\u5b50\u9ec4\u94bb"},{"group_id":"4","vip_level_id":"85","vip_name":"VIP0","group_name":"\u68cb\u724c\u9752\u94bb"},{"group_id":"5","vip_level_id":"136","vip_name":"VIP0","group_name":"\u6355\u9c7c\u84dd\u94bb"},{"group_id":"6","vip_level_id":"187","vip_name":"VIP0","group_name":"\u4f53\u80b2\u7eff\u94bb"},{"group_id":"7","vip_level_id":"238","vip_name":"VIP0","group_name":"\u771f\u4eba\u7ea2\u94bb"},{"group_id":"8","vip_level_id":"239","vip_name":"VIP0","group_name":"\u5f69\u7968\u7d2b\u94bb"},{"group_id":"9","vip_level_id":"240","vip_name":"VIP0","group_name":"\u7535\u7ade\u6a59\u94bb"}]}],"bench":{"start":1624249184.734103,"redis init":0.0012011528015136719,"gameWallet":0.0032830238342285156,"where":"WHERE player_user.is_affiliate=? AND player_user.id  in (?) AND  player_user.id >= 0","countQuery":"SELECT COUNT(1) FROM player_user  WHERE player_user.is_affiliate=? AND player_user.id  in (?) AND  player_user.id >= 0","countQueryBinding":["0","170000127"],"b4countqueryTime":0.003350973129272461,"b4count2queryTime":0.004622936248779297,"b4queryTime":0.00515294075012207,"query":"SELECT player_user.id as id,IF(apu.username IS NULL or apu.username = \"\", \"-\", apu.username) as affiliate_name,player_user.username as username,player_user.full_name as full_name,player_user.mobile_number as mobile_number,player_group.name as member_level,player_vip.name as vip_level,player_user.status as status,player_user.created_at as created_at,COALESCE(player_wallet.wallet_bal,0) as wallet_bal,(SELECT COALESCE(sum(DISTINCT wallet_balance),0) FROM player_game_details where player_id=player_user.id AND game_wallet_id IN (2,9,15,17,19,21,22,23,24,110,112,113,114,115,116,117,118,120,124,125,126,143,144,153)) as game_wallet_bal,player_user.affiliate_id as affiliate_id,player_user.last_login_time as last_login_time FROM player_user LEFT JOIN player_user apu ON player_user.affiliate_id = apu.id LEFT JOIN player_group ON player_user.player_group_id = player_group.id LEFT JOIN player_vip ON player_user.player_vip_id = player_vip.id LEFT JOIN player_wallet ON player_user.id = player_wallet.player_id WHERE player_user.is_affiliate=? AND player_user.id  in (?) AND  player_user.id >= 0  ORDER BY `id` DESC LIMIT 0, 25","queryTime":0.006381988525390625,"afterRebateTime":0.02031397819519043,"afterSubTime":0.02614307403564453}}
    if resp.status_code != 200:
        raise NullError(f'status_code={resp.status_code}')
    content = json.loads(resp.text)
    if 'data' not in content:
        return {
            'IsSuccess': False,
            'ErrorCode': config.JSON_ERROR_CODE.code,
            'ErrorMessage': config.JSON_ERROR_CODE.msg.format(platform=cf.platform),
            'Data': content,
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
def playerBankDetails_all(cf, url: str, params: dict, timeout: tuple=(60), endpoints: str='playerBankDetails/all', **kwargs) -> dict:
    '''CD 查詢綁定銀行卡【会员管理 > 会员列表 > 会员资料 > 检测银行完整资讯】'''
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
    '''CD 查詢IP【风控管理 > IP查询】'''
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
    '''CD 查詢累積存款金額&次數【报表管理 > 会员报表】'''
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
def betsdata(cf, url: str, data: dict, timeout: tuple=(60), endpoints: str='report/betsData', **kwargs) -> dict:
    '''CD 取得注單內容【报表管理 > 投注记录】'''
    resp = session.get(url+endpoints, params=data, headers=default_headers, timeout=30+timeout, verify=False)
    if resp.status_code != 200:
        raise NullError(f'status_code={resp.status_code}')
    content = json.loads(resp.text)
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
def getgameplayhistorygrandtotal(cf, url: str, data: dict, timeout: tuple=(60), endpoints: str='report/getGamePlayHistoryGrandTotal', **kwargs) -> dict:
    '''CD 取得投注總金額【报表管理 > 投注记录】'''
    resp = session.get(url+endpoints, params=data, headers=default_headers, timeout=30+timeout, verify=False)
    if resp.status_code != 200:
        raise NullError(f'status_code={resp.status_code}')
    content = json.loads(resp.text)
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
def getMemberGamePlayHistoryTotal(cf, url: str, data: dict, timeout: tuple=(60), endpoints: str='report/getMemberGamePlayHistoryTotal', **kwargs) -> dict:
    '''CD 取得投注總金額【报表管理 > 会员报表 > 平台】'''
    resp = session.get(url+endpoints, params=data, headers=default_headers, timeout=30+timeout, verify=False)
    if resp.status_code != 200:
        raise NullError(f'status_code={resp.status_code}')
    content = json.loads(resp.text)
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
def game_trans_id(cf, url:str, data:dict, timeout:tuple=(60), endpoints: str='report/betDetails/game_trans_id/', **kwargs) -> dict:
    '''CD 取得麻將遊戲網址【报表管理 > 投注记录 > 投注明细】'''
    resp = session.get(url + endpoints + f"{data['game_trans_id']}/game_id/{data['game_id']}/player_id/{data['player_id']}", headers=default_headers, timeout=30+timeout, verify=False)
    if resp.status_code != 200:
        raise NullError(f'status_code={resp.status_code}')
    
    res = re.search(r'(?<=openGame\(")https://.*?(?=")',resp.text)
    if res:
        return {
            'IsSuccess': True,
            'ErrorCode': config.SUCCESS_CODE.code,
            'ErrorMessage': config.SUCCESS_CODE.msg,
            'Data': res.group(),
            'RawStatusCode': resp.status_code,
            'RawContent': resp.content
        }
    else:
        raise NullError(f'status_code={resp.status_code}')

@log_info
@catch_exception
def getbethistory(cf, url:str, data:dict, timeout:tuple=(60), endpoints: str='https://public-api.pgcool.com/web-api/operator-proxy/v1/History/GetBetHistory?t=', **kwargs) -> dict:
    '''CD 取得麻將資料【报表管理 > 投注记录 > 投注明细】'''
    resp = session.post(endpoints + data['t'], data={'sid':data['sid'], 'gid':data['gid']}, timeout=30+timeout, verify=False)
    if resp.status_code != 200:
        raise NullError(f'status_code={resp.status_code}')
    
    content = json.loads(resp.content)
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
def searchGroup(cf, url: str, params: dict={}, headers=default_headers,
    timeout: tuple=(60), endpoints: str='playerGroup/searchGroup', **kwargs) -> dict:
    '''CD 查詢層級列表【会员管理 > 会员层级】'''
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
    '''CD 移動層級【会员管理 > 会员列表 > 会员资料 > 修改会员等级】'''
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
def messagestore(cf, url: str, data: dict, headers=default_headers,
                   timeout: tuple=(60), endpoints: str='message/store', **kwargs) -> dict:
    '''CD 推播通知【网站管理 > 信息管理 > 站内信 > 新增】'''
    resp = session.post(url+endpoints, data=data, headers=headers, timeout=30+timeout, verify=False)
    # {"success":true,"code":"S0189","message":"新增成功","data":[]}
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
                'ErrorCode': config.UserMessageError.code,
                'ErrorMessage': config.UserMessageError.msg.format(platform=cf.platform, msg=content['message']),
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
def accountEnquiryRecords(cf, url: str, params: dict, headers=default_headers,
                   timeout: tuple=(60), endpoints: str='ipAddressMonitoring/accountEnquiryRecords', **kwargs) -> dict:
    '''CD 查詢會員登錄【风控管理 > 帐号查询】'''

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
