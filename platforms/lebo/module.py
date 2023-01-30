from werdsazxc import log_trackback
from urllib.parse import quote, unquote, urlencode
from . import CODE_DICT as config
from .utils import (
    log_info,
    default_headers,
    catch_exception,
    alert_pattern,
    NotSignError,
    freespin_support,
    bettingbonus_support,
    support,
    NullError
)
import requests_html
import requests
import logging
import json
import re
import bs4
logger = logging.getLogger('robot')


session = requests.Session()
session.login = False



#【登入】
@log_info
@catch_exception
def login(cf, url: str, acc: str, pw: str, otp: str,  
          timeout: tuple = (3, 5), endpoints: str = 'app/daili/login.php', **kwargs) -> dict:
    '''LEBO 登入
    Args:
        url : LEBO後台網址，範例格式 'http://dvl.hpnzl.com/'
        acc : LEBO使用者帳號
        pw : LEBO使用者密碼
        retry_times : 選填, 連線異常時重試次數
    Returns:
    {
        'IsSuccess': 是(登入成功/布林值)否(登入失敗/布林值),
        'ErrorCode': 錯誤代碼(如果前述布林值為False才有內容/文字格式)
        'ErrorMessage': 錯誤原因(如果前述布林值為False才有內容/文字格式)
        'RawStatusCode': LEBO原始回傳狀態碼
        'RawContent': LEBO原始回傳內容
    }
    '''
    # 清空cookies
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

    # 定義變數
    source = urlencode({
        'active': 1,
        'uid': '',
        'langx': 'zh-tw',
        'flag': 1,
        'username': acc,
        'password': pw,
        'otp': '',
        'mtoken': otp,
        'Submit': '登  入'
    })

    # 登入
    resp = session.post(url + endpoints, data=source, headers=default_headers, verify=False, timeout=30)

    # 檢查回傳
    if resp.status_code == 404:
        return {
            'IsSuccess': False,
            'ErrorCode': config['HTML_STATUS_CODE']['code'],
            'ErrorMessage': config['HTML_STATUS_CODE']['msg'].format(platform=cf.platform, status_code=resp.status_code),
            'RawStatusCode': resp.status_code,
            'RawContent': resp.content
        }
    if resp.status_code == 401:
        return {
            'IsSuccess': False,
            'ErrorCode': config['PERMISSION_CODE']['code'],
            'ErrorMessage': config['PERMISSION_CODE']['msg'].format(platform=cf.platform),
            'RawStatusCode': resp.status_code,
            'RawContent': resp.content
        }
    if resp.status_code != 200:
        return {
            'IsSuccess': False,
            'ErrorCode': config['HTML_STATUS_CODE']['code'],
            'ErrorMessage': config['HTML_STATUS_CODE']['msg'].format(platform=cf.platform, status_code=resp.status_code),
            'RawStatusCode': resp.status_code,
            'RawContent': resp.content
        }
    if 'alert' in resp.text:
        alert = alert_pattern.search(resp.text).group()
        if 'Plaese check username/passwd and try again' in alert:
            alert = '帐号或密码错误'
        return {
            'IsSuccess': False,
            'ErrorCode': config['ACC_CODE']['code'],
            'ErrorMessage': f'{cf["platform"]} 显示：{alert}',
            'RawStatusCode': resp.status_code,
            'RawContent': resp.content
        }
    if 'chg_passwd.php' in resp.text:
        return {
            'IsSuccess': False,
            'ErrorCode': config['ACC_CODE']['code'],
            'ErrorMessage': f'{cf["platform"]} 显示：該密碼已久未變更, 為了安全起見, 請先變更密碼後再繼續使用!',
            'RawStatusCode': resp.status_code,
            'RawContent': resp.content
        }

    # 判斷都通過, 但是未獲得cookies, (網址打錯 or 未捕捉到的失敗訊息)
    #if {'game_cookie', 'PHPSESSID'} - session.cookies.get_dict().keys():
    if {'PHPSESSID'} - session.cookies.get_dict().keys():  #●修改
        return {
            'IsSuccess': False,
            'ErrorCode': config['EXCEPTION_CODE']['code'],
            'ErrorMessage': config['EXCEPTION_CODE']['msg'],
            'RawStatusCode': resp.status_code,
            'RawContent': resp.content
        }

    # 紀錄帳密
    session.url = url
    session.acc = acc
    session.pw = pw
    session.login = True

    a = resp.text.rfind('?')
    b = resp.text.rfind('&')
    session.uID = resp.text[(a+5):b]

    logger.info(f'cookies: {session.cookies.get_dict()}')
    # 登入成功回傳
    return {
        'IsSuccess' : True,
        'ErrorCode': config['SUCCESS_CODE']['code'],
        'ErrorMessage': config['SUCCESS_CODE']['msg'],
        'RawStatusCode': resp.status_code,
        'RawContent': resp.content
    }


#【會員層級】
@log_info
@catch_exception
def app_cash_utotal(cf, url: str, params: dict, data: dict={}, headers: dict={},
                    timeout: tuple=(3, 5), endpoints: str='app/cash/utotal.php', method='get', mod_name='', **kwargs) -> dict:
    '''LEBO【現金系統 >> 層級管理 >> 會員查詢】
    Args:
        url : (必填) LEBO 後台網址，範例格式: 'https://shaba888.jf-game.net/'
        params : (必填) LEBO API 網址參數, 使用字典傳入, 範例格式: {
            "username": "hm81448072",                   # 會員帳號
            "savebtn": "查詢"                           # 固定值
        }
        headers : LEBO API 表頭, 使用字典傳入, 預設為: {
            'Content-Type': 'application/x-www-form-urlencoded',
            'X-Requested-With': 'XMLHttpRequest'
        }
    Returns:
        'IsSuccess': 是(查詢成功/布林值)否(查詢失敗/布林值),
        'ErrorCode': 錯誤代碼(如果前述布林值為False才有內容/文字格式)
        'ErrorMessage': 錯誤原因(如果前述布林值為False才有內容/文字格式)
        'Data': 查詢的回傳資料, 字典格式, ex:
                {
                    "sha12": {
                        '會員編號': '45423834',
                        '會員帳號': 'hm81448072',
                        '代理帳號': 'e524app2',
                        '加入時間': '2017-12-04 13:11:42',
                        '最后登录': '2020-05-28 23:15:43',
                        '存款次數': '1366',
                        '存款總額': '149656.34',
                        '最大存款額度': '500.00',
                        '提款次數': '191',
                        '提款總額': '181926.00',
                        '所屬層級': '第四层'
                    }
                }
        'RawStatusCode': LEBO原始回傳狀態碼
        'RawContent': LEBO原始回傳內容
    '''
    headers = headers or default_headers
    params = params or {}

    # 查詢
    resp = getattr(session, method)(url + endpoints, headers=headers, params=params,data=data, verify=False, timeout=30)
    alert_search = alert_pattern.findall(resp.text)

    # 檢查回傳
    if 'logout.php' in resp.text:
        raise NotSignError('帐号已其他地方登入, 请重新登入')

    # 彈出失敗回傳(查詢時會回傳多個alert訊息在js中) or 修改層級時(session.post)只會走到這裡
    if len(alert_search) == 1:
        # 回傳失敗訊息
        if '操作權限' in resp.text:
            return {
                'IsSuccess': False,
                'ErrorCode': config['PERMISSION_CODE']['code'],
                'ErrorMessage': f'{cf["platform"]} 显示：{alert_search[0]}',
                'Data': {},
                'RawStatusCode': resp.status_code,
                'RawContent': resp.content
            }
        # 只有要修改層級時(session.post)才會出現
        elif '修改完成' in resp.text:
            return {
                'IsSuccess': True,
                'ErrorCode': config['SUCCESS_CODE']['code'],
                'ErrorMessage': '',
                'Data': {},
                'RawStatusCode': resp.status_code,
                'RawContent': resp.content
            }

        # else:
        #     return {
        #         'IsSuccess': False,
        #         'ErrorCode': config['SUCCESS_CODE']['code'],
        #         'ErrorMessage': f'{cf["platform"]} 显示：{alert_search[0]}',
        #         'Data': {},
        #         'RawStatusCode': resp.status_code,
        #         'RawContent': resp.content
        #     }

    # 狀態碼錯誤
    if resp.status_code != 200:
        return {
            'IsSuccess': False,
            'ErrorCode': config['HTML_STATUS_CODE']['code'],
            'ErrorMessage': config['HTML_STATUS_CODE']['msg'].format(platform=cf.platform, status_code=resp.status_code),
            'Data': {},
            'RawStatusCode': resp.status_code,
            'RawContent': resp.content
        }
        
    html = requests_html.HTML(html=resp.text)
    # 欄位
    columns = [th.text for th in html.find('tr.m_title > td')]
    # 內容(去掉表頭表尾)
    datas = [tr for tr in html.find('table.m_tab tr')][1:-1]
    # 回傳
    res = {}
    # 表格逐行檢查欄位數、轉為字典、存入res中
    for tr in datas:
        # 轉為字典
        dic = dict(zip(columns, [td.text for td in tr.find('td')]))
        # 存入res中
        dic['會員帳號'] = dic['會員帳號'].split('\n')[0]
        res[dic['會員帳號']] = dic
        # 層級欄位統一名稱為"所屬層級", 未來如LEBO改動欄位名稱, 只需修正以下code, 不必修正MISSION
        if not res[dic['會員帳號']].get('所屬層級'):
            res[dic['會員帳號']]['所屬層級'] = (
                                                tr.find('select > option[selected]', first=True) or
                                                tr.find('select > option', first=True)
                                                ).text
        
        # 取得體驗金移動層級API需要用的參數
        default_level = tr.find('td[align="center"] > select', first=True)
        default_level_number = (
                                tr.find('td[align="center"] > select > option[selected]', first=True) or
                                tr.find('td[align="center"] > select > option', first=True)
                                )
        level_name = default_level.attrs['name'] if default_level else None
        level_number = default_level_number.attrs['value'] if default_level_number else None

        hidden = tr.find('td[align="center"] > input[type="hidden"]', first=True)
        default_value = hidden.attrs['value'] if hidden else None
        default_name = hidden.attrs['name'] if hidden else None

        checkbox = tr.find('td[align="center"] > input[type="checkbox"]', first=True)
        lock_name = checkbox.attrs['name'] if checkbox else None
            
        res[dic['會員帳號']]['level_name'] = level_name
        res[dic['會員帳號']]['level_number'] = level_number 
        res[dic['會員帳號']]['default_value'] = default_value
        res[dic['會員帳號']]['default_name'] = default_name
        res[dic['會員帳號']]['lock_name'] = lock_name

        # 檢查欄位數
        if len(columns) != len(tr.find('td')):
            return {
                'IsSuccess': False,
                'ErrorCode': config['HTML_CONTENT_CODE']['code'],
                'ErrorMessage': config['HTML_CONTENT_CODE']['msg'].format(platform=cf.platform),
                'Data': res,
                'RawStatusCode': resp.status_code,
                'RawContent': resp.content
            }

    # 無回傳資料
    if not datas:
        return {
            'IsSuccess': False,
            'ErrorCode': config['NO_USER']['code'],
            'ErrorMessage': config['NO_USER']['msg'],            
            'Data': res,
            'RawStatusCode': resp.status_code,
            'RawContent': resp.content
        }

    # 成功回傳
    game_css = html.find('table.m_tab tr td[align="center"] > select > option')
    game_dict = {data.text:data.attrs['value'] for data in game_css}  # 遊戲代碼 {'xxx':'888'}
    res['game_dict'] = game_dict
    res['RawContent'] = resp.content
    return {
        'IsSuccess': True,
        'ErrorCode': config['SUCCESS_CODE']['code'],
        'ErrorMessage': config['SUCCESS_CODE']['msg'],
        'Data': res,
        'RawStatusCode': resp.status_code,
        'RawContent': resp.content
    }


# 【注單內容】
@log_info
@catch_exception
def app_mgame_bbin(cf, url: str, mod_name, data: dict, headers: dict = {},
                   timeout: tuple = (3, 5), endpoints: str = 'app/mgame/', **kwargs) -> dict:
    '''LEBO【現金系統 >> 第三方查詢 >> 第三方注單查詢】
    Args:
        url : (必填) LEBO 後台網址，範例格式: 'https://shaba888.jf-game.net/'
        data : (必填) LEBO API 網址參數, 使用字典傳入, 範例格式:
            {
            "account": "abc123",                  # 會員帳號
            "no": "517443637888",                 # 注單號碼
            "searchbtn": "查詢",                   # 固定值
            }
        headers : LEBO API 表頭, 使用字典傳入, 預設為: {
            'Content-Type': 'application/x-www-form-urlencoded',
            'X-Requested-With': 'XMLHttpRequest'
        }
    Returns:
    {
        'IsSuccess': 是(查詢成功/布林值)否(查詢失敗/布林值),
        'ErrorCode': 錯誤代碼(如果前述布林值為False才有內容/文字格式),
        'ErrorMessage': 錯誤原因(如果前述布林值為False才有內容/文字格式),
        'Data': {'注單': {
                            '游戏类别': '糖果派对-极速版', 
                            '注单': '532495929888', 
                            '时间': '2020-10-05 02:58:55', 
                            '基注(额度：分数)': '1:2', 
                            '下注资讯': '每注:5(元),共:1注\n总共:5(元)', 
                            '显示模式': '', 
                            '目前关数/剩余墙数': '1/33'
                         }, 
                  '注單明細': [
                                {
                                    '': '1', 
                                    '次數': '-', 
                                    '牌組資料': '', 
                                    '金額': '0.00'
                                }
                              ],
                  '派彩金額': '168.88'           
                }
        'RawStatusCode': LEBO原始回傳狀態碼,
        'RawContent': LEBO原始回傳內容
    }
    '''
    gamePhpList = ['bbin.php', 'cq.php', 'jdb.php']
    gamePhp = gamePhpList[0]
    gameList_spinlotto = [freespin_support[name].split(']')[1] for name in freespin_support]


    headers = headers or default_headers
    data = data or {}

    while True:
        print(f'★>>>>>>>>>>>>>>>>>>>>>> gamePhp: {gamePhp}')

        resp = session.post(url + endpoints + gamePhp, data=urlencode(data), headers=headers, verify=False, timeout=30, allow_redirects=False)
        resp.encoding = 'utf-8-sig'
        print(f'★>>>>>>>>>>>>>>>>>>>>>> resp.headers: {resp.headers}')

        res = {
            'CategoryName': '',
            'FreeSpin': 0, 
            '注單': {},
            '注單明細': [],
            '派彩金額': '',
        }

        result = {
            'IsSuccess': False,
            'ErrorCode': config['WAGERS_NOT_FOUND']['code'],
            'ErrorMessage': config['WAGERS_NOT_FOUND']['msg'],
            'Data': {},
            'RawStatusCode': resp.status_code,
            'RawContent': resp.content
        }

        alert_search = alert_pattern.findall(resp.text)

        # 檢查回傳
        if 'logout.php' in resp.text:
            raise NotSignError('帐号已其他地方登入, 请重新登入')

        # 彈出失敗回傳(查詢時會回傳多個alert訊息在js中)
        if len(alert_search) == 1:
            # 回傳失敗訊息
            if '操作權限' in resp.text:
                return {
                    'IsSuccess': False,
                    'ErrorCode': config['PERMISSION_CODE']['code'],
                    'ErrorMessage': f'{cf["platform"]} 显示：{alert_search[0]}',
                    'Data': {},
                    'RawStatusCode': resp.status_code,
                    'RawContent': resp.content
                }
            elif '注單編號不存在' in resp.text:
                # if '旋转注单' in  mod_name or '旋轉注單' in mod_name:
                if len(gamePhpList) >= 2:
                    gamePhpList.remove(gamePhp)
                    gamePhp = gamePhpList[0]
                    continue
                else:
                    return result

            return {
                'IsSuccess': False,
                'ErrorCode': config['SUCCESS_CODE']['code'],
                'ErrorMessage': f'{cf["platform"]} 显示：{alert_search[0]}',
                'Data': {},
                'RawStatusCode': resp.status_code,
                'RawContent': resp.content
            }

        # # 狀態碼錯誤
        # if resp.status_code != 200:
        #     return {
        #         'IsSuccess': False,
        #         'ErrorCode': config['HTML_STATUS_CODE']['code'],
        #         'ErrorMessage': config['HTML_STATUS_CODE']['msg'].format(platform=cf.platform, status_code=resp.status_code),
        #         'Data': {},
        #         'RawStatusCode': resp.status_code,
        #         'RawContent': resp.content
        #     }

        try:
            if 'location' in resp.headers:  
                # ../daili/adminsave/bet_record_v2/bbin.php?uid=53391020937b39d28846e9f129fa&mid=63072039&gtype=&no=561059378781
                rLoc = resp.headers["location"]
                a = rLoc.find('?')
                rLocA = rLoc[(a+1):]

                print(f'★>>>>>>>>>>>>>>>>>>>>>> rLocA: {rLocA}')

                # uid=53391020937b39d28846e9f129fa&mid=63072039&gtype=&no=561059378781
                a = rLocA.split('&')
                z = {}
                for i in a:
                    b = i.split('=')
                    z[b[0]] = b[1]
                try:
                    session.typeid = z['gtype']
                except:
                    session.typeid = ''

                print(f'★★★>>>>>>>>>>>>>>>>>>>>>> session.typeid: {session.typeid}')              

                if gamePhp == 'bbin.php':
                    urlA = url + 'app/daili/adminsave/bet_record_v2/bbin.php?' + rLocA
                    resp = session.get(urlA, headers=headers, verify=False, timeout=30)
                    resp.encoding = 'utf-8-sig'


                    html = requests_html.HTML(html=resp.text)

                    tables = html.find('table')

                    if tables:
                        res['CategoryName'] = 'BBIN电子'

                        x = resp.text.split('&nbsp;')
                        x1 = '游戏类别：'
                        for i in x:
                            if x1 in i:
                                gameType = i.replace(x1, '')
                                res['注單']['游戏类别'] = gameType.strip()
                                break

                        for tr in tables[0].find('tr'):
                            res['注單'][tr.find('td')[0].text] = tr.find('td')[1].text

                        columns = ['', '次數', '牌組資料', '金額']

                        try:
                            #for tr in tables[1].find('tr:not(.title-box):not(.total):not(.result)'):
                            for tr in tables[1].find('tr'):       
                                tds = tr.find('td')
                                #tds = [td.text for td in tds]
                                tds = [td.text for td in tds if td.text != '']                
                                if len(tds) == 4:
                                    res['注單明細'].append(dict(zip(columns, tds)))
                                    # {'注單': {'游戏类别': '糖果派对-极速版', '注单': '532495929888', '时间': '2020-10-05 02:58:55', '基注(额度：分数)': '1:2', '下注资讯': '每注:5(元),共:1注\n总共:5(元)', '显示模式': '', '目前关数/剩余墙数': '1/33'}, '注單明細': [{'': '1', '次數': '-', '牌組資料': '', '金額': '0.00'}]}

                                if tds and ' - ' in tds[0] and '=' in tds[0]:
                                    r = tds[0].split('=')
                                    m = r[1].strip()
                                    if not m.isalpha():
                                        res['派彩金額'] = eval(m)
                        except Exception as e:
                            print(f'>>>>>>>>>>>>>>>>>>>>>>>>>>>>> app_mgame_bbin 錯誤1：{e}')


                        if '旋转注单' in  mod_name or '旋轉注單' in mod_name:
                            return {
                                'IsSuccess': False,
                                'ErrorCode': config['CATEGORY_ERROR']['code'],
                                'ErrorMessage': config['CATEGORY_ERROR']['msg'].format(CategoryName=res['CategoryName']),
                                'Data': {},
                                'RawStatusCode': resp.status_code,
                                'RawContent': resp.content
                            }

                        # 回傳結果
                        return {
                            'IsSuccess': True,
                            'ErrorCode': config['SUCCESS_CODE']['code'],
                            'ErrorMessage': config['SUCCESS_CODE']['msg'],
                            'Data': res,
                            'RawStatusCode': resp.status_code,
                            'RawContent': resp.content
                        }

                    else:
                        # # 没有此注单记录！
                        # # 注单与会员帐号不符！(player68513647/player28646622)
                        # if '</html>' not in resp.text:
                        #     if 'ERROR:2' in resp.text:
                        #         erroMsg = '会员帐号格式不正确'
                        #     else:
                        #         erroMsg = f'{cf["platform"]} 显示：{resp.text}'
                        # else:
                        #     erroMsg = '查无注单内容'

                        if '没有此注单记录' in resp.text:
                            # if '旋转注单' in  mod_name or '旋轉注單' in mod_name:
                            if len(gamePhpList) >= 2:       
                                gamePhpList.remove(gamePhp)
                                gamePhp = gamePhpList[0]
                                continue
                            else:
                                return result

                        elif '注单与会员帐号不符' in resp.text:
                            result['ErrorCode'] = config['USER_WAGERS_NOT_MATCH']['code']
                            result['ErrorMessage'] = config['USER_WAGERS_NOT_MATCH']['msg']

                        return result

                elif gamePhp == 'cq.php':
                    cq9_url = rLoc.split('/?token')[0]
                    cq9_data = {
                            'token':rLoc.split('token=')[1].split('&language')[0],
                            'language':'zh-cn',
                            }
                    rCQ9 = cq9(cq9_url, cq9_data)
                    # {'data': {'account': 'player63072149', 'parentacc': 'lebozd', 'actionlist': [{'action': 'bet', 'amount': 10, 'eventtime': '2021-03-23T21:38:45-04:00'}, {'action': 'win', 'amount': 40, 'eventtime': '2021-03-23T21:38:57-04:00'}], 'detail': {'wager': {'seq_no': '548802862755', 'order_time': '2021-03-23T21:38:57-04:00', 'end_time': '2021-03-23T21:38:57-04:00', 'user_id': '5dcbfb0bbf94660001ceb4a6', 'game_id': '52', 'platform': 'web', 'currency': 'CNY', 'start_time': '2021-03-23T21:38:45-04:00', 'server_ip': '10.9.16.16', 'client_ip': '10.9.16.46', 'play_bet': '10', 'play_denom': '1', 'bet_multiple': '20', 'rng': [88, 119, 26, 95, 70], 'multiple': '1', 'base_game_win': '40', 'win_over_limit_lock': 0, 'game_type': 0, 'win_type': 1, 'settle_type': 0, 'wager_type': 0, 'total_win': '40', 'win_line_count': 1, 'bet_tid': 'pro-bet-548802862755', 'win_tid': 'pro-win-548802862755', 'proof': {'win_line_data': [{'line_extra_data': [0], 'line_multiplier': 1, 'line_prize': 4000, 'line_type': 0, 'symbol_id': '13', 'symbol_count': 6, 'num_of_kind': 5, 'win_line_no': 0, 'win_position': [[1, 0, 1, 2, 1], [0, 1, 0, 0, 0], [0, 0, 0, 1, 0]]}], 'symbol_data': ['13,4,13,W,13', '12,13,4,3,F', 'F,F,14,13,14'], 'symbol_data_after': [], 'extra_data': [0], 'lock_position': [], 'reel_pos_chg': [0], 'reel_len_change': [], 'reel_pay': [], 'respin_reels': [0, 0, 0, 0, 0], 'bonus_type': 0, 'special_award': 0, 'special_symbol': 0, 'is_respin': False, 'fg_times': 0, 'fg_rounds': 0, 'next_s_table': 0, 'extend_feature_by_game': [], 'extend_feature_by_game2': []}, 'sub': [], 'pick': []}}}, 'status': {'code': '0', 'message': 'Success', 'datetime': '2021-04-06T06:06:59.219-04:00'}}
                    print(f'★>>>>>>>>>>>>>>>>>>>>>> rCQ9: {rCQ9}')              
                    if rCQ9 == False:
                        return result
                    else:
                        res['CategoryName'] = 'CQ9电子' 

                        #●抓遊戲總表
                        bs = bs4.BeautifulSoup(resp.text, 'html.parser')
                        r = bs.select('select')
                        gameCodeList = {}
                        for i in r[1]:
                            if len(str(i).strip()) != 0:
                                gameCodeList[i['value']] = i.text.split('-')[1].strip()
                      
                        for i in rCQ9['data']['actionlist']:
                            if i['action'] == 'bet':
                                res['注單']['下注资讯'] = f"总共:{i['amount']}(元)"
                                res['注單']['时间'] = i['eventtime'].replace('T', ' ').replace('-04:00', '').strip()  # 押注时间                  
                            elif i['action'] == 'win':
                                res['派彩金額'] = i['amount']
                                # res['注單']['时间'] = i['eventtime'].replace('T', ' ').replace('-04:00', '').strip()  # 成单时间                    

                        session.typeid = rCQ9['data']['detail']['wager']['game_id']

                        print(f'★★★>>>>>>>>>>>>>>>>>>>>>> session.typeid: {session.typeid}')              

                        res['注單']['游戏类别'] = gameCodeList[session.typeid].strip()

                        if rCQ9['data']['detail']['wager']['sub']:
                            res['FreeSpin'] = 1

                        if '旋转注单' in  mod_name or '旋轉注單' in mod_name:
                            if res['注單']['游戏类别'] in gameList_spinlotto:
                                # 回傳結果
                                return {
                                    'IsSuccess': True,
                                    'ErrorCode': config['SUCCESS_CODE']['code'],
                                    'ErrorMessage': config['SUCCESS_CODE']['msg'],
                                    'Data': res,
                                    'RawStatusCode': resp.status_code,
                                    'RawContent': resp.content
                                }
                            else:
                                return {
                                    'IsSuccess': False,
                                    'ErrorCode': config['GAME_ERROR']['code'],
                                    'ErrorMessage': config['GAME_ERROR']['msg'].format(GameName=res['注單']['游戏类别']),
                                    'Data': {},
                                    'RawStatusCode': resp.status_code,
                                    'RawContent': resp.content
                                }

                        return {
                            'IsSuccess': False,
                            'ErrorCode': config['CATEGORY_ERROR']['code'],
                            'ErrorMessage': config['CATEGORY_ERROR']['msg'].format(CategoryName=res['CategoryName']),
                            'Data': {},
                            'RawStatusCode': resp.status_code,
                            'RawContent': resp.content
                        }

            else:
                # if '旋转注单' in  mod_name or '旋轉注單' in mod_name:

                r = getPlayerId(url, data['account'])
                if r == False:
                    result['ErrorCode'] = config['NO_PLAYER']['code']
                    result['ErrorMessage'] = config['NO_PLAYER']['msg']
                    return result
                else:
                    playerId = f'{r}@LB2'
                # playerId = 'player63072149@LB2'
                rJDB = jdb(data['no'], playerId)
                # {'code': '00000', 'data': {'gamehistory': {'gameseqno': '7358091139343', 'gameid': '8003', 'playerid': 'player63072149@LB2', 'aftergamecredits': '33.39', 'playdenom': '0.01', 'ttlbet': '-0.5', 'ttlwingame': '17.1', 'ttlwinjackpot': '0', 'has_freegame': 'true', 'has_bonus': 'false', 'starttime': '2021-03-25 13:38:22.548000', 'score': '1710', 'spin_data': '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'}}}
                # {'code': '00000', 'data': {'gamehistory': ''}}
                print(f'★>>>>>>>>>>>>>>>>>>>>>> rJDB: {rJDB}')              

                if rJDB == False:
                    return result
                else:
                    if rJDB['data']['gamehistory']:
                        res['CategoryName'] = 'JDB电子'

                        #●抓遊戲總表
                        gameCodeList = {}
                        r2 = getJdbGameList()
                        if r2 == False:
                            return result
                        else:
                            gameCodeList = r2

                        session.typeid = rJDB['data']['gamehistory']['gameid']

                        print(f'★★★>>>>>>>>>>>>>>>>>>>>>> session.typeid: {session.typeid}')              

                        res['注單']['游戏类别'] = gameCodeList['data']['gameCName'][session.typeid].strip() 
                        res['注單']['下注资讯'] = f"总共:{rJDB['data']['gamehistory']['ttlbet']}(元)"
                        res['注單']['时间'] = rJDB['data']['gamehistory']['starttime'].split('.')[0]
                        res['派彩金額'] = str(eval(rJDB['data']['gamehistory']['ttlbet']) + eval(rJDB['data']['gamehistory']['ttlwingame']))

                        if rJDB['data']['gamehistory']['has_freegame'] == True or rJDB['data']['gamehistory']['has_freegame'] == 'true':
                            res['FreeSpin'] = 1

                        if '旋转注单' in  mod_name or '旋轉注單' in mod_name:
                            if res['注單']['游戏类别'] in gameList_spinlotto:
                                # 回傳結果
                                return {
                                    'IsSuccess': True,
                                    'ErrorCode': config['SUCCESS_CODE']['code'],
                                    'ErrorMessage': config['SUCCESS_CODE']['msg'],
                                    'Data': res,
                                    'RawStatusCode': resp.status_code,
                                    'RawContent': resp.content
                                }
                            else:
                                return {
                                    'IsSuccess': False,
                                    'ErrorCode': config['GAME_ERROR']['code'],
                                    'ErrorMessage': config['GAME_ERROR']['msg'].format(GameName=res['注單']['游戏类别']),
                                    'Data': {},
                                    'RawStatusCode': resp.status_code,
                                    'RawContent': resp.content
                                }

                        return {
                            'IsSuccess': False,
                            'ErrorCode': config['CATEGORY_ERROR']['code'],
                            'ErrorMessage': config['CATEGORY_ERROR']['msg'].format(CategoryName=res['CategoryName']),
                            'Data': {},
                            'RawStatusCode': resp.status_code,
                            'RawContent': resp.content
                        }

                    else:
                        return result

                # else:
                #     return result

        except Exception as e:
            print(f'>>>>>>>>>>>>>>>>>>>>>>>>> app_mgame_bbin 錯誤2：{e}')
            if 'alert' in resp.text:
                a = resp.text.find('(')
                b = resp.text.rfind(')')
                alert = resp.text[(a + 1):b].replace('"', '').replace("'", "")

                return {
                    'IsSuccess': False,
                    'ErrorCode': '',
                    'ErrorMessage': f'{cf["platform"]} 显示：{alert}',
                    'Data': e,
                    'RawStatusCode': resp.status_code,
                    'RawContent': resp.content
                }

            else:
                return {
                    'IsSuccess': False,
                    'ErrorCode': config['HTML_CONTENT_CODE']['code'],
                    'ErrorMessage': config['HTML_CONTENT_CODE']['msg'].format(platform=cf.platform),
                    'Data': e,
                    'RawStatusCode': resp.status_code,
                    'RawContent': resp.content
                }


# 查JDB「玩家帳號」
def getPlayerId(url, member):
    '''帳號管理 > 帳號轉換'''
    try:
        headers = {'User-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/89.0.4389.90 Safari/537.36'}

        payload = {'user': member,
                   'dr_type': '0',
                   'platform': '0'}
        endpoints = f'/app/daili/adminsave/members/getgameid_v1.php?uid={session.uID}&langx=zh-tw'
        r = session.post(url + endpoints, data=payload, headers=headers, verify=False, timeout=30)
        r.encoding = 'utf-8-sig'
        # print(r.text)
        bs = bs4.BeautifulSoup(r.text, 'html.parser')
        r = bs.select('span[class="red"]')
        return r[1].text

    except Exception as e:
        print(f'●>>>>>>>> getPlayerId 錯誤：{e}')
        return False


#【CQ9電子】
def cq9(cq9_url, cq9_data, endpoint='/api/inquire/v1/db/wager'):
    try:
        headers = {
                    'Accept': 'application/json, text/plain, */*',
                    'Accept-encoding': 'gzip, deflate, br',
                    'Accept-language': 'zh-TW,zh;q=0.9,en-US;q=0.8,en;q=0.7,zh-CN;q=0.6,ja;q=0.5,ms;q=0.4',
                    'sec-ch-ua': '" Not A;Brand";v="99", "Chromium";v="90", "Google Chrome";v="90"',
                    # 'sec-ch-ua': '"Google Chrome";v="89", "Chromium";v="89", ";Not A Brand";v="99"',
                    'sec-ch-ua-mobile': '?0',
                    # 'sec-fetch-dest': 'empty',
                    'Sec-Fetch-Dest': 'empty',                    
                    # 'sec-fetch-mode': 'cors',
                    'Sec-Fetch-Mode': 'cors',                    
                    # 'sec-fetch-site': 'same-site',  
                    'Sec-Fetch-Site': 'same-origin',                     
                    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/89.0.4389.90 Safari/537.36'
        }
        #url = 'https://detail.liulijing520.com/odh5/api/inquire/v1/db/wager'

        r = requests.get(cq9_url+endpoint, params=cq9_data, headers=headers, verify=False, timeout=30)
        r.encoding = 'utf-8-sig'      
        fbk = json.loads(r.text)
        # print(fbk)
        return fbk
    except Exception as e:
        print(f'●>>>>>>>> cq9 錯誤：{e}')
        print(f'●>>>>>>>> cq9 本文：{r.text}')          
        return False


#【JDB】遊戲清單
def getJdbGameList():
    try:
        headers = {'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/89.0.4389.90 Safari/537.36'}
        url = 'https://player.jdb199.com/cache/GetGameResultSetting'
        r = requests.get(url, headers=headers, verify=False, timeout=30)
        r.encoding = 'utf-8-sig'
        fbk = json.loads(r.text)
        return fbk  
    except Exception as e:
        print(f'●>>>>>>>> getJdbGameList 錯誤：{e}')
        return False    


#【JDB】
def jdb(gameSeqNo, playerId):
    try:
        headers = {
                    'accept': 'application/json, text/plain, */*',
                    'accept-encoding': 'gzip, deflate, br',
                    'accept-language': 'zh-TW,zh;q=0.9,en-US;q=0.8,en;q=0.7,zh-CN;q=0.6,ja;q=0.5,ms;q=0.4',
                    'authorization': 'Bearer undefined',
                    'content-type': 'application/json;charset=UTF-8',
                    'sec-ch-ua': '"Google Chrome";v="89", "Chromium";v="89", ";Not A Brand";v="99"',
                    'sec-ch-ua-mobile': '?0',
                    'sec-fetch-dest': 'empty',
                    'sec-fetch-mode': 'cors',
                    'sec-fetch-site': 'same-site',  
                    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/89.0.4389.90 Safari/537.36'
        }

        payload = {'dao': 'GetGameResultByGameSeq_slot', 'gameSeqNo': gameSeqNo, 'playerId': playerId}
        data = json.dumps(payload)
        url = 'https://asapi.jdb199.com/api/runDao'
        r = requests.post(url, data=data, headers=headers, verify=False, timeout=30)
        r.encoding = 'utf-8-sig'
        # print(r.text)
        fbk = json.loads(r.text)
        # print(fbk)
        return fbk
    except Exception as e:
        print(f'●>>>>>>>> jdb 錯誤：{e}')
        return False


#【分類當日投注】
@log_info
@catch_exception
def member_report_result(cf, url: str, params: dict, headers: dict={},
                         timeout: tuple=(3, 5), endpoints: str='app/daili/adminsave/member_report/result.php', **kwargs) -> dict:
    '''LEBO【帳號管理 >> 輸贏查詢】
    Args:
        url : (必填) LEBO 後台網址，範例格式: 'https://shaba888.jf-game.net/'
        params : (必填) LEBO API 網址參數, 使用字典傳入, 範例格式: {
            "date_start": "2020-05-26 00:00:00",        # 搜尋時間(起)
            "date_end": "2020-05-26 23:59:59",          # 搜尋時間(迄)
            "account": "hm81448072",                    # 會員帳號
            "game[]": ['zq', 'cp', 'lebo0'],            # 遊戲類別
            "model": '1',                               #
            "searchbtn": '查詢',                        # 固定值
        }
        headers : LEBO API 表頭, 使用字典傳入, 預設為: {
            'Content-Type': 'application/x-www-form-urlencoded',
            'X-Requested-With': 'XMLHttpRequest'
        }
    Returns:
        'IsSuccess': 是(查詢成功/布林值)否(查詢失敗/布林值),
        'ErrorCode': 錯誤代碼(如果前述布林值為False才有內容/文字格式)
        'ErrorMessage': 錯誤原因(如果前述布林值為False才有內容/文字格式)
        'Data': 查詢的回傳內容, 字典格式, ex:
                (除總計以外其他欄位需要detail為True傳入才有)
                {
                    "體育": {
                        "類別": "體育",
                        "投注金額": "0",
                        "有效投注": "0",
                        "會員輸贏": "0",
                        "彩池抽水": "0",
                        "彩金": "0"
                    },
                    "總計：": {
                        "類別": "總計：",
                        "投注金額": "31473.5",
                        "有效投注": "31453.5778",
                        "會員輸贏": "-1228.76",
                        "彩池抽水": "17.8918",
                        "彩金": "0"
                    }
                }
        'RawStatusCode': LEBO原始回傳狀態碼
        'RawContent': LEBO原始回傳內容
    '''

    headers = headers or default_headers
    params = params or {}

    resp = session.get(url + endpoints, params=params, headers=headers, verify=False, timeout=30)
    alert_search = alert_pattern.findall(resp.text)

    # 檢查回傳
    if 'logout.php' in resp.text:
        raise NotSignError('帐号已其他地方登入, 请重新登入')

    # 彈出失敗回傳(查詢時會回傳多個alert訊息在js中)
    if len(alert_search) == 1:
        # 回傳失敗訊息

        if '操作權限' in resp.text:
            return {
                'IsSuccess': False,
                'ErrorCode': config['PERMISSION_CODE']['code'],
                'ErrorMessage': f'{cf["platform"]} 显示：{alert_search[0]}',
                'Data': {},
                'RawStatusCode': resp.status_code,
                'RawContent': resp.content
            }
        else:
            return {
                'IsSuccess': False,
                'ErrorCode': config['SUCCESS_CODE']['code'],
                'ErrorMessage': f'{cf["platform"]} 显示：{alert_search[0]}',
                'Data': {},
                'RawStatusCode': resp.status_code,
                'RawContent': resp.content
            }

    # 狀態碼錯誤
    if resp.status_code != 200:
        return {
            'IsSuccess': False,
            'ErrorCode': config['HTML_STATUS_CODE']['code'],
            'ErrorMessage': config['HTML_STATUS_CODE']['msg'].format(platform=cf.platform, status_code=resp.status_code),
            'Data': {},
            'RawStatusCode': resp.status_code,
            'RawContent': resp.content
        }

    html = requests_html.HTML(html=resp.text)
    # 欄位
    columns = [th.text for th in html.find('th')]
    # 明細
    datas = [[td.text for td in tr.find('tr:not(.table_total) > td')] for tr in html.find('tr.m_cen')]
    # 總計
    if html.find('tr.table_total'):
        _total = [td.text for td in html.find('tr.table_total > td')]
        total = dict(zip(columns, _total))
    else:
        _total = {c: 0 for c in columns}
        total = {c: 0 for c in columns}
    # 檢查總計欄位數
    if len(columns) != len(_total):
        return {
            'IsSuccess': False,
            'ErrorCode': config['HTML_CONTENT_CODE']['code'],
            'ErrorMessage': config['HTML_CONTENT_CODE']['msg'].format(platform=cf.platform),
            'Data': {},
            'RawStatusCode': resp.status_code,
            'RawContent': resp.content
        }

    # 回傳
    res = {
        '總計：': total
    }

    # 表格逐行檢查欄位數、轉為字典、存入res中
    for row in datas:
        # 轉為字典
        dic = dict(zip(columns, row))

        # 存入res中
        res[dic['類型']] = dic
       
        # 檢查欄位數
        if len(columns) != len(row):
            return {
                'IsSuccess': False,
                'ErrorCode': config['HTML_CONTENT_CODE']['code'],
                'ErrorMessage': config['HTML_CONTENT_CODE']['msg'].format(platform=cf.platform),
                'Data': res,
                'RawStatusCode': resp.status_code,
                'RawContent': resp.content
            }

    # 回傳結果
    return {
        'IsSuccess': True,
        'ErrorCode': config['SUCCESS_CODE']['code'],
        'ErrorMessage': config['SUCCESS_CODE']['msg'],
        'Data': res,
        'RawStatusCode': resp.status_code,
        'RawContent': resp.content
    }


#【本遊戲當日投注】
@log_info
@catch_exception
def bet_record_v2_index(cf, Member, SearchGameCategory, url: str, data: dict, headers: dict={},
                         timeout: tuple=(3, 5), endpoints: str='app/daili/adminsave/bet_record_v2/index.php', **kwargs) -> dict:
    '''LEBO【帳號管理 >> 會員管理 >> 下注】
    Args:
        url : (必填) LEBO 後台網址，範例格式: 'https://shaba888.jf-game.net/'
        data : (必填) LEBO API 網址參數, 使用字典傳入, 範例格式:
            {
            "betno": "517443637888",                 # 注單號碼
            "date_start": "2020-12-23 00:00:00",     # 日期時間(起)
            "date_end" : "2020-12-23 23:59:59"       # 日期時間(迄)
            }
        headers : LEBO API 表頭, 使用字典傳入, 預設為: {
            'Content-Type': 'application/x-www-form-urlencoded',
            'X-Requested-With': 'XMLHttpRequest'
        }
    Returns:
        'IsSuccess': 是(查詢成功/布林值)否(查詢失敗/布林值),
        'ErrorCode': 錯誤代碼(如果前述布林值為False才有內容/文字格式)
        'ErrorMessage': 錯誤原因(如果前述布林值為False才有內容/文字格式)
        'Data': 查詢的回傳內容, 字典格式, ex:
                {
                    "GameCommissionable": "168.88"
                }
        'RawStatusCode': LEBO原始回傳狀態碼
        'RawContent': LEBO原始回傳內容
    '''

    headers = headers or default_headers
    data = data or {}

    memberA = 'b4' + Member
    res = {'GameCommissionable': '0'}

    for gametypeA in SearchGameCategory:
        print(gametypeA, end=', ')
        data['gametype'] = gametypeA
        resp = session.post(url + endpoints + f'?uid={session.uID}&mtype=1&username={memberA}', data=data, headers=headers, verify=False, timeout=30)
        resp.encoding = 'utf-8-sig'

        alert_search = alert_pattern.findall(resp.text)

        # 檢查回傳
        if 'logout.php' in resp.text:
            raise NotSignError('帐号已其他地方登入, 请重新登入')

        # 彈出失敗回傳(查詢時會回傳多個alert訊息在js中)
        if len(alert_search) == 1:
            # 回傳失敗訊息

            if '操作權限' in resp.text:
                return {
                    'IsSuccess': False,
                    'ErrorCode': config['PERMISSION_CODE']['code'],
                    'ErrorMessage': f'{cf["platform"]} 显示：{alert_search[0]}',
                    'Data': {},
                    'RawStatusCode': resp.status_code,
                    'RawContent': resp.content
                }
            else:
                return {
                    'IsSuccess': False,
                    'ErrorCode': config['SUCCESS_CODE']['code'],
                    'ErrorMessage': f'{cf["platform"]} 显示：{alert_search[0]}',
                    'Data': {},
                    'RawStatusCode': resp.status_code,
                    'RawContent': resp.content
                }

        # # 狀態碼錯誤
        # if resp.status_code != 200:
        #     return {
        #         'IsSuccess': False,
        #         'ErrorCode': config['HTML_STATUS_CODE']['code'],
        #         'ErrorMessage': config['HTML_STATUS_CODE']['msg'].format(platform=cf.platform, status_code=resp.status_code),
        #         'Data': {},
        #         'RawStatusCode': resp.status_code,
        #         'RawContent': resp.content
        #     }


        num = resp.text.count(data['betno'])
        if num > 1:
            break
        else:
            pass  

    else:
        return {
            # 'IsSuccess': False,
            'IsSuccess': True,
            'ErrorCode': config['SUCCESS_CODE']['code'],
            # 'ErrorMessage': f'{cf["platform"]} 显示：查无本游戏当日投注',
            'ErrorMessage': config['SUCCESS_CODE']['msg'],
            # 'Data': {},
            'Data': res,
            'RawStatusCode': resp.status_code,
            'RawContent': resp.content
        }

    
    data['typeid'] = session.typeid

    print(f"★★★★★>>>>>>>>>>>>>>>>>>>>>> data['typeid']: {data['typeid']}")              

    data.pop('betno')
    resp = session.post(url + endpoints + f'?uid={session.uID}&mtype=1&username={memberA}', data=data, headers=headers, verify=False, timeout=30)
    resp.encoding = 'utf-8-sig'

    alert_search = alert_pattern.findall(resp.text)

    # 檢查回傳
    if 'logout.php' in resp.text:
        raise NotSignError('帐号已其他地方登入, 请重新登入')

    # 彈出失敗回傳(查詢時會回傳多個alert訊息在js中)
    if len(alert_search) == 1:
        # 回傳失敗訊息

        if '操作權限' in resp.text:
            return {
                'IsSuccess': False,
                'ErrorCode': config['PERMISSION_CODE']['code'],
                'ErrorMessage': f'{cf["platform"]} 显示：{alert_search[0]}',
                'Data': {},
                'RawStatusCode': resp.status_code,
                'RawContent': resp.content
            }
        else:
            return {
                'IsSuccess': False,
                'ErrorCode': config['SUCCESS_CODE']['code'],
                'ErrorMessage': f'{cf["platform"]} 显示：{alert_search[0]}',
                'Data': {},
                'RawStatusCode': resp.status_code,
                'RawContent': resp.content
            }

    # # 狀態碼錯誤
    # if resp.status_code != 200:
    #     return {
    #         'IsSuccess': False,
    #         'ErrorCode': config['HTML_STATUS_CODE']['code'],
    #         'ErrorMessage': config['HTML_STATUS_CODE']['msg'].format(platform=cf.platform, status_code=resp.status_code),
    #         'Data': {},
    #         'RawStatusCode': resp.status_code,
    #         'RawContent': resp.content
    #     }

    bs = bs4.BeautifulSoup(resp.text, 'html.parser')
    data = bs.select('tr')

    # 回傳
    if data:
        num = ''
        for i in data:
            if '有效投注' in i.text:
                x = [j for j in i.text.split('\n') if j != '']
                a = x.index('有效投注')
                b = len(x)
                num = eval(f'-{b-a}')  #「欄位驗證」機制

            if num != '':    
                if '总計' in i.text:
                    x2 = [j for j in i.text.split('\n') if j != '']
                    res['GameCommissionable'] = x2[num]

                    return {
                        'IsSuccess': True,
                        'ErrorCode': config['SUCCESS_CODE']['code'],
                        'ErrorMessage': config['SUCCESS_CODE']['msg'],
                        'Data': res,
                        'RawStatusCode': resp.status_code,
                        'RawContent': resp.content
                    }

        else:
            data2 = bs.select('.m_rig') 
            if len(data2) > 0:                            
                res['GameCommissionable'] = 0

                return {
                    'IsSuccess': True,
                    'ErrorCode': config['SUCCESS_CODE']['code'],
                    'ErrorMessage': config['SUCCESS_CODE']['msg'],
                    'Data': res,
                    'RawStatusCode': resp.status_code,
                    'RawContent': resp.content
                }

            else:
                return {
                    'IsSuccess': False,
                    'ErrorCode': config['NO_GAME_MONEY']['code'],
                    'ErrorMessage': config['NO_GAME_MONEY']['msg'],
                    'Data': res,
                    'RawStatusCode': resp.status_code,
                    'RawContent': resp.content
                }

    else:
        return {
            'IsSuccess': False,
            'ErrorCode': config['NO_GAME_MONEY']['code'],
            'ErrorMessage': config['NO_GAME_MONEY']['msg'],
            'Data': res,
            'RawStatusCode': resp.status_code,
            'RawContent': resp.content
        }


#【充值】
@log_info
@catch_exception
def cash_cash_operation(cf, url: str, source: dict, params: dict={}, headers: dict={},
                   timeout: tuple=(3, 5), endpoints: str='app/cash/cash_operation.php', **kwargs) -> dict:
    '''LEBO【現金系統 >> 存款與取款】
    Args:
        url : (必填) LEBO 後台網址，範例格式: 'https://shaba888.jf-game.net/'
        source : (必填) LEBO API 參數
            現金系統>存款與取款>會員查詢: {
                "username": "hm81448072",               # 會員帳號
                "search": "search",                     # 固定值
            }
            現金系統>存款與取款>人工存取款>操作類型【存款】: {
                "username": "hm81448072",               # 會員帳號
                "userid": "10249577",                   # 會員ID(透過)會員查詢獲得
                "op_type": 1,                           # 操作類型(1:存款, 2:取款)
                "amount": 1,                            # 存款金額
                "ifSp": 0,                              # 存款優惠>>勾選
                "spAmount": 1,                          # 存款優惠>>優惠金額
                "ifSp_other": 0,                        # 匯款優惠>>勾選
                "sp_other": 1,                          # 匯款優惠>>優惠金額
                "isComplex": 0,                         # 綜合打碼量稽核>>勾選
                "ComplexValue": 1,                      # 綜合打碼量稽核>>打碼量
                "isnormality": 0,                       # 常態性稽核
                "type_memo": 1,                         # 存款項目(1:人工存入, 
                                                                   2:存款優惠,
                                                                   3:負數額度歸零,
                                                                   4:取消出款,
                                                                   5:其他,
                                                                   6:活動優惠,
                                                                   7:反點優惠,
                                                                   8:掉單補回,
                                                                   9:線上掉單)
                "isty": 1,                              # 是否退拥(1:寫入, 0:取消)
                "amount_memo": "倍住",                  # 備注
            }
            現金系統>存款與取款>人工存取款>操作類型【取款】: {
                "username": "hm81448072",               # 會員帳號
                "userid": "10249577",                   # 會員ID(透過)會員查詢獲得
                "op_type": 2,                           # 操作類型(1:存款, 2:取款)
                "amount": 1,                            # 取款金額
                "spAmount": 1,                          # 隱藏欄位(smouny的百分之一)
                "sp_other": 0,                          # 隱藏欄位, 固定值
                "ComplexValue": 1,                      # 隱藏欄位
                "type_memo": 1,                         # 存款項目(1:重複出款, 
                                                                   2:公司入款存誤,
                                                                   3:公司負數回沖,
                                                                   4:手動申請出款,
                                                                   5:扣除非法下住派彩,
                                                                   6:放棄存款優惠,
                                                                   7:其他,
                                                                   8:體育投注餘額)
                "isty": 1,                              # 是否退拥(1:寫入, 0:取消)
                "amount_memo": "倍住",                  # 備注
            }
        params : LEBO API 網址參數, 使用字典傳入, 預設為: {}
        headers : LEBO API 表頭, 使用字典傳入, 預設為: {
            'Content-Type': 'application/x-www-form-urlencoded',
            'X-Requested-With': 'XMLHttpRequest'
        }
    Returns:
        'IsSuccess': 是(查詢成功/布林值)否(查詢失敗/布林值),
        'ErrorCode': 錯誤代碼(如果前述布林值為False才有內容/文字格式)
        'ErrorMessage': 錯誤原因(如果前述布林值為False才有內容/文字格式)
        'Data': 查詢結果, 字典格式, ex:
                {
                    'hidden_input': {
                        'search': 'search',
                        'userid': '520018',
                        'username': 'e5sha12'
                    },
                    'info_table': {
                        '帳號': 'sha12',
                        '層級': '第一层',
                        '姓名': '齐天大圣',
                        '操作類型': '存款 取款',
                        '存款金額': '',
                        '存款優惠': '存入，優惠金額：',
                        '匯款優惠': '存入，優惠金額：',
                        '綜合打碼量稽核': '稽核，打碼量：',
                        '常態性稽核': '稽核',
                        '存款项目': '',
                        '是否退拥': '写入 取消',
                        '備注': '',
                        '會員備注': '',
                        'LEBO備注': ''
                    },
                    'balance_table': {
                        'LB余額': '2011.66 元',
                        'MG余額': '0.63 元',
                        'DT余額': '0.00 元',
                        'BBIN余額': '0.60 元',
                        'AG余額': '0.50 元',
                        'OG余額': '0.00 元',
                        '沙巴余額': '0.00 元',
                        'BG余額': '0.00 元',
                        'DG余額': '0.00 元',
                        'MW余額': '0.50 元',
                        'PP余額': '0.00 元',
                    }
                }
        'RawStatusCode': LEBO原始回傳狀態碼
        'RawContent': LEBO原始回傳內容
    '''
    # 定義變數
    headers = headers or default_headers
    params = params or {}
    source = source or {}

    if 'userid' in source:
        try:  #●新增
            # 呼叫任務
            resp = session.post(url + endpoints, data=urlencode(source), headers=headers, timeout=30)

        except requests.exceptions.ConnectionError as e:
            log_trackback()
            return {
                'IsSuccess': False,
                'ErrorCode': config['IGNORE_CODE']['code'],
                'ErrorMessage': config['IGNORE_CODE']['msg'],
            }
    else:
        # 呼叫任務
        resp = session.post(url + endpoints, data=urlencode(source), headers=headers, timeout=30)

    html = requests_html.HTML(html=resp.text)
    alert_search = alert_pattern.findall(resp.text)

    # 失敗 被登出
    if 'logout.php' in resp.text:
        raise NotSignError('帐号已其他地方登入, 请重新登入')
    # 彈出失敗回傳(查詢時會回傳多個alert訊息在js中)
    if len(alert_search) == 1:
        # 回傳失敗訊息
        if alert_search[0] == '操作成功！':
            return {
                'IsSuccess': True,
                'ErrorCode': config['SUCCESS_CODE']['code'],
                'ErrorMessage': config['SUCCESS_CODE']['msg'],
                'RawStatusCode': resp.status_code,
                'RawContent': resp.content
            }
        elif '操作權限' in resp.text:
            return {
                'IsSuccess': False,
                'ErrorCode': config['PERMISSION_CODE']['code'],
                'ErrorMessage': f'{cf["platform"]} 显示：{alert_search[0]}',
                'Data': {},
                'RawStatusCode': resp.status_code,
                'RawContent': resp.content
            }
        elif alert_search[0] == '會員帳號不存在，請重新輸入！':
            return {
                'IsSuccess': False,
                'ErrorCode': config['NO_USER']['code'],
                'ErrorMessage': config['NO_USER']['msg'],
                'Data': {},
                'RawStatusCode': resp.status_code,
                'RawContent': resp.content
            }
        elif '重複提交' in alert_search[0]:
            return {
                'IsSuccess': False,
                'ErrorCode': config.REPEAT_DEPOSIT.code,
                'ErrorMessage': config.REPEAT_DEPOSIT.msg.format(platform=cf.platform, msg=alert_search[0]),
                'Data': {},
                'RawStatusCode': resp.status_code,
                'RawContent': resp.content
            }
        else:
            return {
                'IsSuccess': False,
                'ErrorCode': config['HTML_CONTENT_CODE']['code'],
                'ErrorMessage': f'{cf["platform"]} 显示：{alert_search[0]}',
                'Data': {},
                'RawStatusCode': resp.status_code,
                'RawContent': resp.content
            }

    # 狀態碼錯誤
    if resp.status_code != 200:
        return {
            'IsSuccess': False,
            'ErrorCode': config['HTML_STATUS_CODE']['code'],
            'ErrorMessage': config['HTML_STATUS_CODE']['msg'].format(platform=cf.platform, status_code=resp.status_code),
            'RawStatusCode': resp.status_code,
            'RawContent': resp.content
        }

    # 查詢異常(含有alert錯誤訊息, 狀態碼不正確, 未知錯誤)
    if not html.find('table.m_tab'):
        if 'alert' in resp.text:
            return {
                'IsSuccess': False,
                'ErrorCode': config['HTML_CONTENT_CODE']['code'],
                'ErrorMessage': f'{cf["platform"]} 显示：{alert_pattern.search(resp.text).group()}',
                'Data': {},
                'RawStatusCode': resp.status_code,
                'RawContent': resp.content
            }
        elif resp.status_code != 200:
            return {
                'IsSuccess': False,
                'ErrorCode': config['HTML_STATUS_CODE']['code'],
                'ErrorMessage': config['HTML_STATUS_CODE']['msg'].format(platform=cf.platform, status_code=resp.status_code),
                'Data': {},
                'RawStatusCode': resp.status_code,
                'RawContent': resp.content
            }
        else:
            return {
                'IsSuccess': False,
                'ErrorCode': config['HTML_CONTENT_CODE']['code'],
                'ErrorMessage': config['HTML_CONTENT_CODE']['msg'].format(platform=cf.platform),
                'RawStatusCode': resp.status_code,
                'RawContent': resp.content
            }

    # 解析隱藏屬性
    hidden_input = {ipt.attrs.get('name'): ipt.attrs.get('value') for ipt in html.find('input[type="hidden"]')}
    res = {'hidden_input': hidden_input}

    # 解析上半表格內容
    info_table, balance_table = html.find('table.m_tab')
    res['info_table'] = dict([td.text for td in tr.find('td')] for tr in info_table.find('tr')[1:-1])
    # 解析下半表格內容
    balance_table = [tr.find('td') for tr in balance_table.find('tr')[2:]]
    balance_table = [td.text for td in sum(balance_table, [])]
    res['balance_table'] = dict(zip(balance_table[::2], balance_table[1::2]))

    # 回傳內容
    return {
        'IsSuccess': True,
        'ErrorCode': config['SUCCESS_CODE']['code'],
        'ErrorMessage': config['SUCCESS_CODE']['msg'],
        'Data': res,
        'RawStatusCode': resp.status_code,
        'RawContent': resp.content
    }


# 覆查機制(充值記錄)
@log_info
@catch_exception
def cash_cash_record(cf, url, params: dict, headers: dict={}, timeout: tuple=(3, 5), endpoints: str='app/cash/cash_record.php', **kwargs) -> dict:
    '''LEBO【現金系統 >> 存款與取款 >> 历史查询】
    Args:
        url : (必填) LEBO 後台網址，範例格式: 'https://shaba888.jf-game.net/'
        params : LEBO API 網址參數, 使用字典傳入： {
            'agent': '',
            'date_start': '2021-03-10 00:00:00',    # 開始日期時間
            'date_end': '2021-03-12 23:59:59',      # 結束日期時間
            'account': 'jscs001',                   # 會員帳號
            'otype': '8',                           # 活動優惠(固定值)
            'page_num': '500',                      # 每頁筆數(固定值)
            'page': '1'}                            # 第1頁(固定值)
        }
        headers : LEBO API 表頭, 使用字典傳入：{
        }
    Returns:
        {
            'IsSuccess': True,
            'ErrorCode': 'AA200',
            'ErrorMessage': '',
            'Data': {
                'record': [{'序號': '1', 
                            '會員': 'jscs001', 
                            '操作類型': '活动优惠', 
                            '交易金額': '0', 
                            '存款優惠': '1', 
                            '匯款优惠': '0', 
                            '餘額': '8.19', 
                            '綜合打碼量稽核': '是(打碼量：2)', 
                            '常態性稽核': '否', 
                            '交易日期': '2021-03-08 05:20:53', 
                            '備注': f'★{cf.website}充值測試', 
                            '操作人': 'zhudan04'}],
                'total_page': 1,
            }
        }
    '''
    resp = session.get(url + endpoints, params=params, headers=headers, timeout=timeout)

    html = requests_html.HTML(html=resp.text)
    alert_search = alert_pattern.findall(resp.text)

    # 失敗 被登出
    if 'logout.php' in resp.text:
        raise NotSignError('帐号已其他地方登入, 请重新登入')
    # 彈出成功回傳(查詢時會回傳多個alert訊息在js中)
    if len(alert_search) == 1 and alert_search[0] == '操作成功！':
        return {
            'IsSuccess': True,
            'ErrorCode': config['SUCCESS_CODE']['code'],
            'ErrorMessage': config['SUCCESS_CODE']['msg'],
            'RawStatusCode': resp.status_code,
            'RawContent': resp.content
        }
    # 彈出失敗回傳(查詢時會回傳多個alert訊息在js中)
    if len(alert_search) == 1:
        # 回傳失敗訊息
        if '操作權限' in resp.text:
            return {
                'IsSuccess': False,
                'ErrorCode': config['PERMISSION_CODE']['code'],
                'ErrorMessage': f'{cf["platform"]} 显示：{alert_search[0]}',
                'Data': {},
                'RawStatusCode': resp.status_code,
                'RawContent': resp.content
            }
        else:
            return {
                'IsSuccess': False,
                'ErrorCode': config['SUCCESS_CODE']['code'],
                'ErrorMessage': f'{cf["platform"]} 显示：{alert_search[0]}',
                'Data': {},
                'RawStatusCode': resp.status_code,
                'RawContent': resp.content
            }
    # 狀態碼錯誤
    if resp.status_code != 200:
        return {
            'IsSuccess': False,
            'ErrorCode': config['HTML_STATUS_CODE']['code'],
            'ErrorMessage': config['HTML_STATUS_CODE']['msg'].format(platform=cf.platform, status_code=resp.status_code),
            'RawStatusCode': resp.status_code,
            'RawContent': resp.content
        }
    # 查詢異常(含有alert錯誤訊息, 狀態碼不正確, 未知錯誤)
    if not html.find('table.m_tab'):
        if 'alert' in resp.text:
            return {
                'IsSuccess': False,
                'ErrorCode': config['HTML_CONTENT_CODE']['code'],
                'ErrorMessage': f'{cf["platform"]} 显示：{alert_pattern.search(resp.text).group()}',
                'Data': {},
                'RawStatusCode': resp.status_code,
                'RawContent': resp.content
            }
        elif resp.status_code != 200:
            return {
                'IsSuccess': False,
                'ErrorCode': config['HTML_STATUS_CODE']['code'],
                'ErrorMessage': config['HTML_STATUS_CODE']['msg'].format(platform=cf.platform, status_code=resp.status_code),
                'Data': {},
                'RawStatusCode': resp.status_code,
                'RawContent': resp.content
            }
        else:
            return {
                'IsSuccess': False,
                'ErrorCode': config['HTML_CONTENT_CODE']['code'],
                'ErrorMessage': config['HTML_CONTENT_CODE']['msg'].format(platform=cf.platform),
                'RawStatusCode': resp.status_code,
                'RawContent': resp.content
            }

    table = html.find('table.m_tab[width="99%"]', first=True)
    columns = [td.text for td in table.find('tr.m_title > td')]
    record = [
        dict(zip(columns, [td.text for td in tr.find('td')] ))
        for tr in table.find('tr.m_cen')
        if len(tr.find('td')) == len(columns)
    ]

    total_page = max([int(opt.attrs.get('value')) for opt in html.find('select#page > option')])

    return {
        'IsSuccess': True,
        'ErrorCode': config['SUCCESS_CODE']['code'],
        'ErrorMessage': config['SUCCESS_CODE']['msg'],
        'Data': {
            'total_page': total_page,
            'record': record
        }
    }

#查詢會員註冊時間&會員層級
@log_info
@catch_exception
def members(cf, url: str, data: dict, headers: dict={}, timeout: tuple=(3, 5), 
            endpoints: str='app/daili/adminsave/members/members.php', **kwargs) ->dict:
    '''LEBO【帳號管理>會員管理】'''
    headers = headers or default_headers
    data = data or {}

    resp = session.post(url+endpoints, data=data, headers=headers, verify=False, timeout=30)
    resp.encoding = 'utf-8-sig'
    alert_search = alert_pattern.findall(resp.text)

    # 檢查回傳
    if 'logout.php' in resp.text:
        raise NotSignError('帐号已其他地方登入, 请重新登入')

    # 彈出失敗回傳(查詢時會回傳多個alert訊息在js中)
    if alert_search:
        # 回傳失敗訊息
        if '操作權限' in resp.text:
            return {
                'IsSuccess': False,
                'ErrorCode': config['PERMISSION_CODE']['code'],
                'ErrorMessage': f'{cf["platform"]} 显示：{alert_search[0]}',
                'Data': {},
                'RawStatusCode': resp.status_code,
                'RawContent': resp.content
            }
        else:
            return {
                'IsSuccess': False,
                'ErrorCode': config['HTML_CONTENT_CODE']['code'],
                'ErrorMessage': f'{cf["platform"]} 显示：{alert_search[0]}',
                'Data': {},
                'RawStatusCode': resp.status_code,
                'RawContent': resp.content
            }
    # 狀態碼錯誤
    if resp.status_code != 200:
        return {
            'IsSuccess': False,
            'ErrorCode': config['HTML_STATUS_CODE']['code'],
            'ErrorMessage': config['HTML_STATUS_CODE']['msg'].format(platform=cf.platform, status_code=resp.status_code),
            'Data': {},
            'RawStatusCode': resp.status_code,
            'RawContent': resp.content
        }
    html = requests_html.HTML(html=resp.text)
    #查詢是否有錯誤訊息
    message = html.find('table.m_tab > tr.m_title')[0].text if html.find('table.m_tab > tr.m_title') else None
    if message and message == '目前無任何會員':
        logger.info(f'LEBO回應:{message}')
        return {
                'IsSuccess': False,
                'ErrorCode': config['NO_USER']['code'],
                'ErrorMessage': config['NO_USER']['msg'],
                'Data': {},
                'RawStatusCode': resp.status_code,
                'RawContent': resp.content
                }
    if message:
        logger.info(f'LEBO回應:{message}')
        return {
                'IsSuccess': False,
                'ErrorCode': config['HTML_CONTENT_CODE']['code'],
                'ErrorMessage': f'{cf["platform"]} 显示：{message}',
                'Data': {},
                'RawStatusCode': resp.status_code,
                'RawContent': resp.content
            }
    #取得欄位名稱
    title = html.find('table.m_tab > tr.m_title_over_co')
    if len(title) == 1:
        title = title[0].text.split()
    else:
        return {
                'IsSuccess': False,
                'ErrorCode': config['HTML_CONTENT_CODE']['code'],
                'ErrorMessage': config['HTML_CONTENT_CODE']['msg'].format(platform=cf.platform),
                'Data': {},
                'RawStatusCode': resp.status_code,
                'RawContent': resp.content
            }
    #取得表格內容
    content_css = html.find('table.m_tab > tr.m_cen:not([style])')
    if content_css:
        content = [data.text.replace('\xa0', '').split('\n') for data in content_css]
    else:
        return {
                'IsSuccess': False,
                'ErrorCode': config['HTML_CONTENT_CODE']['code'],
                'ErrorMessage': config['HTML_CONTENT_CODE']['msg'].format(platform=cf.platform),
                'Data': {},
                'RawStatusCode': resp.status_code,
                'RawContent': resp.content
            }
    #取得對應申請會員的表格資料 & 取得對應申請會員的userid
    check_member = [i for i in range(len(title)) if title[i] == '登入帳號']
    if check_member:
        data_member = [i for i in content if i[check_member[0]] == data['uname']]
        result = dict(zip(title, data_member[0])) if data_member else None
    else:
        return {
                'IsSuccess': False,
                'ErrorCode': config['HTML_CONTENT_CODE']['code'],
                'ErrorMessage': config['HTML_CONTENT_CODE']['msg'].format(platform=cf.platform),
                'Data': {},
                'RawStatusCode': resp.status_code,
                'RawContent': resp.content
            }
    #取得userid
    if result:
        count = [counts for counts in range(len(content)) if data['uname'] in content[counts]][0]
        usernid = [re.search(r'(?<=usernid\=)\d+', i.attrs['href']).group() for i in content_css[count].find('td[align="center"] > a') if i.text == '資料'] 
    else:
        return {
                'IsSuccess': False,
                'ErrorCode': config['NO_USER']['code'],
                'ErrorMessage': config['NO_USER']['msg'],
                'Data': {},
                'RawStatusCode': resp.status_code,
                'RawContent': resp.content
                }
    if usernid:
        result['usernid'] = usernid[0]
    else:
        return {
                'IsSuccess': False,
                'ErrorCode': config['HTML_CONTENT_CODE']['code'],
                'ErrorMessage': config['HTML_CONTENT_CODE']['msg'].format(platform=cf.platform),
                'Data': {},
                'RawStatusCode': resp.status_code,
                'RawContent': resp.content
                }

    #取得功能對應連結:
    href = html.find('table.m_tab > tr.m_cen:not([style]) > td[align="center"] a')
    if not href:
        return {
                'IsSuccess': False,
                'ErrorCode': config['HTML_CONTENT_CODE']['code'],
                'ErrorMessage': config['HTML_CONTENT_CODE']['msg'].format(platform=cf.platform),
                'Data': {},
                'RawStatusCode': resp.status_code,
                'RawContent': resp.content
                }
    href_list = [re.search('(javascript:CheckSTOP\(")?(\.\.)?/?(?P<url>[^"]*)', i.attrs['href']).group('url') for i in href]
    
    if '功能' not in result:
        return {
                'IsSuccess': False,
                'ErrorCode': config['HTML_CONTENT_CODE']['code'],
                'ErrorMessage': config['HTML_CONTENT_CODE']['msg'].format(platform=cf.platform),
                'Data': {},
                'RawStatusCode': resp.status_code,
                'RawContent': resp.content
                }
    if len(href_list) != len(result['功能'].replace(' ','').split('/')):
        return {
                'IsSuccess': False,
                'ErrorCode': config['HTML_CONTENT_CODE']['code'],
                'ErrorMessage': config['HTML_CONTENT_CODE']['msg'].format(platform=cf.platform),
                'Data': {},
                'RawStatusCode': resp.status_code,
                'RawContent': resp.content
                }
    result['功能'] = dict(zip(result['功能'].replace(' ','').split('/'), href_list))

    #回傳結果
    return {
        'IsSuccess': True,
        'ErrorCode': config['SUCCESS_CODE']['code'],
        'ErrorMessage': config['SUCCESS_CODE']['msg'],
        'Data': result,
        'RawStatusCode': resp.status_code,
        'RawContent': resp.content
    }
                
#查詢銀行卡&真實姓名
@log_info
@catch_exception
def member_data(cf, url: str, data: dict, member: str, headers: dict={}, timeout: tuple=(3, 5), 
            endpoints: str='app/daili/adminsave/members/member_data.php', **kwargs) ->dict:
    '''LEBO【帳號管理>會員管理>資料】'''
    headers = headers or default_headers
    data = data or {}
    keys = []
    values = []
    keys2 = []
    values2 = []

    resp = session.get(url+endpoints, params=data, headers=headers, verify=False, timeout=30)
    resp.encoding = 'utf-8-sig'

    # 檢查回傳
    if 'logout.php' in resp.text:
        raise NotSignError('帐号已其他地方登入, 请重新登入')

    # 狀態碼錯誤
    if resp.status_code != 200:
        return {
            'IsSuccess': False,
            'ErrorCode': config['HTML_STATUS_CODE']['code'],
            'ErrorMessage': config['HTML_STATUS_CODE']['msg'].format(platform=cf.platform, status_code=resp.status_code),
            'Data': {},
            'RawStatusCode': resp.status_code,
            'RawContent': resp.content
        }
    html = requests_html.HTML(html=resp.text)
    #基本資料欄位
    member_data = html.find('table[class="m_tab_ed"] > tr[class="m_bc_ed"]:not([style]):not([align]) > td[class="m_mem_ed"]')
    member_value = html.find('table[class="m_tab_ed"] > tr[class="m_bc_ed"]:not([style]):not([align]) > td:not([class="m_mem_ed"])')
    if not member_data:
        return {
                'IsSuccess': False,
                'ErrorCode': config['HTML_CONTENT_CODE']['code'],
                'ErrorMessage': config['HTML_CONTENT_CODE']['msg'].format(platform=cf.platform),
                'Data': {},
                'RawStatusCode': resp.status_code,
                'RawContent': resp.content
                }
    keys = [key.text.replace('：','') for key in member_data]
    for value in member_value:
        if value.text and not 'colspan' in value.attrs:
            values.append(value.text.split('\n') if '\n' in value.text else value.text)
        elif value.text and 'colspan' in value.attrs:
            values.append(value.text.split('\n')[0] if len(value.text.split('\n')) >= 2 else '')
        elif value.find('input'):
            values.append(value.find('input')[0].attrs['value'])
        else:
            values.append('')
    if keys and values and len(keys) != len(values):
        return {
                'IsSuccess': False,
                'ErrorCode': config['HTML_CONTENT_CODE']['code'],
                'ErrorMessage': config['HTML_CONTENT_CODE']['msg'].format(platform=cf.platform),
                'Data': {},
                'RawStatusCode': resp.status_code,
                'RawContent': resp.content
                }
    base_data = dict(zip(keys, values))
    # # LEBO帳號會在前面兩位加上英數字必須拿掉
    # base_data['帳號'] = base_data['帳號'][len(base_data['帳號'])-len(member):] if '帳號' in base_data else None
    # if base_data['帳號'] is None:
    #     return {
    #             'IsSuccess': False,
    #             'ErrorCode': config['HTML_CONTENT_CODE']['code'],
    #             'ErrorMessage': config['HTML_CONTENT_CODE']['msg'].format(platform=cf.platform),
    #             'Data': {},
    #             'RawStatusCode': resp.status_code,
    #             'RawContent': resp.content
    #             }
    
    #銀行資料欄位
    bank_data = html.find('table[class="m_tab"] > tr[class="m_title_over_co"] > td')
    bank_value = html.find('table[class="m_tab"] > tr[class="m_cen"] > td')
    if not bank_data:
        return {
                'IsSuccess': False,
                'ErrorCode': config['HTML_CONTENT_CODE']['code'],
                'ErrorMessage': config['HTML_CONTENT_CODE']['msg'].format(platform=cf.platform),
                'Data': {},
                'RawStatusCode': resp.status_code,
                'RawContent': resp.content
                }
    keys2 = [key.text for key in bank_data]
    values2 = [value.text for value in bank_value]
    if keys2 and values2 and len(keys2) != len(values2):
        return {
                'IsSuccess': False,
                'ErrorCode': config['HTML_CONTENT_CODE']['code'],
                'ErrorMessage': config['HTML_CONTENT_CODE']['msg'].format(platform=cf.platform),
                'Data': {},
                'RawStatusCode': resp.status_code,
                'RawContent': resp.content
                }
    bank_data = dict(zip(keys2, values2))
    #回傳結果
    return {
        'IsSuccess': True,
        'ErrorCode': config['SUCCESS_CODE']['code'],
        'ErrorMessage': config['SUCCESS_CODE']['msg'],
        'Data': {'base_data': base_data, 'bank_data': bank_data},
        'RawStatusCode': resp.status_code,
        'RawContent': resp.content
    }

#稽核查詢特定天數內是否有同IP多帳號的情形
@log_info
@catch_exception
def login_log(cf, url: str, data: dict, headers: dict={}, timeout: tuple=(3, 5), 
            endpoints: str='app/daili/adminsave/adminsys/login_log_jh.php', **kwargs) ->dict:
    '''LEBO【其他 >> 登入日志 >> 自動稽核】'''
    headers = headers or default_headers
    data = data or {}
    resp = session.get(url+endpoints, params=data, headers=headers, verify=False, timeout=30)
    resp.encoding = 'utf-8-sig'
    alert_search = alert_pattern.findall(resp.text)

    # 檢查回傳
    if 'logout.php' in resp.text:
        raise NotSignError('帐号已其他地方登入, 请重新登入')

    # 彈出失敗回傳(查詢時會回傳多個alert訊息在js中)
    if alert_search:
        # 回傳失敗訊息
        if '操作權限' in resp.text:
            return {
                'IsSuccess': False,
                'ErrorCode': config['PERMISSION_CODE']['code'],
                'ErrorMessage': f'{cf["platform"]} 显示：{alert_search[0]}',
                'Data': {},
                'RawStatusCode': resp.status_code,
                'RawContent': resp.content
            }
        else:
            return {
                'IsSuccess': False,
                'ErrorCode': config['HTML_CONTENT_CODE']['code'],
                'ErrorMessage': f'{cf["platform"]} 显示：{alert_search[0]}',
                'Data': {},
                'RawStatusCode': resp.status_code,
                'RawContent': resp.content
            }
    # 狀態碼錯誤
    if resp.status_code != 200:
        return {
            'IsSuccess': False,
            'ErrorCode': config['HTML_STATUS_CODE']['code'],
            'ErrorMessage': config['HTML_STATUS_CODE']['msg'].format(platform=cf.platform, status_code=resp.status_code),
            'Data': {},
            'RawStatusCode': resp.status_code,
            'RawContent': resp.content
        }
    html = requests_html.HTML(html=resp.text)
    #取得欄位名稱
    title = html.find('table > tr[class="m_title"] > td[class="table_bg"]')
    if title:
        title_name = [i.text for i in title]
    else:
        return {
                'IsSuccess': False,
                'ErrorCode': config['HTML_CONTENT_CODE']['code'],
                'ErrorMessage': config['HTML_CONTENT_CODE']['msg'].format(platform=cf.platform),
                'Data': {},
                'RawStatusCode': resp.status_code,
                'RawContent': resp.content
                }
    #獲取內容
    content = html.find('table > tr[class="m_cen"]')
    if not content or content[0].text == '暂无数据':
        return {
                'IsSuccess': True,
                'ErrorCode': config['SUCCESS_CODE']['code'],
                'ErrorMessage': config['SUCCESS_CODE']['msg'],
                'Data': {},
                'RawStatusCode': resp.status_code,
                'RawContent': resp.content
                }
                
    if len(content[0].text.split('\n')) == 1 and content[0].text.split('\n')[0] == '暂无数据':
        return {
                'IsSuccess': True,
                'ErrorCode': config['SUCCESS_CODE']['code'],
                'ErrorMessage': config['SUCCESS_CODE']['msg'],
                'Data': {},
                'RawStatusCode': resp.status_code,
                'RawContent': resp.content
                }

    #確認內容欄位數與名稱欄位數是否一致
    if len(content[0].text.split('\n')) != len(title_name):
        return {
                'IsSuccess': False,
                'ErrorCode': config['HTML_CONTENT_CODE']['code'],
                'ErrorMessage': config['HTML_CONTENT_CODE']['msg'].format(platform=cf.platform),
                'Data': {},
                'RawStatusCode': resp.status_code,
                'RawContent': resp.content
                }

    #欄位與內容結合成list(dict(data))
    contents = [dict(zip(title_name, data.text.split('\n'))) for data in content]

    #確認還有幾頁
    page_data = html.find('div[class="con_menu"] > form')[0].text if html.find('div[class="con_menu"] > form') else None
    if not page_data:
        return {
                'IsSuccess': False,
                'ErrorCode': config['HTML_CONTENT_CODE']['code'],
                'ErrorMessage': config['HTML_CONTENT_CODE']['msg'].format(platform=cf.platform),
                'Data': {},
                'RawStatusCode': resp.status_code,
                'RawContent': resp.content
                }        
    page = [re.search(r'(?<=)\d+(?<=)', txt).group()  for txt in page_data.split() if '/' in txt and '頁' in txt]
    if not page:
        return {
                'IsSuccess': False,
                'ErrorCode': config['HTML_CONTENT_CODE']['code'],
                'ErrorMessage': config['HTML_CONTENT_CODE']['msg'].format(platform=cf.platform),
                'Data': {},
                'RawStatusCode': resp.status_code,
                'RawContent': resp.content
                }

    #回傳結果
    return {
            'IsSuccess': True,
            'ErrorCode': config['SUCCESS_CODE']['code'],
            'ErrorMessage': config['SUCCESS_CODE']['msg'],
            'Data': {'total_page':int(page[0]),'content':contents},
            'RawStatusCode': resp.status_code,
            'RawContent': resp.content
            }


##分類本日投注&遊戲本日投注
@log_info
@catch_exception
def bet_record_v2(cf, url: str, data={}, endpoints='', timeout: tuple=(3, 5), method='get', **kwargs) -> dict:
    """LEBO【帳號管理 >> 輸贏查詢 >> 投注金額】
    """
    game_id = {}
    content = {}
    resp = getattr(session, method)(url + endpoints, data=data, headers=default_headers, timeout=30, verify=False)
    resp.encoding = 'utf-8-sig'
    # 狀態碼錯誤
    if resp.status_code != 200:
        return {
            'IsSuccess': False,
            'ErrorCode': config['HTML_STATUS_CODE']['code'],
            'ErrorMessage': config['HTML_STATUS_CODE']['msg'].format(platform=cf.platform, status_code=resp.status_code),
            'RawStatusCode': resp.status_code,
            'RawContent': resp.content
        }
    alert_search = alert_pattern.findall(resp.text)
    # 檢查回傳
    if 'logout.php' in resp.text:
        raise NotSignError('帐号已其他地方登入, 请重新登入')

    # 彈出失敗回傳(查詢時會回傳多個alert訊息在js中)
    if len(alert_search) == 1:
        # 回傳失敗訊息

        if '操作權限' in resp.text:
            return {
                'IsSuccess': False,
                'ErrorCode': config['PERMISSION_CODE']['code'],
                'ErrorMessage': f'{cf["platform"]} 显示：{alert_search[0]}',
                'Data': {},
                'RawStatusCode': resp.status_code,
                'RawContent': resp.content
            }
        else:
            return {
                'IsSuccess': False,
                'ErrorCode': config['HTML_CONTENT_CODE']['code'],
                'ErrorMessage': f'{cf["platform"]} 显示：{alert_search[0]}',
                'Data': {},
                'RawStatusCode': resp.status_code,
                'RawContent': resp.content
            }
    html = requests_html.HTML(html=resp.text)
    # 爛位內容
    rows = [[td.text for td in tr.find('td:not([colspan])')] for tr in html.find('tr.m_rig:not([align])')]
    rows_name = [[td.text.split('：')[0] for td in tr.find('td[colspan]')][0] for tr in html.find('tr.m_rig:not([align])')]
    rows1 = [[td for td in tr.find('td')] for tr in html.find('tr.m_rig:not([align])')]
    colspan = [rows1[i][0].attrs['colspan'] for i in range(len(rows1))]
    if not colspan:
        return {
                'IsSuccess': False,
                'ErrorCode': config['HTML_CONTENT_CODE']['code'],
                'ErrorMessage': config['HTML_CONTENT_CODE']['msg'].format(platform=cf.platform),
                'Data': {},
                'RawStatusCode': resp.status_code,
                'RawContent': resp.content
                }

    # 遊戲代號
    for option in html.find('option'):
        game_id[option.text.split(' - ')[-1]] = option.attrs['value']

    # title欄位
    title = [td.text for td in html.find('tr.m_title > td')]

    # 查詢各筆注單資料
    gamecategory = bettingbonus_support[data['gametype']] if data.get('gametype') else ''
    if gamecategory:
        bet_content = [recode.text.split('\n') for recode in html.find('tr.m_rig[align="left"]')]
        if bet_content and len(title) != len(bet_content[0]):
            return {
                    'IsSuccess': False,
                    'ErrorCode': config['HTML_CONTENT_CODE']['code'],
                    'ErrorMessage': config['HTML_CONTENT_CODE']['msg'].format(platform=cf.platform),
                    'Data': {},
                    'RawStatusCode': resp.status_code,
                    'RawContent': resp.content
                    }
                    
        recode = [dict(zip(title, i)) for i in bet_content]
        for i in recode:
            i['gamecategory'] = gamecategory
        content['recode'] = recode

    # 總共頁數
    page_data = html.find('div[class="con_menu"] > form')[0].text if html.find('div[class="con_menu"] > form') else None
    if not page_data:
        return {
                'IsSuccess': False,
                'ErrorCode': config['HTML_CONTENT_CODE']['code'],
                'ErrorMessage': config['HTML_CONTENT_CODE']['msg'].format(platform=cf.platform),
                'Data': {},
                'RawStatusCode': resp.status_code,
                'RawContent': resp.content
                }
    total_page = [re.search(r'(?<=)\d+(?<=)', txt).group()  for txt in page_data.split('\n') if '/' in txt and '頁' in txt]
    if not total_page:
        return {
                'IsSuccess': False,
                'ErrorCode': config['HTML_CONTENT_CODE']['code'],
                'ErrorMessage': config['HTML_CONTENT_CODE']['msg'].format(platform=cf.platform),
                'Data': {},
                'RawStatusCode': resp.status_code,
                'RawContent': resp.content
                }
    total_page = total_page[0]
    
    content['total_page'] = int(total_page)

    # 合拼總計小計欄位
    total_title = title[int(colspan[0]):]
    for i in range(len(rows)):
        dic = dict(zip(total_title, rows[i]))
        content[rows_name[i]] = dic

    content['遊戲ID'] = game_id
    return {
        'IsSuccess': True,
        'ErrorCode': config['SUCCESS_CODE']['code'],
        'ErrorMessage': config['SUCCESS_CODE']['msg'],
        'Data': content,
        'RawStatusCode': resp.status_code,
        'RawContent': resp.content
        }


##廣播會員
@log_info
@catch_exception
def msg_add(cf, url: str, data:dict, endpoints:str='app/cash/msg_add.php', timeout: tuple=(3, 5), **kwargs) -> dict:
    '''LEBO【其他 >> 會員消息 >> 發佈新消息】
    '''
    resp = session.post(url+endpoints, data=data, headers=default_headers, timeout=30, verify=False)
    resp.encoding = 'utf-8-sig'
    if resp.status_code != 200:
        return {
            'IsSuccess': False,
            'ErrorCode': config['HTML_STATUS_CODE']['code'],
            'ErrorMessage': config['HTML_STATUS_CODE']['msg'].format(platform=cf.platform, status_code=resp.status_code),
            'RawStatusCode': resp.status_code,
            'RawContent': resp.content
        }

    alert_search = alert_pattern.findall(resp.text)
     # 檢查回傳
    if 'logout.php' in resp.text:
        raise NotSignError('帐号已其他地方登入, 请重新登入')

    # 彈出失敗回傳(查詢時會回傳多個alert訊息在js中)
    if len(alert_search) == 1:
        # 回傳失敗訊息

        if '操作權限' in resp.text:
            return {
                'IsSuccess': False,
                'ErrorCode': config['PERMISSION_CODE']['code'],
                'ErrorMessage': f'{cf["platform"]} 显示：{alert_search[0]}',
                'Data': {},
                'RawStatusCode': resp.status_code,
                'RawContent': resp.content
            }
        elif '消息發送成功' in resp.text:
            return {
                'IsSuccess': True,
                'ErrorCode': config['SUCCESS_CODE']['code'],
                'ErrorMessage': '',
                'Data': {},
                'RawStatusCode': resp.status_code,
                'RawContent': resp.content
            }
        else:
            return {
                'IsSuccess': False,
                'ErrorCode': config['HTML_CONTENT_CODE']['code'],
                'ErrorMessage': f'{cf["platform"]} 显示：{alert_search[0]}',
                'Data': {},
                'RawStatusCode': resp.status_code,
                'RawContent': resp.content
            }
    else:
        return {
                'IsSuccess': False,
                'ErrorCode': config['HTML_CONTENT_CODE']['code'],
                'ErrorMessage': f'{cf["platform"]} 显示：{alert_search}',
                'Data': {},
                'RawStatusCode': resp.status_code,
                'RawContent': resp.content
        }

#注單網址查詢
@log_info
@catch_exception
def app_mgame(cf, url: str, data: dict, headers: dict = {},
                   timeout: tuple = (3, 5), endpoints: str = '', allow_redirects: bool = True, **kwargs) -> dict:

    '''LEBO【現金系統 >> 第三方查詢 >> 第三方注單查詢】
    Args:
        url : (必填) LEBO 後台網址，範例格式: 'https://shaba888.jf-game.net/'
        data : (必填) LEBO API 網址參數, 使用字典傳入, 範例格式:
            {
            "account": "abc123",                  # 會員帳號
            "no": "517443637888",                 # 注單號碼
            "searchbtn": "查詢",                   # 固定值
            }
        headers : LEBO API 表頭, 使用字典傳入, 預設為: {
            'Content-Type': 'application/x-www-form-urlencoded',
            'X-Requested-With': 'XMLHttpRequest'
        }
    Returns:
    {
        'IsSuccess': 是(查詢成功/布林值)否(查詢失敗/布林值),
        'ErrorCode': 錯誤代碼(如果前述布林值為False才有內容/文字格式),
        'ErrorMessage': 錯誤原因(如果前述布林值為False才有內容/文字格式),
        'Data': {
                'Server': 'Apache', 
                'Date': 'Thu, 22 Apr 2021 07:18:08 GMT', 
                'Content-Type': 'text/html; charset=utf-8', 
                'Transfer-Encoding': 'chunked', 
                'Connection': 'keep-alive', 
                'X-Request-ID': 'b8c6a44f04b613e500591828b5b2ab44', 
                'X-Protected-By': 'X-Protected-By', 
                'Set-Cookie': 'PHPSESSID=nc4fd02v6ijbavt8reupcu5s25; path=/', 
                'Expires': 'Thu, 19 Nov 1981 08:52:00 GMT', 
                'Cache-Control': 'no-store, no-cache, must-revalidate', 
                'Pragma': 'no-cache', 
                'location': '../daili/adminsave/bet_record_v2/bbin.php?uid=2a47757b47e98bb17a678ef593ef&mid=68582666&gtype=5902&no=568551760965'
                }
        'RawStatusCode': LEBO原始回傳狀態碼,
        'RawContent': LEBO原始回傳內容
    }
    '''

    headers = headers or default_headers
    data = data or {}
    resp = session.post(url + endpoints, data=urlencode(data), headers=headers, verify=False, timeout=30, allow_redirects=allow_redirects)
    resp.encoding = 'utf-8-sig'

    # 狀態碼錯誤
    if resp.status_code not in [302, 200]:
        raise NullError(f'status_code={resp.status_code}')
    
    alert_search = alert_pattern.findall(resp.text)
    # 檢查回傳
    
    if 'logout.php' in resp.text:
        raise NotSignError('帐号已其他地方登入, 请重新登入')
    # 彈出失敗回傳(查詢時會回傳多個alert訊息在js中)
    if len(alert_search) == 1:
        # 回傳失敗訊息
        if '操作權限' in resp.text:
            return {
                'IsSuccess': False,
                'ErrorCode': config['PERMISSION_CODE']['code'],
                'ErrorMessage': f'{cf["platform"]} 显示：{alert_search[0]}',
                'Data': {},
                'RawStatusCode': resp.status_code,
                'RawContent': resp.content
            }
        elif '注單編號不存在' in resp.text:
            # if '旋转注单' in  mod_name or '旋轉注單' in mod_name:
            return {
                    'IsSuccess': False,
                    'ErrorCode': config['WAGERS_NOT_FOUND']['code'],
                    'ErrorMessage': config['WAGERS_NOT_FOUND']['msg'],
                    'Data': {},
                    'RawStatusCode': resp.status_code,
                    'RawContent': resp.content
                    }

        return {
            'IsSuccess': False,
            'ErrorCode': config['SUCCESS_CODE']['code'],
            'ErrorMessage': f'{cf["platform"]} 显示：{alert_search[0]}',
            'Data': {},
            'RawStatusCode': resp.status_code,
            'RawContent': resp.content
        }
    if resp.text in ['没有此注单记录！','ERROR:2','查詢出錯','單號不存在！']:
        return {
                    'IsSuccess': False,
                    'ErrorCode': config['WAGERS_NOT_FOUND']['code'],
                    'ErrorMessage': config['WAGERS_NOT_FOUND']['msg'],
                    'Data': {},
                    'RawStatusCode': resp.status_code,
                    'RawContent': resp.content
                    }
    elif '注单与会员帐号不符' in resp.text:
                return {
                    'IsSuccess': False,
                    'ErrorCode': config['USER_WAGERS_NOT_MATCH']['code'],
                    'ErrorMessage': config['USER_WAGERS_NOT_MATCH']['msg'],
                    'Data': {},
                    'RawStatusCode': resp.status_code,
                    'RawContent': resp.content
                    }

    return {
            'IsSuccess': True,
            'ErrorCode': config['SUCCESS_CODE']['code'],
            'ErrorMessage': config['SUCCESS_CODE']['msg'],
            'Data': resp,
            'RawStatusCode': resp.status_code,
            'RawContent': resp.content
            }
            
# 處理BBIN电子注單前的iframe網址
@log_info
@catch_exception
def iframe_processing(cf, resp, **kwargs) -> dict:
    '''LEBO【現金系統 >> 第三方查詢 >> 第三方注單查詢 >> BBIN电子】解析iframe'''
    html = requests_html.HTML(html=resp.text)
    iframe = html.find('iframe', first=True)
    if not iframe or not iframe.attrs.get('src'):
        return {
            'IsSuccess': False,
            'ErrorCode': config['HTML_CONTENT_CODE']['code'],
            'ErrorMessage': config['HTML_CONTENT_CODE']['msg'].format(platform=cf.platform),
            'Data': {},
            'RawStatusCode': resp.status_code,
            'RawContent': resp.content
            }
    url = iframe.attrs.get('src')
    resp = requests.get(url)
    if resp.status_code != 200:
        return {
            'IsSuccess': False,
            'ErrorCode': config['HTML_STATUS_CODE']['code'],
            'ErrorMessage': config['HTML_STATUS_CODE']['msg'].format(platform=cf.platform, status_code=resp.status_code),
            'Data': {},
            'RawStatusCode': resp.status_code,
            'RawContent': resp.content
            }
    return {
            'IsSuccess': True,
            'ErrorCode': config['SUCCESS_CODE']['code'],
            'ErrorMessage': config['SUCCESS_CODE']['msg'],
            'Data': resp,
            'RawStatusCode': resp.status_code,
            'RawContent': resp.content
            }

# 查詢BBIN电子注單
@log_info
@catch_exception
def process_bbin(cf, resp, iframe_content, **kwargs) -> dict:
    """LEBO【現金系統 >> 第三方查詢 >> 第三方注單查詢 >> BBIN电子】
    """
    columns = []
    values = []
    columns1 = []
    values1 = []

    html = requests_html.HTML(html=resp.text)
    gamename = html.search('游戏类别：{}&nbsp')[0]
    if gamename not in [re.search(r"\[.*\](?P<key>.*)", i).group('key') for i in support['BBIN电子']['gamename'].values()]:
        return {
                'IsSuccess': False,
                'ErrorCode': config['GAME_ERROR']['code'],
                'ErrorMessage': config['GAME_ERROR']['msg'].format(GameName=gamename),
                'Data': {},
                'RawStatusCode': resp.status_code,
                'RawContent': resp.content
                }
    html = requests_html.HTML(html=iframe_content.text)
    table = html.find('table[class*="table"]')
    for tr in table[0].find('tr'):
        if len(tr.find('td')) == 3:
            columns.append(tr.find('td')[-2].text)
            values.append(tr.find('td')[-1].text)
        elif len(tr.find('td')) == 2:
            columns.append(tr.find('td')[0].text)
            values.append(tr.find('td')[-1].text)
        else:
            logger.warning('網頁異動')
            return {
                'IsSuccess': False,
                'ErrorCode': config['HTML_CONTENT_CODE']['code'],
                'ErrorMessage': config['HTML_CONTENT_CODE']['msg'].format(platform=cf.platform),
                'Data': {},
                'RawStatusCode': resp.status_code,
                'RawContent': resp.content
                }
    result = dict(zip(columns, values))
    if '总共' in result:
        result['下注资讯'] = result.pop('总共')
    for tr in table[-1].find('tr'):
        if not tr.find('td'):
            continue
        if tr.find('td')[0].text.isdigit():
            columns1.append(tr.find('td')[0].text)
            values1.append(tr.find('td')[1].text)

    # table2_tr = table[-1].find('tr[class="result"]')
    # if len(table2_tr) != 1:
    #     return {
    #             'IsSuccess': False,
    #             'ErrorCode': config['HTML_CONTENT_CODE']['code'],
    #             'ErrorMessage': config['HTML_CONTENT_CODE']['msg'].format(platform=cf.platform),
    #             'Data': {},
    #             'RawStatusCode': resp.status_code,
    #             'RawContent': resp.content
    #             }
    # PayoutAmount = table2_tr[0].text.split('=')[1].strip()

    result['游戏类别'] = gamename
    result['消除次數'] = columns1
    result['消除符號'] = values1
    # result['总派彩'] = PayoutAmount
    result['总派彩'] = [i.text.split('\n')[0] for i in html.find('tr.result')[0].find('td')][0].split('=')[1].strip()


    return {
            'IsSuccess': True,
            'ErrorCode': config['SUCCESS_CODE']['code'],
            'ErrorMessage': config['SUCCESS_CODE']['msg'],
            'Data': result,
            'RawStatusCode': resp.status_code,
            'RawContent': resp.content
            }


#CQ9电子
@log_info
@catch_exception
def detail_cqgame_cc_odh5(cf, url: str, data: dict,
                          timeout: tuple = (3, 5), endpoints='/api/inquire/v1/db/wager', **kwargs) -> dict:
    """LEBO【現金系統 >> 第三方查詢 >> 第三方注單查詢 >> CQ9电子】
    """
    resp = requests.get(url+endpoints, params=data, timeout=30, verify=False)

    # 狀態碼錯誤
    if resp.status_code != 200:
        return {
            'IsSuccess': False,
            'ErrorCode': config['HTML_STATUS_CODE']['code'],
            'ErrorMessage': config['HTML_STATUS_CODE']['msg'].format(platform=cf.platform, status_code=resp.status_code),
            'Data': {},
            'RawStatusCode': resp.status_code,
            'RawContent': resp.content
        }
    js_resp = json.loads(resp.text)
    return {
            'IsSuccess': True,
            'ErrorCode': config['SUCCESS_CODE']['code'],
            'ErrorMessage': config['SUCCESS_CODE']['msg'],
            'Data': js_resp,
            'RawStatusCode': resp.status_code,
            'RawContent': resp.content
            }

#JDB电子
@log_info
@catch_exception
def api_runDao(cf, url: str, data: dict,
                timeout: tuple = (3, 5), **kwargs) -> dict:
    """LEBO【現金系統 >> 第三方查詢 >> 第三方注單查詢 >> JDB电子】
    """
    headers = {
            'content-type': 'application/json;charset=UTF-8'
    }

    resp = requests.post(url, data=json.dumps(data), headers=headers, verify=False, timeout=30)
    # 狀態碼錯誤
    if resp.status_code != 200:
        return {
            'IsSuccess': False,
            'ErrorCode': config['HTML_STATUS_CODE']['code'],
            'ErrorMessage': config['HTML_STATUS_CODE']['msg'].format(platform=cf.platform, status_code=resp.status_code),
            'Data': {},
            'RawStatusCode': resp.status_code,
            'RawContent': resp.content
        }
    js_resp = json.loads(resp.text)
    return {
            'IsSuccess': True,
            'ErrorCode': config['SUCCESS_CODE']['code'],
            'ErrorMessage': config['SUCCESS_CODE']['msg'],
            'Data': js_resp,
            'RawStatusCode': resp.status_code,
            'RawContent': resp.content
            }

#所有分類投注
@log_info
@catch_exception
def member_report(cf, url: str, params: dict, headers: dict={},
                         timeout: tuple=(3, 5), endpoints: str='app/daili/adminsave/member_report/result.php', **kwargs) -> dict:

    '''LEBO【帳號管理 >> 輸贏查詢】
    Args:
        url : (必填) LEBO 後台網址，範例格式: 'https://shaba888.jf-game.net/'
        params : (必填) LEBO API 網址參數, 使用字典傳入, 範例格式: {
            "date_start": "2020-05-26 00:00:00",        # 搜尋時間(起)
            "date_end": "2020-05-26 23:59:59",          # 搜尋時間(迄)
            "account": "hm81448072",                    # 會員帳號
            "game[]": ['zq', 'cp', 'lebo0'],            # 遊戲類別
            "model": '1',                               #
            "searchbtn": '查詢',                        # 固定值
        }
        headers : LEBO API 表頭, 使用字典傳入, 預設為: {
            'Content-Type': 'application/x-www-form-urlencoded',
            'X-Requested-With': 'XMLHttpRequest'
        }
    Returns:
        'IsSuccess': 是(查詢成功/布林值)否(查詢失敗/布林值),
        'ErrorCode': 錯誤代碼(如果前述布林值為False才有內容/文字格式)
        'ErrorMessage': 錯誤原因(如果前述布林值為False才有內容/文字格式)
        'Data': 查詢的回傳內容, 字典格式, ex:
                (除總計以外其他欄位需要detail為True傳入才有)
                {
                    "體育": {
                        "類別": "體育",
                        "投注金額": "0",
                        "有效投注": "0",
                        "會員輸贏": "0",
                        "彩池抽水": "0",
                        "彩金": "0"
                    },
                    "總計：": {
                        "類別": "總計：",
                        "投注金額": "31473.5",
                        "有效投注": "31453.5778",
                        "會員輸贏": "-1228.76",
                        "彩池抽水": "17.8918",
                        "彩金": "0"
                    }
                }
        'RawStatusCode': LEBO原始回傳狀態碼
        'RawContent': LEBO原始回傳內容
    '''

    headers = headers or default_headers
    params = params or {}

    resp = session.get(url + endpoints, params=params, headers=headers, verify=False, timeout=30)
    alert_search = alert_pattern.findall(resp.text)

    # 檢查回傳
    if 'logout.php' in resp.text:
        raise NotSignError('帐号已其他地方登入, 请重新登入')

    # 彈出失敗回傳(查詢時會回傳多個alert訊息在js中)
    if alert_search:
        # 回傳失敗訊息

        if '操作權限' in resp.text:
            return {
                'IsSuccess': False,
                'ErrorCode': config['PERMISSION_CODE']['code'],
                'ErrorMessage': f'{cf["platform"]} 显示：{alert_search[0]}',
                'Data': {},
                'RawStatusCode': resp.status_code,
                'RawContent': resp.content
            }
        else:
            return {
                'IsSuccess': False,
                'ErrorCode': config['SUCCESS_CODE']['code'],
                'ErrorMessage': f'{cf["platform"]} 显示：{alert_search[0]}',
                'Data': {},
                'RawStatusCode': resp.status_code,
                'RawContent': resp.content
            }

    # 狀態碼錯誤
    if resp.status_code != 200:
        raise NullError(f'status_code={resp.status_code}')

    html = requests_html.HTML(html=resp.text)
    # 欄位
    columns = [th.text for th in html.find('th')]
    # 明細
    datas = [[td.text for td in tr.find('tr:not(.table_total) > td')] for tr in html.find('tr.m_cen')]
    datas1 = [[td for td in tr.find('tr:not(.table_total) > td > a')] for tr in html.find('tr.m_cen')]
    for i in range(len(datas1)):
        href = datas1[i][0].attrs['href'].split(",'/")[1].split("')")[0]
        datas[i].append(href)
    # 總計
    if html.find('tr.table_total'):
        _total = [td.text for td in html.find('tr.table_total > td')]
        total = dict(zip(columns, _total))
    else:
        _total = {c: 0 for c in columns}
        total = {c: 0 for c in columns}
    # 檢查總計欄位數
    if len(columns) != len(_total):
        return {
            'IsSuccess': False,
            'ErrorCode': config['HTML_CONTENT_CODE']['code'],
            'ErrorMessage': config['HTML_CONTENT_CODE']['msg'].format(platform=cf.platform),
            'Data': {},
            'RawStatusCode': resp.status_code,
            'RawContent': resp.content
        }
    columns.append('連結')
    # 回傳
    res = {'總計：': total}
    # 表格逐行檢查欄位數、轉為字典、存入res中
    for row in datas:
        # 轉為字典
        dic = dict(zip(columns, row))
        # 存入res中
        res[dic['類型']] = dic
        # 檢查欄位數
        if len(columns) != len(row):
            return {
                'IsSuccess': False,
                'ErrorCode': config['HTML_CONTENT_CODE']['code'],
                'ErrorMessage': config['HTML_CONTENT_CODE']['msg'].format(platform=cf.platform),
                'Data': {},
                'RawStatusCode': resp.status_code,
                'RawContent': resp.content
                }
    # 回傳結果
    return {
        'IsSuccess': True,
        'ErrorCode': config['SUCCESS_CODE']['code'],
        'ErrorMessage': config['SUCCESS_CODE']['msg'],
        'Data': res,
        'RawStatusCode': resp.status_code,
        'RawContent': resp.content
    }