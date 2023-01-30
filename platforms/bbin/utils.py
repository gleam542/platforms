from . import CODE_DICT as config
from inspect import signature
from functools import partial
from functools import wraps
from urllib import parse
import threading
import werdsazxc
import requests
import inspect
import logging
import json
import time
import re
requests.packages.urllib3.disable_warnings()
logger = logging.getLogger('robot')


class ThreadProgress(threading.Thread):
    def __init__(self, cf, mod_key, detail=True):
        super().__init__()
        self.lst = []
        self.cf = cf
        self.mod_key = mod_key
        self.running = True
        self.detail = detail

    def stop(self):
        self.running = False

    def run(self):
        from gui.Apps import return_schedule
        # 新版進度回傳API保留所有進度狀態, 全部回傳
        if self.detail:
            while self.running or self.lst:
                try:
                    time.sleep(.1)
                    item = self.lst.pop(0)
                    return_schedule(self.cf, self.mod_key, **item)
                except IndexError as e:
                    continue
        # 舊版進度回傳API保留最後一個進度狀態, 只回傳最後一個
        else:
            while self.running:
                try:
                    time.sleep(1)
                    item = self.lst.pop()
                    self.lst.clear()
                    return_schedule(self.cf, self.mod_key, **item)
                except IndexError as e:
                    continue


alert_pattern = re.compile('(?<=alert\([\'\"]).*?(?=[\'\"]\))')
default_headers = {
    'content-type': 'application/json;charset=UTF-8',
    'x-requested-with': 'XMLHttpRequest',
}
def urlparser(url):
    return {
        'scheme': parse.urlparse(url).scheme,
        'hostname': parse.urlparse(url).hostname,
        'path': parse.urlparse(url).path, 
        'query': dict([
            parse.unquote(qs).split('=') for qs in
            parse.urlparse(url).query.split('&')
        ]),
    }
class NotSignError(Exception):
    '''自定義當出現帳號被登出時, 自動登入GPK平台'''
    pass

def log_info(func=None, log_args=True, log_result=True, **kw):
    if func is None:
        return partial(log_info, log_args=log_args, log_result=log_result, **kw)

    @wraps(func)
    def wrapper(*args, **kwargs):
        # 讀取說明文件第一行, 作為函數名後續紀錄log使用
        funcname = func.__doc__.split('\n')[0]
        # 對應傳入參數, 產生參數字典
        sig = signature(func)
        bound = sig.bind_partial(*args, **kwargs)
        bound.apply_defaults()
        arguments = bound.arguments
        # 紀錄擋排除參數陣列
        exclud_args = ["url", "endpoints", "timeout", "args", "kwargs"]

        # 紀錄參數
        if log_args:
            logger.info(f'{funcname} 網址: {arguments.get("url")}{arguments.get("endpoints")}')

        # 執行函數
        result = func(*args, **kwargs)

        # 有錯誤則打印整串返回的內容, 打印完後將原始資料刪除
        if result.get("IsSuccess") is False and result['ErrorCode'] != config.SUCCESS_CODE.code:
            if result.get('RawStatusCode'):
                logger.warning(f"網頁原始狀態碼為: {result.get('RawStatusCode')}")
            if result.get('RawContent'):
                logger.warning(f"網頁原始內容為: {result.get('RawContent')}")

        if result.get('RawStatusCode'):
            del result["RawStatusCode"]
        if result.get('RawContent'):
            del result["RawContent"]

        # 紀錄結果
        if log_result:
            r = {
                'IsSuccess': result['IsSuccess'],
                'ErrorCode': result['ErrorCode'],
                'ErrorMessage': result['ErrorMessage'],
                'Data': {
                    k: v if kw.get(k, True) else '...'
                    for k, v in result.get('Data', {}).items()
                }
            }
            logger.info(f'{funcname} 返回: {r}')

        return result
    return wrapper


def catch_exception(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        # 讀取說明文件第一行, 作為函數名後續紀錄log使用
        funcname = func.__doc__.split('\n')[0]
        # 對應傳入參數, 產生參數字典
        sig = signature(func)
        bound = sig.bind_partial(*args, **kwargs)
        bound.apply_defaults()
        arguments = bound.arguments
        # 計算錯誤次數
        count = 1
        cf = arguments['cf']

        while count <= cf['retry_times']:
            try:
                result = func(*args, **kwargs)
                break

            # 檢查schema是否輸入
            except requests.exceptions.MissingSchema as e:
                logger.debug(f'{e.__class__.__name__} {e}')
                return {
                    'IsSuccess': False,
                    'ErrorCode': config.EXCEPTION_CODE.code,
                    'ErrorMessage': f'平台设定错误, 通讯协定(http或https)未输入',
                }
            # 檢查schema是否合法
            except requests.exceptions.InvalidSchema as e:
                logger.debug(f'{e.__class__.__name__} {e}')
                return {
                    'IsSuccess': False,
                    'ErrorCode': config.EXCEPTION_CODE.code,
                    'ErrorMessage': f'平台设定错误, 通讯协定(http或https)无法解析',
                }
            # 檢查網址是否合法
            except requests.exceptions.InvalidURL as e:
                logger.debug(f'{e.__class__.__name__} {e}')
                return {
                    'IsSuccess': False,
                    'ErrorCode': config.EXCEPTION_CODE.code,
                    'ErrorMessage': f'平台设定错误, 无法解析',
                }
            # 發生重導向異常
            except requests.exceptions.TooManyRedirects as e:
                logger.debug(f'{e.__class__.__name__} {e}')
                return {
                    'IsSuccess': False,
                    'ErrorCode': config.EXCEPTION_CODE.code,
                    'ErrorMessage': f'平台设定错误, 发生重导向异常',
                }
            # 捕捉被登出
            except (NotSignError, json.JSONDecodeError) as e:
                from .module import session
                from .module import login
                if type(e) == json.JSONDecodeError:
                    logger.info(f'json.JSONDecodeError: {e.doc}')
                if (cf['need_backend_otp'] is False and
                    hasattr(session, 'url') and
                    hasattr(session, 'acc') and
                    hasattr(session, 'pw')):
                    login(cf=cf, url=session.url, acc=session.acc, pw=session.pw, otp='')
                    continue
                return {
                    'IsSuccess': False,
                    'ErrorCode': config.SIGN_OUT_CODE.code,
                    'ErrorMessage': config.SIGN_OUT_CODE.msg.format(platform=cf['platform']),
                }
            # 捕捉連線異常
            except (requests.exceptions.ConnectionError,
                    requests.exceptions.Timeout,
                    requests.exceptions.ContentDecodingError) as e:
                logger.debug(f'{e.__class__.__name__} {e}')
                logger.debug(f'{e.__class__.__name__} ({count}/3)...')
                if count >= 3:
                    return {
                        'IsSuccess': False,
                        'ErrorCode': config.CONNECTION_CODE.code,
                        'ErrorMessage': config.CONNECTION_CODE.msg,
                    }
                if f'与{cf.get("platform")}连线异常...' not in cf['error_msg']:
                    cf['error_msg'].append(f'与{cf.get("platform")}连线异常...')
                time.sleep(3)
                count += 1
            # 捕捉程式錯誤
            except Exception as e:
                werdsazxc.log_trackback()
                local_envs = inspect.getinnerframes(e.__traceback__)
                local_envs = [frame for frame in local_envs if frame.function == func.__name__]
                if local_envs:
                    local_envs = local_envs[-1].frame.f_locals
                    resp = local_envs.get('resp')
                    if resp:
                        status_code = resp.status_code
                        content = resp.content
                    else:
                        status_code = '函數未設定resp變數, 請修改變數命名規則'
                        content = '函數未設定resp變數, 請修改變數命名規則'
                else:
                    status_code = ''
                    content = ''
                return {
                    'IsSuccess': False,
                    'ErrorCode': config.EXCEPTION_CODE.code,
                    'ErrorMessage': f'未知异常- {e.__class__.__name__}: {e}',
                    'RawStatusCode': status_code,
                    'RawContent': content
                }
        return result
    return wrapper


# 裝飾器, 加上後每隔60秒會延長一次liveTime
def keep_connect(func):
    class Keep(threading.Thread):
        def __init__(self, cf):
            super().__init__()
            self.cf = cf
        def run(self):
            from gui.Apps import keep_connect
            t = threading.current_thread()
            while getattr(t, 'running', True) and self.cf['token']:
                keep_connect(self.cf)
                time.sleep(55)

    @wraps(func)
    def wrapper(*args, **kwargs):
        # 讀取說明文件第一行, 作為函數名後續紀錄log使用
        funcname = func.__doc__.split('\n')[0]
        # 對應傳入參數, 產生參數字典
        sig = signature(func)
        bound = sig.bind_partial(*args, **kwargs)
        bound.apply_defaults()
        arguments = bound.arguments
        # 建立執行續進行保持連線
        cf = arguments['cf']
        t = Keep(cf)
        t.start()
        # 執行功能
        result = func(*args, **kwargs)
        # 停止執行續
        t.running = False
        return result
    return wrapper


MAX_WORKERS = 3
BETSLIP_ALLOW_LIST = {
    # -----電子-----
    # SW電子、DF電子 住單號碼發現過同時存在大小寫
    # 但是注單查詢大小寫錯誤仍然能夠查詢
    '5': 'BB电子',
    '20': 'PT电子',
    '28': 'GNS电子',
    '29': 'ISB电子',
    '32': 'HB电子',
    '37': 'PP电子',
    '39': 'JDB电子',
    '40': 'AG电子',
    '41': '大满贯电子',
    '44': 'SG电子',
    '46': 'SW电子',
    '50': 'WM电子',
    '52': 'CQ9电子',
    '53': 'KA电子',
    '58': 'PG电子',
    '59': 'FG电子',
    '71': 'MT电子',
    '76': 'XBB电子',
    '79': 'PS电子',
    # '85': 'DF电子',  #★2022-08-17 下架
    '82': 'MG电子', # 局查詢無日期查詢
    '95': 'BNG电子',
    '107': 'BBP电子',
    '114': 'FC电子',
    '116': 'VA电子',
    '117': '榕盈电子',
    '118': 'RSG电子',
    '124': 'RiCH88电子',
    '128': 'AT电子',
    '130': 'AE电子',
    '132': 'RT电子',
    '133': 'HC电子',
    '138': 'TP电子',

    # -----視訊-----
    # '3': 'BB视讯', 只有局號，需要確定欄位後才能開發
    '19': 'AG视讯',
    '22': '欧博视讯',
    '36': 'BG视讯',
    '47': 'EVO视讯',
    '54': 'eBET视讯',
    '72': 'MG视讯',
    '75': 'XBB视讯',
    '93': 'NBB区块链',
    '104': 'N2视讯',
    '105': 'AE视讯',
    '127': 'PT视讯',
    '129': 'XG视讯', # 新版沒單，沒實測過
    '131': '亚星CQ9视讯',

    # -----捕魚-----
    # '38': 'BB捕鱼大师', 查詢的網址不同，需要另外開發
    '30': 'BB捕鱼达人',

    # -----體育----- 都不能查
    # '109': 'BB体育'
    # '31': 'New BB体育'
    # '55': '波音体育',
    # '106': '沙巴体育',
    # '113': '新宝体育',

    # -----彩票-----
    '73': 'XBB彩票',
    # '12': 'BB彩票', # 局查詢無日期查詢, 會員查詢視窗與其他不同
    # '45': 'VR彩票', # 局查詢無日期查詢, 會員查詢視窗與其他不同
    '134': 'BB数位趋势',

    # -----棋牌-----
    '49': '开元棋牌',
    '64': 'MT棋牌',
    '66': 'BB棋牌',
    '68': 'JDB棋牌',
    '69': 'FG棋牌',
    '77': 'ACE棋牌',
    '81': '幸运棋牌',
    '83': '乐游棋牌',
    '115': '欢乐棋牌',
    '112': 'BBP棋牌',
    '126': '百胜棋牌',
    '135': 'KX棋牌',
    '151': 'WG棋牌',
}
BETSLIP_RAWWAGERS_BARID = {
    '19': ('3', 'ReferenceID'),
    '22': ('3', 'ReferenceID'),
    '36': ('3', 'ReferenceID'),
    '47': ('3', 'ReferenceID'),
    '54': ('3', 'ReferenceID'),
    '72': ('3', 'ReferenceID'),
    '75': ('3', 'ReferenceID'),
    '93': ('3', 'ReferenceID'),
    '104': ('3', 'ReferenceID'),
    '105': ('3', 'ReferenceID'),
    '127': ('3', 'ReferenceID'),
    '129': ('3', 'ReferenceID'),
    '131': ('3', 'ReferenceID'),
    '5': ('2', 'Wagersid'),
    '30': ('4', 'Wagersid'),
    '20': ('3', 'ReferenceID'),
    '28': ('3', 'ReferenceID'),
    '29': ('3', 'ReferenceID'),
    '32': ('3', 'ReferenceID'),
    '37': ('3', 'ReferenceID'),
    '39': ('3', 'ReferenceID'),
    '40': ('3', 'ReferenceID'),
    '41': ('3', 'ReferenceID'),
    '44': ('3', 'ReferenceID'),
    '46': ('3', 'ReferenceID'),
    '50': ('3', 'ReferenceID'),
    '52': ('3', 'ReferenceID'),
    '53': ('3', 'ReferenceID'),
    '58': ('3', 'ReferenceID'),
    '59': ('3', 'ReferenceID'),
    '71': ('3', 'ReferenceID'),
    '76': ('4', 'Wagersid'),
    '79': ('3', 'ReferenceID'),
    '82': ('3', 'ReferenceID'),
    # '85': ('3', 'ReferenceID'),  #★2022-08-17 下架
    '95': ('3', 'ReferenceID'),
    '107': ('3', 'ReferenceID'),
    '114': ('3', 'ReferenceID'),
    '116': ('3', 'ReferenceID'),
    '117': ('3', 'ReferenceID'),
    '118': ('3', 'ReferenceID'),
    '124': ('3', 'ReferenceID'),
    '128': ('3', 'ReferenceID'),
    '130': ('3', 'ReferenceID'),
    '132': ('3', 'ReferenceID'),
    '133': ('3', 'ReferenceID'),
    '138': ('3', 'ReferenceID'),
    '73': ('4', 'Wagersid'),
    '134': ('4', 'Wagersid'),
    '49': ('3', 'ReferenceID'),
    '64': ('3', 'ReferenceID'),
    '66': ('3', 'ReferenceID'),
    '68': ('3', 'ReferenceID'),
    '69': ('3', 'ReferenceID'),
    '81': ('3', 'ReferenceID'),
    '83': ('3', 'ReferenceID'),
    '115': ('3', 'ReferenceID'),
    '126': ('3', 'ReferenceID'),
    '135': ('3', 'ReferenceID'),
    '151': ('3','ReferenceID'),
}

# 某些類別查詢500筆會只返回50筆, 單獨設定這些類別查詢筆數
BETSLIP_LIMIT = {
    '3': 200,  # BB视讯
    '99': 200, # BB小費
    '55': 100, # 波音体育
    '65': 100, # 皇冠体育
    '102': 100, # MG虚拟
    '106': 100, # 沙巴体育
    '108': 100, # IM电竞
    '113': 100, # Asia365
    '73': 100, # XBB彩票
}