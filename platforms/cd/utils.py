from inspect import signature
from functools import wraps
import werdsazxc
from platforms.config import CODE_DICT as config
import json
import threading
import inspect
import pickle
import time
import requests
import logging
import re
logger = logging.getLogger('robot')
requests.packages.urllib3.disable_warnings()

alert_pattern = re.compile('(?<=alert\([\'\"]).*?(?=[\'\"]\))')

default_headers = {'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/86.0.4240.183 Safari/537.36'}

spin_betslip_gamedict = {
                        '3034':'[PG 电子]麻将胡了', '3046':'[PG 电子]麻将胡了2','3029':'[PG 电子]爱尔兰精灵', '3043':'[PG 电子]赢财神', '3048':'[PG 电子]福运象财神', 
                        '3057':'[PG 电子]寻宝黄金城', '26527':'[PG 电子]赏金女王',

                        '3023':'[PG 电子]双囍临门', '3027':'[PG 电子]赏金船长', '27929':'[PG 电子]麒麟送宝' , '3030':'[PG 电子]唐伯虎点秋香' , '27924':'[PG 电子]宝石传奇', 
                        '27927':'[PG 电子]亡灵大盗' , '26530':'[PG 电子]招财喵' , '3051':'[PG 电子]澳门壕梦' , '3026':'[PG 电子]嘻游记' , '3050':'[PG 电子]凤凰传奇',
                        
                        '2084':'[CQ9 电子]跳高高', '2085':'[CQ9 电子]跳高高2', '2087':'[CQ9 电子]跳起来', '2088':'[CQ9 电子]五福临门', '2090':'[CQ9 电子]鸿福齐天', 
                        '2091':'[CQ9 电子]武圣', '2092':'[CQ9 电子]宙斯', '2095':'[CQ9 电子]直式蹦迪', '2096':'[CQ9 电子]单手跳高高', '2102':'[CQ9 电子]六颗糖', 
                        '2103':'[CQ9 电子]直式洪福齐天', '2105':'[CQ9 电子]发财神2', '2106':'[CQ9 电子]金鸡报喜',

                        '2093': '[CQ9 电子]蹦迪', '2100': '[CQ9 电子]野狼Disco', '2094': '[CQ9 电子]跳过来', '2149': '[CQ9 电子]血之吻', '2099': '[CQ9 电子]跳起来2', 
                        '2108': '[CQ9 电子]火烧连环船2', '2107': '[CQ9 电子]东方神起',

                        '2505':'[JDB 电子]变脸', '1950':'[JDB 电子]飞鸟派对', '2508':'[JDB 电子]台湾黑熊', '2511':'[JDB 电子]月光秘宝', '2516':'[JDB 电子]芝麻开门', 
                        '2533':'[JDB 电子]江山美人', '2545':'[JDB 电子]亿万富翁', '2553':'[JDB 电子]王牌特工', '4079':'[JDB 电子]富豪哥',

                        '27931': '[JDB 电子]玛雅金疯狂', '2536': '[JDB 电子]芝麻开门2', '27922': '[JDB 电子]金刚', 
                        '2577': '[JDB 电子]聚宝盆', '2572': '[JDB 电子]龙舞', '2535': '[JDB 电子]变脸2', '2576': '[JDB 电子]雷神之锤'
                        }

spin_betslip_gametype = {'25':'PG 电子','60':'CQ9 电子','61':'JDB 电子'}


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


class NotSignError(Exception):
    '''自定義當出現帳號被登出時, 自動登入GPK平台'''
    pass

class NullError(Exception):
    '''自定義當出現帳號被登出時, 自動登入GPK平台'''
    pass

def log_info(func):
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
        logger.info(f'{funcname} 網址: {arguments.get("url")}{arguments.get("endpoints")}')
        # logger.info(f'{funcname} 參數: {dict((k, v) for k, v in arguments.items() if k not in exclud_args)}')

        # 執行函數
        result = func(*args, **kwargs)

        # 有錯誤則打印整串返回的內容, 打印完後將原始資料刪除
        if result["IsSuccess"] is False and result['ErrorCode'] != config.SUCCESS_CODE.code:
            if result.get('RawStatusCode'):
                logger.warning(f"網頁原始狀態碼為: {result.get('RawStatusCode')}")
            if result.get('RawContent'):
                logger.warning(f"網頁原始內容為: {result.get('RawContent')}")

        if result.get('RawStatusCode'):
            del result["RawStatusCode"]
        if result.get('RawContent'):
            del result["RawContent"]

        # 紀錄結果
        logger.info(f'{funcname} 返回: {werdsazxc.Dict(result)}')

        return result
    return wrapper

#檢查系統傳過來的參數型別是否正常
def check_type(cls):
    @wraps(cls)
    def wrapper(*args, **kwargs):
        from .mission import BaseFunc
        # 對應傳入參數, 產生參數字典
        sig = signature(cls)
        bound = sig.bind_partial(*args, **kwargs)
        bound.apply_defaults()
        arguments = bound.arguments.get('kwargs')
        system_dict = {**BaseFunc.Meta.system_dict, **cls.Meta.return_value['include']}
        [system_dict.pop(i,None) for i in cls.Meta.return_value['exclude']]
        rep = [k for k, v in arguments.items() if k in system_dict and type(v) != system_dict[k]]
        if rep:
            logger.warning(f"型別異常參數: {rep}")
            return_content = {
                            'IsSuccess': False, 
                            'ErrorCode': config.PARAMETER_ERROR.code,
                            'ErrorMessage': config.PARAMETER_ERROR.msg
                            }
            for i in cls.Meta.return_value['data']:
                if i in arguments:
                    return_content[i] = arguments[i]
                elif i in ['BetAmount','AllCategoryCommissionable','GameCommissionable','SingleCategoryCommissionable']:
                    return_content[i] = '0.00'
                else:
                    return_content[i] = ''
            return return_content
        # 執行函數
        result = cls(*args, **kwargs)
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
                    'ErrorMessage': config.SIGN_OUT_CODE.msg.format(platform=cf.platform),
                }
            
            # 
            except NullError as e:
                if count < cf['retry_times']:
                    time.sleep(1)
                    continue
                return {
                    'IsSuccess': False,
                    'ErrorCode': config.SIGN_OUT_CODE.code,
                    'ErrorMessage': config.SIGN_OUT_CODE.msg.format(platform=cf.platform),
                }
            
            # key error
            except KeyError as e:
                logger.debug(f'{e.__class__.__name__} {e}')
                return {
                    'IsSuccess': False,
                    'ErrorCode': config.EXCEPTION_CODE.code,
                    'ErrorMessage': config.EXCEPTION_CODE.msg,
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

            # 欄位異常、無權限等
            except IndexError as e:
                logger.debug(f'{e.__class__.__name__} {e}')
                local_envs = inspect.getinnerframes(e.__traceback__)[-1].frame.f_locals
                resp = local_envs.get('resp')
                if resp:
                    status_code = resp.status_code
                    content = resp.content
                else:
                    status_code = '函數未設定resp變數, 請修改變數命名規則'
                    content = '函數未設定resp變數, 請修改變數命名規則'
                return {
                    'IsSuccess': False,
                    'ErrorCode': config.HTML_CONTENT_CODE.code,
                    'ErrorMessage': config.HTML_CONTENT_CODE.msg,
                    'RawStatusCode': resp.status_code,
                    'RawContent': resp.content
                }
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


