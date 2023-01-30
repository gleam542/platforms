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

default_headers = {'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/103.0.0.0 Safari/537.36'}

BETSLIP_ALLOW_LIST = {'1_13': '棋牌_銀河', '1_2': '棋牌_开元', '1_5': '棋牌_JDB', '1_7': '棋牌_MG', '1_9': '棋牌_LEG', '1_15': '棋牌_SGWIN', '1_18': '棋牌_新世界', '1_20': '棋牌_美天', '1_21': '棋牌_百胜', '1_22': '棋牌_FG', '1_34': '棋牌_PS', '2_13': '捕鱼_銀河', '2_2': '捕鱼_开元', '2_3': '捕鱼_CQ9', '2_5': '捕鱼_JDB', '2_7': '捕鱼_MG', '2_8': '捕鱼_BBIN', '2_9': '捕鱼_LEG', '2_10': '捕鱼_AG', '2_16': '捕鱼_BG', '2_18': '捕鱼_新世界', '2_20': '捕鱼_美天', '2_21': '捕鱼_百胜', '2_22': '捕鱼_FG', '2_24': '捕鱼_FC', '2_27': '捕鱼_KA', '2_34': '捕鱼_PS', '3_13': '电子_銀河', '3_3': '电子_CQ9', '3_5': '电子_JDB', '3_7': '电子_MG', '3_8': '电子_BBIN(旧)', '3_9': '电子_LEG', '3_10': '电子_AG', '3_14': '电子_PG', '3_20': '电子_美天', '3_21': '电子_百胜', '3_22': '电子_FG', '3_24': '电子_FC', '3_27': '电子_KA', '3_33': '电子_BNG', '3_34': '电子_PS', '3_37': '电子_PP','3_75': '电子_BBIN', '4_8': '真人_BBIN', '4_10': '真人_AG', '4_16': '真人_BG', '4_17': '真人_eBET', '5_6': '体育_利记', '5_8': '体育_BBIN', '5_19': '体育_沙巴', '5_36': '体育_三昇', '8_8': '彩票_BBIN', '8_11': '彩票_双赢', '8_35': '彩票_云博'}

spin_betslip_gamedict = {
    '140048': '[PG电子]双囍临门', '140050': '[PG电子]嘻游记', '140054': '[PG电子]赏金船长', '140060': '[PG电子]爱尔兰精灵', '140061': '[PG电子]唐伯虎点秋香', '140065': '[PG电子]麻将胡了', '140071': '[PG电子]赢财神', '140074': '[PG电子]麻将胡了2', '140075': '[PG电子]福运象财神', '140079': '[PG电子]澳门壕梦', '140082': '[PG电子]凤凰传奇', '140083': '[PG电子]火树赢花', '140084': '[PG电子]赏金女王', '140087': '[PG电子]寻宝黄金城', '140089': '[PG电子]招财喵', '140091': '[PG电子]冰火双娇', '140095': '[PG电子]宝石传奇', '140100': '[PG电子]糖心风暴', '140104': '[PG电子]亡灵大盗', '140105': '[PG电子]霹雳神偷', '140106': '[PG电子]麒麟送宝', '140119': '[PG电子]百鬼夜行', '140121': '[PG电子]日月星辰', '140122': '[PG电子]神鹰宝石',

    '30016': '[CQ9电子]六颗扭蛋', '30022': '[CQ9电子]跳高高', '30023': '[CQ9电子]跳起来', '30025': '[CQ9电子]跳高高2', '30026': '[CQ9电子]五福临门', '30028': '[CQ9电子]鸿福齐天', '30029': '[CQ9电子]武圣', '30030': '[CQ9电子]宙斯', '30031': '[CQ9电子]蹦迪', '30032': '[CQ9电子]跳过来', '30033': '[CQ9电子]直式蹦迪', '30034': '[CQ9电子]单手跳高高', '30037': '[CQ9电子]跳起来2', '30038': '[CQ9电子]野狼Disco', '30040': '[CQ9电子]六颗糖', '30041': '[CQ9电子]直式洪福齐天', '30043': '[CQ9电子]发财神2', '30044': '[CQ9电子]金鸡报喜', '30045': '[CQ9电子]东方神起', '30046': '[CQ9电子]火烧连环船2', '30086': '[CQ9电子]血之吻',

    '8003': '[JDB电子]变脸', '8006': '[JDB电子]台湾黑熊', '8015': '[JDB电子]月光秘宝', '8020': '[JDB电子]芝麻开门', '8044': '[JDB电子]江山美人', '8047': '[JDB电子]变脸2', '8048': '[JDB电子]芝麻开门2', '14006': '[JDB电子]亿万富翁', '14016': '[JDB电子]王牌特工', '14030': '[JDB电子]三倍金刚', '14033': '[JDB电子]飞鸟派对', '14035': '[JDB电子]龙舞', '14041': '[JDB电子]雷神之锤', '14061': '[JDB电子]玛雅金疯狂', '14042': '[JDB电子]聚宝盆', '514079': '[JDB电子]富豪哥2',
}

spin_betslip_gametype = {'3_14':'PG 电子','3_3':'CQ9 电子','3_5':'JDB 电子'}


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


