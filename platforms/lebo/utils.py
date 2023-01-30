from inspect import signature
from functools import wraps
import werdsazxc
from . import CODE_DICT as config
import threading
import requests
import logging
import inspect
import pickle
import time
import json
import re


logger = logging.getLogger('robot')
requests.packages.urllib3.disable_warnings()

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
    'Content-Type': 'application/x-www-form-urlencoded',
    'X-Requested-With': 'XMLHttpRequest'
}
support = {'BBIN电子':{
                    'GameCategory':'bbdz',
                    'api_name':'bbin',
                    'gtype':'5902',
                    'gamename':{
                                '0': '[BBIN电子]夹猪珠',
                                '1': '[BBIN电子]Staronic',
                                '2': '[BBIN电子]空战英豪',
                                '3': '[BBIN电子]鱼虾蟹',
                                '4': '[BBIN电子]钻石水果盘',
                                '5': '[BBIN电子]步步高升',
                                '6': '[BBIN电子]发大财',
                                '7': '[BBIN电子]魔法元素',
                                '8': '[BBIN电子]秦皇秘宝',
                                '9': '[BBIN电子]海底世界',
                                '10': '[BBIN电子]秘境冒险',
                                '11': '[BBIN电子]阿兹特克宝藏',
                                '12': '[BBIN电子]金狗旺岁',
                                '13': '[BBIN电子]外星争霸',
                                '14': '[BBIN电子]中秋月光派对',
                                '15': '[BBIN电子]情人夜',
                                '16': '[BBIN电子]乐透转轮',
                                '17': '[BBIN电子]金莲',
                                '18': '[BBIN电子]连环夺宝',
                                '19': '[BBIN电子]航海时代',
                                '20': '[BBIN电子]Jenga',
                                '21': '[BBIN电子]美式轮盘',
                                '22': '[BBIN电子]浓情巧克力',
                                '23': '[BBIN电子]外星战记',
                                '24': '[BBIN电子]激爆水果盘',
                                '25': '[BBIN电子]三元四喜', 
                                '26': '[BBIN电子]雷神索尔',
                                '27': '[BBIN电子]大话西游',
                                '28': '[BBIN电子]开心消消乐',
                                '29': '[BBIN电子]恶龙传说',
                                '30': '[BBIN电子]五行',
                                '31': '[BBIN电子]疯狂水果盘',
                                '32': '[BBIN电子]史前丛林冒险',
                                '33': '[BBIN电子]凯萨帝国',
                                '34': '[BBIN电子]斗魂',
                                '35': '[BBIN电子]九尾狐',
                                '36': '[BBIN电子]欧式轮盘',
                                '37': '[BBIN电子]圣诞派对',
                                '38': '[BBIN电子]祖玛帝国',
                                '39': '[BBIN电子]FIFA2010',
                                '40': '[BBIN电子]神舟27',
                                '41': '[BBIN电子]浪人武士',
                                '42': '[BBIN电子]功夫龙',
                                '43': '[BBIN电子]经典高球',
                                '44': '[BBIN电子]喜福猴年',
                                '45': '[BBIN电子]女娲补天',
                                '46': '[BBIN电子]聚宝盆',
                                '47': '[BBIN电子]龙卷风',
                                '48': '[BBIN电子]糖果派对2',
                                '49': '[BBIN电子]金钱豹',
                                '50': '[BBIN电子]球球大作战',
                                '51': '[BBIN电子]老船长',
                                '52': '[BBIN电子]封神榜',
                                '53': '[BBIN电子]彩金轮盘',
                                '54': '[BBIN电子]7PK',
                                '55': '[BBIN电子]斗牛',
                                '56': '[BBIN电子]夜上海',
                                '57': '[BBIN电子]2012 伦敦奥运',
                                '58': '[BBIN电子]圣兽传说',
                                '59': '[BBIN电子]金瓶梅',
                                '60': '[BBIN电子]爆骰',
                                '61': '[BBIN电子]月光宝盒',
                                '62': '[BBIN电子]大红帽与小野狼',
                                '63': '[BBIN电子]爱你一万年',
                                '64': '[BBIN电子]大明星',
                                '65': '[BBIN电子]初音大进击',
                                '66': '[BBIN电子]夜市人生',
                                '67': '[BBIN电子]连环夺宝2',
                                '68': '[BBIN电子]东海龙宫',
                                '69': '[BBIN电子]奥林帕斯',
                                '70': '[BBIN电子]斗牛赢家',
                                '71': '[BBIN电子]金矿工',
                                '72': '[BBIN电子]神秘岛',
                                '73': '[BBIN电子]绝地求生',
                                '74': '[BBIN电子]埃及传奇',
                                '75': '[BBIN电子]钻石列车',
                                '76': '[BBIN电子]阿基里斯',
                                '77': '[BBIN电子]奇幻花园',
                                '78': '[BBIN电子]斗大',
                                '79': '[BBIN电子]水果大转轮',
                                '80': '[BBIN电子]七夕',
                                '81': '[BBIN电子]霸王龙',
                                '82': '[BBIN电子]火焰山',
                                '83': '[BBIN电子]魔光幻音',
                                '84': '[BBIN电子]红狗', 
                                '85': '[BBIN电子]金鸡报喜', 
                                '86': '[BBIN电子]啤酒嘉年华', 
                                '87': '[BBIN电子]糖果派', 
                                '88': '[BBIN电子]丛林', 
                                '89': '[BBIN电子]沙滩排球', 
                                '90': '[BBIN电子]糖果派对3', 
                                '91': '[BBIN电子]猴子爬树', 
                                '92': '[BBIN电子]宝石传奇', 
                                '93': '[BBIN电子]连连看', 
                                '94': '[BBIN电子]星际大战', 
                                '95': '[BBIN电子]百搭二王', 
                                '96': '[BBIN电子]糖果派对', 
                                '97': '[BBIN电子]加勒比扑克', 
                                '98': '[BBIN电子]金刚爬楼', 
                                '99': '[BBIN电子]水果擂台', 
                                '100': '[BBIN电子]趣味台球', 
                                '101': '[BBIN电子]喜福牛年', 
                                '102': '[BBIN电子]发达啰', 
                                '103': '[BBIN电子]动物奇观五', 
                                '104': '[BBIN电子]金瓶梅2', 
                                '105': '[BBIN电子]齐天大圣', 
                                '106': '[BBIN电子]酷搜马戏团', 
                                '107': '[BBIN电子]多福多财', 
                                '108': '[BBIN电子]惑星战记', 
                                '109': '[BBIN电子]熊猫乐园', 
                                '110': '[BBIN电子]糖果派对-极速版', 
                                '111': '[BBIN电子]开心蛋', 
                                '112': '[BBIN电子]幸运财神', 
                                '113': '[BBIN电子]葫芦娃', 
                                '114': '[BBIN电子]海底传奇', 
                                '115': '[BBIN电子]电音之王', 
                                '116': '[BBIN电子]月狼', 
                                '117': '[BBIN电子]野蛮战国', 
                                '118': '[BBIN电子]马到成功', 
                                '119': '[BBIN电子]蒸汽王国', 
                                '120': '[BBIN电子]招财进宝', 
                                '121': '[BBIN电子]莲花', 
                                '122': '[BBIN电子]金蟾吐珠', 
                                '123': '[BBIN电子]满天星', 
                                '124': '[BBIN电子]偏财神', 
                                '125': '[BBIN电子]黎利', 
                                '126': '[BBIN电子]好事成双', 
                                '127': '[BBIN电子]抖音DJ', 
                                '128': '[BBIN电子]扭转钱坤', 
                                '129': '[BBIN电子]鱼跃龙门', 
                                '130': '[BBIN电子]大丰收', 
                                '131': '[BBIN电子]忍者大师', 
                                '132': '[BBIN电子]新年快乐', 
                                '133': '[BBIN电子]黄金财神', 
                                '134': '[BBIN电子]猪宝满满', 
                                '135': '[BBIN电子]牛运当头', 
                                '136': '[BBIN电子]下龙湾神话', 
                                '137': '[BBIN电子]蜂赢百搭', 
                                '138': '[BBIN电子]街头美食', 
                                '139': '[BBIN电子]鱼虾蟹开了', 
                                '140': '[BBIN电子]七福报喜', 
                                '141': '[BBIN电子]金龟神弩', 
                                '142': '[BBIN电子]怪物派对', 
                                '143': '[BBIN电子]武松打虎',
                                '144': '[BBIN电子]哪吒',
                                '145': '[BBIN电子]水果忍者',
                                '146': '[BBIN电子]今晚有戏',
                                '147': '[BBIN电子]多福财神',
                                '148': '[BBIN电子]人鱼秘宝',
                                '149': '[BBIN电子]水果幸运星',
                                '150': '[BBIN电子]筒子拉霸',
                                '151': '[BBIN电子]足球拉霸',
                                '152': '[BBIN电子]任你钻',
                                '153': '[BBIN电子]富贵金蟾',
                                '154': '[BBIN电子]疯狂果酱罐',
                                '155': '[BBIN电子]五福临门',
                                '155': '[BBIN电子]舞动巴厘岛',
                                '156': '[BBIN电子]连消1024',
                                }
                    }
            }
freespin_support = {'52': '[CQ9 电子]跳高高',
                    '179': '[CQ9 电子]跳高高2',
                    '7': '[CQ9 电子]跳起来',
                    '64': '[CQ9 电子]宙斯',
                    '31': '[CQ9 电子]武圣',
                    '153': '[CQ9 电子]六颗糖',
                    '160': '[CQ9 电子]发财神2',
                    '15': '[CQ9 电子]金鸡报喜',
                    '50': '[CQ9 电子]鸿福齐天',
                    '137': '[CQ9 电子]直式蹦迪',
                    '10': '[CQ9 电子]五福临门',
                    '105': '[CQ9 电子]单手跳高高',

                    '205': '[CQ9 电子]蹦迪',
                    '117': '[CQ9 电子]东方神起',
                    '140': '[CQ9 电子]火烧连环船2',
                    '24': '[CQ9 电子]跳起来2',
                    '138': '[CQ9 电子]跳过来',
                    '3': '[CQ9 电子]血之吻',
                    '183': '[CQ9 电子]野狼Disco',

                    '8006': '[JDB 电子]台湾黑熊',
                    '8015': '[JDB 电子]月光秘宝',
                    '8020': '[JDB 电子]芝麻开门',
                    '8044': '[JDB 电子]江山美人',
                    '14006': '[JDB 电子]亿万富翁',
                    '14016': '[JDB 电子]王牌特工',
                    '14047': '[JDB 电子]富豪哥',
                    '14055': '[JDB 电子]金刚',
                    '8003': '[JDB 电子]变脸',
                    '14033': '[JDB 电子]飞鸟派对',
                    
                    '8047': '[JDB 电子]变脸2',
                    '8048': '[JDB 电子]芝麻开门2',
                    '14016': '[JDB 电子]王牌特工',
                    '14035': '[JDB 电子]龙舞',
                    '14041': '[JDB 电子]雷神之锤',
                    '14042': '[JDB 电子]聚宝盆',
                    '14061': '[JDB 电子]MayaGoldCrazy'}

bettingbonus_support = {
                        'agdz': 'AG電子',
                        'bbdz': 'BB電子',
                        'gns': 'GNS電子'
                        }


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


class NotSignError(Exception):
    '''自定義當出現帳號被登出時, 自動登入GPK平台'''
    pass
class NullError(Exception):
    '''自定義當出現status_code非200時, 自動重試'''
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
        if result.get("IsSuccess") is False and result['ErrorCode'] != config['SUCCESS_CODE']['code']:
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
        # 檢查傳入型別
        for key, value in arguments.items():
            if key == 'timeout':
                if type(value) not in (int, tuple):
                    raise RuntimeError(f"{key} 型別應為: int 或 tuple")
            elif func.__annotations__.get(key) and type(value) != func.__annotations__.get(key):
                raise RuntimeError(f"{key} 型別應為: {func.__annotations__[key]}")

        # 計算錯誤次數
        count = 1
        cf = arguments['cf']
        while count <= cf['retry_times']:
            try:
                result = func(*args, **kwargs)
                break

            # 檢查schema是否輸入
            except requests.exceptions.MissingSchema as e:
                return {
                    'IsSuccess': False,
                    'ErrorCode': config['EXCEPTION_CODE']['code'],
                    'ErrorMessage': f'平台设定错误, 通讯协定(http或https)未输入',
                }
            # 檢查schema是否合法
            except requests.exceptions.InvalidSchema as e:
                return {
                    'IsSuccess': False,
                    'ErrorCode': config['EXCEPTION_CODE']['code'],
                    'ErrorMessage': f'平台设定错误, 通讯协定(http或https)无法解析',
                }
            # 檢查網址是否合法
            except requests.exceptions.InvalidURL as e:
                return {
                    'IsSuccess': False,
                    'ErrorCode': config['EXCEPTION_CODE']['code'],
                    'ErrorMessage': f'平台设定错误, 无法解析',
                }
            # 發生重導向異常
            except requests.exceptions.TooManyRedirects as e:
                return {
                    'IsSuccess': False,
                    'ErrorCode': config['EXCEPTION_CODE']['code'],
                    'ErrorMessage': f'平台设定错误, 发生重导向异常',
                }
            # 捕捉被登出
            except NotSignError as e:
                from .module import session
                from .module import login
                if (cf['need_backend_otp'] is False and
                    hasattr(session, 'url') and
                    hasattr(session, 'acc') and
                    hasattr(session, 'pw')):
                    login(cf=cf, url=session.url, acc=session.acc, pw=session.pw, otp='')
                    continue
                return {
                    'IsSuccess': False,
                    'ErrorCode': config['SIGN_OUT_CODE']['code'],
                    'ErrorMessage': config['SIGN_OUT_CODE']['msg'].format(platform=cf.platform),
                }

            # 發生錯誤重試
            except NullError as e:
                if count < cf['retry_times']:
                    time.sleep(1)
                    continue
                return {
                    'IsSuccess': False,
                    'ErrorCode': config.SIGN_OUT_CODE.code,
                    'ErrorMessage': config.SIGN_OUT_CODE.msg.format(platform=cf.platform),
                }
                
            # 捕捉連線異常
            except (requests.exceptions.ConnectionError,
                    requests.exceptions.Timeout,
                    requests.exceptions.ContentDecodingError) as e:
                if count == cf['retry_times']:
                    rtn = {'IsSuccess': False, 'ErrorCode': config['CONNECTION_CODE']['code'], 'ErrorMessage': config['CONNECTION_CODE']['msg']}
                    return rtn
                
                logger.debug(f'{e.__class__.__name__} ({count}/{cf["retry_times"]})...')

                if f'与{cf.get("platform")}连线异常...' not in cf['error_msg']:
                    cf['error_msg'].append(f'与{cf.get("platform")}连线异常...')   #●新增

                time.sleep(1)
                count += 1
            # 欄位異常、無權限等
            except IndexError as e:
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
                    'ErrorCode': config['HTML_CONTENT_CODE']['code'],
                    'ErrorMessage': config['HTML_CONTENT_CODE']['msg'].format(platform=cf.platform),
                    'RawStatusCode': resp.status_code,
                    'RawContent': resp.content
                }
            # 捕捉json解析錯誤
            except json.JSONDecodeError as e:
                logger.info(f'json.JSONDecodeError: {e.doc}')
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
                    'ErrorCode': config['JSON_ERROR_CODE']['code'],
                    'ErrorMessage': config['JSON_ERROR_CODE']['msg'].format(platform=cf.platform),
                    'RawStatusCode':status_code,
                    'RawContent': content
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

        # 檢查傳出型別
        if type(result) != func.__annotations__['return']:
            raise RuntimeError(f"回傳型別應為: {func.__annotations__['return']}")
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

# 檢查會員字串長度
def split_by_len(users, max_length=20000):
    lst = []
    for i, user in enumerate(users):
        if len(','.join(lst)) + len(user) + 1 > max_length:
            yield lst
            lst = [user]
        lst.append(user)
    yield lst

