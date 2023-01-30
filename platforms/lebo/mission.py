import logging, datetime, requests, requests_html, json, re, time

from platforms.bbin.utils import keep_connect
from . import CODE_DICT as config
from . import module as lebo
from .utils import ThreadProgress,freespin_support, bettingbonus_support ,split_by_len, support
from itertools import groupby
from collections import Counter
import pytz
from pathlib import Path
from itertools import groupby

logger = logging.getLogger('robot')
gameList = []



def getGameList():
    try:
        url = 'http://118.163.18.126:62341/public/API/lebo_pull.php'
        r = requests.get(url)
        fbk = json.loads(r.text)
        for i in fbk['Data']:
            for j in fbk['Data'][i]:
                gameList.append(j[1])
        print(f'★ gameList：{gameList}')
        return gameList
    except Exception as e:
        print(f'● getGameList 錯誤：{e}')
        try:
            print(f'● getGameList 原文：{r.text}')
        except:
            pass
        return False


class BaseFunc:
    class Meta:
        extra = {}

    @classmethod
    def return_schedule(cls, **kwargs):
        if getattr(cls, 'th', None):
            if cls.th.detail:
                cls.th.lst.append({k: v for k, v in kwargs.items() if k in ['Action', 'DBId', 'Status', 'Progress', 'Detail']})
            else:
                cls.th.lst.append({
                    'feedback': '1',
                    'Action': kwargs['Action'],
                    'DBId': kwargs['DBId'],
                    'Member': kwargs['Member'],
                    'current_status': kwargs.get('current_status', kwargs.get('Status')),
                })


    @classmethod
    def deposit(cls, url, DBId, Member, DepositAmount, mod_name, mod_key,
        timeout, backend_remarks, multiple, cf, amount_memo='', Note='', increasethebet_switch=0, increasethebet='0', **kwargs):
        '''
            LEBO 充值
            需要有【現金系統 >> 存款與取款】权限
        '''
        member = Member.lower()
        if int(increasethebet_switch):
            logger.info(f'使用PR6流水倍數：{int(increasethebet)}')
            multiple = int(increasethebet)
        else:
            logger.info(f'使用機器人打碼量：{int(multiple)}')
            multiple = int(multiple)

        # 判斷充值金額-------------------------------------------------
        if not float(DepositAmount) <= float(cf['amount_below']):
            # 查詢失敗直接回傳
            return {
                'IsSuccess': 0,
                'ErrorCode': config['AMOUNT_CODE']['code'],
                'ErrorMessage': config['AMOUNT_CODE']['msg'],
                'DBId': DBId,
                'Member': Member,
            }

        #★查詢【帳號ID】
        user_result = lebo.cash_cash_operation(cf, url, source={
            "username": Member,
            "search": "search",
        })
        userid = user_result.get('Data', {}).get('hidden_input', {}).get('userid', '')
        if not user_result["IsSuccess"]:
            Detail = f'查询失败\n搜寻帐号：{Member}\n{user_result["ErrorMessage"]}'
        else:
            Detail = f'查询成功\n搜寻帐号：{Member}\n会员是否存在：{"是" if userid else "否"}'
        cls.return_schedule(Action='chkpoint', DBId=DBId, Member=Member, Status='【現金系統】 - 【存款與取款】 - 【查询】', Progress=f'1/1', Detail=Detail)

        # 查詢不到會員直接回傳
        if user_result['ErrorCode'] == config['NO_USER']['code']:
            return {
                'IsSuccess': int(user_result['IsSuccess']),
                'ErrorCode': user_result['ErrorCode'],
                'ErrorMessage': user_result['ErrorMessage'],
                'DBId': DBId,
                'Member': Member,
            }
        # 連線異常、被登出，重試
        if user_result["ErrorCode"] == config['CONNECTION_CODE']['code']:
            return user_result
        if user_result["ErrorCode"] == config['SIGN_OUT_CODE']['code']:
            return user_result
        # 其餘異常彈出視窗
        if user_result["ErrorCode"] != config['SUCCESS_CODE']['code']:
            return user_result["ErrorMessage"]

        #★進行【充值】
        amount_memo = amount_memo or f'{mod_name}({mod_key}-{DBId}){Note}'
        if backend_remarks:
            amount_memo += f'：{backend_remarks}'

        count = 1
        while True:
            result_deposit = lebo.cash_cash_operation(cf, url, source={
                'userid': userid,
                'username': Member,
                'op_type': 1,
                'amount': 0,
                'ifSp': 1,
                'spAmount': float(DepositAmount),
                'sp_other': 0,
                'isComplex': 1,
                #'ComplexValue': float(DepositAmount) * cf['multiple'],
                # 'ComplexValue': float(DepositAmount) * int(cf['multiple']),  #●修改
                'ComplexValue': float(DepositAmount) * multiple,
                'type_memo': 8,
                'isty': 1,
                #'amount_memo': f'{mod_name}系统({mod_key})，编号{DBId}，{cf["backend_remarks"]}',
                'amount_memo': amount_memo,
                'savebtn': '正在提交，請勿關閉或離開...'
            })
            if result_deposit['IsSuccess'] is False:
                Detail = f'充值失败\n{result_deposit["ErrorMessage"]}'
            else:
                Detail = f'充值成功'
            cls.return_schedule(Action='chkpoint', DBId=DBId, Member=Member, Status='【現金系統】 - 【存款與取款】 - 【人工存款】', Progress=f'{count}/{count}', Detail=Detail)

            # 自動重新充值開啟, 進行重試
            if cf['recharg'] and result_deposit['ErrorCode'] == config.REPEAT_DEPOSIT.code:
                time.sleep(10)
                count += 1
                continue
            # 充值結果未知，跳過回傳
            if result_deposit['ErrorCode'] == config.CONNECTION_CODE.code:
                return result_deposit
            if result_deposit['ErrorCode'] == config.IGNORE_CODE.code:
                return result_deposit
            # 內容錯誤，彈出視窗
            if result_deposit['ErrorCode'] == config.PERMISSION_CODE.code:
                return result_deposit['ErrorMessage']
            if result_deposit['ErrorCode'] == config.HTML_STATUS_CODE.code:
                return result_deposit['ErrorMessage']
            if result_deposit['ErrorCode'] == config.HTML_CONTENT_CODE.code:
                return result_deposit['ErrorMessage']
            # 被登出，回主程式重試
            if result_deposit['ErrorCode'] == config.SIGN_OUT_CODE.code:
                return result_deposit

            #★【充值】失敗複查
            if result_deposit["IsSuccess"] is False:
                total_page = 1
                page = 1
                while page<=total_page:
                    result_check = lebo.cash_cash_record(url, params={
                        'date_start': datetime.datetime.now(tz=datetime.timezone(datetime.timedelta(hours=-4))).strftime('%Y-%m-%d 00:00:00'),
                        'date_end': datetime.datetime.now(tz=datetime.timezone(datetime.timedelta(hours=-4))).strftime('%Y-%m-%d 23:59:59'),
                        'account': member,
                        'page_num': '500',
                        'page': page,
                    })
                    cls.return_schedule(Action='chkpoint', DBId=DBId, Member=Member, Status='【現金系統】 - 【存款與取款】 - 【历史查詢】', Progress=f'{page}/{total_page}', Detail='复查充值结果')
                    # 內容錯誤，彈出視窗
                    if result_check['ErrorCode'] == config.PERMISSION_CODE.code:
                        return result_check["ErrorMessage"]
                    if result_check['ErrorCode'] == config.HTML_STATUS_CODE.code:
                        return result_check['ErrorMessage']
                    if result_check['ErrorCode'] == config.HTML_CONTENT_CODE.code:
                        return result_check['ErrorMessage']
                    # 連線異常、被登出，略過回傳
                    if result_check['ErrorCode'] == config.CONNECTION_CODE.code:
                        return result_check
                    if result_check['ErrorCode'] == config.SIGN_OUT_CODE.code:
                        return result_check
                    # 檢查充值是否有成功
                    if amount_memo in [r['備注'] for r in result_check['Data']['record']]:
                        # 找到充值成功回傳成功
                        return {
                            'IsSuccess': 1,
                            'ErrorCode': config.SUCCESS_CODE.code,
                            'ErrorMessage': config.SUCCESS_CODE.msg,
                            'DBId': DBId,
                            'Member': Member,
                        }
                    page += 1
                # 找不到充值成功回傳失敗
                return {
                    'IsSuccess': 0,
                    'ErrorCode': result_deposit['ErrorCode'],
                    'ErrorMessage': result_deposit['ErrorMessage'],
                    'DBId': DBId,
                    'Member': Member,
                }

            # 【充值成功回傳】
            return {
                'IsSuccess': 1,
                'ErrorCode': config.SUCCESS_CODE.code,
                'ErrorMessage': config.SUCCESS_CODE.msg,
                'DBId': DBId,
                'Member': Member,
            }


class hongbao(BaseFunc):
    '''LEBO 紅包'''


class passthrough(BaseFunc):
    '''LEBO 闖關'''


class happy7days(BaseFunc):
    '''LEBO 七天樂'''


class pointsbonus(BaseFunc):
    '''LEBO 积分红包'''
    @classmethod
    def deposit(cls, *args, **kwargs):
        '''
            LEBO 充值
            需要有【現金系統 >> 存款與取款】权限
        '''
        kwargs['SupportStatus'] = 1
        cls.th = ThreadProgress(kwargs['cf'], kwargs['mod_key'])
        cls.th.start()

        result = cls._deposit(*args, **kwargs)

        cls.return_schedule(Action='chkpoint', DBId=kwargs['DBId'], Status='【充值机器人处理完毕】', Progress='-', Detail='-')
        cls.th.stop()
        return result

    @classmethod
    def _deposit(cls, url, DBId, Member, DepositAmount, mod_name, mod_key,
        timeout, backend_remarks, multiple, cf, amount_memo='', Note='', increasethebet_switch=0, increasethebet='0', **kwargs):
        '''LEBO 积分红包 充值'''

        return super().deposit(url, DBId, Member, DepositAmount, mod_name, mod_key,
        timeout, backend_remarks, multiple, cf, amount_memo, Note, increasethebet_switch, increasethebet, **kwargs)


class promo(BaseFunc):
    class Meta:
        suport = {}  #●修改：新增

        extra = {
            # # 需要登入第二個平台使用
            # 'platform': {
            #     'info': '平台',
            #     'var': list,
            #     'choise': ['GPK', 'LEBO', 'BBIN'],
            #     'default': 'GPK',
            # },
            # 'url': {
            #     'info': '網址',
            #     'var': str,
            #     'default': '',
            # },
            # 'username': {
            #     'info': '帳號',
            #     'var': str,
            #     'default': '',
            # },
            # 'password': {
            #     'info': '密碼',
            #     'var': str,
            #     'default': '',
            # },
        }

    @classmethod
    def deposit(cls, url, DBId, Member, DepositAmount, mod_name, mod_key,
        timeout, backend_remarks, multiple, cf, Note='', increasethebet_switch=0, increasethebet='0', **kwargs):
        '''
            LEBO 充值、移动层级
            需要有【現金系統 >> 存款與取款】权限。
            需要有【現金系統 >> 层级管理 >> 会员查询 >> 移动层级】权限。
        '''
        if kwargs.get('SupportStatus'):
            cls.th = ThreadProgress(cf, mod_key)
            cls.th.start()

        result = super().deposit(url, DBId, Member, DepositAmount, mod_name, mod_key,
            timeout, backend_remarks, multiple, cf, Note, increasethebet_switch, increasethebet, **kwargs)

        if kwargs.get('SupportStatus'):
            cls.return_schedule(Action='chkpoint', DBId=DBId, Status='【充值机器人处理完毕】', Progress='-', Detail='-')
            cls.th.stop()

        return result

    @classmethod
    def audit(cls, *args, **kwargs):
        '''
            LEBO 活动大厅
            需要有【帳號管理 >> 會員管理】权限。
            需要有【帳號管理 >> 會員管理 >> 資料】权限。
            需要有【其他 >> 登入日志】权限。
        '''
        if kwargs.get('SupportStatus'):
            cls.th = ThreadProgress(kwargs['cf'], kwargs['mod_key'])
            cls.th.start()

        result = cls._audit(*args, **kwargs)

        if kwargs.get('SupportStatus'):
            cls.return_schedule(Action='chkbbin', DBId=kwargs['DBId'], Status='【审核机器人处理完毕】', Progress='-', Detail='-')
            cls.th.stop()

        return result

    @classmethod
    def _audit(cls, url, DBId, mod_name, Member, cf, **kwargs):
        '''
            LEBO 活动大厅
            需要有【现金系统 >> 层级管理 >> 会员查询】权限
        '''
        # 取得會員層級
        result_level = lebo.app_cash_utotal(cf, url, params={
                                                                "username": Member,
                                                                "savebtn": "查詢"
                                                            }
                                                            )
        if result_level['IsSuccess']:
            BlockMemberLayer = result_level['Data'][Member]['所屬層級']

        if kwargs.get('SupportStatus'):
            if not result_level["IsSuccess"] and result_level['ErrorCode'] != config.NO_USER.code:
                Detail = f'查询失败\n搜寻帐号：{Member}\n{result_level["ErrorMessage"]}'
            else:
                Detail = f'查询成功\n搜寻帐号：{Member}\n会员是否存在：{"否" if result_level["ErrorCode"] == config.NO_USER.code else "是"}'
                if result_level['IsSuccess']:
                    Detail += f'\n会员层级：{BlockMemberLayer}'

            cls.return_schedule(Action='chkbbin', DBId=DBId, Status='【现金系统】 - 【层级管理】- 【会员查询】', Progress=f'1/1', Detail=Detail)

        # 連線異常, 設定connect為False
        if result_level["ErrorCode"] == config['CONNECTION_CODE']['code']:
            #return result_level["ErrorMessage"] + '，请检查网路后再次启动机器人'
            return result_level  #●新增
        if result_level["ErrorCode"] == config['SIGN_OUT_CODE']['code']:
            return result_level
        # 查詢失敗直接回傳
        if result_level['IsSuccess'] is False:
            return {
                'IsSuccess': int(result_level['IsSuccess']),
                'ErrorCode': result_level['ErrorCode'],
                'ErrorMessage': result_level['ErrorMessage'],
                'DBId': DBId,
                'Member': Member,
                'BlockMemberLayer': ''
            }

        if result_level["ErrorCode"] != config['SUCCESS_CODE']['code']:
            return result_level["ErrorMessage"]

        return {
            'IsSuccess': int(result_level['IsSuccess']),
            'ErrorCode': result_level['ErrorCode'],
            'ErrorMessage': result_level['ErrorMessage'],
            'DBId': DBId,
            'Member': Member,
            #'BlockMemberLayer': result_level['Data'][Member]['会员等级']
            'BlockMemberLayer': BlockMemberLayer  #●新增
        }


class _betslip(BaseFunc):
    class Meta:
        suport = support['BBIN电子']['gamename']
        extra = {
            # # 需要登入第二個平台使用
            # 'platform': {
            #     'info': '平台',
            #     'var': list,
            #     'choise': ['GPK', 'LEBO', 'BBIN'],
            #     'default': 'GPK',
            # },
            # 'url': {
            #     'info': '網址',
            #     'var': str,
            #     'default': '',
            # },
            # 'username': {
            #     'info': '帳號',
            #     'var': str,
            #     'default': '',
            # },
            # 'password': {
            #     'info': '密碼',
            #     'var': str,
            #     'default': '',
            # },
        }

    @classmethod
    def audit(cls, url, DBId, mod_name, Member, SearchGameCategory, SearchDate, timeout, cf, RawWagersId, ExtendLimit, GameCountType, **kwargs):
        '''
            LEBO 注单系统
            需要有【帐号管理 >> 输赢查询, 现金系统 >>第三方查询 / 层级管理 >> 会员查询】权限。支援「BBIN电子」。
        '''
        SearchDate = SearchDate.replace('/', '-')
        global gameList

        result = {
                'Action': 'chkbbin',
                'IsSuccess': 0,
                'ErrorCode': '',
                'ErrorMessage': '',
                'DBId': DBId,
                'RawWagersId': RawWagersId,
                'Member': Member,
                'BlockMemberLayer': '',
                'GameName': '',
                'WagersTimeString': '',
                'WagersTimeStamp': '',
                'BetAmount': '0.00',
                'AllCategoryCommissionable': '0.00',
                'GameCommissionable': '0.00',
                'SingleCategoryCommissionable': '0.00',
                'CategoryName': '',
                'ExtendLimit': ExtendLimit,
                'GameCountType': GameCountType
                }


        #★取得【會員層級】
        result_level = lebo.app_cash_utotal(cf, url, params={
            "username": Member,
            "savebtn": "查詢"
        })

        # 連線異常, 設定connect為False
        if result_level["ErrorCode"] == config['CONNECTION_CODE']['code']:
            #return result_level["ErrorMessage"] + '，请检查网路后再次启动机器人'
            return result_level  #●新增
        if result_level["ErrorCode"] == config['SIGN_OUT_CODE']['code']:
            return result_level

        # 查詢失敗直接回傳
        if result_level['IsSuccess'] is False:
            result.update({
                         'IsSuccess': int(result_level['IsSuccess']),
                         'ErrorCode': result_level['ErrorCode'],
                         'ErrorMessage': result_level['ErrorMessage']
                         })
            return result

        elif result_level['ErrorMessage']:
            result.update({'ErrorMessage': result_level['ErrorMessage']})
            return result

        else:
            result.update({'BlockMemberLayer': result_level['Data'][Member]['所屬層級']})

        if result_level["ErrorCode"] != config['SUCCESS_CODE']['code']:
            return result_level["ErrorMessage"]

        #★取得【注單內容】
        result_wagers = lebo.app_mgame_bbin(cf, url, mod_name, data={
            "account": Member,
            "no": RawWagersId,
            "searchbtn": "查詢",
            })

        # 連線異常, 設定connect為False
        if result_wagers["ErrorCode"] == config['CONNECTION_CODE']['code']:
            return result_wagers
        if result_wagers["ErrorCode"] == config['SIGN_OUT_CODE']['code']:
            return result_wagers

        # 查詢失敗直接回傳
        if result_wagers['IsSuccess'] is False:
            result.update({
                         'IsSuccess': int(result_wagers['IsSuccess']),
                         'ErrorCode': result_wagers['ErrorCode'],
                         'ErrorMessage': result_wagers['ErrorMessage']
                         })
            return result

        elif result_wagers['ErrorMessage']:
            result.update({'ErrorMessage': result_wagers['ErrorMessage']})
            return result

        if result_wagers["ErrorCode"] != config['SUCCESS_CODE']['code']:
            return result_wagers["ErrorMessage"]

        txt = result_wagers['Data']['注單']['下注资讯']
        #每注:5(元),共:1注\n总共:5(元)
        a = txt.rfind(':')
        b = txt.rfind('(元)')
        num = txt[(a + 1):b]

        dt = datetime.datetime.strptime(f"{result_wagers['Data']['注單']['时间']} -0400", "%Y-%m-%d %H:%M:%S %z")
        WagersTimeString = dt.strftime(r'%Y/%m/%d %H:%M:%S %z')
        WagersTimeStamp = str(int(dt.timestamp())*1000)

        n = str(num).replace(',', '')
        result.update({'CategoryName': result_wagers['Data']['CategoryName'],
                       'GameName': result_wagers['Data']['注單']['游戏类别'],
                       'WagersTimeString': WagersTimeString,
                       'WagersTimeStamp': WagersTimeStamp,
                       'BetAmount': f'{abs(eval(n)):.2f}'})


        if ExtendLimit == '1' or ExtendLimit == 1:

            if GameCountType == '0' or GameCountType == 0:
                try:
                    if not gameList:
                        r = getGameList()
                        if r != False:
                            gameList = r

                    if gameList:
                        if set(SearchGameCategory) - set(gameList):
                            # return config['CATEGORY_NOT_SUPPORT']['msg']
                            return '< 分类配置 > 错误！请到后台「网站配置」→「分类配置」执行LEBO【分类更新】。'
                except:
                    pass

                #★取得【分類當日投注】
                result_amount = lebo.member_report_result(cf, url, params={
                    'date_start': f'{SearchDate} 00:00:00',
                    'date_end': f'{SearchDate} 23:59:59',
                    'account': Member,
                    'game[]': SearchGameCategory,
                    'model': '1',
                    'searchbtn': '查詢'}
                    )

                # 連線異常, 設定connect為False
                if result_amount["ErrorCode"] == config['CONNECTION_CODE']['code']:
                    return result_amount
                if result_amount["ErrorCode"] == config['SIGN_OUT_CODE']['code']:
                    return result_amount
                if result_amount["ErrorCode"] != config['SUCCESS_CODE']['code']:
                    return result_amount["ErrorMessage"]

                # 查詢投注金額失敗直接回傳
                if result_amount['IsSuccess'] is False:

                    result.update({
                                'IsSuccess': int(result_amount['IsSuccess']),
                                'ErrorCode': result_amount['ErrorCode'],
                                'ErrorMessage': result_amount['ErrorMessage']
                                })

                    return result

                elif result_amount['ErrorMessage']:
                    result.update({'ErrorMessage': result_amount['ErrorMessage']})
                    return result

                else:
                    if result_amount['Data']['總計：']['類型'] == '總計：':
                        n = str(result_amount['Data']['總計：']['有效投注']).replace(',', '')
                        result.update({'AllCategoryCommissionable': f'{eval(n):.2f}'})



            elif GameCountType == '1' or GameCountType == 1:
                try:
                    if not gameList:
                        r = getGameList()
                        if r != False:
                            gameList = r

                    if gameList:
                        if set(SearchGameCategory) - set(gameList):
                            # return config['CATEGORY_NOT_SUPPORT']['msg']
                            return '< 分类配置 > 错误！请到后台「网站配置」→「分类配置」执行LEBO【分类更新】。'
                except:
                    pass

                #★取得【本遊戲當日投注】
                result_amount = lebo.bet_record_v2_index(cf, Member, SearchGameCategory, url, data={
                    'betno': RawWagersId,
                    'date_start': f'{SearchDate} 00:00:00',
                    'date_end': f'{SearchDate} 23:59:59'}
                    )

                # 連線異常, 設定connect為False
                if result_amount["ErrorCode"] == config['CONNECTION_CODE']['code']:
                    return result_amount
                if result_amount["ErrorCode"] == config['SIGN_OUT_CODE']['code']:
                    return result_amount
                if result_amount["ErrorCode"] != config['SUCCESS_CODE']['code']:
                    return result_amount["ErrorMessage"]

                # 查詢投注金額失敗直接回傳
                if result_amount['IsSuccess'] is False:

                    result.update({
                                'IsSuccess': int(result_amount['IsSuccess']),
                                'ErrorCode': result_amount['ErrorCode'],
                                'ErrorMessage': result_amount['ErrorMessage']
                                })

                    return result

                elif result_amount['ErrorMessage']:
                    result.update({'ErrorMessage': result_amount['ErrorMessage']})
                    return result

                else:
                    if result_amount['Data']['GameCommissionable'] != '':
                        n = str(result_amount['Data']['GameCommissionable']).replace(',', '')
                        result.update({'GameCommissionable': f'{eval(n):.2f}'})



            elif GameCountType == '2' or GameCountType == 2:
                params = {
                    'date_start': f'{SearchDate} 00:00:00',
                    'date_end': f'{SearchDate} 23:59:59',
                    'account': Member,
                    'game[]': '',
                    'model': '1',
                    'searchbtn': '查詢'
                    }

                if result['CategoryName'] == 'BBIN电子':
                    params['game[]'] = ['bbdz']
                elif result['CategoryName'] == 'CQ9电子':
                    params['game[]'] = ['cq']
                elif result['CategoryName'] == 'JDB电子':
                    params['game[]'] = ['jdb']

                #★取得【本分類當日投注】
                result_amount = lebo.member_report_result(cf, url, params=params)

                # 連線異常, 設定connect為False
                if result_amount["ErrorCode"] == config['CONNECTION_CODE']['code']:
                    return result_amount
                if result_amount["ErrorCode"] == config['SIGN_OUT_CODE']['code']:
                    return result_amount
                if result_amount["ErrorCode"] != config['SUCCESS_CODE']['code']:
                    return result_amount["ErrorMessage"]

                # 查詢投注金額失敗直接回傳
                if result_amount['IsSuccess'] is False:

                    result.update({
                                'IsSuccess': int(result_amount['IsSuccess']),
                                'ErrorCode': result_amount['ErrorCode'],
                                'ErrorMessage': result_amount['ErrorMessage']
                                })

                    return result

                elif result_amount['ErrorMessage']:
                    result.update({'ErrorMessage': result_amount['ErrorMessage']})
                    return result

                else:
                    if result_amount['Data']['總計：']['類型'] == '總計：':
                        n = str(result_amount['Data']['總計：']['有效投注']).replace(',', '')
                        result.update({'SingleCategoryCommissionable': f'{eval(n):.2f}'})



        result.update({'IsSuccess': 1})
        return result


class betslip(BaseFunc):
    class Meta:
        suport = support['BBIN电子']['gamename']
        extra = {
                # # 需要登入第二個平台使用
                # 'platform': {
                #     'info': '平台',
                #     'var': list,
                #     'choise': ['GPK', 'LEBO', 'BBIN'],
                #     'default': 'GPK',
                # },
                # 'url': {
                #     'info': '網址',
                #     'var': str,
                #     'default': '',
                # },
                # 'username': {
                #     'info': '帳號',
                #     'var': str,
                #     'default': '',
                # },
                # 'password': {
                #     'info': '密碼',
                #     'var': str,
                #     'default': '',
                # },
            }
    @classmethod
    def audit(cls, url, DBId, Member, SearchGameCategory, SearchDate, timeout, cf, RawWagersId, ExtendLimit, GameCountType, **kwargs):
        '''
            LEBO 注单系统
            需要有【帐号管理 >> 输赢查询, 现金系统 >>第三方查询 / 层级管理 >> 会员查询】权限。支援「BBIN电子」。
        '''
        wagers_not_found = 0
        support_game = [i['GameCategory'] for i in support.values()]
        support_name = list(support.keys())
        not_search_game = list(set(support_game) - set(SearchGameCategory))
        not_search_name = [
            {data['GameCategory']: name for name, data in support.items()}.get(game, '')
            for game in not_search_game
        ]
        if set(support_game) & set(SearchGameCategory):
            logger.info(f'修正前:SearchGameCategory={SearchGameCategory}')
            SearchGameCategory = list(set(support_game) & set(SearchGameCategory))
            logger.info(f'機器人僅支援:{support_game},修正後:SearchGameCategory={SearchGameCategory}')
        else:
            return {
                    'IsSuccess': 0,
                    'ErrorCode': config.CATEGORY_NOT_SUPPORT.code,
                    'ErrorMessage': config.CATEGORY_NOT_SUPPORT.msg.format(supported=','.join(support_name)),
                    'DBId': DBId,
                    'RawWagersId': RawWagersId,
                    'Member': Member,
                    'BlockMemberLayer': '',
                    'GameName': '',
                    'WagersTimeString': '',
                    'WagersTimeStamp': '',
                    'BetAmount': '0.00',
                    'AllCategoryCommissionable': '0.00',
                    'GameCommissionable': '0.00',
                    'SingleCategoryCommissionable': '0.00',
                    'CategoryName': '',
                    'ExtendLimit': ExtendLimit,
                    'GameCountType': GameCountType
                    }

        SearchDate = SearchDate.replace('/', '-')

        # 取得會員層級
        level_result = lebo.app_cash_utotal(cf=cf, url=url, params={
                                                                    'username': Member.lower(),
                                                                    'savebtn': '查詢'
                                                                    }, timeout=timeout)
        if level_result['ErrorCode'] in (config.PERMISSION_CODE.code, config.HTML_STATUS_CODE.code):
            return level_result['ErrorMessage']
        if level_result['IsSuccess'] is False and level_result['ErrorCode'] == config.SUCCESS_CODE.code:
            return {
                    'IsSuccess': int(level_result['IsSuccess']),
                    'ErrorCode': level_result['ErrorCode'],
                    'ErrorMessage': level_result['ErrorMessage'],
                    'DBId': DBId,
                    'RawWagersId': RawWagersId,
                    'Member': Member,
                    'BlockMemberLayer': '',
                    'GameName': '',
                    'WagersTimeString': '',
                    'WagersTimeStamp': '',
                    'BetAmount': '0.00',
                    'AllCategoryCommissionable': '0.00',
                    'GameCommissionable': '0.00',
                    'SingleCategoryCommissionable': '0.00',
                    'CategoryName': '',
                    'ExtendLimit': ExtendLimit,
                    'GameCountType': GameCountType
                    }
        if level_result["ErrorCode"] in (config.CONNECTION_CODE.code,config.SIGN_OUT_CODE.code):
            return level_result
        if level_result['ErrorCode'] in (config.HTML_CONTENT_CODE.code, config.NO_USER.code):
            return {
                    'IsSuccess': int(level_result['IsSuccess']),
                    'ErrorCode': level_result['ErrorCode'],
                    'ErrorMessage': level_result['ErrorMessage'],
                    'DBId': DBId,
                    'RawWagersId': RawWagersId,
                    'Member': Member,
                    'BlockMemberLayer': '',
                    'GameName': '',
                    'WagersTimeString': '',
                    'WagersTimeStamp': '',
                    'BetAmount': '0.00',
                    'AllCategoryCommissionable': '0.00',
                    'GameCommissionable': '0.00',
                    'SingleCategoryCommissionable': '0.00',
                    'CategoryName': '',
                    'ExtendLimit': ExtendLimit,
                    'GameCountType': GameCountType
                    }

        BlockMemberLayer = level_result['Data'][Member.lower()]['所屬層級']

        # 查詢注單金額
        search_game = [support[i]['api_name'] for i in support if support[i]['GameCategory'] in SearchGameCategory]
        wagers_not_found_msg = ',目前不支援' + ','.join(not_search_name) + '等查詢' if not_search_name else ''
        if not search_game:
            return {
                        'IsSuccess': 0,
                        'ErrorCode': config.CATEGORY_NOT_SUPPORT.code,
                        'ErrorMessage': config.CATEGORY_NOT_SUPPORT.msg.format(supported=','.join([key for key in support])),
                        'DBId': DBId,
                        'RawWagersId': RawWagersId,
                        'Member': Member,
                        'BlockMemberLayer': BlockMemberLayer,
                        'GameName': '',
                        'WagersTimeString': '',
                        'WagersTimeStamp': '',
                        'BetAmount': '0.00',
                        'AllCategoryCommissionable': '0.00',
                        'GameCommissionable': '0.00',
                        'SingleCategoryCommissionable': '0.00',
                        'CategoryName': '',
                        'ExtendLimit': ExtendLimit,
                        'GameCountType': GameCountType
                        }
        for api_name in search_game:
            if api_name in ['bbin']:
                data_result = lebo.app_mgame(cf=cf, url=url, data={
                                                                    'account': Member.lower(),
                                                                    'no': RawWagersId,
                                                                    'searchbtn': '查詢',
                                                                    }, endpoints=f'app/mgame/{api_name}.php')
                if data_result['ErrorCode'] in (config.PERMISSION_CODE.code, config.HTML_STATUS_CODE.code):
                    return data_result['ErrorMessage']
                if data_result['ErrorCode'] in (config.CONNECTION_CODE.code, config.SIGN_OUT_CODE.code):
                    return data_result
                if data_result['ErrorCode'] in (config.WAGERS_NOT_FOUND.code, config.USER_WAGERS_NOT_MATCH.code):
                    wagers_not_found += 1
                    continue
                if data_result['ErrorCode'] == config.HTML_CONTENT_CODE.code:
                    return {
                            'IsSuccess': int(data_result['IsSuccess']),
                            'ErrorCode': data_result['ErrorCode'],
                            'ErrorMessage': data_result['ErrorMessage'],
                            'DBId': DBId,
                            'RawWagersId': RawWagersId,
                            'Member': Member,
                            'BlockMemberLayer': BlockMemberLayer,
                            'GameName': '',
                            'WagersTimeString': '',
                            'WagersTimeStamp': '',
                            'BetAmount': '0.00',
                            'AllCategoryCommissionable': '0.00',
                            'GameCommissionable': '0.00',
                            'SingleCategoryCommissionable': '0.00',
                            'CategoryName': '',
                            'ExtendLimit': ExtendLimit,
                            'GameCountType': GameCountType
                            }
                # 處理iframe
                iframe_result = lebo.iframe_processing(cf=cf, resp=data_result['Data'])
                if iframe_result['ErrorCode'] == config.HTML_CONTENT_CODE.code:
                    return {
                            'IsSuccess': int(data_result['IsSuccess']),
                            'ErrorCode': data_result['ErrorCode'],
                            'ErrorMessage': data_result['ErrorMessage'],
                            'DBId': DBId,
                            'RawWagersId': RawWagersId,
                            'Member': Member,
                            'BlockMemberLayer': BlockMemberLayer,
                            'GameName': '',
                            'WagersTimeString': '',
                            'WagersTimeStamp': '',
                            'BetAmount': '0.00',
                            'AllCategoryCommissionable': '0.00',
                            'GameCommissionable': '0.00',
                            'SingleCategoryCommissionable': '0.00',
                            'CategoryName': '',
                            'ExtendLimit': ExtendLimit,
                            'GameCountType': GameCountType
                            }
                if iframe_result['ErrorCode'] == config.HTML_STATUS_CODE.code:
                    return iframe_result['ErrorMessage']
                if iframe_result['ErrorCode'] == config.CONNECTION_CODE.code:
                    return iframe_result
                # 處理BBIN电子資訊
                bets_result = lebo.process_bbin(cf=cf, resp=data_result['Data'], iframe_content=iframe_result['Data'])
                if bets_result['ErrorCode'] in (config.HTML_CONTENT_CODE.code, config.GAME_ERROR.code):
                    return {
                        'IsSuccess': int(bets_result['IsSuccess']),
                        'ErrorCode': bets_result['ErrorCode'],
                        'ErrorMessage': bets_result['ErrorMessage'],
                        'DBId': DBId,
                        'RawWagersId': RawWagersId,
                        'Member': Member,
                        'BlockMemberLayer': BlockMemberLayer,
                        'GameName': '',
                        'WagersTimeString': '',
                        'WagersTimeStamp': '',
                        'BetAmount': '0.00',
                        'AllCategoryCommissionable': '0.00',
                        'GameCommissionable': '0.00',
                        'SingleCategoryCommissionable': '0.00',
                        'CategoryName': '',
                        'ExtendLimit': ExtendLimit,
                        'GameCountType': GameCountType
                        }
                if bets_result['ErrorCode'] != config.SUCCESS_CODE.code:
                    return bets_result['ErrorMessage']
                GameName = bets_result['Data']['游戏类别']
                dt = datetime.datetime.strptime(bets_result['Data']['时间']+' -0400',"%Y-%m-%d %H:%M:%S %z")
                WagersTimeStamp = str(int(dt.timestamp()) * 1000)
                WagersTimeString = dt.strftime('%Y/%m/%d %H:%M:%S %z')
                BetAmount = re.findall(r'[-\d.,]+',bets_result['Data']['下注资讯'])[-1]
                CategoryName = [key for key in support if support[key]['api_name'] == api_name][0]
                AllCategoryCommissionable = '0.00' #選取分類當日投注
                GameCommissionable = '0.00' #本遊戲當日投注
                SingleCategoryCommissionable = '0.00' #本分類當日投注
                break
            elif api_name == 'cq':
                pass
            elif api_name == 'jdb':
                pass
            else:
                pass
        if len(search_game) == wagers_not_found:
            return {
                    'IsSuccess': 0,
                    'ErrorCode': config.WAGERS_NOT_FOUND.code,
                    'ErrorMessage': f'{config.WAGERS_NOT_FOUND.msg}{wagers_not_found_msg}',
                    'DBId': DBId,
                    'RawWagersId': RawWagersId,
                    'Member': Member,
                    'BlockMemberLayer': BlockMemberLayer,
                    'GameName': '',
                    'WagersTimeString': '',
                    'WagersTimeStamp': '',
                    'BetAmount': '0.00',
                    'AllCategoryCommissionable': '0.00',
                    'GameCommissionable': '0.00',
                    'SingleCategoryCommissionable': '0.00',
                    'CategoryName': '',
                    'ExtendLimit': ExtendLimit,
                    'GameCountType': GameCountType
                    }
        if not int(ExtendLimit):
            return {
                    "IsSuccess": 1,
                    "ErrorCode": config.SUCCESS_CODE.code,
                    "ErrorMessage": config.SUCCESS_CODE.msg,
                    "DBId": DBId,
                    "RawWagersId": RawWagersId,
                    "Member": Member,
                    "BlockMemberLayer": BlockMemberLayer,
                    "GameName": GameName,
                    "WagersTimeString": WagersTimeString,
                    "WagersTimeStamp": WagersTimeStamp,
                    "BetAmount": BetAmount,
                    "AllCategoryCommissionable": AllCategoryCommissionable,
                    "GameCommissionable": GameCommissionable,
                    "SingleCategoryCommissionable": SingleCategoryCommissionable,
                    "CategoryName": CategoryName,
                    "ExtendLimit": ExtendLimit,
                    "GameCountType": GameCountType
                }

        # 計算總投注金額
        search_GameCategory = [support[i]['GameCategory'] for i in support if support[i]['GameCategory'] in SearchGameCategory]
        GameCountType_result = lebo.member_report(cf=cf, url=url, params={
                                                                        'date_start': f'{SearchDate} 00:00:00',
                                                                        'date_end': f'{SearchDate} 23:59:59',
                                                                        'account': Member.lower(),
                                                                        'game[]': search_GameCategory,
                                                                        'model': '1',
                                                                        'searchbtn': '查詢'
                                                                        }, timeout=timeout)
        if GameCountType_result['ErrorCode'] in (config.PERMISSION_CODE.code, config.HTML_CONTENT_CODE.code, config.HTML_STATUS_CODE.code):
            return GameCountType_result['ErrorMessage']
        if GameCountType_result['ErrorCode'] in (config.CONNECTION_CODE.code, config.SIGN_OUT_CODE.code):
            return GameCountType_result
        if GameCountType == '0':#選取所有分類當日投注
            #AllCategoryCommissionable = str(sum([float(GameCountType_result['Data'][classes]['有效投注']) for classes in GameCountType_result['Data']]))
            AllCategoryCommissionable = str([float(GameCountType_result['Data'][classes]['有效投注']) for classes in GameCountType_result['Data'] if classes == '總計：'][0])
            AllCategoryCommissionable = '.'.join([AllCategoryCommissionable.partition('.')[0],AllCategoryCommissionable.partition('.')[2][:2]]).replace(',','') #無條件捨去至小數第2位
        else:#選取本分類當日投注以及本遊戲當日投注
            GameCategory = [support[key]['GameCategory'] for key in support if key == CategoryName][0]
            endpoints = [GameCountType_result['Data'][classes]['連結'] for classes in GameCountType_result['Data'] if '連結' in GameCountType_result['Data'][classes] and GameCountType_result['Data'][classes]['連結'].split('gametype=')[1] == GameCategory][0]
            Single_result = lebo.bet_record_v2(cf=cf, url=url, endpoints=endpoints, timeout=timeout)
            if Single_result['ErrorCode'] in (config.PERMISSION_CODE.code, config.HTML_STATUS_CODE.code):
                return Single_result['ErrorMessage']
            if Single_result['ErrorCode'] in (config.CONNECTION_CODE.code, config.SIGN_OUT_CODE.code):
                return Single_result
            if Single_result['ErrorCode'] != config.SUCCESS_CODE.code:
                return Single_result['ErrorMessage']
            SingleCategoryCommissionable = Single_result['Data']['总計']['有效投注']
            SingleCategoryCommissionable = '.'.join([SingleCategoryCommissionable.partition('.')[0],SingleCategoryCommissionable.partition('.')[2][:2]]).replace(',','') #無條件捨去至小數第2位
            typeid = Single_result['Data']['遊戲ID'][GameName]
            Game_result = lebo.bet_record_v2(cf=cf, url=url, data={'gametype':GameCategory,
                                                        'typeid':typeid,
                                                        'date_start': f'{SearchDate} 00:00:00',
                                                        'date_end': f'{SearchDate} 23:59:59',
                                                        }, endpoints=endpoints, method='post', timeout=timeout)
            if Game_result['ErrorCode'] in (config.PERMISSION_CODE.code, config.HTML_STATUS_CODE.code):
                return Game_result['ErrorMessage']
            if Game_result['ErrorCode'] in (config.CONNECTION_CODE.code, config.SIGN_OUT_CODE.code):
                return Game_result
            if Game_result['ErrorCode'] != config.SUCCESS_CODE.code:
                return Game_result['ErrorMessage']
            GameCommissionable = Game_result['Data']['总計']['有效投注']
            GameCommissionable = '.'.join([GameCommissionable.partition('.')[0],GameCommissionable.partition('.')[2][:2]]).replace(',','') #無條件捨去至小數第2位
        return {
                "IsSuccess": 1,
                "ErrorCode": config.SUCCESS_CODE.code,
                "ErrorMessage": config.SUCCESS_CODE.msg,
                "DBId": DBId,
                "RawWagersId": RawWagersId,
                "Member": Member,
                "BlockMemberLayer": BlockMemberLayer,
                "GameName": GameName,
                "WagersTimeString": WagersTimeString,
                "WagersTimeStamp": WagersTimeStamp,
                "BetAmount": BetAmount,
                "AllCategoryCommissionable": AllCategoryCommissionable,
                "GameCommissionable": GameCommissionable,
                "SingleCategoryCommissionable": SingleCategoryCommissionable,
                "CategoryName": CategoryName,
                "ExtendLimit": ExtendLimit,
                "GameCountType": GameCountType
        }

class enjoyup(BaseFunc):
    '''LEBO 喜上喜'''
    class Meta:
        suport = {}
        extra = {
            # # 需要登入第二個平台使用
            # 'platform': {
            #     'info': '平台',
            #     'var': list,
            #     'choise': ['GPK', 'LEBO', 'BBIN'],
            #     'default': 'GPK',
            # },
            # 'url': {
            #     'info': '網址',
            #     'var': str,
            #     'default': '',
            # },
            # 'username': {
            #     'info': '帳號',
            #     'var': str,
            #     'default': '',
            # },
            # 'password': {
            #     'info': '密碼',
            #     'var': str,
            #     'default': '',
            # },
        }

    @classmethod
    def deposit(cls, *args, **kwargs):
        '''
            LEBO 充值
            需要有【現金系統 >> 存款與取款】权限
        '''
        SupportStatus = kwargs.get('SupportStatus')
        cls.th = ThreadProgress(kwargs['cf'], kwargs['mod_key'], detail=bool(SupportStatus))
        cls.th.start()

        result = cls._deposit(*args, **kwargs)

        if SupportStatus:
            cls.return_schedule(Action='chkpoint', DBId=kwargs['DBId'], Status='【充值机器人处理完毕】', Progress='-', Detail='-')
        else:
            cls.return_schedule(Action='chkpoint', DBId=kwargs['DBId'], Member=kwargs['Member'], current_status='【充值机器人处理完毕】')

        cls.th.stop()
        return result

    @classmethod
    def audit(cls, *args, **kwargs):
        '''
            LEBO 喜上喜系统
            需要有【帐号管理 >> 输赢查询, 现金系统 >>第三方查询 / 层级管理 >> 会员查询】权限。支援「BBIN电子」。
        '''
        SupportStatus = kwargs.get('SupportStatus')
        cls.th = ThreadProgress(kwargs['cf'], kwargs['mod_key'], detail=bool(SupportStatus))
        cls.th.start()

        result = cls._audit(*args, **kwargs)

        if SupportStatus:
            cls.return_schedule(Action='chkbbin', DBId=kwargs['DBId'], Status='【审核机器人处理完毕】', Progress='-', Detail='-')
        else:
            cls.return_schedule(Action='chkbbin', DBId=kwargs['DBId'], Member=kwargs['Member'], current_status='【审核机器人处理完毕】')
        cls.th.stop()
        return result

    @classmethod
    def _deposit(cls, url, DBId, Member, DepositAmount, mod_name, mod_key,
        timeout, backend_remarks, multiple, cf, amount_memo='', Note='', increasethebet_switch=0, increasethebet='0', **kwargs):
        '''
            LEBO 充值
            需要有【現金系統 >> 存款與取款】权限
        '''
        return BaseFunc.deposit(url, DBId, Member, DepositAmount, mod_name, mod_key,
            timeout, backend_remarks, multiple, cf, amount_memo, Note, increasethebet_switch, increasethebet, **kwargs)

    @classmethod
    def _audit(cls, url, DBId, mod_name, Member, SearchGameCategory, SearchDate, timeout, cf, RawWagersId, ExtendLimit, GameCountType, **kwargs):
        '''
            LEBO 喜上喜系统
            需要有【帐号管理 >> 输赢查询, 现金系统 >>第三方查询 / 层级管理 >> 会员查询】权限。支援「BBIN电子」。
        '''
        wagers_not_found = 0
        support_game = [i['GameCategory'] for i in support.values()]
        not_search_game = list(set(support_game) - set(SearchGameCategory))
        not_search_name = [
            {data['GameCategory']: name for name, data in support.items()}.get(game, '')
            for game in not_search_game
        ]
        if set(support_game) & set(SearchGameCategory):
            logger.info(f'修正前:SearchGameCategory={SearchGameCategory}')
            SearchGameCategory = list(set(support_game) & set(SearchGameCategory))
            logger.info(f'機器人僅支援:{support_game},修正後:SearchGameCategory={SearchGameCategory}')
        else:
            return {
                    'IsSuccess': 0,
                    'ErrorCode': config.CATEGORY_NOT_SUPPORT.code,
                    'ErrorMessage': config.CATEGORY_NOT_SUPPORT.msg.format(supported=','.join(support.keys())),
                    'DBId': DBId,
                    'RawWagersId': RawWagersId,
                    'Member': Member,
                    'BlockMemberLayer': '',
                    'GameName': '',
                    'WagersTimeString': '',
                    'WagersTimeStamp': '',
                    'BetAmount': '0.00',
                    'AllCategoryCommissionable': '0.00',
                    'GameCommissionable': '0.00',
                    'SingleCategoryCommissionable': '0.00',
                    'CategoryName': '',
                    'ExtendLimit': ExtendLimit,
                    'GameCountType': GameCountType,
                    'PayoutAmount': '0.00'
                    }

        SearchDate = SearchDate.replace('/', '-')
        SupportStatus = kwargs.get('SupportStatus')
        member = Member.lower()

        # 取得會員層級
        level_result = lebo.app_cash_utotal(cf=cf, url=url, params={
                                                                    'username': member,
                                                                    'savebtn': '查詢'
                                                                    }, timeout=timeout)
        BlockMemberLayer = level_result.get('Data', {}).get(member, {}).get('所屬層級', '')
        # 【查詢會員帳號】
        if SupportStatus:
            if not level_result["IsSuccess"]:
                Detail = f'查询失败\n搜寻帐号：{member}\n{level_result["ErrorMessage"]}'
            else:
                Detail = f'查询成功\n搜寻帐号：{member}'
                if member in level_result["Data"]:
                    Detail += f'\n会员是否存在：是'
                    Detail += f'\n会员层级：{BlockMemberLayer}'
                else:
                    Detail += f'\n会员是否存在：否'
            cls.return_schedule(Action='chkbbin', DBId=DBId, Status='【会员列表】 - 【帐号列表】', Progress=f'1/1', Detail=Detail)
        else:
            cls.return_schedule(Action='chkbbin', DBId=DBId, Member=Member, current_status='【会员列表】 - 【帐号列表】')
        if level_result['ErrorCode'] in (config.PERMISSION_CODE.code, config.HTML_STATUS_CODE.code):
            return level_result['ErrorMessage']
        if level_result['IsSuccess'] is False and level_result['ErrorCode'] == config.SUCCESS_CODE.code:
            return {
                    'IsSuccess': int(level_result['IsSuccess']),
                    'ErrorCode': level_result['ErrorCode'],
                    'ErrorMessage': level_result['ErrorMessage'],
                    'DBId': DBId,
                    'RawWagersId': RawWagersId,
                    'Member': Member,
                    'BlockMemberLayer': '',
                    'GameName': '',
                    'WagersTimeString': '',
                    'WagersTimeStamp': '',
                    'BetAmount': '0.00',
                    'AllCategoryCommissionable': '0.00',
                    'GameCommissionable': '0.00',
                    'SingleCategoryCommissionable': '0.00',
                    'CategoryName': '',
                    'ExtendLimit': ExtendLimit,
                    'GameCountType': GameCountType,
                    'PayoutAmount': '0.00'
                    }
        if level_result["ErrorCode"] in (config.CONNECTION_CODE.code, config.SIGN_OUT_CODE.code):
            return level_result
        if level_result['ErrorCode'] in (config.HTML_CONTENT_CODE.code, config.NO_USER.code):
            return {
                    'IsSuccess': int(level_result['IsSuccess']),
                    'ErrorCode': level_result['ErrorCode'],
                    'ErrorMessage': level_result['ErrorMessage'],
                    'DBId': DBId,
                    'RawWagersId': RawWagersId,
                    'Member': Member,
                    'BlockMemberLayer': '',
                    'GameName': '',
                    'WagersTimeString': '',
                    'WagersTimeStamp': '',
                    'BetAmount': '0.00',
                    'AllCategoryCommissionable': '0.00',
                    'GameCommissionable': '0.00',
                    'SingleCategoryCommissionable': '0.00',
                    'CategoryName': '',
                    'ExtendLimit': ExtendLimit,
                    'GameCountType': GameCountType,
                    'PayoutAmount': '0.00'
                    }

        # 查詢注單金額
        search_game = [support[i]['api_name'] for i in support if support[i]['GameCategory'] in SearchGameCategory]
        wagers_not_found_msg = f',目前不支援{",".join(not_search_name)}等查詢' if not_search_name else ''
        for i, api_name in enumerate(search_game):
            if api_name in ['bbin']:
                data_result = lebo.app_mgame(cf=cf, url=url, data={
                                                                    'account': Member.lower(),
                                                                    'no': RawWagersId,
                                                                    'searchbtn': '查詢',
                                                                    }, endpoints=f'app/mgame/{api_name}.php')

                if SupportStatus:
                    if data_result['ErrorCode'] != config.SUCCESS_CODE.code:
                        Detail = f'查询失败\n{data_result["ErrorMessage"]}'
                        Progress = f'{i}/{len(search_game)}'
                    else:
                        Detail = f'查询成功'
                        Progress = f'{i}/{i}'
                    cls.return_schedule(Action='chkbbin', DBId=DBId, Status='【現金系統 >> 第三方查詢 >> 第三方注單查詢】', Progress=Progress, Detail=Detail)
                else:
                    cls.return_schedule(Action='chkbbin', DBId=DBId, Member=Member, current_status='【現金系統 >> 第三方查詢 >> 第三方注單查詢】')

                if data_result['ErrorCode'] in (config.PERMISSION_CODE.code, config.HTML_STATUS_CODE.code):
                    return data_result['ErrorMessage']
                if data_result['ErrorCode'] in (config.CONNECTION_CODE.code, config.SIGN_OUT_CODE.code):
                    return data_result
                if data_result['ErrorCode'] in (config.WAGERS_NOT_FOUND.code, config.USER_WAGERS_NOT_MATCH.code):
                    wagers_not_found += 1
                    continue
                if data_result['ErrorCode'] == config.HTML_CONTENT_CODE.code:
                    return {
                            'IsSuccess': int(data_result['IsSuccess']),
                            'ErrorCode': data_result['ErrorCode'],
                            'ErrorMessage': data_result['ErrorMessage'],
                            'DBId': DBId,
                            'RawWagersId': RawWagersId,
                            'Member': Member,
                            'BlockMemberLayer': BlockMemberLayer,
                            'GameName': '',
                            'WagersTimeString': '',
                            'WagersTimeStamp': '',
                            'BetAmount': '0.00',
                            'AllCategoryCommissionable': '0.00',
                            'GameCommissionable': '0.00',
                            'SingleCategoryCommissionable': '0.00',
                            'CategoryName': '',
                            'ExtendLimit': ExtendLimit,
                            'GameCountType': GameCountType,
                            'PayoutAmount': '0.00'
                            }
                # 處理iframe
                iframe_result = lebo.iframe_processing(cf=cf, resp=data_result['Data'])
                if iframe_result['ErrorCode'] == config.HTML_STATUS_CODE.code:
                    return iframe_result['ErrorMessage']
                if iframe_result['ErrorCode'] == config.CONNECTION_CODE.code:
                    return iframe_result
                # 處理BBIN电子資訊
                bets_result = lebo.process_bbin(cf=cf, resp=data_result['Data'], iframe_content=iframe_result['Data'])
                if bets_result['ErrorCode'] in (config.HTML_CONTENT_CODE.code, config.GAME_ERROR.code):
                    return {
                        'IsSuccess': int(bets_result['IsSuccess']),
                        'ErrorCode': bets_result['ErrorCode'],
                        'ErrorMessage': bets_result['ErrorMessage'],
                        'DBId': DBId,
                        'RawWagersId': RawWagersId,
                        'Member': Member,
                        'BlockMemberLayer': BlockMemberLayer,
                        'GameName': '',
                        'WagersTimeString': '',
                        'WagersTimeStamp': '',
                        'BetAmount': '0.00',
                        'AllCategoryCommissionable': '0.00',
                        'GameCommissionable': '0.00',
                        'SingleCategoryCommissionable': '0.00',
                        'CategoryName': '',
                        'ExtendLimit': ExtendLimit,
                        'GameCountType': GameCountType,
                        'PayoutAmount': '0.00'
                        }
                if bets_result['ErrorCode'] != config.SUCCESS_CODE.code:
                    return bets_result['ErrorMessage']
                GameName = bets_result['Data']['游戏类别']
                dt = datetime.datetime.strptime(bets_result['Data']['时间']+' -0400',"%Y-%m-%d %H:%M:%S %z")
                WagersTimeStamp = str(int(dt.timestamp()) * 1000)
                WagersTimeString = dt.strftime('%Y/%m/%d %H:%M:%S %z')
                BetAmount = re.findall(r'[-\d.,]+',bets_result['Data']['下注资讯'])[-1]
                BetAmount = f'{float(BetAmount):.2f}'
                PayoutAmount = f"{float(bets_result['Data']['总派彩']):.2f}"
                CategoryName = [key for key in support if support[key]['api_name'] == api_name][0]
                AllCategoryCommissionable = '0.00' #選取分類當日投注
                GameCommissionable = '0.00' #本遊戲當日投注
                SingleCategoryCommissionable = '0.00' #本分類當日投注
                break
            elif api_name == 'cq':
                pass
            elif api_name == 'jdb':
                pass
            else:
                pass
        if len(search_game) == wagers_not_found:
            return {
                    'IsSuccess': 0,
                    'ErrorCode': config.WAGERS_NOT_FOUND.code,
                    'ErrorMessage': f'{config.WAGERS_NOT_FOUND.msg}{wagers_not_found_msg}',
                    'DBId': DBId,
                    'RawWagersId': RawWagersId,
                    'Member': Member,
                    'BlockMemberLayer': BlockMemberLayer,
                    'GameName': '',
                    'WagersTimeString': '',
                    'WagersTimeStamp': '',
                    'BetAmount': '0.00',
                    'AllCategoryCommissionable': '0.00',
                    'GameCommissionable': '0.00',
                    'SingleCategoryCommissionable': '0.00',
                    'CategoryName': '',
                    'ExtendLimit': ExtendLimit,
                    'GameCountType': GameCountType,
                    'PayoutAmount': '0.00',
                    }
        if not int(ExtendLimit):
            return {
                    "IsSuccess": 1,
                    "ErrorCode": config.SUCCESS_CODE.code,
                    "ErrorMessage": config.SUCCESS_CODE.msg,
                    "DBId": DBId,
                    "RawWagersId": RawWagersId,
                    "Member": Member,
                    "BlockMemberLayer": BlockMemberLayer,
                    "GameName": GameName,
                    "WagersTimeString": WagersTimeString,
                    "WagersTimeStamp": WagersTimeStamp,
                    "BetAmount": BetAmount,
                    "AllCategoryCommissionable": AllCategoryCommissionable,
                    "GameCommissionable": GameCommissionable,
                    "SingleCategoryCommissionable": SingleCategoryCommissionable,
                    "CategoryName": CategoryName,
                    "ExtendLimit": ExtendLimit,
                    "GameCountType": GameCountType,
                    "PayoutAmount": PayoutAmount
                }

        # 計算總投注金額
        GameCountType_result = lebo.member_report(cf=cf, url=url, params={
                                                                        'date_start': f'{SearchDate} 00:00:00',
                                                                        'date_end': f'{SearchDate} 23:59:59',
                                                                        'account': member,
                                                                        'game[]': SearchGameCategory,
                                                                        'model': '1',
                                                                        'searchbtn': '查詢'
                                                                        }, timeout=timeout)
        if SupportStatus:
            if not GameCountType_result["IsSuccess"]:
                Detail = f'查询失败\n{GameCountType_result["ErrorMessage"]}'
            else:
                Detail = f'查询成功'
            cls.return_schedule(Action='chkbbin', DBId=DBId, Status='【帳號管理 >> 輸贏查詢】', Progress='1/1', Detail=Detail)
        else:
            cls.return_schedule(Action='chkbbin', DBId=DBId, Member=Member, current_status='【帳號管理 >> 輸贏查詢】')

        if GameCountType_result['ErrorCode'] in (config.PERMISSION_CODE.code, config.HTML_CONTENT_CODE.code, config.HTML_STATUS_CODE.code):
            return GameCountType_result['ErrorMessage']
        if GameCountType_result['ErrorCode'] in (config.CONNECTION_CODE.code, config.SIGN_OUT_CODE.code):
            return GameCountType_result
        if GameCountType == '0':#選取所有分類當日投注
            AllCategoryCommissionable = GameCountType_result['Data'].get('總計：', {}).get('有效投注', '0.00')
            AllCategoryCommissionable = f"{float(AllCategoryCommissionable.replace(',', ''))*100//1/100:.2f}"
        else:#選取本分類當日投注以及本遊戲當日投注
            GameCategory = support[CategoryName]['GameCategory']
            endpoints = [data['連結'] for data in GameCountType_result['Data'].values() if f'gametype={GameCategory}' in data.get('連結', '')][0]
            Single_result = lebo.bet_record_v2(cf=cf, url=url, endpoints=endpoints, timeout=timeout)
            if SupportStatus:
                if not Single_result["IsSuccess"]:
                    Detail = f'查询失败\n{Single_result["ErrorMessage"]}'
                else:
                    Detail = f'查询成功'
                cls.return_schedule(Action='chkbbin', DBId=DBId, Status='【帳號管理 >> 輸贏查詢 >> 投注金額】', Progress='1/2', Detail=Detail)
            else:
                cls.return_schedule(Action='chkbbin', DBId=DBId, Member=Member, current_status='【帳號管理 >> 輸贏查詢 >> 投注金額】')

            if Single_result['ErrorCode'] in (config.PERMISSION_CODE.code, config.HTML_STATUS_CODE.code):
                return Single_result['ErrorMessage']
            if Single_result['ErrorCode'] in (config.CONNECTION_CODE.code, config.SIGN_OUT_CODE.code):
                return Single_result
            if Single_result['ErrorCode'] != config.SUCCESS_CODE.code:
                return Single_result['ErrorMessage']

            SingleCategoryCommissionable = Single_result['Data']['总計']['有效投注']
            SingleCategoryCommissionable = '.'.join([SingleCategoryCommissionable.partition('.')[0],SingleCategoryCommissionable.partition('.')[2][:2]]).replace(',','') #無條件捨去至小數第2位
            typeid = Single_result['Data']['遊戲ID'][GameName]
            Game_result = lebo.bet_record_v2(cf=cf, url=url, data={'gametype':GameCategory,
                                                        'typeid':typeid,
                                                        'date_start': f'{SearchDate} 00:00:00',
                                                        'date_end': f'{SearchDate} 23:59:59',
                                                        }, endpoints=endpoints, method='post', timeout=timeout)
            if SupportStatus:
                if not Game_result["IsSuccess"]:
                    Detail = f'查询失败\n{Game_result["ErrorMessage"]}'
                else:
                    Detail = f'查询成功'
                cls.return_schedule(Action='chkbbin', DBId=DBId, Status='【帳號管理 >> 輸贏查詢 >> 投注金額】', Progress='2/2', Detail=Detail)
            else:
                cls.return_schedule(Action='chkbbin', DBId=DBId, Member=Member, current_status='【帳號管理 >> 輸贏查詢 >> 投注金額】')

            if Game_result['ErrorCode'] in (config.PERMISSION_CODE.code, config.HTML_STATUS_CODE.code):
                return Game_result['ErrorMessage']
            if Game_result['ErrorCode'] in (config.CONNECTION_CODE.code, config.SIGN_OUT_CODE.code):
                return Game_result
            if Game_result['ErrorCode'] != config.SUCCESS_CODE.code:
                return Game_result['ErrorMessage']
            GameCommissionable = Game_result['Data']['总計']['有效投注']
            GameCommissionable = '.'.join([GameCommissionable.partition('.')[0],GameCommissionable.partition('.')[2][:2]]).replace(',','') #無條件捨去至小數第2位
        return {
                "IsSuccess": 1,
                "ErrorCode": config.SUCCESS_CODE.code,
                "ErrorMessage": config.SUCCESS_CODE.msg,
                "DBId": DBId,
                "RawWagersId": RawWagersId,
                "Member": Member,
                "BlockMemberLayer": BlockMemberLayer,
                "GameName": GameName,
                "WagersTimeString": WagersTimeString,
                "WagersTimeStamp": WagersTimeStamp,
                "BetAmount": BetAmount,
                "AllCategoryCommissionable": AllCategoryCommissionable,
                "GameCommissionable": GameCommissionable,
                "SingleCategoryCommissionable": SingleCategoryCommissionable,
                "CategoryName": CategoryName,
                "ExtendLimit": ExtendLimit,
                "GameCountType": GameCountType,
                "PayoutAmount": PayoutAmount
        }


class freespin(BaseFunc):
    '''LEBO 旋轉注單'''
    class Meta:
        suport = freespin_support
        extra = {
            # # 需要登入第二個平台使用
            # 'platform': {
            #     'info': '平台',
            #     'var': list,
            #     'choise': ['GPK', 'LEBO', 'BBIN'],
            #     'default': 'GPK',
            # },
            # 'url': {
            #     'info': '網址',
            #     'var': str,
            #     'default': '',
            # },
            # 'username': {
            #     'info': '帳號',
            #     'var': str,
            #     'default': '',
            # },
            # 'password': {
            #     'info': '密碼',
            #     'var': str,
            #     'default': '',
            # },
        }

    @classmethod
    def deposit(cls, *args, **kwargs):
        '''
            LEBO 充值
            需要有【現金系統 >> 存款與取款】权限
        '''
        cls.th = ThreadProgress(kwargs['cf'], kwargs['mod_key'], detail=False)
        cls.th.start()

        result = cls._deposit(*args, **kwargs)

        cls.return_schedule(Action='chkpoint', DBId=kwargs['DBId'], Member=kwargs['Member'], current_status='【充值机器人处理完毕】')
        cls.th.stop()
        return result

    @classmethod
    def audit(cls, *args, **kwargs):
        '''
            LEBO 旋转注单系统
            需要有【帐号管理 >> 输赢查询 / 帐号转换, 现金系统 >>第三方查询 / 层级管理 >> 会员查询】权限。支援「CQ9电子」(跳高高、跳高高2、跳起来) &「JDB电子」(变脸、飞鸟派对)。
        '''
        SupportStatus = kwargs.get('SupportStatus')
        cls.th = ThreadProgress(kwargs['cf'], kwargs['mod_key'], detail=bool(SupportStatus))
        cls.th.start()

        result = cls._audit(*args, **kwargs)

        if SupportStatus:
            cls.return_schedule(Action='chkbbin', DBId=kwargs['DBId'], Status='【审核机器人处理完毕】', Progress='-', Detail='-')
        else:
           cls.return_schedule(Action='chkbbin', DBId=kwargs['DBId'], Member=kwargs['Member'], current_status='【审核机器人处理完毕】')
        cls.th.stop()
        return result

    @classmethod
    def _deposit(cls, url, DBId, Member, DepositAmount, mod_name, mod_key,
        timeout, backend_remarks, multiple, cf, amount_memo='', Note='', increasethebet_switch=0, increasethebet='0', **kwargs):
        '''
            LEBO 充值
            需要有【現金系統 >> 存款與取款】权限
        '''
        return BaseFunc.deposit(url, DBId, Member, DepositAmount, mod_name, mod_key,
            timeout, backend_remarks, multiple, cf, amount_memo, Note, increasethebet_switch, increasethebet, **kwargs)

    @classmethod
    def _audit(cls, url, DBId, mod_name, mod_key, Member, SearchGameCategory, SearchDate, timeout, cf, RawWagersId, ExtendLimit, GameCountType, **kwargs):
        '''
            LEBO 旋转注单系统
            需要有【帐号管理 >> 输赢查询 / 帐号转换, 现金系统 >>第三方查询 / 层级管理 >> 会员查询】权限。支援「CQ9电子」(跳高高、跳高高2、跳起来) &「JDB电子」(变脸、飞鸟派对)。
        '''
        SupportStatus = kwargs.get('SupportStatus')
        SearchDate = SearchDate.replace('/', '-')
        global gameList

        result = {
                'Action': 'chkbbin',
                'IsSuccess': 0,
                'ErrorCode': '',
                'ErrorMessage': '',
                'DBId': DBId,
                'RawWagersId': RawWagersId,
                'Member': Member,
                'BlockMemberLayer': '',
                'GameName': '',
                'WagersTimeString': '',
                'WagersTimeStamp': '',
                'BetAmount': '0.00',
                'AllCategoryCommissionable': '0.00',
                'GameCommissionable': '0.00',
                'SingleCategoryCommissionable': '0.00',
                'CategoryName': '',
                'FreeSpin': 0,
                'ExtendLimit': ExtendLimit,
                'GameCountType': GameCountType
                }


        #★取得【會員層級】
        result_level = lebo.app_cash_utotal(cf, url, params={
            "username": Member,
            "savebtn": "查詢"
        })
        if SupportStatus:
            if not result_level["IsSuccess"]:
                Detail = f"查询失败\n搜寻帐号：{Member}\n{result_level['ErrorMessage']}"
            else:
                Detail = f"查询成功\n搜寻帐号：{Member}\n会员是否存在：{'是' if Member in result_level['Data'] else '否'}"
                if Member in result_level['Data']:
                    Detail += f"\n会员层级：{result_level['Data'][Member]['所屬層級']}"

            cls.return_schedule(Action='chkbbin', DBId=DBId, Status='取得【会员层级】', Progress='1/1', Detail=Detail)
        else:
            cls.return_schedule(Action='chkbbin', DBId=DBId, Member=Member, current_status='取得【会员层级】')
        # 連線異常, 設定connect為False
        if result_level["ErrorCode"] == config['CONNECTION_CODE']['code']:
            #return result_level["ErrorMessage"] + '，请检查网路后再次启动机器人'
            return result_level  #●新增
        if result_level["ErrorCode"] == config['SIGN_OUT_CODE']['code']:
            return result_level

        # 查詢失敗直接回傳
        if result_level['IsSuccess'] is False:
            result.update({
                         'IsSuccess': int(result_level['IsSuccess']),
                         'ErrorCode': result_level['ErrorCode'],
                         'ErrorMessage': result_level['ErrorMessage']
                         })
            return result

        elif result_level['ErrorMessage']:
            result.update({'ErrorMessage': result_level['ErrorMessage']})
            return result

        else:
            result.update({'BlockMemberLayer': result_level['Data'][Member]['所屬層級']})

        if result_level["ErrorCode"] != config['SUCCESS_CODE']['code']:
            return result_level["ErrorMessage"]

        #★取得【注單內容】
        result_wagers = lebo.app_mgame_bbin(cf, url, mod_name, data={
            "account": Member,
            "no": RawWagersId,
            "searchbtn": "查詢",
            })
        if SupportStatus:
            if not result_wagers["IsSuccess"]:
                Detail = f"查询失败\n搜寻注单：{RawWagersId}\n{result_wagers['ErrorMessage']}"
            else:
                Detail = f"查询成功\n搜寻注单：{RawWagersId}\n是否为免费旋转：{'是' if result_wagers['Data']['FreeSpin'] else '否'}"

            cls.return_schedule(Action='chkbbin', DBId=DBId, Status='取得【注单内容】', Progress='1/1', Detail=Detail)
        else:
            cls.return_schedule(Action='chkbbin', DBId=DBId, Member=Member, current_status='取得【注单内容】')
        # 連線異常, 設定connect為False
        if result_wagers["ErrorCode"] == config['CONNECTION_CODE']['code']:
            return result_wagers
        if result_wagers["ErrorCode"] == config['SIGN_OUT_CODE']['code']:
            return result_wagers

        # 查詢失敗直接回傳
        if result_wagers['IsSuccess'] is False:
            result.update({
                         'IsSuccess': int(result_wagers['IsSuccess']),
                         'ErrorCode': result_wagers['ErrorCode'],
                         'ErrorMessage': result_wagers['ErrorMessage']
                         })
            return result

        elif result_wagers['ErrorMessage']:
            result.update({'ErrorMessage': result_wagers['ErrorMessage']})
            return result

        if result_wagers["ErrorCode"] != config['SUCCESS_CODE']['code']:
            return result_wagers["ErrorMessage"]

        txt = result_wagers['Data']['注單']['下注资讯']
        #每注:5(元),共:1注\n总共:5(元)
        a = txt.rfind(':')
        b = txt.rfind('(元)')
        num = txt[(a + 1):b]

        dt = datetime.datetime.strptime(f"{result_wagers['Data']['注單']['时间']} -0400", "%Y-%m-%d %H:%M:%S %z")
        WagersTimeString = dt.strftime(r'%Y/%m/%d %H:%M:%S %z')
        WagersTimeStamp = str(int(dt.timestamp())*1000)

        n = str(num).replace(',', '')
        result.update({'CategoryName': result_wagers['Data']['CategoryName'],
                       'GameName': result_wagers['Data']['注單']['游戏类别'],
                       'WagersTimeString': WagersTimeString,
                       'WagersTimeStamp': WagersTimeStamp,
                       'BetAmount': f'{abs(eval(n)):.2f}',
                       'FreeSpin': result_wagers['Data']['FreeSpin']})


        if ExtendLimit == '1' or ExtendLimit == 1:

            if GameCountType == '0' or GameCountType == 0:
                try:
                    if not gameList:
                        r = getGameList()
                        if r != False:
                            gameList = r

                    if gameList:
                        if set(SearchGameCategory) - set(gameList):
                            # return config['CATEGORY_NOT_SUPPORT']['msg']
                            return '< 分类配置 > 错误！请到后台「网站配置」→「分类配置」执行LEBO【分类更新】。'
                except:
                    pass

                #★取得【分類當日投注】
                result_amount = lebo.member_report_result(cf, url, params={
                    'date_start': f'{SearchDate} 00:00:00',
                    'date_end': f'{SearchDate} 23:59:59',
                    'account': Member,
                    'game[]': SearchGameCategory,
                    'model': '1',
                    'searchbtn': '查詢'}
                    )
                if SupportStatus:
                    cls.return_schedule(Action='chkbbin', DBId=DBId, Status='取得【分类当日投注】', Progress='1/1', Detail=f"查询{'成功' if result_amount.get('IsSuccess') else '失败'}")
                else:
                    cls.return_schedule(Action='chkbbin', DBId=DBId, Member=Member, current_status='取得【分类当日投注】')
                # 連線異常, 設定connect為False
                if result_amount["ErrorCode"] == config['CONNECTION_CODE']['code']:
                    return result_amount
                if result_amount["ErrorCode"] == config['SIGN_OUT_CODE']['code']:
                    return result_amount
                if result_amount["ErrorCode"] != config['SUCCESS_CODE']['code']:
                    return result_amount["ErrorMessage"]

                # 查詢投注金額失敗直接回傳
                if result_amount['IsSuccess'] is False:

                    result.update({
                                'IsSuccess': int(result_amount['IsSuccess']),
                                'ErrorCode': result_amount['ErrorCode'],
                                'ErrorMessage': result_amount['ErrorMessage']
                                })

                    return result

                elif result_amount['ErrorMessage']:
                    result.update({'ErrorMessage': result_amount['ErrorMessage']})
                    return result

                else:
                    if result_amount['Data']['總計：']['類型'] == '總計：':
                        n = str(result_amount['Data']['總計：']['有效投注']).replace(',', '')
                        result.update({'AllCategoryCommissionable': f'{eval(n):.2f}'})



            elif GameCountType == '1' or GameCountType == 1:
                try:
                    if not gameList:
                        r = getGameList()
                        if r != False:
                            gameList = r

                    if gameList:
                        if set(SearchGameCategory) - set(gameList):
                            # return config['CATEGORY_NOT_SUPPORT']['msg']
                            return '< 分类配置 > 错误！请到后台「网站配置」→「分类配置」执行LEBO【分类更新】。'
                except:
                    pass

                #★取得【本遊戲當日投注】
                result_amount = lebo.bet_record_v2_index(cf, Member, SearchGameCategory, url, data={
                    'betno': RawWagersId,
                    'date_start': f'{SearchDate} 00:00:00',
                    'date_end': f'{SearchDate} 23:59:59'}
                    )
                if SupportStatus:
                    cls.return_schedule(Action='chkbbin', DBId=DBId, Status='取得【本游戏当日投注】', Progress='1/1', Detail=f"查询{'成功' if result_amount.get('IsSuccess') else '失败'}")
                else:
                    cls.return_schedule(Action='chkbbin', DBId=DBId, Member=Member, current_status='取得【本游戏当日投注】')
                # 連線異常, 設定connect為False
                if result_amount["ErrorCode"] == config['CONNECTION_CODE']['code']:
                    return result_amount
                if result_amount["ErrorCode"] == config['SIGN_OUT_CODE']['code']:
                    return result_amount
                if result_amount["ErrorCode"] != config['SUCCESS_CODE']['code']:
                    return result_amount["ErrorMessage"]

                # 查詢投注金額失敗直接回傳
                if result_amount['IsSuccess'] is False:

                    result.update({
                                'IsSuccess': int(result_amount['IsSuccess']),
                                'ErrorCode': result_amount['ErrorCode'],
                                'ErrorMessage': result_amount['ErrorMessage']
                                })

                    return result

                elif result_amount['ErrorMessage']:
                    result.update({'ErrorMessage': result_amount['ErrorMessage']})
                    return result

                else:
                    if result_amount['Data']['GameCommissionable'] != '':
                        n = str(result_amount['Data']['GameCommissionable']).replace(',', '')
                        result.update({'GameCommissionable': f'{eval(n):.2f}'})



            elif GameCountType == '2' or GameCountType == 2:
                params = {
                    'date_start': f'{SearchDate} 00:00:00',
                    'date_end': f'{SearchDate} 23:59:59',
                    'account': Member,
                    'game[]': '',
                    'model': '1',
                    'searchbtn': '查詢'
                    }

                if result['CategoryName'] == 'BBIN电子':
                    params['game[]'] = ['bbdz']
                elif result['CategoryName'] == 'CQ9电子':
                    params['game[]'] = ['cq']
                elif result['CategoryName'] == 'JDB电子':
                    params['game[]'] = ['jdb']

                #★取得【本分類當日投注】
                result_amount = lebo.member_report_result(cf, url, params=params)
                if SupportStatus:
                    cls.return_schedule(Action='chkbbin', DBId=DBId, Status='取得【本分类当日投注】', Progress='1/1', Detail=f"查询{'成功' if result_amount.get('IsSuccess') else '失败'}")
                else:
                    cls.return_schedule(Action='chkbbin', DBId=DBId, Member=Member, current_status='取得【本分类当日投注】')
                # 連線異常, 設定connect為False
                if result_amount["ErrorCode"] == config['CONNECTION_CODE']['code']:
                    return result_amount
                if result_amount["ErrorCode"] == config['SIGN_OUT_CODE']['code']:
                    return result_amount
                if result_amount["ErrorCode"] != config['SUCCESS_CODE']['code']:
                    return result_amount["ErrorMessage"]

                # 查詢投注金額失敗直接回傳
                if result_amount['IsSuccess'] is False:

                    result.update({
                                'IsSuccess': int(result_amount['IsSuccess']),
                                'ErrorCode': result_amount['ErrorCode'],
                                'ErrorMessage': result_amount['ErrorMessage']
                                })

                    return result

                elif result_amount['ErrorMessage']:
                    result.update({'ErrorMessage': result_amount['ErrorMessage']})
                    return result

                else:
                    if result_amount['Data']['總計：']['類型'] == '總計：':
                        n = str(result_amount['Data']['總計：']['有效投注']).replace(',', '')
                        result.update({'SingleCategoryCommissionable': f'{eval(n):.2f}'})



        result.update({'IsSuccess': 1})
        return result


class experiencebonus(BaseFunc):
    '''LEBO 體驗金'''

    @classmethod
    def deposit(cls, *args, **kwargs):
        '''
            LEBO 充值、移动层级
            需要有【現金系統 >> 存款與取款】权限。
            需要有【現金系統 >> 层级管理 >> 会员查询 >> 移动层级】权限。
        '''
        cls.th = ThreadProgress(kwargs['cf'], kwargs['mod_key'])
        cls.th.start()

        result = cls._deposit(*args, **kwargs)

        cls.return_schedule(Action='chkpoint', DBId=kwargs['DBId'], Status='【充值机器人处理完毕】', Progress='-', Detail='-')
        cls.th.stop()
        return result

    @classmethod
    def audit(cls, *args, **kwargs):
        '''
            LEBO 体验金
            需要有【帳號管理 >> 會員管理】权限。
            需要有【帳號管理 >> 會員管理 >> 資料】权限。
            需要有【其他 >> 登入日志】权限。
        '''
        cls.th = ThreadProgress(kwargs['cf'], kwargs['mod_key'])
        cls.th.start()

        result = cls._audit(*args, **kwargs)

        cls.return_schedule(Action='chkbbin', DBId=kwargs['DBId'], Status='【审核机器人处理完毕】', Progress='-', Detail='-')
        cls.th.stop()
        return result

    @classmethod
    def _deposit(cls, url, DBId, Member, DepositAmount, ChangeLayer, BlockMemberLayer,
        mod_name, mod_key, timeout, backend_remarks, multiple, cf, amount_memo='', Note='', increasethebet_switch=0, increasethebet='0', **kwargs):
        '''LEBO 體驗金 充值'''
        member = Member.lower()
        # 【充值】
        result_deposit = super().deposit(url, DBId, Member, DepositAmount, mod_name, mod_key,
            timeout, backend_remarks, multiple, cf, amount_memo, Note, increasethebet_switch, increasethebet, **kwargs)
        if isinstance(result_deposit, str):
            return result_deposit
        elif not result_deposit['IsSuccess']:
            return {**result_deposit, 'BlockMemberLayer': '-', 'LayerMessage': ''}
        if not ChangeLayer:
            return {**result_deposit, 'BlockMemberLayer': '-', 'LayerMessage': ''}

        # 【移動層級】 - 【查詢層級列表及必要參數】
        count = 1
        while True:
            result_level = lebo.app_cash_utotal(cf, url, params={
                                                        "username": member,
                                                        "savebtn": "查詢"
                                                    }, mod_name=mod_name)

            # 整理查詢結果
            game_dict = result_level['Data']['game_dict'] if result_level['IsSuccess'] else {}
            if not game_dict:
                return {
                        'IsSuccess': 0,
                        'ErrorCode': config.HTML_CONTENT_CODE.code,
                        'ErrorMessage': config.HTML_CONTENT_CODE.msg.format(platform=cf.platform),
                        'DBId': DBId,
                        'Member': Member,
                        'BlockMemberLayer': '-',
                        'LayerMessage': config.HTML_CONTENT_CODE.msg.format(platform=cf.platform)
                        }
            # 更新【查詢層級列表及必要參數】的結果
            if not result_level["IsSuccess"] and result_level['ErrorCode'] != config.NO_USER.code:
                Detail = f'查询失败\n查询层级：{BlockMemberLayer}\n{result_level["ErrorMessage"]}'
            else:
                Detail = f'查询成功\n查询层级：{BlockMemberLayer}\n会员是否存在：{"否" if result_level["ErrorCode"] == config.NO_USER.code else "是"}\n层级是否存在：{"是" if BlockMemberLayer in list(game_dict.keys()) else "否"}'
            cls.return_schedule(Action='chkpoint', DBId=DBId, Status='【現金系統 - 【层级管理】 - 【会员查询】', Progress=f'{count}/{count}', Detail=Detail)

            if result_level["ErrorCode"] == config.CONNECTION_CODE.code:
                time.sleep(1)
                continue

            # 檢查登出或連線異常, 回傳後讓主程式重試
            if result_level["ErrorCode"] == config.SIGN_OUT_CODE.code:
                return {
                        'IsSuccess': 1,
                        'ErrorCode': config.LayerError.code,
                        'ErrorMessage': config.LayerError.msg.format(platform=cf.platform, msg='查询层级列表被登出，无法移动层级'),
                        'DBId': DBId,
                        'Member': Member,
                        'BlockMemberLayer': '-',
                        'LayerMessage': '查询层级列表被登出，无法移动层级'
                    }
            if result_level['ErrorCode'] in (config.PERMISSION_CODE.code, config.HTML_STATUS_CODE.code):
                return result_level['ErrorMessage']

            if result_level['IsSuccess'] is False and result_level['ErrorCode'] == config.SUCCESS_CODE.code:
                return {
                        'IsSuccess': int(result_level['IsSuccess']),
                        'ErrorCode': result_level['ErrorCode'],
                        'ErrorMessage': result_level['ErrorMessage'],
                        'DBId': DBId,
                        'Member': Member,
                        'BlockMemberLayer': '-',
                        'LayerMessage': result_level['ErrorMessage']
                        }
            if result_level['ErrorCode'] in (config.HTML_CONTENT_CODE.code, config.NO_USER.code):
                return {
                        'IsSuccess': int(result_level['IsSuccess']),
                        'ErrorCode': result_level['ErrorCode'],
                        'ErrorMessage': result_level['ErrorMessage'],
                        'DBId': DBId,
                        'Member': Member,
                        'BlockMemberLayer': '-',
                        'LayerMessage': result_level['ErrorMessage']
                        }
            if result_level['ErrorCode'] != config.SUCCESS_CODE.code:
                return result_level['ErrorMessage']

            if BlockMemberLayer not in game_dict.keys():
                return {
                    'IsSuccess': 1,
                    'ErrorCode': config.LayerError.code,
                    'ErrorMessage': config.LayerError.msg.format(platform=cf.platform, msg=f'无【{BlockMemberLayer}】层级'),
                    'DBId': DBId,
                    'Member': Member,
                    'BlockMemberLayer': '-',
                    'LayerMessage': f'无【{BlockMemberLayer}】层级'
                }
            if (result_level['Data'][member]['level_name'] is None or
                result_level['Data'][member]['default_value'] is None or
                result_level['Data'][member]['default_name'] is None or
                result_level['Data'][member]['lock_name'] is None):
                return {
                        'IsSuccess': 0,
                        'ErrorCode': config.HTML_CONTENT_CODE.code,
                        'ErrorMessage': config.HTML_CONTENT_CODE.msg.format(platform=cf.platform),
                        'DBId': DBId,
                        'Member': Member,
                        'BlockMemberLayer': '-',
                        'LayerMessage': config.HTML_CONTENT_CODE.msg.format(platform=cf.platform)
                        }

            # 【移動層級】
            level_name = result_level['Data'][member]['level_name'] #需要移動的層級名稱
            default_value = result_level['Data'][member]['default_value'] #原來的層級名稱
            default_name = result_level['Data'][member]['default_name'] #原來層級名稱的KEY
            lock_name = result_level['Data'][member]['lock_name'] #是否要鎖定帳號 #看原帳號有鎖定就需要加入
            data = {
                    'hidnSelectFlag': '1',
                    level_name: game_dict[BlockMemberLayer],
                    default_name: default_value
                    }
            if default_value.split(',')[1] == '1':
                data[lock_name] = default_value.split(',')[1]

            update_level = lebo.app_cash_utotal(cf, url, params={
                                                            "username": member,
                                                            "savebtn": "查詢"
                                                            }, data=data, method='post', mod_name=mod_name)
            if not update_level["IsSuccess"]:
                Detail = f'层级移动失败\n{update_level["ErrorMessage"]}'
            else:
                Detail = f'层级移动成功'
            cls.return_schedule(Action='chkpoint', DBId=DBId, Status='【現金系統 - 【层级管理】 - 【会员查询】 - 【层级移动】', Progress=f'{count}/{count}', Detail=Detail)
            # 檢查登出或連線異常, 回傳後讓主程式重試
            if update_level["ErrorCode"] == config.CONNECTION_CODE.code:
                time.sleep(1)
                continue

            if update_level["ErrorCode"] == config.SIGN_OUT_CODE.code:
                return {
                        'IsSuccess': 1,
                        'ErrorCode': config.LayerError.code,
                        'ErrorMessage': config.LayerError.msg.format(platform=cf.platform, msg='查询层级列表被登出，无法移动层级'),
                        'DBId': DBId,
                        'Member': Member,
                        'BlockMemberLayer': '-',
                        'LayerMessage': '查询层级列表被登出，无法移动层级'
                    }

            if update_level['ErrorCode'] in (config.PERMISSION_CODE.code, config.HTML_STATUS_CODE.code):
                return update_level['ErrorMessage']

            if update_level['IsSuccess'] is False and update_level['ErrorCode'] == config.SUCCESS_CODE.code:
                return {
                        'IsSuccess': 1,
                        'ErrorCode': update_level['ErrorCode'],
                        'ErrorMessage': update_level['ErrorMessage'],
                        'DBId': DBId,
                        'Member': Member,
                        'BlockMemberLayer': '-',
                        'LayerMessage': update_level['ErrorMessage']
                        }
            if update_level['ErrorCode'] != config.SUCCESS_CODE.code:
                return {
                        'IsSuccess': 1,
                        'ErrorCode': update_level['ErrorCode'],
                        'ErrorMessage': update_level['ErrorMessage'],
                        'DBId': DBId,
                        'Member': Member,
                        'BlockMemberLayer': '-',
                        'LayerMessage': update_level['ErrorMessage']
                        }

            return {
                'IsSuccess': 1,
                'ErrorCode': config.SUCCESS_CODE.code,
                'ErrorMessage': config.SUCCESS_CODE.msg,
                'DBId': DBId,
                'Member': Member,
                'BlockMemberLayer': BlockMemberLayer,
                'LayerMessage': ''
            }

    @classmethod
    def _audit(cls, url, DBId, Member, SearchDate, AuditDays, timeout, cf, **kwargs):
        '''LEBO 體驗金 監控'''
        member = Member.lower()
        if len(member) < 4 or len(member) > 16:
            return {
                'IsSuccess': 0,
                'ErrorCode': config.ACCOUNT_COUNT_ERROR.code,
                'ErrorMessage': config.ACCOUNT_COUNT_ERROR.msg.format(platform=cf.platform),
                'DBId': DBId,
                'Member': Member,
                'BlockMemberLayer': '',
                'RealName': '',
                'BankAccountExists': 0,
                'AutoAudit': 0,
                'RegisterTimeString': '',
                'RegisterTimeStamp': '',
            }
        # 查詢會員註冊時間&會員層級
        result_member = lebo.members(cf, url, data={
                                                'enable': 'Y',
                                                'onlinestatus': 'all',
                                                'sort': 'adddate',
                                                'orderby': 'desc',
                                                'page_num': '1000',
                                                'page': '0',
                                                'startdate': '',
                                                'enddate': '',
                                                'group': '0',
                                                'utype': '0',
                                                'uname': member}, timeout=timeout)
        if result_member['IsSuccess']:
            BlockMemberLayer = result_member['Data']['層級']
            create_time = result_member['Data']['新增日期']
            userid = result_member['Data']['usernid']
        if not result_member["IsSuccess"] and result_member['ErrorCode'] != config.NO_USER.code:
            Detail = f'查询失败\n搜寻帐号：{Member}\n{result_member["ErrorMessage"]}'
        else:
            Detail = f'查询成功\n搜寻帐号：{Member}\n会员是否存在：{"否" if result_member["ErrorCode"] == config.NO_USER.code else "是"}'
            if result_member['IsSuccess']:
                Detail += f'\n会员注册时间：{create_time}'
                Detail += f'\n会员层级：{BlockMemberLayer}'
        cls.return_schedule(Action='chkbbin', DBId=DBId, Status='【帐号管理】 - 【会员管理】', Progress=f'1/1', Detail=Detail)

        if result_member['ErrorCode'] in (config.PERMISSION_CODE.code, config.HTML_STATUS_CODE.code):
            return result_member['ErrorMessage']
        if result_member["ErrorCode"] in (config.CONNECTION_CODE.code,config.SIGN_OUT_CODE.code):
            return result_member
        if result_member['ErrorCode'] in (config.HTML_CONTENT_CODE.code, config.NO_USER.code):
            return {
                    'IsSuccess': int(result_member['IsSuccess']),
                    'ErrorCode': result_member['ErrorCode'],
                    'ErrorMessage': result_member['ErrorMessage'],
                    'DBId': DBId,
                    'Member': Member,
                    'BlockMemberLayer': '',
                    'RealName': '',
                    'BankAccountExists': 0,
                    'AutoAudit': 0,
                    'RegisterTimeString': '',
                    'RegisterTimeStamp': '',
                    }
        if result_member['ErrorCode'] != config.SUCCESS_CODE.code:
            return result_member['ErrorMessage']
        create_time = datetime.datetime.strptime(f'{create_time} -0400', r'%Y-%m-%d %H:%M:%S %z')

        # 查詢銀行卡&真實姓名
        result_member_data = lebo.member_data(cf, url, data={
                                                            'usernid': userid
                                                            },member=member, timeout=timeout)

        real_name = result_member_data["Data"]["base_data"].get('真實姓名', '') if result_member_data["IsSuccess"] else ''
        bank_account_exists = int(bool(result_member_data["Data"]["bank_data"] if result_member_data["IsSuccess"] else ''))

        if not result_member_data["IsSuccess"]:
            Detail = f'查询失败\n{result_member_data["ErrorMessage"]}'
        else:
            Detail = f'查询成功\n真实姓名为：{real_name}\n是否绑定银行卡：{"是" if bank_account_exists else "否"}'
        cls.return_schedule(Action='chkbbin', DBId=DBId, Status='【帐号管理】 - 【会员管理】 - 【资料】', Progress=f'1/1', Detail=Detail)

        if result_member_data['ErrorCode'] in (config.PERMISSION_CODE.code, config.HTML_STATUS_CODE.code):
            return result_member['ErrorMessage']
        if result_member_data["ErrorCode"] in (config.CONNECTION_CODE.code,config.SIGN_OUT_CODE.code):
            return result_member_data
        if result_member_data['ErrorCode'] == config.HTML_CONTENT_CODE.code:
            return {
                    'IsSuccess': int(result_member_data['IsSuccess']),
                    'ErrorCode': result_member_data['ErrorCode'],
                    'ErrorMessage': result_member_data['ErrorMessage'],
                    'DBId': DBId,
                    'Member': Member,
                    'BlockMemberLayer': '',
                    'RealName': '',
                    'BankAccountExists': 0,
                    'AutoAudit': 0,
                    'RegisterTimeString': '',
                    'RegisterTimeStamp': '',
                    }
        if result_member_data['ErrorCode'] != config.SUCCESS_CODE.code:
            return result_member_data['ErrorMessage']

        # 稽核查詢特定天數內是否有同IP多帳號的情形
        EndDate = datetime.datetime.strptime(SearchDate, r'%Y/%m/%d')
        StartDate = EndDate - datetime.timedelta(days=int(AuditDays))
        EndDate = EndDate.strftime(r'%Y-%m-%d')
        StartDate = StartDate.strftime(r'%Y-%m-%d')
        total_page = 1
        content = []
        data = {
                'date_start': StartDate,
                'date_end': EndDate,
                'username': member,
                'ip': '',
                'page_num': '1000',
                'page': 1
                }
        while total_page>=data['page']:
            result_login_log = lebo.login_log(cf, url, data=data, timeout=timeout)
            content += result_login_log['Data'].get('content', []) if result_login_log['IsSuccess'] else []
            auto_audit = int(len(set([data['帐号'] for data in content])) > 1)
            if not result_login_log["IsSuccess"]:
                Detail = f'查询失败\n{result_login_log["ErrorMessage"]}'
            else:
                Detail = f'查询成功\n查询到的帐号列表：{list(set(data["帐号"] for data in content))}\n查询到的IP列表：{list(set(data["IP位置"] for data in content))}'
                Detail += f'\n是否有多帳號同IP：{"是" if auto_audit else "否"}'
            cls.return_schedule(Action='chkbbin', DBId=DBId, Status='【其他】 - 【登入日志】 - 【自动稽核】', Progress=f'{data["page"]}/{total_page}', Detail=Detail)

            if result_login_log['ErrorCode'] in (config.PERMISSION_CODE.code, config.HTML_STATUS_CODE.code):
                return result_member['ErrorMessage']
            if result_login_log["ErrorCode"] in (config.CONNECTION_CODE.code,config.SIGN_OUT_CODE.code):
                return result_login_log
            if result_login_log['ErrorCode'] == config.HTML_CONTENT_CODE.code:
                return {
                        'IsSuccess': int(result_login_log['IsSuccess']),
                        'ErrorCode': result_login_log['ErrorCode'],
                        'ErrorMessage': result_login_log['ErrorMessage'],
                        'DBId': DBId,
                        'Member': Member,
                        'BlockMemberLayer': '',
                        'RealName': '',
                        'BankAccountExists': 0,
                        'AutoAudit': 0,
                        'RegisterTimeString': '',
                        'RegisterTimeStamp': '',
                        }
            if result_login_log['ErrorCode'] != config.SUCCESS_CODE.code:
                return result_login_log['ErrorMessage']

            total_page = result_login_log['Data'].get('total_page', 1)
            data['page'] += 1

        # 回傳結果
        return {
            'IsSuccess': 1,
            'ErrorCode': config.SUCCESS_CODE.code,
            'ErrorMessage': config.SUCCESS_CODE.msg,
            'DBId': DBId,
            'Member': Member,
            'BlockMemberLayer': BlockMemberLayer,
            'RealName': real_name,
            'BankAccountExists': bank_account_exists,
            'AutoAudit': auto_audit,
            'RegisterTimeString': create_time.strftime(r'%Y/%m/%d %H:%M:%S %z'),
            'RegisterTimeStamp': f'{int(create_time.timestamp()) * 1000}',
        }

class _bettingbonus(BaseFunc):
    '''LEBO 注注有獎'''
    class Meta:
        suport = bettingbonus_support
        extra = {
            # # 需要登入第二個平台使用
            # 'platform': {
            #     'info': '平台',
            #     'var': list,
            #     'choise': ['GPK', 'LEBO', 'BBIN'],
            #     'default': 'GPK',
            # },
            # 'url': {
            #     'info': '網址',
            #     'var': str,
            #     'default': '',
            # },
            # 'username': {
            #     'info': '帳號',
            #     'var': str,
            #     'default': '',
            # },
            # 'password': {
            #     'info': '密碼',
            #     'var': str,
            #     'default': '',
            # },
        }
    @classmethod
    def deposit(cls, *args, **kwargs):
        '''
            LEBO 充值、移动层级
            需要有【現金系統 >> 存款與取款】权限。
            需要有【現金系統 >> 层级管理 >> 会员查询 >> 移动层级】权限。
        '''
        cls.th = ThreadProgress(kwargs['cf'], kwargs['mod_key'])
        cls.th.start()

        result = cls._deposit(*args, **kwargs)

        cls.return_schedule(Action='chkpoint', DBId=kwargs['DBId'], Status='【充值机器人处理完毕】', Progress='-', Detail='-')
        cls.th.stop()
        return result

    @classmethod
    def audit(cls, *args, **kwargs):
        '''LEBO 注注有獎
            需要有【帳號管理 >> 會員管理】权限。
            需要有【帳號管理 >> 會員管理 >> 資料】权限。
            需要有【其他 >> 登入日志】权限。
        '''
        cls.th = ThreadProgress(kwargs['cf'], kwargs['mod_key'])
        cls.th.start()

        result = cls._audit(*args, **kwargs)

        cls.return_schedule(Action='chkbbin', DBId=kwargs['DBId'], Status='【审核机器人处理完毕】', Progress='-', Detail='-')
        cls.th.stop()
        return result

    @classmethod
    def _deposit(cls, url, DBId, Member, DepositAmount, mod_name, mod_key, timeout, backend_remarks, multiple, cf, amount_memo='',Note='', increasethebet_switch=0, increasethebet='0', **kwargs):
        '''LEBO 注注有獎 充值'''
        # 【充值】
        return super().deposit(url, DBId, Member, DepositAmount, mod_name, mod_key,
                                timeout, backend_remarks, multiple, cf, amount_memo, Note, increasethebet_switch, increasethebet, **kwargs)

    @classmethod
    def _audit(cls, url, DBId, mod_name, mod_key, Member, SearchGameCategory, SearchDate, AmountAbove, timeout, cf, **kwargs):
        member = Member.lower()
        if len(member) < 4 or len(member) > 16:
            return {
                'IsSuccess': 0,
                'ErrorCode': config.ACCOUNT_COUNT_ERROR.code,
                'ErrorMessage': config.ACCOUNT_COUNT_ERROR.msg.format(platform=cf.platform),
                'DBId': DBId,
                'Member': Member,
                'BlockMemberLayer': '',
                'BetList':[]
            }
        # 列出可查遊戲
        keys = set(bettingbonus_support.keys())
        support_GameCategory = keys & set(SearchGameCategory)
        if not support_GameCategory:
            return {
                'IsSuccess': 0,
                'ErrorCode': config.CATEGORY_NOT_SUPPORT.code,
                'ErrorMessage': config.CATEGORY_NOT_SUPPORT.msg.format(supported=','.join(bettingbonus_support.values())),
                'DBId': DBId,
                'Member': Member,
                'BlockMemberLayer': '',
                'BetList':[]
            }
        # 查詢會員層級
        result_member = lebo.members(cf, url, data={
                                                'enable': 'Y',
                                                'onlinestatus': 'all',
                                                'sort': 'adddate',
                                                'orderby': 'desc',
                                                'page_num': '1000',
                                                'page': '0',
                                                'startdate': '',
                                                'enddate': '',
                                                'group': '0',
                                                'utype': '0',
                                                'uname': member}, timeout=timeout)
        if result_member['IsSuccess']:
            BlockMemberLayer = result_member['Data']['層級']
        if not result_member["IsSuccess"] and result_member['ErrorCode'] != config.NO_USER.code:
            Detail = f'查询失败\n搜寻帐号：{Member}\n{result_member["ErrorMessage"]}'
        else:
            Detail = f'查询成功\n搜寻帐号：{Member}\n会员是否存在：{"否" if result_member["ErrorCode"] == config.NO_USER.code else "是"}'
            if result_member['IsSuccess']:
                Detail += f'\n会员层级：{BlockMemberLayer}'
        cls.return_schedule(Action='chkbbin', DBId=DBId, Status='【帐号管理】 - 【会员管理】', Progress=f'1/1', Detail=Detail)

        if result_member['ErrorCode'] in (config.PERMISSION_CODE.code, config.HTML_STATUS_CODE.code):
            return result_member['ErrorMessage']
        if result_member["ErrorCode"] in (config.CONNECTION_CODE.code,config.SIGN_OUT_CODE.code):
            return result_member
        if result_member['ErrorCode'] in (config.HTML_CONTENT_CODE.code, config.NO_USER.code):
            return {
                    'IsSuccess': int(result_member['IsSuccess']),
                    'ErrorCode': result_member['ErrorCode'],
                    'ErrorMessage': result_member['ErrorMessage'],
                    'DBId': DBId,
                    'Member': Member,
                    'BlockMemberLayer': BlockMemberLayer,
                    'BetList':[]
                    }
        if result_member['ErrorCode'] != config.SUCCESS_CODE.code:
            return result_member['ErrorMessage']
        if '下注' not in result_member['Data']['功能']:
            return {
                    'IsSuccess': 0,
                    'ErrorCode': config.HTML_CONTENT_CODE.code,
                    'ErrorMessage': config.HTML_CONTENT_CODE.msg.format(platform=cf.platform),
                    'DBId': DBId,
                    'Member': Member,
                    'BlockMemberLayer': BlockMemberLayer,
                    'BetList':[]
                    }
        # 查詢注單列表
        content = []
        for GameCategory in support_GameCategory:
            total_page = 1
            data={
                'gametype': GameCategory,
                'orders': '0',
                'date_start': f"{SearchDate.replace('/', '-')} 00:00:00",
                'date_end': f"{SearchDate.replace('/', '-')} 23:59:59",
                'pagesize': '1000',
                'page': 1
                }
            while total_page >= data['page']:
                result_bet_record = lebo.bet_record_v2(cf, url, data=data,
                                                                endpoints = 'app/daili/adminsave/' + result_member['功能']['下注'],
                                                                method='post'
                                                        )
                if not result_bet_record['IsSuccess']:
                    Detail = f'查詢注單列表失败\n{result_bet_record["ErrorMessage"]}'
                else:
                    Detail = f'查詢注單列表成功'
                cls.return_schedule(Action='chkpoint', DBId=DBId, Status='【帳號管理】 - 【會員管理】 - 【下注】', Progress=f"{data['page']}/{total_page}", Detail=Detail)
                if result_bet_record['ErrorCode'] in (config.PERMISSION_CODE.code, config.HTML_STATUS_CODE.code):
                    return result_bet_record['ErrorMessage']
                if result_bet_record["ErrorCode"] in (config.CONNECTION_CODE.code,config.SIGN_OUT_CODE.code):
                    return result_bet_record
                if result_bet_record['ErrorCode'] in (config.HTML_CONTENT_CODE.code):
                    return {
                            'IsSuccess': int(result_bet_record['IsSuccess']),
                            'ErrorCode': result_bet_record['ErrorCode'],
                            'ErrorMessage': result_bet_record['ErrorMessage'],
                            'DBId': DBId,
                            'Member': Member,
                            'BlockMemberLayer': BlockMemberLayer,
                            'BetList':[]
                            }
                if result_bet_record['ErrorCode'] != config.SUCCESS_CODE.code:
                    return result_bet_record['ErrorMessage']
                content += result_bet_record['Data'].get('recode', {})
                total_page = result_bet_record.get('Data', 1).get('total_page', 1) #total_page 返回int型別
                data['page'] += 1

        if not content:
            {
                'IsSuccess': 1,
                'ErrorCode': config.SUCCESS_CODE.code,
                'ErrorMessage': config.SUCCESS_CODE.msg,
                'DBId': DBId,
                'Member': Member,
                'BlockMemberLayer': BlockMemberLayer,
                'BetList':[]
                }

        for bet in content:
            if '類別' not in bet or '有效投注' not in bet:
                return {
                        'IsSuccess': 0,
                        'ErrorCode': config.HTML_CONTENT_CODE.code,
                        'ErrorMessage': config.HTML_CONTENT_CODE.msg.format(platform=cf.platform),
                        'DBId': DBId,
                        'Member': Member,
                        'BlockMemberLayer': BlockMemberLayer,
                        'BetList':[]
                        }


        BetLists = [{'GameCategory':i['gamecategory'], 'GameName':i['類別'], 'BetAmount':i['有效投注']} for i in content if float(i['有效投注']) >= float(AmountAbove)]
        BetList =  [
                    {
                        'GameCategory':GameCategory,
                        'GameName':GameName,
                        'BetAmount':BetAmount,
                        'count':len(list(val))
                    }
                    for (GameCategory, GameName, BetAmount), val in groupby(
                        sorted(BetLists, key=lambda x: (x['GameCategory'], x['GameName'], x['BetAmount'])),
                        key=lambda x:(x['GameCategory'], x['GameName'], x['BetAmount'])
                    )
                    ]

        return {
                'IsSuccess': 1,
                'ErrorCode': config.SUCCESS_CODE.code,
                'ErrorMessage': config.SUCCESS_CODE.msg,
                'DBId': DBId,
                'Member': Member,
                'BlockMemberLayer': BlockMemberLayer,
                'BetList':BetList
                }


class apploginbonus(BaseFunc):
    '''LEBO 登入禮'''
    @classmethod
    @keep_connect
    def deposit(cls, *args, **kwargs):
        '''
            LEBO 充值、推播
            需要有【現金系統 >> 存款與取款】权限。
            需要有【其他 >> 會員消息】权限。
        '''
        cls.th = ThreadProgress(kwargs['cf'], kwargs['mod_key'])
        cls.th.start()

        result = cls._deposit(*args, **kwargs)

        cls.return_schedule(Action='chkpoint', DBId=kwargs['DBId'], Status='【充值机器人处理完毕】', Progress='-', Detail='-')
        cls.th.stop()
        return result

    @classmethod
    @keep_connect
    def _deposit(cls, url, DBId, Member, DepositAmount, NotifyAllowReply, NotifyTitle, NotifyContent,
                                                        mod_name, mod_key, timeout, backend_remarks, multiple, cf, amount_memo='',
                                                        Note='', increasethebet_switch=0, increasethebet='0', **kwargs):
        '''LEBO APP登入禮 充值'''
        # 【充值】
        result_deposit = super().deposit(url, DBId, Member, DepositAmount, mod_name, mod_key,
                                timeout, backend_remarks, multiple, cf, amount_memo, Note, increasethebet_switch, increasethebet, **kwargs)
        if isinstance(result_deposit, str):
            return result_deposit
        if not result_deposit['IsSuccess']:
            return {**result_deposit, 'NotifyMessage': ''}
        if not any([NotifyAllowReply, NotifyTitle, NotifyContent]):
            return {**result_deposit, 'NotifyMessage': ''}

        count = 1
        while True:
            # 【廣播通知】
            result_send = lebo.msg_add(cf=cf, url=url, data={
                                                            'wtype': '3',
                                                            'level': '765',
                                                            'to_user_account': Member,
                                                            'title': NotifyTitle,
                                                            'content': NotifyContent,
                                                            }, timeout=timeout)
            if not result_send["IsSuccess"]:
                Detail = f'發布推播失敗\n{result_send["ErrorMessage"]}'
            else:
                Detail = f'發布推播成功'
            cls.return_schedule(Action='chkpoint', DBId=DBId, Status='【推播通知】 - 【發布推播】', Progress=f'{count}/{count}', Detail=Detail)

            if result_send['ErrorCode'] in (config.PERMISSION_CODE.code, config.HTML_STATUS_CODE.code):
                return result_send['ErrorMessage']
            if result_send['ErrorCode'] == config.CONNECTION_CODE.code:
                time.sleep(1)
                continue
            if result_send["ErrorCode"] == config.SIGN_OUT_CODE.code:
                return {
                        **result_deposit,
                        'NotifyMessage': config.UserMessageError.msg.format(platform=cf.platform, msg='推播通知被登出，无法推播通知')
                        }
            if result_send['ErrorCode'] == config.HTML_CONTENT_CODE.code:
                return {**result_deposit, 'NotifyMessage': result_send['ErrorMessage']}
            if result_send['ErrorCode'] != config.SUCCESS_CODE.code:
                return {**result_deposit, 'NotifyMessage': result_send['ErrorMessage']}

            return {**result_deposit, 'NotifyMessage': ''}

    @classmethod
    @keep_connect
    def _audit(cls, url, timeout, cf, mod_key=None, AuditAPP=1, AuditUniversal=0,AuditMobilesite=0,AuditUniversalPc=0,AuditUniversalApp=0,AuditCustomizationApp=0, ImportColumns:list = ["Member", "BlockMemberLayer", "LoginDateTime","VipLevel"], **kwargs):
        '''
            LEBO 登入禮 監控
            需要有【現金系統 >> 層級管理 >> 會員查詢】权限。
            需要有【其他 >> 登入日志】权限。
        '''
        if int(AuditAPP) == 0:
            return {
                    'IsSuccess': 1,
                    'ErrorCode': config.SUCCESS_CODE.code,
                    'ErrorMessage': config.SUCCESS_CODE.msg,
                    'Data':[]
                    }
        # 設定時區
        tz = pytz.timezone('America/New_York')

        # 取得資料夾內日期最大的csv
        path = Path('./config/data') / (mod_key or '.')/'LEBO'
        if not path.exists():
            path.mkdir()
        csv_day = [i.stem for i in path.iterdir() if '.csv' == i.suffix]
        csv_day = max(csv_day) if csv_day else None
        today = datetime.datetime.now(tz=tz).strftime("%Y-%m-%d")

        # 設定搜尋時間
        if (today == csv_day) or (csv_day is None):
            date_start = date_end = datetime.datetime.now(tz=tz).strftime("%Y-%m-%d")
        else:
            date_start = datetime.datetime.now(tz=tz).strftime("%Y-%m-%d")
            date_end = datetime.datetime.now(tz=tz).strftime("%Y-%m-%d")

        # 找出最後一筆檔案
        last_date = None
        if csv_day:
            path = path / f'{csv_day}.csv'
            with path.open('r', encoding='utf-8') as f:
                data = f.read()
            # 將讀出檔案轉成陣列物件
            data_list = [i.split(',') for i in data.split('\n') if i]
            # 取出最大日期  範例值:['heedjkk', '第五層', '2021-09-16 01:54:45'] => '2021-09-16 01:54:45'
            last_date = max(filter(lambda x:bool(re.search(r'\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}', x[3])), data_list), key=lambda x:x[3])[3]
            # max(filter(lambda x:re.search(r'\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}', x), [i[-1] for i in data_list]))

        # 查搜APP登入資料
        ltype = [0,3] #查詢類別0=LEBOAPP, 3=簡易版APP
        result = []
        for tp in ltype:
            logger.info(f"查詢:{'LEBOAPP' if tp == 0 else '簡易版APP'}")
            logger.info(f"搜尋日期{date_start} - {date_end}")
            logger.info(f"csv最大日期:{last_date}")
            tp_result = []
            total_page = 1
            data = {
                'date_start': date_start,
                'date_end': date_end,
                'is_success': -1,
                'ltype':tp,
                'page_num': '1000',
                'page': 1
                }
            while total_page>=data['page']:
                result_login_log = lebo.login_log(cf=cf, url=url, data=data, timeout=timeout, endpoints='app/daili/adminsave/adminsys/login_log.php')
                if result_login_log['ErrorCode'] in (config.PERMISSION_CODE.code, config.HTML_STATUS_CODE.code):
                    return result_login_log['ErrorMessage']
                if result_login_log["ErrorCode"] in (config.CONNECTION_CODE.code,config.SIGN_OUT_CODE.code):
                    return result_login_log
                if result_login_log['ErrorCode'] == config.HTML_CONTENT_CODE.code:
                    return result_login_log['ErrorMessage']
                if result_login_log['ErrorCode'] != config.SUCCESS_CODE.code:
                    return result_login_log['ErrorMessage']
                # 總頁數
                total_page = result_login_log.get('Data', 1).get('total_page', 1) #total_page返回int型別
                logger.info(f"頁數/總頁數={data['page']}/{total_page}")
                # 拿出資料
                content = result_login_log.get('Data', None).get('content', None)
                # 查無資料就離開
                if not content:
                    logger.info('查無資料')
                    break
                tp_result += [[i['帐号'], i['登入时间']] for i in content if i['层级'] == '會員']
                # 查詢資料時間如果小於csv最後時間,就離開因為後續都是已查過的舊資料
                if last_date and min([i[-1] for i in tp_result]) < last_date:
                    logger.info('找到csv最後一筆資料時間,停止向後查詢')
                    # 取出大於csv最後時間的資料
                    tp_result = [i for i in tp_result if i[-1] > last_date]
                    break
                data['page'] += 1
            result += tp_result

        if not result:
            return {
                    'IsSuccess': 1,
                    'ErrorCode': config.SUCCESS_CODE.code,
                    'ErrorMessage': config.SUCCESS_CODE.msg,
                    'Data':[]
                    }
        # 檢查會員字串字元數
        users = [i[0] for i in result]
        member_level = {}
        CumulativeDepositAmount = {}
        CumulativeDepositsTimes = {}
        # 切割總會員數的字元長度(lebo 字元限制)
        for n in split_by_len(users):
            # 查詢會員層級
            username = ','.join(set(n))
            result_level = lebo.app_cash_utotal(
                                                    cf,
                                                    url,
                                                    params={
                                                            "username": username,
                                                            "savebtn": "查詢"
                                                            },
                                                )

            if result_level["ErrorCode"] == (config.CONNECTION_CODE.code, config.SIGN_OUT_CODE.code):
                return result_level
            if result_level['ErrorCode'] in (config.PERMISSION_CODE.code, config.HTML_STATUS_CODE.code):
                return result_level['ErrorMessage']
            if result_level['IsSuccess'] is False and result_level['ErrorCode'] == config.SUCCESS_CODE.code:
                return result_level['ErrorMessage']
            if result_level['ErrorCode'] == config.HTML_CONTENT_CODE.code:
                return result_level['ErrorMessage']
            if result_level['ErrorCode'] == config.NO_USER.code:
                continue
            if result_level['ErrorCode'] != config.SUCCESS_CODE.code:
                return result_level['ErrorMessage']
            member_level.update({i:result_level['Data'][i]['所屬層級'] for i in result_level['Data'] if i not in ['game_dict','RawContent']})
            CumulativeDepositAmount.update({i:str(float(result_level['Data'][i].get('存款總額', 0))) for i in result_level['Data'] if i not in ['game_dict','RawContent']})
            CumulativeDepositsTimes.update({i:str(int(result_level['Data'][i].get('存款次數', 0))) for i in result_level['Data'] if i not in ['game_dict','RawContent']})
        # 補上層級
        result = [[i[0], member_level.get(i[0], ''), '-',i[1], CumulativeDepositAmount.get(i[0],'0.0'), CumulativeDepositsTimes.get(i[0],'0')] for i in result]
        # 有會員查不到層級
        for i, (acc, lv,viplevel, date, total_amounts, total_counts) in enumerate(result):
            if lv:
                continue
            #★查詢【帳號層級】
            logger.info(f"會員:{acc}, 需要單獨另查層級")
            user_result = lebo.cash_cash_operation(cf, url, source={
                "username": acc,
                "search": "search",
            })
            # 查詢不到會員直接回傳
            if user_result['ErrorCode'] == config['NO_USER']['code']:
                logger.info(f"會員:{acc}，仍無法查詢到層級，即將刪除")
                continue
            # 連線異常、被登出，重試
            if user_result["ErrorCode"] == config['CONNECTION_CODE']['code']:
                return user_result
            if user_result["ErrorCode"] == config['SIGN_OUT_CODE']['code']:
                return user_result
            # 其餘異常彈出視窗
            if user_result["ErrorCode"] != config['SUCCESS_CODE']['code']:
                return user_result["ErrorMessage"]

            if '帳號' in user_result['Data']['info_table'] and '層級' in user_result['Data']['info_table']:
                result[i][1] = user_result['Data']['info_table']['層級']
            else:
                return config.HTML_CONTENT_CODE.msg.format(platform=cf.platform)

        # 刪除重查後, 仍查不到層級的會員
        result = list(filter(lambda x: x[1], result))
        # 去重複會員(依時間, 只保留最大時間)
        result = [max(j, key=lambda x:x[-1]) for i,j in groupby(sorted(result, key=lambda x:x[0]), key=lambda x:x[0])]
        if ImportColumns == ["Member", "BlockMemberLayer", "LoginDateTime","VipLevel"]:
            # 舊版系統則去除CumulativeDepositAmount, CumulativeDepositsTimes欄位
            result = [i[0:3] for i in result] 

        return {
                'IsSuccess': 1,
                'ErrorCode': config.SUCCESS_CODE.code,
                'ErrorMessage': config.SUCCESS_CODE.msg,
                'Data': result
                }

    @classmethod
    @keep_connect
    def test_audit(cls, url, timeout, cf, pr6_time, pr6_zone, platform_time, mod_key=None, AuditAPP=1, AuditUniversal=0,
                   AuditMobilesite=0, AuditUniversalPc=0, AuditUniversalApp=0, AuditCustomizationApp=0,
                   ImportColumns: list = ["Member", "BlockMemberLayer", "LoginDateTime", "VipLevel"], **kwargs):
        '''
            LEBO 登入禮 監控
            需要有【現金系統 >> 層級管理 >> 會員查詢】权限。
            需要有【其他 >> 登入日志】权限。
        '''
        if int(AuditAPP) == 0:
            return {
                'IsSuccess': 1,
                'ErrorCode': config.SUCCESS_CODE.code,
                'ErrorMessage': config.SUCCESS_CODE.msg,
                'Data': []
            }
        platform_time -= datetime.timedelta(hours=12)  # LEBO平台美東時間等於北京時間-12小時
        platform_time = platform_time.replace(tzinfo=None)
        logger.info(f'設定檔{cf}\n\n pr6--時區:{pr6_zone} 時間:{pr6_time}\n 平台時間(美東):{platform_time}')

        pr6_time_standard = datetime.datetime.strptime(pr6_time,
                                                       r'%Y-%m-%d %H:%M:%S')  # 設一個維持在時間型態的變數 判別平台時間是否跟PR6時間保持一致
        time_loss = 0  # 時間誤差值
        loss_range = datetime.timedelta(minutes=3)  # 時間誤差標準值為3分鐘
        if platform_time >= pr6_time_standard:
            time_loss = platform_time - pr6_time_standard
        else:
            time_loss = pr6_time_standard - platform_time
        logger.info(f'平台時間與pr6時間誤差:{time_loss}')
        if time_loss > loss_range:  # 誤差時間超過3分鐘 錯誤彈窗
            logger.warning(f'警告!!平台時間與pr6時間誤差:{time_loss}!! 誤差標準範圍為:{loss_range}')
            return (f'平台时间与pr6时间误差大于三分钟')

        # 取得資料夾內日期最大的csv
        path = Path('./config/data') / (mod_key or '.') / 'LEBO'
        if not path.exists():
            path.mkdir()
        csv_date = [i.stem for i in path.iterdir() if '.csv' == i.suffix]
        if cf.last_read_time == '':  # 如果最後讀取時間為空值
            if pr6_time[0:10] in csv_date:  # 先找找有無pr6當日日期的csv檔案
                csv_date = pr6_time[0:10]  # 如果有的話 csv_date是今天
            else:
                csv_date = None  # 如果沒有 csv_date=None
                csv_date_contain = None  # csv的內容也是None
        elif cf.last_read_time[0:10] in csv_date:  # 如果最後讀取時間的日期已經有csv檔案在裡面
            csv_date = cf.last_read_time[0:10]  # csv_date 也是今天


        else:  # 如果都沒有
            csv_date = None  # 今日還沒有csv檔
            csv_date_contain = None  # 也沒有今日csv檔的內容
            logger.info('無當日csv檔')

        if csv_date:
            path = path / f'{csv_date}.csv'
            with path.open('r', encoding='utf-8') as f:
                data = f.read()
            # 讀出檔案轉成陣列
            csv_date_contain = [i.split(',') for i in data.split('\n') if i]
            logger.info(f'當日原有csv檔案內容:{csv_date_contain}')

        if cf.last_read_time and cf.last_read_time != '':  # 如果有輸入上次讀取時間
            if cf.last_read_time[0:10] == pr6_time[0:10]:  # 如果上次讀取時間和PR6時間為同一天
                StartDate = cf.last_read_time[0:10]
                EndDate = pr6_time
            elif cf.last_read_time[0:10] < pr6_time[0:10]:
                StartDate = cf.last_read_time[0:10]
                EndDate = cf.last_read_time[0:10] + ' 23:59:59'
            logger.info(f'上次讀取時間{cf.last_read_time}')

        else:  # 使用者沒有輸入上次讀取時間也沒有紀錄
            logger.info(f'無上次讀取時間--系統預設PR6日期00:00分開始 {pr6_time[0:10]}')
            StartDate = pr6_time[0:10]
            EndDate = pr6_time

        # 查搜APP登入資料
        ltype = [0, 3]  # 查詢類別0=LEBOAPP, 3=簡易版APP
        result = []
        for tp in ltype:
            logger.info(f"查詢:{'LEBOAPP' if tp == 0 else '簡易版APP'}")
            logger.info(f"搜尋日期和型態{type(StartDate)}{StartDate} - {type(EndDate)}{EndDate}")

            tp_result = []
            total_page = 1
            data = {
                'date_start': StartDate,
                'date_end': EndDate,
                'is_success': -1,
                'ltype': tp,
                'page_num': '1000',
                'page': 1
            }
            while total_page >= data['page']:
                result_login_log = lebo.login_log(cf=cf, url=url, data=data, timeout=timeout,
                                                  endpoints='app/daili/adminsave/adminsys/login_log.php')
                if result_login_log['ErrorCode'] in (config.PERMISSION_CODE.code, config.HTML_STATUS_CODE.code):
                    return result_login_log['ErrorMessage']
                if result_login_log["ErrorCode"] in (config.CONNECTION_CODE.code, config.SIGN_OUT_CODE.code):
                    return result_login_log
                if result_login_log['ErrorCode'] == config.HTML_CONTENT_CODE.code:
                    return result_login_log['ErrorMessage']
                if result_login_log['ErrorCode'] != config.SUCCESS_CODE.code:
                    return result_login_log['ErrorMessage']
                # 總頁數
                total_page = result_login_log.get('Data', 1).get('total_page', 1)  # total_page返回int型別
                logger.info(f"頁數/總頁數={data['page']}/{total_page}")
                # 拿出資料
                content = result_login_log.get('Data', None).get('content', None)
                # 查無資料就離開
                if not content:
                    logger.info('查無資料')
                    cf.last_read_time = EndDate
                    logger.info(f'最後讀入時間={cf.last_read_time}')
                    break
                tp_result += [[i['帐号'], i['登入时间']] for i in content if i['层级'] == '會員']
                # 查詢資料時間如果小於csv最後時間,就離開因為後續都是已查過的舊資料
                logger.info(f'tp_result={tp_result}')

                data['page'] += 1
            result += tp_result

        if not result:
            return {
                'IsSuccess': 1,
                'ErrorCode': config.SUCCESS_CODE.code,
                'ErrorMessage': config.SUCCESS_CODE.msg,
                'Data': []
            }
        # 檢查會員字串字元數
        users = [i[0] for i in result]
        member_level = {}
        CumulativeDepositAmount = {}
        CumulativeDepositsTimes = {}
        # 切割總會員數的字元長度(lebo 字元限制)
        for n in split_by_len(users):
            # 查詢會員層級
            username = ','.join(set(n))
            result_level = lebo.app_cash_utotal(
                cf,
                url,
                params={
                    "username": username,
                    "savebtn": "查詢"
                },
            )

            if result_level["ErrorCode"] == (config.CONNECTION_CODE.code, config.SIGN_OUT_CODE.code):
                return result_level
            if result_level['ErrorCode'] in (config.PERMISSION_CODE.code, config.HTML_STATUS_CODE.code):
                return result_level['ErrorMessage']
            if result_level['IsSuccess'] is False and result_level['ErrorCode'] == config.SUCCESS_CODE.code:
                return result_level['ErrorMessage']
            if result_level['ErrorCode'] == config.HTML_CONTENT_CODE.code:
                return result_level['ErrorMessage']
            if result_level['ErrorCode'] == config.NO_USER.code:
                continue
            if result_level['ErrorCode'] != config.SUCCESS_CODE.code:
                return result_level['ErrorMessage']
            member_level.update({i: result_level['Data'][i]['所屬層級'] for i in result_level['Data'] if
                                 i not in ['game_dict', 'RawContent']})
            CumulativeDepositAmount.update(
                {i: str(float(result_level['Data'][i].get('存款總額', 0))) for i in result_level['Data'] if
                 i not in ['game_dict', 'RawContent']})
            CumulativeDepositsTimes.update(
                {i: str(int(result_level['Data'][i].get('存款次數', 0))) for i in result_level['Data'] if
                 i not in ['game_dict', 'RawContent']})
        # 補上層級
        result = [[i[0], member_level.get(i[0], ''), '-', i[1], CumulativeDepositAmount.get(i[0], '0.0'),
                   CumulativeDepositsTimes.get(i[0], '0')] for i in result]
        # 有會員查不到層級
        for i, (acc, lv, viplevel, date, total_amounts, total_counts) in enumerate(result):
            if lv:
                continue
            # ★查詢【帳號層級】
            logger.info(f"會員:{acc}, 需要單獨另查層級")
            user_result = lebo.cash_cash_operation(cf, url, source={
                "username": acc,
                "search": "search",
            })
            # 查詢不到會員直接回傳
            if user_result['ErrorCode'] == config['NO_USER']['code']:
                logger.info(f"會員:{acc}，仍無法查詢到層級，即將刪除")
                continue
            # 連線異常、被登出，重試
            if user_result["ErrorCode"] == config['CONNECTION_CODE']['code']:
                return user_result
            if user_result["ErrorCode"] == config['SIGN_OUT_CODE']['code']:
                return user_result
            # 其餘異常彈出視窗
            if user_result["ErrorCode"] != config['SUCCESS_CODE']['code']:
                return user_result["ErrorMessage"]

            if '帳號' in user_result['Data']['info_table'] and '層級' in user_result['Data']['info_table']:
                result[i][1] = user_result['Data']['info_table']['層級']
            else:
                return config.HTML_CONTENT_CODE.msg.format(platform=cf.platform)

        # 刪除重查後, 仍查不到層級的會員
        result = list(filter(lambda x: x[1], result))
        # 去重複會員(依時間, 只保留最大時間)
        result = [max(j, key=lambda x: x[-1]) for i, j in
                  groupby(sorted(result, key=lambda x: x[0]), key=lambda x: x[0])]
        if ImportColumns == ["Member", "BlockMemberLayer", "LoginDateTime", "VipLevel"]:
            # 舊版系統則去除CumulativeDepositAmount, CumulativeDepositsTimes欄位
            result = [i[0:3] for i in result]
        logger.info(f'抓到的csv資料{result}')
        catch_new_data = []  # 紀錄此次抓取新的資料
        if csv_date_contain:
            logger.info(f'原有CSV檔案{csv_date_contain}')
            for i in result:
                if i not in csv_date_contain:
                    catch_new_data.append(i)
            result = catch_new_data
        logger.info(f'抓到{len(result)}筆新資料')
        cf.last_read_time = EndDate
        logger.info(f'最後讀入時間={cf.last_read_time}')
        return {
            'IsSuccess': 1,
            'ErrorCode': config.SUCCESS_CODE.code,
            'ErrorMessage': config.SUCCESS_CODE.msg,
            'Data': result
        }
        
class registerbonus(BaseFunc):
    '''LEBO 註冊紅包'''

    @classmethod
    def deposit(cls, *args, **kwargs):
        '''
            LEBO 充值
            需要有【現金系統 >> 存款與取款】权限。
            需要有【現金系統 >> 层级管理 >> 会员查询】权限。
        '''
        kwargs['SupportStatus'] = 1
        cls.th = ThreadProgress(kwargs['cf'], kwargs['mod_key'], detail=True)
        cls.th.start()

        result = cls._deposit(*args, **kwargs)

        cls.return_schedule(Action='chkpoint', DBId=kwargs['DBId'], Status='【充值机器人处理完毕】', Progress='-', Detail='-')
        cls.th.stop()
        return result

    @classmethod
    def audit(cls, *args, **kwargs):
        '''
            LEBO 註冊紅包
            需要有【帳號管理 >> 會員管理】权限。
            需要有【帳號管理 >> 會員管理 >> 資料】权限。
            需要有【其他 >> 登入日志】权限。
            需要有【現金系統 >> 层级管理 >> 会员查询】权限。
        '''
        cls.th = ThreadProgress(kwargs['cf'], kwargs['mod_key'])
        cls.th.start()

        result = cls._audit(*args, **kwargs)

        cls.return_schedule(Action='chkbbin', DBId=kwargs['DBId'], Status='【审核机器人处理完毕】', Progress='-', Detail='-')
        cls.th.stop()
        return result

    @classmethod
    def _deposit(cls, url, DBId, Member, DepositAmount, BlockMemberLayer,
        mod_name, mod_key, timeout, backend_remarks, multiple, cf, amount_memo='', Note='', increasethebet_switch=0, increasethebet='0', **kwargs):
        '''LEBO 註冊紅包 充值'''
        # 【充值】
        return super().deposit(url, DBId, Member, DepositAmount, mod_name, mod_key,
                                timeout, backend_remarks, multiple, cf, amount_memo, Note, increasethebet_switch, increasethebet, **kwargs)

    @classmethod
    def _audit(cls, url, DBId, Member, SearchDate, AuditDays, timeout, cf, **kwargs):
        '''LEBO 註冊紅包 監控'''
        member = Member.lower()
        if len(member) < 4 or len(member) > 16:
            return {
                'IsSuccess': 0,
                'ErrorCode': config.ACCOUNT_COUNT_ERROR.code,
                'ErrorMessage': config.ACCOUNT_COUNT_ERROR.msg.format(platform=cf.platform),
                'DBId': DBId,
                'Member': Member,
                'BlockMemberLayer': '',
                'RealName': '',
                'BankAccountExists': 0,
                'AutoAudit': 0,
                'CumulativeDepositAmount': '0.00',
                'CumulativeDepositsTimes': '0',
            }
        # 查詢會員註冊時間&會員層級
        result_member = lebo.members(cf, url, data={
                                                'enable': 'Y',
                                                'onlinestatus': 'all',
                                                'sort': 'adddate',
                                                'orderby': 'desc',
                                                'page_num': '1000',
                                                'page': '0',
                                                'startdate': '',
                                                'enddate': '',
                                                'group': '0',
                                                'utype': '0',
                                                'uname': member}, timeout=timeout)
        BlockMemberLayer = result_member.get('Data', '').get('層級','')
        userid = result_member.get('Data','').get('usernid','')
        # if result_member['IsSuccess']:
        #     BlockMemberLayer = result_member['Data']['層級']
        #     userid = result_member['Data']['usernid']
        if not result_member["IsSuccess"] and result_member['ErrorCode'] != config.NO_USER.code:
            Detail = f'查询失败\n搜寻帐号：{Member}\n{result_member["ErrorMessage"]}'
        else:
            Detail = f'查询成功\n搜寻帐号：{Member}\n会员是否存在：{"否" if result_member["ErrorCode"] == config.NO_USER.code else "是"}'
            if result_member['IsSuccess']:
                Detail += f'\n会员层级：{BlockMemberLayer}'
        cls.return_schedule(Action='chkbbin', DBId=DBId, Status='【帐号管理】 - 【会员管理】', Progress=f'1/1', Detail=Detail)

        if result_member['ErrorCode'] in (config.PERMISSION_CODE.code, config.HTML_STATUS_CODE.code):
            return result_member['ErrorMessage']
        if result_member["ErrorCode"] in (config.CONNECTION_CODE.code,config.SIGN_OUT_CODE.code):
            return result_member
        if result_member['ErrorCode'] in (config.HTML_CONTENT_CODE.code, config.NO_USER.code):
            return {
                    'IsSuccess': int(result_member['IsSuccess']),
                    'ErrorCode': result_member['ErrorCode'],
                    'ErrorMessage': result_member['ErrorMessage'],
                    'DBId': DBId,
                    'Member': Member,
                    'BlockMemberLayer': BlockMemberLayer,
                    'RealName': '',
                    'BankAccountExists': 0,
                    'AutoAudit': 0,
                    'CumulativeDepositAmount': '0.00',
                    'CumulativeDepositsTimes': '0',
                    }
        if result_member['ErrorCode'] != config.SUCCESS_CODE.code:
            return result_member['ErrorMessage']

        # 查詢銀行卡&真實姓名
        result_member_data = lebo.member_data(cf, url, data={
                                                            'usernid': userid
                                                            },member=member, timeout=timeout)

        RealName = result_member_data["Data"]["base_data"].get('真實姓名', '') if result_member_data["IsSuccess"] else ''
        BankAccountExists = int(bool(result_member_data["Data"]["bank_data"] if result_member_data["IsSuccess"] else ''))

        if not result_member_data["IsSuccess"]:
            Detail = f'查询失败\n{result_member_data["ErrorMessage"]}'
        else:
            Detail = f'查询成功\n真实姓名为：{RealName}\n是否绑定银行卡：{"是" if BankAccountExists else "否"}'
        cls.return_schedule(Action='chkbbin', DBId=DBId, Status='【帐号管理】 - 【会员管理】 - 【资料】', Progress=f'1/1', Detail=Detail)

        if result_member_data['ErrorCode'] in (config.PERMISSION_CODE.code, config.HTML_STATUS_CODE.code):
            return result_member['ErrorMessage']
        if result_member_data["ErrorCode"] in (config.CONNECTION_CODE.code,config.SIGN_OUT_CODE.code):
            return result_member_data
        if result_member_data['ErrorCode'] == config.HTML_CONTENT_CODE.code:
            return {
                    'IsSuccess': int(result_member_data['IsSuccess']),
                    'ErrorCode': result_member_data['ErrorCode'],
                    'ErrorMessage': result_member_data['ErrorMessage'],
                    'DBId': DBId,
                    'Member': Member,
                    'BlockMemberLayer': BlockMemberLayer,
                    'RealName': RealName,
                    'BankAccountExists': BankAccountExists,
                    'AutoAudit': 0,
                    'CumulativeDepositAmount': '0.00',
                    'CumulativeDepositsTimes': '0',
                    }
        if result_member_data['ErrorCode'] != config.SUCCESS_CODE.code:
            return result_member_data['ErrorMessage']

        # 稽核查詢特定天數內是否有同IP多帳號的情形
        EndDate = datetime.datetime.strptime(SearchDate, r'%Y/%m/%d')
        StartDate = EndDate - datetime.timedelta(days=int(AuditDays))
        EndDate = EndDate.strftime(r'%Y-%m-%d')
        StartDate = StartDate.strftime(r'%Y-%m-%d')
        total_page = 1
        content = []
        data = {
                'date_start': StartDate,
                'date_end': EndDate,
                'username': member,
                'ip': '',
                'page_num': '1000',
                'page': 1
                }
        while total_page>=data['page']:
            result_login_log = lebo.login_log(cf, url, data=data, timeout=timeout)
            content += result_login_log['Data'].get('content', []) if result_login_log['IsSuccess'] else []
            AutoAudit = int(len(set([data['帐号'] for data in content])) > 1)
            if not result_login_log["IsSuccess"]:
                Detail = f'查询失败\n{result_login_log["ErrorMessage"]}'
            else:
                Detail = f'查询成功\n查询到的帐号列表：{list(set(data["帐号"] for data in content))}\n查询到的IP列表：{list(set(data["IP位置"] for data in content))}'
                Detail += f'\n是否有多帳號同IP：{"是" if AutoAudit else "否"}'
            cls.return_schedule(Action='chkbbin', DBId=DBId, Status='【其他】 - 【登入日志】 - 【自动稽核】', Progress=f'{data["page"]}/{total_page}', Detail=Detail)

            if result_login_log['ErrorCode'] in (config.PERMISSION_CODE.code, config.HTML_STATUS_CODE.code):
                return result_member['ErrorMessage']
            if result_login_log["ErrorCode"] in (config.CONNECTION_CODE.code,config.SIGN_OUT_CODE.code):
                return result_login_log
            if result_login_log['ErrorCode'] == config.HTML_CONTENT_CODE.code:
                return {
                        'IsSuccess': int(result_login_log['IsSuccess']),
                        'ErrorCode': result_login_log['ErrorCode'],
                        'ErrorMessage': result_login_log['ErrorMessage'],
                        'DBId': DBId,
                        'Member': Member,
                        'BlockMemberLayer': BlockMemberLayer,
                        'RealName': RealName,
                        'BankAccountExists': BankAccountExists,
                        'AutoAudit': AutoAudit,
                        'CumulativeDepositAmount': '0.00',
                        'CumulativeDepositsTimes': '0',
                        }
            if result_login_log['ErrorCode'] != config.SUCCESS_CODE.code:
                return result_login_log['ErrorMessage']

            total_page = result_login_log['Data'].get('total_page', 1)
            data['page'] += 1


        #★取得【會員存款金額、存款次數】
        result_deposit_and_withdraw = lebo.app_cash_utotal(cf=cf, url=url, params={
                                                                    'username': Member.lower(),
                                                                    'savebtn': '查詢'
                                                                    }, timeout=timeout)
        CumulativeDepositAmount = float(result_deposit_and_withdraw.get('Data', {}).get(Member.lower(),{}).get('存款總額') or 0)
        CumulativeDepositsTimes = int(result_deposit_and_withdraw.get('Data', {}).get(Member.lower(),{}).get('存款次數') or 0)
        if not result_deposit_and_withdraw['IsSuccess']:
            Detail = f'查询失败\n{result_login_log["ErrorMessage"]}'
        else:
            Detail = '查询成功'
            Detail += f'\n累積存款次數：{CumulativeDepositsTimes}\n累积存款金额：{CumulativeDepositAmount:.2f}'
        cls.return_schedule(Action='chkbbin', DBId=DBId, Status='【现金系统】 - 【层级管理】 - 【会员查询】', Progress='1/1', Detail=Detail)
        if result_deposit_and_withdraw['ErrorCode'] in (config.PERMISSION_CODE.code, config.HTML_STATUS_CODE.code):
            return result_deposit_and_withdraw['ErrorMessage']
        if result_deposit_and_withdraw['IsSuccess'] is False and result_deposit_and_withdraw['ErrorCode'] == config.SUCCESS_CODE.code:
            return {
                    'IsSuccess': int(result_deposit_and_withdraw['IsSuccess']),
                    'ErrorCode': result_deposit_and_withdraw['ErrorCode'],
                    'ErrorMessage': result_deposit_and_withdraw['ErrorMessage'],
                    'DBId': DBId,
                    'Member': Member,
                    'BlockMemberLayer': BlockMemberLayer,
                    'RealName': RealName,
                    'BankAccountExists': BankAccountExists,
                    'AutoAudit': AutoAudit,
                    'CumulativeDepositAmount': f'{float(CumulativeDepositAmount):.2f}',
                    'CumulativeDepositsTimes': f'{float(CumulativeDepositsTimes):.0f}',
                    }
        if result_deposit_and_withdraw["ErrorCode"] in (config.CONNECTION_CODE.code,config.SIGN_OUT_CODE.code):
            return result_deposit_and_withdraw
        if result_deposit_and_withdraw['ErrorCode'] in (config.HTML_CONTENT_CODE.code, config.NO_USER.code):
            return {
                    'IsSuccess': int(result_deposit_and_withdraw['IsSuccess']),
                    'ErrorCode': result_deposit_and_withdraw['ErrorCode'],
                    'ErrorMessage': result_deposit_and_withdraw['ErrorMessage'],
                    'DBId': DBId,
                    'Member': Member,
                    'BlockMemberLayer': BlockMemberLayer,
                    'RealName': RealName,
                    'BankAccountExists': BankAccountExists,
                    'AutoAudit': AutoAudit,
                    'CumulativeDepositAmount': f'{float(CumulativeDepositAmount):.2f}',
                    'CumulativeDepositsTimes': f'{float(CumulativeDepositsTimes):.0f}',
                    }
                    
        # 回傳結果
        return {
            'IsSuccess': 1,
            'ErrorCode': config.SUCCESS_CODE.code,
            'ErrorMessage': config.SUCCESS_CODE.msg,
            'DBId': DBId,
            'Member': Member,
            'BlockMemberLayer': BlockMemberLayer,
            'RealName': RealName,
            'BankAccountExists': BankAccountExists,
            'AutoAudit': AutoAudit,
            'CumulativeDepositAmount': f'{float(CumulativeDepositAmount):.2f}',
            'CumulativeDepositsTimes': f'{float(CumulativeDepositsTimes):.0f}',
        }