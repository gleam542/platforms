from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from itertools import groupby
from . import CODE_DICT as config
from . import module as bbin
from .utils import BETSLIP_RAWWAGERS_BARID
from .utils import BETSLIP_ALLOW_LIST
from .utils import ThreadProgress
from .utils import BETSLIP_LIMIT
from .utils import keep_connect
from .utils import MAX_WORKERS
from .utils import urlparser
import datetime
import logging
import pytz
import copy
import json
import time
import re
logger = logging.getLogger('robot')


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
    @keep_connect
    def deposit(cls, url, DBId, Member, DepositAmount, mod_name, mod_key, 
        timeout, backend_remarks, multiple, cf, amount_memo='', Note='', increasethebet_switch=0, increasethebet='0', **kwargs):
        '''
            BBIN 充值
            需要有【现金系统 >> 现金系统】、【现金系统 >> BB现金系统】权限。
        '''
        SupportStatus = kwargs.get('SupportStatus')
        member = Member.lower()
        if int(increasethebet_switch):
            logger.info(f'使用PR6流水倍數：{int(increasethebet)}')
            multiple = int(increasethebet)
        else:
            logger.info(f'使用機器人打碼量：{int(multiple)}')
            multiple = int(multiple)

        if float(DepositAmount) > float(cf['amount_below']):
            logger.info(f'充值金额{DepositAmount}大于自动出款金额: {cf["amount_below"]}')
            return {
                'IsSuccess': False,
                'ErrorCode': config.AMOUNT_CODE.code,
                'ErrorMessage': config.AMOUNT_CODE.msg,
                'DBId': DBId,
                'Member': Member,
            }
        count = 1
        while True:
            # 【查詢會員ID】
            result_member = bbin.agv3_cl(cf, url,
                params={'module': 'Deposit', 'method': 'query', 'sid': ''},
                data={'search_name': Member},
                timeout=timeout,
                headers={}
            )
            userid = result_member.get('Data', {}).get('user_id')
            if not result_member["IsSuccess"]:
                Detail = f'查询失败\n搜寻帐号：{Member}\n{result_member["ErrorMessage"]}'
            else:
                Detail = f'查询成功\n搜寻帐号：{Member}\n会员是否存在：{"是" if result_member.get("Data", {}).get("LoginName") else "否"}'
                if userid:
                    Detail += f'\n会员ID：{userid}'

            if SupportStatus:
                cls.return_schedule(Action='chkpoint', DBId=DBId, Status='【现金系统】 - 【人工存入】 - 【查询】', Progress=f'{count}/{count}', Detail=Detail)
            else:
                cls.return_schedule(Action='chkpoint', DBId=DBId, Member=Member, current_status='【现金系统】 - 【人工存入】 - 【查询】')

            # 檢查登出或連線異常, 回傳後讓主程式重試
            if result_member['ErrorCode'] == config.SIGN_OUT_CODE.code:
                return result_member
            if result_member['ErrorCode'] == config.CONNECTION_CODE.code:
                return result_member
            # 查詢失敗結束
            if result_member['ErrorCode'] != config.SUCCESS_CODE.code:
                return result_member['ErrorMessage']
            # 查詢不到會員回傳失敗
            if not result_member['Data']['LoginName']:
                return {
                    'IsSuccess': 0,
                    'ErrorCode': config.NO_USER.code,
                    'ErrorMessage': config.NO_USER.msg,
                    'DBId': DBId,
                    'Member': Member,
                }

            # 【充值】
            amount_memo = amount_memo or f'{mod_name}({mod_key}-{DBId}){Note}'
            if backend_remarks and not amount_memo.endswith(f'：{backend_remarks}'):
                amount_memo += f'：{backend_remarks}'

            result_deposit = bbin.agv3_cl(cf, url,
                params={'module': 'Deposit', 'method': 'deposit', 'sid': ''},
                data={
                    'user_id': userid,
                    'hallid': result_member['Data']['HallID'],
                    'CHK_ID': result_member['Data']['CHK_ID'],
                    'user_name': result_member['Data']['user_name'],
                    'date': result_member['Data']['date'],
                    'currency': 'RMB',
                    'abamount_limit': '0',
                    'amount': float(DepositAmount),  # 充值金額
                    'amount_memo': amount_memo,
                    'ComplexAuditCheck': '1',
                    'complex': float(DepositAmount) * multiple,  # 打碼量
                    'CommissionCheck': 'Y',
                    'DepositItem': 'ARD8'
                },
                timeout=timeout,
                headers={}
            )
            if not result_deposit["IsSuccess"]:
                Detail = f'充值失败\n{result_deposit["ErrorMessage"]}'
            else:
                Detail = f'充值成功'

            if SupportStatus:
                cls.return_schedule(Action='chkpoint', DBId=DBId, Status='【现金系统】 - 【人工存入】', Progress=f'{count}/{count}', Detail=Detail)
            else:
                cls.return_schedule(Action='chkpoint', DBId=DBId, Member=Member, current_status='【现金系统】 - 【人工存入】')

            # 自動重新充值開啟, 進行重試
            if cf['recharg'] and result_deposit['ErrorCode'] == config.REPEAT_DEPOSIT.code:
                time.sleep(10)
                count += 1
                continue
            # 充值結果未知，跳過回傳
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

            # 【充值失敗再驗證】
            if result_deposit['IsSuccess'] is False:
                dt = datetime.datetime.strptime(result_member['Data']['date'], '%Y-%m-%d %H:%M:%S %p')
                total_page = 1
                data = {
                    'currency': 'RMB',
                    'start': f'{dt.strftime("%Y-%m-%d")} 00:00:00',
                    'end': f'{dt.strftime("%Y-%m-%d")} 23:59:59',
                    'searchType': '1',
                    'username': member,
                    'sortType': '2',
                    'page': 1,
                    'pageCount': '500',
                    'searchCategory': '119',
                }
                while total_page >= data['page']:
                    result_check = bbin.cash_system_search(cf, url,
                        data=data,
                        timeout=timeout,
                    )

                    if SupportStatus:
                        cls.return_schedule(Action='chkpoint', DBId=DBId, Status='【现金系统】 - 【BB现金系统】', Progress=f'{data["page"]}/{total_page}', Detail='复查充值结果')
                    else:
                        cls.return_schedule(Action='chkpoint', DBId=DBId, Member=Member, current_status='【现金系统】 - 【BB现金系统】')

                    # 內容錯誤，彈出視窗
                    if result_check['ErrorCode'] == config.PERMISSION_CODE.code:
                        return result_check["ErrorMessage"]
                    if result_check['ErrorCode'] == config.HTML_STATUS_CODE.code:
                        return result_check['ErrorMessage']
                    # 連線異常、被登出，略過回傳
                    if result_check['ErrorCode'] == config.CONNECTION_CODE.code:
                        result_check['ErrorCode'] = config.IGNORE_CODE.code
                        return result_check
                    if result_check['ErrorCode'] == config.SIGN_OUT_CODE.code:
                        result_check['ErrorCode'] = config.IGNORE_CODE.code
                        return result_check
                    # 檢查充值是否有成功
                    memos = [r['memo']['content'] for r in result_check['Data']['data']['cashEntryList']]
                    logger.info(memos)
                    if amount_memo in memos:
                        # 找到充值成功回傳成功
                        return {
                            'IsSuccess': 1,
                            'ErrorCode': config.SUCCESS_CODE.code,
                            'ErrorMessage': config.SUCCESS_CODE.msg,
                            'DBId': DBId,
                            'Member': Member,
                        }
                    total_page = int(result_check['Data']['data']['pageNum'])
                    data['page'] += 1
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
    '''BBIN 紅包'''


class passthrough(BaseFunc):
    '''BBIN 闖關'''


class happy7days(BaseFunc):
    '''BBIN 七天樂'''


class pointsbonus(BaseFunc):
    '''BBIN 积分红包'''
    @classmethod
    def deposit(cls, *args, **kwargs):
        '''
            BBIN 充值
            需要有【现金系统 >> 现金系统】、【现金系统 >> BB现金系统】权限。
        '''
        kwargs['SupportStatus'] = 1
        cls.th = ThreadProgress(kwargs['cf'], kwargs['mod_key'], detail=bool(kwargs['SupportStatus']))
        cls.th.start()

        result = cls._deposit(*args, **kwargs)

        cls.return_schedule(Action='chkpoint', DBId=kwargs['DBId'], Status='【充值机器人处理完毕】', Progress='-', Detail='-')
        cls.th.stop()
        return result

    @classmethod
    @keep_connect
    def _deposit(cls, url, DBId, Member, DepositAmount, mod_name, mod_key,
        timeout, backend_remarks, multiple, cf, amount_memo='', Note='', increasethebet_switch=0, increasethebet='0', **kwargs):
        '''BBIN 积分红包 充值'''

        return super().deposit(url, DBId, Member, DepositAmount, mod_name, mod_key, 
            timeout, backend_remarks, multiple, cf, amount_memo, Note, increasethebet_switch, increasethebet, **kwargs)


class promo(BaseFunc):
    '''BBIN 活動大廳'''

    @classmethod
    def deposit(cls, url, DBId, Member, DepositAmount, mod_name, mod_key, 
        timeout, backend_remarks, multiple, cf, amount_memo='', Note='', increasethebet_switch=0, increasethebet='0', **kwargs):
        '''
            BBIN 充值
            需要有【现金系统 >> 现金系统】、【现金系统 >> BB现金系统】权限。
        '''
        if kwargs.get('SupportStatus'):
            cls.th = ThreadProgress(cf, mod_key)
            cls.th.start()

        result = super().deposit(url, DBId, Member, DepositAmount, mod_name, mod_key, 
        timeout, backend_remarks, multiple, cf, amount_memo, Note, increasethebet_switch, increasethebet, **kwargs)

        if kwargs.get('SupportStatus'):
            cls.return_schedule(Action='chkpoint', DBId=DBId, Status='【充值机器人处理完毕】', Progress='-', Detail='-')
            cls.th.stop()
        return result

    @classmethod
    def audit(cls, *args, **kwargs):
        '''
            BBIN 活動大廳
            需要有【帐号管理 >> 层级管理】权限。
            需要有【会员列表 >> 帐号列表】权限。
        '''
        if kwargs.get('SupportStatus'):
            cls.th = ThreadProgress(kwargs['cf'], kwargs['mod_key'])
            cls.th.start()

        result = cls._audit(*args, **kwargs)

        if kwargs.get('SupportStatus'):
            cls.return_schedule(Action='chkbbin', DBId=kwargs['DBId'], Member=kwargs['Member'], current_status='【审核机器人处理完毕】')
            cls.th.stop()
        return result

    @classmethod
    @keep_connect
    def _audit(cls, url, DBId, Member, timeout, cf, **kwargs):
        '''
            BBIN 活動大廳
            需要有【帐号管理 >> 层级管理】权限。
            需要有【会员列表 >> 帐号列表】权限。
        '''
        member = Member.lower()
        # 【查詢會員帳號】
        result_member = bbin.agv3_cl(cf, url,
            params={'module': 'Level', 'method': 'searchMemList'},
            data={'Users': Member},
            timeout=timeout
        )
        # 整理查詢結果
        levels = {select['level_id']: select for select in result_member.get('Data', {}).get('select', [])}
        users = {user['user_name']: user for user in result_member.get('Data', {}).get('user_list', [])}
        user_levelid = users.get(member, {}).get('level_id', '')
        create_time = users.get(member, {}).get('create_time', '')
        BlockMemberLayer = levels.get(user_levelid, {}).get('alias', '')
        if kwargs.get('SupportStatus'):
            if not result_member["IsSuccess"]:
                Detail = f'查询失败\n搜寻帐号：{Member}\n{result_member["ErrorMessage"]}'
            else:
                Detail = f'查询成功\n搜寻帐号：{Member}\n会员是否存在：{"是" if member in users else "否"}'
                if member in users:
                    Detail += f'\n会员注册时间：{create_time}'
                    Detail += f'\n会员层级：{BlockMemberLayer}'
            cls.return_schedule(Action='chkbbin', DBId=DBId, Status='【会员列表】 - 【帐号列表】', Progress=f'1/1', Detail=Detail)
        # 檢查登出或連線異常, 回傳後讓主程式重試
        if result_member['ErrorCode'] == config.SIGN_OUT_CODE.code:
            return result_member
        if result_member['ErrorCode'] == config.CONNECTION_CODE.code:
            return result_member
        # 查詢失敗結束
        if result_member['ErrorCode'] != config.SUCCESS_CODE.code:
            return result_member['ErrorMessage']
        # 查詢無會員回傳
        if member not in users:
            return {
                'IsSuccess': 0,
                'ErrorCode': config.NO_USER.code,
                'ErrorMessage': config.NO_USER.msg,
                'BlockMemberLayer': '',
                'DBId': DBId,
                'Member': Member,
            }
        # 成功查詢到會員回傳
        return {
            'IsSuccess': 1,
            'ErrorCode': config.SUCCESS_CODE.code,
            'ErrorMessage': config.SUCCESS_CODE.msg,
            'BlockMemberLayer': BlockMemberLayer,
            'DBId': DBId,
            'Member': Member,
        }


class betslip(BaseFunc):
    '''BBIN 注單'''
    class Meta:
        extra = {}
        suport = BETSLIP_ALLOW_LIST
    @classmethod
    @keep_connect
    def audit(cls, url, DBId, Member, RawWagersId, SearchGameCategory, SearchDate,
             ExtendLimit, GameCountType, timeout, cf, **kwargs):
        '''
            BBIN 注單系統
            需要有【帐号管理 >> 帐号列表】权限。
            需要有【报表/查询 >> 局查询】权限。
            需要有【会员投注记录】权限。
        '''
        member = Member.lower()
        # 檢查類別選擇是否支援
        target_categories = set(BETSLIP_ALLOW_LIST.keys()) & set(SearchGameCategory)
        if not target_categories:
            return config.CATEGORY_NOT_SUPPORT.msg.format(
                supported=list(BETSLIP_ALLOW_LIST.values())
            )
        logger.info(f'即將查詢類別：{[BETSLIP_ALLOW_LIST[cat] for cat in target_categories]}')
        
         # 【查詢會員帳號】
        result_member = bbin.agv3_cl(cf, url,
            params={'module': 'Level', 'method': 'searchMemList'},
            data={'Users': Member},
            timeout=timeout
        )
        # 整理查詢結果
        levels = {select['level_id']: select for select in result_member.get('Data', {}).get('select', [])}
        users = {user['user_name'].lower(): user for user in result_member.get('Data', {}).get('user_list', [])}
        user_levelid = users.get(member, {}).get('level_id', '')
        BlockMemberLayer = levels.get(user_levelid, {}).get('alias', '')
        userid = users.get(member, {}).get('user_id')
        # 檢查登出或連線異常, 回傳後讓主程式重試
        if result_member['ErrorCode'] == config.SIGN_OUT_CODE.code:
            return result_member
        if result_member['ErrorCode'] == config.CONNECTION_CODE.code:
            return result_member
        # 查詢失敗結束
        if result_member['ErrorCode'] != config.SUCCESS_CODE.code:
            return result_member['ErrorMessage']
        # 查詢無會員回傳
        if member not in users:
            return {
                'IsSuccess': 0,
                'ErrorCode': config.NO_USER.code,
                'ErrorMessage': config.NO_USER.msg,
                'DBId': DBId,
                'Member': Member,
                'BlockMemberLayer': '',

                'RawWagersId': RawWagersId,
                'GameName': '',
                'WagersTimeString': '',
                'WagersTimeStamp': '',
                'BetAmount': '0.00',

                'AllCategoryCommissionable': '0.00',
                'GameCommissionable': '0.00',
                'SingleCategoryCommissionable': "0.00",
                'CategoryName': '',
                'ExtendLimit': ExtendLimit,
                'GameCountType': GameCountType,
            }

        # 【查詢類別清單】
        if not hasattr(cls, 'AMENU'):
            result_amenu = bbin.game_betrecord_search(cf, url, {'SearchData': 'BetQuery'})
            # 檢查登出或連線異常, 回傳後讓主程式重試
            if result_amenu['ErrorCode'] == config.SIGN_OUT_CODE.code:
                return result_amenu
            if result_amenu['ErrorCode'] == config.CONNECTION_CODE.code:
                return result_amenu
            # 查詢失敗結束
            if result_amenu['ErrorCode'] != config.SUCCESS_CODE.code:
                return result_amenu['ErrorMessage']
            # 撈各類別
            cls.AMENU = {k: {**urlparser(v['url']), **v} for k, v in result_amenu['Data']['amenu'].items()}

        # 【訂單查詢】
        for kind in target_categories:
            menu = cls.AMENU[kind]
            result_order = bbin.game_betrecord_search(
                url=url,
                cf=cf,
                endpoints=menu['path'][1:],
                timeout=60,
                params={
                    **menu['query'],
                    'SearchData': 'BetQuery',
                    'BarID': BETSLIP_RAWWAGERS_BARID[kind][0],
                    'GameKind': kind,
                    BETSLIP_RAWWAGERS_BARID[kind][1]: RawWagersId,
                }
            )
            # 檢查登出或連線異常, 回傳後讓主程式重試
            if result_order['ErrorCode'] == config.HTML_STATUS_CODE.code:
                continue
            if result_order['ErrorCode'] == config.SIGN_OUT_CODE.code:
                return result_order
            if result_order['ErrorCode'] == config.CONNECTION_CODE.code:
                return result_order
            # 查詢失敗結束
            if result_order['ErrorCode'] != config.SUCCESS_CODE.code:
                return result_order['ErrorMessage']
            
            # 查到訂單就整理資料並中斷，中斷後跳過else區塊
            if result_order['Data']['records']:
                # 由於某些類別注單號可能有多筆相同，因此排序後取金額最大者
                result_order['Data']['records'] = sorted(
                    result_order['Data']['records'],
                    key=lambda x: x['投注金额'],
                    reverse=True,
                )
                # 另存變數
                games = result_order['Data']['games']
                order = result_order['Data']['records'][0]
                # 添加類別資訊
                order['kind'] = kind
                order['category'] = menu['title']
                # 調整金額格式
                order['投注金额'] = float(order.get('投注金额', '0').replace(',', ''))
                order['派彩金额'] = float(order.get('派彩金额', '0').replace(',', ''))
                # 計算時戳
                wagager_time = order.get('时间', order.get('下注时间'))
                wagager_time = datetime.datetime.strptime(f'{wagager_time} -0400', r'%Y-%m-%d %H:%M:%S %z')
                # 紀錄該筆訂單
                logger.info(f'[局查詢] 訂單資訊: {order}')
                break
        # 查無注單回傳
        else:
            return {
                'IsSuccess': 0,
                'ErrorCode': config.WAGERS_NOT_FOUND.code,
                'ErrorMessage': config.WAGERS_NOT_FOUND.msg,
                'DBId': DBId,
                'Member': Member,
                'BlockMemberLayer': BlockMemberLayer,

                'RawWagersId': RawWagersId,
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
            }

        # 會員與注單不匹配回傳
        if order.get('帐号', '').lower() != member:
            return {
                'IsSuccess': 0,
                'ErrorCode': config.USER_WAGERS_NOT_MATCH.code,
                'ErrorMessage': config.USER_WAGERS_NOT_MATCH.msg,
                'DBId': DBId,
                'Member': Member,
                'BlockMemberLayer': BlockMemberLayer,

                'RawWagersId': RawWagersId,
                'GameName': order.get('游戏类别', ''),
                'WagersTimeString': wagager_time.strftime(r'%Y/%m/%d %H:%M:%S %z'),
                'WagersTimeStamp': f'{int(wagager_time.timestamp()) * 1000}',
                'BetAmount': f'{order["投注金额"]:.2f}',

                'AllCategoryCommissionable': '0.00',
                'GameCommissionable': '0.00',
                'SingleCategoryCommissionable': '0.00',
                'CategoryName': order['category'],
                'ExtendLimit': ExtendLimit,
                'GameCountType': GameCountType,
            }

        # 總計初始值
        TotalCommissionable = 0
        GameCommissionable = 0
        SingleCategoryCommissionable = 0
        # 日期格式轉換
        SearchDate = SearchDate.replace('/', '-') if SearchDate else SearchDate

        # 【會員投注查詢】
        if ExtendLimit != '1':
            pass
        elif GameCountType == '0':
            for kind in target_categories:
                menu = cls.AMENU[kind]
                result_total = bbin.game_betrecord_search(
                    url=url,
                    cf=cf,
                    timeout=cf.timeout,
                    endpoints=menu['path'][1:],
                    params={
                        'GameType': '0',
                        'Limit': '50',
                        'Sort': 'DESC',
                        'BarID': '1',
                        'page': 1,
                        'UserID': userid,
                        'GameKind': kind,
                        'SearchData': 'MemberBets',
                        'date_start': SearchDate,
                        'date_end': SearchDate
                    }
                )
                # 檢查登出或連線異常, 回傳後讓主程式重試
                if result_total['ErrorCode'] == config.SIGN_OUT_CODE.code:
                    return result_total
                if result_total['ErrorCode'] == config.CONNECTION_CODE.code:
                    return result_total
                # 查詢失敗結束
                if result_total['ErrorCode'] != config.SUCCESS_CODE.code:
                    return result_total['ErrorMessage']

                commissionable = result_total['Data']['summary']['总计'].get('投注金额', '0').replace(',', '')
                TotalCommissionable += float(commissionable)
        elif GameCountType == '1':
            if order.get('游戏类别') not in games:
                return {
                    'IsSuccess': 0,
                    'ErrorCode': config.GAME_ERROR.code,
                    'ErrorMessage': f'[{order.get("category")}]选单内无[{order.get("游戏类别", "")}]，无法查询【本游戏当前投注】',
                    'DBId': DBId,
                    'Member': Member,
                    'BlockMemberLayer': BlockMemberLayer,

                    'RawWagersId': RawWagersId,
                    'GameName': order.get('游戏类别', ''),
                    'WagersTimeString': wagager_time.strftime(r'%Y/%m/%d %H:%M:%S %z'),
                    'WagersTimeStamp': f'{int(wagager_time.timestamp()) * 1000}',
                    'BetAmount': f'{order["投注金额"]:.2f}',

                    'AllCategoryCommissionable': '0.00',
                    'GameCommissionable': '0.00',
                    'SingleCategoryCommissionable': '0.00',
                    'CategoryName': order['category'],
                    'ExtendLimit': ExtendLimit,
                    'GameCountType': GameCountType,
                }
            result_total = bbin.game_betrecord_search(
                url=url,
                cf=cf,
                timeout=cf.timeout,
                endpoints=menu['path'][1:],
                params={
                    'GameType': games[order['游戏类别']],
                    'Limit': '50',
                    'Sort': 'DESC',
                    'BarID': '1',
                    'page': 1,
                    'UserID': userid,
                    'GameKind': order['kind'],
                    'SearchData': 'MemberBets',
                    'date_start': SearchDate,
                    'date_end': SearchDate
                }
            )
            # 檢查登出或連線異常, 回傳後讓主程式重試
            if result_total['ErrorCode'] == config.SIGN_OUT_CODE.code:
                return result_total
            if result_total['ErrorCode'] == config.CONNECTION_CODE.code:
                return result_total
            # 查詢失敗結束
            if result_total['ErrorCode'] != config.SUCCESS_CODE.code:
                return result_total['ErrorMessage']
            commissionable = result_total['Data']['summary']['总计'].get('投注金额', '0').replace(',', '')
            GameCommissionable = float(commissionable)
        elif GameCountType == '2':
            result_total = bbin.game_betrecord_search(
                url=url,
                cf=cf,
                timeout=cf.timeout,
                endpoints=menu['path'][1:],
                params={
                    'GameType': '0',
                    'Limit': '50',
                    'Sort': 'DESC',
                    'BarID': '1',
                    'page': 1,
                    'UserID': userid,
                    'GameKind': order['kind'],
                    'SearchData': 'MemberBets',
                    'date_start': SearchDate,
                    'date_end': SearchDate
                }
            )
            # 檢查登出或連線異常, 回傳後讓主程式重試
            if result_total['ErrorCode'] == config.SIGN_OUT_CODE.code:
                return result_total
            if result_total['ErrorCode'] == config.CONNECTION_CODE.code:
                return result_total
            # 查詢失敗結束
            if result_total['ErrorCode'] != config.SUCCESS_CODE.code:
                return result_total['ErrorMessage']
            commissionable = result_total['Data']['summary']['总计'].get('投注金额', '0').replace(',', '')
            SingleCategoryCommissionable = float(commissionable)

        return {
            'IsSuccess': 1,
            'ErrorCode': config.SUCCESS_CODE.code,
            'ErrorMessage': config.SUCCESS_CODE.msg,
            'DBId': DBId,
            'Member': Member,
            'BlockMemberLayer': BlockMemberLayer,

            'RawWagersId': RawWagersId,
            'GameName': order.get('游戏类别', ''),
            'WagersTimeString': wagager_time.strftime(r'%Y/%m/%d %H:%M:%S %z'),
            'WagersTimeStamp': f'{int(wagager_time.timestamp()) * 1000}',
            'BetAmount': f'{order["投注金额"]:.2f}',

            'AllCategoryCommissionable': f'{TotalCommissionable:.2f}',
            'GameCommissionable': f'{GameCommissionable:.2f}',
            'SingleCategoryCommissionable': f'{SingleCategoryCommissionable:.2f}',
            'CategoryName': order['category'],
            'ExtendLimit': ExtendLimit,
            'GameCountType': GameCountType,
        }


class enjoyup(BaseFunc):
    '''BBIN 喜上喜'''
    class Meta:
        extra = {}
        suport = BETSLIP_ALLOW_LIST

    @classmethod
    def deposit(cls, *args, **kwargs):
        '''
            BBIN 充值
            需要有【现金系统 >> 现金系统】、【现金系统 >> BB现金系统】权限。
        '''
        SupportStatus = kwargs.get('SupportStatus')
        cls.th = ThreadProgress(kwargs['cf'], kwargs['mod_key'])
        cls.th.start()

        result = cls._deposit(*args, **kwargs)

        if SupportStatus:
            cls.return_schedule(Action='chkpoint', DBId=kwargs['DBId'], Status='【充值机器人处理完毕】', Progress='-', Detail='-')
        else:
            cls.return_schedule(Action='chkpoint', DBId=kwargs['DBId'], Member=kwargs['Member'], current_status='【充值机器人处理完毕】')

        cls.th.stop()
        return result

    @classmethod
    @keep_connect
    def _deposit(cls, url, DBId, Member, DepositAmount, mod_name, mod_key,
        timeout, backend_remarks, multiple, cf, amount_memo='', Note='', increasethebet_switch=0, increasethebet='0', **kwargs):
        '''BBIN 喜上喜 充值'''

        return super().deposit(url, DBId, Member, DepositAmount, mod_name, mod_key, 
            timeout, backend_remarks, multiple, cf, amount_memo, Note, increasethebet_switch, increasethebet, **kwargs)

    @classmethod
    def audit(cls, *args, **kwargs):
        '''
            BBIN 喜上喜系統
            需要有【帐号管理 >> 帐号列表】权限。
            需要有【报表/查询 >> 局查询】权限。
            需要有【会员投注记录】权限。
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
    @keep_connect
    def _audit(cls, url, DBId, Member, RawWagersId, SearchGameCategory, SearchDate,
             ExtendLimit, GameCountType, timeout, cf, **kwargs):
        '''BBIN 喜上喜系統'''
        SupportStatus = kwargs.get('SupportStatus')
        member = Member.lower()
        # 檢查類別選擇是否支援
        target_categories = set(BETSLIP_ALLOW_LIST.keys()) & set(SearchGameCategory)
        if not target_categories:
            return config.CATEGORY_NOT_SUPPORT.msg.format(
                supported=list(BETSLIP_ALLOW_LIST.values())
            )
        logger.info(f'即將查詢類別：{[BETSLIP_ALLOW_LIST[cat] for cat in target_categories]}')

        # 【查詢會員帳號】
        result_member = bbin.agv3_cl(cf, url,
            params={'module': 'Level', 'method': 'searchMemList'},
            data={'Users': Member},
            timeout=timeout
        )
        # 整理查詢結果
        levels = {select['level_id']: select for select in result_member.get('Data', {}).get('select', [])}
        users = {user['user_name'].lower(): user for user in result_member.get('Data', {}).get('user_list', [])}
        user_levelid = users.get(member, {}).get('level_id', '')
        BlockMemberLayer = levels.get(user_levelid, {}).get('alias', '')
        userid = users.get(member, {}).get('user_id')
        # 【查詢會員帳號】
        if SupportStatus:
            if not result_member["IsSuccess"]:
                Detail = f'查询失败\n搜寻帐号：{Member}\n{result_member["ErrorMessage"]}'
            else:
                Detail = f'查询成功\n搜寻帐号：{Member}\n会员是否存在：{"是" if member in users else "否"}'
                if member in users:
                    Detail += f'\n会员层级：{BlockMemberLayer}'
            cls.return_schedule(Action='chkbbin', DBId=DBId, Status='【会员列表】 - 【帐号列表】', Progress=f'1/1', Detail=Detail)
        else:
            cls.return_schedule(Action='chkbbin', DBId=DBId, Member=Member, current_status='【会员列表】 - 【帐号列表】')
        # 檢查登出或連線異常, 回傳後讓主程式重試
        if result_member['ErrorCode'] == config.SIGN_OUT_CODE.code:
            return result_member
        if result_member['ErrorCode'] == config.CONNECTION_CODE.code:
            return result_member
        # 查詢失敗結束
        if result_member['ErrorCode'] != config.SUCCESS_CODE.code:
            return result_member['ErrorMessage']
        # 查詢無會員回傳
        if member not in users:
            return {
                'IsSuccess': 0,
                'ErrorCode': config.NO_USER.code,
                'ErrorMessage': config.NO_USER.msg,
                'DBId': DBId,
                'Member': Member,
                'BlockMemberLayer': '',

                'RawWagersId': RawWagersId,
                'GameName': '',
                'WagersTimeString': '',
                'WagersTimeStamp': '',
                'BetAmount': '0.00',

                'AllCategoryCommissionable': '0.00',
                'GameCommissionable': '0.00',
                'SingleCategoryCommissionable': "0.00",
                'PayoutAmount': '0.00',
                'CategoryName': '',
                'ExtendLimit': ExtendLimit,
                'GameCountType': GameCountType,
            }

        # 【查詢類別清單】
        if not hasattr(cls, 'AMENU'):
            result_amenu = bbin.game_betrecord_search(cf, url, {'SearchData': 'BetQuery'})
            if SupportStatus:
                cls.return_schedule(Action='chkbbin', DBId=DBId, Status='【查询类别清单】', Progress=f'1/1', Detail=f'查詢成功：{result_amenu.get("IsSuccess")}')
            else:
                cls.return_schedule(Action='chkbbin', DBId=DBId, Member=Member, current_status='【查询类别清单】')
            # 檢查登出或連線異常, 回傳後讓主程式重試
            if result_amenu['ErrorCode'] == config.SIGN_OUT_CODE.code:
                return result_amenu
            if result_amenu['ErrorCode'] == config.CONNECTION_CODE.code:
                return result_amenu
            # 查詢失敗結束
            if result_amenu['ErrorCode'] != config.SUCCESS_CODE.code:
                return result_amenu['ErrorMessage']
            # 撈各類別
            cls.AMENU = {k: {**urlparser(v['url']), **v} for k, v in result_amenu['Data']['amenu'].items()}

        # 【訂單查詢】
        for kind in target_categories:
            menu = cls.AMENU[kind]
            result_order = bbin.game_betrecord_search(
                url=url,
                cf=cf,
                endpoints=menu['path'][1:],
                timeout=60,
                params={
                    **menu['query'],
                    'SearchData': 'BetQuery',
                    'BarID': BETSLIP_RAWWAGERS_BARID[kind][0],
                    'GameKind': kind,
                    BETSLIP_RAWWAGERS_BARID[kind][1]: RawWagersId,
                }
            )
            # 檢查登出或連線異常, 回傳後讓主程式重試
            if result_order['ErrorCode'] == config.HTML_STATUS_CODE.code:
                continue
            if result_order['ErrorCode'] == config.SIGN_OUT_CODE.code:
                return result_order
            if result_order['ErrorCode'] == config.CONNECTION_CODE.code:
                return result_order
            # 查詢失敗結束
            if result_order['ErrorCode'] != config.SUCCESS_CODE.code:
                return result_order['ErrorMessage']
            
            # 查到訂單就整理資料並中斷，中斷後跳過else區塊
            if result_order['Data']['records']:
                # 由於某些類別注單號可能有多筆相同，因此排序後取金額最大者
                result_order['Data']['records'] = sorted(
                    result_order['Data']['records'],
                    key=lambda x: x['投注金额'],
                    reverse=True,
                )
                # 另存變數
                games = result_order['Data']['games']
                order = result_order['Data']['records'][0]
                # 添加類別資訊
                order['kind'] = kind
                order['category'] = menu['title']
                # 調整金額格式
                order['投注金额'] = float(order.get('投注金额', '0').replace(',', ''))
                order['派彩金额'] = float(order.get('派彩金额', '0').replace(',', ''))
                # 計算時戳
                wagager_time = order.get('时间', order.get('下注时间'))
                wagager_time = datetime.datetime.strptime(f'{wagager_time} -0400', r'%Y-%m-%d %H:%M:%S %z')
                # 紀錄該筆訂單
                logger.info(f'[局查詢] 訂單資訊: {order}')
                break
        # 查無注單回傳
        else:
            return {
                'IsSuccess': 0,
                'ErrorCode': config.WAGERS_NOT_FOUND.code,
                'ErrorMessage': config.WAGERS_NOT_FOUND.msg,
                'DBId': DBId,
                'Member': Member,
                'BlockMemberLayer': BlockMemberLayer,

                'RawWagersId': RawWagersId,
                'GameName': '',
                'WagersTimeString': '',
                'WagersTimeStamp': '',
                'BetAmount': '0.00',
                'PayoutAmount': '0.00',

                'AllCategoryCommissionable': '0.00',
                'GameCommissionable': '0.00',
                'SingleCategoryCommissionable': '0.00',
                'CategoryName': '',
                'ExtendLimit': ExtendLimit,
                'GameCountType': GameCountType,
            }

        # 會員與注單不匹配回傳
        if order.get('帐号', '').lower() != member:
            return {
                'IsSuccess': 0,
                'ErrorCode': config.USER_WAGERS_NOT_MATCH.code,
                'ErrorMessage': config.USER_WAGERS_NOT_MATCH.msg,
                'DBId': DBId,
                'Member': Member,
                'BlockMemberLayer': BlockMemberLayer,

                'RawWagersId': RawWagersId,
                'GameName': order.get('游戏类别', ''),
                'WagersTimeString': wagager_time.strftime(r'%Y/%m/%d %H:%M:%S %z'),
                'WagersTimeStamp': f'{int(wagager_time.timestamp()) * 1000}',
                'BetAmount': f'{order["投注金额"]:.2f}',
                'PayoutAmount': f'{order["派彩金额"]:.2f}',

                'AllCategoryCommissionable': '0.00',
                'GameCommissionable': '0.00',
                'SingleCategoryCommissionable': '0.00',
                'CategoryName': order['category'],
                'ExtendLimit': ExtendLimit,
                'GameCountType': GameCountType,
            }

        # 總計初始值
        TotalCommissionable = 0
        GameCommissionable = 0
        SingleCategoryCommissionable = 0
        # 日期格式轉換
        SearchDate = SearchDate.replace('/', '-') if SearchDate else SearchDate

        # 【會員投注查詢】
        if ExtendLimit != '1':
            pass
        elif GameCountType == '0':
            for kind in target_categories:
                menu = cls.AMENU[kind]
                result_total = bbin.game_betrecord_search(
                    url=url,
                    cf=cf,
                    timeout=cf.timeout,
                    endpoints=menu['path'][1:],
                    params={
                        'GameType': '0',
                        'Limit': '50',
                        'Sort': 'DESC',
                        'BarID': '1',
                        'page': 1,
                        'UserID': userid,
                        'GameKind': kind,
                        'SearchData': 'MemberBets',
                        'date_start': SearchDate,
                        'date_end': SearchDate
                    }
                )
                # 檢查登出或連線異常, 回傳後讓主程式重試
                if result_total['ErrorCode'] == config.SIGN_OUT_CODE.code:
                    return result_total
                if result_total['ErrorCode'] == config.CONNECTION_CODE.code:
                    return result_total
                # 查詢失敗結束
                if result_total['ErrorCode'] != config.SUCCESS_CODE.code:
                    return result_total['ErrorMessage']

                commissionable = result_total['Data']['summary']['总计'].get('投注金额', '0').replace(',', '')
                TotalCommissionable += float(commissionable)
        elif GameCountType == '1':
            if order.get('游戏类别') not in games:
                return {
                    'IsSuccess': 0,
                    'ErrorCode': config.GAME_ERROR.code,
                    'ErrorMessage': f'[{order.get("category")}]选单内无[{order.get("游戏类别", "")}]，无法查询【本游戏当前投注】',
                    'DBId': DBId,
                    'Member': Member,
                    'BlockMemberLayer': BlockMemberLayer,

                    'RawWagersId': RawWagersId,
                    'GameName': order.get('游戏类别', ''),
                    'WagersTimeString': wagager_time.strftime(r'%Y/%m/%d %H:%M:%S %z'),
                    'WagersTimeStamp': f'{int(wagager_time.timestamp()) * 1000}',
                    'BetAmount': f'{order["投注金额"]:.2f}',
                    'PayoutAmount': f'{order["派彩金额"]:.2f}',

                    'AllCategoryCommissionable': '0.00',
                    'GameCommissionable': '0.00',
                    'SingleCategoryCommissionable': '0.00',
                    'CategoryName': order['category'],
                    'ExtendLimit': ExtendLimit,
                    'GameCountType': GameCountType,
                }
            result_total = bbin.game_betrecord_search(
                url=url,
                cf=cf,
                timeout=cf.timeout,
                endpoints=menu['path'][1:],
                params={
                    'GameType': games[order['游戏类别']],
                    'Limit': '50',
                    'Sort': 'DESC',
                    'BarID': '1',
                    'page': 1,
                    'UserID': userid,
                    'GameKind': order['kind'],
                    'SearchData': 'MemberBets',
                    'date_start': SearchDate,
                    'date_end': SearchDate
                }
            )
            # 檢查登出或連線異常, 回傳後讓主程式重試
            if result_total['ErrorCode'] == config.SIGN_OUT_CODE.code:
                return result_total
            if result_total['ErrorCode'] == config.CONNECTION_CODE.code:
                return result_total
            # 查詢失敗結束
            if result_total['ErrorCode'] != config.SUCCESS_CODE.code:
                return result_total['ErrorMessage']
            commissionable = result_total['Data']['summary']['总计'].get('投注金额', '0').replace(',', '')
            GameCommissionable = float(commissionable)
        elif GameCountType == '2':
            result_total = bbin.game_betrecord_search(
                url=url,
                cf=cf,
                timeout=cf.timeout,
                endpoints=menu['path'][1:],
                params={
                    'GameType': '0',
                    'Limit': '50',
                    'Sort': 'DESC',
                    'BarID': '1',
                    'page': 1,
                    'UserID': userid,
                    'GameKind': order['kind'],
                    'SearchData': 'MemberBets',
                    'date_start': SearchDate,
                    'date_end': SearchDate
                }
            )
            # 檢查登出或連線異常, 回傳後讓主程式重試
            if result_total['ErrorCode'] == config.SIGN_OUT_CODE.code:
                return result_total
            if result_total['ErrorCode'] == config.CONNECTION_CODE.code:
                return result_total
            # 查詢失敗結束
            if result_total['ErrorCode'] != config.SUCCESS_CODE.code:
                return result_total['ErrorMessage']
            commissionable = result_total['Data']['summary']['总计'].get('投注金额', '0').replace(',', '')
            SingleCategoryCommissionable = float(commissionable)

        return {
            'IsSuccess': 1,
            'ErrorCode': config.SUCCESS_CODE.code,
            'ErrorMessage': config.SUCCESS_CODE.msg,
            'DBId': DBId,
            'Member': Member,
            'BlockMemberLayer': BlockMemberLayer,

            'RawWagersId': RawWagersId,
            'GameName': order.get('游戏类别', ''),
            'WagersTimeString': wagager_time.strftime(r'%Y/%m/%d %H:%M:%S %z'),
            'WagersTimeStamp': f'{int(wagager_time.timestamp()) * 1000}',
            'BetAmount': f'{order["投注金额"]:.2f}',
            'PayoutAmount': f'{order["派彩金额"]:.2f}',

            'AllCategoryCommissionable': f'{TotalCommissionable:.2f}',
            'GameCommissionable': f'{GameCommissionable:.2f}',
            'SingleCategoryCommissionable': f'{SingleCategoryCommissionable:.2f}',
            'CategoryName': order['category'],
            'ExtendLimit': ExtendLimit,
            'GameCountType': GameCountType,
        }


class freespin(BaseFunc):
    '''BBIN 旋轉注單'''
    class Meta:
        extra = {}
        suport = {
            # '5_1': 'BB电子 糖果派对',
            # '5_2': 'BB电子 糖果派对2',
            # '5_3': 'BB电子 糖果派对3',
            # '5_4': 'BB电子 糖果派对-极速版',

            '58_1': 'PG电子 麻将胡了',
            '58_2': 'PG电子 麻将胡了2',
            '58_3': 'PG电子 寻宝黄金城',
            '58_4': 'PG电子 麒麟送宝',
            '58_5': 'PG电子 亡灵大盗',
            '58_6': 'PG电子 福运象财神',
            '58_7': 'PG电子 爱尔兰精灵',
            '58_8': 'PG电子 赏金女王',
            '58_9': 'PG电子 糖心风暴',
            '58_10': 'PG电子 赢财神',

            '58_11': 'PG电子 双囍临门',
            '58_12': 'PG电子 赏金船长',
            '58_13': 'PG电子 麒麟送宝',
            '58_14': 'PG电子 唐伯虎点秋香',
            '58_15': 'PG电子 宝石传奇',
            '58_16': 'PG电子 亡灵大盗',
            '58_17': 'PG电子 招财喵',
            '58_18': 'PG电子 澳门壕梦',
            '58_19': 'PG电子 嘻游记',
            '58_20': 'PG电子 凤凰传奇',

            '58_21': 'PG电子 霹雳神偷',
            '58_22': 'PG电子 日月星辰',
            '58_23': 'PG电子 神鹰宝石',
            '58_24': 'PG电子 火树赢花',
            '58_25': 'PG电子 百鬼夜行',
            '58_26': 'PG电子 冰火双娇',

            '52_1': 'CQ9电子 跳高高',
            '52_2': 'CQ9电子 跳高高2',
            '52_3': 'CQ9电子 跳起来',
            '52_4': 'CQ9电子 宙斯',
            '52_5': 'CQ9电子 发财神2',
            '52_6': 'CQ9电子 五福临门',
            '52_7': 'CQ9电子 单手跳高高',
            '52_8': 'CQ9电子 直式洪福齐天',
            '52_9': 'CQ9电子 六颗糖',
            '52_10': 'CQ9电子 鸿福齐天',
            '52_11': 'CQ9电子 直式蹦迪',
            '52_12': 'CQ9电子 武圣',
            '52_13': 'CQ9电子 金鸡报喜',

            '52_14': 'CQ9电子 蹦迪',
            '52_15': 'CQ9电子 野狼Disco',
            '52_16': 'CQ9电子 跳过来',
            '52_17': 'CQ9电子 血之吻',
            '52_18': 'CQ9电子 跳起来2',
            '52_19': 'CQ9电子 火烧连环船2',
            '52_20': 'CQ9电子 东方神起',

            '39_1': 'JDB电子 变脸',
            '39_2': 'JDB电子 飞鸟派对',
            '39_3': 'JDB电子 王牌特工',
            '39_4': 'JDB电子 金刚',
            '39_5': 'JDB电子 富豪哥',
            '39_6': 'JDB电子 江山美人',
            '39_7': 'JDB电子 芝麻开门',
            '39_8': 'JDB电子 亿万富翁',
            '39_9': 'JDB电子 台湾黑熊',
            '39_10': 'JDB电子 月光秘宝',

            '39_11': 'JDB电子 玛雅金疯狂',
            '39_12': 'JDB电子 芝麻开门II',
            '39_13': 'JDB电子 聚宝盆',
            '39_14': 'JDB电子 龙舞',
            '39_15': 'JDB电子 变脸II',
            '39_16': 'JDB电子 雷神之锤'
        }

    @classmethod
    def deposit(cls, *args, **kwargs):
        '''
            BBIN 充值
            需要有【现金系统 >> 现金系统】、【现金系统 >> BB现金系统】权限。
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
    @keep_connect
    def _deposit(cls, url, DBId, Member, DepositAmount, mod_name, mod_key,
        timeout, backend_remarks, multiple, cf, amount_memo='', Note='', increasethebet_switch=0, increasethebet='0', **kwargs):
        '''BBIN 旋轉注單 充值'''

        return super().deposit(url, DBId, Member, DepositAmount, mod_name, mod_key, 
            timeout, backend_remarks, multiple, cf, amount_memo, Note, increasethebet_switch, increasethebet, **kwargs)

    @classmethod
    def audit(cls, *args, **kwargs):
        '''
            BBIN 旋轉注單
            需要有【帐号管理 >> 帐号列表】权限。
            需要有【报表/查询 >> 局查询】权限。
            需要有【会员投注记录】权限。
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
    @keep_connect
    def _audit(cls, url, DBId, Member, RawWagersId, SearchGameCategory, SearchDate,
             ExtendLimit, GameCountType, timeout, cf, mod_name, Action, **kwargs):
        '''BBIN 旋轉注單'''
        SupportStatus = kwargs.get('SupportStatus')
        member = Member.lower()
        # 檢查類別選擇是否支援
        target_categories = set(['58', '52', '39']) & set(SearchGameCategory)
        if not target_categories:
            return config.CATEGORY_NOT_SUPPORT.msg.format(
                supported=list(map(lambda k: BETSLIP_ALLOW_LIST[k], ['58', '52', '39']))
            )
        logger.info(f'即將查詢類別：{[BETSLIP_ALLOW_LIST[cat] for cat in target_categories]}')

        # 【查詢會員帳號】
        result_member = bbin.agv3_cl(cf, url,
            params={'module': 'Level', 'method': 'searchMemList'},
            data={'Users': Member},
            timeout=timeout
        )
        # 整理查詢結果
        levels = {select['level_id']: select for select in result_member.get('Data', {}).get('select', [])}
        users = {user['user_name'].lower(): user for user in result_member.get('Data', {}).get('user_list', [])}
        user_levelid = users.get(member, {}).get('level_id', '')
        BlockMemberLayer = levels.get(user_levelid, {}).get('alias', '')
        userid = users.get(member, {}).get('user_id')
        if SupportStatus:
            if not result_member["IsSuccess"]:
                Detail = f'查询失败\n搜寻帐号：{member}\n{result_member["ErrorMessage"]}'
            else:
                Detail = f'查询成功\n搜寻帐号：{member}\n会员是否存在：{"是" if member in users else "否"}'
                if member in users:
                    Detail += f'\n会员层级：{BlockMemberLayer}'
            cls.return_schedule(Action='chkbbin', DBId=DBId, Status='【会员列表】 - 【帐号列表】', Progress=f'1/1', Detail=Detail)
        else:
            cls.return_schedule(Action='chkbbin', DBId=DBId, Member=Member, current_status='【会员列表】 - 【帐号列表】')
        # 檢查登出或連線異常, 回傳後讓主程式重試
        if result_member['ErrorCode'] == config.SIGN_OUT_CODE.code:
            return result_member
        if result_member['ErrorCode'] == config.CONNECTION_CODE.code:
            return result_member
        # 查詢失敗結束
        if result_member['ErrorCode'] != config.SUCCESS_CODE.code:
            return result_member['ErrorMessage']
        # 查詢無會員回傳
        if member not in users:
            return {
                'IsSuccess': 0,
                'ErrorCode': config.NO_USER.code,
                'ErrorMessage': config.NO_USER.msg,
                'DBId': DBId,
                'Member': Member,
                'BlockMemberLayer': '',

                'RawWagersId': RawWagersId,
                'GameName': '',
                'WagersTimeString': '',
                'WagersTimeStamp': '',
                'BetAmount': '0.00',

                'AllCategoryCommissionable': '0.00',
                'GameCommissionable': '0.00',
                'SingleCategoryCommissionable': "0.00",
                'CategoryName': '',
                'ExtendLimit': ExtendLimit,
                'GameCountType': GameCountType,
            }

        # 【查詢類別清單】
        if not hasattr(cls, 'AMENU'):
            result_amenu = bbin.game_betrecord_search(cf, url, {'SearchData': 'BetQuery'})
            if SupportStatus:
                cls.return_schedule(Action='chkbbin', DBId=DBId, Status='【查询类别清单】', Progress=f'1/1', Detail=f'查詢成功：{result_amenu.get("IsSuccess")}')
            else:
                cls.return_schedule(Action='chkbbin', DBId=DBId, Member=Member, current_status='【查询类别清单】')
            # 檢查登出或連線異常, 回傳後讓主程式重試
            if result_amenu['ErrorCode'] == config.SIGN_OUT_CODE.code:
                return result_amenu
            if result_amenu['ErrorCode'] == config.CONNECTION_CODE.code:
                return result_amenu
            # 查詢失敗結束
            if result_amenu['ErrorCode'] != config.SUCCESS_CODE.code:
                return result_amenu['ErrorMessage']
            # 撈各類別
            cls.AMENU = {k: {**urlparser(v['url']), **v} for k, v in result_amenu['Data']['amenu'].items()}

        # 【訂單查詢】
        for kind in target_categories:
            menu = cls.AMENU[kind]
            result_order = bbin.game_betrecord_search(
                url=url,
                cf=cf,
                endpoints=menu['path'][1:],
                timeout=60,
                params={
                    **menu['query'],
                    'SearchData': 'BetQuery',
                    'BarID': BETSLIP_RAWWAGERS_BARID[kind][0],
                    'GameKind': kind,
                    BETSLIP_RAWWAGERS_BARID[kind][1]: RawWagersId,
                }
            )
            # 檢查登出或連線異常, 回傳後讓主程式重試
            if result_order['ErrorCode'] == config.HTML_STATUS_CODE.code:
                continue
            if result_order['ErrorCode'] == config.SIGN_OUT_CODE.code:
                return result_order
            if result_order['ErrorCode'] == config.CONNECTION_CODE.code:
                return result_order
            # 查詢失敗結束
            if result_order['ErrorCode'] != config.SUCCESS_CODE.code:
                return result_order['ErrorMessage']
            
            # 查到訂單就整理資料並中斷，中斷後跳過else區塊
            if result_order['Data']['records']:
                # 由於某些類別注單號可能有多筆相同，因此排序後取金額最大者
                result_order['Data']['records'] = sorted(
                    result_order['Data']['records'],
                    key=lambda x: x['投注金额'],
                    reverse=True,
                )
                # 另存變數
                games = result_order['Data']['games']
                order = result_order['Data']['records'][0]
                # 添加類別資訊
                order['kind'] = kind
                order['category'] = menu['title']
                # 調整金額格式
                order['投注金额'] = float(order.get('投注金额', '0').replace(',', ''))
                order['派彩金额'] = float(order.get('派彩金额', '0').replace(',', ''))
                # 計算時戳
                wagager_time = order.get('时间', order.get('下注时间'))
                wagager_time = datetime.datetime.strptime(f'{wagager_time} -0400', r'%Y-%m-%d %H:%M:%S %z')
                # 紀錄該筆訂單
                logger.info(f'[局查詢] 訂單資訊: {order}')
                break
        # 查無注單回傳
        else:
            return {
                'IsSuccess': 0,
                'ErrorCode': config.WAGERS_NOT_FOUND.code,
                'ErrorMessage': config.WAGERS_NOT_FOUND.msg,
                'DBId': DBId,
                'Member': Member,
                'BlockMemberLayer': BlockMemberLayer,

                'RawWagersId': RawWagersId,
                'GameName': '',
                'WagersTimeString': '',
                'WagersTimeStamp': '',
                'BetAmount': '0.00',
                'PayoutAmount': '0.00',

                'AllCategoryCommissionable': '0.00',
                'GameCommissionable': '0.00',
                'SingleCategoryCommissionable': '0.00',
                'CategoryName': '',
                'ExtendLimit': ExtendLimit,
                'GameCountType': GameCountType,
            }

        # 會員與注單不匹配回傳
        if order.get('帐号', '').lower() != member:
            return {
                'IsSuccess': 0,
                'ErrorCode': config.USER_WAGERS_NOT_MATCH.code,
                'ErrorMessage': config.USER_WAGERS_NOT_MATCH.msg,
                'DBId': DBId,
                'Member': Member,
                'BlockMemberLayer': BlockMemberLayer,

                'RawWagersId': RawWagersId,
                'GameName': order.get('游戏类别', ''),
                'WagersTimeString': wagager_time.strftime(r'%Y/%m/%d %H:%M:%S %z'),
                'WagersTimeStamp': f'{int(wagager_time.timestamp()) * 1000}',
                'BetAmount': f'{order["投注金额"]:.2f}',
                'PayoutAmount': f'{order["派彩金额"]:.2f}',

                'AllCategoryCommissionable': '0.00',
                'GameCommissionable': '0.00',
                'SingleCategoryCommissionable': '0.00',
                'CategoryName': order['category'],
                'ExtendLimit': ExtendLimit,
                'GameCountType': GameCountType,
            }

        # 總計初始值
        TotalCommissionable = 0
        GameCommissionable = 0
        SingleCategoryCommissionable = 0
        # 日期格式轉換
        SearchDate = SearchDate.replace('/', '-') if SearchDate else SearchDate

        # 【會員投注查詢】
        if ExtendLimit != '1':
            pass
        elif GameCountType == '0':
            for kind in target_categories:
                menu = cls.AMENU[kind]
                result_total = bbin.game_betrecord_search(
                    url=url,
                    cf=cf,
                    timeout=cf.timeout,
                    endpoints=menu['path'][1:],
                    params={
                        'GameType': '0',
                        'Limit': '50',
                        'Sort': 'DESC',
                        'BarID': '1',
                        'page': 1,
                        'UserID': userid,
                        'GameKind': kind,
                        'SearchData': 'MemberBets',
                        'date_start': SearchDate,
                        'date_end': SearchDate
                    }
                )
                # 檢查登出或連線異常, 回傳後讓主程式重試
                if result_total['ErrorCode'] == config.SIGN_OUT_CODE.code:
                    return result_total
                if result_total['ErrorCode'] == config.CONNECTION_CODE.code:
                    return result_total
                # 查詢失敗結束
                if result_total['ErrorCode'] != config.SUCCESS_CODE.code:
                    return result_total['ErrorMessage']

                commissionable = result_total['Data']['summary']['总计'].get('投注金额', '0').replace(',', '')
                TotalCommissionable += float(commissionable)
        elif GameCountType == '1':
            if order.get('游戏类别') not in games:
                return {
                    'IsSuccess': 0,
                    'ErrorCode': config.GAME_ERROR.code,
                    'ErrorMessage': f'[{order.get("category")}]选单内无[{order.get("游戏类别", "")}]，无法查询【本游戏当前投注】',
                    'DBId': DBId,
                    'Member': Member,
                    'BlockMemberLayer': BlockMemberLayer,

                    'RawWagersId': RawWagersId,
                    'GameName': order.get('游戏类别', ''),
                    'WagersTimeString': wagager_time.strftime(r'%Y/%m/%d %H:%M:%S %z'),
                    'WagersTimeStamp': f'{int(wagager_time.timestamp()) * 1000}',
                    'BetAmount': f'{order["投注金额"]:.2f}',
                    'FreeSpin': f'{order["派彩金额"]:.2f}',

                    'AllCategoryCommissionable': '0.00',
                    'GameCommissionable': '0.00',
                    'SingleCategoryCommissionable': '0.00',
                    'CategoryName': order['category'],
                    'ExtendLimit': ExtendLimit,
                    'GameCountType': GameCountType,
                }
            result_total = bbin.game_betrecord_search(
                url=url,
                cf=cf,
                timeout=cf.timeout,
                endpoints=menu['path'][1:],
                params={
                    'GameType': games[order['游戏类别']],
                    'Limit': '50',
                    'Sort': 'DESC',
                    'BarID': '1',
                    'page': 1,
                    'UserID': userid,
                    'GameKind': order['kind'],
                    'SearchData': 'MemberBets',
                    'date_start': SearchDate,
                    'date_end': SearchDate
                }
            )
            # 檢查登出或連線異常, 回傳後讓主程式重試
            if result_total['ErrorCode'] == config.SIGN_OUT_CODE.code:
                return result_total
            if result_total['ErrorCode'] == config.CONNECTION_CODE.code:
                return result_total
            # 查詢失敗結束
            if result_total['ErrorCode'] != config.SUCCESS_CODE.code:
                return result_total['ErrorMessage']
            commissionable = result_total['Data']['summary']['总计'].get('投注金额', '0').replace(',', '')
            GameCommissionable = float(commissionable)
        elif GameCountType == '2':
            result_total = bbin.game_betrecord_search(
                url=url,
                cf=cf,
                timeout=cf.timeout,
                endpoints=menu['path'][1:],
                params={
                    'GameType': '0',
                    'Limit': '50',
                    'Sort': 'DESC',
                    'BarID': '1',
                    'page': 1,
                    'UserID': userid,
                    'GameKind': order['kind'],
                    'SearchData': 'MemberBets',
                    'date_start': SearchDate,
                    'date_end': SearchDate
                }
            )
            # 檢查登出或連線異常, 回傳後讓主程式重試
            if result_total['ErrorCode'] == config.SIGN_OUT_CODE.code:
                return result_total
            if result_total['ErrorCode'] == config.CONNECTION_CODE.code:
                return result_total
            # 查詢失敗結束
            if result_total['ErrorCode'] != config.SUCCESS_CODE.code:
                return result_total['ErrorMessage']
            commissionable = result_total['Data']['summary']['总计'].get('投注金额', '0').replace(',', '')
            SingleCategoryCommissionable = float(commissionable)

        # 【查詢遊戲詳細內容】 - 【查詢詳細頁面網址】
        result_url = bbin.betrecord_betrecord_url(
            cf=cf,
            url=url,
            params={
                'gamekind': order['kind'],
                'userid': users[member]['user_id'],
                'wagersid': order['注单编号'],
                'SearchData': 'MemberBets'
            }
        )
        # 內容錯誤，彈出視窗
        if result_url['ErrorCode'] == config.PERMISSION_CODE.code:
            return result_url["ErrorMessage"]
        if result_url['ErrorCode'] == config.HTML_STATUS_CODE.code:
            return result_url['ErrorMessage']
        if result_url['ErrorCode'] == config.HTML_CONTENT_CODE.code:
            return result_url['ErrorMessage']
        # 連線異常、被登出重試
        if result_url['ErrorCode'] == config.SIGN_OUT_CODE.code:
            return result_url
        if result_url['ErrorCode'] == config.CONNECTION_CODE.code:
            return result_url
        parsed_url = urlparser(result_url['Data']['data'])
        query = parsed_url['query']
        logger.info(f'注單詳細內容參數: {urlparser(result_url["Data"]["data"])}')
        if SupportStatus:
            cls.return_schedule(Action='chkbbin', DBId=DBId, Status='【查询游戏详细内容】 - 【查询详细页面网址】', Progress=f'1/1', Detail='-')
        else:
            cls.return_schedule(Action='chkbbin', DBId=DBId, Member=Member, current_status='【查询游戏详细内容】 - 【查询详细页面网址】')

        # 【查詢遊戲詳細內容】 - 【查詢詳細頁面內容】
        support_game = [
            v.split(' ')[1]
            for k, v in cls.Meta.suport.items()
            if k.split('_')[0] == order['kind']
        ]
        # BB电子
        # if order['kind'] == '5' and order.get('游戏类别') in support_game:
        #     # 【查詢詳細內容】
        #     pass
        # PG電子, 目前只支援麻將胡了, 麻将胡了2
        if order['kind'] == '58' and order.get('游戏类别') in support_game:
            # 由於pg_url約每幾個月會換一次，需要動態抓取
            # 動態抓取回應時間較長會導致後續查詢異常
            # 因此將pg_url存在cls中，發生異常時重啟
            if not hasattr(cls, 'pg_url'):
                r = bbin.session.get(result_url['Data']['data'], verify=False, timeout=timeout)
                redirect_url = r.html.find('script[src*="redirect"]', first=True).attrs.get('src')

                r1 = bbin.session.get(f'{parsed_url["scheme"]}://{parsed_url["hostname"]}/history/{redirect_url[2:]}', verify=False, timeout=timeout)
                cls.pg_url = re.search('//\w+-\w+.\w+.com', r1.text).group(0)

            # 【查詢詳細內容】
            result_detail = bbin.pg_bet_history(
                cf=cf,
                url=f'{parsed_url["scheme"]}:{cls.pg_url}/',
                params={'t': query['t']},
                data={'sid': query['psid']},
                timeout=timeout + 30
            )
            # 取得免費旋轉資訊
            detail = [bd for bd in result_detail.get('Data', {}).get('dt', {}).get('bh', {}).get('bd', []) if bd.get('gd', {}).get('fs', {})]
            if detail:
                FreeSpin = 1
            else:
                FreeSpin = 0

            if SupportStatus:
                Detail = (
                    f'查询是否成功: {result_detail["IsSuccess"]}\n'
                    f'是否为免费旋转：{"是" if FreeSpin else "否"}'
                )
                cls.return_schedule(Action='chkbbin', DBId=DBId, Status='【查询游戏详细内容】 - 【查询详细页面内容】', Progress=f'1/1', Detail=Detail)
            else:
                cls.return_schedule(Action='chkbbin', DBId=DBId, Member=Member, current_status='【查询游戏详细内容】 - 【查询详细页面内容】')

            # 連線異常、被登出重試
            if result_detail['ErrorCode'] == config.SIGN_OUT_CODE.code:
                return result_detail
            if result_detail['ErrorCode'] == config.CONNECTION_CODE.code:
                return result_detail
            # PG回傳錯誤
            if result_detail['Data']['err']:
                return f'PG电子显示：{result_detail["Data"]["err"]["msg"]}({result_detail["Data"]["err"]["cd"]})'
        # CQ9電子, 目前支援【跳高高】【跳高高2】【跳起来】
        elif order['kind'] == '52' and order.get('游戏类别') in support_game:
            # 【查詢詳細內容】
            cq9_url = urlparser(result_url['Data']['data'])['scheme'] + '://'
            cq9_url += urlparser(result_url['Data']['data'])['hostname'] + '/'
            result_detail = bbin.cq9_bet_history(
                url=cq9_url, cf=cf, params=query, timeout=timeout+30)
            # 取得免費旋轉資訊
            FreeSpin = int(bool(result_detail.get('Data', {}).get('data', {}).get('detail', {}).get('wager', {}).get('sub')))

            if SupportStatus:
                Detail = (
                    f'查询是否成功: {result_detail["IsSuccess"]}\n'
                    f'是否为免费旋转：{"是" if FreeSpin else "否"}'
                )
                cls.return_schedule(Action='chkbbin', DBId=DBId, Status='【查询游戏详细内容】 - 【查询详细页面内容】', Progress=f'1/1', Detail=Detail)
            else:
                cls.return_schedule(Action='chkbbin', DBId=DBId, Member=Member, current_status='【查询游戏详细内容】 - 【查询详细页面内容】')

            # 連線異常、被登出重試
            if result_detail['ErrorCode'] == config.SIGN_OUT_CODE.code:
                return result_detail
            if result_detail['ErrorCode'] == config.CONNECTION_CODE.code:
                return result_detail
            # CQ9回傳錯誤
            if result_detail['Data']['status']['code'] != '0':
                return f'CQ9电子显示：{result_detail["Data"]["status"]["message"]}({result_detail["Data"]["status"]["code"]})'
        # JDB電子, 目前支援【变脸】【飞鸟派对】
        elif order['kind'] == '39' and order.get('游戏类别') in support_game:
            # if not hasattr(cls, 'jdb_url'):
            #     r = bbin.session.get(f'{parsed_url["scheme"]}://{parsed_url["hostname"]}/api/env', verify=False, timeout=timeout)
            #     cls.jdb_url = r.json()['BASE_URL']
            # 【查詢詳細內容】
            result_detail = bbin.jdb_bet_history(
                cf=cf,
                # url=cls.jdb_url,
                

                # params={},
                # data=json.dumps({
                #     'dao': "GetGameResultByGameSeq_slot",
                #     'gameSeqNo': query['gameSeq'],
                #     'playerId': query['playerId'],
                # }),


                params={
                    'gameSeqNo': query['gameSeq'],
                    'playerId': query['playerId'],  
                    'gameGroupType': 0,                  
                },
                data={},


                headers={
                    'content-type': 'application/json;charset=UTF-8'
                },
                timeout=timeout + 30
            )
            # 取得免費旋轉資訊
            # if result_detail.get('Data', {}).get('data', {}).get('gamehistory', {}).get('has_freegame') == 'true':
            if result_detail.get('Data', {}).get('data', {}).get('has_freegame') == 'true':
                FreeSpin = 1
            else:
                FreeSpin = 0

            if SupportStatus:
                Detail = (
                    f'查询是否成功: {result_detail["IsSuccess"]}\n'
                    f'是否为免费旋转：{"是" if FreeSpin else "否"}'
                )
                cls.return_schedule(Action='chkbbin', DBId=DBId, Status='【查询游戏详细内容】 - 【查询详细页面内容】', Progress=f'1/1', Detail=Detail)
            else:
                cls.return_schedule(Action='chkbbin', DBId=DBId, Member=Member, current_status='【查询游戏详细内容】 - 【查询详细页面内容】')

            # 連線異常、被登出重試
            if result_detail['ErrorCode'] == config.SIGN_OUT_CODE.code:
                return result_detail
            if result_detail['ErrorCode'] == config.CONNECTION_CODE.code:
                return result_detail
            # JDB回傳錯誤
            if (
                result_detail['Data']['code'] != '00000' 
                # or not result_detail['Data'].get('data', {}).get('gamehistory', {})
            ):
                return f'JDB电子显示：{result_detail["Data"]}'

        # 其餘不支援的訂單
        else:
            if SupportStatus:
                Detail = (
                    f'分类及游戏类别是否正确：否\n'
                    f'注单分类为：{order["kind"]}\n'
                    f'注单游戏类别为：{order["游戏类别"]}'
                )
                cls.return_schedule(Action='chkbbin', DBId=DBId, Status='【查询游戏详细内容】 - 【查询详细页面内容】', Progress=f'1/1', Detail=Detail)
            else:
                cls.return_schedule(Action='chkbbin', DBId=DBId, Member=Member, current_status='【查询游戏详细内容】 - 【查询详细页面内容】')

            if order['kind'] not in ['58', '52', '39']:
                code = config.CATEGORY_ERROR.code
                msg = config.CATEGORY_ERROR.msg.format(CategoryName=order['category'])
            else:
                code = config.GAME_ERROR.code
                msg = config.GAME_ERROR.msg.format(GameName=order['游戏类别'])
            return {
                'IsSuccess': 0,
                'ErrorCode': code,
                'ErrorMessage': msg,
                'DBId': DBId,
                'Member': Member,
                'BlockMemberLayer': levels[users[member]['level_id']]['alias'],

                'RawWagersId': RawWagersId,
                'GameName': order.get('游戏类别', ''),
                'WagersTimeString': wagager_time.strftime(r'%Y/%m/%d %H:%M:%S %z'),
                'WagersTimeStamp': f'{int(wagager_time.timestamp()) * 1000}',
                'BetAmount': f'{order["投注金额"]:.2f}',
                'FreeSpin': 0,

                'AllCategoryCommissionable': f'{TotalCommissionable:.2f}',
                'GameCommissionable': f'{GameCommissionable:.2f}',
                'SingleCategoryCommissionable': f'{SingleCategoryCommissionable:.2f}',
                'CategoryName': order['category'],
                'ExtendLimit': ExtendLimit,
                'GameCountType': GameCountType,
            }

        return {
            'IsSuccess': 1,
            'ErrorCode': config.SUCCESS_CODE.code,
            'ErrorMessage': config.SUCCESS_CODE.msg,
            'DBId': DBId,
            'Member': Member,
            'BlockMemberLayer': levels[users[member]['level_id']]['alias'],

            'RawWagersId': RawWagersId,
            'GameName': order.get('游戏类别', ''),
            'WagersTimeString': wagager_time.strftime(r'%Y/%m/%d %H:%M:%S %z'),
            'WagersTimeStamp': f'{int(wagager_time.timestamp()) * 1000}',
            'BetAmount': f'{order["投注金额"]:.2f}',
            'FreeSpin': FreeSpin,

            'AllCategoryCommissionable': f'{TotalCommissionable:.2f}',
            'GameCommissionable': f'{GameCommissionable:.2f}',
            'SingleCategoryCommissionable': f'{SingleCategoryCommissionable:.2f}',
            'CategoryName': order['category'],
            'ExtendLimit': ExtendLimit,
            'GameCountType': GameCountType,
        }


class experiencebonus(BaseFunc):
    '''BBIN 體驗金'''

    @classmethod
    def deposit(cls, *args, **kwargs):
        '''
            BBIN 充值、移动层级
            需要有【现金系统 >> 现金系统】、【现金系统 >> BB现金系统】权限。
            需要有【帐号管理 >> 层级管理】权限。
        '''
        kwargs['SupportStatus'] = 1
        cls.th = ThreadProgress(kwargs['cf'], kwargs['mod_key'], detail=bool(kwargs['SupportStatus']))
        cls.th.start()

        result = cls._deposit(*args, **kwargs)

        cls.return_schedule(Action='chkpoint', DBId=kwargs['DBId'], Status='【充值机器人处理完毕】', Progress='-', Detail='-')
        cls.th.stop()
        return result

    @classmethod
    def audit(cls, *args, **kwargs):
        '''
            BBIN 体验金
            需要有【登入纪录 >> 自动稽核】权限。
            需要有【会员列表 >> 帐号列表】权限。
            需要有【会员详细资料】权限。
        '''
        cls.th = ThreadProgress(kwargs['cf'], kwargs['mod_key'])
        cls.th.start()

        result = cls._audit(*args, **kwargs)

        cls.return_schedule(Action='chkbbin', DBId=kwargs['DBId'], Status='【审核机器人处理完毕】', Progress='-', Detail='-')
        cls.th.stop()
        return result

    @classmethod
    @keep_connect
    def _deposit(cls, url, DBId, Member, DepositAmount, ChangeLayer, BlockMemberLayer,
        mod_name, mod_key, timeout, backend_remarks, multiple, cf, amount_memo='', Note='', increasethebet_switch=0, increasethebet='0', **kwargs):
        '''BBIN 體驗金 充值'''
        member = Member.lower()
        # 【充值】
        result_deposit = super().deposit(url, DBId, Member, DepositAmount, mod_name, mod_key, 
            timeout, backend_remarks, multiple, cf, amount_memo, Note, increasethebet_switch, increasethebet, **kwargs)
        if type(result_deposit) == str:
            return result_deposit
        if not result_deposit['IsSuccess']:
            return {**result_deposit, 'BlockMemberLayer': '-', 'LayerMessage': ''}
        if not ChangeLayer:
            return {**result_deposit, 'BlockMemberLayer': '-', 'LayerMessage': ''}

        # 【移動層級】 - 【查詢層級列表】
        count = 1
        while True:
            result_member = bbin.agv3_cl(cf, url,
                params={'module': 'Level', 'method': 'searchMemList'},
                data={'Users': Member},
                timeout=timeout
            )
            # 整理查詢結果
            levels = {select['alias']: select for select in result_member.get('Data', {}).get('select', [])}
            users = {user['user_name']: user for user in result_member.get('Data', {}).get('user_list', [])}
            if not result_member["IsSuccess"]:
                Detail = f'查询失败\n查询层级：{BlockMemberLayer}\n{result_member["ErrorMessage"]}'
            else:
                Detail = f'查询成功\n查询层级：{BlockMemberLayer}\n会员是否存在：{"是" if member in users else "否"}\n层级是否存在：{"是" if BlockMemberLayer in levels else "否"}'
            cls.return_schedule(Action='chkpoint', DBId=DBId, Status='【层级管理】 - 【会员查询】', Progress=f'{count}/{count}', Detail=Detail)

            # 檢查登出或連線異常, 回傳後讓主程式重試
            if result_member['ErrorCode'] == config.SIGN_OUT_CODE.code:
                return {
                    'IsSuccess': 1,
                    'ErrorCode': config.LayerError.code,
                    'ErrorMessage': config.LayerError.msg.format(platform=cf.platform, msg='查询层级列表被登出，无法移动层级'),
                    'DBId': DBId,
                    'Member': Member,
                    'BlockMemberLayer': '-',
                    'LayerMessage': '查询层级列表被登出，无法移动层级'
                }
            if result_member['ErrorCode'] == config.CONNECTION_CODE.code:
                time.sleep(1)
                continue
            # 查詢失敗結束
            if result_member['ErrorCode'] != config.SUCCESS_CODE.code:
                return result_member['ErrorMessage']
            if BlockMemberLayer not in levels:
                return {
                    'IsSuccess': 1,
                    'ErrorCode': config.LayerError.code,
                    'ErrorMessage': config.LayerError.msg.format(platform=cf.platform, msg=f'无【{BlockMemberLayer}】层级'),
                    'DBId': DBId,
                    'Member': Member,
                    'BlockMemberLayer': '-',
                    'LayerMessage': f'无【{BlockMemberLayer}】层级'
                }
            # 【移動層級】
            result_layer = bbin.agv3_cl(cf, url,
                params={'module': 'Level', 'method': 'searchMemListSet'},
                data={'Change[0][user_id]': str(users[member]["user_id"]), 'Change[0][target]': str(levels[BlockMemberLayer]['level_id'])}
            )
            if not result_layer["IsSuccess"]:
                Detail = f'层级移动失败\n{result_layer["ErrorMessage"]}'
            else:
                Detail = f'层级移动成功'
            cls.return_schedule(Action='chkpoint', DBId=DBId, Status='【會員查詢結果】 - 【分層調整】', Progress=f'{count}/{count}', Detail=Detail)
            # 檢查登出或連線異常, 回傳後讓主程式重試
            if result_layer['ErrorCode'] == config.SIGN_OUT_CODE.code:
                return {
                    'IsSuccess': 1,
                    'ErrorCode': config.LayerError.code,
                    'ErrorMessage': config.LayerError.msg.format(platform=cf.platform, msg='移动层级被登出，无法移动层级'),
                    'DBId': DBId,
                    'Member': Member,
                    'BlockMemberLayer': '-',
                    'LayerMessage': f'移动层级被登出，无法移动层级'
                }
            if result_layer['ErrorCode'] == config.CONNECTION_CODE.code:
                time.sleep(1)
                continue
            # 查詢失敗結束
            if result_layer['ErrorCode'] != config.SUCCESS_CODE.code:
                return {
                    'IsSuccess': 1,
                    'ErrorCode': result_deposit['ErrorCode'],
                    'ErrorMessage': result_deposit['ErrorMessage'],
                    'DBId': DBId,
                    'Member': Member,
                    'BlockMemberLayer': '-',
                    'LayerMessage': result_layer['ErrorMessage']
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
    @keep_connect
    def _audit(cls, url, DBId, Member, SearchDate, AuditDays, timeout, cf, SearchDateTime='',  AppLoginPeriod='', **kwargs):
        '''BBIN 體驗金 監控'''
        member = Member.lower()
        # 【查詢會員帳號】
        result_member = bbin.agv3_cl(cf, url,
            params={'module': 'Level', 'method': 'searchMemList'},
            data={'Users': Member},
            timeout=timeout
        )
        # 整理查詢結果
        levels = {select['level_id']: select for select in result_member.get('Data', {}).get('select', [])}
        users = {user['user_name']: user for user in result_member.get('Data', {}).get('user_list', [])}
        userid = users.get(member, {}).get('user_id')
        user_levelid = users.get(member, {}).get('level_id', '')
        create_time = users.get(member, {}).get('create_time', '')
        BlockMemberLayer = levels.get(user_levelid, {}).get('alias', '')
        TotalAddAmount = f"{eval(str(users.get(member, {}).get('deposit_total', '0.00')).replace(',', '')):.2f}"
        if not result_member["IsSuccess"]:
            Detail = f'查询失败\n搜寻帐号：{Member}\n{result_member["ErrorMessage"]}'
        else:
            Detail = f'查询成功\n搜寻帐号：{Member}\n会员是否存在：{"是" if member in users else "否"}'
            if member in users:
                Detail += f'\n会员注册时间：{create_time}'
                Detail += f'\n会员层级：{BlockMemberLayer}'
                Detail += f'\n存款总额：{TotalAddAmount}'
        cls.return_schedule(Action='chkbbin', DBId=DBId, Status='【会员列表】 - 【帐号列表】', Progress=f'1/1', Detail=Detail)
        # 檢查登出或連線異常, 回傳後讓主程式重試
        if result_member['ErrorCode'] == config.SIGN_OUT_CODE.code:
            return result_member
        if result_member['ErrorCode'] == config.CONNECTION_CODE.code:
            return result_member
        # 查詢失敗結束
        if result_member['ErrorCode'] != config.SUCCESS_CODE.code:
            return result_member['ErrorMessage']
        # 查詢無會員回傳
        if member not in users:
            return {
                'IsSuccess': 0,
                'ErrorCode': config.NO_USER.code,
                'ErrorMessage': config.NO_USER.msg,
                'BlockMemberLayer': '',
                'TotalAddAmount': '0.00',
                'DBId': DBId,
                'Member': Member,
                'RealName': '',
                'BankAccountExists': 0,
                'AutoAudit': 0,
                'RegisterTimeString': '',
                'RegisterTimeStamp': '',
            }
        create_time = datetime.datetime.strptime(f'{create_time} -0400', r'%Y-%m-%d %H:%M:%S %z')

        # 權限檢查 (通過後保存sid於cls中，當重登或重啟後才會再次進行權限檢查)
        if not hasattr(cls, 'sid') or cls.sid != bbin.session.cookies.get('sid'):
            result_permissions = bbin.users_detail_permission(cf=cf, url=url, headers={
                    'permname': 'UserDetailInfo',
                    'referer': url + f'vi/user/{userid}/detail_info'
            })
            account_permissions = {p['name'] for p in result_permissions.get('Data', {}).get('permissions', [])}
            need_permissions = {'GetAccount': '银行帐户资讯', 'NameReal': '真实姓名'}
            miss_permissions = need_permissions.keys() - account_permissions
            if not result_permissions["IsSuccess"]:
                Detail = f'查询失败\n{result_permissions["ErrorMessage"]}'
            elif miss_permissions:
                Detail = f'查询失败\n缺少权限：{"、".join([need_permissions[p] for p in miss_permissions])}'
            else:
                Detail = f'查询成功'
            cls.return_schedule(Action='chkbbin', DBId=DBId, Status='【会员详细资料】', Progress=f'1/1', Detail=Detail)
            # 檢查登出或連線異常, 回傳後讓主程式重試
            if result_permissions['ErrorCode'] == config.SIGN_OUT_CODE.code:
                return result_permissions
            if result_permissions['ErrorCode'] == config.CONNECTION_CODE.code:
                return result_permissions
            # 查詢失敗結束
            if result_permissions['ErrorCode'] != config.SUCCESS_CODE.code:
                return result_permissions['ErrorMessage']
            # 權限不足結束
            if 'UserDetailInfo' not in account_permissions:
                return f'【BBIN >> 會員詳細資訊】 權限未開啟。'
            if miss_permissions:
                return f'【BBIN >> 會員詳細資訊 >> {"、".join([need_permissions[p] for p in miss_permissions])}】 權限未開啟。'
        cls.sid = bbin.session.cookies.get('sid')

        # 【會員詳細資料】 - 【真實姓名】
        result_realname = bbin.users_detail(cf, url,
            endpoints=f'hex/user/{userid}/detail',
            headers={
                'permname': 'UserDetailInfo',
                'referer': url + f'vi/user/{userid}/detail_info'
            }
        )
        real_name = result_realname.get('Data', {}).get('name_real', '')
        if not result_realname["IsSuccess"]:
            Detail = f'查询失败\n{result_realname["ErrorMessage"]}'
        else:
            Detail = f'查询成功\n真实姓名为：{real_name}'
        cls.return_schedule(Action='chkbbin', DBId=DBId, Status='【会员详细资料】 - 【真实姓名】', Progress=f'1/1', Detail=Detail)
        # 檢查登出或連線異常, 回傳後讓主程式重試
        if result_realname['ErrorCode'] == config.SIGN_OUT_CODE.code:
            return result_realname
        if result_realname['ErrorCode'] == config.CONNECTION_CODE.code:
            return result_realname
        # 查詢失敗結束
        if result_realname['ErrorCode'] != config.SUCCESS_CODE.code:
            return result_realname['ErrorMessage']

        # 【會員詳細資料】 - 【綁定銀行卡】
        result_bank_account = bbin.users_bank_account(cf, url, 
            params={'users[]': [userid]},
            headers={
                'permname': 'UserDetailInfo',
                'referer': url + f'vi/user/{userid}/detail_info'
            }
        )
        bank_account_exists = int(bool(result_bank_account.get('Data', {}).get(userid, {}).get('bank')))
        if not result_bank_account["IsSuccess"]:
            Detail = f'查询失败\n{result_bank_account["ErrorMessage"]}'
        else:
            Detail = f'查询成功\n是否绑定银行卡：{"是" if bank_account_exists else "否"}'
        cls.return_schedule(Action='chkbbin', DBId=DBId, Status='【会员详细资料】 - 【绑定银行卡】', Progress=f'1/1', Detail=Detail)
        # 檢查登出或連線異常, 回傳後讓主程式重試
        if result_bank_account['ErrorCode'] == config.SIGN_OUT_CODE.code:
            return result_bank_account
        if result_bank_account['ErrorCode'] == config.CONNECTION_CODE.code:
            return result_bank_account
        # 查詢失敗結束
        if result_bank_account['ErrorCode'] != config.SUCCESS_CODE.code:
            return result_bank_account['ErrorMessage']

        # 【自動稽核】
        EndDate = datetime.datetime.strptime(SearchDate, r'%Y/%m/%d')
        StartDate = EndDate - datetime.timedelta(days=int(AuditDays))
        EndDate = EndDate.strftime(r'%Y-%m-%d')
        StartDate = StartDate.strftime(r'%Y-%m-%d')
        total_page = 1
        params = {
            'AccName': member,
            'StartDate': StartDate,
            'EndDate': EndDate,
            'show': '100',
            'page': 1,
        }
        content = []
        while total_page>=params['page']:
            result_record_auto = bbin.login_record_info_auto(cf, url, params=params)
            content += result_record_auto.get('Data', {}).get('content', [])
            auto_audit = int(len({r['帐号'] for r in content}) > 1)
            if not result_record_auto["IsSuccess"]:
                Detail = f'查询失败\n{result_record_auto["ErrorMessage"]}'
            else:
                Detail = f'查询成功\n查询到的帐号列表：{list(set(r["帐号"] for r in content))}\n查询到的IP列表：{list(set(r["IP位置"] for r in content))}'
                Detail += f'\n是否有多帳號同IP：{"是" if auto_audit else "否"}'
            cls.return_schedule(Action='chkbbin', DBId=DBId, Status='【自动稽核】', Progress=f'{params["page"]}/{total_page}', Detail=Detail)
            # 檢查登出或連線異常, 回傳後讓主程式重試
            if result_record_auto['ErrorCode'] == config.SIGN_OUT_CODE.code:
                return result_record_auto
            if result_record_auto['ErrorCode'] == config.CONNECTION_CODE.code:
                return result_record_auto
            # 查詢失敗結束
            if result_record_auto['ErrorCode'] != config.SUCCESS_CODE.code:
                return result_record_auto['ErrorMessage']
            params['page'] += 1
            total_page = result_record_auto['Data']['total_page']
            if auto_audit:
                break

        rtn = {
            'IsSuccess': 1,
            'ErrorCode': config.SUCCESS_CODE.code,
            'ErrorMessage': config.SUCCESS_CODE.msg,
            'DBId': DBId,
            'Member': Member,
            'BlockMemberLayer': BlockMemberLayer,
            'TotalAddAmount': TotalAddAmount,
            'RealName': real_name,
            'BankAccountExists': bank_account_exists,
            'AutoAudit': auto_audit,
            'RegisterTimeString': create_time.strftime(r'%Y/%m/%d %H:%M:%S %z'),
            'RegisterTimeStamp': f'{int(create_time.timestamp()) * 1000}',
        }

        # 【手機APP登入】
        if SearchDateTime:
            StartDateTime = (datetime.datetime.strptime(SearchDateTime, '%Y/%m/%d %H:%M:%S') - datetime.timedelta(hours=int(AppLoginPeriod))).strftime('%Y-%m-%d %H:%M:%S')
            params = {
                'page': 1,
                'Role': '1',
                'LoginSource': 'APP',
                'Username': member,
                'StartDate': StartDateTime,
                'EndDate': SearchDateTime.replace('/', '-'),
                'show': '100',
            }
            result_login_record = bbin.login_record_info_mobile2(cf, url, params=params)
            loginList = [i for i in result_login_record['Data'].get('content', []) if i['帐号'] == member]  
            AppLogin = 1 if loginList else 0
            if not result_login_record["IsSuccess"]:
                Detail = f'查询失败\n{result_login_record["ErrorMessage"]}'
            else:
                Detail = f'查询成功\n是否在{AppLoginPeriod}小时内使用手机APP登入：{"是" if AppLogin else "否"}'
            cls.return_schedule(Action='chkbbin', DBId=DBId, Status='【手机APP登入】', Progress='1/1', Detail=Detail)
            # 檢查登出或連線異常, 回傳後讓主程式重試
            if result_login_record['ErrorCode'] == config.SIGN_OUT_CODE.code:
                return result_login_record
            if result_login_record['ErrorCode'] == config.CONNECTION_CODE.code:
                return result_login_record
            # 查詢失敗結束
            if result_login_record['ErrorCode'] != config.SUCCESS_CODE.code:
                return result_login_record['ErrorMessage']
            rtn['AppLogin'] = AppLogin

        # 回傳結果
        return rtn


class apploginbonus(BaseFunc):
    '''BBIN App登入禮'''

    @classmethod
    @keep_connect
    def _audit(cls, url, timeout, cf, mod_key=None, VIPcategory ='版本1.5以前',
               AuditAPP=1, AuditMobilesite=0, AuditUniversal=0, AuditUniversalPc=0, AuditUniversalApp=0,
               AuditCustomizationApp=0, ImportColumns=["Member", "BlockMemberLayer", "LoginDateTime", "VipLevel", ],
               **kwargs):
        '''
            BBIN App登入禮
            需要有【帐号管理 >> 层级管理】权限。
            需要有【帐号管理 >> 帐号列表】权限。
            需要有【帐号管理 >> 帐号列表 >> 會員詳細 >> 金流資訊】权限。
            需要有【登入/注册查询 >> 手机登入】权限。
            需要有【登入/注册查询 >> 寰宇浏览器】权限。
        '''
        # 【設定查詢範圍】
        # 判斷新舊版
        # 新版(1.3.0版以後)
        logger.info(f'VIPcategory={VIPcategory}')

        if "VipLevel" in ImportColumns:
            last_data = None
            EndDate = datetime.datetime.now(tz=pytz.timezone('America/New_York'))
            p = Path(f'config/data') / (mod_key or '.') / 'BBIN'
            if not p.exists():
                p.mkdir()
            p = [f for f in p.iterdir() if f.suffix == '.csv']
            if not p:
                StartDate = EndDate.strftime(r'%Y-%m-%d 00:00:00')
            else:
                p = max(p, key=lambda x: x.stem)
                lines = [
                    line.split(',')[3]
                    for line in p.read_text(encoding='utf8').split('\n')[1:]
                    if line]
                if lines:
                    last_data = max(lines + [
                        EndDate.strftime(r'%Y-%m-%d 00:00:00')
                    ])
                StartDate = last_data or EndDate.strftime(r'%Y-%m-%d 00:00:00')
            # last_data = last_data or f'{StartDate} 00:00:00'
            last_data = last_data or StartDate
            logger.info(last_data)
            # 判斷是否需查找10分鐘內的資料
            thirtydata = datetime.datetime.strptime(last_data, r'%Y-%m-%d %H:%M:%S')
            thirtydata = thirtydata + datetime.timedelta(minutes=10)
            if thirtydata.timestamp() < EndDate.timestamp():
                EndDate = thirtydata
            else:
                EndDate = EndDate.strftime(r'%Y-%m-%d %H:%M:%S')

        # 舊版(1.3.0版以前)
        else:
            last_data = None
            EndDate = datetime.datetime.now(tz=pytz.timezone('America/New_York'))
            p = Path(f'config/data') / (mod_key or '.') / 'BBIN'
            if not p.exists():
                p.mkdir()
            p = [f for f in p.iterdir() if f.suffix == '.csv']
            if not p:
                StartDate = EndDate.strftime(r'%Y-%m-%d 00:00:00')
            else:
                p = max(p, key=lambda x: x.stem)
                lines = [
                    line.split(',')[2]
                    for line in p.read_text(encoding='utf8').split('\n')[1:]
                    if line]
                if lines:
                    last_data = max(lines + [
                        EndDate.strftime(r'%Y-%m-%d 00:00:00')
                    ])
                StartDate = last_data or EndDate.strftime(r'%Y-%m-%d 00:00:00')

            last_data = last_data or f'{StartDate} 00:00:00'
            EndDate = EndDate.strftime(r'%Y-%m-%d %H:%M:%S')
        content = {}
        # 【查詢APP登入紀錄】
        if int(AuditAPP):
            total_page = 1
            params = {
                'page': 1,
                'Role': '1',
                'LoginSource': 'APP',
                'Username': '',
                'StartDate': StartDate,
                'EndDate': EndDate,
                'show': '100',
            }
            while total_page >= params['page']:
                result_login_record = bbin.login_record_info_mobile(
                    cf, url, params=params, endpoints='user/login_record_info/mobile')
                logger.info(
                    f'BBIN APP登入纪录 第{params["page"]:2d}頁 '
                    f'時間區間: {StartDate} - {EndDate} '
                    f'{result_login_record["ErrorCode"]} '
                    f'{result_login_record["ErrorMessage"]}')
                content = {
                    **content,
                    **{x['帐号']: x for x in result_login_record.get('Data', {}).get('content', [])},
                }

                # 檢查登出或連線異常, 回傳後讓主程式重試
                if result_login_record['ErrorCode'] == config.SIGN_OUT_CODE.code:
                    return result_login_record
                if result_login_record['ErrorCode'] == config.CONNECTION_CODE.code:
                    return result_login_record
                # 查詢失敗結束
                if result_login_record['ErrorCode'] != config.SUCCESS_CODE.code:
                    return result_login_record['ErrorMessage']
                if not content:
                    break
                # 查到小於前次最後時間時停止
                first_data = min(content.values(), key=lambda c: c['最后登入时间(美东时间)'])['最后登入时间(美东时间)']
                if first_data and first_data < last_data:
                    content = {k: v for k, v in content.items() if v['最后登入时间(美东时间)'] >= last_data}
                    break
                # 紀錄頁數
                params['page'] += 1
                total_page = result_login_record['Data']['total_page']
        # 【查詢APPWEB登入紀錄】
        if int(AuditMobilesite):
            total_page = 1
            params = {
                'page': 1,
                'Role': '1',
                'LoginSource': 'MOBILEWEB',
                'Username': '',
                'StartDate': StartDate,
                'EndDate': EndDate,
                'show': '100',
            }
            while total_page >= params['page']:
                result_login_record = bbin.login_record_info_mobile(
                    cf, url, params=params, endpoints='user/login_record_info/mobile')
                logger.info(
                    f'BBIN APP_WEB登入纪录 第{params["page"]:2d}頁 '
                    f'時間區間: {StartDate} - {EndDate} '
                    f'{result_login_record["ErrorCode"]} '
                    f'{result_login_record["ErrorMessage"]}')
                content = {
                    **content,
                    **{x['帐号']: x for x in result_login_record.get('Data', {}).get('content', [])},
                }

                # 檢查登出或連線異常, 回傳後讓主程式重試
                if result_login_record['ErrorCode'] == config.SIGN_OUT_CODE.code:
                    return result_login_record
                if result_login_record['ErrorCode'] == config.CONNECTION_CODE.code:
                    return result_login_record
                # 查詢失敗結束
                if result_login_record['ErrorCode'] != config.SUCCESS_CODE.code:
                    return result_login_record['ErrorMessage']
                if not content:
                    break
                # 查到小於前次最後時間時停止
                first_data = min(content.values(), key=lambda c: c['最后登入时间(美东时间)'])['最后登入时间(美东时间)']
                if first_data and first_data < last_data:
                    content = {k: v for k, v in content.items() if v['最后登入时间(美东时间)'] >= last_data}
                    break
                # 紀錄頁數
                params['page'] += 1
                total_page = result_login_record['Data']['total_page']

        # 【查詢UB全部登入紀錄，AuditUniversal後台1.4.0版後改成不傳，以防後台為舊版，留此參數】
        if int(AuditUniversal):
            total_page = 1
            params = {
                'page': 1,
                'Role': '1',
                'LoginSource': 'ALL_UB',
                'Username': '',
                'StartDate': StartDate,
                'EndDate': EndDate,
                'show': '100',
            }
            while total_page >= params['page']:
                result_login_record = bbin.login_record_info_mobile(
                    cf, url, params=params, endpoints='user/login_record_info/ub')
                logger.info(
                    f'BBIN UB登入纪录 第{params["page"]:2d}頁 '
                    f'時間區間: {StartDate} - {EndDate} '
                    f'{result_login_record["ErrorCode"]} '
                    f'{result_login_record["ErrorMessage"]}')
                content = {
                    **content,
                    **{x['帐号']: x for x in result_login_record.get('Data', {}).get('content', [])},
                }

                # 檢查登出或連線異常, 回傳後讓主程式重試
                if result_login_record['ErrorCode'] == config.SIGN_OUT_CODE.code:
                    return result_login_record
                if result_login_record['ErrorCode'] == config.CONNECTION_CODE.code:
                    return result_login_record
                # 查詢失敗結束
                if result_login_record['ErrorCode'] != config.SUCCESS_CODE.code:
                    return result_login_record['ErrorMessage']
                if not content:
                    break
                # 查到小於前次最後時間時停止
                first_data = min(content.values(), key=lambda c: c['最后登入时间(美东时间)'])['最后登入时间(美东时间)']
                if first_data and first_data < last_data:
                    content = {k: v for k, v in content.items() if v['最后登入时间(美东时间)'] >= last_data}
                    break
                # 紀錄頁數
                params['page'] += 1
                total_page = int(result_login_record['Data']['total_page'])
        # 【查詢PC_UB登入紀錄】
        if int(AuditUniversalPc):
            total_page = 1
            params = {
                'page': 1,
                'Role': '1',
                'LoginSource': 'PC_UB',
                'Username': '',
                'StartDate': StartDate,
                'EndDate': EndDate,
                'show': '100',
            }
            while total_page >= params['page']:
                result_login_record = bbin.login_record_info_mobile(
                    cf, url, params=params, endpoints='user/login_record_info/ub')
                logger.info(
                    f'BBIN PC_UB登入纪录 第{params["page"]:2d}頁 '
                    f'時間區間: {StartDate} - {EndDate} '
                    f'{result_login_record["ErrorCode"]} '
                    f'{result_login_record["ErrorMessage"]}')
                content = {
                    **content,
                    **{x['帐号']: x for x in result_login_record.get('Data', {}).get('content', [])},
                }

                # 檢查登出或連線異常, 回傳後讓主程式重試
                if result_login_record['ErrorCode'] == config.SIGN_OUT_CODE.code:
                    return result_login_record
                if result_login_record['ErrorCode'] == config.CONNECTION_CODE.code:
                    return result_login_record
                # 查詢失敗結束
                if result_login_record['ErrorCode'] != config.SUCCESS_CODE.code:
                    return result_login_record['ErrorMessage']
                if not content:
                    break
                # 查到小於前次最後時間時停止
                first_data = min(content.values(), key=lambda c: c['最后登入时间(美东时间)'])['最后登入时间(美东时间)']
                if first_data and first_data < last_data:
                    content = {k: v for k, v in content.items() if v['最后登入时间(美东时间)'] >= last_data}
                    break
                # 紀錄頁數
                params['page'] += 1
                total_page = int(result_login_record['Data']['total_page'])
        # 【查詢APP_UB登入紀錄】
        if int(AuditUniversalApp):
            total_page = 1
            params = {
                'page': 1,
                'Role': '1',
                'LoginSource': 'APP_UB',
                'Username': '',
                'StartDate': StartDate,
                'EndDate': EndDate,
                'show': '100',
            }
            while total_page >= params['page']:
                result_login_record = bbin.login_record_info_mobile(
                    cf, url, params=params, endpoints='user/login_record_info/ub')
                logger.info(
                    f'BBIN APP_UB登入纪录 第{params["page"]:2d}頁 '
                    f'時間區間: {StartDate} - {EndDate} '
                    f'{result_login_record["ErrorCode"]} '
                    f'{result_login_record["ErrorMessage"]}')
                content = {
                    **content,
                    **{x['帐号']: x for x in result_login_record.get('Data', {}).get('content', [])},
                }

                # 檢查登出或連線異常, 回傳後讓主程式重試
                if result_login_record['ErrorCode'] == config.SIGN_OUT_CODE.code:
                    return result_login_record
                if result_login_record['ErrorCode'] == config.CONNECTION_CODE.code:
                    return result_login_record
                # 查詢失敗結束
                if result_login_record['ErrorCode'] != config.SUCCESS_CODE.code:
                    return result_login_record['ErrorMessage']
                if not content:
                    break
                # 查到小於前次最後時間時停止
                first_data = min(content.values(), key=lambda c: c['最后登入时间(美东时间)'])['最后登入时间(美东时间)']
                if first_data and first_data < last_data:
                    content = {k: v for k, v in content.items() if v['最后登入时间(美东时间)'] >= last_data}
                    break
                # 紀錄頁數
                params['page'] += 1
                total_page = int(result_login_record['Data']['total_page'])
        # 【查詢APP_UB_CUSTOM登入紀錄】
        if int(AuditCustomizationApp):
            total_page = 1
            params = {
                'page': 1,
                'Role': '1',
                'LoginSource': 'APP_UB_CUSTOM',
                'Username': '',
                'StartDate': StartDate,
                'EndDate': EndDate,
                'show': '100',
            }
            while total_page >= params['page']:
                result_login_record = bbin.login_record_info_mobile(
                    cf, url, params=params, endpoints='user/login_record_info/ub')
                logger.info(
                    f'BBIN APP_UB_CUSTOM 第{params["page"]:2d}頁 '
                    f'時間區間: {StartDate} - {EndDate} '
                    f'{result_login_record["ErrorCode"]} '
                    f'{result_login_record["ErrorMessage"]}')
                content = {
                    **content,
                    **{x['帐号']: x for x in result_login_record.get('Data', {}).get('content', [])},
                }

                # 檢查登出或連線異常, 回傳後讓主程式重試
                if result_login_record['ErrorCode'] == config.SIGN_OUT_CODE.code:
                    return result_login_record
                if result_login_record['ErrorCode'] == config.CONNECTION_CODE.code:
                    return result_login_record
                # 查詢失敗結束
                if result_login_record['ErrorCode'] != config.SUCCESS_CODE.code:
                    return result_login_record['ErrorMessage']
                if not content:
                    break
                # 查到小於前次最後時間時停止
                first_data = min(content.values(), key=lambda c: c['最后登入时间(美东时间)'])['最后登入时间(美东时间)']
                if first_data and first_data < last_data:
                    content = {k: v for k, v in content.items() if v['最后登入时间(美东时间)'] >= last_data}
                    break
                # 紀錄頁數
                params['page'] += 1
                total_page = int(result_login_record['Data']['total_page'])
        lencontent = len(content)
        logger.info(f'content的長度{lencontent}')
        # 【查詢層級】
        grouped_users = [
            [x for i, x in xs]
            for g, xs in groupby(
                enumerate(content.keys()),
                key=lambda ix: ix[0] // 1000
            )
        ]
        for users in grouped_users:
            result_member = bbin.agv3_cl(cf, url,
                                         params={'module': 'Level', 'method': 'searchMemList'},
                                         data={'Users': ','.join(users)},
                                         timeout=timeout
                                         )
            # 檢查登出或連線異常, 回傳後讓主程式重試
            if result_member['ErrorCode'] == config.SIGN_OUT_CODE.code:
                return result_member
            if result_member['ErrorCode'] == config.CONNECTION_CODE.code:
                return result_member
            # 查詢失敗結束
            if result_member['ErrorCode'] != config.SUCCESS_CODE.code:
                return result_member['ErrorMessage']
            # 將查詢結果製作成dict
            levels = {select['level_id']: select for select in result_member.get('Data', {}).get('select', [])}
            users = {user['user_name']: user['level_id'] for user in result_member.get('Data', {}).get('user_list', [])}
            for user_name, level_id in users.items():
                content[user_name]['BlockMemberLayer'] = levels[level_id]['alias']

        # 累積存款金額、次數
        if 'CumulativeDepositAmount' in ImportColumns:
            # 權限檢查 (通過後保存sid於cls中，當重登或重啟後才會再次進行權限檢查)
            if not hasattr(cls, 'sid') or cls.sid != bbin.session.cookies.get('sid'):
                result_permissions = bbin.users_detail_permission(cf=cf, url=url, headers={
                    'permname': 'UserDetailInfo',
                    'referer': url + f'vi/user/{123}/detail_info'
                })
                account_permissions = {p['name'] for p in result_permissions.get('Data', {}).get('permissions', [])}
                # 檢查登出或連線異常, 回傳後讓主程式重試
                if result_permissions['ErrorCode'] == config.SIGN_OUT_CODE.code:
                    return result_permissions
                if result_permissions['ErrorCode'] == config.CONNECTION_CODE.code:
                    return result_permissions
                # 查詢失敗結束
                if result_permissions['ErrorCode'] != config.SUCCESS_CODE.code:
                    return result_permissions['ErrorMessage']
                # 權限不足結束
                if 'UserDetailInfo' not in account_permissions:
                    return f'【BBIN >> 會員詳細資訊】 權限未開啟。'
            cls.sid = bbin.session.cookies.get('sid')

            # 查詢所有會員ID
            grouped_users = [
                [x for i, x in xs]
                for g, xs in groupby(
                    enumerate(content.keys()),
                    key=lambda ix: ix[0] // 10
                )
            ]
            dic_users = {}
            for users in grouped_users:
                result_member = bbin.user_list(cf, url,
                                               params={
                                                   'Status': 'All',
                                                   'role': 'All',
                                                   'dispensing': 'all',
                                                   'Batch': '1',
                                                   'SearchField': 'username',
                                                   'SearchValue': '\n'.join(users),
                                               }
                                               )

                # 檢查登出或連線異常, 回傳後讓主程式重試
                if result_member['ErrorCode'] == config.SIGN_OUT_CODE.code:
                    return result_member
                if result_member['ErrorCode'] == config.CONNECTION_CODE.code:
                    return result_member
                # 查詢失敗結束
                if result_member['ErrorCode'] != config.SUCCESS_CODE.code:
                    return result_member['ErrorMessage']

                dic_users = {
                    **dic_users,
                    **{username: user['userid'] for username, user in result_member.get('Data', {}).items()}
                }

            lendic_user = len(dic_users)
            logger.info(f'dic_user的長度{lendic_user}')
            # 查詢累積存款金額、次數
            for username, userid in dic_users.items():
                # 【會員詳細資料】 - 【累積存提款】
                result_deposit_and_withdraw = bbin.deposit_and_withdraw_info(cf, url,
                                                                             endpoints=f'hex/user/{userid}/deposit_and_withdraw/info',
                                                                             headers={
                                                                                 'permname': 'UserDetailInfo',
                                                                                 'referer': url + f'vi/user/{userid}/detail_info'
                                                                             }
                                                                             )
                content[username][
                    'CumulativeDepositsTimes'] = f"{int(result_deposit_and_withdraw.get('Data', {}).get('deposit_count') or 0):.0f}"
                content[username][
                    'CumulativeDepositAmount'] = f"{float(result_deposit_and_withdraw.get('Data', {}).get('deposit_amount') or 0):.2f}"
                # 檢查登出或連線異常, 回傳後讓主程式重試
                if result_deposit_and_withdraw['ErrorCode'] == config.SIGN_OUT_CODE.code:
                    return result_deposit_and_withdraw
                if result_deposit_and_withdraw['ErrorCode'] == config.CONNECTION_CODE.code:
                    return result_deposit_and_withdraw
                # 查詢失敗結束
                if result_deposit_and_withdraw['ErrorCode'] != config.SUCCESS_CODE.code:
                    return result_deposit_and_withdraw['ErrorMessage']
        if 'VipLevel' in ImportColumns:
            # 權限檢查 (通過後保存sid於cls中，當重登或重啟後才會再次進行權限檢查)
            if not hasattr(cls, 'sid') or cls.sid != bbin.session.cookies.get('sid'):
                result_permissions = bbin.users_detail_permission(cf=cf, url=url, headers={
                    'permname': 'UserDetailInfo',
                    'referer': url + f'vi/user/{123}/detail_info'
                })
                account_permissions = {p['name'] for p in result_permissions.get('Data', {}).get('permissions', [])}
                # 檢查登出或連線異常, 回傳後讓主程式重試
                if result_permissions['ErrorCode'] == config.SIGN_OUT_CODE.code:
                    return result_permissions
                if result_permissions['ErrorCode'] == config.CONNECTION_CODE.code:
                    return result_permissions
                # 查詢失敗結束
                if result_permissions['ErrorCode'] != config.SUCCESS_CODE.code:
                    return result_permissions['ErrorMessage']
                # 權限不足結束
                if 'UserDetailInfo' not in account_permissions:
                    return f'【BBIN >> 會員詳細資訊】 權限未開啟。'
            cls.sid = bbin.session.cookies.get('sid')

            # 查詢VIP等級

            for username in grouped_users:
                # 【會員層級】 - 【查詢VIP等級】
                result_users_vip_level = bbin.users_vip_level(cf, url, VIPcategory,
                                                              params={
                                                                  'account': ','.join(username),
                                                                  'vipId[]': 'all',
                                                                  'period': 'current',
                                                                  'hallId': '3820036',
                                                                  'category': 'all',
                                                                  'page': '1',
                                                                  'categoryType': 'general',
                                                              }
                                                              )
                # 檢查登出或連線異常, 回傳後讓主程式重試
                if result_users_vip_level['ErrorCode'] == config.SIGN_OUT_CODE.code:
                    return result_users_vip_level
                if result_users_vip_level['ErrorCode'] == config.CONNECTION_CODE.code:
                    return result_users_vip_level
                # 查詢失敗結束
                if result_users_vip_level['ErrorCode'] != config.SUCCESS_CODE.code:
                    return result_users_vip_level['ErrorMessage']
                if result_users_vip_level['ErrorCode'] == config.HTML_CONTENT_CODE.code:
                    return result_users_vip_level['ErrorMessage']

                for name, vip in result_users_vip_level['Data'].items():
                    content[name]['VipLevel'] = vip['VipLevel']

        content = [
            {
                'Member': username,
                'BlockMemberLayer': user.get('BlockMemberLayer', ''),
                'VipLevel': user.get('VipLevel', ''),
                'LoginDateTime': user.get('最后登入时间(美东时间)', ''),
                'CumulativeDepositAmount': user.get('CumulativeDepositAmount', '0.00'),
                'CumulativeDepositsTimes': user.get('CumulativeDepositsTimes', '0'),
            }
            for username, user in content.items()
        ]
        # 成功回傳
        return {
            'IsSuccess': 1,
            'ErrorCode': config.SUCCESS_CODE.code,
            'ErrorMessage': config.SUCCESS_CODE.msg,
            'Data': [
                [user[col] for col in ImportColumns]
                for user in content
            ]
        }

    @classmethod
    @keep_connect
    def test_audit(cls, url, timeout, cf, pr6_time, pr6_zone, platform_time, mod_key=None, VIPcategory='版本1.5以前',
                   AuditAPP=1, AuditMobilesite=0, AuditUniversal=0, AuditUniversalPc=0, AuditUniversalApp=0,
                   AuditCustomizationApp=0, ImportColumns=["Member", "BlockMemberLayer", "LoginDateTime", "VipLevel", ],
                   **kwargs):
        '''
            BBIN App登入禮
            需要有【帐号管理 >> 层级管理】权限。
            需要有【帐号管理 >> 帐号列表】权限。
            需要有【帐号管理 >> 帐号列表 >> 會員詳細 >> 金流資訊】权限。
            需要有【登入/注册查询 >> 手机登入】权限。
            需要有【登入/注册查询 >> 寰宇浏览器】权限。
        '''
        # 【設定查詢範圍】
        # 判斷新舊版
        # 新版(1.3.0版以後)

        platform_time -= datetime.timedelta(hours=12)  # 平台時間的美東時間等於北京時間-12小時
        platform_time = platform_time.replace(tzinfo=None)
        logger.info(
            f'設定檔{cf}\n VIPcategory = {VIPcategory} \n pr6--時區:{pr6_zone} 時間:{pr6_time}\n 平台時間(美東):{platform_time}')
        pr6_time_standard = datetime.datetime.strptime(pr6_time,
                                                       r'%Y-%m-%d %H:%M:%S')  # 設一個維持在時間型態的變數 判別平台時間是否跟PR6時間保持一致
        time_loss = 0  # 時間誤差值
        loss_range = datetime.timedelta(minutes=3)  # 時間誤差值標準為三分鐘
        if platform_time >= pr6_time_standard:
            time_loss = platform_time - pr6_time_standard
        else:
            time_loss = pr6_time_standard - platform_time
        logger.info(f'平台時間與pr6時間誤差:{time_loss}')
        if time_loss > loss_range:  # 誤差時間超過3分鐘 錯誤彈窗
            logger.warning(f'警告!!平台時間與pr6時間誤差:{time_loss}!! 誤差標準範圍為:{loss_range}')
            return (f'平台时间与pr6时间误差大于三分钟')

        p = Path(f'config/data') / (mod_key or '.') / 'BBIN'  # 若沒有資料夾就先創一個放csv紀錄的資料夾
        if not p.exists():
            p.mkdir()

        if cf.last_read_time and cf.last_read_time != '':  # 如果有輸入上次讀取時間
            if not isinstance(cf.last_read_time, datetime.datetime):
                cf.last_read_time = datetime.datetime.strptime(cf.last_read_time, r'%Y-%m-%d %H:%M:%S')

            logger.info(f'上次讀取時間{cf.last_read_time}')
        else:  # 如果沒有 用PR6時間0:00分 轉為時間格式
            if not isinstance(pr6_time, datetime.datetime):
                first_time = datetime.datetime.strptime(pr6_time, r'%Y-%m-%d %H:%M:%S')
                first_time = first_time.strftime(r'%Y-%m-%d 00:00:00')  # 因為字串型態才能轉00:00:00分
                cf.last_read_time = datetime.datetime.strptime(first_time, r'%Y-%m-%d %H:%M:%S')
            else:
                first_time = pr6_time.strftime(r'%Y-%m-%d 00:00:00')
                cf.last_read_time = datetime.datetime.strptime(first_time, r'%Y-%m-%d %H:%M:%S')
            logger.info(f'無上次讀取時間--系統預設PR6日期00:00分開始 {cf.last_read_time}')

        if not isinstance(pr6_time, datetime.datetime):  # pr6轉成時間格式 才能對時間做比較
            pr6_time = datetime.datetime.strptime(pr6_time, r'%Y-%m-%d %H:%M:%S')
        cross_day = cf.last_read_time.strftime(r'%Y-%m-%d 23:59:59')  # 判斷增加10分鐘有無跨日
        cross_day = datetime.datetime.strptime(cross_day, r'%Y-%m-%d %H:%M:%S')

        logger.info(f'當下pr6時間:{pr6_time}')  # 要做時間加減都要用datetime型態
        # 使用者輸入時間或是系統預設00:00

        if (cf.last_read_time + datetime.timedelta(minutes=10)) > pr6_time:  # 如果起始時間加10分鐘大於現在時間 結束時間等於現在時間 結束時間等於起始時間+10分鐘
            EndDate = pr6_time

        elif (cf.last_read_time + datetime.timedelta(minutes=10)) > cross_day:  # 判斷加10分鐘有無跨日
            if cf.last_read_time != cross_day:  # 如果上次讀取時間沒有等於23:59:59  抓到23:59:59
                EndDate = cross_day
            else:
                cf.last_read_time += datetime.timedelta(seconds=1)  # 如果剛好等於23:59:59 就幫他+一秒
                EndDate = cf.last_read_time + datetime.timedelta(minutes=10)
        else:
            EndDate = cf.last_read_time + datetime.timedelta(minutes=10)  # 往下抓10分鐘

        StartDate = cf.last_read_time

        content = {}

        if not isinstance(cf.last_read_time, str):
            cf.last_read_time = cf.last_read_time.strftime(r'%Y-%m-%d %H:%M:%S')

        if not isinstance(StartDate, str):  # 檢查開始時間 結束時間是否為str型態
            StartDate = StartDate.strftime(r'%Y-%m-%d %H:%M:%S')

        if not isinstance(EndDate, str):
            EndDate = EndDate.strftime(r'%Y-%m-%d %H:%M:%S')

        logger.info(f'bbin查詢時間需要輸入str類型--StartDate型別為:{type(StartDate)} EndDate型別為:{type(EndDate)}')
        logger.info(f'開始查詢時間{StartDate}, 結束時間{EndDate}')
        # 【查詢APP登入紀錄】
        if int(AuditAPP):
            total_page = 1
            params = {
                'page': 1,
                'Role': '1',
                'LoginSource': 'APP',
                'Username': '',
                'StartDate': StartDate,
                'EndDate': EndDate,
                'show': '100',
            }
            while total_page >= params['page']:
                result_login_record = bbin.login_record_info_mobile(
                    cf, url, params=params, endpoints='user/login_record_info/mobile')
                logger.info(
                    f'BBIN APP登入纪录 第{params["page"]:2d}頁 '
                    f'時間區間: {StartDate} - {EndDate} '
                    f'{result_login_record["ErrorCode"]} '
                    f'{result_login_record["ErrorMessage"]}')
                content = {
                    **content,
                    **{x['帐号']: x for x in result_login_record.get('Data', {}).get('content', [])},
                }

                # 檢查登出或連線異常, 回傳後讓主程式重試
                if result_login_record['ErrorCode'] == config.SIGN_OUT_CODE.code:
                    return result_login_record
                if result_login_record['ErrorCode'] == config.CONNECTION_CODE.code:
                    return result_login_record
                # 查詢失敗結束
                if result_login_record['ErrorCode'] != config.SUCCESS_CODE.code:
                    return result_login_record['ErrorMessage']
                if not content:
                    break
                # 查到小於前次最後時間時停止
                # first_data = min(content.values(), key=lambda c: c['最后登入时间(美东时间)'])['最后登入时间(美东时间)']
                # if first_data and first_data < last_data:
                #    content = {k: v for k, v in content.items() if v['最后登入时间(美东时间)'] >= last_data}
                #    break
                # 紀錄頁數
                params['page'] += 1
                total_page = result_login_record['Data']['total_page']
        # 【查詢APPWEB登入紀錄】

        if int(AuditMobilesite):
            total_page = 1
            params = {
                'page': 1,
                'Role': '1',
                'LoginSource': 'MOBILEWEB',
                'Username': '',
                'StartDate': StartDate,
                'EndDate': EndDate,
                'show': '100',
            }
            while total_page >= params['page']:
                result_login_record = bbin.login_record_info_mobile(
                    cf, url, params=params, endpoints='user/login_record_info/mobile')
                logger.info(
                    f'BBIN APP_WEB登入纪录 第{params["page"]:2d}頁 '
                    f'時間區間: {StartDate} - {EndDate} '
                    f'{result_login_record["ErrorCode"]} '
                    f'{result_login_record["ErrorMessage"]}')
                content = {
                    **content,
                    **{x['帐号']: x for x in result_login_record.get('Data', {}).get('content', [])},
                }

                # 檢查登出或連線異常, 回傳後讓主程式重試
                if result_login_record['ErrorCode'] == config.SIGN_OUT_CODE.code:
                    return result_login_record
                if result_login_record['ErrorCode'] == config.CONNECTION_CODE.code:
                    return result_login_record
                # 查詢失敗結束
                if result_login_record['ErrorCode'] != config.SUCCESS_CODE.code:
                    return result_login_record['ErrorMessage']
                if not content:
                    break
                # 查到小於前次最後時間時停止
                # first_data = min(content.values(), key=lambda c: c['最后登入时间(美东时间)'])['最后登入时间(美东时间)']
                # if first_data and first_data < last_data:
                #    content = {k: v for k, v in content.items() if v['最后登入时间(美东时间)'] >= last_data}
                #    break
                # 紀錄頁數
                params['page'] += 1
                total_page = result_login_record['Data']['total_page']

        # 【查詢UB全部登入紀錄，AuditUniversal後台1.4.0版後改成不傳，以防後台為舊版，留此參數】
        if int(AuditUniversal):
            total_page = 1
            params = {
                'page': 1,
                'Role': '1',
                'LoginSource': 'ALL_UB',
                'Username': '',
                'StartDate': StartDate,
                'EndDate': EndDate,
                'show': '100',
            }
            while total_page >= params['page']:
                result_login_record = bbin.login_record_info_mobile(
                    cf, url, params=params, endpoints='user/login_record_info/ub')
                logger.info(
                    f'BBIN UB登入纪录 第{params["page"]:2d}頁 '
                    f'時間區間: {StartDate} - {EndDate} '
                    f'{result_login_record["ErrorCode"]} '
                    f'{result_login_record["ErrorMessage"]}')
                content = {
                    **content,
                    **{x['帐号']: x for x in result_login_record.get('Data', {}).get('content', [])},
                }

                # 檢查登出或連線異常, 回傳後讓主程式重試
                if result_login_record['ErrorCode'] == config.SIGN_OUT_CODE.code:
                    return result_login_record
                if result_login_record['ErrorCode'] == config.CONNECTION_CODE.code:
                    return result_login_record
                # 查詢失敗結束
                if result_login_record['ErrorCode'] != config.SUCCESS_CODE.code:
                    return result_login_record['ErrorMessage']
                if not content:
                    break
                # 查到小於前次最後時間時停止
                # first_data = min(content.values(), key=lambda c: c['最后登入时间(美东时间)'])['最后登入时间(美东时间)']
                # if first_data and first_data < last_data:
                #    content = {k: v for k, v in content.items() if v['最后登入时间(美东时间)'] >= last_data}
                #    break
                # 紀錄頁數
                params['page'] += 1
                total_page = int(result_login_record['Data']['total_page'])
        # 【查詢PC_UB登入紀錄】
        if int(AuditUniversalPc):
            total_page = 1
            params = {
                'page': 1,
                'Role': '1',
                'LoginSource': 'PC_UB',
                'Username': '',
                'StartDate': StartDate,
                'EndDate': EndDate,
                'show': '100',
            }
            while total_page >= params['page']:
                result_login_record = bbin.login_record_info_mobile(
                    cf, url, params=params, endpoints='user/login_record_info/ub')
                logger.info(
                    f'BBIN PC_UB登入纪录 第{params["page"]:2d}頁 '
                    f'時間區間: {StartDate} - {EndDate} '
                    f'{result_login_record["ErrorCode"]} '
                    f'{result_login_record["ErrorMessage"]}')
                content = {
                    **content,
                    **{x['帐号']: x for x in result_login_record.get('Data', {}).get('content', [])},
                }

                # 檢查登出或連線異常, 回傳後讓主程式重試
                if result_login_record['ErrorCode'] == config.SIGN_OUT_CODE.code:
                    return result_login_record
                if result_login_record['ErrorCode'] == config.CONNECTION_CODE.code:
                    return result_login_record
                # 查詢失敗結束
                if result_login_record['ErrorCode'] != config.SUCCESS_CODE.code:
                    return result_login_record['ErrorMessage']
                if not content:
                    break
                # 查到小於前次最後時間時停止
                # first_data = min(content.values(), key=lambda c: c['最后登入时间(美东时间)'])['最后登入时间(美东时间)']
                # if first_data and first_data < last_data:
                #    content = {k: v for k, v in content.items() if v['最后登入时间(美东时间)'] >= last_data}
                #    break
                # 紀錄頁數
                params['page'] += 1
                total_page = int(result_login_record['Data']['total_page'])
        # 【查詢APP_UB登入紀錄】
        if int(AuditUniversalApp):
            total_page = 1
            params = {
                'page': 1,
                'Role': '1',
                'LoginSource': 'APP_UB',
                'Username': '',
                'StartDate': StartDate,
                'EndDate': EndDate,
                'show': '100',
            }
            while total_page >= params['page']:
                result_login_record = bbin.login_record_info_mobile(
                    cf, url, params=params, endpoints='user/login_record_info/ub')
                logger.info(
                    f'BBIN APP_UB登入纪录 第{params["page"]:2d}頁 '
                    f'時間區間: {StartDate} - {EndDate} '
                    f'{result_login_record["ErrorCode"]} '
                    f'{result_login_record["ErrorMessage"]}')
                content = {
                    **content,
                    **{x['帐号']: x for x in result_login_record.get('Data', {}).get('content', [])},
                }

                # 檢查登出或連線異常, 回傳後讓主程式重試
                if result_login_record['ErrorCode'] == config.SIGN_OUT_CODE.code:
                    return result_login_record
                if result_login_record['ErrorCode'] == config.CONNECTION_CODE.code:
                    return result_login_record
                # 查詢失敗結束
                if result_login_record['ErrorCode'] != config.SUCCESS_CODE.code:
                    return result_login_record['ErrorMessage']
                if not content:
                    break
                # 查到小於前次最後時間時停止
                # first_data = min(content.values(), key=lambda c: c['最后登入时间(美东时间)'])['最后登入时间(美东时间)']
                # if first_data and first_data < last_data:
                #    content = {k: v for k, v in content.items() if v['最后登入时间(美东时间)'] >= last_data}
                #    break
                # 紀錄頁數
                params['page'] += 1
                total_page = int(result_login_record['Data']['total_page'])
        # 【查詢APP_UB_CUSTOM登入紀錄】
        if int(AuditCustomizationApp):
            total_page = 1
            params = {
                'page': 1,
                'Role': '1',
                'LoginSource': 'APP_UB_CUSTOM',
                'Username': '',
                'StartDate': StartDate,
                'EndDate': EndDate,
                'show': '100',
            }
            while total_page >= params['page']:
                result_login_record = bbin.login_record_info_mobile(
                    cf, url, params=params, endpoints='user/login_record_info/ub')
                logger.info(
                    f'BBIN APP_UB_CUSTOM 第{params["page"]:2d}頁 '
                    f'時間區間: {StartDate} - {EndDate} '
                    f'{result_login_record["ErrorCode"]} '
                    f'{result_login_record["ErrorMessage"]}')
                content = {
                    **content,
                    **{x['帐号']: x for x in result_login_record.get('Data', {}).get('content', [])},
                }

                # 檢查登出或連線異常, 回傳後讓主程式重試
                if result_login_record['ErrorCode'] == config.SIGN_OUT_CODE.code:
                    return result_login_record
                if result_login_record['ErrorCode'] == config.CONNECTION_CODE.code:
                    return result_login_record
                # 查詢失敗結束
                if result_login_record['ErrorCode'] != config.SUCCESS_CODE.code:
                    return result_login_record['ErrorMessage']
                if not content:
                    break
                # 查到小於前次最後時間時停止
                # first_data = min(content.values(), key=lambda c: c['最后登入时间(美东时间)'])['最后登入时间(美东时间)']
                # if first_data and first_data < last_data:
                #    content = {k: v for k, v in content.items() if v['最后登入时间(美东时间)'] >= last_data}
                #    break
                # 紀錄頁數
                params['page'] += 1
                total_page = int(result_login_record['Data']['total_page'])
        lencontent = len(content)
        logger.info(f'APP登入筆數:{lencontent}')
        # 【查詢層級】
        grouped_users = [
            [x for i, x in xs]
            for g, xs in groupby(
                enumerate(content.keys()),
                key=lambda ix: ix[0] // 1000
            )
        ]
        for users in grouped_users:
            result_member = bbin.agv3_cl(cf, url,
                                         params={'module': 'Level', 'method': 'searchMemList'},
                                         data={'Users': ','.join(users)},
                                         timeout=timeout
                                         )
            # 檢查登出或連線異常, 回傳後讓主程式重試
            if result_member['ErrorCode'] == config.SIGN_OUT_CODE.code:
                return result_member
            if result_member['ErrorCode'] == config.CONNECTION_CODE.code:
                return result_member
            # 查詢失敗結束
            if result_member['ErrorCode'] != config.SUCCESS_CODE.code:
                return result_member['ErrorMessage']
            # 將查詢結果製作成dict
            levels = {select['level_id']: select for select in result_member.get('Data', {}).get('select', [])}
            users = {user['user_name']: user['level_id'] for user in result_member.get('Data', {}).get('user_list', [])}
            for user_name, level_id in users.items():
                content[user_name]['BlockMemberLayer'] = levels[level_id]['alias']

        # 累積存款金額、次數
        if 'CumulativeDepositAmount' in ImportColumns:
            # 權限檢查 (通過後保存sid於cls中，當重登或重啟後才會再次進行權限檢查)
            if not hasattr(cls, 'sid') or cls.sid != bbin.session.cookies.get('sid'):
                result_permissions = bbin.users_detail_permission(cf=cf, url=url, headers={
                    'permname': 'UserDetailInfo',
                    'referer': url + f'vi/user/{123}/detail_info'
                })
                account_permissions = {p['name'] for p in result_permissions.get('Data', {}).get('permissions', [])}
                # 檢查登出或連線異常, 回傳後讓主程式重試
                if result_permissions['ErrorCode'] == config.SIGN_OUT_CODE.code:
                    return result_permissions
                if result_permissions['ErrorCode'] == config.CONNECTION_CODE.code:
                    return result_permissions
                # 查詢失敗結束
                if result_permissions['ErrorCode'] != config.SUCCESS_CODE.code:
                    return result_permissions['ErrorMessage']
                # 權限不足結束
                if 'UserDetailInfo' not in account_permissions:
                    return f'【BBIN >> 會員詳細資訊】 權限未開啟。'
            cls.sid = bbin.session.cookies.get('sid')

            # 查詢所有會員ID
            grouped_users = [
                [x for i, x in xs]
                for g, xs in groupby(
                    enumerate(content.keys()),
                    key=lambda ix: ix[0] // 10
                )
            ]
            dic_users = {}
            for users in grouped_users:
                result_member = bbin.user_list(cf, url,
                                               params={
                                                   'Status': 'All',
                                                   'role': 'All',
                                                   'dispensing': 'all',
                                                   'Batch': '1',
                                                   'SearchField': 'username',
                                                   'SearchValue': '\n'.join(users),
                                               }
                                               )

                # 檢查登出或連線異常, 回傳後讓主程式重試
                if result_member['ErrorCode'] == config.SIGN_OUT_CODE.code:
                    return result_member
                if result_member['ErrorCode'] == config.CONNECTION_CODE.code:
                    return result_member
                # 查詢失敗結束
                if result_member['ErrorCode'] != config.SUCCESS_CODE.code:
                    return result_member['ErrorMessage']

                dic_users = {
                    **dic_users,
                    **{username: user['userid'] for username, user in result_member.get('Data', {}).items()}
                }

            lendic_user = len(dic_users)
            logger.info(f'查詢到會員資料筆數:{lendic_user}')
            # 查詢累積存款金額、次數
            for username, userid in dic_users.items():
                # 【會員詳細資料】 - 【累積存提款】
                result_deposit_and_withdraw = bbin.deposit_and_withdraw_info(cf, url,
                                                                             endpoints=f'hex/user/{userid}/deposit_and_withdraw/info',
                                                                             headers={
                                                                                 'permname': 'UserDetailInfo',
                                                                                 'referer': url + f'vi/user/{userid}/detail_info'
                                                                             }
                                                                             )
                content[username][
                    'CumulativeDepositsTimes'] = f"{int(result_deposit_and_withdraw.get('Data', {}).get('deposit_count') or 0):.0f}"
                content[username][
                    'CumulativeDepositAmount'] = f"{float(result_deposit_and_withdraw.get('Data', {}).get('deposit_amount') or 0):.2f}"
                # 檢查登出或連線異常, 回傳後讓主程式重試
                if result_deposit_and_withdraw['ErrorCode'] == config.SIGN_OUT_CODE.code:
                    return result_deposit_and_withdraw
                if result_deposit_and_withdraw['ErrorCode'] == config.CONNECTION_CODE.code:
                    return result_deposit_and_withdraw
                # 查詢失敗結束
                if result_deposit_and_withdraw['ErrorCode'] != config.SUCCESS_CODE.code:
                    return result_deposit_and_withdraw['ErrorMessage']
            logger.info('---查詢累積存款金額完畢---')
        if 'VipLevel' in ImportColumns:
            # 權限檢查 (通過後保存sid於cls中，當重登或重啟後才會再次進行權限檢查)
            if not hasattr(cls, 'sid') or cls.sid != bbin.session.cookies.get('sid'):
                result_permissions = bbin.users_detail_permission(cf=cf, url=url, headers={
                    'permname': 'UserDetailInfo',
                    'referer': url + f'vi/user/{123}/detail_info'
                })
                account_permissions = {p['name'] for p in result_permissions.get('Data', {}).get('permissions', [])}
                # 檢查登出或連線異常, 回傳後讓主程式重試
                if result_permissions['ErrorCode'] == config.SIGN_OUT_CODE.code:
                    return result_permissions
                if result_permissions['ErrorCode'] == config.CONNECTION_CODE.code:
                    return result_permissions
                # 查詢失敗結束
                if result_permissions['ErrorCode'] != config.SUCCESS_CODE.code:
                    return result_permissions['ErrorMessage']
                # 權限不足結束
                if 'UserDetailInfo' not in account_permissions:
                    return f'【BBIN >> 會員詳細資訊】 權限未開啟。'
            cls.sid = bbin.session.cookies.get('sid')

            # 查詢VIP等級

            for username in grouped_users:
                # 【會員層級】 - 【查詢VIP等級】
                result_users_vip_level = bbin.users_vip_level(cf, url, VIPcategory,
                                                              params={
                                                                  'account': ','.join(username),
                                                                  'vipId[]': 'all',
                                                                  'period': 'current',
                                                                  'hallId': '3820036',
                                                                  'category': 'all',
                                                                  'page': '1',
                                                                  'categoryType': 'general',
                                                              }
                                                              )
                # 檢查登出或連線異常, 回傳後讓主程式重試
                if result_users_vip_level['ErrorCode'] == config.SIGN_OUT_CODE.code:
                    return result_users_vip_level
                if result_users_vip_level['ErrorCode'] == config.CONNECTION_CODE.code:
                    return result_users_vip_level
                # 查詢失敗結束
                if result_users_vip_level['ErrorCode'] != config.SUCCESS_CODE.code:
                    return result_users_vip_level['ErrorMessage']
                if result_users_vip_level['ErrorCode'] == config.HTML_CONTENT_CODE.code:
                    return result_users_vip_level['ErrorMessage']

                for name, vip in result_users_vip_level['Data'].items():
                    content[name]['VipLevel'] = vip['VipLevel']
        logger.info(f'查詢完的所有會員資料:{content}\n')
        content = [
            {
                'Member': username,
                'BlockMemberLayer': user.get('BlockMemberLayer', ''),
                'VipLevel': user.get('VipLevel', ''),
                'LoginDateTime': user.get('最后登入时间(美东时间)', ''),
                'CumulativeDepositAmount': user.get('CumulativeDepositAmount', '0.00'),
                'CumulativeDepositsTimes': user.get('CumulativeDepositsTimes', '0'),
            }
            for username, user in content.items()
        ]
        # 成功回傳
        if cf.last_read_time == StartDate:
            cf.last_read_time = EndDate  # 若使用者沒有輸入新的值 下一次起始時間從上一次抓取結束時間開始抓取
        logger.info(f'bbin查詢時間需要輸入str型別--StartDate的型別為:{type(StartDate)}  EndDate的型別為:{type(EndDate)}')
        logger.info(f'此次結束 查詢範圍為:{StartDate} ~ {EndDate}, 最後讀取時間為:{cf.last_read_time}')

        return {
            'IsSuccess': 1,
            'ErrorCode': config.SUCCESS_CODE.code,
            'ErrorMessage': config.SUCCESS_CODE.msg,
            'Data': [
                [user[col] for col in ImportColumns]
                for user in content
            ]
        }

    @classmethod
    def deposit(cls, *args, **kwargs):
        '''
            BBIN 充值、推播通知
            需要有【现金系统 >> 现金系统】、【现金系统 >> BB现金系统】权限。
            需要有【会员讯息管理 >> 新增一般讯息】权限。
        '''
        kwargs['SupportStatus'] = 1
        cls.th = ThreadProgress(kwargs['cf'], kwargs['mod_key'], detail=bool(kwargs['SupportStatus']))
        cls.th.start()

        result = cls._deposit(*args, **kwargs)

        cls.return_schedule(Action='chkpoint', DBId=kwargs['DBId'], Status='【充值机器人处理完毕】', Progress='-', Detail='-')
        cls.th.stop()
        return result

    @classmethod
    @keep_connect
    def _deposit(cls, url, DBId, Member, DepositAmount, NotifyAllowReply, NotifyTitle, NotifyContent,
        mod_name, mod_key, timeout, backend_remarks, multiple, cf,
        amount_memo='', Note='', increasethebet_switch=0, increasethebet='0', **kwargs):
        '''BBIN App登入禮 充值、推播通知'''
        member = Member.lower()
        # 【充值】
        result_deposit = super().deposit(url, DBId, Member, DepositAmount, mod_name, mod_key, 
            timeout, backend_remarks, multiple, cf, amount_memo, Note, increasethebet_switch, increasethebet, **kwargs)
        if type(result_deposit) == str:
            return result_deposit
        if not result_deposit['IsSuccess']:
            return {**result_deposit, 'NotifyMessage': ''}
        if not any([int(NotifyAllowReply), NotifyTitle, NotifyContent]):
            return {**result_deposit, 'NotifyMessage': ''}

        count = 1
        while True:
            # 【推播通知】 - 【会员查询】
            result_member = bbin.agv3_cl(cf, url,
                params={'module': 'Level', 'method': 'searchMemList'},
                data={'Users': Member},
                timeout=timeout
            )
            # 整理查詢結果
            users = {user['user_name']: user for user in result_member.get('Data', {}).get('user_list', [])}
            userid = users.get(member, {}).get("user_id", '')
            if not result_member["IsSuccess"]:
                Detail = f'查询失敗\n{result_member["ErrorMessage"]}'
            else:
                Detail = f'查询成功\n会员是否存在：{"是" if userid else "否"}'
            cls.return_schedule(Action='chkpoint', DBId=DBId, Status='【推播通知】 - 【会员查询】', Progress=f'{count}/{count}', Detail=Detail)
            # 檢查登出或連線異常, 回傳後讓主程式重試
            if result_member['ErrorCode'] == config.SIGN_OUT_CODE.code:
                return {
                    'IsSuccess': 1,
                    'ErrorCode': result_deposit['ErrorCode'],
                    'ErrorMessage': result_deposit['ErrorMessage'],
                    'DBId': DBId,
                    'Member': Member,
                    'NotifyMessage': config.UserMessageError.msg.format(platform=cf.platform, msg='推播通知被登出，无法推播通知'),
                }
            if result_member['ErrorCode'] == config.CONNECTION_CODE.code:
                time.sleep(1)
                continue
            # 查詢失敗結束
            if result_member['ErrorCode'] != config.SUCCESS_CODE.code:
                return result_member['ErrorMessage']
            
            # 【推播通知】 - 【新增推播】
            startTime = datetime.datetime.now(tz=pytz.timezone('America/New_York'))
            #startTime = datetime.datetime.now(tz=pytz.timezone('Asia/Taipei'))
            #startTime = datetime.datetime.fromtimestamp(startTime.timestamp() - 43200)
            #startTime += datetime.timedelta(days=1)
            endTime = startTime + datetime.timedelta(days=1)
            endTime = endTime.strftime(r'%Y-%m-%d 00:00:00')
            startTime = startTime.strftime(r'%Y-%m-%d 00:00:00')
            logger.info(f'推播時間: {{{startTime}}} {{{endTime}}}')
            result_add = bbin.user_msg_add(cf, url,
                data={
                    'hallId': '',                                           # 未知, BBIN本身即帶空值
                    'type': '1',                                            # 发布对象, 0:體系,2:層級
                    'replySwitch': 'Y' if int(NotifyAllowReply) else 'N',        # 开放会员回覆
                    'startTime': startTime,                                 # 发布时间(美东)-開始日期
                    'endTime': endTime,                                     # 发布时间(美东)-結束日期
                    'title': json.dumps({'zh-tw': NotifyTitle, 'zh-cn': NotifyTitle, 'en': NotifyTitle, 'vi': '', 'ko': ''}),
                    'content': json.dumps({'zh-tw': NotifyContent, 'zh-cn': NotifyContent, 'en': NotifyContent, 'vi': '', 'ko': ''}),
                },
                timeout=cf.timeout
                )
            #時區修正
            if '请输入正确开始日期' in result_add.get('Data').get('message'):
                startTime = datetime.datetime.now(tz=pytz.timezone('Asia/Taipei'))
                #startTime = datetime.datetime.now(tz=pytz.timezone('America/New_York'))
                endTime = startTime + datetime.timedelta(days=1)
                endTime = endTime.strftime(r'%Y-%m-%d 00:00:00')
                startTime = startTime.strftime(r'%Y-%m-%d 00:00:00')
                logger.info(f'推播時間更正: {{{startTime}}} {{{endTime}}}')
                
                result_add = bbin.user_msg_add(cf, url,
                data={
                    'hallId': '',                                           # 未知, BBIN本身即帶空值
                    'type': '1',                                            # 发布对象, 0:體系,2:層級
                    'replySwitch': 'Y' if int(NotifyAllowReply) else 'N',        # 开放会员回覆
                    'startTime': startTime,                                 # 发布时间(美东)-開始日期
                    'endTime': endTime,                                     # 发布时间(美东)-結束日期
                    'title': json.dumps({'zh-tw': NotifyTitle, 'zh-cn': NotifyTitle, 'en': NotifyTitle, 'vi': '', 'ko': ''}),
                    'content': json.dumps({'zh-tw': NotifyContent, 'zh-cn': NotifyContent, 'en': NotifyContent, 'vi': '', 'ko': ''}),
                },
                timeout=cf.timeout
                )
            msgid = result_add.get('Data', {}).get('data', {}).get('msgId', '')
            if not result_add["IsSuccess"]:
                Detail = f'新增推播失敗\n{result_add["ErrorMessage"]}'
            else:
                Detail = f'新增推播成功'
            cls.return_schedule(Action='chkpoint', DBId=DBId, Status='【推播通知】 - 【新增推播】', Progress=f'{count}/{count}', Detail=Detail)
            # 檢查登出或連線異常, 回傳後讓主程式重試
            if result_add['ErrorCode'] == config.SIGN_OUT_CODE.code:
                return {
                    'IsSuccess': 1,
                    'ErrorCode': result_deposit['ErrorCode'],
                    'ErrorMessage': result_deposit['ErrorMessage'],
                    'DBId': DBId,
                    'Member': Member,
                    'NotifyMessage': result_add['ErrorMessage'],
                }
            if result_add['ErrorCode'] == config.CONNECTION_CODE.code:
                time.sleep(1)
                continue
            # 查詢失敗結束
            if result_add['ErrorCode'] != config.SUCCESS_CODE.code:
                return {
                    'IsSuccess': 1,
                    'ErrorCode': result_deposit['ErrorCode'],
                    'ErrorMessage': result_deposit['ErrorMessage'],
                    'DBId': DBId,
                    'Member': Member,
                    'NotifyMessage': result_add['ErrorMessage'],
                }

            # 【推播通知】 - 【發布推播】
            result_send = bbin.user_msg_send(cf, url,
                data={
                    'hallId': '',
                    'msgId': msgid,
                    'recipient': json.dumps({"1":[userid],"2":[],"3":[],"4":[],"5":[],"6":[],"7":[]}),
                },
                timeout=cf.timeout
            )
            if not result_send["IsSuccess"]:
                Detail = f'發布推播失敗\n{result_send["ErrorMessage"]}'
            else:
                Detail = f'發布推播成功'
            cls.return_schedule(Action='chkpoint', DBId=DBId, Status='【推播通知】 - 【發布推播】', Progress=f'{count}/{count}', Detail=Detail)
            # 檢查登出或連線異常, 回傳後讓主程式重試
            if result_send['ErrorCode'] == config.SIGN_OUT_CODE.code:
                return {
                    'IsSuccess': 1,
                    'ErrorCode': result_deposit['ErrorCode'],
                    'ErrorMessage': result_deposit['ErrorMessage'],
                    'DBId': DBId,
                    'Member': Member,
                    'NotifyMessage': result_send['ErrorMessage'],
                }
            if result_send['ErrorCode'] == config.CONNECTION_CODE.code:
                time.sleep(1)
                continue
            # 查詢失敗結束
            if result_send['ErrorCode'] != config.SUCCESS_CODE.code:
                return {
                    'IsSuccess': 1,
                    'ErrorCode': result_deposit['ErrorCode'],
                    'ErrorMessage': result_deposit['ErrorMessage'],
                    'DBId': DBId,
                    'Member': Member,
                    'NotifyMessage': result_send['ErrorMessage'],
                }

            return {
                'IsSuccess': 1,
                'ErrorCode': result_deposit['ErrorCode'],
                'ErrorMessage': result_deposit['ErrorMessage'],
                'DBId': DBId,
                'Member': Member,
                'NotifyMessage': '',
            }


class registerbonus(BaseFunc):
    '''BBIN 註冊紅包'''
    @classmethod
    def deposit(cls, *args, **kwargs):
        '''
            BBIN 充值
            需要有【现金系统 >> 现金系统】、【现金系统 >> BB现金系统】权限。
        '''
        kwargs['SupportStatus'] = 1
        cls.th = ThreadProgress(kwargs['cf'], kwargs['mod_key'], detail=bool(kwargs['SupportStatus']))
        cls.th.start()

        result = cls._deposit(*args, **kwargs)

        cls.return_schedule(Action='chkpoint', DBId=kwargs['DBId'], Status='【充值机器人处理完毕】', Progress='-', Detail='-')
        cls.th.stop()
        return result

    @classmethod
    @keep_connect
    def _deposit(cls, url, DBId, Member, DepositAmount, mod_name, mod_key,
        timeout, backend_remarks, multiple, cf, amount_memo='', Note='', increasethebet_switch=0, increasethebet='0', **kwargs):
        '''BBIN 註冊紅包 充值'''

        return super().deposit(url, DBId, Member, DepositAmount, mod_name, mod_key, 
            timeout, backend_remarks, multiple, cf, amount_memo, Note, increasethebet_switch, increasethebet, **kwargs)

    @classmethod
    def audit(cls, *args, **kwargs):
        '''
            BBIN 註冊紅包
            需要有【登入纪录 >> 自动稽核】权限。
            需要有【会员列表 >> 帐号列表】权限。
            需要有【层级管理 >> 帐号查询】权限。
            需要有【会员详细资料 >> 金流资讯 >> 真实姓名】权限。
            需要有【会员详细资料 >> 金流资讯 >> 银行帐户资讯】权限。
            需要有【会员详细资料 >> 金流资讯 >> 存提款资讯】权限。
        '''
        cls.th = ThreadProgress(kwargs['cf'], kwargs['mod_key'])
        cls.th.start()

        result = cls._audit(*args, **kwargs)

        cls.return_schedule(Action='chkbbin', DBId=kwargs['DBId'], Status='【审核机器人处理完毕】', Progress='-', Detail='-')
        cls.th.stop()
        return result

    @classmethod
    @keep_connect
    def _audit(cls, url, DBId, Member, SearchDate, AuditDays, timeout, cf, **kwargs):
        '''BBIN 註冊紅包 監控'''
        member = Member.lower()
        # 【查詢會員帳號】
        result_member = bbin.agv3_cl(cf, url,
            params={'module': 'Level', 'method': 'searchMemList'},
            data={'Users': Member},
            timeout=timeout
        )
        # 整理查詢結果
        levels = {select['level_id']: select for select in result_member.get('Data', {}).get('select', [])}
        users = {user['user_name']: user for user in result_member.get('Data', {}).get('user_list', [])}
        userid = users.get(member, {}).get('user_id')
        user_levelid = users.get(member, {}).get('level_id', '')
        create_time = users.get(member, {}).get('create_time', '')
        BlockMemberLayer = levels.get(user_levelid, {}).get('alias', '')
        if not result_member["IsSuccess"]:
            Detail = f'查询失败\n搜寻帐号：{Member}\n{result_member["ErrorMessage"]}'
        else:
            Detail = f'查询成功\n搜寻帐号：{Member}\n会员是否存在：{"是" if member in users else "否"}'
            if member in users:
                Detail += f'\n会员注册时间：{create_time}'
                Detail += f'\n会员层级：{BlockMemberLayer}'
        cls.return_schedule(Action='chkbbin', DBId=DBId, Status='【会员列表】 - 【帐号列表】', Progress=f'1/1', Detail=Detail)
        # 檢查登出或連線異常, 回傳後讓主程式重試
        if result_member['ErrorCode'] == config.SIGN_OUT_CODE.code:
            return result_member
        if result_member['ErrorCode'] == config.CONNECTION_CODE.code:
            return result_member
        # 查詢失敗結束
        if result_member['ErrorCode'] != config.SUCCESS_CODE.code:
            return result_member['ErrorMessage']
        # 查詢無會員回傳
        if member not in users:
            return {
                'IsSuccess': 0,
                'ErrorCode': config.NO_USER.code,
                'ErrorMessage': config.NO_USER.msg,
                'BlockMemberLayer': '',
                'DBId': DBId,
                'Member': Member,
                'RealName': '',
                'BankAccountExists': 0,
                'AutoAudit': 0,
                'CumulativeDepositsTimes': '0',
                'CumulativeDepositAmount': '0.00',
            }
        create_time = datetime.datetime.strptime(f'{create_time} -0400', r'%Y-%m-%d %H:%M:%S %z')

        # 權限檢查 (通過後保存sid於cls中，當重登或重啟後才會再次進行權限檢查)
        if not hasattr(cls, 'sid') or cls.sid != bbin.session.cookies.get('sid'):
            result_permissions = bbin.users_detail_permission(cf=cf, url=url, headers={
                    'permname': 'UserDetailInfo',
                    'referer': url + f'vi/user/{userid}/detail_info'
            })
            account_permissions = {p['name'] for p in result_permissions.get('Data', {}).get('permissions', [])}
            need_permissions = {'GetAccount': '银行帐户资讯', 'NameReal': '真实姓名'}
            miss_permissions = need_permissions.keys() - account_permissions
            if not result_permissions["IsSuccess"]:
                Detail = f'查询失败\n{result_permissions["ErrorMessage"]}'
            elif miss_permissions:
                Detail = f'查询失败\n缺少权限：{"、".join([need_permissions[p] for p in miss_permissions])}'
            else:
                Detail = f'查询成功'
            cls.return_schedule(Action='chkbbin', DBId=DBId, Status='【会员详细资料】', Progress=f'1/1', Detail=Detail)
            # 檢查登出或連線異常, 回傳後讓主程式重試
            if result_permissions['ErrorCode'] == config.SIGN_OUT_CODE.code:
                return result_permissions
            if result_permissions['ErrorCode'] == config.CONNECTION_CODE.code:
                return result_permissions
            # 查詢失敗結束
            if result_permissions['ErrorCode'] != config.SUCCESS_CODE.code:
                return result_permissions['ErrorMessage']
            # 權限不足結束
            if 'UserDetailInfo' not in account_permissions:
                return f'【BBIN >> 會員詳細資訊】 權限未開啟。'
            if miss_permissions:
                return f'【BBIN >> 會員詳細資訊 >> {"、".join([need_permissions[p] for p in miss_permissions])}】 權限未開啟。'
        cls.sid = bbin.session.cookies.get('sid')

        # 【會員詳細資料】 - 【真實姓名】
        result_realname = bbin.users_detail(cf, url,
            endpoints=f'hex/user/{userid}/detail',
            headers={
                'permname': 'UserDetailInfo',
                'referer': url + f'vi/user/{userid}/detail_info'
            }
        )
        real_name = result_realname.get('Data', {}).get('name_real', '')
        if not result_realname["IsSuccess"]:
            Detail = f'查询失败\n{result_realname["ErrorMessage"]}'
        else:
            Detail = f'查询成功\n真实姓名为：{real_name}'
        cls.return_schedule(Action='chkbbin', DBId=DBId, Status='【会员详细资料】 - 【真实姓名】', Progress=f'1/1', Detail=Detail)
        # 檢查登出或連線異常, 回傳後讓主程式重試
        if result_realname['ErrorCode'] == config.SIGN_OUT_CODE.code:
            return result_realname
        if result_realname['ErrorCode'] == config.CONNECTION_CODE.code:
            return result_realname
        # 查詢失敗結束
        if result_realname['ErrorCode'] != config.SUCCESS_CODE.code:
            return result_realname['ErrorMessage']

        # 【會員詳細資料】 - 【綁定銀行卡】
        result_bank_account = bbin.users_bank_account(cf, url, 
            params={'users[]': [userid]},
            headers={
                'permname': 'UserDetailInfo',
                'referer': url + f'vi/user/{userid}/detail_info'
            }
        )
        bank_account_exists = int(bool(result_bank_account.get('Data', {}).get(userid, {}).get('bank')))
        if not result_bank_account["IsSuccess"]:
            Detail = f'查询失败\n{result_bank_account["ErrorMessage"]}'
        else:
            Detail = f'查询成功\n是否绑定银行卡：{"是" if bank_account_exists else "否"}'
        cls.return_schedule(Action='chkbbin', DBId=DBId, Status='【会员详细资料】 - 【绑定银行卡】', Progress=f'1/1', Detail=Detail)
        # 檢查登出或連線異常, 回傳後讓主程式重試
        if result_bank_account['ErrorCode'] == config.SIGN_OUT_CODE.code:
            return result_bank_account
        if result_bank_account['ErrorCode'] == config.CONNECTION_CODE.code:
            return result_bank_account
        # 查詢失敗結束
        if result_bank_account['ErrorCode'] != config.SUCCESS_CODE.code:
            return result_bank_account['ErrorMessage']

        # 【會員詳細資料】 - 【累積存提款】
        result_deposit_and_withdraw = bbin.deposit_and_withdraw_info(cf, url,
            endpoints=f'hex/user/{userid}/deposit_and_withdraw/info',
            headers={
                'permname': 'UserDetailInfo',
                'referer': url + f'vi/user/{userid}/detail_info'
            }
        )
        CumulativeDepositsTimes = int(result_deposit_and_withdraw.get('Data', {}).get('deposit_count') or 0)
        CumulativeDepositAmount = float(result_deposit_and_withdraw.get('Data', {}).get('deposit_amount') or 0)
        if not result_deposit_and_withdraw["IsSuccess"]:
            Detail = f'查询失败\n{result_deposit_and_withdraw["ErrorMessage"]}'
        else:
            Detail = f'查询成功'
            Detail += f'\n累积存款次数：{CumulativeDepositsTimes}\n累积存款金额：{CumulativeDepositAmount:.2f}'
        cls.return_schedule(Action='chkbbin', DBId=DBId, Status='【會員詳細資料】 - 【累積存提款】', Progress=f'1/1', Detail=Detail)
        # 檢查登出或連線異常, 回傳後讓主程式重試
        if result_deposit_and_withdraw['ErrorCode'] == config.SIGN_OUT_CODE.code:
            return result_deposit_and_withdraw
        if result_deposit_and_withdraw['ErrorCode'] == config.CONNECTION_CODE.code:
            return result_deposit_and_withdraw
        # 查詢失敗結束
        if result_deposit_and_withdraw['ErrorCode'] != config.SUCCESS_CODE.code:
            return result_deposit_and_withdraw['ErrorMessage']

        # 【自動稽核】
        EndDate = datetime.datetime.strptime(SearchDate, r'%Y/%m/%d')
        StartDate = EndDate - datetime.timedelta(days=int(AuditDays))
        EndDate = EndDate.strftime(r'%Y-%m-%d')
        StartDate = StartDate.strftime(r'%Y-%m-%d')
        total_page = 1
        params = {
            'AccName': member,
            'StartDate': StartDate,
            'EndDate': EndDate,
            'show': '100',
            'page': 1,
        }
        content = []
        while total_page>=params['page']:
            result_record_auto = bbin.login_record_info_auto(cf, url, params=params)
            content += result_record_auto.get('Data', {}).get('content', [])
            auto_audit = int(len({r['帐号'] for r in content}) > 1)
            if not result_record_auto["IsSuccess"]:
                Detail = f'查询失败\n{result_record_auto["ErrorMessage"]}'
            else:
                Detail = f'查询成功\n查询到的帐号列表：{list(set(r["帐号"] for r in content))}\n查询到的IP列表：{list(set(r["IP位置"] for r in content))}'
                Detail += f'\n是否有多帳號同IP：{"是" if auto_audit else "否"}'
            cls.return_schedule(Action='chkbbin', DBId=DBId, Status='【自动稽核】', Progress=f'{params["page"]}/{total_page}', Detail=Detail)
            # 檢查登出或連線異常, 回傳後讓主程式重試
            if result_record_auto['ErrorCode'] == config.SIGN_OUT_CODE.code:
                return result_record_auto
            if result_record_auto['ErrorCode'] == config.CONNECTION_CODE.code:
                return result_record_auto
            # 查詢失敗結束
            if result_record_auto['ErrorCode'] != config.SUCCESS_CODE.code:
                return result_record_auto['ErrorMessage']
            params['page'] += 1
            total_page = result_record_auto['Data']['total_page']

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
            'CumulativeDepositsTimes': f'{float(CumulativeDepositsTimes):.0f}',
            'CumulativeDepositAmount': f'{float(CumulativeDepositAmount):.2f}',
        }
