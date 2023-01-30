import logging
from platforms.config import CODE_DICT as config
from . import module as cd
from .utils import (
    ThreadProgress,
    spin_betslip_gamedict,
    spin_betslip_gametype,
    keep_connect
)
import datetime
import time
import pytz
import json
from collections import Counter
from pathlib import Path
import csv


logger = logging.getLogger('robot')


class BaseFunc:
    class Meta:
        extra = {}
        # 需要檢查的系統傳入參數
        system_dict = {
                'DBId':str, 'Member':str, 'DepositAmount':str,
                'SearchGameCategory':list, 'SearchDate':str,
                'RawWagersId':str, 'ExtendLimit':str, 'GameCountType':str
                }

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
    def deposit(cls, url, DBId, Member, DepositAmount, mod_name, mod_key, amount_below,
        timeout, backend_remarks, multiple, cf, amount_memo='', Note='', increasethebet_switch=0, increasethebet='0', **kwargs):
        '''
        CD 充值
        需要有【会员管理 > 会员列表】权限。
        需要有【会员管理 > 会员列表 > 用户名 > 会员资料 > 人工存入】权限。
        需要有【报表管理 > 额度管理】权限。
        '''
        SupportStatus = kwargs.get('SupportStatus')

        if int(increasethebet_switch):
            logger.info(f'●使用PR6流水倍數：{str(increasethebet)}')
            multipleA = str(increasethebet)
        else:
            logger.info(f'●使用機器人打碼量：{str(multiple)}')
            multipleA = str(multiple)

        #判斷充值金額
        if not float(DepositAmount) <= float(amount_below):
            Detail = f'充值失敗\n{config.AMOUNT_CODE.msg}'
            cls.return_schedule(Action='chkpoint', DBId=DBId, Member=Member, Status='【判斷充值金額】', Progress='1/1', Detail=Detail)

            # 查詢失敗直接回傳
            return {
                'IsSuccess': 0,
                'ErrorCode': config.AMOUNT_CODE.code,
                'ErrorMessage': config.AMOUNT_CODE.msg,
                'DBId': DBId,
                'Member': Member,
            }
        else:
            Detail = '充值金额符合自动出款金额'
            cls.return_schedule(Action='chkpoint', DBId=DBId, Member=Member, Status='【判斷充值金額】', Progress='1/1', Detail=Detail)

        #【会员列表】-【会员资料】
        user_result = cd.searchusername(cf, url, data={'usernameArr': f'["{Member}"]'}, timeout=timeout)
        users = {u['username'].lower(): u for u in user_result.get('Data', {}).get('results', [])}
        userid = users.get(Member.lower(), {}).get('id')
        if not user_result["IsSuccess"]:
            Detail = f'查询失敗\n搜寻帐号：{Member}\n{user_result["ErrorMessage"]}'
        else:
            Detail = f'查询成功\n搜寻帐号：{Member}\n会员是否存在：{"是" if userid else "否"}'
            if userid:
                Detail += f'\n会员ID：{userid}'

        if SupportStatus:
            cls.return_schedule(Action='chkpoint', DBId=DBId, Status='【会员列表】-【会员资料】', Progress=f'1/1', Detail=Detail)
        else:
            cls.return_schedule(Action='chkpoint', DBId=DBId, Member=Member, current_status='【会员列表】-【会员资料】')


        # 檢查登出或連線異常, 回傳後讓主程式重試
        if user_result["ErrorCode"] == config.CONNECTION_CODE.code:
            return user_result
        if user_result['ErrorCode'] == config.SIGN_OUT_CODE.code:
            return user_result
        # 查詢失敗結束
        if user_result["ErrorCode"] != config.SUCCESS_CODE.code:
            return user_result["ErrorMessage"]

        # 查詢無會員回傳
        if not userid:
            return {
                'IsSuccess': 0,
                'ErrorCode': config.NO_USER.code,
                'ErrorMessage': config.NO_USER.msg,
                'DBId': DBId,
                'Member': Member
            }

        #【充值】
        amount_memo = amount_memo or f'{mod_name}({mod_key}-{DBId}){Note}'
        if backend_remarks and not amount_memo.endswith(backend_remarks):
            amount_memo += f'：{backend_remarks}'
        result_deposit = cd.depositsave(cf=cf,
                                    url=url,
                                    data={
                                        'member_ids': userid,
                                        'amount': DepositAmount,  # 充入金額
                                        'audit_method': '1',  # 存款稽核
                                        # 'audit_times': str(cf['multiple']), #稽核倍率
                                        'audit_times': multipleA,  # 流水倍數
                                        'deposit_type': '2',  # 類型:優惠
                                        'is_actual': '0',
                                        'player_remark': backend_remarks,
                                        'admin_remark': amount_memo,
                                        'is_affiliate': '0',
                                        'securityType': '1'
                                    }, timeout=timeout)
        msg = result_deposit.get('Data', {}).get('message', '')
        if not result_deposit["IsSuccess"]:
            Detail = f'充值失敗\n{result_deposit["ErrorMessage"]}'
        elif msg != '新增成功':
            Detail = f'充值失敗\n{msg}'
        else:
            Detail = f'充值成功'

        if SupportStatus:
            cls.return_schedule(Action='chkpoint', DBId=DBId, Status='【会员资料】-【人工存入】', Progress=f'1/1', Detail=Detail)
        else:
            cls.return_schedule(Action='chkpoint', DBId=DBId, Member=Member, current_status='【会员资料】-【人工存入】')

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

        #【充值失敗再驗證】
        if not result_deposit.get('Data') or result_deposit['Data'].get('message') != '新增成功':
            dt = datetime.datetime.fromtimestamp(time.time(), tz=pytz.timezone('Asia/Shanghai'))
            start_time = (dt + datetime.timedelta(days=-1)).strftime('%Y-%m-%d') + ' ' + '00:00:00'
            end_time = (dt + datetime.timedelta(days=1)).strftime('%Y-%m-%d') + ' ' + '23:59:59'
            date = start_time + ' - ' + end_time
            start = 100
            count = 0
            while True:
                result_check = cd.creditflowmanagementdata(cf=cf, url=url, params={
                                                                                'start': str(start * count),
                                                                                'length':start,
                                                                                'search_date': date,
                                                                                'search_usernames': userid,
                                                                                }, timeout=timeout)
                Progress = f'{str(start * count)}/{len(result_check["Data"]["data"])}'

                if SupportStatus:
                    cls.return_schedule(Action='chkpoint', DBId=DBId, Status='【报表管理】-【额度管理】', Progress=Progress, Detail='复查充值结果')
                else:
                    cls.return_schedule(Action='chkpoint', DBId=DBId, Member=Member, current_status='【报表管理】-【额度管理】')

                if result_check.get('Data'):
                    record = [i for i in result_check['Data']['data'] if i['details'] == amount_memo]
                    if record:
                        result_deposit['IsSuccess'] = result_check['IsSuccess']
                        result_deposit['ErrorCode'] = result_check['ErrorCode']
                        result_deposit['ErrorMessage'] = result_check['ErrorMessage']
                        # break
                    # elif not result_check['Data']['data']:
                        # break
                    else:
                        result_deposit['IsSuccess'] = 0
                        result_deposit['ErrorCode'] = config.REPEAT_DEPOSIT.code
                        result_deposit['ErrorMessage'] = config.REPEAT_DEPOSIT.msg.format(platform=cf.platform, msg=result_deposit["ErrorMessage"] or msg)
                    # else:
                    #     count += 1 #進行下一個迴圈
                    #     time.sleep(1)
                    #     continue
                elif result_check['ErrorCode'] == config.SIGN_OUT_CODE.code: #判斷被登出
                    return result_check
                else: #其他錯誤原因一律直接做下一筆
                    result_deposit['IsSuccess'] = 0
                    result_deposit['ErrorCode'] = config.IGNORE_CODE.code
                    result_deposit['ErrorMessage'] = config.IGNORE_CODE.msg
                    # return {
                    #     'IsSuccess': 0,
                    #     'ErrorCode': config.IGNORE_CODE.code,
                    #     'ErrorMessage': config.IGNORE_CODE.msg,
                    # }
                break

        #回傳結果
        return {
            'IsSuccess': int(result_deposit['IsSuccess']),
            'ErrorCode': result_deposit['ErrorCode'],
            'ErrorMessage': result_deposit['ErrorMessage'],
            'DBId': DBId,
            'Member': Member
        }


class hongbao(BaseFunc):
    '''CD 红包'''

class passthrough(BaseFunc):
    '''CD 闯关'''

class happy7days(BaseFunc):
    '''CD 七天乐'''

class pointsbonus(BaseFunc):
    '''CD 积分红包'''
    @classmethod
    def deposit(cls, *args, **kwargs):
        '''
        CD 充值
        需要有【会员管理 > 会员列表】权限。
        需要有【会员管理 > 会员列表 > 用户名 > 会员资料 > 人工存入】权限。
        需要有【会员管理 > 会员列表 > 会员資料 > 額度管理】权限。
        '''
        kwargs['SupportStatus'] = 1
        cls.th = ThreadProgress(kwargs['cf'], kwargs['mod_key'])
        cls.th.start()

        result = cls._deposit(*args, **kwargs)

        cls.return_schedule(Action='chkpoint', DBId=kwargs['DBId'], Status='【充值机器人处理完毕】', Progress='-', Detail='-')
        cls.th.stop()
        return result

    @classmethod
    def _deposit(cls, url, DBId, Member, DepositAmount, mod_name, mod_key, amount_below,
        timeout, backend_remarks, multiple, cf, amount_memo='', Note='', increasethebet_switch=0, increasethebet='0', **kwargs):
        '''CD 积分红包 充值'''
        return super().deposit(url, DBId, Member, DepositAmount, mod_name, mod_key, amount_below,
            timeout, backend_remarks, multiple, cf, amount_memo, Note, increasethebet_switch, increasethebet, **kwargs)


class promo_old(BaseFunc):
    '''CD 活动大厅'''
    # class Meta:
    #     extra = {}
    #     return_value = {'data':['DBId','Member','BlockMemberLayer'],'include':{},'exclude':[]}

    @classmethod
    def audit(cls, url, DBId, Member, timeout, cf, **kwargs):
        '''
        《CD 活動大廳》
        需要有【会员管理 > 会员列表】权限。
        '''
        # if len(Member) < 4 or len(Member) > 16:
        #     return {
        #         'IsSuccess': 0,
        #         'ErrorCode': config.ACCOUNT_COUNT_ERROR.code,
        #         'ErrorMessage': config.ACCOUNT_COUNT_ERROR.msg.format(platform=cf.platform),
        #         'DBId': DBId,
        #         'Member': Member,
        #         'BlockMemberLayer':''
        #     }

        # 查詢帳號ID
        # params = {'term':Member.lower(), 'page':1}
        data = {'usernameArr': f'["{Member}"]'}
        while True:
            user_result = cd.searchusername(cf=cf, url=url, data=data, timeout=timeout)
            if user_result["ErrorCode"] == config.CONNECTION_CODE.code:
                return user_result
            if user_result["ErrorCode"] == config.SIGN_OUT_CODE.code:
                return user_result
            if user_result["ErrorCode"] != config.SUCCESS_CODE.code:
                return user_result["ErrorMessage"]
            if not user_result['Data'].get('results'):
                return {
                        'IsSuccess': 0,
                        'ErrorCode': config.NO_USER.code,
                        'ErrorMessage': config.NO_USER.msg,
                        'DBId': DBId,
                        'Member': Member,
                        'BlockMemberLayer':''
                }
            ids = [i for i in user_result['Data']['results'] if i['username'].lower() == Member.lower()]
            if ids:
                break
            else:
            # elif user_result['Data']['pagination']['more'] is False:
                return {
                    'IsSuccess': 0,
                    'ErrorCode': config.NO_USER.code,
                    'ErrorMessage': config.NO_USER.msg,
                    'DBId': DBId,
                    'Member': Member,
                    'BlockMemberLayer':''
                }
            # params['page'] += 1
        ids = ids[0]['id']

        # 查詢會員層級
        level_result = cd.memberlist_all(cf=cf, url=url, data={'id':ids}, timeout=timeout)
        if level_result['ErrorCode'] == config.CONNECTION_CODE.code:
            return level_result
        if level_result == config.SIGN_OUT_CODE.code:
            return level_result
        if level_result['ErrorCode'] != config.SUCCESS_CODE.code:
            return level_result['ErrorMessage']

        # BlockMemberLayer = [i['member_level'] for i in level_result['Data']['data'] if i['id'] == ids][0]
        BlockMemberLayer = {i['id']: i['member_level'] for i in level_result['Data']['data']}.get(ids, '')
        if not BlockMemberLayer:
            #查詢失敗
            return '[查询会员层级失败]CD平台回应异常'
        return {
                'IsSuccess': int(level_result['IsSuccess']),
                'ErrorCode': level_result['ErrorCode'],
                'ErrorMessage': level_result['ErrorMessage'],
                'DBId': DBId,
                'Member': Member,
                'BlockMemberLayer': BlockMemberLayer
            }


class promo(BaseFunc):
    '''CD 活动大厅'''
    # class Meta:
    #     extra = {}
    #     return_value = {'data':['DBId','Member','BlockMemberLayer'],'include':{},'exclude':[]}


    @classmethod
    def deposit(cls, *args, **kwargs):
        '''
        CD 充值
        需要有【会员管理 > 会员列表】权限。
        需要有【会员管理 > 会员列表 > 用户名 > 会员资料 > 人工存入】权限。
        需要有【报表管理 > 额度管理】权限。
        '''
        cls.th = ThreadProgress(kwargs['cf'], kwargs['mod_key'])
        cls.th.start()

        result = super().deposit(*args, **kwargs)

        cls.return_schedule(Action='chkpoint', DBId=kwargs['DBId'], Status='【充值机器人处理完毕】', Progress='-', Detail='-')
        cls.th.stop()
        return result


    @classmethod
    def audit(cls, *args, **kwargs):
        '''
            CD 活动大厅
            需要有【会员管理 > 会员列表】权限。
        '''
        if kwargs.get('SupportStatus', 0):
            cls.th = ThreadProgress(kwargs['cf'], kwargs['mod_key'])
            cls.th.start()

        result = cls._audit(*args, **kwargs)

        if kwargs.get('SupportStatus', 0):
            cls.return_schedule(Action='chkbbin', DBId=kwargs['DBId'], Status='【审核机器人处理完毕】', Progress='-', Detail='-')
            cls.th.stop()

        return result


    @classmethod
    def _audit(cls, url, DBId, Member, timeout, cf, **kwargs):
        '''CD 活动大厅 监控'''
        member = Member.lower()
        fbkA = {
                'IsSuccess': 0,
                'ErrorCode': '',
                'ErrorMessage': '',
                'BlockMemberLayer': '',
                'DBId': DBId,
                'Member': Member,
                'BlockMemberLayer': '',
            }

        #【会员列表】-【会员资料】1/2(查詢帳號ID)
        user_result = cd.searchusername(cf=cf, url=url, data={'usernameArr': f'["{member}"]'} , timeout=timeout)
        users = {u['username'].lower(): u for u in user_result.get('Data', {}).get('results', [])}
        userid = users.get(member, {}).get('id')
        if kwargs.get('SupportStatus', 0):
            if not user_result["IsSuccess"]:
                Detail = f'查询失敗\n搜寻帐号：{Member}\n{user_result["ErrorMessage"]}'
            else:
                if user_result['Data'].get('results', {}):
                    Detail = f'查询成功\n搜寻帐号：{Member}\n会员ID是否存在：{"是" if userid else "否"}'
                else:
                    Detail = f'查询成功\n搜寻帐号：{Member}\n查无会员ID'
            cls.return_schedule(Action='chkbbin', DBId=DBId, Status='【会员列表】-【会员资料】', Progress='1/2', Detail=Detail)

        # 檢查登出或連線異常, 回傳後讓主程式重試
        if user_result["ErrorCode"] == config.CONNECTION_CODE.code:
            return user_result
        if user_result["ErrorCode"] == config.SIGN_OUT_CODE.code:
            return user_result
        # 查詢失敗結束
        if user_result["ErrorCode"] != config.SUCCESS_CODE.code:
            return user_result["ErrorMessage"]
        # 查詢無會員回傳
        if not userid:
            fbkA.update({'ErrorCode': config.NO_USER.code,
                         'ErrorMessage': config.NO_USER.msg})
            return fbkA

        #【会员列表】-【会员资料】2/2(查詢會員層級)
        level_result = cd.memberlist_all(cf=cf, url=url, data={'id':userid}, timeout=timeout)
        user = {u['id']: u for u in level_result.get('Data', {}).get('data', [])}.get(userid, {})
        fbkA['BlockMemberLayer'] = user.get('member_level', '')  #（會員層級）
        if kwargs.get('SupportStatus', 0):
            if not level_result["IsSuccess"]:
                Detail = f'查询失敗\n搜寻帐号：{Member}\n{level_result["ErrorMessage"]}'
            else:
                Detail = f'查询成功\n搜寻帐号：{Member}'
            if fbkA['BlockMemberLayer']:
                Detail += f'\n会员层级：{fbkA["BlockMemberLayer"]}'
            cls.return_schedule(Action='chkbbin', DBId=DBId, Status='【会员列表】-【会员资料】', Progress='2/2', Detail=Detail)

        # 檢查登出或連線異常, 回傳後讓主程式重試
        if level_result['ErrorCode'] == config.CONNECTION_CODE.code:
            return level_result
        if level_result['ErrorCode'] == config.SIGN_OUT_CODE.code:
            return level_result
        # 查詢失敗結束
        if level_result['ErrorCode'] != config.SUCCESS_CODE.code:
            return level_result['ErrorMessage']

        # 查詢無會員回傳
        if not user:
            fbkA.update({'ErrorCode': config.NO_USER.code,
                         'ErrorMessage': config.NO_USER.msg})
            return fbkA

        # 回傳結果
        fbkA.update({
            'IsSuccess': 1,
            'ErrorCode': config.SUCCESS_CODE.code,
            'ErrorMessage': config.SUCCESS_CODE.msg
            })
        return fbkA


class enjoyup(BaseFunc):
    '''CD 喜上喜'''
    # class Meta:
    #     extra = {}
    #     return_value = {'data':[
    #                                 'DBId','RawWagersId','Member','BlockMemberLayer',
    #                                 'GameName','WagersTimeString','WagersTimeStamp',
    #                                 'BetAmount','AllCategoryCommissionable',
    #                                 'GameCommissionable','SingleCategoryCommissionable',
    #                                 'PayoutAmount','CategoryName','ExtendLimit','GameCountType'
    #                                 ],'include':{},'exclude':[]}

    @classmethod
    def deposit(cls, *args, **kwargs):
        '''
        CD 充值
        需要有【会员管理 > 会员列表】权限。
        需要有【会员管理 > 会员列表 > 用户名 > 会员资料 > 人工存入】权限。
        需要有【会员管理 > 会员列表 > 会员資料 > 額度管理】权限。
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
        《CD 喜上喜》
        需要有【会员管理 > 会员列表 | 报表管理 > 投注记录】权限。
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
    def _deposit(cls, url, DBId, Member, DepositAmount, mod_name, mod_key, amount_below,
        timeout, backend_remarks, multiple, cf, amount_memo='', Note='', increasethebet_switch=0, increasethebet='0', **kwargs):
        '''CD 喜上喜 充值'''
        return super().deposit(url, DBId, Member, DepositAmount, mod_name, mod_key, amount_below,
            timeout, backend_remarks, multiple, cf, amount_memo, Note, increasethebet_switch, increasethebet, **kwargs)

    @classmethod
    @keep_connect
    def _audit(cls, url, DBId, Member, SearchGameCategory, SearchDate, timeout, cf, RawWagersId, ExtendLimit, GameCountType, **kwargs):
        '''
        《CD 喜上喜》
        需要有【会员管理 > 会员列表 | 报表管理 > 投注记录】权限。
        '''
        SupportStatus = bool(kwargs.get('SupportStatus'))

        #【会员列表】-【会员资料】
        user_result = cd.searchusername(cf, url, data={'usernameArr': f'["{Member}"]'}, timeout=timeout)
        users = {u['username'].lower(): u for u in user_result.get('Data', {}).get('results', [])}
        userid = users.get(Member.lower(), {}).get('id')
        if not user_result["IsSuccess"]:
            Detail = f'查询失敗\n搜寻帐号：{Member}\n{user_result["ErrorMessage"]}'
        else:
            Detail = f'查询成功\n搜寻帐号：{Member}\n会员是否存在：{"是" if userid else "否"}'
            if userid:
                Detail += f'\n会员ID：{userid}'
        if SupportStatus:
            cls.return_schedule(Action='chkbbin', DBId=DBId, Status='【会员列表】-【会员资料】', Progress='1/1', Detail=Detail)
        else:
            cls.return_schedule(Action='chkbbin', DBId=DBId, Member=Member, current_status='【会员列表】-【会员资料】')

        if user_result["ErrorCode"] == config.CONNECTION_CODE.code:
            return user_result
        if user_result['ErrorCode'] == config.SIGN_OUT_CODE.code:
            return user_result
        if user_result["ErrorCode"] != config.SUCCESS_CODE.code:
            return user_result["ErrorMessage"]
        if not userid:
            return {
                "IsSuccess": 0,
                "ErrorCode": config.NO_USER.code,
                "ErrorMessage": config.NO_USER.msg,
                "DBId": DBId,
                "RawWagersId": RawWagersId,
                "Member": Member,
                "BlockMemberLayer": "",
                "GameName": "",
                "WagersTimeString": "",
                "WagersTimeStamp": "",
                "BetAmount": "0.00",
                "AllCategoryCommissionable": "0.00",
                "GameCommissionable": "0.00",
                "SingleCategoryCommissionable": "0.00",
                "PayoutAmount": "0.00",
                "CategoryName": "",
                "ExtendLimit": ExtendLimit,
                "GameCountType": GameCountType
            }

        #★查詢【會員層級】
        level_result = cd.memberlist_all(cf=cf, url=url, data={'id': userid})
        BlockMemberLayer = {i['id']: i['member_level'] for i in level_result['Data'].get('data', {})}.get(userid, '')
        if not level_result["IsSuccess"]:
            Detail = f'查询失敗\n错误讯息：{level_result["ErrorMessage"]}'
        else:
            Detail = f'查询成功\n会员层级：{BlockMemberLayer}'
        if SupportStatus:
            cls.return_schedule(Action='chkbbin', DBId=DBId, Status='查询【会员层级】', Progress='1/1', Detail=Detail)
        else:
            cls.return_schedule(Action='chkbbin', DBId=DBId, Member=Member, current_status='查询【会员层级】')
        if level_result['ErrorCode'] == config.CONNECTION_CODE.code:
            return level_result
        if level_result['ErrorCode'] == config.SIGN_OUT_CODE.code:
            return level_result
        if level_result['ErrorCode'] != config.SUCCESS_CODE.code:
            return level_result['ErrorMessage']
        if not BlockMemberLayer:
            #查無此會員
            return {
                    "IsSuccess": 0,
                    "ErrorCode": config.NO_USER.code,
                    "ErrorMessage": config.NO_USER.msg,
                    "DBId": DBId,
                    "RawWagersId": RawWagersId,
                    "Member": Member,
                    "BlockMemberLayer": "",
                    "GameName": "",
                    "WagersTimeString": "",
                    "WagersTimeStamp": "",
                    "BetAmount": "0.00",
                    "AllCategoryCommissionable": "0.00",
                    "GameCommissionable": "0.00",
                    "SingleCategoryCommissionable": "0.00",
                    "PayoutAmount": "0.00",
                    "CategoryName": "",
                    "ExtendLimit": ExtendLimit,
                    "GameCountType": GameCountType
            }

        #★查詢【注單內容】
        date = SearchDate + ' 23:59:59'
        date_a = datetime.datetime.strptime(date,'%Y/%m/%d %H:%M:%S')
        date_b = date_a + datetime.timedelta(days=-1)
        date_a = date_a + datetime.timedelta(days=+1) #怕系統傳入-4時區, 但CD平台用+8時區查詢會有時間差
        date_a = datetime.datetime.strftime(date_a, '%Y-%m-%d %H:%M:%S')
        date_b = datetime.datetime.strftime(date_b, '%Y-%m-%d 00:00:00')
        totalday = date_b + ' - ' + date_a
        GameKind = ','.join(SearchGameCategory)
        bets_result = cd.betsdata(cf=cf, url=url, data={
                                                        'length':'100',
                                                        'search_bet_date':totalday,#資料格式:'2020-11-25 00:00:00 - 2020-11-26 23:59:59'
                                                        'search_bet_id':RawWagersId,
                                                        'search_vendor_game_categories':GameKind
                                                         },timeout=timeout)

        data = {i['bet_id'].lower(): i for i in bets_result.get('Data', {}).get('data', [])}.get(RawWagersId.lower(), {})
        BetAmount = f"{float(data.get('effective_turnover', '0.00').replace(',', ''))*100//1/100:.2f}"
        GameName = data.get('game_name', '') #遊戲名稱
        Game_Id = data.get('game_id', '') #遊戲名稱ID
        Game_category_Id = data.get('vendor_game_category_id', '') #遊戲類別ID
        CategoryName = data.get('vendor', '') #遊戲類別名稱
        PayoutAmount = data.get('win_loss', '') #注單派彩金額
        AllCategoryCommissionable = '0.00'
        SingleCategoryCommissionable = '0.00'
        GameCommissionable = '0.00'
        if data.get('bet_date'):
            WagersTimeStamp = str(int(data.get('bet_date')) * 1000) #時間戳轉換至毫秒
            dt = datetime.datetime.fromtimestamp(int(WagersTimeStamp)/1000, tz=pytz.timezone('Asia/Shanghai')) #轉換成時間格式
            WagersTimeString = dt.strftime(r'%Y/%m/%d %H:%M:%S %z') #時間格式轉換成字串
        else:
            WagersTimeStamp = ''
            WagersTimeString = ''

        if not bets_result["IsSuccess"]:
            Detail = f'查询失敗\n错误讯息：{bets_result["ErrorMessage"]}'
        else:
            Detail = (
                f'查询成功\n'
                f'注单是否存在：{"是" if data else "否"}'
            )
            if data:
                Detail += f'\n注单会员：{data["username"]}'
                Detail += f'\n注单時間：{WagersTimeString}'
                Detail += f'\n注单金额：{BetAmount}'
                Detail += f'\n游戏名称：{GameName}'
                Detail += f'\n游戏类别名称：{CategoryName}'
                Detail += f'\n单注派彩金额：{PayoutAmount}'
        if SupportStatus:
            cls.return_schedule(Action='chkbbin', DBId=DBId, Status='查询【注单内容】', Progress='1/1', Detail=Detail)
        else:
            cls.return_schedule(Action='chkbbin', DBId=DBId, Member=Member, current_status='查询【注单内容】')

        if bets_result['ErrorCode'] == config.CONNECTION_CODE.code:
            return bets_result
        if bets_result['ErrorCode'] == config.SIGN_OUT_CODE.code:
            return bets_result
        if bets_result['ErrorCode'] != config.SUCCESS_CODE.code:
            return bets_result['ErrorMessage']
        #查無此注單號
        if not bets_result['Data']['data']: #如無data欄位也會回傳空的list,因此不用get判斷
            return {
                    "IsSuccess": 0,
                    "ErrorCode": config.WAGERS_NOT_FOUND.code,
                    "ErrorMessage": config.WAGERS_NOT_FOUND.msg,
                    "DBId": DBId,
                    "RawWagersId": RawWagersId,
                    "Member": Member,
                    "BlockMemberLayer": BlockMemberLayer,
                    "GameName": "",
                    "WagersTimeString": "",
                    "WagersTimeStamp": "",
                    "BetAmount": "0.00",
                    "AllCategoryCommissionable": AllCategoryCommissionable,
                    "GameCommissionable": GameCommissionable,
                    "SingleCategoryCommissionable": SingleCategoryCommissionable,
                    "PayoutAmount": "0.00",
                    "CategoryName": "",
                    "ExtendLimit": ExtendLimit,
                    "GameCountType": GameCountType
            }
        if data.get('username').lower() != Member.lower(): #注單號與會員不符
            return {
                    "IsSuccess": 0,
                    "ErrorCode": config.USER_WAGERS_NOT_MATCH.code,
                    "ErrorMessage": config.USER_WAGERS_NOT_MATCH.msg,
                    "DBId": DBId,
                    "RawWagersId": RawWagersId,
                    "Member": Member,
                    "BlockMemberLayer": BlockMemberLayer,
                    "GameName": "",
                    "WagersTimeString": "",
                    "WagersTimeStamp": "",
                    "BetAmount": "0.00",
                    "AllCategoryCommissionable": AllCategoryCommissionable,
                    "GameCommissionable": GameCommissionable,
                    "SingleCategoryCommissionable": SingleCategoryCommissionable,
                    "PayoutAmount": "0.00",
                    "CategoryName": "",
                    "ExtendLimit": ExtendLimit,
                    "GameCountType": GameCountType
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
                "PayoutAmount": PayoutAmount,
                "CategoryName": CategoryName,
                "ExtendLimit": ExtendLimit,
                "GameCountType": GameCountType
            }

        # 查詢各式投注金額
        searchdate = f'{SearchDate} 00:00:00 - {SearchDate} 23:59:59'.replace('/', '-') #格式化日期
        GameCountType_result = cd.getgameplayhistorygrandtotal(cf=cf, url=url,
                                                               data={
                                                                'search_bet_date':searchdate,
                                                                'search_vendor_game_categories':GameKind if GameCountType == '0' else Game_category_Id,
                                                                'search_games':Game_Id if GameCountType == '1' else '',
                                                                'search_usernames':userid
                                                                }, timeout=timeout)

        if GameCountType_result.get('Data', {}).get('data', []):
            if GameCountType == '0':  #★查詢【選取分類當日投注】
                AllCategoryCommissionable = GameCountType_result.get('Data', {}).get('data', [{}])[0].get('total_effective_turnover') or '0.00' #當無total_effective_turnover無值時會收到Null
                AllCategoryCommissionable = f"{float(AllCategoryCommissionable.replace(',', ''))*100//1/100:.2f}"
                Status = '选取分类当日投注'
                Detail = f'{Status}：{AllCategoryCommissionable}'
            elif GameCountType == '1':  #★查詢【本遊戲當日投注】
                GameCommissionable = GameCountType_result['Data']['data'][0]['total_effective_turnover'] or '0.00' #當無total_effective_turnover無值時會收到Null
                GameCommissionable = '.'.join([GameCommissionable.partition('.')[0],GameCommissionable.partition('.')[2][:2]]) #無條件捨去至小數第2位
                Status = '本游戏当日投注'
                Detail = f'{Status}：{GameCommissionable}'
            else:  #★查詢【本分類當日投注】
                SingleCategoryCommissionable = GameCountType_result['Data']['data'][0]['total_effective_turnover'] or '0.00' #當無total_effective_turnover無值時會收到Null
                SingleCategoryCommissionable = '.'.join([SingleCategoryCommissionable.partition('.')[0],SingleCategoryCommissionable.partition('.')[2][:2]]) #無條件捨去至小數第2位
                Status = '本分类当日投注'
                Detail = f'{Status}：{SingleCategoryCommissionable}'

            if not GameCountType_result["IsSuccess"]:
                Detail = f'查询失敗\n错误讯息：{GameCountType_result["ErrorMessage"]}'
            else:
                Detail = f'查询成功\n{Detail}'

            if SupportStatus:
                cls.return_schedule(Action='chkbbin', DBId=DBId, Status=f"查询【{Status}】", Progress='1/1', Detail=Detail)
            else:
                cls.return_schedule(Action='chkbbin', DBId=DBId, Member=Member, current_status=Status)

        else:
            logger.warning('計算總投注金額時CD回傳空list')
            logger.warning(f'回傳內容:{GameCountType_result["Data"]}')

        if GameCountType_result['ErrorCode'] == config.CONNECTION_CODE.code:
            return GameCountType_result
        if GameCountType_result['ErrorCode'] == config.SIGN_OUT_CODE.code:
            return GameCountType_result
        if GameCountType_result['ErrorCode'] != config.SUCCESS_CODE.code:
            return GameCountType_result['ErrorMessage']

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
                "PayoutAmount": PayoutAmount,
                "CategoryName": CategoryName,
                "ExtendLimit": ExtendLimit,
                "GameCountType": GameCountType
            }


class betslip(BaseFunc):
    '''CD 注單'''
    # class Meta:
    #     extra = {}
    #     return_value = {'data':[
    #                             'DBId','RawWagersId','Member','BlockMemberLayer',
    #                             'GameName','WagersTimeString','WagersTimeStamp',
    #                             'BetAmount','AllCategoryCommissionable',
    #                             'GameCommissionable','SingleCategoryCommissionable',
    #                             'CategoryName','ExtendLimit','GameCountType'
    #                             ],
    #                             'include':{},
    #                             'exclude':['DBId']
    #                             }

    @classmethod
    def audit(cls, url, DBId, Member, SearchGameCategory, SearchDate, timeout, cf, RawWagersId, ExtendLimit, GameCountType, **kwargs):
        '''
        《CD 注單》
        需要有【会员管理 > 会员列表 | 报表管理 > 投注记录】权限。
        '''
        # if len(Member) < 4 or len(Member) > 16:
        #     return {
        #         'IsSuccess': 0,
        #         'ErrorCode': config.ACCOUNT_COUNT_ERROR.code,
        #         'ErrorMessage': config.ACCOUNT_COUNT_ERROR.msg.format(platform=cf.platform),
        #         'DBId': DBId,
        #         "RawWagersId": RawWagersId,
        #         "Member": Member,
        #         "BlockMemberLayer": "",
        #         "GameName": "",
        #         "WagersTimeString": "",
        #         "WagersTimeStamp": "",
        #         "BetAmount": "0.00",
        #         "AllCategoryCommissionable": "0.00",
        #         "GameCommissionable": "0.00",
        #         "SingleCategoryCommissionable": "0.00",
        #         "CategoryName": "",
        #         "ExtendLimit": ExtendLimit,
        #         "GameCountType": GameCountType
        #     }

        # 查詢帳號ID
        cls.return_schedule(Action='chkbbin', DBId=DBId, Member=Member, current_status='查詢帳號ID')
        # params = {'term':Member.lower(), 'page':1}
        data = {'usernameArr': f'["{Member}"]'}
        while True:
            user_result = cd.searchusername(cf=cf, url=url, data=data, timeout=timeout)
            if user_result["ErrorCode"] == config.CONNECTION_CODE.code:
                return user_result
            if user_result["ErrorCode"] == config.SIGN_OUT_CODE.code:
                return user_result
            if user_result["ErrorCode"] != config.SUCCESS_CODE.code:
                return user_result["ErrorMessage"]
            if not user_result['Data'].get('results'):
                return {
                    "IsSuccess": 0,
                    "ErrorCode": config.NO_USER.code,
                    "ErrorMessage": config.NO_USER.msg,
                    "DBId": DBId,
                    "RawWagersId": RawWagersId,
                    "Member": Member,
                    "BlockMemberLayer": "",
                    "GameName": "",
                    "WagersTimeString": "",
                    "WagersTimeStamp": "",
                    "BetAmount": "0.00",
                    "AllCategoryCommissionable": "0.00",
                    "GameCommissionable": "0.00",
                    "SingleCategoryCommissionable": "0.00",
                    "CategoryName": "",
                    "ExtendLimit": ExtendLimit,
                    "GameCountType": GameCountType
                }
            ids = [i for i in user_result['Data'].get('results') if i['username'].lower() == Member.lower()]
            if ids:
                break
            else:
            # elif user_result['Data']['pagination']['more'] is False:
                return {
                    "IsSuccess": 0,
                    "ErrorCode": config.NO_USER.code,
                    "ErrorMessage": config.NO_USER.msg,
                    "DBId": DBId,
                    "RawWagersId": RawWagersId,
                    "Member": Member,
                    "BlockMemberLayer": "",
                    "GameName": "",
                    "WagersTimeString": "",
                    "WagersTimeStamp": "",
                    "BetAmount": "0.00",
                    "AllCategoryCommissionable": "0.00",
                    "GameCommissionable": "0.00",
                    "SingleCategoryCommissionable": "0.00",
                    "CategoryName": "",
                    "ExtendLimit": ExtendLimit,
                    "GameCountType": GameCountType
                }
            # params['page'] += 1

        ids = ids[0]['id'] #會員ID
        # 查詢會員層級
        cls.return_schedule(Action='chkbbin', DBId=DBId, Member=Member, current_status='查詢會員層級')
        level_result = cd.memberlist_all(cf=cf, url=url, data={'id':ids}, timeout=timeout)
        if level_result['ErrorCode'] == config.CONNECTION_CODE.code:
            return level_result
        if level_result['ErrorCode'] == config.SIGN_OUT_CODE.code:
            return level_result
        if level_result['ErrorCode'] != config.SUCCESS_CODE.code:
            return level_result['ErrorMessage']

        # BlockMemberLayer = [i['member_level'] for i in level_result['Data']['data'] if i['id'] == ids][0]
        BlockMemberLayer = {i['id']: i['member_level'] for i in level_result['Data']['data']}.get(ids, '')
        if not BlockMemberLayer:
            #查無此會員
            return {
                    "IsSuccess": 0,
                    "ErrorCode": config.NO_USER.code,
                    "ErrorMessage": config.NO_USER.msg,
                    "DBId": DBId,
                    "RawWagersId": RawWagersId,
                    "Member": Member,
                    "BlockMemberLayer": "",
                    "GameName": "",
                    "WagersTimeString": "",
                    "WagersTimeStamp": "",
                    "BetAmount": "0.00",
                    "AllCategoryCommissionable": "0.00",
                    "GameCommissionable": "0.00",
                    "SingleCategoryCommissionable": "0.00",
                    "CategoryName": "",
                    "ExtendLimit": ExtendLimit,
                    "GameCountType": GameCountType
            }

        #查尋注單金額
        cls.return_schedule(Action='chkbbin', DBId=DBId, Member=Member, current_status='查詢注單金額')
        date = SearchDate + ' 23:59:59'
        date_a = datetime.datetime.strptime(date,'%Y/%m/%d %H:%M:%S')
        date_b = date_a + datetime.timedelta(days=-1)
        date_a = date_a + datetime.timedelta(days=+1) #怕系統傳入-4時區, 但CD平台用+8時區查詢會有時間差
        date_a = datetime.datetime.strftime(date_a, '%Y-%m-%d %H:%M:%S')
        date_b = datetime.datetime.strftime(date_b, '%Y-%m-%d 00:00:00')
        totalday = date_b + ' - ' + date_a
        GameKind = ','.join(SearchGameCategory)
        bets_result = cd.betsdata(cf=cf, url=url, data={
                                                        'length':'100',
                                                        'search_bet_date':totalday,#資料格式:'2020-11-25 00:00:00 - 2020-11-26 23:59:59'
                                                        'search_bet_id':RawWagersId,
                                                        'search_vendor_game_categories':GameKind
                                                         },timeout=timeout)
        if bets_result['ErrorCode'] == config.CONNECTION_CODE.code:
            return bets_result
        if bets_result['ErrorCode'] == config.SIGN_OUT_CODE.code:
            return bets_result
        if bets_result['ErrorCode'] != config.SUCCESS_CODE.code:
            return bets_result['ErrorMessage']


        if not bets_result['Data']['data']: #如無data欄位也會回傳空的list,因此不用get判斷
            #查無此注單號
            return {
                    "IsSuccess": 0,
                    "ErrorCode": config.WAGERS_NOT_FOUND.code,
                    "ErrorMessage": config.WAGERS_NOT_FOUND.msg,
                    "DBId": DBId,
                    "RawWagersId": RawWagersId,
                    "Member": Member,
                    "BlockMemberLayer": BlockMemberLayer,
                    "GameName": "",
                    "WagersTimeString": "",
                    "WagersTimeStamp": "",
                    "BetAmount": "0.00",
                    "AllCategoryCommissionable": "0.00",
                    "GameCommissionable": "0.00",
                    "SingleCategoryCommissionable": "0.00",
                    "CategoryName": "",
                    "ExtendLimit": ExtendLimit,
                    "GameCountType": GameCountType
            }
        data = [i for i in bets_result['Data']['data'] if i['bet_id'].lower() == RawWagersId.lower()][0]
        if data['username'].lower() != Member.lower(): #注單號與會員不符
            return {
                    "IsSuccess": 0,
                    "ErrorCode": config.USER_WAGERS_NOT_MATCH.code,
                    "ErrorMessage": config.USER_WAGERS_NOT_MATCH.msg,
                    "DBId": DBId,
                    "RawWagersId": RawWagersId,
                    "Member": Member,
                    "BlockMemberLayer": BlockMemberLayer,
                    "GameName": "",
                    "WagersTimeString": "",
                    "WagersTimeStamp": "",
                    "BetAmount": "0.00",
                    "AllCategoryCommissionable": "0.00",
                    "GameCommissionable": "0.00",
                    "SingleCategoryCommissionable": "0.00",
                    "CategoryName": "",
                    "ExtendLimit": ExtendLimit,
                    "GameCountType": GameCountType
            }
        BetAmount =  str(abs(eval('.'.join([data['effective_turnover'].partition('.')[0],data['effective_turnover'].partition('.')[2][:2]])))) #注單金額取小數第2位並無條件捨去
        GameName = data['game_name'] #遊戲名稱
        Game_Id = data['game_id'] #遊戲名稱ID
        Game_category_Id = data['vendor_game_category_id'] #遊戲類別ID
        CategoryName = data['vendor'] #遊戲類別名稱
        #PayoutAmount = data['win_loss'] #注單派彩金額
        tz = pytz.timezone('Asia/Shanghai') #時區
        WagersTimeStamp = str(int(data['bet_date']) * 1000) #時間戳轉換至毫秒
        dt = datetime.datetime.fromtimestamp(int(WagersTimeStamp)/1000, tz=tz) #轉換成時間格式
        WagersTimeString = dt.strftime('%Y/%m/%d %H:%M:%S %z') #時間格式轉換成字串
        AllCategoryCommissionable = '0.00' #選取分類當日投注
        GameCommissionable = '0.00' #本遊戲當日投注
        SingleCategoryCommissionable = '0.00' #本分類當日投注
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
        searchdate = SearchDate.replace('/','-') + ' ' + '00:00:00' + ' - ' + SearchDate.replace('/','-') + ' ' + '23:59:59' #格式化日期
        # 計算總投注金額
        cls.return_schedule(Action='chkbbin', DBId=DBId, Member=Member, current_status='計算總投注金額')
        GameCountType_result = cd.getgameplayhistorygrandtotal(cf=cf, url=url,
                                                               data={
                                                                'search_bet_date':searchdate,
                                                                'search_vendor_game_categories':GameKind if GameCountType == '0' else Game_category_Id,
                                                                'search_games':Game_Id if GameCountType == '1' else '',
                                                                'search_usernames':ids
                                                                }, timeout=timeout)

        if GameCountType_result['ErrorCode'] == config.CONNECTION_CODE.code:
            return GameCountType_result
        if GameCountType_result['ErrorCode'] == config.SIGN_OUT_CODE.code:
            return GameCountType_result
        if GameCountType_result['ErrorCode'] != config.SUCCESS_CODE.code:
            return GameCountType_result['ErrorMessage']

        if GameCountType_result['Data']['data']: #如無data欄位也會回傳空的list,因此不用get判斷
            if GameCountType == '0':#選取分類當日投注
                AllCategoryCommissionable = GameCountType_result['Data']['data'][0]['total_effective_turnover'] or '0.00' #當無total_effective_turnover無值時會收到Null
                AllCategoryCommissionable = '.'.join([AllCategoryCommissionable.partition('.')[0],AllCategoryCommissionable.partition('.')[2][:2]]) #無條件捨去至小數第2位
            elif GameCountType == '1':#本遊戲當日投注
                GameCommissionable = GameCountType_result['Data']['data'][0]['total_effective_turnover'] or '0.00' #當無total_effective_turnover無值時會收到Null
                GameCommissionable = '.'.join([GameCommissionable.partition('.')[0],GameCommissionable.partition('.')[2][:2]]) #無條件捨去至小數第2位
            else:#本分類當日投注
                SingleCategoryCommissionable = GameCountType_result['Data']['data'][0]['total_effective_turnover'] or '0.00' #當無total_effective_turnover無值時會收到Null
                SingleCategoryCommissionable = '.'.join([SingleCategoryCommissionable.partition('.')[0],SingleCategoryCommissionable.partition('.')[2][:2]]) #無條件捨去至小數第2位
        else:
            logger.warning('計算總投注金額時CD回傳空list')
            logger.warning(f'回傳內容:{GameCountType_result["Data"]}')

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


class freespin(BaseFunc):
    '''CD 旋转注单'''
    class Meta:
        # extra = {}
        # return_value = {'data':[
        #                             'DBId','RawWagersId','Member','BlockMemberLayer',
        #                             'GameName','WagersTimeString','WagersTimeStamp',
        #                             'BetAmount','AllCategoryCommissionable',
        #                             'GameCommissionable','SingleCategoryCommissionable',
        #                             'CategoryName','ExtendLimit','GameCountType','FreeSpin'
        #                             ],
        #                             'include':{},
        #                             'exclude':['DBId']
        #                             }
        # 支援清單，新增了就會支援!! 原始資料在utils
        suport = spin_betslip_gamedict
    @classmethod
    def deposit(cls, *args, **kwargs):
        '''
        CD 充值
        需要有【会员管理 > 会员列表】权限。
        需要有【会员管理 > 会员列表 > 用户名 > 会员资料 > 人工存入】权限。
        需要有【会员管理 > 会员列表 > 会员資料 > 額度管理】权限。
        '''
        SupportStatus = kwargs.get('SupportStatus')
        cls.th = ThreadProgress(kwargs['cf'], kwargs['mod_key'], detail=False)
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
        《CD 旋轉注單》
        需要有【会员管理 > 会员列表 | 报表管理 > 投注记录】权限。
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
    def _deposit(cls, url, DBId, Member, DepositAmount, mod_name, mod_key, amount_below,
        timeout, backend_remarks, multiple, cf, amount_memo='', Note='', increasethebet_switch=0, increasethebet='0', **kwargs):
        '''CD 旋转注单 充值'''
        return super().deposit(url, DBId, Member, DepositAmount, mod_name, mod_key, amount_below,
            timeout, backend_remarks, multiple, cf, amount_memo, Note, increasethebet_switch, increasethebet, **kwargs)

    @classmethod
    def _audit(cls, url, DBId, Member, SearchGameCategory, SearchDate, timeout, cf, RawWagersId, ExtendLimit, GameCountType, **kwargs):
        '''
        《CD 旋转注单》
        需要有【会员管理 > 会员列表 | 报表管理 > 投注记录】权限。
        '''
        fbkA = {
                'IsSuccess': 0,
                'ErrorCode': '',
                'ErrorMessage': '',
                'DBId': DBId,
                "RawWagersId": RawWagersId,
                "Member": Member,
                "BlockMemberLayer": "",
                "GameName": "",
                "WagersTimeString": "",
                "WagersTimeStamp": "",
                "BetAmount": "0.00",
                "AllCategoryCommissionable": "0.00",
                "GameCommissionable": "0.00",
                "SingleCategoryCommissionable": "0.00",
                "CategoryName": "",
                "ExtendLimit": ExtendLimit,
                "GameCountType": GameCountType,
                'FreeSpin':0
            }
        SupportStatus = bool(kwargs.get('SupportStatus'))
        # if len(Member) < 4 or len(Member) > 16:
        #     fbkA.update({'ErrorCode': config.ACCOUNT_COUNT_ERROR.code,
        #                  'ErrorMessage': config.ACCOUNT_COUNT_ERROR.msg.format(platform=cf.platform)})
        #     return fbkA

        #★查詢【會員ID】
        # params = {'term':Member.lower(), 'page':1}
        data = {'usernameArr': f'["{Member}"]'}
        user_result = cd.searchusername(cf=cf, url=url, data=data, timeout=timeout)
        ids = [i for i in user_result.get('Data', {}).get('results', {}) if i['username'].lower() == Member.lower()]
        if not user_result['IsSuccess']:
            Detail = f'搜尋調件: {Member}\\n搜尋結果: {user_result["ErrorMessge"]}'
        else:
            Detail = f'搜尋調件: {Member}\\n搜尋結果: {"是" if ids else "否"}'
        if SupportStatus:
            cls.return_schedule(Action='chkbbin', DBId=DBId, Member=Member, Status='查询【会员ID】', Progress='1/1', Detail=Detail)
        else:
            cls.return_schedule(Action='chkbbin', DBId=DBId, Member=Member, current_status='查询【会员ID】')
        if user_result["ErrorCode"] == config.CONNECTION_CODE.code:
            return user_result
        if user_result["ErrorCode"] == config.SIGN_OUT_CODE.code:
            return user_result
        if user_result["ErrorCode"] != config.SUCCESS_CODE.code:
            return user_result["ErrorMessage"]
        if not user_result['Data'].get('results'):
            fbkA.update({'ErrorCode': config.NO_USER.code,
                            'ErrorMessage': config.NO_USER.msg})
            return fbkA
        if not ids:
            fbkA.update({'ErrorCode': config.NO_USER.code, 'ErrorMessage': config.NO_USER.msg})
            return fbkA

        ids = ids[0]['id'] #會員ID
        #★查詢【會員層級】
        level_result = cd.memberlist_all(cf=cf, url=url, data={'id':ids}, timeout=timeout)
        BlockMemberLayer = {i['id']: i['member_level'] for i in level_result.get('Data',{}).get('data',{})}.get(ids, '')
        if not level_result['IsSuccess']:
            Detail = f'搜尋調件: {Member}\\n搜尋結果: {level_result["ErrorMessge"]}'
        else:
            Detail = f'搜尋調件: {Member}\\n搜尋結果: {"是" if BlockMemberLayer else "否"}'
        if SupportStatus:
            cls.return_schedule(Action='chkbbin', DBId=DBId, Member=Member, Status='查询【会员层级】', Progress='1/1', Detail=Detail)
        else:
            cls.return_schedule(Action='chkbbin', DBId=DBId, Member=Member, current_status='查询【会员层级】')
        if level_result['ErrorCode'] == config.CONNECTION_CODE.code:
            return level_result
        if level_result['ErrorCode'] == config.SIGN_OUT_CODE.code:
            return level_result
        if level_result['ErrorCode'] != config.SUCCESS_CODE.code:
            return level_result['ErrorMessage']
        if not BlockMemberLayer:
            #查無此會員
            fbkA.update({'ErrorCode': config.NO_USER.code,
                         'ErrorMessage': config.NO_USER.msg})
            return fbkA

        #★查詢【注單內容】
        date = SearchDate + ' 23:59:59'
        date_a = datetime.datetime.strptime(date,'%Y/%m/%d %H:%M:%S')
        date_b = date_a + datetime.timedelta(days=-1)
        date_a = date_a + datetime.timedelta(days=+1) #怕系統傳入-4時區, 但CD平台用+8時區查詢會有時間差
        date_a = datetime.datetime.strftime(date_a, '%Y-%m-%d %H:%M:%S')
        date_b = datetime.datetime.strftime(date_b, '%Y-%m-%d 00:00:00')
        totalday = date_b + ' - ' + date_a
        GameKind = ','.join(SearchGameCategory)
        bets_result = cd.betsdata(cf=cf, url=url, data={
                                                        # 'length':'100',
                                                        'search_bet_date':totalday,#資料格式:'2020-11-25 00:00:00 - 2020-11-26 23:59:59'
                                                        'search_bet_id':RawWagersId,
                                                        # 'search_vendor_game_categories':GameKind
                                                        },
                                                        timeout=timeout
                                                        )
        data = [i for i in bets_result.get('Data',{}).get('data',{}) if i['bet_id'].lower() == RawWagersId.lower()]
        data = data[0] if data else []
        if data and data['username'].lower() != Member.lower(): #注單號與會員不符
            fbkA.update({'ErrorCode': config.USER_WAGERS_NOT_MATCH.code,
                         'ErrorMessage': config.USER_WAGERS_NOT_MATCH.msg,
                         'BlockMemberLayer': BlockMemberLayer})
            return fbkA
        if not bets_result['IsSuccess']:
            Detail = f'搜尋調件: {Member}\\n搜尋結果: {bets_result["ErrorMessge"]}'
        else:
            Detail = f'搜尋調件: {Member}\\n搜尋結果: {"是" if data else "否"}'
        if SupportStatus:
            cls.return_schedule(Action='chkbbin', DBId=DBId, Member=Member, Status='查询【注单内容】', Progress='1/1', Detail=Detail)
        else:
            cls.return_schedule(Action='chkbbin', DBId=DBId, Member=Member, current_status='查询【注单内容】')
        if bets_result['ErrorCode'] == config.CONNECTION_CODE.code:
            return bets_result
        if bets_result['ErrorCode'] == config.SIGN_OUT_CODE.code:
            return bets_result
        if bets_result['ErrorCode'] != config.SUCCESS_CODE.code:
            return bets_result['ErrorMessage']

        if not bets_result['Data']['data']: #如無data欄位也會回傳空的list,因此不用get判斷
            #查無此注單號
            fbkA.update({'ErrorCode': config.WAGERS_NOT_FOUND.code,
                         'ErrorMessage': config.WAGERS_NOT_FOUND.msg,
                         'BlockMemberLayer': BlockMemberLayer})
            return fbkA

        BetAmount =  str(abs(eval('.'.join([data['effective_turnover'].partition('.')[0],data['effective_turnover'].partition('.')[2][:2]])))) #注單金額取小數第2位並無條件捨去
        GameName = data['game_name'] #遊戲名稱
        Game_Id = data['game_id'] #遊戲名稱ID
        Game_category_Id = data['vendor_game_category_id'] #遊戲類別ID
        CategoryName = data['vendor'] #遊戲類別名稱
        #PayoutAmount = data['win_loss'] #注單派彩金額
        tz = pytz.timezone('Asia/Shanghai') #時區
        WagersTimeStamp = str(int(data['bet_date']) * 1000) #時間戳轉換至毫秒
        dt = datetime.datetime.fromtimestamp(int(WagersTimeStamp)/1000, tz=tz) #轉換成時間格式
        WagersTimeString = dt.strftime('%Y/%m/%d %H:%M:%S %z') #時間格式轉換成字串
        AllCategoryCommissionable = '0.00' #選取分類當日投注
        GameCommissionable = '0.00' #本遊戲當日投注
        SingleCategoryCommissionable = '0.00' #本分類當日投注


        #查詢是否支援麻將遊戲類別
        gametype_id = [i for i in spin_betslip_gametype]
        #gametype = [spin_betslip_gametype[i] for i in spin_betslip_gametype]
        if Game_category_Id not in gametype_id:
            fbkA.update({'ErrorCode': config.CATEGORY_ERROR.code,
                         'ErrorMessage': config.CATEGORY_ERROR.msg.format(CategoryName=CategoryName),
                         'BlockMemberLayer': BlockMemberLayer})
            return fbkA
        #查詢是否支援麻將遊戲名稱
        gamedict_id = [i for i in spin_betslip_gamedict]
        #gamedict = [spin_betslip_gamedict[i].split(']')[1] for i in spin_betslip_gamedict]
        if Game_Id not in gamedict_id:
            fbkA.update({'ErrorCode': config.GAME_ERROR.code,
                         'ErrorMessage': config.GAME_ERROR.msg.format(GameName=GameName),
                         'BlockMemberLayer': BlockMemberLayer})
            return fbkA


        # pg电子,麻將胡了,麻將胡了2
        # if Game_Id in ['3034','3046'] and Game_category_Id == '25':
        if Game_category_Id == '25':
            #查詢是否免費旋轉
            mj_url_result = cd.game_trans_id(cf=cf, url=url, data={
                                                                'game_trans_id':RawWagersId,
                                                                'game_id':Game_Id,
                                                                'player_id':ids
                                                            }, timeout=timeout)
            if mj_url_result['ErrorCode'] == config.CONNECTION_CODE.code:
                return mj_url_result
            if mj_url_result['ErrorCode'] == config.SIGN_OUT_CODE.code:
                return mj_url_result
            if mj_url_result['ErrorCode'] != config.SUCCESS_CODE.code:
                return mj_url_result['ErrorMessage']
            t = mj_url_result['Data'].split('t=')[1].split('&')[0]
            gid = mj_url_result['Data'].split('gid=')[1].split('&')[0]
            sid = mj_url_result['Data'].split('sid=')[1].split('&')[0]
            psid = mj_url_result['Data'].split('psid=')[1].split('&')[0]

            getbethistory_result = cd.getbethistory(cf=cf, url=url, data={
                                                                        't':t,
                                                                        'sid':psid,
                                                                        'gid':gid
                                                                        }, timeout=timeout)
            if getbethistory_result['ErrorCode'] == config.CONNECTION_CODE.code:
                return getbethistory_result
            if getbethistory_result['ErrorCode'] == config.SIGN_OUT_CODE.code:
                return getbethistory_result
            if getbethistory_result['ErrorCode'] != config.SUCCESS_CODE.code:
                return getbethistory_result['ErrorMessage']
            if not getbethistory_result['Data'].get('dt'): #網頁有問題
                fbkA.update({'ErrorCode': config.HTML_CONTENT_CODE.code,
                             'ErrorMessage': config.HTML_CONTENT_CODE.msg,
                             'BlockMemberLayer': BlockMemberLayer,
                             "GameName": GameName,
                             "WagersTimeString": WagersTimeString,
                             "WagersTimeStamp": WagersTimeStamp,
                             "BetAmount": BetAmount,
                             "AllCategoryCommissionable": AllCategoryCommissionable,
                             "GameCommissionable": GameCommissionable,
                             "SingleCategoryCommissionable": SingleCategoryCommissionable,
                             "CategoryName": CategoryName})
                return fbkA

            # data = [i for i in getbethistory_result['Data']['dt']['bh']['bd'] if i['tid'] == RawWagersId][0]
            data = [i for i in getbethistory_result['Data']['dt']['bh']['bd'] if i['gd']['fs']]
            # if data['gd']['fs']:
            if data:
                FreeSpin = 1
            else:
                FreeSpin = 0

        # [CQ9电子,JDB电子],[跳高高,跳高高2,跳起来,变脸,飞鸟派对]
        #elif Game_Id in ['2084','2085','2087','2505','1950'] and Game_category_Id in ['60','61']:
        else:
            if data['free'] == '是':
                FreeSpin = 1
            else:
                FreeSpin = 0

        if not int(ExtendLimit):
            fbkA.update({"IsSuccess": 1,
                         'ErrorCode': config.SUCCESS_CODE.code,
                         'ErrorMessage': config.SUCCESS_CODE.msg,
                         'BlockMemberLayer': BlockMemberLayer,
                         "GameName": GameName,
                         "WagersTimeString": WagersTimeString,
                         "WagersTimeStamp": WagersTimeStamp,
                         "BetAmount": BetAmount,
                         "AllCategoryCommissionable": AllCategoryCommissionable,
                         "GameCommissionable": GameCommissionable,
                         "SingleCategoryCommissionable": SingleCategoryCommissionable,
                         "CategoryName": CategoryName,
                         "FreeSpin":FreeSpin})
            return fbkA

        searchdate = SearchDate.replace('/','-') + ' ' + '00:00:00' + ' - ' + SearchDate.replace('/','-') + ' ' + '23:59:59' #格式化日期
        # 查詢各式投注金額
        GameCountType_result = cd.getgameplayhistorygrandtotal(cf=cf, url=url,
                                                                                data={
                                                                                    'search_bet_date':searchdate,
                                                                                    'search_vendor_game_categories':GameKind if GameCountType == '0' else Game_category_Id,
                                                                                    'search_games':Game_Id if GameCountType == '1' else '',
                                                                                    'search_usernames':ids
                                                                                    },
                                                                                    timeout=timeout
                                                                )
        if not GameCountType_result['IsSuccess']:
            Detail = f'搜尋調件: {Member}\\n搜尋結果: {GameCountType_result["ErrorMessge"]}'
        else:
            Detail = f'搜尋調件: {Member}\\n搜尋結果: {"是" if GameCountType_result.get("Data",{}).get("data",{}) else "否"}'

        GameCountType_str = '查询【选取分类当日投注】' if GameCountType == '0' else '查询【本游戏当日投注】' if GameCountType == '1' else '查询【本分类当日投注】'

        if SupportStatus:
            cls.return_schedule(Action='chkbbin', DBId=DBId, Member=Member, Status=GameCountType_str, Progress='1/1', Detail=Detail)
        else:
            cls.return_schedule(Action='chkbbin', DBId=DBId, Member=Member, current_status=GameCountType_str)


        if GameCountType_result['ErrorCode'] == config.CONNECTION_CODE.code:
            return GameCountType_result
        if GameCountType_result['ErrorCode'] == config.SIGN_OUT_CODE.code:
            return GameCountType_result
        if GameCountType_result['ErrorCode'] != config.SUCCESS_CODE.code:
            return GameCountType_result['ErrorMessage']

        if GameCountType_result['Data']['data']: #如無data欄位也會回傳空的list,因此不用get判斷
            if GameCountType == '0':  #★查詢【選取分類當日投注】
                AllCategoryCommissionable = GameCountType_result['Data']['data'][0]['total_effective_turnover'] or '0.00' #當無total_effective_turnover無值時會收到Null
                AllCategoryCommissionable = '.'.join([AllCategoryCommissionable.partition('.')[0],AllCategoryCommissionable.partition('.')[2][:2]]) #無條件捨去至小數第2位
            elif GameCountType == '1':  #★查詢【本遊戲當日投注】
                GameCommissionable = GameCountType_result['Data']['data'][0]['total_effective_turnover'] or '0.00' #當無total_effective_turnover無值時會收到Null
                GameCommissionable = '.'.join([GameCommissionable.partition('.')[0],GameCommissionable.partition('.')[2][:2]]) #無條件捨去至小數第2位
            else:  #★查詢【本分類當日投注】
                SingleCategoryCommissionable = GameCountType_result['Data']['data'][0]['total_effective_turnover'] or '0.00' #當無total_effective_turnover無值時會收到Null
                SingleCategoryCommissionable = '.'.join([SingleCategoryCommissionable.partition('.')[0],SingleCategoryCommissionable.partition('.')[2][:2]]) #無條件捨去至小數第2位
        else:
            logger.warning('計算總投注金額時CD回傳空list')
            logger.warning(f'回傳內容:{GameCountType_result["Data"]}')


        fbkA.update({"IsSuccess": 1,
                     'ErrorCode': config.SUCCESS_CODE.code,
                     'ErrorMessage': config.SUCCESS_CODE.msg,
                     'BlockMemberLayer': BlockMemberLayer,
                     "GameName": GameName,
                     "WagersTimeString": WagersTimeString,
                     "WagersTimeStamp": WagersTimeStamp,
                     "BetAmount": BetAmount,
                     "AllCategoryCommissionable": AllCategoryCommissionable,
                     "GameCommissionable": GameCommissionable,
                     "SingleCategoryCommissionable": SingleCategoryCommissionable,
                     "CategoryName": CategoryName,
                     "FreeSpin":FreeSpin})
        return fbkA


class experiencebonus(BaseFunc):
    '''CD 体验金'''


    @classmethod
    def deposit(cls, *args, **kwargs):
        '''
        CD 充值、移动层级
        需要有【会员管理 > 会员列表】权限。
        需要有【会员管理 > 会员列表 > 用户名 > 会员资料 > 人工存入】权限。
        需要有【报表管理 > 额度管理】权限。
        需要有【会员管理 > 会员层级】权限。
        需要有【会员管理 > 会员列表 > 会员资料 > 修改会员等级】权限。
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
            CD 体验金
            需要有【会员管理 > 会员列表】权限。
            需要有【会员管理 > 会员列表 > 会员资料 > 检测银行完整资讯】权限。
            需要有【风险管理 > IP查询 】权限。
        '''
        cls.th = ThreadProgress(kwargs['cf'], kwargs['mod_key'])
        cls.th.start()

        result = cls._audit(*args, **kwargs)

        cls.return_schedule(Action='chkbbin', DBId=kwargs['DBId'], Status='【审核机器人处理完毕】', Progress='-', Detail='-')
        cls.th.stop()
        return result


    @classmethod
    @keep_connect
    def _deposit(cls, url, DBId, Member, DepositAmount, mod_name, mod_key, amount_below,
        timeout, backend_remarks, multiple, cf, amount_memo='', Note='', increasethebet_switch=0, increasethebet='0',
        ChangeLayer=0, BlockMemberLayer='', **kwargs):
        '''CD 体验金 充值'''
        member = Member.lower()
        # 【充值】
        result_deposit = super().deposit(url, DBId, Member, DepositAmount, mod_name, mod_key, amount_below,
            timeout, backend_remarks, multiple, cf, amount_memo, Note, increasethebet_switch, increasethebet, **kwargs)
        if type(result_deposit) == str:
            return result_deposit
        if not result_deposit['IsSuccess']:
            return {**result_deposit, 'BlockMemberLayer': '-', 'LayerMessage': ''}
        if not int(ChangeLayer):
            return {**result_deposit, 'BlockMemberLayer': '-', 'LayerMessage': ''}

        count = 1
        levels = {}
        level_page = 1
        while True:
            # 【查詢層級清單】
            if locals().get('levelid') is None:
                result_levels = cd.searchGroup(cf, url, params={'page': level_page})
                for lv in result_levels.get('Data', {}).get('results', []):
                    levels[lv['name']] = lv['id']
                # 進度回傳
                if not result_levels["IsSuccess"]:
                    Detail = f'查询层级清单失敗\n{result_levels["ErrorMessage"]}'
                else:
                    Detail = f'查询层级清单成功\n层级清单：{[lv for lv in levels]}'
                cls.return_schedule(Action='chkpoint', DBId=DBId, Status='【查询层级清单】', Progress=f'{level_page}/{level_page}', Detail=Detail)
                # 連線異常重試
                if result_levels['ErrorCode'] == config.CONNECTION_CODE.code:
                    time.sleep(1)
                    continue
                # 被登出、查詢失敗結束
                if result_levels['ErrorCode'] == config.SIGN_OUT_CODE.code:
                    return {
                        'IsSuccess': 1,
                        'ErrorCode': result_deposit['ErrorCode'],
                        'ErrorMessage': result_deposit['ErrorMessage'],
                        'DBId': DBId,
                        'Member': Member,
                        'BlockMemberLayer': '-',
                        'LayerMessage': f'查询层级清单被登出，无法移动层级'
                    }
                if result_levels['ErrorCode'] != config.SUCCESS_CODE.code:
                    return {
                        'IsSuccess': 1,
                        'ErrorCode': result_deposit['ErrorCode'],
                        'ErrorMessage': result_deposit['ErrorMessage'],
                        'DBId': DBId,
                        'Member': Member,
                        'BlockMemberLayer': '-',
                        'LayerMessage': result_levels['ErrorMessage']
                    }
                # 沒查到需要的層級
                levelid = levels.get(BlockMemberLayer)
                if not levelid:
                    # 有下一頁，繼續查
                    if result_levels['Data']['pagination']['more']:
                        level_page += 1
                        continue
                    # 已經全部查完，回傳沒有該層級
                    else:
                        return {
                            'IsSuccess': 1,
                            'ErrorCode': config.LayerError.code,
                            'ErrorMessage': config.LayerError.msg.format(platform=cf.platform, msg=f'无【{BlockMemberLayer}】层级'),
                            'DBId': DBId,
                            'Member': Member,
                            'BlockMemberLayer': '-',
                            'LayerMessage': f'无【{BlockMemberLayer}】层级'
                        }

            # 【查詢會員ID】
            if locals().get('userid') is None:
                user_result = cd.searchusername(cf, url, data={'usernameArr': f'["{Member}"]'}, timeout=timeout)
                users = {u['username'].lower(): u for u in user_result.get('Data', {}).get('results', [])}
                userid = users.get(Member.lower(), {}).get('id')
                # 進度回傳
                if not user_result["IsSuccess"]:
                    Detail = f'查询失敗\n搜寻帐号：{Member}\n{user_result["ErrorMessage"]}'
                else:
                    Detail = f'查询成功\n搜寻帐号：{Member}\n会员是否存在：{"是" if userid else "否"}'
                    if userid:
                        Detail += f'\n会员ID：{userid}'
                cls.return_schedule(Action='chkpoint', DBId=DBId, Member=Member, Status='【会员列表】-【会员资料】', Progress='1/1', Detail=Detail)
                # 連線異常重試
                if user_result['ErrorCode'] == config.CONNECTION_CODE.code:
                    time.sleep(1)
                    continue
                # 被登出、查詢失敗結束
                if user_result['ErrorCode'] == config.SIGN_OUT_CODE.code:
                    return {
                        'IsSuccess': 1,
                        'ErrorCode': result_deposit['ErrorCode'],
                        'ErrorMessage': result_deposit['ErrorMessage'],
                        'DBId': DBId,
                        'Member': Member,
                        'BlockMemberLayer': '-',
                        'LayerMessage': f'查询会员ID被登出，无法移动层级'
                    }
                if user_result['ErrorCode'] != config.SUCCESS_CODE.code:
                    return {
                        'IsSuccess': 1,
                        'ErrorCode': result_deposit['ErrorCode'],
                        'ErrorMessage': result_deposit['ErrorMessage'],
                        'DBId': DBId,
                        'Member': Member,
                        'BlockMemberLayer': '-',
                        'LayerMessage': user_result['ErrorMessage']
                    }

            # 【查詢管理員ID】  (由activation_token取得，存放在session中)
            admin_id = getattr(cd.session, 'admin_id', None)
            if admin_id is None:
                result_token = cd.activation_token(cf, url)
                # 連線異常重試
                if result_token['ErrorCode'] == config.CONNECTION_CODE.code:
                    time.sleep(1)
                    continue
                # 被登出、查詢失敗結束
                if result_token['ErrorCode'] == config.SIGN_OUT_CODE.code:
                    return {
                        'IsSuccess': 1,
                        'ErrorCode': config.LayerError.code,
                        'ErrorMessage': config.LayerError.msg.format(platform=cf.platform, msg='查询管理员ID被登出，无法移动层级'),
                        'DBId': DBId,
                        'Member': Member,
                        'BlockMemberLayer': '-',
                        'LayerMessage': f'查询管理员ID被登出，无法移动层级'
                    }
                if result_token['ErrorCode'] != config.SUCCESS_CODE.code:
                    return {
                        'IsSuccess': 1,
                        'ErrorCode': result_deposit['ErrorCode'],
                        'ErrorMessage': result_deposit['ErrorMessage'],
                        'DBId': DBId,
                        'Member': Member,
                        'BlockMemberLayer': '-',
                        'LayerMessage': result_token['ErrorMessage']
                    }
                # 管理員ID取得失敗，彈跳視窗
                admin_id = getattr(cd.session, 'admin_id', None)
                if admin_id is None:
                    return '【移动层级】【管理员ID】取得失败，请联系开发团队'

            # 【移動層級】
            result_layer = cd.updateBulklist(cf, url,
                data={
                    'id': userid,
                    'admin_id': cd.session.admin_id,
                    'rf_cs_rForm_': cd.session.rf_cs_rForm,
                    'username': cd.session.acc,
                    'password': cd.session.pw,
                    'val': levelid,
                    'col': 'player_group_id',
                    'type': '1'
                },
                timeout=timeout)
            # 進度回傳
            if not result_layer["IsSuccess"]:
                Detail = f'层级移动失敗\n{result_layer["ErrorMessage"]}'
            else:
                Detail = f'层级移动成功'
            cls.return_schedule(Action='chkpoint', DBId=DBId, Status='【移动层级】', Progress='1/1', Detail=Detail)
            # 連線異常重試
            if result_layer['ErrorCode'] == config.CONNECTION_CODE.code:
                time.sleep(1)
                continue
            # 被登出、查詢失敗結束
            if result_layer['ErrorCode'] == config.SIGN_OUT_CODE.code:
                return {
                    'IsSuccess': 1,
                    'ErrorCode': result_deposit['ErrorCode'],
                    'ErrorMessage': result_deposit['ErrorMessage'],
                    'DBId': DBId,
                    'Member': Member,
                    'BlockMemberLayer': '-',
                    'LayerMessage': f'移动层级被登出，无法移动层级'
                }
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
    def _audit(cls, url, DBId, Member, SearchDate, AuditDays, timeout, cf, **kwargs):
        '''CD 体验金 监控'''
        member = Member.lower()
        fbkA = {
                'IsSuccess': 0,
                'ErrorCode': '',
                'ErrorMessage': '',
                'DBId': DBId,
                'Member': Member,
                'BlockMemberLayer': '',
                'RealName': '',
                'BankAccountExists': 0,
                'AutoAudit': 0,
                'RegisterTimeString': '',
                'RegisterTimeStamp': '',
            }

        #【会员列表】-【会员资料】1/2
        user_result = cd.searchusername(cf, url, data={'usernameArr': f'["{member}"]'}, timeout=timeout)
        users = {u['username'].lower(): u for u in user_result.get('Data', {}).get('results', [])}
        userid = users.get(member, {}).get('id')
        if not user_result["IsSuccess"]:
            Detail = f'查询失敗\n搜寻帐号：{Member}\n{user_result["ErrorMessage"]}'
        else:
            if user_result['Data'].get('results', {}):
                Detail = f'查询成功\n搜寻帐号：{Member}\n会员ID是否存在：{"是" if userid else "否"}'
            else:
                Detail = f'查询成功\n搜寻帐号：{Member}\n查无会员ID'
        cls.return_schedule(Action='chkbbin', DBId=DBId, Status='【会员列表】-【会员资料】', Progress='1/2', Detail=Detail)

        # 檢查登出或連線異常, 回傳後讓主程式重試
        if user_result["ErrorCode"] == config.CONNECTION_CODE.code:
            return user_result
        if user_result["ErrorCode"] == config.SIGN_OUT_CODE.code:
            return user_result
        # 查詢失敗結束
        if user_result["ErrorCode"] != config.SUCCESS_CODE.code:
            return user_result["ErrorMessage"]
        # 查詢無會員回傳
        if not userid:
            fbkA.update({'ErrorCode': config.NO_USER.code,
                         'ErrorMessage': config.NO_USER.msg})
            return fbkA

        #【会员列表】-【会员资料】2/2
        level_result = cd.memberlist_all(cf, url, data={'id': userid}, timeout=timeout)
        user = {u['id']: u for u in level_result.get('Data', {}).get('data', [])}.get(userid, {})
        fbkA['RealName'] = user.get('full_name', '')  #（真實姓名）
        fbkA['BlockMemberLayer'] = user.get('member_level', '')  #（會員層級）
        fbkA['RegisterTimeStamp'] = f"{int(user['created_at']) * 1000}"  #（註冊時間戳）
        fbkA['RegisterTimeString'] = datetime.datetime.fromtimestamp(int(user['created_at'])).strftime(r'%Y-%m-%d %H:%M:%S +0800')  #（註冊時間）
        if not level_result["IsSuccess"]:
            Detail = f'查询失敗\n搜寻帐号：{Member}\n{level_result["ErrorMessage"]}'
        else:
            Detail = f'查询成功\n搜寻帐号：{Member}'
        if fbkA['BlockMemberLayer']:
            Detail += f'\n会员层级：{fbkA["BlockMemberLayer"]}'
        if fbkA['RealName']:
            Detail += f'\n會員真實姓名：{fbkA["RealName"]}'
        if fbkA['RegisterTimeString']:
            Detail += f'\n會員註冊時間：{fbkA["RegisterTimeString"]}'
        cls.return_schedule(Action='chkbbin', DBId=DBId, Status='【会员列表】-【会员资料】', Progress='2/2', Detail=Detail)

        # 檢查登出或連線異常, 回傳後讓主程式重試
        if level_result['ErrorCode'] == config.CONNECTION_CODE.code:
            return level_result
        if level_result['ErrorCode'] == config.SIGN_OUT_CODE.code:
            return level_result
        # 查詢失敗結束
        if level_result['ErrorCode'] != config.SUCCESS_CODE.code:
            return level_result['ErrorMessage']
        # 查詢無會員回傳
        if not user:
            fbkA.update({'ErrorCode': config.NO_USER.code,
                         'ErrorMessage': config.NO_USER.msg})
            return fbkA

        #【会员资料】-【绑定银行卡】
        result_bank_account = cd.playerBankDetails_all(cf, url, params= {'player_id': userid, 'bank_status': '1'}, timeout=timeout)
        fbkA['BankAccountExists'] = int(bool(result_bank_account.get('Data', {}).get('data', [])))  #（綁定銀行卡）
        if not result_bank_account["IsSuccess"]:
            Detail = f'查询失敗\n{result_bank_account["ErrorMessage"]}'
        else:
            Detail = f'查询成功\n是否绑定银行卡：{"是" if fbkA["BankAccountExists"] else "否"}'
        cls.return_schedule(Action='chkbbin', DBId=DBId, Status='【会员资料】-【绑定银行卡】', Progress='1/1', Detail=Detail)

        # 檢查登出或連線異常, 回傳後讓主程式重試
        if result_bank_account['ErrorCode'] == config.SIGN_OUT_CODE.code:
            return result_bank_account
        if result_bank_account['ErrorCode'] == config.CONNECTION_CODE.code:
            return result_bank_account
        # 查詢失敗結束
        if result_bank_account['ErrorCode'] != config.SUCCESS_CODE.code:
            return result_bank_account['ErrorMessage']

        #【自动稽核】-【IP查询】
        end_time = int(datetime.datetime.strptime(f'{SearchDate} 23:59:59', '%Y/%m/%d %H:%M:%S').timestamp())
        start_time = int((datetime.datetime.strptime(SearchDate, r'%Y/%m/%d') - datetime.timedelta(days=int(AuditDays))).timestamp())

        result_record_auto = cd.ipEnquiryRecords(cf, url,
            params={'player_id': userid,
                    'start_time': start_time,
                    'end_time': end_time,
                    'type': 'login'},
            timeout=timeout)
        lst_ips = list(set([i['ip'] for i in result_record_auto.get('Data', {}).get('data', [])]))
        if not result_record_auto["IsSuccess"]:
            Detail = f'查询失敗\n{result_record_auto["ErrorMessage"]}'
        elif lst_ips:
            Detail = f'查询成功\n查询到的IP列表：{lst_ips}'
        else:
            Detail = f'查询成功\n搜寻帐号：{Member}\n查无会员IP列表'
        cls.return_schedule(Action='chkbbin', DBId=DBId, Status='【自动稽核】-【IP查询】', Progress='1/1', Detail=Detail)

        # 檢查登出或連線異常, 回傳後讓主程式重試
        if result_record_auto['ErrorCode'] == config.SIGN_OUT_CODE.code:
            return result_record_auto
        if result_record_auto['ErrorCode'] == config.CONNECTION_CODE.code:
            return result_record_auto
        # 查詢失敗結束
        if result_record_auto['ErrorCode'] != config.SUCCESS_CODE.code:
            return result_record_auto['ErrorMessage']

        lst_members = []
        if lst_ips:
            #【自动稽核】-【同IP帐号查询】
            for num, ip in enumerate(lst_ips):
                result_record_auto = cd.ipEnquiryRecords(cf, url,
                    params={'ip': ip,
                            'start_time': start_time,
                            'end_time': end_time,
                            'type': 'login'},
                    timeout=timeout)
                users = [record['username'].lower() for record in result_record_auto.get('Data', {}).get('data', [])]
                lst_members.extend(users)
                if not result_record_auto["IsSuccess"]:
                    Detail = f'查询失敗\n{result_record_auto["ErrorMessage"]}'
                elif users:
                    Detail = f'查询成功\n查询到的帐号列表：{list(set(lst_members))}'
                else:
                    Detail = f'查询成功\n搜寻帐号：{Member}\n查无同IP帐号列表'
                cls.return_schedule(Action='chkbbin', DBId=DBId, Status='【自动稽核】-【同IP帐号查询】', Progress=f'{num+1}/{len(lst_ips)}', Detail=Detail)
                if len(set(lst_members)) > 1:
                    Detail += '（查詢結束）'
                    cls.return_schedule(Action='chkbbin', DBId=DBId, Status='【自动稽核】-【同IP帐号查询】', Progress=f'{num+1}/{len(lst_ips)}', Detail=Detail)
                    break

                # 檢查登出或連線異常, 回傳後讓主程式重試
                if result_record_auto['ErrorCode'] == config.SIGN_OUT_CODE.code:
                    return result_record_auto
                if result_record_auto['ErrorCode'] == config.CONNECTION_CODE.code:
                    return result_record_auto
                # 查詢失敗結束
                if result_record_auto['ErrorCode'] != config.SUCCESS_CODE.code:
                    return result_record_auto['ErrorMessage']

        fbkA['AutoAudit'] = 1 if set(lst_members) - set([member]) else 0  #（自動稽核）

        # 回傳結果
        fbkA.update({
            'IsSuccess': 1,
            'ErrorCode': config.SUCCESS_CODE.code,
            'ErrorMessage': config.SUCCESS_CODE.msg
            })
        return fbkA


class registerbonus(BaseFunc):
    '''CD 注册红包'''


    @classmethod
    def deposit(cls, *args, **kwargs):
        '''
        CD 充值
        需要有【会员管理 > 会员列表】权限。
        需要有【会员管理 > 会员列表 > 用户名 > 会员资料 > 人工存入】权限。
        需要有【报表管理 > 额度管理】权限。
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
            CD 注册红包
            需要有【会员管理 > 会员列表】权限。
            需要有【会员管理 > 会员列表 > 会员资料 > 检测银行完整资讯】权限。
            需要有【风险管理 > IP查询】权限。
            需要有【会员管理 > 会员列表】权限。
            需要有【报表管理 > 会员报表】权限。
        '''
        cls.th = ThreadProgress(kwargs['cf'], kwargs['mod_key'])
        cls.th.start()

        result = cls._audit(*args, **kwargs)

        cls.return_schedule(Action='chkbbin', DBId=kwargs['DBId'], Status='【审核机器人处理完毕】', Progress='-', Detail='-')
        cls.th.stop()
        return result


    @classmethod
    @keep_connect
    def _deposit(cls, url, DBId, Member, DepositAmount, mod_name, mod_key, amount_below,
        timeout, backend_remarks, multiple, cf, amount_memo='', Note='', increasethebet_switch=0, increasethebet='0',
        ChangeLayer=0, BlockMemberLayer='', **kwargs):
        '''CD 注册红包 充值'''
        return super().deposit(url, DBId, Member, DepositAmount, mod_name, mod_key, amount_below,
            timeout, backend_remarks, multiple, cf, amount_memo, Note, increasethebet_switch, increasethebet, **kwargs)


    @classmethod
    @keep_connect
    def _audit(cls, url, DBId, Member, SearchDate, AuditDays, timeout, cf, **kwargs):
        '''CD 注册红包 监控'''
        member = Member.lower()
        fbkA = {
                'IsSuccess': 0,
                'ErrorCode': '',
                'ErrorMessage': '',
                'DBId': DBId,
                'Member': Member,
                'BlockMemberLayer': '',
                'RealName': '',
                'BankAccountExists': 0,
                'AutoAudit': 0,
                'CumulativeDepositAmount': '0.00',
                'CumulativeDepositsTimes': '0',
            }

        #【会员列表】-【会员资料】1/2
        user_result = cd.searchusername(cf, url, data={'usernameArr': f'["{member}"]'}, timeout=timeout)
        users = {u['username'].lower(): u for u in user_result.get('Data', {}).get('results', [])}
        userid = users.get(member, {}).get('id')
        if not user_result["IsSuccess"]:
            Detail = f'查询失敗\n搜寻帐号：{Member}\n{user_result["ErrorMessage"]}'
        else:
            if user_result['Data'].get('results', {}):
                Detail = f'查询成功\n搜寻帐号：{Member}\n会员ID是否存在：{"是" if userid else "否"}'
            else:
                Detail = f'查询成功\n搜寻帐号：{Member}\n查无会员ID'
        cls.return_schedule(Action='chkbbin', DBId=DBId, Status='【会员列表】-【会员资料】', Progress='1/2', Detail=Detail)

        # 檢查登出或連線異常, 回傳後讓主程式重試
        if user_result["ErrorCode"] == config.CONNECTION_CODE.code:
            return user_result
        if user_result["ErrorCode"] == config.SIGN_OUT_CODE.code:
            return user_result
        # 查詢失敗結束
        if user_result["ErrorCode"] != config.SUCCESS_CODE.code:
            return user_result["ErrorMessage"]
        # 查詢無會員回傳
        if not userid:
            fbkA.update({'ErrorCode': config.NO_USER.code,
                         'ErrorMessage': config.NO_USER.msg})
            return fbkA

        #【会员列表】-【会员资料】2/2
        level_result = cd.memberlist_all(cf, url, data={'id': userid}, timeout=timeout)
        user = {u['id']: u for u in level_result.get('Data', {}).get('data', [])}.get(userid, {})
        fbkA['RealName'] = user.get('full_name', '')  #（真實姓名）
        fbkA['BlockMemberLayer'] = user.get('member_level', '')  #（會員層級）
        fbkA['CumulativeDepositAmount'] = f'{float(user.get("lifetime_deposit_total_amt", "0.00")):.2f}'  #（累積存款金額）      
        fbkA['CumulativeDepositsTimes'] = f'{float(user.get("lifetime_deposit_no", "0")):.0f}'  #（累積存款次數） 

        if not level_result["IsSuccess"]:
            Detail = f'查询失敗\n搜寻帐号：{Member}\n{level_result["ErrorMessage"]}'
        else:
            Detail = f'查询成功\n搜寻帐号：{Member}'
            Detail += f'\n会员是否存在：{"是" if user else "否"}'
        if fbkA['BlockMemberLayer']:
            Detail += f'\n会员层级：{fbkA["BlockMemberLayer"]}'
        if fbkA['RealName']:
            Detail += f'\n會員真實姓名：{fbkA["RealName"]}'
        if fbkA['CumulativeDepositAmount']:
            Detail += f'\n累積存款金額：{fbkA["CumulativeDepositAmount"]}'
        if fbkA['CumulativeDepositsTimes']:
            Detail += f'\n累積存款次數：{fbkA["CumulativeDepositsTimes"]}'
        cls.return_schedule(Action='chkbbin', DBId=DBId, Status='【会员列表】-【会员资料】', Progress='2/2', Detail=Detail)

        # 檢查登出或連線異常, 回傳後讓主程式重試
        if level_result['ErrorCode'] == config.CONNECTION_CODE.code:
            return level_result
        if level_result['ErrorCode'] == config.SIGN_OUT_CODE.code:
            return level_result
        # 查詢失敗結束
        if level_result['ErrorCode'] != config.SUCCESS_CODE.code:
            return level_result['ErrorMessage']
        # 查詢無會員回傳
        if not user:
            fbkA.update({'ErrorCode': config.NO_USER.code,
                         'ErrorMessage': config.NO_USER.msg})
            return fbkA

        #【会员资料】-【绑定银行卡】
        result_bank_account = cd.playerBankDetails_all(cf, url, params= {'player_id': userid, 'bank_status': '1'}, timeout=timeout)
        fbkA['BankAccountExists'] = int(bool(result_bank_account.get('Data', {}).get('data', [])))  #（綁定銀行卡）
        if not result_bank_account["IsSuccess"]:
            Detail = f'查询失敗\n{result_bank_account["ErrorMessage"]}'
        else:
            Detail = f'查询成功\n是否绑定银行卡：{"是" if fbkA["BankAccountExists"] else "否"}'
        cls.return_schedule(Action='chkbbin', DBId=DBId, Status='【会员资料】-【绑定银行卡】', Progress='1/1', Detail=Detail)

        # 檢查登出或連線異常, 回傳後讓主程式重試
        if result_bank_account['ErrorCode'] == config.SIGN_OUT_CODE.code:
            return result_bank_account
        if result_bank_account['ErrorCode'] == config.CONNECTION_CODE.code:
            return result_bank_account
        # 查詢失敗結束
        if result_bank_account['ErrorCode'] != config.SUCCESS_CODE.code:
            return result_bank_account['ErrorMessage']

        #【自动稽核】-【IP查询】
        end_time = int(datetime.datetime.strptime(f'{SearchDate} 23:59:59', '%Y/%m/%d %H:%M:%S').timestamp())
        start_time = int((datetime.datetime.strptime(SearchDate, r'%Y/%m/%d') - datetime.timedelta(days=int(AuditDays))).timestamp())

        result_record_auto = cd.ipEnquiryRecords(cf, url,
            params={'player_id': userid,
                    'start_time': start_time,
                    'end_time': end_time,
                    'type': 'login'},
            timeout=timeout)
        lst_ips = list(set([i['ip'] for i in result_record_auto.get('Data', {}).get('data', [])]))
        if not result_record_auto["IsSuccess"]:
            Detail = f'查询失敗\n{result_record_auto["ErrorMessage"]}'
        elif lst_ips:
            Detail = f'查询成功\n查询到的IP列表：{lst_ips}'
        else:
            Detail = f'查询成功\n搜寻帐号：{Member}\n查无会员IP列表'
        cls.return_schedule(Action='chkbbin', DBId=DBId, Status='【自动稽核】-【IP查询】', Progress='1/1', Detail=Detail)

        # 檢查登出或連線異常, 回傳後讓主程式重試
        if result_record_auto['ErrorCode'] == config.SIGN_OUT_CODE.code:
            return result_record_auto
        if result_record_auto['ErrorCode'] == config.CONNECTION_CODE.code:
            return result_record_auto
        # 查詢失敗結束
        if result_record_auto['ErrorCode'] != config.SUCCESS_CODE.code:
            return result_record_auto['ErrorMessage']

        lst_members = []
        if lst_ips:
            #【自动稽核】-【同IP帐号查询】
            for num, ip in enumerate(lst_ips):
                result_record_auto = cd.ipEnquiryRecords(cf, url,
                    params={'ip': ip,
                            'start_time': start_time,
                            'end_time': end_time,
                            'type': 'login'},
                    timeout=timeout)
                users = [record['username'].lower() for record in result_record_auto.get('Data', {}).get('data', [])]
                lst_members.extend(users)
                if not result_record_auto["IsSuccess"]:
                    Detail = f'查询失敗\n{result_record_auto["ErrorMessage"]}'
                elif users:
                    Detail = f'查询成功\n查询到的帐号列表：{list(set(lst_members))}'
                else:
                    Detail = f'查询成功\n搜寻帐号：{Member}\n查无同IP帐号列表'
                cls.return_schedule(Action='chkbbin', DBId=DBId, Status='【自动稽核】-【同IP帐号查询】', Progress=f'{num+1}/{len(lst_ips)}', Detail=Detail)
                if len(set(lst_members)) > 1:
                    Detail += '（查詢結束）'
                    cls.return_schedule(Action='chkbbin', DBId=DBId, Status='【自动稽核】-【同IP帐号查询】', Progress=f'{num+1}/{len(lst_ips)}', Detail=Detail)
                    break

                # 檢查登出或連線異常, 回傳後讓主程式重試
                if result_record_auto['ErrorCode'] == config.SIGN_OUT_CODE.code:
                    return result_record_auto
                if result_record_auto['ErrorCode'] == config.CONNECTION_CODE.code:
                    return result_record_auto
                # 查詢失敗結束
                if result_record_auto['ErrorCode'] != config.SUCCESS_CODE.code:
                    return result_record_auto['ErrorMessage']

        fbkA['AutoAudit'] = 1 if set(lst_members) - set([member]) else 0  #（自動稽核）

        # #【报表管理】-【会员报表】
        # end_time = int(datetime.datetime.now().timestamp())  #（機器人查詢之時）
        # start_time = int(user['created_at'])  #（註冊時間戳）
        # startStr = datetime.datetime.fromtimestamp(start_time).strftime(r'%Y-%m-%d')
        # endStr = datetime.datetime.fromtimestamp(end_time).strftime(r'%Y-%m-%d')
        # search_date = f'{startStr} - {endStr}'

        # result_saving = cd.creditsTotalOptimized(cf, url,
        #     data={'search_date': search_date,
        #           'search_players': userid},
        #     timeout=timeout)

        # if not result_saving["IsSuccess"]:
        #     Detail = f'查询失敗\n{result_saving["ErrorMessage"]}'
        # else:
        #     Detail = f'查询成功\n搜寻帐号：{Member}\n累積存款金額/次數'
        # cls.return_schedule(Action='chkbbin', DBId=DBId, Status='【报表管理】-【会员报表】', Progress='1/1', Detail=Detail)

        # # 檢查登出或連線異常, 回傳後讓主程式重試
        # if result_saving['ErrorCode'] == config.SIGN_OUT_CODE.code:
        #     return result_saving
        # if result_saving['ErrorCode'] == config.CONNECTION_CODE.code:
        #     return result_saving
        # # 查詢失敗結束
        # if result_saving['ErrorCode'] != config.SUCCESS_CODE.code:
        #     return result_saving['ErrorMessage']

        # fbkA['CumulativeDepositAmount'] = result_saving['Data']['total_deposit']  #（存款金額）      
        # fbkA['CumulativeDepositsTimes'] = result_saving['Data']['total_deposit_count']  #（存款次數） 

        # 回傳結果
        fbkA.update({
            'IsSuccess': 1,
            'ErrorCode': config.SUCCESS_CODE.code,
            'ErrorMessage': config.SUCCESS_CODE.msg
            })
        return fbkA



class bettingbonus(BaseFunc):
    '''CD 注注有奖'''


    @classmethod
    def deposit(cls, *args, **kwargs):
        '''
        CD 充值
        需要有【会员管理 > 会员列表】权限。
        需要有【会员管理 > 会员列表 > 用户名 > 会员资料 > 人工存入】权限。
        需要有【报表管理 > 额度管理】权限。
        '''
        cls.th = ThreadProgress(kwargs['cf'], kwargs['mod_key'])
        cls.th.start()

        # result = cls._deposit(*args, **kwargs)
        result = super().deposit(*args, **kwargs)

        cls.return_schedule(Action='chkpoint', DBId=kwargs['DBId'], Status='【充值机器人处理完毕】', Progress='-', Detail='-')
        cls.th.stop()
        return result


    @classmethod
    def audit(cls, *args, **kwargs):
        '''
            CD 注注有奖
            需要有【会员管理 > 会员列表】权限。
            需要有【报表管理 > 投注记录】权限。
        '''
        cls.th = ThreadProgress(kwargs['cf'], kwargs['mod_key'])
        cls.th.start()

        result = cls._audit(*args, **kwargs)

        cls.return_schedule(Action='chkbbin', DBId=kwargs['DBId'], Status='【审核机器人处理完毕】', Progress='-', Detail='-')
        cls.th.stop()
        return result


    @classmethod
    @keep_connect
    def _audit(cls, url, DBId, Member, AmountAbove, SearchGameCategory, SearchDate, timeout, cf, **kwargs):
        '''CD 注注有奖 监控'''
        member = Member.lower()
        fbkA = {
                'IsSuccess': 0,
                'ErrorCode': '',
                'ErrorMessage': '',
                'DBId': DBId,
                'Member': Member,
                'BlockMemberLayer': '',
                'BetList': [],
            }

        #【会员列表】-【会员资料】1/2
        user_result = cd.searchusername(cf, url, data={'usernameArr': f'["{member}"]'}, timeout=timeout)
        users = {u['username'].lower(): u for u in user_result.get('Data', {}).get('results', [])}
        userid = users.get(member, {}).get('id')
        if not user_result["IsSuccess"]:
            Detail = f'查询失敗\n搜寻帐号：{Member}\n{user_result["ErrorMessage"]}'
        else:
            if user_result['Data'].get('results', {}):
                Detail = f'查询成功\n搜寻帐号：{Member}\n会员ID是否存在：{"是" if userid else "否"}'
            else:
                Detail = f'查询成功\n搜寻帐号：{Member}\n查无会员ID'
        cls.return_schedule(Action='chkbbin', DBId=DBId, Status='【会员列表】-【会员资料】', Progress='1/2', Detail=Detail)

        # 檢查登出或連線異常, 回傳後讓主程式重試
        if user_result["ErrorCode"] == config.CONNECTION_CODE.code:
            return user_result
        if user_result["ErrorCode"] == config.SIGN_OUT_CODE.code:
            return user_result
        # 查詢失敗結束
        if user_result["ErrorCode"] != config.SUCCESS_CODE.code:
            return user_result["ErrorMessage"]
        # 查詢無會員回傳
        if not userid:
            fbkA.update({'ErrorCode': config.NO_USER.code,
                         'ErrorMessage': config.NO_USER.msg})
            return fbkA

        #【会员列表】-【会员资料】2/2
        level_result = cd.memberlist_all(cf, url, data={'id': userid}, timeout=timeout)
        user = {u['id']: u for u in level_result.get('Data', {}).get('data', [])}.get(userid, {})
        fbkA['BlockMemberLayer'] = user.get('member_level', '')  #（會員層級）
        if not level_result["IsSuccess"]:
            Detail = f'查询失敗\n搜寻帐号：{Member}\n{level_result["ErrorMessage"]}'
        else:
            Detail = f'查询成功\n搜寻帐号：{Member}'
        if fbkA['BlockMemberLayer']:
            Detail += f'\n会员层级：{fbkA["BlockMemberLayer"]}'
        cls.return_schedule(Action='chkbbin', DBId=DBId, Status='【会员列表】-【会员资料】', Progress='2/2', Detail=Detail)

        # 檢查登出或連線異常, 回傳後讓主程式重試
        if level_result['ErrorCode'] == config.CONNECTION_CODE.code:
            return level_result
        if level_result['ErrorCode'] == config.SIGN_OUT_CODE.code:
            return level_result
        # 查詢失敗結束
        if level_result['ErrorCode'] != config.SUCCESS_CODE.code:
            return level_result['ErrorMessage']

        # 查詢無會員回傳
        if not user:
            fbkA.update({'ErrorCode': config.NO_USER.code,
                         'ErrorMessage': config.NO_USER.msg})
            return fbkA

        #查尋注單金額
        cls.return_schedule(Action='chkbbin', DBId=DBId, Member=Member, current_status='查詢注單金額')

        #(1)查前一日
        # date = datetime.datetime.strptime(SearchDate,'%Y/%m/%d')
        # pre_date = date + datetime.timedelta(days=-1)
        # pre_date = datetime.datetime.strftime(pre_date, '%Y-%m-%d')
        # totalday = f'{pre_date} 00:00:00 - {pre_date} 23:59:59'

        #(2)查當日
        date = SearchDate.replace('/', '-')
        totalday = f'{date} 00:00:00 - {date} 23:59:59'

        GameKind = ','.join(SearchGameCategory)
        bets_result = cd.betsdata(cf=cf, url=url, data={
                                                        'search_usernames': userid,
                                                        'search_bet_date':totalday,#資料格式:'2020-11-25 00:00:00 - 2020-11-26 23:59:59'
                                                        'search_vendor_game_categories':GameKind
                                                         },timeout=timeout)
        fbkA['BetList'] = [str({'GameCategory': ''.join(i['vendor'].split()), 'GameName': i['game_name'], 'BetAmount': f"{eval(i['effective_turnover']):.2f}"}) for i in bets_result['Data']['data'] if eval(i['effective_turnover']) >= eval(AmountAbove)]

        if not bets_result["IsSuccess"]:
            Detail = f'查询失敗\n搜寻帐号：{Member}\n{bets_result["ErrorMessage"]}'
        else:
            Detail = f'查询成功\n搜寻帐号：{Member}'
        if fbkA['BetList']:
            Detail += f'\n注单笔数：{len(fbkA["BetList"])}'
        cls.return_schedule(Action='chkbbin', DBId=DBId, Status='【投注记录】-【注单资料】', Progress='1/1', Detail=Detail)

        # 檢查登出或連線異常, 回傳後讓主程式重試
        if bets_result['ErrorCode'] == config.CONNECTION_CODE.code:
            return bets_result
        if bets_result['ErrorCode'] == config.SIGN_OUT_CODE.code:
            return bets_result
        # 查詢失敗結束
        if bets_result['ErrorCode'] != config.SUCCESS_CODE.code:
            return bets_result['ErrorMessage']

        # if not bets_result['Data']['data']:
        #     #查無注單
        #     fbkA.update({'ErrorCode': config.WAGERS_NOT_FOUND.code,
        #                  'ErrorMessage': config.WAGERS_NOT_FOUND.msg})
        #     return fbkA

        # fbkA['BetList'] = [dict(**json.loads(i.replace("'", '"')), count=j) for i, j in dict(Counter(fbkA['BetList'])).items()]
        # fbkA['BetList'] = [{**eval(i), 'count': j} for i, j in dict(Counter(fbkA['BetList'])).items()]
        fbkA['BetList'] = [{**eval(i), 'count': fbkA['BetList'].count(i)} for i in set(fbkA['BetList'])]


        # 回傳結果
        fbkA.update({
            'IsSuccess': 1,
            'ErrorCode': config.SUCCESS_CODE.code,
            'ErrorMessage': config.SUCCESS_CODE.msg
            })
        return fbkA



class apploginbonus(BaseFunc):
    '''CD APP登入礼'''


    @classmethod
    def deposit(cls, *args, **kwargs):
        '''
        CD 充值、推播通知
        需要有【会员管理 > 会员列表】权限。
        需要有【会员管理 > 会员列表 > 用户名 > 会员资料 > 人工存入】权限。
        需要有【报表管理 > 额度管理】权限。
        需要有【会员管理 > 会员层级】权限。
        需要有【网站管理 > 信息管理 > 站内信 > 新增】权限。
        '''
        cls.th = ThreadProgress(kwargs['cf'], kwargs['mod_key'])
        cls.th.start()

        result = cls._deposit(*args, **kwargs)

        cls.return_schedule(Action='chkpoint', DBId=kwargs['DBId'], Status='【充值机器人处理完毕】', Progress='-', Detail='-')
        cls.th.stop()
        return result


    # @classmethod
    # def audit(cls, *args, **kwargs):
    #     '''
    #         CD APP登入礼
    #         需要有【会员管理 > 会员列表】权限。
    #         需要有【风险管理 > IP查询 】权限。
    #     '''
    #     # cls.th = ThreadProgress(kwargs['cf'], kwargs['mod_key'])
    #     # cls.th.start()

    #     result = cls._audit(*args, **kwargs)

    #     # cls.return_schedule(Action='chkbbin', DBId=kwargs['DBId'], Status='【审核机器人处理完毕】', Progress='-', Detail='-')
    #     # cls.th.stop()
    #     return result


    @classmethod
    @keep_connect
    def _deposit(cls, url, DBId, Member, DepositAmount, mod_name, mod_key, amount_below,
        timeout, backend_remarks, multiple, cf, amount_memo='', Note='', increasethebet_switch=0, increasethebet='0',
        ChangeLayer=0, BlockMemberLayer='', NotifyTitle='', NotifyContent='', **kwargs):
        '''CD APP登入礼 充值'''
        member = Member.lower()
        # 【充值】
        result_deposit = super().deposit(url, DBId, Member, DepositAmount, mod_name, mod_key, amount_below,
            timeout, backend_remarks, multiple, cf, amount_memo, Note, increasethebet_switch, increasethebet, **kwargs)
        if type(result_deposit) == str:
            return result_deposit
        if not result_deposit['IsSuccess']:
            # return {**result_deposit, 'NotifyMessage': ''}
            return {
                'IsSuccess': result_deposit['IsSuccess'],
                'ErrorCode': result_deposit['ErrorCode'],
                'ErrorMessage': result_deposit['ErrorMessage'],
                'DBId': DBId,
                'Member': Member,
                'NotifyMessage': ''
            }

        levels = {}
        level_page = 1
        while True:
            # 【查詢會員ID】
            if locals().get('userid') is None:
                user_result = cd.searchusername(cf, url, data={'usernameArr': f'["{Member}"]'}, timeout=timeout)
                users = {u['username'].lower(): u for u in user_result.get('Data', {}).get('results', [])}
                userid = users.get(Member.lower(), {}).get('id')
                # 進度回傳
                if not user_result["IsSuccess"]:
                    Detail = f'查询失敗\n搜寻帐号：{Member}\n{user_result["ErrorMessage"]}'
                else:
                    Detail = f'查询成功\n搜寻帐号：{Member}\n会员是否存在：{"是" if userid else "否"}'
                    if userid:
                        Detail += f'\n会员ID：{userid}'
                cls.return_schedule(Action='chkpoint', DBId=DBId, Member=Member, Status='【会员列表】-【会员资料】', Progress='1/1', Detail=Detail)
                # 連線異常重試
                if user_result['ErrorCode'] == config.CONNECTION_CODE.code:
                    time.sleep(1)
                    continue
                # 被登出、查詢失敗結束
                if user_result['ErrorCode'] == config.SIGN_OUT_CODE.code:
                    return {
                        'IsSuccess': result_deposit['IsSuccess'],
                        'ErrorCode': user_result['ErrorCode'],
                        'ErrorMessage': '查询会员ID被登出，无法移动层级',
                        'DBId': DBId,
                        'Member': Member,
                        'NotifyMessage': ''
                    }
                if user_result['ErrorCode'] != config.SUCCESS_CODE.code:
                    return {
                        'IsSuccess': result_deposit['IsSuccess'],
                        'ErrorCode': user_result['ErrorCode'],
                        'ErrorMessage': user_result['ErrorMessage'],
                        'DBId': DBId,
                        'Member': Member,
                        'NotifyMessage': ''
                    }

            # 【推播通知】
            result_notify = cd.messagestore(cf, url,
                data = {
                    'csv': 'undefined',
                    'send_to': 'selected_players',
                    'send_at': 'now',
                    'scheduled_time': '',
                    'template_id': '无模板',
                    'title': NotifyTitle,
                    'message': NotifyContent,
                    'app_message': 'Hello _var_',
                    'rf_cs_rForm_': cd.session.rf_cs_rForm,
                    'selected_players': userid},
                timeout=timeout)
            # 進度回傳
            if not result_notify["IsSuccess"]:
                Detail = f'推播通知失敗\n{result_notify["ErrorMessage"]}'
            else:
                Detail = f'推播通知成功'
            cls.return_schedule(Action='chkpoint', DBId=DBId, Status='【推播通知】', Progress='1/1', Detail=Detail)
            # 連線異常重試
            if result_notify['ErrorCode'] == config.CONNECTION_CODE.code:
                time.sleep(1)
                continue
            # 被登出、查詢失敗結束
            if result_notify['ErrorCode'] == config.SIGN_OUT_CODE.code:
                return {
                    'IsSuccess': result_deposit['IsSuccess'],
                    'ErrorCode': result_notify['ErrorCode'],
                    'ErrorMessage': '推播通知被登出，无法移动层级',
                    'DBId': DBId,
                    'Member': Member,
                    'NotifyMessage': ''
                }
            if result_notify['ErrorCode'] != config.SUCCESS_CODE.code:
                return {
                    'IsSuccess': result_deposit['IsSuccess'],
                    'ErrorCode': result_notify['ErrorCode'],
                    'ErrorMessage': result_notify['ErrorMessage'],
                    'DBId': DBId,
                    'Member': Member,
                    'NotifyMessage': result_notify['Data']['message']
                }
            return {
                'IsSuccess': result_deposit['IsSuccess'],
                'ErrorCode': config.SUCCESS_CODE.code,
                'ErrorMessage': config.SUCCESS_CODE.msg,
                'DBId': DBId,
                'Member': Member,
                'NotifyMessage': result_notify['Data']['message']
            }


    @classmethod
    @keep_connect
    def _audit(cls, url, timeout, cf, mod_key=None, 
        AuditAPP=1, AuditUniversal=0,AuditMobilesite=0,AuditUniversalPc=0,AuditUniversalApp=0,AuditCustomizationApp=0, ImportColumns=[], **kwargs):
        '''CD APP登入礼 监控'''

        pathDir = Path(r'.\config\data') / (mod_key or '.')/'CD'
        if not pathDir.exists():
                pathDir.mkdir()
        now = datetime.datetime.now()

        logger.info(pathDir.iterdir())
        while True:
            if pathDir.exists():
                start_time = f"{now.strftime('%Y-%m-%d')} 00:00:00"
                csvList = sorted([i.name for i in pathDir.iterdir() if i.suffix=='.csv'], reverse=True)
                if csvList:
                    logger.info(f'●latest【{csvList[0]}】')
                    if (time.strftime('%Y-%m-%d') == csvList[0].split('.')[0] or
                       now.strftime('%Y-%m-%d') == csvList[0].split('.')[0]):
                        with open(str(pathDir)+'\\'+csvList[0], encoding='utf-8') as f:
                            file = csv.reader(f)
                            data = list(file)
                        if data:
                            start_time = data[-1][3]
                else:
                    logger.info('●no【.csv】')
                break
            else:
                logger.info(f'●no【{str(pathDir)}】go create')
                pathDir.mkdir()

        interval = int(cf['update_times'])
        end_time = (now - datetime.timedelta(seconds=interval+5)).strftime('%Y-%m-%d %H:%M:%S')

        dt1 = int(datetime.datetime.strptime(start_time, '%Y-%m-%d %H:%M:%S').timestamp())
        dt2 = int(datetime.datetime.strptime(end_time, '%Y-%m-%d %H:%M:%S').timestamp())
        logger.info(f'●start_time: {start_time} ({dt1}) | ●end_time: {end_time} ({dt2})')

        # 【查詢會員登錄】
        def getData(cf, url, timeout, mode, dt1, dt2):
            params = {
                'start_time': dt1,
                'end_time': dt2,
                'length': '1000000',
                'device': 'app' if mode == 'AuditAPP' else '',
                'browser': 'UB' if mode == 'AuditUniversal' else '',
            }
            result_enquiry = cd.accountEnquiryRecords(cf, url,
                                                      params=params,
                                                      timeout=timeout)

            # 檢查登出或連線異常, 回傳後讓主程式重試
            if result_enquiry["ErrorCode"] == config.CONNECTION_CODE.code:
                return False, result_enquiry
            if result_enquiry["ErrorCode"] == config.SIGN_OUT_CODE.code:
                return False, result_enquiry
            # 查詢失敗結束
            if result_enquiry["ErrorCode"] != config.SUCCESS_CODE.code:
                return False, result_enquiry["ErrorMessage"]

            return True, result_enquiry['Data']


        listData = []
        for mode in ['AuditAPP' if int(AuditAPP) else '', 'AuditUniversal' if int(AuditUniversal) else '']:
            if mode:
                fbk, content = getData(cf, url, timeout, mode, dt1, dt2)
                if fbk:
                    if ImportColumns:
                        listData += [[i['username'],  # 會員帳號一定要有
                                      i['member_level'] if 'BlockMemberLayer' in ImportColumns else '',
                                      i['vip_level'] if 'VipLevel' in ImportColumns else '',
                                      datetime.datetime.fromtimestamp(int(i['last_login_time'])).strftime('%Y-%m-%d %H:%M:%S')  if 'LoginDateTime' in ImportColumns else ''] 
                                      for i in content['data']]
                else:
                    return content                                    

        userDict = {}
        if ImportColumns:
            if 'CumulativeDepositAmount' in ImportColumns or 'CumulativeDepositsTimes' in ImportColumns:
                memberList = [i[0] for i in listData]

                num = len(memberList)
                for i in range((num//100) if num%100 == 0 else (num//100)+1):
                    memberAll = ','.join([f'"{i}"' for i in memberList[(100*i):100+(100*i)]])  # 一次頂多查100筆
                    members = f'[{memberAll}]'
                    r = cd.searchusername(cf, url, data={'usernameArr': members}, timeout=timeout)
                    # 檢查登出或連線異常, 回傳後讓主程式重試
                    if r["ErrorCode"] == config.CONNECTION_CODE.code:
                        return r
                    if r["ErrorCode"] == config.SIGN_OUT_CODE.code:
                        return r
                    # 查詢失敗結束
                    if r["ErrorCode"] != config.SUCCESS_CODE.code:
                        return r["ErrorMessage"]

                    idList = [i['id'] for i in r['Data']['results']]
                    ids = ','.join(idList)
                    r = cd.memberlist_all(cf, url, data={'length': '1000000', 'id': ids}, timeout=30)
                    # 檢查登出或連線異常, 回傳後讓主程式重試
                    if r["ErrorCode"] == config.CONNECTION_CODE.code:
                        return r
                    if r["ErrorCode"] == config.SIGN_OUT_CODE.code:
                        return r
                    # 查詢失敗結束
                    if r["ErrorCode"] != config.SUCCESS_CODE.code:
                        return r["ErrorMessage"]

                    userDictTemp = {i['username']: [i['lifetime_deposit_total_amt'] if 'CumulativeDepositAmount' in ImportColumns else '',  #（累積存款金額）
                                    i['lifetime_deposit_no'] if 'CumulativeDepositsTimes' in ImportColumns else '']  #（累積存款次數）
                                    for i in r.get('Data', {}).get('data', [])}   
                    userDict.update(userDictTemp)
                    time.sleep(.1) 

        if userDict:   
            listData = [i + userDict[i[0]] for i in listData]

        if {'Member', 'BlockMemberLayer', 'LoginDateTime','VipLevel', 'CumulativeDepositAmount', 'CumulativeDepositsTimes'} - set(ImportColumns):
            listData = [[j for j in i if j] for i in listData]  # 去除空值      

        listData = sorted(listData, key=lambda x: x[2])  # 以「登錄時間」排序
        numD = len(listData)
        msg = f'●共：{numD}筆\n●第1筆：{listData[0]}\n●最後1筆：{listData[-1]}' if numD else f'●共：{numD}筆'
        logger.info(msg)

        # 回傳結果
        return {
            'IsSuccess': 1,
            'ErrorCode': config.SUCCESS_CODE.code,
            'ErrorMessage': config.SUCCESS_CODE.msg,
            'Data': listData,
        }

    @classmethod
    @keep_connect
    def test_audit(cls, url, timeout, cf, pr6_time, pr6_zone, platform_time, mod_key=None,
                   AuditAPP=1, AuditUniversal=0, AuditMobilesite=0, AuditUniversalPc=0, AuditUniversalApp=0,
                   AuditCustomizationApp=0, ImportColumns=[], **kwargs):
        '''CD APP登入礼 监控'''

        path = Path(r'.\config\data') / (mod_key or '.') / 'CD'
        if not path.exists():
            path.mkdir()

        csv_date = [i.stem for i in path.iterdir() if '.csv' == i.suffix]  # 資料夾內所有.csv檔案list

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

        platform_time = platform_time.replace(tzinfo=None)
        logger.info(f'設定檔{cf}\n pr6--時區:{pr6_zone} 時間:{pr6_time}\n 平台時間(北京):{platform_time}')
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

        if not isinstance(pr6_time, str):  # pr6轉成時間格式 才能對時間做比較
            pr6_time = pr6_time.strftime(r'%Y-%m-%d %H:%M:%S')

        if cf.last_read_time and cf.last_read_time != '':  # 如果有輸入上次讀取時間
            if not isinstance(cf.last_read_time, str):  # 如果使用者輸入時間是字串或不為時間格式
                cf.last_read_time = cf.last_read_time.strftime(r'%Y-%m-%d %H:%M:%S')

            if cf.last_read_time[0:10] == pr6_time[0:10]:  # 如果上次讀取時間和PR6時間為同一天
                StartDate = cf.last_read_time[0:10]
                EndDate = pr6_time
            elif cf.last_read_time[0:10] < pr6_time[0:10]:  # 如果日期跟PR6日期不同天 搜尋過去紀錄 0:00~23:59:59
                StartDate = cf.last_read_time[0:10]
                EndDate = cf.last_read_time[0:10] + ' 23:59:59'
            logger.info(f'上次讀取時間{cf.last_read_time}')


        else:  # 如果沒有 用PR6時間0:00分 轉為時間格式
            logger.info(f'無上次讀取時間--系統預設PR6日期00:00分開始 {pr6_time[0:10]}')
            StartDate = pr6_time[0:10]
            EndDate = pr6_time
            cf.last_read_time = pr6_time[0:10] + ' 00:00:00'

        logger.info(f'當下pr6時間:{pr6_time}')

        if not isinstance(cf.last_read_time, datetime.datetime):
            cf.last_read_time = datetime.datetime.strptime(cf.last_read_time, r'%Y-%m-%d %H:%M:%S')

        if not isinstance(StartDate, datetime.datetime):  # 檢查開始時間 結束時間是否為datetime型態
            StartDate = datetime.datetime.strptime(StartDate, r'%Y-%m-%d')

        if not isinstance(EndDate, datetime.datetime):
            EndDate = datetime.datetime.strptime(EndDate, r'%Y-%m-%d %H:%M:%S')

        interval = int(cf['update_times'])
        dt1 = int(StartDate.timestamp())
        dt2 = int(EndDate.timestamp())
        logger.info(f'●start_time: {StartDate} ({dt1}) | ●end_time: {EndDate} ({dt2})')

        # 【查詢會員登錄】
        def getData(cf, url, timeout, mode, dt1, dt2):
            params = {
                'start_time': dt1,
                'end_time': dt2,
                'length': '1000000',
                'device': 'app' if mode == 'AuditAPP' else '',
                'browser': 'UB' if mode == 'AuditUniversal' else '',
            }
            logger.info(url)
            result_enquiry = cd.accountEnquiryRecords(cf, url,
                                                      params=params,
                                                      timeout=timeout)

            # 檢查登出或連線異常, 回傳後讓主程式重試
            if result_enquiry["ErrorCode"] == config.CONNECTION_CODE.code:
                return False, result_enquiry
            if result_enquiry["ErrorCode"] == config.SIGN_OUT_CODE.code:
                return False, result_enquiry
            # 查詢失敗結束
            if result_enquiry["ErrorCode"] != config.SUCCESS_CODE.code:
                return False, result_enquiry["ErrorMessage"]

            return True, result_enquiry['Data']

        listData = []
        for mode in ['AuditAPP' if int(AuditAPP) else '', 'AuditUniversal' if int(AuditUniversal) else '']:
            if mode:
                fbk, content = getData(cf, url, timeout, mode, dt1, dt2)
                if fbk:
                    if ImportColumns:
                        listData += [[i['username'],  # 會員帳號一定要有
                                      i['member_level'] if 'BlockMemberLayer' in ImportColumns else '',
                                      i['vip_level'] if 'VipLevel' in ImportColumns else '',
                                      datetime.datetime.fromtimestamp(int(i['last_login_time'])).strftime(
                                          '%Y-%m-%d %H:%M:%S') if 'LoginDateTime' in ImportColumns else '']
                                     for i in content['data']]
                else:
                    return content

        userDict = {}
        if ImportColumns:
            if 'CumulativeDepositAmount' in ImportColumns or 'CumulativeDepositsTimes' in ImportColumns:
                memberList = [i[0] for i in listData]

                num = len(memberList)
                for i in range((num // 100) if num % 100 == 0 else (num // 100) + 1):
                    memberAll = ','.join([f'"{i}"' for i in memberList[(100 * i):100 + (100 * i)]])  # 一次頂多查100筆
                    members = f'[{memberAll}]'
                    r = cd.searchusername(cf, url, data={'usernameArr': members}, timeout=timeout)
                    # 檢查登出或連線異常, 回傳後讓主程式重試
                    if r["ErrorCode"] == config.CONNECTION_CODE.code:
                        return r
                    if r["ErrorCode"] == config.SIGN_OUT_CODE.code:
                        return r
                    # 查詢失敗結束
                    if r["ErrorCode"] != config.SUCCESS_CODE.code:
                        return r["ErrorMessage"]

                    idList = [i['id'] for i in r['Data']['results']]
                    ids = ','.join(idList)
                    r = cd.memberlist_all(cf, url, data={'length': '1000000', 'id': ids}, timeout=30)
                    # 檢查登出或連線異常, 回傳後讓主程式重試
                    if r["ErrorCode"] == config.CONNECTION_CODE.code:
                        return r
                    if r["ErrorCode"] == config.SIGN_OUT_CODE.code:
                        return r
                    # 查詢失敗結束
                    if r["ErrorCode"] != config.SUCCESS_CODE.code:
                        return r["ErrorMessage"]

                    userDictTemp = {i['username']: [
                        i['lifetime_deposit_total_amt'] if 'CumulativeDepositAmount' in ImportColumns else '',
                        # （累積存款金額）
                        i['lifetime_deposit_no'] if 'CumulativeDepositsTimes' in ImportColumns else '']  # （累積存款次數）
                        for i in r.get('Data', {}).get('data', [])}
                    userDict.update(userDictTemp)
                    time.sleep(.1)

        if userDict:
            listData = [i + userDict[i[0]] for i in listData]

        if {'Member', 'BlockMemberLayer', 'LoginDateTime', 'VipLevel', 'CumulativeDepositAmount',
            'CumulativeDepositsTimes'} - set(ImportColumns):
            listData = [[j for j in i if j] for i in listData]  # 去除空值

        logger.info(f'抓到的csv資料{listData}')
        catch_new_data = []  # 紀錄此次抓取新的資料
        if csv_date_contain:
            logger.info(f'原有CSV檔案{csv_date_contain}')
            for i in listData:
                if i not in csv_date_contain:
                    catch_new_data.append(i)
            listData = catch_new_data
        listData = sorted(listData, key=lambda x: x[3])  # 以「登錄時間」排序
        numD = len(listData)
        msg = f'抓到{numD}筆新資料'
        logger.info(msg)

        cf.last_read_time = EndDate
        if not isinstance(cf.last_read_time, str):  # pr6轉成時間格式 才能對時間做比較
            cf.last_read_time = cf.last_read_time.strftime(r'%Y-%m-%d %H:%M:%S')
        logger.info(f'最後讀入時間={cf.last_read_time}')

        # 回傳結果
        return {
            'IsSuccess': 1,
            'ErrorCode': config.SUCCESS_CODE.code,
            'ErrorMessage': config.SUCCESS_CODE.msg,
            'Data': listData,
        }