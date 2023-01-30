import logging
from platforms.config import CODE_DICT as config
from . import module as wg
from .utils import (
    BETSLIP_ALLOW_LIST,
    ThreadProgress,
    spin_betslip_gamedict,
    spin_betslip_gametype,
    keep_connect
)
import datetime
import time
import pytz
from pathlib import Path
import csv
import re
from urllib import parse
from tkinter import messagebox


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
        timeout, backend_remarks, multiple, cf, amount_memo='', Note='', increasethebet_switch=0, increasethebet='0', frontend_remarks='', activeId='', **kwargs):
        '''
        WG 充值
        需要有【优惠中心 > 活动中心 > 活动列表 | 派发审核】权限。
        '''
        SupportStatus = bool(kwargs.get('SupportStatus'))#查看有無加值過

        if int(increasethebet_switch):#若 increasethebet_switch 為1 用自訂流水倍數
            logger.info(f'●使用PR6流水倍數：{str(increasethebet)}')
            multipleA = str(increasethebet)
            # if multipleA == '0':   設置零倍的時候彈出錯誤彈窗 目前不需要 先註解起來
            # logger.info('警告!您的流水倍數設置為0')
            # MsgBox = messagebox.askquestion('警告!!', '警告!您的流水倍數設置為【0倍】')
            # if MsgBox == 'yes':
            #   pass
            # else:
            #    return 'cancel'

        else:#否則用機器人設置倍數
            logger.info(f'●使用機器人打碼量：{str(multiple)}')
            multipleA = str(multiple)

        #判斷充值金額
        if not float(DepositAmount) <= float(amount_below):#amount_below 儲值最低金額
            Detail = f'充值失败\n{config.AMOUNT_CODE.msg}'
            cls.return_schedule(Action='chkpoint', DBId=DBId, Member=Member, Status='【判断充值金额】', Progress='1/1', Detail=Detail)

            # 查詢失败直接回傳
            return {
                'IsSuccess': 0,
                'ErrorCode': config.AMOUNT_CODE.code,
                'ErrorMessage': config.AMOUNT_CODE.msg,
                'DBId': DBId,
                'Member': Member,
            }
        else:
            Detail = '充值金额符合自动出款金额'
            cls.return_schedule(Action='chkpoint', DBId=DBId, Member=Member, Status='【判断充值金额】', Progress='1/1', Detail=Detail)


        #【充值ID_活動名稱對照表】
        result_activeName = wg.api_active_page(cf, url, timeout=timeout)
        try:
            activeNameDict = {str(i['id']): i['name'] for i in result_activeName['Data']['data']['records']}
            Detail = f'查询成功\n充值ID_活动名称对照表'
        except:
            Detail = f'查询失败\n充值ID_活动名称对照表\n{result_activeName["ErrorMessage"]}'

        if SupportStatus:
            cls.return_schedule(Action='chkpoint', DBId=DBId, Status='【充值ID_活动名称对照表】', Progress=f'1/1', Detail=Detail)
        else:
            cls.return_schedule(Action='chkpoint', DBId=DBId, Member=Member, current_status='【充值ID_活动名称对照表】')

        # 檢查登出或連線異常, 回傳後讓主程式重試
        if result_activeName["ErrorCode"] == config.CONNECTION_CODE.code:
            return result_activeName
        if result_activeName['ErrorCode'] == config.SIGN_OUT_CODE.code:
            return result_activeName
        # 查詢失败結束
        if result_activeName["ErrorCode"] != config.SUCCESS_CODE.code:
            return result_activeName["ErrorMessage"]

        # 查詢無充值ID_活動名稱對照表回傳
        if not activeNameDict:
            return {
                'IsSuccess': 0,
                'ErrorCode': config.NO_RECHARGE_ID_LIST.code,
                'ErrorMessage': config.NO_RECHARGE_ID_LIST.msg,
                'DBId': DBId,
                'Member': Member,
            }


        #【進行充值】
        activeId = activeId if activeId else cf.get('rechargeDict', {}).get(mod_key)
        if not activeNameDict.get(str(activeId)):
            return f'充值ID【{activeId}】不存在！请至机器人介面「机器人工作设置&开启平台帐号权限」修改正确'     
        amount_memo = amount_memo or f'{mod_name}({mod_key}-{DBId}){Note}'
        if backend_remarks and not amount_memo.endswith(backend_remarks):
            amount_memo += f'：{backend_remarks}'
        result_deposit = wg.active_reward_add(cf=cf,
                                    url=url,
                                    data={
                                        'activeId': int(activeId),
                                        'rewardData': [{
                                            'userName': Member,  # 会员账号
                                            'amount': eval(str(DepositAmount)),  # 奖励金额
                                            'audit': eval(str(multipleA)),  # 稽核倍数(打码倍数)
                                            'backRemark': backend_remarks,  # 后台备注
                                            'frontRemark': frontend_remarks,  # 前台备注                                               
                                        }],    
                                        #'rewardRemark': amount_memo,  # 奖励说明  
                                    }, timeout=timeout)      
        
        if result_deposit["IsSuccess"]:
            Detail = f'进行充值成功'
        else:
            Detail = f'进行充值失败\n{result_deposit["ErrorMessage"]}'

        if SupportStatus:
            cls.return_schedule(Action='chkpoint', DBId=DBId, Status='【进行充值】', Progress=f'1/3', Detail=Detail)
        else:
            cls.return_schedule(Action='chkpoint', DBId=DBId, Member=Member, current_status='【进行充值】')
        
        # WG平台回应失败：dcy328981839:已禁止领取，请联系客服,跳過回傳(暫時)
        if '已禁止领取' in result_deposit['ErrorMessage']:
            return result_deposit


        # 充值結果未知，跳過回傳
        if result_deposit['ErrorCode'] == config.IGNORE_CODE.code:
            return result_deposit
        # 被登出，回主程式重試
        if result_deposit['ErrorCode'] == config.SIGN_OUT_CODE.code:
            return result_deposit            
        # 內容錯誤，彈出視窗
        if result_activeName["ErrorCode"] != config.SUCCESS_CODE.code:
            return result_deposit["ErrorMessage"]

        timestampStr = result_deposit['Data'].get('timestamp')
        if timestampStr:
            timestampStr = str(timestampStr)[:10]
            logger.info(f'●timestampStr: {timestampStr}')
        else:
            return {
                'IsSuccess': 0,
                'ErrorCode': config.IGNORE_CODE.code,
                'ErrorMessage': config.IGNORE_CODE.msg,
                'DBId': DBId,
                'Member': Member,
            }
        logger.info('現在正在等待系統抓取')
        time.sleep(5)  #★必要等待(產生時間)
        logger.info('抓取到資料')
        #【確認待通過充值】
        result_deposit = wg.active_reward_batchDetail(cf=cf,
                                    url=url,
                                    data={
                                        "userName": Member,
                                        "activeId": activeId
                                    }, timeout=timeout)   
        if result_deposit["IsSuccess"]:
            Detail = f'确认待通过充值成功'
        else:
            Detail = f'确认待通过充值失败\n{result_deposit["ErrorMessage"]}'

        if SupportStatus:
            cls.return_schedule(Action='chkpoint', DBId=DBId, Status='【确认待通过充值】', Progress=f'2/3', Detail=Detail)
        else:
            cls.return_schedule(Action='chkpoint', DBId=DBId, Member=Member, current_status='【确认待通过充值】')

        # 檢查登出或連線異常, 回傳後讓主程式重試
        if result_deposit["ErrorCode"] == config.CONNECTION_CODE.code:
            return result_deposit
        if result_deposit['ErrorCode'] == config.SIGN_OUT_CODE.code:
            return result_deposit
        # 查詢失败結束
        if result_deposit["ErrorCode"] != config.SUCCESS_CODE.code:
            return result_deposit["ErrorMessage"]
        logger.info(f'先印一下result_deposit[Data][data][record]:\n{result_deposit["Data"]["data"]["record"]}')
        logger.info(f'timestampStr{int(timestampStr)},\n cf[後台名稱]{cf["backend_username"]},\n int(activeId){int(activeId)}\n,'
                    f'Member{Member}\n, i[checkStatus]\n, eval(str(DepositAmount)) {eval(str(DepositAmount))}\n '
                    f' eval(str(multipleA)){eval(str(multipleA))},\n frontend_remarks{frontend_remarks}, backend_remarks {backend_remarks}')

        cfmList = [i for i in result_deposit['Data']['data']['record'] if i['createTime'] == int(timestampStr) and#看時間是不是同一筆
                                                                          i['createUser'] == cf['backend_username'] and#審核名字對不對
                                                                          i['activeId'] == int(activeId) and#獎勵ID對不對
                                                                          i['userName'] == Member and #會員帳號對不對
                                                                          not i['checkStatus'] and#是否被加值
                                                                          i['amount'] == eval(str(DepositAmount)) and#金額是否小於自動儲值金額
                                                                          #i['audit'] == eval(str(multipleA)) and    這條判斷有問題先註解
                                                                          i['frontRemark'] == frontend_remarks and #前台備註是否一樣
                                                                          i['backRemark'] == backend_remarks # 後台備註是否一樣
        ]

        logger.info(f'●cfmList: {cfmList}')
        if cfmList:
            rewardId = cfmList[0]['id']
            logger.info(f'●rewardId: {rewardId}')
        else:
            return {
                'IsSuccess': 0,
                # 'ErrorCode': config.REPEAT_DEPOSIT.code,
                # 'ErrorMessage': config.REPEAT_DEPOSIT.msg.format(platform=cf.platform, msg='查无该笔待通过充值，请手动检查平台充值结果'),
                'ErrorCode': config.IGNORE_CODE.code,
                'ErrorMessage': config.IGNORE_CODE.msg, 
                'DBId': DBId,
                'Member': Member,            
            }  

        #【通過充值】
        result_deposit = wg.active_reward_check(cf=cf,
                                    url=url,
                                    data={
                                        'activeId': int(activeId),
                                        'rewardId': rewardId, 
                                        #'rewardRemark': amount_memo,  # 奖励说明  
                                    }, timeout=timeout)      
        
        if result_deposit["IsSuccess"]:
            Detail = f'通过充值成功'
        else:
            Detail = f'通过充值失败\n{result_deposit["ErrorMessage"]}'

        if SupportStatus:
            cls.return_schedule(Action='chkpoint', DBId=DBId, Status='【通过充值】', Progress=f'3/3', Detail=Detail)
        else:
            cls.return_schedule(Action='chkpoint', DBId=DBId, Member=Member, current_status='【通过充值】')

        # 檢查登出或連線異常, 回傳後讓主程式重試
        if result_deposit["ErrorCode"] == config.CONNECTION_CODE.code:
            return result_deposit
        if result_deposit['ErrorCode'] == config.SIGN_OUT_CODE.code:
            return result_deposit
        # 查詢失败結束
        if result_deposit["ErrorCode"] != config.SUCCESS_CODE.code:
            return result_deposit["ErrorMessage"]

        #回傳結果
        return {
            'IsSuccess': int(result_deposit['IsSuccess']),
            'ErrorCode': result_deposit['ErrorCode'],
            'ErrorMessage': result_deposit['ErrorMessage'],
            'DBId': DBId,
            'Member': Member
        }


class hongbao(BaseFunc):
    '''WG 红包'''


class pointsbonus(BaseFunc):
    '''WG 积分红包'''
    @classmethod
    def deposit(cls, *args, **kwargs):
        '''
        WG 充值
        需要有【优惠中心 > 活动中心 > 活动列表 | 派发审核】权限。
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
        timeout, backend_remarks, multiple, cf, amount_memo='', Note='', increasethebet_switch=0, increasethebet='0', frontend_remarks='', **kwargs):
        '''WG 积分红包 充值'''
        return super().deposit(url, DBId, Member, DepositAmount, mod_name, mod_key, amount_below,
            timeout, backend_remarks, multiple, cf, amount_memo, Note, increasethebet_switch, increasethebet, frontend_remarks, **kwargs)


class enjoyup(BaseFunc):
    '''WG 喜上喜'''
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
        WG 充值
        需要有【优惠中心 > 活动中心 > 活动列表 | 派发审核】权限。
        '''
        SupportStatus = bool(kwargs.get('SupportStatus'))
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
        《WG 喜上喜》
        需要有【会员管理 > 所有会员】权限。
        需要有【报表统计 > 投注记录 > 投注明细】权限。
        '''
        result = cls._audit(*args, **kwargs)
        return result

    @classmethod
    def _deposit(cls, url, DBId, Member, DepositAmount, mod_name, mod_key, amount_below,
        timeout, backend_remarks, multiple, cf, amount_memo='', Note='', increasethebet_switch=0, increasethebet='0', frontend_remarks='', **kwargs):
        '''WG 喜上喜 充值'''
        return super().deposit(url, DBId, Member, DepositAmount, mod_name, mod_key, amount_below,
            timeout, backend_remarks, multiple, cf, amount_memo, Note, increasethebet_switch, increasethebet, frontend_remarks, **kwargs)

    @classmethod
    @keep_connect
    def _audit(cls, url, DBId, Member, SearchGameCategory, SearchDate, timeout, cf, RawWagersId, ExtendLimit, GameCountType, **kwargs):
        '''WG 喜上喜 監控'''
        func = betslip()
        kwargs['externalSig'] = 1
        return func.audit(
            url=url, 
            DBId=DBId, 
            Member=Member, 
            SearchGameCategory=SearchGameCategory, 
            SearchDate=SearchDate, 
            timeout=timeout, 
            cf=cf, 
            RawWagersId=RawWagersId, 
            ExtendLimit=ExtendLimit, 
            GameCountType=GameCountType, 
            **kwargs
        )
        

class betslip(BaseFunc):
    '''WG 注單'''
    class Meta:
        extra = {}
        # suport = BETSLIP_ALLOW_LIST


    @classmethod
    def audit(cls, *args, **kwargs):
        '''
        《WG 注單》
        需要有【会员管理 > 所有会员】权限。
        需要有【报表统计 > 投注记录 > 投注明细】权限。
        '''
        externalSig = bool(kwargs.get('externalSig'))
        if externalSig:
            SupportStatus = bool(kwargs.get('SupportStatus'))
            cls.th = ThreadProgress(kwargs['cf'], kwargs['mod_key'], detail=bool(SupportStatus))
            cls.th.start()

        result = cls._audit(*args, **kwargs)

        if externalSig:
            if SupportStatus:
                cls.return_schedule(Action='chkbbin', DBId=kwargs['DBId'], Status='【审核机器人处理完毕】', Progress='-', Detail='-')
            else:
                cls.return_schedule(Action='chkbbin', DBId=kwargs['DBId'], Member=kwargs['Member'], current_status='【审核机器人处理完毕】')

            cls.th.stop()
        return result


    @classmethod
    @keep_connect
    def _audit(cls, url, DBId, Member, SearchGameCategory, SearchDate, timeout, cf, RawWagersId, ExtendLimit, GameCountType, support_key={}, **kwargs):
        '''WG 注單 監控'''
        externalSig = bool(kwargs.get('externalSig'))
        SupportStatus = bool(kwargs.get('SupportStatus'))

        member = Member.lower()
        # 檢查類別選擇是否支援
        target_categories = (support_key if support_key else set(BETSLIP_ALLOW_LIST.keys())) & set(SearchGameCategory)
        if not target_categories:
            return config.CATEGORY_NOT_SUPPORT.msg.format(
                supported=list(BETSLIP_ALLOW_LIST.values())
            )
        logger.info(f'●即將查詢類別：{[BETSLIP_ALLOW_LIST[cat] for cat in target_categories]}')

        # 查詢會員層級
        params = {'usernameLike': member}
        user_result = wg.member_userMember_allUser(cf=cf, url=url, params=params, timeout=timeout)
        
        sig = 1
        if not user_result["IsSuccess"]:
            Detail = f'查询失败\n搜寻帐号：{Member}\n{user_result["ErrorMessage"]}'
            sig = 0
        else:
            if not user_result['Data'].get('data', {}).get('data', []):
                sig = 0
            else:
                members = [i for i in user_result['Data']['data']['data'] if i['username'].lower() == member]
                if not members:
                    sig = 0
            Detail = f'查询成功\n搜寻帐号：{Member}\n会员是否存在：{"是" if sig else "否"}'
            if sig:
                BlockMemberLayer = members[0].get('memberLevelName', '')
                logger.info(f'●（{Member}）會員層級【{BlockMemberLayer}】')
                if not BlockMemberLayer:
                    sig = 0         
            Detail += f'\n会员层级：{BlockMemberLayer}' if sig else ''

        if externalSig:
            if SupportStatus:
                cls.return_schedule(Action='chkbbin', DBId=DBId, Status='查询会员层级', Progress='1/1', Detail=Detail)
            else:
                cls.return_schedule(Action='chkbbin', DBId=DBId, Member=Member, current_status='查询会员层级')      
        
        if user_result["ErrorCode"] == config.CONNECTION_CODE.code:
            return user_result
        if user_result["ErrorCode"] == config.SIGN_OUT_CODE.code:
            return user_result
        if user_result["ErrorCode"] != config.SUCCESS_CODE.code:
            return user_result["ErrorMessage"]
        
        error_No_User = {
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

        if not sig:
            return error_No_User

        # 查詢注單內容
        bets_result = wg.report_v2_betting(cf=cf, url=url, params={
                                                        # 'order_no':RawWagersId
                                                        'orderNo':RawWagersId
                                                         },timeout=timeout)
        sig = 1
        if not bets_result["IsSuccess"]:
            Detail = f'查询失败\n错误讯息：{bets_result["ErrorMessage"]}'
            # 查無此注單號
            error_No_User.update({
                "ErrorCode": config.WAGERS_NOT_FOUND.code,
                "ErrorMessage": config.WAGERS_NOT_FOUND.msg,
                "BlockMemberLayer": BlockMemberLayer,
            })              
            sig = 0
        else:
            if not bets_result['Data'].get('data', {}).get('data', []):
                # 查無此注單號
                error_No_User.update({
                    "ErrorCode": config.WAGERS_NOT_FOUND.code,
                    "ErrorMessage": config.WAGERS_NOT_FOUND.msg,
                    "BlockMemberLayer": BlockMemberLayer,
                })                
                sig = 0
            Detail = f'查询成功\n注单是否存在：{"是" if sig else "否"}'
            if sig:
                data = [i for i in bets_result['Data']['data']['data'] if i['order_no'] == RawWagersId]
                if data and data[0]['account_name'].lower() != member: 
                    # 注單號的會員不符
                    error_No_User.update({
                            "ErrorCode": config.USER_WAGERS_NOT_MATCH.code,
                            "ErrorMessage": config.USER_WAGERS_NOT_MATCH.msg,
                            "BlockMemberLayer": BlockMemberLayer,
                    })                    
                    sig = 0
                Detail += '' if sig else f'\n{config.USER_WAGERS_NOT_MATCH.msg}'

        if externalSig:
            if SupportStatus:
                cls.return_schedule(Action='chkbbin', DBId=DBId, Status='查询注单内容', Progress='1/1', Detail=Detail)
            else:
                cls.return_schedule(Action='chkbbin', DBId=DBId, Member=Member, current_status='查询注单内容')
    
        if bets_result['ErrorCode'] == config.CONNECTION_CODE.code:
            return bets_result
        if bets_result['ErrorCode'] == config.SIGN_OUT_CODE.code:
            return bets_result
        if bets_result['ErrorCode'] != config.SUCCESS_CODE.code:
            return bets_result['ErrorMessage']

        if not sig:
            return error_No_User

        PayoutAmount = f"{eval(str(data[0]['net_profit'])):.2f}"  # 注單派彩金額取小數第2位並無條件捨去
        BetAmount =  f"{eval(str(data[0]['valid_bet'])):.2f}"  # 注單金額取小數第2位並無條件捨去
        GameName = data[0]['game_name']  # 遊戲名稱
        Game_Id = data[0]['game_id']  # 遊戲名稱ID
        Game_category_Id = data[0]['game_category_id']  # 遊戲類別ID
        Platform_Id = data[0]['platform_id']  # 平台類別ID
        Category_Platform_Id = f'{Game_category_Id}_{Platform_Id}'  # 遊戲平台類別ID
        CategoryName = f"{data[0]['platform_name']}_{data[0]['game_category_name']}"  # 遊戲類別名稱
        # PlatformName = data[0]['platform_name']  # 遊戲平台名稱
        tz = pytz.timezone('Asia/Shanghai') #時區
        WagersTimeStamp = str(int(data[0]['bet_time']) * 1000)  # 時間戳轉換至毫秒
        dt = datetime.datetime.fromtimestamp(int(WagersTimeStamp)/1000, tz=tz)  # 轉換成時間格式
        WagersTimeString = dt.strftime('%Y/%m/%d %H:%M:%S %z')  # 時間格式轉換成字串
        logger.info(f'●（{RawWagersId}）注單內容【{CategoryName} | {Category_Platform_Id}】【{GameName}】【{BetAmount}】【{WagersTimeString} | {WagersTimeStamp}】')
        rtn_msg = {
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
                "PayoutAmount": PayoutAmount,
                "BetAmount": float(BetAmount),
                "AllCategoryCommissionable": '0.00',
                "GameCommissionable": '0.00',
                "SingleCategoryCommissionable": '0.00',
                "CategoryName": CategoryName,
                "ExtendLimit": ExtendLimit,
                "GameCountType": GameCountType,
                "Platform_Id": Platform_Id,
                "Game_category_Id": Game_category_Id,
                "Game_Id": Game_Id,
        }
        if not int(ExtendLimit):
            return rtn_msg

        t1 = f"{SearchDate.replace('/','-')} 00:00:00"
        bet_time_start = int(datetime.datetime.strptime(t1, '%Y-%m-%d %H:%M:%S').timestamp())
        t2 = f"{SearchDate.replace('/','-')} 23:59:59"
        bet_time_end = int(datetime.datetime.strptime(t2, '%Y-%m-%d %H:%M:%S').timestamp())
        
        # 計算總投注金額
        calList = []
        for category_platform in (target_categories if GameCountType == '0' else [Category_Platform_Id]):
            game_category_id, platform_id = category_platform.split('_')
            GameCountType_result = wg.report_v2_bettingStat(cf=cf, url=url,
                                                        params={
                                                            # 'game_category_id': game_category_id if GameCountType != '1' else '',
                                                            'gameCategoryIdList': game_category_id if GameCountType != '1' else '',
                                                            # 'platform_id': platform_id if GameCountType != '1' else '',
                                                            'platformId': platform_id if GameCountType != '1' else '',   
                                                            # 'account_name': member,                                                         
                                                            'accountName': member,
                                                            'gameName': GameName if GameCountType == '1' else '',
                                                            # 'bet_time_start':bet_time_start,
                                                            'betTimeStart':bet_time_start,
                                                            # 'bet_time_end': bet_time_end
                                                            'betTimeEnd': bet_time_end,
                                                            'currency':'CNY',
                                                        }, timeout=timeout)

            if GameCountType_result['ErrorCode'] == config.CONNECTION_CODE.code:
                return GameCountType_result
            if GameCountType_result['ErrorCode'] == config.SIGN_OUT_CODE.code:
                return GameCountType_result
            if GameCountType_result['ErrorCode'] != config.SUCCESS_CODE.code:
                return GameCountType_result['ErrorMessage']

            total_valid_bet = GameCountType_result['Data'].get('data', {}).get('total_valid_bet', 0) 
            logger.info(f'→ total_valid_bet【{game_category_id}_{platform_id}_{GameName}】{total_valid_bet}')           
            calList.append(eval(total_valid_bet if total_valid_bet else '0'))

        if GameCountType == '0':
            # 選取分類當日投注        
            AllCategoryCommissionable = f"{eval(str(sum(calList))):.2f}"
            Status = '选取分类当日投注'
            Detail = f'{Status}：{AllCategoryCommissionable}'
            msg = f'●（{Member}）{Status}【{AllCategoryCommissionable}】'
            rtn_msg.update({"AllCategoryCommissionable": AllCategoryCommissionable})            
        elif GameCountType == '1':
            # 本遊戲當日投注
            GameCommissionable = f"{eval(str(sum(calList))):.2f}"
            Status = '本游戏当日投注'
            Detail = f'{Status}：{GameCommissionable}'
            msg = f'●（{Member}）{Status}【{GameCommissionable}】'
            rtn_msg.update({"GameCommissionable": GameCommissionable})
        elif GameCountType == '2':
            # 本分類當日投注
            SingleCategoryCommissionable = f"{eval(str(sum(calList))):.2f}"
            Status = '本分类当日投注'
            Detail = f'{Status}：{SingleCategoryCommissionable}'
            msg = f'●（{Member}）{Status}【{SingleCategoryCommissionable}】'
            rtn_msg.update({"SingleCategoryCommissionable": SingleCategoryCommissionable})
        else:
            Status = ''
            Detail = ''
            msg = ''
        
        logger.info(msg)

        if externalSig:
            if SupportStatus:
                cls.return_schedule(Action='chkbbin', DBId=DBId, Status=f'计算{Status}', Progress='1/1', Detail=Detail)
            else:
                cls.return_schedule(Action='chkbbin', DBId=DBId, Member=Member, current_status=f'计算{Status}')

        return rtn_msg


class freespin(BaseFunc):
    '''WG 旋转注单'''
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
        WG 充值
        要有【优惠中心 > 活动中心 > 活动列表 | 派发审核】权限。
        '''
        SupportStatus = bool(kwargs.get('SupportStatus'))
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
        《WG 旋轉注單》
        需要有【会员管理 > 所有会员】权限。
        需要有【报表统计 > 投注记录 > 投注明细】权限。
        '''
        SupportStatus = bool(kwargs.get('SupportStatus'))
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
        timeout, backend_remarks, multiple, cf, amount_memo='', Note='', increasethebet_switch=0, increasethebet='0', frontend_remarks='', **kwargs):
        '''WG 旋转注单 充值'''
        return super().deposit(url, DBId, Member, DepositAmount, mod_name, mod_key, amount_below,
            timeout, backend_remarks, multiple, cf, amount_memo, Note, increasethebet_switch, increasethebet, frontend_remarks, **kwargs)

    @classmethod
    def _audit(cls, url, DBId, Member, SearchGameCategory, SearchDate, timeout, cf, RawWagersId, ExtendLimit, GameCountType, **kwargs):
        '''WG 旋转注单 監控'''
        func = betslip()
        kwargs['externalSig'] = 1
        result = func.audit(
            url=url, 
            DBId=DBId, 
            Member=Member, 
            SearchGameCategory=SearchGameCategory, 
            SearchDate=SearchDate, 
            timeout=timeout, 
            cf=cf, 
            RawWagersId=RawWagersId, 
            ExtendLimit=ExtendLimit, 
            GameCountType=GameCountType, 
            **kwargs
        )
        logger.info(f'✪result: {result}')

        if type(result) == str:
            return result

        IsSuccess = True if bool(result['IsSuccess']) else False
        result['FreeSpin'] = 0

        if IsSuccess:
            SupportStatus = bool(kwargs.get('SupportStatus'))
            
            # 查詢是否支援遊戲類別
            Game_category_Id = f"{result['Game_category_Id']}_{result['Platform_Id']}"
            if Game_category_Id not in spin_betslip_gametype:
                result.update({
                    'IsSuccess': 0,
                    'ErrorCode': config.CATEGORY_ERROR.code,
                    'ErrorMessage': config.CATEGORY_ERROR.msg.format(CategoryName=result['CategoryName'])})
                return result

            # 查詢是否支援遊戲名稱
            Game_Id = str(result['Game_Id'])
            if Game_Id not in spin_betslip_gamedict:
                result.update({
                    'IsSuccess': 0,
                    'ErrorCode': config.GAME_ERROR.code,
                    'ErrorMessage': config.GAME_ERROR.msg.format(GameName=result['GameName'])})
                return result

            # 查詢注單詳情
            detail = wg.v2_betting_detail(
                cf=cf,
                url=url,
                params={
                    'order_no': RawWagersId,
                    'platform_id': result['Platform_Id'],
                    'game_category_id': result['Game_category_Id'],
                    'game_id': result['Game_Id'],                    
                },
                timeout=timeout,
            )            
            # 檢查登出或連線異常, 回傳後讓主程式重試
            if detail['ErrorCode'] == config.SIGN_OUT_CODE.code:
                return detail
            if detail['ErrorCode'] == config.CONNECTION_CODE.code:
                return detail
            # 查詢失败結束
            if detail['ErrorCode'] != config.SUCCESS_CODE.code:
                return detail['ErrorMessage']

            order_detail = detail['Data'].get('data', {}).get('order_detail', '')
            if order_detail and re.match('http', order_detail):
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
                query = urlparser(order_detail)['query']
                #【PG电子】
                if Game_category_Id == '3_14':                    
                    result_detail = wg.pg_bet_history(
                        cf=cf,
                        params={'t': query['t']},
                        data={'sid': query['psid']},
                        timeout=timeout,
                    )
                    # 取得免費旋轉資訊
                    detail = [bd for bd in result_detail.get('Data', {}).get('dt', {}).get('bh', {}).get('bd', []) if bd.get('gd', {}).get('fs', {})]
                    FreeSpin = 1 if detail else 0

                #【CQ9电子】
                elif Game_category_Id == '3_3':
                    cq9_url = urlparser(order_detail)['scheme'] + '://'
                    cq9_url += urlparser(order_detail)['hostname'] + '/'
                    result_detail = wg.cq9_bet_history(
                        cf=cf,
                        url=cq9_url,
                        params=query,
                        timeout=timeout,
                    )
                    # 取得免費旋轉資訊
                    FreeSpin = int(bool(result_detail.get('Data', {}).get('data', {}).get('detail', {}).get('wager', {}).get('sub')))
                
                #【JDB电子】
                elif Game_category_Id == '3_5':
                    result_detail = wg.jdb_bet_history(
                        cf=cf,
                        params={
                            'gameSeqNo': query['gameSeq'],
                            'playerId': query['playerId'],  
                            'gameGroupType': 0,                  
                        },         
                        headers={
                            'content-type': 'application/json;charset=UTF-8'
                        },                                       
                        timeout=timeout,
                    )
                    # 取得免費旋轉資訊
                    FreeSpin = 1 if result_detail.get('Data', {}).get('data', {}).get('has_freegame') == 'true' else 0
                        
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

                if Game_category_Id == '3_14':    
                    # PG回傳錯誤
                    if result_detail['Data']['err']:
                        return f'●PG电子显示：{result_detail["Data"]["err"]["msg"]}({result_detail["Data"]["err"]["cd"]})'
                elif Game_category_Id == '3_3':
                    # CQ9回傳錯誤
                    if result_detail['Data']['status']['code'] != '0':
                        return f'●CQ9电子显示：{result_detail["Data"]["status"]["message"]}({result_detail["Data"]["status"]["code"]})'
                elif Game_category_Id == '3_5':
                    # JDB回傳錯誤
                    if (
                        result_detail['Data']['code'] != '00000' 
                        # or not result_detail['Data'].get('data', {}).get('gamehistory', {})
                    ):
                        return f'●JDB电子显示：{result_detail["Data"]}'
                result['FreeSpin'] = FreeSpin

            else:
                result.update({
                    'IsSuccess': 0,
                    'ErrorCode': config.WAGERS_NOT_FOUND.code,
                    'ErrorMessage': '查无注单详情'})

        return result   


class apploginbonus(BaseFunc):
    '''WG APP登入礼'''


    @classmethod
    def deposit(cls, *args, **kwargs):
        '''
        WG 充值、推播通知
        要有【优惠中心 > 活动中心 > 活动列表 | 派发审核】权限。
        要有【运营管理 > 消息管理 > 通知消息 > 新增】权限。
        '''
        SupportStatus = bool(kwargs.get('SupportStatus')) or True
        cls.th = ThreadProgress(kwargs['cf'], kwargs['mod_key'], detail=bool(SupportStatus))
        cls.th.start()

        result = cls._deposit(*args, **kwargs)

        if SupportStatus:
            cls.return_schedule(Action='chkpoint', DBId=kwargs['DBId'], Status='【充值机器人处理完毕】', Progress='-', Detail='-')
        else:
            cls.return_schedule(Action='chkpoint', DBId=kwargs['DBId'], Member=kwargs['Member'], current_status='【充值机器人处理完毕】')

        cls.th.stop()
        return result


    # @classmethod
    # def audit(cls, *args, **kwargs):
    #     '''
    #         WG APP登入礼
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
        NotifyTitle='', NotifyContent='', frontend_remarks='', **kwargs):
        '''WG APP登入礼 充值'''
        SupportStatus = bool(kwargs.get('SupportStatus')) or True
        kwargs['SupportStatus'] = SupportStatus

        rtn_msg = {
            "IsSuccess": 1,
            "ErrorCode": config.SUCCESS_CODE.code,
            "ErrorMessage": config.SUCCESS_CODE.msg,
            'DBId': DBId,
            'Member': Member,
            'NotifyMessage': ''
        }
    
        # 【充值】
        result_deposit = super().deposit(url, DBId, Member, DepositAmount, mod_name, mod_key, amount_below,
            timeout, backend_remarks, multiple, cf, amount_memo, Note, increasethebet_switch, increasethebet, frontend_remarks, **kwargs)
            
        if type(result_deposit) == str:
            return result_deposit
        if not result_deposit['IsSuccess']:
            rtn_msg.update({
                'IsSuccess': result_deposit['IsSuccess'],
                'ErrorCode': result_deposit['ErrorCode'],
                'ErrorMessage': result_deposit['ErrorMessage']
            })
            return rtn_msg

        sig = 1
        # 【推播通知：新增】
        send_time = int(datetime.datetime.now().timestamp())
        result_notify = wg.operation_message_add(cf, url,
            data = {
                'content': f'<p>{NotifyContent}</p>',
                'content_origin': NotifyContent,
                'jpush': 0,  #★不要開啟app推送通知
            #     'level_ids': "",
                'receiver_type': 2,
                'send_time': send_time,
                'title': NotifyTitle,
                'type': 1,
                'username_ids': Member,
            #     'vip_ids': "",
            },
            timeout=timeout)
        # 進度回傳
        if not result_notify["IsSuccess"]:
            Detail = f'推播通知：新增失败\n{result_notify["ErrorMessage"]}'
        else:
            if result_notify["Data"]["success"]:
                Detail = f'推播通知：新增成功'
            else:
                Detail = f'推播通知：新增失败\n{result_notify["Data"]["msg"]}'
                sig = 0

        if SupportStatus:
            cls.return_schedule(Action='chkpoint', DBId=DBId, Status='【推播通知：新增】', Progress='1/3', Detail=Detail)
        else:
            cls.return_schedule(Action='chkpoint', DBId=DBId, Member=Member, current_status='【推播通知：新增】')        
        
        if result_notify["ErrorCode"] == config.CONNECTION_CODE.code:
            return result_notify
        if result_notify["ErrorCode"] == config.SIGN_OUT_CODE.code:
            return result_notify
        if result_notify["ErrorCode"] != config.SUCCESS_CODE.code:
            return result_notify["ErrorMessage"]

        if not sig:
            rtn_msg.update({'NotifyMessage': Detail})
            return rtn_msg

        # 【推播通知：待審核】
        result_notify = wg.operation_message_index(cf, url,
            params = {
                'title': NotifyTitle,
                'publish_status': 3,
                'type': 1,
                'start_time': send_time,
                'end_time': send_time,
                'current': 1,
                'size': 20,
            },
            timeout=timeout)
        # 進度回傳
        if not result_notify["IsSuccess"]:
            Detail = f'推播通知：待审核失败\n{result_notify["ErrorMessage"]}'
        else:
            if result_notify["Data"]["success"]:
                dataList = result_notify['Data']['data'].get('records', [])
                if dataList:                
                    Detail = f'推播通知：待审核成功'
                else:
                    Detail = f'推播通知：待审核失败\n查无清单！'
                    sig = 0
            else:
                Detail = f'推播通知：待审核失败\n{result_notify["Data"]["msg"]}'
                sig = 0

        if SupportStatus:
            cls.return_schedule(Action='chkpoint', DBId=DBId, Status='【推播通知：待审核】', Progress='2/3', Detail=Detail)
        else:
            cls.return_schedule(Action='chkpoint', DBId=DBId, Member=Member, current_status='【推播通知：待审核】')        
        
        if result_notify["ErrorCode"] == config.CONNECTION_CODE.code:
            return result_notify
        if result_notify["ErrorCode"] == config.SIGN_OUT_CODE.code:
            return result_notify
        if result_notify["ErrorCode"] != config.SUCCESS_CODE.code:
            return result_notify["ErrorMessage"]

        if not sig:
            rtn_msg.update({'NotifyMessage': Detail})
            return rtn_msg

        # 【推播通知：審核發布】
        result_notify = wg.operation_message_publish(cf, url,
            data = {
                'id': dataList[0]['id'],
                'type': 1,
            },
            timeout=timeout)
        # 進度回傳
        if not result_notify["IsSuccess"]:
            Detail = f'推播通知：审核发布失败\n{result_notify["ErrorMessage"]}'
        else:
            if result_notify["Data"]["success"]:
                Detail = f'推播通知：审核发布核成功'
            else:
                Detail = f'推播通知：审核发布失败\n{result_notify["Data"]["msg"]}'
                sig = 0

        if SupportStatus:
            cls.return_schedule(Action='chkpoint', DBId=DBId, Status='【推播通知：审核发布】', Progress='3/3', Detail=Detail)
        else:
            cls.return_schedule(Action='chkpoint', DBId=DBId, Member=Member, current_status='【推播通知：审核发布】')        
        
        if result_notify["ErrorCode"] == config.CONNECTION_CODE.code:
            return result_notify
        if result_notify["ErrorCode"] == config.SIGN_OUT_CODE.code:
            return result_notify
        if result_notify["ErrorCode"] != config.SUCCESS_CODE.code:
            return result_notify["ErrorMessage"]

        if not sig:
            rtn_msg.update({'NotifyMessage': Detail})
            return rtn_msg

        rtn_msg.update({'NotifyMessage': result_notify['Data']['msg']})

        return rtn_msg


    @classmethod
    @keep_connect
    def _audit(cls, url, timeout, cf, mod_key=None, 
        AuditAPP=1,AuditUniversal=0,AuditMobilesite=0,AuditUniversalPc=0,AuditUniversalApp=0,AuditCustomizationApp=0, ImportColumns=[], **kwargs):
        '''WG APP登入礼 监控'''

        pathDir = Path(r'.\config\data') / (mod_key or '.')/'WG'
        if not pathDir.exists():
            pathDir.mkdir()
        now = datetime.datetime.now()

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
                            start_time = data[-1][ImportColumns.index('LoginDateTime')]
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

        listData = []

        # 【查詢登錄會員】
        params = {
            'selectTimeKey': 1,  # 登錄時間
            'current': 1,
            'size': 99999,
            'loginTimeFrom': dt1,
            'loginTimeTo': dt2, 
        }  
        result_enquiry = wg.member_userMember_allUser(
                            cf, url,
                            params=params,
                            timeout=timeout,
                            selectTimeKey=1,
                        )

        # 檢查登出或連線異常, 回傳後讓主程式重試
        if result_enquiry["ErrorCode"] == config.CONNECTION_CODE.code:
            return False, result_enquiry
        if result_enquiry["ErrorCode"] == config.SIGN_OUT_CODE.code:
            return False, result_enquiry
        # 查詢失败結束
        if result_enquiry["ErrorCode"] != config.SUCCESS_CODE.code:
            return False, result_enquiry["ErrorMessage"]

        idList = [i['useridx'] for i in result_enquiry['Data']['data']['data']]
        logger.info(f'●共有會員數：{len(idList)}')
        importDict = {
            'Member': 'platform_id',
            'BlockMemberLayer': 'level_name',
            'VipLevel': 'vip_name',
            'LoginDateTime': 'last_login_time',
            'CumulativeDepositAmount': 'total_deposit',
            'CumulativeDepositsTimes': 'order_count',
        }
        if idList:            
            for username in idList:
                # 【查詢會員詳情】
                params = {'username': username} 
                result_enquiry = wg.user_account_info(
                                    cf, url,
                                    params=params,
                                    timeout=timeout
                                )
                # 檢查登出或連線異常, 回傳後讓主程式重試
                if result_enquiry["ErrorCode"] == config.CONNECTION_CODE.code:
                    return False, result_enquiry
                if result_enquiry["ErrorCode"] == config.SIGN_OUT_CODE.code:
                    return False, result_enquiry
                # 查詢失败結束
                if result_enquiry["ErrorCode"] != config.SUCCESS_CODE.code:
                    return False, result_enquiry["ErrorMessage"]

                if int(AuditAPP):  # WG無法判斷使用哪種瀏覽器(AuditUniversal)
                    if result_enquiry['Data']['data'].get('login_os_type', 0) not in [1 ,2]:  # (1) 苹果 (2) 安卓 (3) PC
                        continue
                z = []
                for ic in ImportColumns:
                    zs = str(result_enquiry['Data']['data'].get(importDict[ic]))
                    if zs is not None:
                        z.append(zs if ic != 'LoginDateTime' else datetime.datetime.fromtimestamp(int(zs)).strftime('%Y-%m-%d %H:%M:%S'))

                if len(z) == len(ImportColumns):
                    listData.append(z)
                else:
                    return False, '栏位资讯不足'
                time.sleep(.1)

        listData = sorted(listData, key=lambda x: x[ImportColumns.index('LoginDateTime')])  # 以「登錄時間」排序
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
        AuditAPP=1,AuditUniversal=0,AuditMobilesite=0,AuditUniversalPc=0,AuditUniversalApp=0,AuditCustomizationApp=0, ImportColumns=[], **kwargs):
        '''WG APP登入礼 监控'''

        pathDir = Path(r'.\config\data') / (mod_key or '.')/'WG'
        if not pathDir.exists():
            pathDir.mkdir()
        platform_time = platform_time.replace(tzinfo=None)
        logger.info(f'設定檔{cf}\n pr6--時區:{pr6_zone} 時間:{pr6_time}\n 平台時間(北京):{platform_time}')
        pr6_time_standard = datetime.datetime.strptime(pr6_time, r'%Y-%m-%d %H:%M:%S')  # 設一個維持在時間型態的變數 判別平台時間是否跟PR6時間保持一致
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

        if cf.last_read_time and cf.last_read_time != '':  # 如果有輸入上次讀取時間
            if not isinstance(cf.last_read_time, datetime.datetime):  # 如果使用者輸入時間是字串或不為時間格式
                cf.last_read_time = datetime.datetime.strptime(cf.last_read_time, r'%Y-%m-%d %H:%M:%S')  # 把她轉成時間格式

            logger.info(f'上次讀取時間{cf.last_read_time}')


        else:  #如果沒有 用PR6時間0:00分 轉為時間格式
            if not isinstance(pr6_time, datetime.datetime):
                first_time = datetime.datetime.strptime(pr6_time, r'%Y-%m-%d %H:%M:%S')
                first_time = first_time.strftime(r'%Y-%m-%d 00:00:00') #因為字串型態才能轉00:00:00分
                cf.last_read_time =datetime.datetime.strptime(first_time, r'%Y-%m-%d %H:%M:%S')
            else:
                first_time = pr6_time.strftime(r'%Y-%m-%d 00:00:00')
                cf.last_read_time = datetime.datetime.strptime(first_time, r'%Y-%m-%d %H:%M:%S')
            logger.info(f'無上次讀取時間--系統預設PR6日期00:00分開始 {cf.last_read_time}')

        if not isinstance(pr6_time, datetime.datetime):  # pr6轉成時間格式 才能對時間做比較
            pr6_time = datetime.datetime.strptime(pr6_time, r'%Y-%m-%d %H:%M:%S')

        logger.info(f'當下pr6時間:{pr6_time}')
        cross_day = cf.last_read_time.strftime(r'%Y-%m-%d 23:59:59')  # 判斷增加10分鐘有無跨日
        cross_day = datetime.datetime.strptime(cross_day, r'%Y-%m-%d %H:%M:%S')


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


        logger.info(f'開始查詢時間{StartDate}, 結束時間{EndDate}')

        interval = int(cf['update_times'])
        #end_time = (now - datetime.timedelta(seconds=interval+5)).strftime('%Y-%m-%d %H:%M:%S')

        dt1 = int(StartDate.timestamp())
        dt2 = int(EndDate.timestamp())

        logger.info(f'●start_time: {StartDate} ({dt1}) | ●end_time: {EndDate} ({dt2})')

        listData = []

        # 【查詢登錄會員】
        params = {
            'selectTimeKey': 1,  # 登錄時間
            'current': 1,
            'size': 99999,
            'loginTimeFrom': dt1,
            'loginTimeTo': dt2,
        }
        result_enquiry = wg.member_userMember_allUser(
                            cf, url,
                            params=params,
                            timeout=timeout,
                            selectTimeKey=1,
                        )

        # 檢查登出或連線異常, 回傳後讓主程式重試
        if result_enquiry["ErrorCode"] == config.CONNECTION_CODE.code:
            return False, result_enquiry
        if result_enquiry["ErrorCode"] == config.SIGN_OUT_CODE.code:
            return False, result_enquiry
        # 查詢失败結束
        if result_enquiry["ErrorCode"] != config.SUCCESS_CODE.code:
            return False, result_enquiry["ErrorMessage"]

        idList = [i['useridx'] for i in result_enquiry['Data']['data']['data']]
        logger.info(f'●共有會員數：{len(idList)}')
        importDict = {
            'Member': 'platform_id',
            'BlockMemberLayer': 'level_name',
            'VipLevel': 'vip_name',
            'LoginDateTime': 'last_login_time',
            'CumulativeDepositAmount': 'total_deposit',
            'CumulativeDepositsTimes': 'order_count',
        }
        if idList:
            for username in idList:
                # 【查詢會員詳情】
                params = {'username': username}
                result_enquiry = wg.user_account_info(
                                    cf, url,
                                    params=params,
                                    timeout=timeout
                                )
                # 檢查登出或連線異常, 回傳後讓主程式重試
                if result_enquiry["ErrorCode"] == config.CONNECTION_CODE.code:
                    return False, result_enquiry
                if result_enquiry["ErrorCode"] == config.SIGN_OUT_CODE.code:
                    return False, result_enquiry
                # 查詢失败結束
                if result_enquiry["ErrorCode"] != config.SUCCESS_CODE.code:
                    return False, result_enquiry["ErrorMessage"]

                if int(AuditAPP):  # WG無法判斷使用哪種瀏覽器(AuditUniversal)
                    if result_enquiry['Data']['data'].get('login_os_type', 0) not in [1 ,2]:  # (1) 苹果 (2) 安卓 (3) PC
                        continue
                z = []
                for ic in ImportColumns:
                    zs = str(result_enquiry['Data']['data'].get(importDict[ic]))
                    if zs is not None:
                        z.append(zs if ic != 'LoginDateTime' else datetime.datetime.fromtimestamp(int(zs)).strftime('%Y-%m-%d %H:%M:%S'))

                if len(z) == len(ImportColumns):
                    listData.append(z)
                else:
                    return False, '栏位资讯不足'
                time.sleep(.1)

        listData = sorted(listData, key=lambda x: x[ImportColumns.index('LoginDateTime')])  # 以「登錄時間」排序
        numD = len(listData)
        msg = f'●共：{numD}筆\n●第1筆：{listData[0]}\n●最後1筆：{listData[-1]}' if numD else f'●共：{numD}筆'
        logger.info(msg)

        if cf.last_read_time:
            if not isinstance(cf.last_read_time, datetime.datetime):
                cf.last_read_time = datetime.datetime.strptime(cf.last_read_time, r'%Y-%m-%d %H:%M:%S')

        if cf.last_read_time == StartDate:
            cf.last_read_time = EndDate #若使用者沒有輸入新的值 下一次起始時間從上一次抓取結束時間開始抓取
        logger.info(f'StartDate 型態={type(StartDate)}  EndDate 型態 = {type(EndDate) }')

        if not isinstance(cf.last_read_time, str):  # pr6轉成時間格式 才能對時間做比較
            cf.last_read_time = cf.last_read_time.strftime(r'%Y-%m-%d %H:%M:%S')

        logger.info(f'此次結束 查詢範圍為:{StartDate} ~ {EndDate}, 上次讀取登入時間為:{cf.last_read_time}')

        # 回傳結果
        return {
            'IsSuccess': 1,
            'ErrorCode': config.SUCCESS_CODE.code,
            'ErrorMessage': config.SUCCESS_CODE.msg,
            'Data': listData,
        }