import werdsazxc


# 錯誤碼對應中文
CODE_DICT = werdsazxc.Dict({
    # 成功回傳
    'SUCCESS_CODE': {
        'code': 'AA200',
        'msg': '',
    },
    # 忽略回傳
    'IGNORE_CODE': {
        'code': 'AA204',
        'msg': '与平台连线失败，略过回传，请手动检查平台充值结果',
    },
    # 帳密錯誤
    'ACC_CODE': {
        'code': 'AA400',
        'msg': '会员帐号或密码输入错误，请重新输入',
    },
    # 動態密碼錯誤
    'OTP_CODE': {
        'code': 'AA401',
        'msg': 'OTP输入错误，请重新输入',
    },
    # 權限不足
    'PERMISSION_CODE': {
        'code': 'AA411',
        'msg': '{platform}管理者权限错误, 请确认权限后重启机器人',
    },
    # 網站改版
    'HTML_CONTENT_CODE': {
        'code': 'AA412',
        'msg': '{platform}网页内容无法辨识。请先检查管理者权限。如{platform}改版请联系开发团队。',
    },
    # 帳號被登出
    'SIGN_OUT_CODE': {
        'code': 'AA413',
        'msg': '{platform}帐号已在其他地方登入, 请重新登入',
    },
    # IP錯誤
    'IP_CODE': {
        'code': 'AA414',
        'msg': 'IP不在服务范围，请在{platform}绑定机器人IP',
    },
    # 充值金額錯誤
    'AMOUNT_CODE': {
        'code': 'AA415',
        'msg': '充值金额超过自动出款金额',
    },
    # 充值會員查詢失敗
    'NO_USER': {
        'code': 'AA416',
        'msg': '查无此帐号',
    },
    # 會員與注單號不匹配
    'USER_WAGERS_NOT_MATCH': {
        'code': 'AA417',
        'msg': '【注单号码】与【会员帐号】不匹配',
    },
    # 查不到注單號
    'WAGERS_NOT_FOUND': {
        'code': 'AA418',
        'msg': '查无此【注单号码】',
    },
    # 所選類別不支援
    'CATEGORY_NOT_SUPPORT': {
        'code': 'AA419',
        'msg': '您选择的分类配置尚未支持，请至PR6后台后台确认分类配置后再重新启用。 \n目前机器人仅支援：{supported}',
    },
    # 注單遊戲不支援
    'GAME_ERROR': {
        'code': 'AA420',
        'msg': '【{GameName}】不支援',
    },
    # 注單類別不支援
    'CATEGORY_ERROR': {
        'code': 'AA421',
        'msg': '【{CategoryName}】不支援',
    },
    # 充值ID_活動名稱對照表查詢失敗
    'NO_RECHARGE_ID_LIST': {
        'code': 'AA422',
        'msg': '查无充值ID_活动名称对照表',
    },
    # 訂單處理超過時限
    'TIME_LIMIT_FAIL': {
        'code': 'AA430',
        'msg': '订单处理超过时限',
    },
    # 充值失敗
    'DEPOSIT_FAIL': {
        'code': 'AA431',
        'msg': '{platform}平台回应失败：{msg}',
    },
    'REPEAT_DEPOSIT': {
        'code': 'AA432',
        'msg': '{platform}平台回应充值失败：{msg}',
    },
    # 層級移動失敗
    'LayerError': {
        'code': 'AA433',
        'msg': '{platform}平台，移动层级失败：{msg}',
    },
    # 推播失敗
    'UserMessageError': {
        'code': 'AA434',
        'msg': '{platform}平台，讯息推播失败：{msg}',
    },
    # 密鑰錯誤
    'KEY_CODE': {
        'code': 'AA500',
        'msg': '机器人密钥错误，请至PR6后台设置匹配的密钥后重启机器人',
    },
    # 狀態碼異常
    'HTML_STATUS_CODE': {
        'code': 'AA501',
        'msg': '{status_code}异常, 请确认{platform}网址、帐号、密码后再次尝试',
    },
    # 解析失敗
    'JSON_ERROR_CODE': {
        'code': 'AA502',
        'msg': '{platform}网页回应异常，请联系{platform}',
    },
    # 連線異常
    'CONNECTION_CODE': {
        'code': 'AA503',
        'msg': '连线异常，请检查本机网路是否稳定，平台是否可以正常操作。',
    },
    # 程式報錯
    'EXCEPTION_CODE': {
        'code': 'AA599',
        'msg': '未知错误﹐请联系开发团队',
    },
    # 系統傳回參數錯誤
    'PARAMETER_ERROR':{
        'code': 'AA598',
        'msg':'PR6后台传入机器人参数型别异常,请连系开发人员'
    },
    # 超過平台會員帳號字數限制
    'ACCOUNT_COUNT_ERROR':{
        'code': 'AA699',
        'msg': '会员帐号错误(超过{platform}平台会员帐号字数限制)'
    }
})
