from tkinter import messagebox
from werdsazxc import Dict
from pathlib import Path
from tkinter import ttk
import tkinter as tk
import subprocess
import builtins
import logging
import socket
import signal
import re
import platforms
logger = logging.getLogger('robot')

class EndlessList:
    def __init__(self, lst):
        self.lst = lst
        self.i = len(lst)
    def __eq__(self, other):
        if getattr(other, 'lst', None):
            return self.lst == other.lst
        return self.lst == other
    def __len__(self):
        return len(self.lst)
    def __iter__(self):
        return self
    def __next__(self):
        self.i += 1
        try:
            return self.lst[self.i]
        except IndexError as e:
            self.i = 0
            return self.lst[self.i]


class Frame(tk.Frame):
    '''主視窗UI'''
    def __init__(self, root):
        '''主視窗UI初始化'''
        self.root = root
        super().__init__()
        self.version_number = builtins.VERSION

        # 變數設定
        self.action_info = tk.StringVar()
        self.platform = tk.StringVar()
        self.backend_username = tk.StringVar()
        self.backend_password = tk.StringVar()
        self.need_backend_otp = tk.BooleanVar()
        self.update_times = tk.StringVar()
        self.switching = tk.StringVar()
        self.super_pass = tk.StringVar()
        self.clock = tk.StringVar()
        self.ip = tk.StringVar()
        self.url = tk.StringVar()
        self.api = tk.StringVar()
        self.proname = tk.StringVar()
        self.old_pass = tk.StringVar()
        self.new_pass = tk.StringVar()
        self.confirm_pass = tk.StringVar()
        self.amount_below = tk.StringVar()
        self.frontend_remarks = tk.StringVar() if self.cf['platform'] == 'WG' else None
        self.backend_remarks = tk.StringVar()
        self.multiple = tk.StringVar()
        self.robot_act = tk.StringVar()
        self.robot_pw = tk.StringVar()
        self.bk_otp = tk.StringVar()
        self.need_bk_otp = tk.BooleanVar()
        self.secret_key = tk.StringVar()
        self.run_robot = tk.StringVar()
        self.monitor = tk.IntVar()
        self.deposit = tk.IntVar()
        self.recharge_cfm = tk.StringVar()
        self.super_pass = tk.StringVar()
        self.old_pass = tk.StringVar()
        self.new_pass = tk.StringVar()
        self.confirm_pass = tk.StringVar()
        self.backend_otp = tk.StringVar()

        self.timeout = tk.StringVar()
        self.recharg = tk.StringVar()

        self.style = ttk.Style()
        self.style.configure('WTBG.Label', background='white')
        self.style.configure('CL.TButton', foreground='black')
        self.title_font = ('Tahoma', 10, 'bold')
        self.mark_font = ('Tahoma', 9)

        # 設定畫面並記錄讀取的設定檔
        self.setup_base_setting()
        self.setup_game_setting()
        self.root.geometry('1200x380')
        self.msg_zone.pack(expand=1, fill='both')

    # PR6后台畫面
    def login_backend(self):
        self.login_btn.pack_forget()
        self.robot_btn.pack(fill='both', side='top')
        self.tabControl.pack_forget()
        self.tabControl.pack(expand=1, fill='both')
    def logout_backend(self):
        self.login_btn.pack(fill='both', side='top')
        self.robot_btn.pack_forget()
        self.tabControl.pack_forget()
        self.tabControl.pack(expand=1, fill='both')

    # 機器人設置
    def setup_base_setting(self, mode=1):
        '''初始化視窗'''
        if mode:
            # 添加版本號
            self.msg_zone = ttk.Frame(self.root)
            ttk.Label(
                self.msg_zone,
                text=f' Version: {self.version_number}',
                style='WTBG.Label'
            ).pack(
                fill='both',
                side='top'
            )
            # 添加登入按鈕
            self.login_btn = ttk.Button(
                self.msg_zone,
                text='登入',
                command=lambda: self.pop_otp('backend', self.cf['need_bk_otp'])
            )
            self.login_btn.pack(fill='both', side='top')
            # 添加啟動機器人按鈕
            self.robot_btn = ttk.Button(
                self.msg_zone,
                textvariable=self.run_robot,
                style='CL.TButton'
            )
            self.run_robot.set('启动机器人')

            self.tabControl = ttk.Notebook(self.msg_zone)

        self.tab0 = ttk.Frame(self.tabControl)
        self.tabControl.add(self.tab0, text='机器人设置')
        self.tabControl.pack(expand=1, fill='both')

        # 為了讓表單內容留點邊距
        ttk.Label(self.tab0, width=5, text=' ').grid(column=0, row=0)

        rowCount = 1

        # 平台帳號 backend_username
        rowCount += 1
        self.lbl_backend_username = ttk.Label(self.tab0, text=f'帐号：', font=self.title_font)
        self.lbl_backend_username.grid(column=2, row=rowCount, pady=5, sticky='E')
        self.entry_backend_username = tk.Entry(self.tab0, width=30, textvariable=self.backend_username)
        self.entry_backend_username.grid(column=3, row=rowCount, sticky='W')

        # 平台是否啟用 OTP
        self.chk_backend_otp = tk.Checkbutton(self.tab0, text='启用动态码', command=self.change_setting,
                                              variable=self.need_backend_otp, onvalue=True, offvalue=False)
        self.chk_backend_otp.grid(column=4, row=rowCount, sticky='W')

        # 平台密碼 backend_password
        rowCount += 1
        self.lbl_backend_password = ttk.Label(self.tab0, text=f'密码：', font=self.title_font)
        self.lbl_backend_password.grid(column=2, row=rowCount, pady=5, sticky='E')
        self.entry_backend_password = tk.Entry(self.tab0, width=30, textvariable=self.backend_password)
        self.entry_backend_password.grid(column=3, row=rowCount, sticky='W')

        self.lbl_frontend_remarks = ttk.Label(self.tab0, text=f'前台充值备注：',font=self.title_font)
        if self.cf['platform'] == 'WG':
            # PR6前台充值備註 frontend_remarks
            rowCount += 3
            self.lbl_frontend_remarks.grid(column=2, row=rowCount, pady=5, sticky='E')
            self.entry_frontend_remarks = tk.Entry(self.tab0, width=30, textvariable=self.frontend_remarks)
            self.entry_frontend_remarks.grid(column=3, row=rowCount, sticky='W')

        # PR6后台充值備註 backend_remarks
        rowCount += (1 if self.cf['platform'] == 'WG' else 3)
        self.lbl_backend_remarks = ttk.Label(self.tab0, text='后台充值备注：' if self.cf['platform'] == 'WG' else '充值备注：',font=self.title_font)
        self.lbl_backend_remarks.grid(column=2, row=rowCount, pady=5, sticky='E')
        self.entry_backend_remarks = tk.Entry(self.tab0, width=30, textvariable=self.backend_remarks)
        self.entry_backend_remarks.grid(column=3, row=rowCount, sticky='W')

        # 打碼量 multiple
        rowCount += 1
        self.lbl_multiple = ttk.Label(self.tab0, text=f'打码量：',font=self.title_font)
        self.lbl_multiple.grid(column=2, row=rowCount, pady=5, sticky='E')
        self.cbb_multiple = ttk.Combobox(self.tab0, width=27, textvariable=self.multiple, state='readonly')
        self.cbb_multiple['values'] = ('1倍打码', '2倍打码', '3倍打码', '4倍打码', '5倍打码', '6倍打码', '7倍打码', '8倍打码', '9倍打码', '10倍打码')
        self.cbb_multiple.grid(column=3, row=rowCount)

        # 小額自動 amount_below
        rowCount += 1
        self.lbl_amount_below = ttk.Label(self.tab0, text='小额自动：',font=self.title_font)
        self.lbl_amount_below.grid(column=2, row=rowCount, pady=5, sticky='E')
        self.entry_amount_below = tk.Entry(self.tab0, width=30, textvariable=self.amount_below)
        self.entry_amount_below.grid(column=3, row=rowCount, sticky='W')
        self.lbl_amount_below_info = ttk.Label(self.tab0, text='低于此金额，由充值机器人自动派彩', foreground='grey')
        self.lbl_amount_below_info.grid(column=4, row=rowCount, padx=5, pady=5, sticky='W')

        # 冷卻後重新充值 recharg
        rowCount += 1
        self.lbl_recharg = ttk.Label(self.tab0, text=f'冷却后重新充值：', font=self.title_font)
        self.lbl_recharg.grid(column=2, row=rowCount, pady=5, sticky='E')
        self.cbb_recharg = ttk.Combobox(self.tab0, width=27, textvariable=self.recharg, state='readonly')
        self.cbb_recharg['values'] = ('开启', '关闭')
        self.cbb_recharg.grid(column=3, row=rowCount)
        self.lbl_recharg_info = ttk.Label(self.tab0, text='平台回应10秒内已执行人工存入\n机器人将会暂停10秒后重新尝试充值', foreground='red')
        self.lbl_recharg_info.grid(column=4, row=rowCount, padx=5, pady=5, sticky='W')

        # 機器人帳號 robot_act
        self.lbl_robot_act = ttk.Label(self.tab0, text='PR6后台帐号：',font=self.title_font)
        self.lbl_robot_act.grid(column=5, row=2, pady=5, sticky='E')
        self.entry_robot_act = tk.Entry(self.tab0, width=30, textvariable=self.robot_act)
        self.entry_robot_act.grid(column=6, row=2, sticky='W')
        self.chk_bk_otp = tk.Checkbutton(self.tab0, text='启用动态码', command=self.change_setting,
                                         variable=self.need_bk_otp, onvalue=True, offvalue=False)
        self.chk_bk_otp.grid(column=7, row=2, sticky='W')

        # 機器人密碼 robot_pw
        self.lbl_robot_pw = ttk.Label(self.tab0, text='PR6后台密码：',font=self.title_font)
        self.lbl_robot_pw.grid(column=5, row=3, pady=5, sticky='E')
        self.entry_robot_pw = tk.Entry(self.tab0, width=30, textvariable=self.robot_pw)
        self.entry_robot_pw.grid(column=6, row=3, sticky='W')

        # 自動連接 switching
        self.lbl_switching = ttk.Label(self.tab0, text='自动连接：', font=self.title_font)
        self.lbl_switching.grid(column=5, row=6, pady=5, sticky='E')
        self.cbb_switching = ttk.Combobox(self.tab0, width=27, textvariable=self.switching, state='readonly')
        self.cbb_switching["values"] = ('关闭', '开启')
        self.cbb_switching.grid(column=6, row=6)
        self.lbl_switching_info = ttk.Label(self.tab0, text='帐号掉线，自动重新连接', foreground='grey')
        self.lbl_switching_info.grid(column=7, row=6, padx=5, pady=5, sticky='W')

        # 等待回應時間 timeout
        self.lbl_timeout = ttk.Label(self.tab0, text=f'等待回应时间：', font=self.title_font)
        self.lbl_timeout.grid(column=5, row=7, pady=5, sticky='E')
        self.cbb_timeout = ttk.Combobox(self.tab0, width=27, textvariable=self.timeout, state='readonly')
        self.cbb_timeout["values"] = ('15秒', '30秒', '40秒', '50秒', '60秒', '120秒')
        self.cbb_timeout.grid(column=6, row=7)
        self.lbl_timeout_info = ttk.Label(self.tab0, text='机器人等待平台及PR6后台回应时间\n当订单连续多笔回应连线逾时, 请加大此设置秒数', foreground='grey')
        self.lbl_timeout_info.grid(column=7, row=7, padx=5, pady=5, sticky='W')

        # 刷新頻率 update_times
        self.lbl_update_times = ttk.Label(self.tab0, text='刷新频率：', font=self.title_font)
        self.lbl_update_times.grid(column=5, row=8, pady=5, sticky='E')
        self.cbb_update_times = ttk.Combobox(self.tab0, width=27, textvariable=self.update_times, state='readonly')
        self.cbb_update_times["values"] = ('1秒', '2秒', '5秒')
        self.cbb_update_times.grid(column=6, row=8)
        self.lbl_update_times_info = ttk.Label(self.tab0, text='机器人掉线，请加大刷新频率\n默认2秒，若频繁掉线，请调高刷新频率', foreground='grey')
        self.lbl_update_times_info.grid(column=7, row=8, padx=5, pady=5, sticky='W')

        # 開啟紀錄檔資料夾按鈕
        self.log_btn = ttk.Button(self.tab0, text='开启纪录档资料夹',
            command=lambda: subprocess.Popen(f'explorer "{Path("config/log").absolute()}"'))
        self.log_btn.grid(column=2, row=10, pady=30, sticky='W')

        # 保存按鈕
        self.btn_save = ttk.Button(self.tab0, text='保存', width=30)
        self.btn_save.grid(column=3, row=10, pady=30)

        # 時間
        self.clock_label1 = tk.Label(self.tab0, justify='center', textvariable=self.clock)
        self.clock_label1.grid(column=7, row=10, pady=30, sticky='E')

        # 綁定關閉視窗動作
        self.root.protocol('WM_DELETE_WINDOW', self.quit)
        self.root.bind('<Control-q>', self.quit)
        signal.signal(signal.SIGINT, self.quit)
        # 綁定超級密碼彈出視窗
        self.root.bind('<Control-Shift-F12>', self.pop_admin)

    # 子系统清单視窗設置
    def setup_game_setting(self):
        self.action_info.set(200*' ')
        self.tab3 = ttk.Frame(self.tabControl)
        tab_game_tree = ttk.Frame(self.tab3)
        tab_btns = ttk.Frame(self.tab3)
        self.tabControl.add(self.tab3, text='机器人工作设置&开启平台帐号权限')
        # 支援遊戲表格
        self.suport_tree = ttk.Treeview(self.tab3, show='headings')
        self.suport_scrollbar = tk.Scrollbar(self.tab3, command=self.suport_tree.yview)
        self.suport_tree.configure(yscroll=self.suport_scrollbar.set)
        self.suport_tree['columns'] = ('one', 'two')
        self.suport_tree.column('one', width=50, anchor='center', stretch=tk.NO)
        self.suport_tree.column('two', width=200, stretch=tk.NO)
        self.suport_tree.heading('one', text='编号', anchor=tk.W)
        self.suport_tree.heading('two', text='游戏名称', anchor=tk.W)
        self.suport_label = tk.Label(self.tab3, text='可查询之游戏清单：')
        self.suport_label.grid(column=2, row=1, padx=10, sticky="W")
        self.suport_tree.grid(column=2, row=2, rowspan=100, padx=10, pady=10, sticky='NSE')
        self.suport_scrollbar.grid(column=2, row=2, rowspan=100, padx=10, pady=10, sticky='NSE')
        # 子系统清单表格
        self.tree = ttk.Treeview(tab_game_tree, show='headings', selectmode='browse')
        self.scrollbar = tk.Scrollbar(tab_game_tree, command=self.tree.yview)
        self.tree.configure(yscroll=self.scrollbar.set)
        self.tree.tag_configure('enabled', foreground='green')
        self.tree.tag_configure('disabled', foreground='red')
        self.tree['columns'] = ('one', 'two', 'three', 'four', 'five') if self.cf['platform'] == 'WG' else ('one', 'two', 'three', 'four')
        # 表頭定位
        self.tree.column('one', width=50, minwidth=50, anchor='center', stretch=tk.NO)
        self.tree.column('two', width=50, minwidth=50, stretch=tk.NO)
        self.tree.column('three', width=50, minwidth=50, stretch=tk.NO)
        self.tree.column('four', width=100, minwidth=50, stretch=tk.NO)
        self.tree.column('five', width=100, minwidth=50, stretch=tk.NO) if self.cf['platform'] == 'WG' else None
        # 表頭文字
        self.tree.heading('one', text='编号', anchor=tk.W)
        self.tree.heading('two', text='监控', anchor=tk.W)
        self.tree.heading('three', text='充值', anchor=tk.W)
        self.tree.heading('four', text='活动名称', anchor=tk.W)
        self.tree.heading('five', text='充值ID', anchor=tk.W) if self.cf['platform'] == 'WG' else None
        # 表格定位
        self.tree.pack(expand=1, side='left', fill='both')
        self.scrollbar.pack(side='right', fill='both')

        # 子系統說明
        self.lbl_action_info = ttk.Label(self.tab3, textvariable=self.action_info, foreground='grey')
        self.lbl_action_info.grid(column=3, row=1, padx=10, pady=10, columnspan=3, rowspan=5, sticky='NEW')

        # 讀取清單按紐
        self.btn_reload = ttk.Button(tab_btns, text='重新读取PR6后台清单', width=30)
        self.btn_reload.pack(side='left')

        # 保存按鈕
        self.btn_save_game = ttk.Button(tab_btns, text='保存', width=30)
        self.btn_save_game.pack(side='right')

        tab_game_tree.grid(column=0, row=0, rowspan=100, padx=5, pady=5, sticky='NSEW')
        tab_btns.grid(column=0, row=101, padx=5, pady=5, sticky='NSEW')

    # 彈出視窗, 並會記錄log
    def msg_box(self, types, msg, parent=None):
        '''彈窗訊息'''
        parent = parent or self
        title = f'【{types}】：{self.platform.get()} {self.proname.get()} - {self.robot_act.get()}'
        messagebox.showerror(title=title, message=msg, parent=parent)
        logger.info(f'彈窗訊息 {types}：{msg}')


    # 輸入充值ID視窗
    def pop_recharge(self, name, rechargeID, mod, idx, *args):
        '''開啟充值ID輸入視窗'''
        # 如果有開啟過視窗的紀錄，會先將原有視窗關閉後再另開新視窗
        if getattr(self, 'recharge_page', None):
            self.recharge_page.destroy()
            self.recharge_page = None
        # 取得最上層位置(至頂顯示)
        self.recharge_page = tk.Toplevel()
        self.recharge_page.wm_attributes('-topmost', 1)
        self.recharge_page.geometry('350x175')

        # 實體化LabelFrame於最上層
        msg = ttk.LabelFrame(self.recharge_page, text=name)
        msg.pack(expand=1, fill='both')

        # 一行一行設定內容
        tk.Label(msg).pack()
        rmk = '确认' if rechargeID else '输入'
        ttk.Label(msg, text=f'请 {rmk}【充值ID】须为「正整数」：', font=self.title_font).pack()
        tk.Label(msg).pack()
        entry = tk.Entry(msg, width=30, textvariable=self.recharge_cfm)
        entry.pack()
        self.recharge_cfm.set(rechargeID if rechargeID else '')
        entry.focus_set()
        tk.Label(msg).pack()
        btn = ttk.Button(msg, text='确认')
        btn.pack()
        btn.bind("<Button-1>", lambda e, mod=mod, idx=idx: self.submit_recharge(mod, idx))

        # 綁定Enter按鈕觸發 submit_recharge
        self.recharge_page.bind('<Return>', lambda e, mod=mod, idx=idx: self.submit_recharge(mod, idx))
        # 綁定關閉視窗觸發 close_recharge
        self.recharge_page.protocol('WM_DELETE_WINDOW', self.close_recharge)


    def close_recharge(self):
        '''關閉充值ID輸入視窗'''
        if not self.recharge_cfm.get():
            self.msg_box('错误', '充值ID为必填内容！请确实输入～', parent=self.recharge_page)
            return        
        if not self.recharge_cfm.get().isdigit():
            self.msg_box('错误', '充值ID输入错误：非正整数！请重新输入～', parent=self.recharge_page)
            return
        self.recharge_page.destroy()
        self.recharge_page = None


    def submit_recharge(self, mod, idx, *args):
        '''充值ID確認後, 存檔'''
        # 充值ID輸入不正確, 離開函式
        if not self.recharge_cfm.get():
            self.msg_box('错误', '充值ID为必填内容！请确实输入', parent=self.recharge_page)
            return        
        if not self.recharge_cfm.get().isdigit():
            self.msg_box('错误', '充值ID输入错误：非正整数！请重新输入', parent=self.recharge_page)
            return

        if self.cf.get('rechargeDict'):
            self.cf['rechargeDict'][mod] = self.recharge_cfm.get()
        else:
            self.cf['rechargeDict'] = {mod: self.recharge_cfm.get()}
        logger.info(f'●充值ID【{self.recharge_cfm.get()}】（{mod}）確認')
        # 關閉充值ID視窗
        self.close_recharge()
        self.tree.set(idx, column='five', value=self.cf['rechargeDict'][mod])    
        if '设定已修改, 请保存后继续' not in self.cf['error_msg']:
            self.cf['error_msg'].extend(['设定已修改, 请保存后继续'])
            self.upd_btn_msg()
            self.upd_title()


    # 輸入超級密碼視窗
    def pop_admin(self, *args):
        '''開啟超級密碼輸入視窗'''
        # 如果有開啟過視窗的紀錄，會先將原有視窗關閉後再另開新視窗
        if getattr(self, 'admin_page', None):
            self.admin_page.destroy()
            self.admin_page = None
        # 取得最上層位置(至頂顯示)
        self.admin_page = tk.Toplevel()
        self.admin_page.wm_attributes('-topmost', 1)
        self.admin_page.geometry('400x200')

        # 實體化LabelFrame於最上層
        msg = ttk.LabelFrame(self.admin_page, text=' 超级管理员 ')
        msg.pack(expand=1, fill='both')

        # 一行一行設定內容
        tk.Label(msg).pack()
        ttk.Label(msg, text='请输入超级密码登入', font=self.title_font).pack()
        tk.Label(msg).pack()
        entry = tk.Entry(msg, width=30, textvariable=self.super_pass)
        entry.pack()
        entry.focus_set()
        tk.Label(msg).pack()
        btn = ttk.Button(msg, text='登入')
        btn.pack()
        btn.bind("<Button-1>", self.submit_admin)

        # 綁定Enter按鈕觸發 submit_admin
        self.admin_page.bind('<Return>', lambda e: self.submit_admin())
        # 綁定關閉視窗觸發 close_admin
        self.admin_page.protocol('WM_DELETE_WINDOW', self.close_admin)
    def close_admin(self):
        '''關閉超級密碼輸入視窗'''
        self.admin_page.destroy()
        self.admin_page = None
    def submit_admin(self, *args):
        '''超級密碼確認後, 開啟管理員設置視窗'''
        # 超級密碼輸入錯誤, 離開函式
        if self.super_pass.get() != self.cf['super_pass']:
            self.msg_box('错误', '超级密码错误，请重新输入。', parent=self.admin_page)
            return

        # 重置超級密碼
        self.super_pass.set('')
        # 關閉超級密碼視窗
        self.close_admin()
        # 開啟超級管理員視窗
        self.pop_super_setting()

    # 管理員設置視窗
    def pop_super_setting(self, *args):
        '''超級管理員設置視窗'''
        # 如果有開啟過視窗的紀錄，會先將原有視窗關閉後再另開新視窗
        if getattr(self, 'super_setting', None):
            self.close_super_setting()
        # 取得最上層位置(至頂顯示)
        self.super_setting = tk.Toplevel()
        self.super_setting.wm_attributes("-topmost", 1)
        self.super_setting.geometry('750x300')

        # 修改超級密碼按鈕
        btn = ttk.Button(self.super_setting, text='修改超级管理员密码')
        btn.pack(expand=1, fill='both')
        btn.bind("<Button-1>", self.pop_admin_alter)
        # 實體化LabelFrame於最上層
        msg = ttk.LabelFrame(self.super_setting, text=' 管理员设置 ')
        msg.pack(expand=1, fill='both')

        # 為了讓表單內容留點邊距
        ttk.Label(msg, width=5, text=' ').grid(column=0, row=0)

        # 機器人窗口標題 proname
        self.lbl_proname = ttk.Label(msg, text='机器人窗口标题：', font=self.title_font)
        self.lbl_proname.grid(column=2, row=1, pady=5, sticky='E')
        self.entry_proname = tk.Entry(msg, width=30, textvariable=self.proname)
        self.entry_proname.grid(column=3, row=1, sticky='W')
        self.lbl_proname_info = ttk.Label(msg, text='例：红包系统', font=self.mark_font, foreground='grey')
        self.lbl_proname_info.grid(column=4, row=1, padx=5, sticky='W')

        # 機器人綁定域名 api
        self.lbl_api = ttk.Label(msg, text='PR6后台域名：', font=self.title_font)
        self.lbl_api.grid(column=2, row=2, pady=5, sticky='E')
        self.entry_api = tk.Entry(msg, width=30, textvariable=self.api)
        self.entry_api.grid(column=3, row=2, sticky='W')
        self.lbl_api_info = ttk.Label(msg, text='例：https://xxxx.xxxxx.com/，请勿添加后缀(如login.php)', font=self.mark_font, foreground='grey')
        self.lbl_api_info.grid(column=4, row=2, padx=5, sticky='W')

        # 機器人綁定IP ip
        self.lbl_ip = ttk.Label(msg, text='服务器IP：', font=self.title_font)
        self.lbl_ip.grid(column=2, row=3, pady=5, sticky='E')
        self.entry_ip = tk.Entry(msg, width=30, textvariable=self.ip)
        self.entry_ip.grid(column=3, row=3, sticky='W')
        self.lbl_ip_info = ttk.Label(msg, text='域名指向的服务器IP, 如有多个IP请使用 , 分隔', font=self.mark_font, foreground='grey')
        self.lbl_ip_info.grid(column=4, row=3, padx=5, sticky='W')

        # 平台 platform
        self.lbl_platform = ttk.Label(msg, text='平台：',font=self.title_font).grid(column=2, row=5, pady=5, sticky='E')
        self.cbb_platform = ttk.Combobox(msg, width=27, textvariable=self.platform, state='readonly')
        self.cbb_platform['values'] = ('LEBO', 'BBIN', 'CD', 'WG')
        self.cbb_platform.grid(column=3, row=5)
        self.cbb_platform.bind("<<ComboboxSelected>>", lambda x: self.change_platform())
        self.cbb_platform.bind('<Return>', lambda e: self.save_setting(msgbox=self.tabControl.tab(self.tabControl.select(), "text")))

        # 平台PR6后台 url
        self.lbl_url = ttk.Label(msg, text=self.cf['platform'] + '平台网址：', font=self.title_font)
        self.lbl_url.grid(column=2, row=6, pady=5, sticky='E')
        self.entry_url = tk.Entry(msg, width=30, textvariable=self.url)
        self.entry_url.grid(column=3, row=6, sticky='W')
        self.lbl_url_info = ttk.Label(msg, text='例：https://xxxx.xxxxx.com/，请勿添加后缀(如login.php)', font=self.mark_font, foreground='grey')
        self.lbl_url_info.grid(column=4, row=6, padx=5, sticky='W')

        # PR6后台密鑰 secret_key
        self.lbl_secret_key = ttk.Label(msg, text='机器人密钥：', font=self.title_font)
        self.lbl_secret_key.grid(column=2, row=9, pady=5, sticky='E')
        self.entry_secret_key = tk.Entry(msg, width=30, textvariable=self.secret_key)
        self.entry_secret_key.grid(column=3, row=9, sticky='W')
        self.lbl_secret_key_info = ttk.Label(msg, text='由【PR6后台】的【系统设定】的【网站配置】取得', font=self.mark_font, foreground='grey')
        self.lbl_secret_key_info.grid(column=4, row=9, padx=5, sticky='W')

        # 保存按鈕
        ttk.Label(msg, width=5, text=' ').grid(column=0, row=10)
        self.btn_save_supper_setting = ttk.Button(msg, text='保存')
        self.btn_save_supper_setting.grid(column=3, row=10, sticky='WE')

        # 更新輸入框、按鈕、下拉選單狀態
        self.switch_input()
        # 綁定關閉視窗觸發 close_super_setting
        self.super_setting.protocol('WM_DELETE_WINDOW', self.close_super_setting)
    def close_super_setting(self):
        '''關閉管理員設置視窗'''
        self.super_setting.destroy()
        self.super_setting = None
    def submit_super_setting(self, *args):
        '''保存超級管理員設置'''
        # 獲取變數
        old_url = self.cf['url']
        old_api = self.cf['api']

        # 檢查IP與域名是否匹配(不能為CDN域名)
        api = self.api.get().strip()
        domain = re.search('https?://(?P<domain>[^/:]*)/?', api)
        if not domain:
            self.msg_box('错误', 'PR6后台域名格式错误', parent=self.super_setting)
            return False
        domain = domain.group('domain')
        try:
            ips = [ip[4][0] for ip in socket.getaddrinfo(domain, None)]
        except Exception as e:
            self.msg_box('错误', 'PR6后台域名格式错误', parent=self.super_setting)
            return False
        if len(ips) > 1:
            self.msg_box('错误', 'PR6后台域名请设定非CDN之域名', parent=self.super_setting)
            return False

        if not set(ips) & set(re.findall(r'\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}', self.ip.get())):
            self.msg_box('错误', '请确认PR6后台域名及服务器IP是否输入正确', parent=self.super_setting)
            return False

        # 檢查PR6后台網址是否正確
        api = self.api.get()
        if not bool(re.search('^https?://.*/$', api)):
            self.msg_box('错误', 'PR6后台域名输入格式有误', parent=self.super_setting)
            return

        # 檢查平台網址輸入是否正確
        url = self.url.get()
        if not bool(re.search('^https?://.*/$', url)):
            self.msg_box('错误', self.platform.get() + '平台网址输入格式有误', parent=self.super_setting)
            return

        # 儲存PR6后台網址
        r = self.save_setting(msgbox='管理员设置')
        if r:
            self.close_super_setting()

    # 修改超級密碼視窗
    def pop_admin_alter(self, *args):
        '''修改超級密碼視窗'''
        # 如果有開啟過視窗的紀錄，會先將原有視窗關閉後再另開新視窗
        if getattr(self, 'admin_alter', None):
            self.close_admin_alter()
        # 取得最上層位置(至頂顯示)
        self.admin_alter = tk.Toplevel()
        self.admin_alter.wm_attributes("-topmost", 1)
        self.admin_alter.geometry('450x230')

        # 實體化LabelFrame於最上層
        msg = ttk.LabelFrame(self.admin_alter, text=' 修改超级管理员密码 ')
        msg.pack(expand=1, fill='both')

        # 為了讓表單內容留點邊距
        ttk.Label(msg, width=5, text=' ').grid(column=0, row=0)

        # 舊密碼
        ttk.Label(msg, text='请输入旧密码：', font=self.title_font).grid(column=2, row=1, pady=5, sticky='E')
        self.entry_old_pass = tk.Entry(msg, width=30, textvariable=self.old_pass)
        self.entry_old_pass.grid(column=3, row=1, sticky='W')
        self.entry_old_pass.focus_set()
        # 新密碼
        ttk.Label(msg, text='请输入新密码：', font=self.title_font).grid(column=2, row=2, pady=5, sticky='E')
        self.entry_new_pass = tk.Entry(msg, width=30, textvariable=self.new_pass)
        self.entry_new_pass.grid(column=3, row=2, sticky='W')
        # 確認新密碼
        ttk.Label(msg, text='确认新密码：', font=self.title_font).grid(column=2, row=3, pady=5, sticky='E')
        self.entry_confirm_pass = tk.Entry(msg, width=30, textvariable=self.confirm_pass)
        self.entry_confirm_pass.grid(column=3, row=3, sticky='W')
        # 保存按鈕
        ttk.Label(msg, width=5, text=' ').grid(column=0, row=4)
        self.btn_save_admin_alter = ttk.Button(msg, text='保存')
        self.btn_save_admin_alter.grid(column=3, row=5, sticky='WE')
        self.btn_save_admin_alter.bind("<Button-1>", self.submit_admin_alter)

        # 更新輸入框、按鈕、下拉選單狀態
        self.switch_input()
        # 綁定 Enter 按鈕, 執行 submit_admin_alter
        self.admin_alter.bind('<Return>', lambda e: self.submit_admin_alter())
        # 綁定關閉視窗, 執行 close_admin_alter
        self.admin_alter.protocol('WM_DELETE_WINDOW', self.close_admin_alter)
    def close_admin_alter(self):
        '''關閉修改超級密碼視窗'''
        self.admin_alter.destroy()
        self.admin_alter = None
    def submit_admin_alter(self, *args):
        '''保存超級密碼'''
        # 檢查舊密碼是否正確
        if self.old_pass.get() != self.cf['super_pass']:
            self.msg_box('错误', '旧密码输入错误，请重新输入。', parent=self.admin_alter)
            return

        # 檢查是否有輸入新密碼
        if not self.new_pass.get():
            self.msg_box('讯息', '新超级管理员密码不能为空', parent=self.admin_alter)
            return

        # 檢查兩次新密碼是否相同
        new = self.new_pass.get()
        if new != self.confirm_pass.get():
            self.msg_box('错误', '新密码两次输入不相符，请重新输入。', parent=self.admin_alter)
            return

        # 保存超級管理員設置
        self.save_setting(msgbox='管理员密码')
        self.close_admin_alter()

    # 輸入OTP視窗
    def pop_otp(self, case, need_otp):
        '''跳出OTP視窗'''
        if case == 'backend' and self.cf['need_bk_otp'] != self.need_bk_otp.get():
            self.msg_box('錯誤', '機器人動態密碼啟用有修改，請按下保存再次嘗試')
            return
        # 判斷傳入參數決定是否彈出OTP視窗
        if not need_otp:
            self.submit_otp(case)
            return
        # 檢查管理員設置
        if self.cf['proname'] in ('', '无资料'):
            self.msg_box('错误', '活动名称未设置，请求超级管理员协助设置。')
            return
        if self.cf['api'] in ('', '无资料'):
            self.msg_box('错误', 'PR6后台域名未设置，请求超级管理员协助设置。')
            return
        if self.cf['url'] in ('', '无资料'):
            self.msg_box('错误', f"{self.cf['platform']}平台网址未设置，请求超级管理员协助设置。")
            return
        # 檢查機器人設置
        if self.cf['backend_username'] in ('', '无资料'):
            self.msg_box('错误', f"{self.cf['platform']}帐号未设置，请检查{self.cf['platform']}帐号内容。")
            return
        if self.cf['backend_password'] in ('', '无资料'):
            self.msg_box('错误', f"{self.cf['platform']}密码未设置，请检{self.cf['platform']}查密码内容。")
            return
        if self.cf['robot_act'] in ('', '无资料'):
            self.msg_box('错误', f'机器人帐号未设置，请检查机器人帐号内容。')
            return
        if self.cf['robot_pw'] in ('', '无资料'):
            self.msg_box('错误', f'机器人密码未设置，请检查机器人密码内容。')
            return
        if self.cf['platform'] == 'WG':
            if self.cf['frontend_remarks'] in ('', '无资料'):
                self.msg_box('错误', f'前台充值备注未设置，请检查前台充值备注内容。')
                return
        if self.cf['backend_remarks'] in ('', '无资料'):
            self.msg_box('错误', '后台充值备注未设置，请检查后台充值备注内容。' if self.cf['platform'] == 'WG' else '平台充值备注未设置，请检查平台充值备注内容。')
            return

        # 如果有開啟過視窗的紀錄，會先將原有視窗關閉後再另開新視窗
        if getattr(self, 'otp', None):
            self.close_otp(case)
        # 暫停任務
        self.mission.pause()
        # 獲取最上層位置
        self.otp = tk.Toplevel()
        self.otp.wm_attributes("-topmost", 1)
        self.otp.geometry('300x120')
        # 開啟一個新視窗, 要求輸入OTP
        msg = ttk.LabelFrame(self.otp, text='')
        msg.pack(expand=1, fill='both')

        # OTP輸入框 backend_otp
        self.backend_otp.set('')
        ttk.Label(msg, font=self.title_font,
                text=f'登入 请输入{self.cf["platform"] if case == "platform" else "平台"} OTP').pack()
        entry = tk.Entry(msg, width=20, textvariable=self.backend_otp)
        entry.pack()
        entry.focus_set()
        tk.Label(msg).pack()

        # 確認按鈕
        btn = ttk.Button(msg, text='确认')
        btn.pack()
        btn.bind("<Button-1>", lambda e: self.submit_otp(case))

        # 綁定 Enter 按鈕, 執行 submit_otp
        self.otp.bind('<Return>', lambda e: self.submit_otp(case))
        # 綁定關閉視窗, 執行 close_otp
        self.otp.protocol('WM_DELETE_WINDOW', lambda: self.close_otp(case))
    def close_otp(self, case):
        '''關閉OTP視窗'''
        if getattr(self, 'otp', None):
            self.otp.destroy()
            self.otp = None
        if case == 'platform':
            self.mission.resume()
    def submit_otp(self, case):
        '''提交OTP'''
        # 獲取OTP
        otp_number = self.backend_otp.get().strip()
        # 檢查OTP格式
        if ((case == 'backend' and self.cf['need_bk_otp']) or
            (case == 'platform' and self.cf['need_backend_otp'])):
            if not otp_number.isdigit():
                self.msg_box('错误', 'OTP请输入英数字', parent=self.otp)
                return
            if len(otp_number) != 6:
                self.msg_box('错误', 'OTP请输入六位数字', parent=self.otp)
                return

        if case == 'backend':
            self.bk_otp.set(otp_number)
            self.login_backend()
        if case == 'platform':
            self.cf['backend_otp'] = otp_number
            self.mission.resume()

        # 關閉OTP視窗
        self.close_otp(case)
