from . plugins import Frame
from . plugins import EndlessList
from tkinter import ttk
import tkinter as tk
from . import Apps
import werdsazxc
import threading
import platforms
import datetime
import requests
import logging
import pickle
import time
import log
import re
logger = logging.getLogger('robot')


class MainPage(Frame):
    '''主視窗'''
    def __init__(self, root):
        '''主視窗初始化'''
        self.load_setting()
        logger.info('正在讀入設定檔')
        super().__init__(root)
        self.running = True
        #檢查介面各功能設定
        self.bind_base_setting()
        #查詢本機外網IP
        self.myip = Apps.myip()

        # 讀取設定檔, 並接著更新各變數平台顯示
        self.load_setting()
        self.change_platform()

        # 初始化迴圈任務
        self.mission = Mission(self)
        logger.info('正在初始化任務')
        # self.connection = Connection(self)

        # 紀錄機器人狀態
        logger.info(f"【{self.cf['proname']}】-------------------------版本：V{self.version_number}")
        logger.info(f"PR6后台域名：{self.cf['api']}")
        logger.info(f"{self.cf['platform']}平台：{self.cf['url']}")
        logger.info(f"設定檔內容: {self.cf}")

        # 特殊項目
        self.extra_info = []

        # 初始畫面為後台登入畫面
        self.logout_backend()

    # 主畫面功能綁定 # 可以使操作者在操作介面時點擊鍵盤或點擊滑鼠，將其成為信息
    def bind_base_setting(self):
        self.robot_btn.bind('<Button-1>', self.switch_status)
        self.entry_backend_username.bind('<KeyRelease>', lambda x: self.change_setting())
        self.entry_backend_password.bind('<KeyRelease>', lambda x: self.change_setting())
        self.entry_robot_act.bind('<KeyRelease>', lambda x: self.change_setting())
        self.entry_robot_pw.bind('<KeyRelease>', lambda x: self.change_setting())
        self.cbb_update_times.bind('<<ComboboxSelected>>', lambda x: self.change_setting())
        self.cbb_switching.bind('<<ComboboxSelected>>', lambda x: self.change_setting())
        self.entry_amount_below.bind('<KeyRelease>', lambda x: self.change_setting())
        if self.cf['platform'] == 'WG':
            self.entry_frontend_remarks.bind('<KeyRelease>', lambda x: self.change_setting())
        self.entry_backend_remarks.bind('<KeyRelease>', lambda x: self.change_setting())
        self.cbb_multiple.bind('<<ComboboxSelected>>', lambda x: self.change_setting())
        # 綁定Enter按鈕觸發 save_setting
        self.btn_save.bind('<Button-1>', lambda e: self.save_setting(msgbox=self.tabControl.tab(self.tabControl.select(), "text")))
        self.entry_backend_username.bind('<Return>', lambda e: self.save_setting(msgbox=self.tabControl.tab(self.tabControl.select(), "text")))
        self.entry_backend_password.bind('<Return>', lambda e: self.save_setting(msgbox=self.tabControl.tab(self.tabControl.select(), "text")))
        self.entry_amount_below.bind('<Return>', lambda e: self.save_setting(msgbox=self.tabControl.tab(self.tabControl.select(), "text")))
        if self.cf['platform'] == 'WG':
            self.entry_frontend_remarks.bind('<Return>', lambda e: self.save_setting(msgbox=self.tabControl.tab(self.tabControl.select(), "text")))
        self.entry_backend_remarks.bind('<Return>', lambda e: self.save_setting(msgbox=self.tabControl.tab(self.tabControl.select(), "text")))
        self.cbb_multiple.bind('<Return>', lambda e: self.save_setting(msgbox=self.tabControl.tab(self.tabControl.select(), "text")))
        self.cbb_switching.bind('<Return>', lambda e: self.save_setting(msgbox=self.tabControl.tab(self.tabControl.select(), "text")))
        self.cbb_update_times.bind('<Return>', lambda e: self.save_setting(msgbox=self.tabControl.tab(self.tabControl.select(), "text")))
        # 子系统清单
        self.btn_reload.bind('<Button-1>', self.get_list)
        self.btn_save_game.bind('<Button-1>', lambda e: self.save_setting(msgbox='子系统清单设置'))
        # 點擊子系統清單
        self.tree.tag_bind('disabled', '<ButtonRelease-1>', callback=self.clk_tree)
        self.tree.tag_bind('enabled', '<ButtonRelease-1>', callback=self.clk_tree)


    # 更新按鈕錯誤訊息
    def upd_btn_msg(self):
        '''
        更新按鈕錯誤訊息, 格式為:
        (機器人運行/暫停中), ([錯誤訊息1, 錯誤訊息2, ...]), (等待連線...)
        若沒有錯誤訊息, 則沒有中間錯誤訊息
        若沒有連線異常(或自動重連設定為關閉), 則沒有最後等待連線
        '''
        # 是否有錯誤訊息
        if self.cf['error_msg']:
            error_msg = f",{','.join(self.cf['error_msg'])}"
        else:
            error_msg = ''
        # 是否有獲取子系统清单
        if not self.cf['game']:
            game_msg = ',子系统清单尚未取得'
        else:
            game_msg = ''

        # 修改按鈕顏色
        if ('连线异常' in ','.join(self.cf['error_msg']) or 
            '解析错误' in ','.join(self.cf['error_msg']) or
            not self.cf['connect']):
            self.style.configure('CL.TButton', foreground='#B8860B')
        elif '无法运行' in self.run_robot.get():
            self.style.configure('CL.TButton', foreground='red')
        elif re.split("[：,]", self.run_robot.get())[0] in ('启动机器人', '机器人暂停中'):
            self.style.configure('CL.TButton', foreground='black')
        else:
            self.style.configure('CL.TButton', foreground='green')

        # 回傳按鈕內容
        self.run_robot.set((
            f'{re.split("[：,]", self.run_robot.get())[0]}'
            f'{error_msg}'
            f'{game_msg}'
        ))
    # 更新標題
    def upd_title(self, **kwargs):
        '''
        更新所有視窗(主畫面、超級密碼、管理員設置、修改超級密碼視窗、OTP)標題
        標題格式為：
        機器人設置:平台 活動大廳機器人 - 管理員設置:機器人窗口標題 - 機器人設置:機器人帳號
        '''
        self.title = (
            f"{kwargs.get('platform') or self.platform.get()} "
            f"{kwargs.get('proname') or self.proname.get()} - "
            f"{kwargs.get('robot_act') or self.robot_act.get()}"
            f" - {self.myip}"
        )
        # 更新[主畫面]標題
        self.root.title(self.title)
        # 更新[超級密碼]標題
        admin_page = getattr(self, 'admin_page', None)
        try:
            if admin_page:
                admin_page.title(self.title)
        except Exception as e:
            pass
        # 更新[管理員設置]標題
        super_setting = getattr(self, 'super_setting', None)
        try:
            if super_setting:
                super_setting.title(self.title)
        except Exception as e:
            pass
        # 更新[修改超級密碼視窗]標題
        admin_alter = getattr(self, 'admin_alter', None)
        try:
            if admin_alter:
                admin_alter.title(self.title)
        except Exception as e:
            pass
        # 更新[OTP]標題
        otp = getattr(self, 'otp', None)
        try:
            if otp:
                otp.title(self.title)
        except Exception as e:
            pass
    # 更新輸入框、下拉選單、按鈕是否可更改
    def switch_input(self):
        if self.run_robot.get() == '机器人运行中':
            try:
                # 機器人設置
                self.entry_backend_username.config(state='disabled')
                self.entry_backend_password.config(state='disabled')
                self.entry_robot_act.config(state='disabled')
                self.entry_robot_pw.config(state='disabled')
                self.chk_backend_otp.config(state='disabled')
                self.chk_bk_otp.config(state='disabled')
                # 管理員設置
                self.cbb_platform.config(state='disabled')
                self.entry_api.config(state='disabled')
                self.entry_url.config(state='disabled')
                self.entry_ip.config(state='disabled')
            except Exception as e:
                # logger.debug(f'{e.__class__.__name__}: {e}')
                pass
        else:
            try:
                # 機器人設置
                self.entry_backend_username.config(state='normal')
                self.entry_backend_password.config(state='normal')
                self.entry_robot_act.config(state='normal')
                self.entry_robot_pw.config(state='normal')
                self.chk_backend_otp.config(state='normal')
                self.chk_bk_otp.config(state='normal')
                # 管理員設置
                self.cbb_platform.config(state='readonly')
                self.entry_api.config(state='normal')
                self.entry_url.config(state='normal')
                self.entry_ip.config(state='normal')
            except Exception as e:
                # logger.debug(f'{e.__class__.__name__}: {e}')
                pass
    # 按下機器人按鈕, 切換機器人狀態
    def switch_status(self, user=True, *args):
        '''
        更新主要動作按鈕文字.
        首次案下會啟動mission.
        如運行中會暫停mission.
        如暫停中會重啟mission.
        '''
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
                self.msg_box('错误', f"{self.cf['platform']}前台充值备注未设置，请检查{self.cf['platform']}前台充值备注内容。")
                return
        if self.cf['backend_remarks'] in ('', '无资料'):
            self.msg_box('错误', f"{self.cf['platform']}后台充值备注未设置，请检查{self.cf['platform']}后台充值备注内容。" if self.cf['platform'] == 'WG' else f"{self.cf['platform']}平台充值备注未设置，请检查{self.cf['platform']}平台充值备注内容。")
            return
        # 檢查子系统清单設置
        if not list(filter(lambda g: g.monitor == 1 or g.deposit == 1, self.cf['game_setting'].values())):
            self.msg_box('错误', f'子系统清单请至少任选一项启用。')
            return

        # 檢查修改設置後有儲存
        if '设定已修改, 请保存后继续' in self.cf['error_msg']:
            self.msg_box('错误', f"{','.join(self.cf['error_msg'])}, 请储存后继续")
            return

        # 檢查子系统清单是否正常取得
        if not self.cf['game_setting']:
            self.msg_box('错误', '子系统清单尚未取得, 请尝试重新读取子系统清单')
            return

        # 切換按鈕文字、執行續狀態
        w = self.run_robot.get()
        if '启动机器人' in w:
            if user:
                logger.info('操作者按下【啟動】按鈕')
                self.cf['connect'] = Apps.canConnect(self.cf)
            self.mission.resume()
            self.run_robot.set('机器人运行中')
            self.style.configure('CL.TButton', foreground='green')
            self.switch_input() #判斷按鍵狀態
            return
        if '机器人运行中' in w:
            if user:
                logger.info('操作者按下【暫停】按鈕')
            self.mission.pause()
            self.run_robot.set('机器人暂停中')
            self.style.configure('CL.TButton', foreground='black')
            self.switch_input()
            return
        if '机器人暂停中' in w:
            if user:
                logger.info('操作者按下【繼續運行】按鈕')
                self.cf['connect'] = Apps.canConnect(self.cf)
            self.mission.resume()
            self.run_robot.set('机器人运行中')
            self.style.configure('CL.TButton', foreground='green')
            self.switch_input()
            return

    # 存、讀 設定檔
    def load_setting(self, game=False, token=''):
        '''讀取設定資訊, 並更新畫面'''
        with open('config/setting.ddl', 'rb') as f:
            self.cf = werdsazxc.Dict({k:v for k,v in pickle.load(f).items() if not k.startswith('//')}) #startswith 判斷字首

            self.cf['game'] = game
            self.cf['token'] = token
            self.cf['connect'] = Apps.canConnect(self.cf)
            self.cf['timeout'] = int(self.cf['timeout'])
            self.cf['update_times'] = int(self.cf['update_times'])
        for key, value in self.cf.items():
            entry = getattr(self, key, None)
            if not entry:
                continue
            if key == 'super_pass':
                pass
            elif key == 'update_times':
                entry.set(f'{value}秒')
            elif key == 'timeout':
                entry.set(f'{value}秒')
            elif key == 'switching':
                entry.set('开启' if bool(value) else '关闭')
            elif key == 'recharg':
                entry.set('开启' if bool(value) else '关闭')
            elif key == 'multiple':
                entry.set(f'{value}倍打码')
            else:
                entry.set(value)
    def save_setting(self, keys=None, msgbox=True):
        '''檢查, 並儲存設定資訊'''
        # 根據msgbox設定keys
        if msgbox == '机器人设置':
            parent = self
            keys = [
                'backend_username', 'backend_password', 'robot_act', 'robot_pw', 'update_times',
                'switching', 'amount_below', 'backend_remarks', 'multiple',
                'need_backend_otp', 'need_bk_otp', 'timeout', 'recharg'
            ]
            if self.cf['platform'] == 'WG':
                keys.append('frontend_remarks')
        if msgbox == '子系统清单设置':
            parent = self
            keys = ['game_setting']
        if msgbox == '管理员设置':
            parent = self.super_setting
            keys = ['platform', 'proname', 'api', 'url', 'secret_key', 'ip']
        if msgbox == '管理员密码':
            parent = self.admin_alter
            keys = ['super_pass']

        # 帳密有修改時清空cookie
        if (self.url.get() != self.cf['url'] or
            self.backend_username.get() != self.cf['backend_username'] or
            self.backend_password.get() != self.cf['backend_password']):
            platform = getattr(platforms, self.cf['platform'].lower())
            platform.session.cookies.clear()
            platform.session.login = False
        # 變更到WG平台，更新子系統清單視窗    
        if self.platform.get() != self.cf['platform']:
            sig = self.platform.get() == 'WG' or self.cf['platform'] == 'WG'
            self.cf['platform'] = self.platform.get()
            self.change_platform()
            if sig:
                self.tab0.destroy()
                self.setup_base_setting(mode=0)
                self.tab3.destroy()
                self.setup_game_setting()
                # 子系统清单
                self.btn_reload.bind('<Button-1>', self.get_list)
                self.btn_save_game.bind('<Button-1>', lambda e: self.save_setting(msgbox='子系统清单设置'))
                # 點擊子系統清單
                self.tree.tag_bind('disabled', '<ButtonRelease-1>', callback=self.clk_tree)
                self.tree.tag_bind('enabled', '<ButtonRelease-1>', callback=self.clk_tree)

        # 修改PR6后台帳密時登出
        if (self.api.get() != self.cf['api'] or
            self.robot_act.get() != self.cf['robot_act'] or
            self.robot_pw.get() != self.cf['robot_pw']):
            self.logout_backend()
        # 逐一檢查變數是否填寫
        for key in keys:
            if key == 'game_setting':
                continue
            elif key == 'super_pass':
                continue
            elif getattr(self, key).get() in ('', '无资料'):
                lbl = getattr(self, f'lbl_{key}')['text']
                self.msg_box('错误', f'{lbl} 未填写')
                return
        # 逐一檢查變數格式
        for key in keys:
            if key == 'game_setting':
                # 逐一儲存活動狀態
                idxs = self.tree.get_children()
                for idx in idxs:
                    values = self.tree.item(idx, 'values')
                    mod = list(filter(lambda x: x.name == values[3], self.cf['game_setting'].values()))[0].mod
                    # 監控設定
                    if values[1] == '启用':
                        monitor = 1
                    if values[1] == '关闭':
                        monitor = 0
                    if values[1] == '无':
                        monitor = 0
                    if values[1] == '不支援':
                        monitor = -1
                    self.cf['game_setting'][mod].monitor = monitor
                    # 充值設定
                    if values[2] == '启用':
                        deposit = 1
                    if values[2] == '关闭':
                        deposit = 0
                    if values[2] == '无':
                        deposit = 0
                    if values[2] == '不支援':
                        deposit = -1
                    self.cf['game_setting'][mod].deposit = deposit
                    # 充值ID設定
                    if self.cf['platform'] == 'WG' and values[4]:
                        if self.cf.get('rechargeDict'):
                            self.cf['rechargeDict'][mod] = values[4]
                        else:
                            self.cf['rechargeDict'] = {mod: values[4]}
                        logger.info(f'●充值ID【{values[4]}】（{mod}）存檔')

            elif key == 'update_times':
                update_times = self.update_times.get().replace('秒', '').strip().replace('\n', '')
                try:
                    self.cf[key] = int(update_times)
                except ValueError:
                    self.msg_box('错误', '刷新频率请设定数字')
                    return
            elif key == 'multiple':
                self.cf[key] = getattr(self, key).get().replace('倍打码','').strip().replace('\n', '')
            elif key == 'switching':
                self.cf[key] = getattr(self, key).get() == '开启'
            elif key == 'recharg':
                self.cf[key] = getattr(self, key).get() == '开启'
            elif key == 'amount_below':
                try:
                    self.cf[key] = float(getattr(self, key).get().strip().replace('\n', ''))
                except ValueError as e:
                    self.msg_box('错误', '小额自动异常', parent=self)
                    return
            elif key == 'super_pass':
                self.cf[key] = getattr(self, 'new_pass').get().strip().replace('\n', '')
            elif key == 'timeout':
                timeout = self.timeout.get().replace('秒', '').strip().replace('\n', '')
                try:
                    self.cf[key] = int(timeout)
                except ValueError:
                    self.msg_box('错误', '等待回应时间请设定数字')
                    return
            elif key in ('need_backend_otp', 'need_bk_otp'):
                self.cf[key] = getattr(self, key).get()
            else:
                self.cf[key] = getattr(self, key).get().strip().replace('\n', '')

        # 儲存設定檔
        try:
            self.cf['connect'] = False
            self.cf['bk_otp'] = ''
            self.cf['error_msg'] = []
            self.cf['backend_otp'] = ''
            with open('config/setting.ddl', 'wb') as f:
                pickle.dump(self.cf, f)
            self.load_setting(game=self.cf['game'], token=self.cf['token'])
            if msgbox:
                self.msg_box('讯息', f'[{msgbox}]保存成功', parent=parent)
            self.change_setting()
            # 活動PR6后台更動, 獲取子系统清单
            if 'api' in keys:
                self.logout_backend()
            logger.info(self.cf)
            return True
        except PermissionError as e:
            self.msg_box('错误', '无档案存取权限，请关闭相关视窗后重试', parent=parent)
        except Exception as e:
            self.msg_box('错误', f'设定档储存失败({e.__class__.__name__})，请联系软件管理员。({e})', parent=self)
            werdsazxc.log_trackback()
    def change_setting(self):
        '''檢查設定資訊, 有異動時在error_msg中紀錄, 阻止機器人繼續運行'''
        for key, value in self.cf.items():
            entry = getattr(self, key, None)
            if not entry:
                continue
            if key in ('super_pass', 'backend_otp'):
                continue
            elif key == 'update_times':
                nvalue = re.search('[0-9]+', entry.get()).group()
                nvalue = int(nvalue)
            elif key == 'switching':
                nvalue = (entry.get() == '开启')
            elif key == 'multiple':
                nvalue = re.search('[0-9]+', entry.get()).group()
            elif key == 'amount_below':
                try:
                    nvalue = float(entry.get())
                except ValueError as e:
                    nvalue = 0
            elif key == 'timeout':
                try:
                    timeout = self.timeout.get().replace('秒', '').strip().replace('\n', '')
                    nvalue = int(timeout)
                except ValueError as e:
                    nvalue = 5
            elif key == 'recharg':
                nvalue = (entry.get() == '开启')
            else:
                nvalue = entry.get()

            if nvalue != value:
                if '设定已修改, 请保存后继续' not in self.cf['error_msg']:
                    self.cf['error_msg'].extend(['设定已修改, 请保存后继续'])
                self.upd_btn_msg()
                self.upd_title()
                return

        # 子系统清单逐一檢查是否啟用
        idxs = self.tree.get_children()
        for idx in idxs:
            values = self.tree.item(idx, 'values')
            mod = list(filter(lambda x: x.name == values[3], self.cf['game_setting'].values()))[0].mod
            game = self.cf['game_setting'][mod]
            # 讀取監控設定
            if values[1] == '启用':
                monitor = 1
            if values[1] == '关闭':
                monitor = 0
            if values[1] == '无':
                monitor = 0
            if values[1] == '不支援':
                monitor = -1
            if monitor != game.monitor:
                if '设定已修改, 请保存后继续' not in self.cf['error_msg']:
                    self.cf['error_msg'].extend(['设定已修改, 请保存后继续'])
                self.upd_btn_msg()
                self.upd_title()
                return
            # 讀取充值設定
            if values[2] == '启用':
                deposit = 1
            if values[2] == '关闭':
                deposit = 0
            if values[2] == '无':
                deposit = 0
            if values[2] == '不支援':
                deposit = -1
            if deposit != game.deposit:
                if '设定已修改, 请保存后继续' not in self.cf['error_msg']:
                    self.cf['error_msg'].extend(['设定已修改, 请保存后继续'])
                self.upd_btn_msg()
                self.upd_title()
                return

        # 恢復按鈕文字
        self.cf['error_msg'] = [
            msg for msg in self.cf['error_msg']
            if msg != '设定已修改, 请保存后继续'
        ]
        self.upd_btn_msg()
    def change_platform(self, *args):
        self.upd_title(platform=self.platform.get())
        self.lbl_backend_username.config(text=f'{self.platform.get()}帐号：')
        self.lbl_backend_password.config(text=f'{self.platform.get()}密码：')
        if self.cf['platform'] == 'WG':
            self.lbl_frontend_remarks.config(text=f'{self.platform.get()}前台充值备注：')
        self.lbl_backend_remarks.config(text=f'{self.platform.get()}后台充值备注：' if self.cf['platform'] == 'WG' else f'{self.platform.get()}充值备注：')
        self.lbl_multiple.config(text=f'{self.platform.get()}打码量：')
        lbl_url = getattr(self, 'lbl_url', None)
        if lbl_url:
            lbl_url.config(text=f'{self.platform.get()}平台网址：')
        self.change_setting()

    # 登入登出機器人PR6后台
    def login_backend(self):
        if self.cf['token']:
            return
        if not self.robot_act.get():
            self.msg_box('错误', '请输入机器人帐号')
            return
        if not self.robot_pw.get():
            self.msg_box('错误', '请输入机器人密码')
            return
        if self.cf['need_bk_otp'] and not self.bk_otp.get():
            self.msg_box('错误', '请输入动态密码')
            return
        if self.cf['need_bk_otp'] and not re.search('^[0-9]{6}$', self.bk_otp.get()):
            self.msg_box('错误', '动态密码请输入6位数字')
            return

        self.cf['robot_act'] = self.robot_act.get().strip()
        self.cf['robot_pw'] = self.robot_pw.get().strip()
        self.cf['bk_otp'] = self.bk_otp.get().strip()
        result = self.login()
        if result != '登入成功':
            return
        self.get_list(msg=False)
        self.bk_otp.set('')
        super().login_backend()
    def logout_backend(self):
        self.cf['token'] = ''
        super().logout_backend()

    # 點選遊戲清單, 右側畫面切換
    def clk_tree(self, event, *args):
        idx = self.tree.focus()
        if not idx:
            return

        # 切換右側當前內容
        values = self.tree.item(idx)['values']
        i, monitor, deposit, name, rechargeID, mod = values
        platform = getattr(platforms, self.cf['platform'].lower())
        func = getattr(platform.mission, self.cf.game_setting[mod].func, None)
        if not func:
            return

        # 清空支援遊戲清單
        self.suport_tree.delete(*self.suport_tree.get_children())
        # 清空額外參數
        for i in range(len(self.extra_info)):
            key, var, extra_info = self.extra_info.pop()
            if extra_info:
                try:
                    extra_info.grid_forget()
                except AttributeError as e:
                    pass

        # 定義修改額外參數呼叫的function
        def chg(*args):
            game.extra = {
                key: var.get()
                for key, var, lbl in self.extra_info
                if key and var
            }

        # 說明文字
        docs = []
        if hasattr(func, 'audit'):
            docs.extend([func.audit.__doc__, ''])
        if hasattr(func, 'deposit'):
            docs.extend([func.deposit.__doc__, 200*' '])
        self.action_info.set('\n'.join(docs))

        extra = getattr(func.Meta, 'extra', {})
        suport = getattr(func.Meta, 'suport', {})
        # 設定參數
        if extra:
            for i, (key, value) in enumerate(extra.items()):
                if value['var'] in (int, float):
                    var = tk.IntVar()
                    var.set(game.extra.get(key, value.get('default', 0)))
                    var.trace('w', lambda name, index, mode: chg())
                    lbl = ttk.Label(self.tab3, text=value['info'], font=self.title_font)
                    lbl.grid(column=3, row=8 + i, padx=5, pady=5, sticky='WS')
                    entry = tk.Entry(self.tab3, textvariable=var)
                    entry.grid(column=4, row=8 + i, pady=5, sticky='WS')
                    self.extra_info.extend([(None, None, lbl)])
                    self.extra_info.extend([(key, var, entry)])
                elif value['var'] == list:
                    var = tk.StringVar()
                    var.set(game.extra.get(key, value.get('default', '')))
                    var.trace('w', lambda name, index, mode: chg())
                    lbl = ttk.Label(self.tab3, text=value['info'], font=self.title_font)
                    lbl.grid(column=3, row=8 + i, padx=5, pady=5, sticky='WS')
                    entry = tk.ttk.Combobox(self.tab3, textvariable=var, state='readonly')
                    entry['values'] = value.get('choice', [])
                    entry.grid(column=4, row=8 + i, pady=5, sticky='WS')
                    self.extra_info.extend([(None, None, lbl)])
                    self.extra_info.extend([(key, var, entry)])
                elif value['var'] == str:
                    var = tk.StringVar()
                    var.set(game.extra.get(key, value.get('default', '')))
                    var.trace('w', lambda name, index, mode: chg())
                    lbl = ttk.Label(self.tab3, text=value['info'], font=self.title_font)
                    lbl.grid(column=3, row=8 + i, padx=5, pady=5, sticky='WS')
                    entry = tk.Entry(self.tab3, textvariable=var)
                    entry.grid(column=4, row=8 + i, pady=5, sticky='WS')
                    self.extra_info.extend([(None, None, lbl)])
                    self.extra_info.extend([(key, var, entry)])
                elif value['var'] == bool:
                    var = tk.BooleanVar()
                    var.set(game.extra.get(key, value.get('default', False)))
                    chk = tk.Checkbutton(self.tab3, text=value['info'], variable=var,
                        onvalue=True, offvalue=False, state='disabled', command=chg)
                    chk.grid(column=3, row=8 + i, padx=5, pady=5, sticky='WS')
                    self.extra_info.extend([(key, var, chk)])
        # 設定支援遊戲清單
        if suport:
            for i, (key, value) in enumerate(suport.items()):
                self.suport_tree.insert('', i, values=(i, value))
        
        pop_menu = tk.Menu(self.root, tearoff=0)
        # 設定選單監控功能
        if monitor != '无':
            label1 = f'{"关闭" if monitor == "启用" else "启用"}监控'
            pop_menu.add_command(
                label=label1,
                command=lambda: self.clk_game_menu(idx, label1)
            )
        # 設定選單充值功能
        if deposit != '无':
            label2 = f'{"关闭" if deposit == "启用" else "启用"}充值'
            pop_menu.add_command(
                label=label2,
                command=lambda: self.clk_game_menu(idx, label2, name, rechargeID, mod)
            )
            if self.cf['platform'] == 'WG' and deposit == "启用":
                label3 = f'修改充值ID'
                pop_menu.add_command(
                    label=label3,
                    command=lambda: self.clk_game_menu(idx, label3, name, rechargeID, mod)
                )
            if self.cf['platform'] == 'WG' and deposit == "关闭":
                if rechargeID:
                    label4 = f'清空充值ID'
                    pop_menu.add_command(
                        label=label4,
                        command=lambda: self.clk_game_menu(idx, label4, name, rechargeID, mod)
                    )                
        # 運行選單
        try:
            pop_menu.post(event.x_root, event.y_root)
        finally:
            pop_menu.grab_release()

    def clk_game_menu(self, idx, command, name='', rechargeID='', mod='', *args):
        status = command[:2]
        column = command[2:]
        # 調整文字
        if '监控' == column:
            self.monitor.set(0)
            self.tree.set(idx, column='two', value=status)
        if '充值' in column:
            if '充值' == column:
                self.deposit.set(0)
                self.tree.set(idx, column='three', value=status)            
            if self.cf['platform'] == 'WG':
                if '启用' in status or '修改' in status:
                    self.pop_recharge(name, rechargeID, mod, idx)
                if '清空' in status:
                    logger.info(f'●充值ID【{rechargeID}】（{mod}）清空')
                    self.tree.set(idx, column='five', value='')  
                    self.cf['rechargeDict'][mod] = ''
                    if '设定已修改, 请保存后继续' not in self.cf['error_msg']:
                        self.cf['error_msg'].extend(['设定已修改, 请保存后继续'])
                        self.upd_btn_msg()
                        self.upd_title()
                        return
        # 調整顏色
        if '关闭' in self.tree.item(idx)['values']:
            tags = 'disabled'
        else:
            tags = 'enabled'
        self.tree.item(idx, tags=tags)
        # 切換按鈕狀態
        self.change_setting()


    # 登入並重新讀取遊戲清單
    def login(self, msg=True, *args):
        # 登入
        logger.info('===與PR6后台連線====================')
        self.run_robot.set('机器人PR6后台登入中...')
        result = Apps.login_api(self.cf)
        logger.info(f"[{'登入成功' if self.cf['token'] else '登入失敗'}]: {self.cf['token']}")
        if result != '登入成功':
            if msg:
                self.msg_box('错误', result)
            self.cf['game'] = False
            self.run_robot.set('启动机器人')
            return result
        self.run_robot.set('启动机器人')
        return result
    def get_list(self, msg=True, *args):
        if not self.cf['token']:
            self.msg_box('错误', '您尚未登入, 请登入后重试', parent=self)
            return
        self.tree.delete(*self.tree.get_children())
        # 獲取子系统清单
        logger.info('===獲取子系统清单======================')
        result = Apps.get_list(self.cf)
        if type(result) == str:
            if '该帐号已在其他地方登入' in result or '连线逾时' in result:
                self.logout_backend()
            if msg:
                self.msg_box('错误', result)
            return

        platform = getattr(platforms, self.cf['platform'].lower(), None)
        for i, (mod, setting) in enumerate(self.cf['game_setting'].items()):
            action = getattr(platform.mission, setting.func, None)
            # 判斷監控文字
            if hasattr(action, 'audit'):
                monitor = '启用' if setting.monitor else '关闭'
            else:
                monitor = '无'
            # 判斷充值文字
            if hasattr(action, 'deposit'):
                deposit = '启用' if setting.deposit else '关闭'
            else:
                deposit = '无'
            # 調整tag
            if ['无', '无'] == [monitor, deposit]:
                tag = 'error'
            elif '关闭' in [monitor, deposit]:
                tag = 'disabled'
            else:
                tag = 'enabled'
            # 畫面設定
            # self.tree.insert('', i, values=(i, monitor, deposit, setting.name, mod), tags=tag)
            try:
                rechargeID = self.cf['rechargeDict'].get(mod, '')
            except:
                rechargeID = ''
            logger.info(f'●充值ID【{rechargeID}】（{mod}）清單') if rechargeID else None
            self.tree.insert('', i, values=(i, monitor, deposit, setting.name, rechargeID, mod), tags=tag)
        self.save_setting(keys=['game_setting'], msgbox='')
        self.cf['game'] = True
        self.change_setting()

    # 管理員視窗功能綁定
    def pop_super_setting(self, *args):
        super().pop_super_setting()
        # 管理員設置
        self.entry_proname.bind('<KeyRelease>', lambda x: self.change_setting())
        self.entry_api.bind('<KeyRelease>', lambda x: self.change_setting())
        self.entry_url.bind('<KeyRelease>', lambda x: self.change_setting())
        self.entry_secret_key.bind('<KeyRelease>', lambda x: self.change_setting())
        self.btn_save_supper_setting.bind("<Button-1>", self.submit_super_setting)
        self.super_setting.bind('<Return>', lambda e: self.submit_super_setting())

    # 已登入狀態, 呼叫登出api, 結束視窗
    def quit(self, *args):
        def _quit():
            self.running = False
            self.mission.task = False
            self.mission.join()

            if self.cf['token']:
                Apps.logout_api(self.cf)

            try:
                self.root.quit()
                logger.info('關閉GUI：成功')
            except Exception as e:
                logger.info(f'關閉GUI：失敗 ({e})')

        logger.info('=== 使用者按下关闭按钮 ========================')
        self.style.configure('CL.TButton', foreground='#B8860B')
        self.run_robot.set('等待线程关闭中')
        threading.Thread(target=_quit).start()


class Mission(threading.Thread):
    '''任務執行續'''
    def __init__(self, parent, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.parent = parent
        self.__flag = threading.Event()
        self.task = False
        self.start()

    def pause(self):
        self.__flag.clear()

    def resume(self):
        self.__flag.set()

    def run(self):
        checklist = EndlessList(list(filter(lambda g: g.monitor == 1 or g.deposit == 1, self.parent.cf['game_setting'].values())))
        self.task = False
        while self.parent.running:
            try:
                # 更新時間
                t = '本地时间为：' + time.strftime(r'%Y-%m-%d %H:%M:%S', time.localtime())
                self.parent.clock.set(t)

                # 暫停執行續直到resume
                if not self.__flag.is_set():
                    time.sleep(1)
                    continue
                # 無法運行狀態
                if '机器人无法运行' in self.parent.run_robot.get():
                    continue
                # 更新按鈕文字
                self.parent.upd_btn_msg()

                # 連線異常狀態, 進行連線測試後重新進行迴圈
                if bool(self.parent.cf['connect']) is False:
                    if self.parent.cf.switching:
                        self.parent.cf['connect'] = Apps.canConnect(self.parent.cf)
                    else:
                        self.parent.run_robot.set('机器人无法运行：网路状态异常，且自动连接设置关闭，请确认网路状态后，重启程序。')
                        self.parent.style.configure('CL.TButton', foreground='red')
                    time.sleep(1)
                    continue
                else:
                    self.parent.cf['error_msg'] = []
                    self.parent.upd_btn_msg()

                # 使用者修改了設定, 可能導致錯誤, 中斷一秒後再次嘗試
                if '设定已修改, 请保存后继续' in self.parent.cf['error_msg']:
                    time.sleep(1)
                    continue

                # 重新讀取設定檔確保使用最新設定執行任務
                with open('config/setting.ddl', 'rb') as f:
                    cf = werdsazxc.Dict({k:v for k,v in pickle.load(f).items() if not k.startswith('//')})
                    self.parent.cf['update_times'] = cf['update_times']
                    self.parent.cf['switching'] = cf['switching']
                    self.parent.cf['amount_below'] = cf['amount_below']
                    if self.parent.cf['platform'] == 'WG':
                        self.parent.cf['frontend_remarks'] = cf['frontend_remarks']
                    self.parent.cf['backend_remarks'] = cf['backend_remarks']
                    self.parent.cf['multiple'] = cf['multiple']
                    self.parent.cf['proname'] = cf['proname']
                    self.parent.cf['secret_key'] = cf['secret_key']

                # 讀取平台
                platform = getattr(platforms, self.parent.cf['platform'].lower())

                # 保持PR6后台登入狀態
                logger.info(f"===[延長登入狀態]==========================")
                Apps.keep_connect(self.parent.cf)

                # 連線異常狀態, 中斷一秒後再次嘗試
                if not self.parent.cf['connect']:
                    time.sleep(1)
                    continue

                # 當未登入前(彈跳OTP)進行登入
                if not platform.session.login:
                    if self.parent.cf['need_backend_otp']:
                        # 跳出OTP視窗
                        self.parent.pop_otp('platform', self.parent.cf['need_backend_otp'])
                        self.__flag.wait()
                        # 使用者中斷輸入otp
                        if not self.parent.cf['backend_otp']:
                            self.parent.run_robot.set('机器人暂停中')
                            self.parent.style.configure('CL.TButton', foreground='black')
                            self.parent.switch_input()
                            self.parent.upd_btn_msg()
                            self.pause()
                            continue
                    # 登入平台
                    logger.info(f"===與平台連線[{self.parent.cf['backend_username']}]:[{self.parent.cf['backend_password']}]====================")
                    login_result = Apps.login_platform(
                        platform=platform,
                        url=self.parent.cf['url'],
                        acc=self.parent.cf['backend_username'],
                        pw=self.parent.cf['backend_password'],
                        otp=self.parent.cf['backend_otp'],
                        timeout=self.parent.cf['timeout'],
                        cf=self.parent.cf
                    )
                    logger.info(f'平台登入狀況:{login_result}')
                    # 登入失敗時顯示錯誤, 中斷迴圈
                    if login_result != '登入成功':
                        self.parent.run_robot.set('机器人暂停中')
                        self.parent.style.configure('CL.TButton', foreground='black')
                        self.parent.switch_input()
                        self.parent.upd_btn_msg()
                        self.pause()
                        self.parent.msg_box('错误', f'登入平台失败：{login_result}')
                        continue

                # 檢查子系統設定是否需要額外登入平台
                flag = False
                for game, setting in self.parent.cf['game_setting'].items():
                    if set(['platform', 'url', 'username', 'password', 'need_otp']) - setting['extra'].keys():
                        continue
                    platform2 = getattr(platforms, setting['extra'].platform.lower())
                    if platform2.session.login:
                        continue
                    if setting['monitor'] != 1:
                        continue
                    if setting['extra'].need_otp:
                        # 跳出OTP視窗
                        self.parent.pop_otp('platform', setting['extra'].need_otp)
                        self.__flag.wait()
                        # 使用者中斷輸入otp
                        if not self.parent.cf['backend_otp']:
                            self.parent.run_robot.set('机器人暂停中')
                            self.parent.style.configure('CL.TButton', foreground='black')
                            self.parent.switch_input()
                            self.parent.upd_btn_msg()
                            self.pause()
                            flag = True
                            continue
                    # 登入平台
                    logger.info(f"===與平台連線[{setting['extra'].platform}]:[{setting['extra'].username}]====================")
                    login_result = Apps.login_platform(
                        platform=platform2,
                        url=setting['extra'].url,
                        acc=setting['extra'].username,
                        pw=setting['extra'].password,
                        otp=self.parent.cf['backend_otp'],
                        timeout=self.parent.cf['timeout'],
                        cf=self.parent.cf
                    )
                    # 登入失敗時顯示錯誤, 中斷迴圈
                    if login_result != '登入成功':
                        self.parent.run_robot.set('机器人暂停中')
                        self.parent.style.configure('CL.TButton', foreground='black')
                        self.parent.cf['error_msg'].clear()
                        self.parent.switch_input()
                        self.parent.upd_btn_msg()
                        self.pause()
                        self.parent.msg_box('错误', f'登入平台失败：{login_result}')
                        flag = True
                        continue
                if flag:
                    continue

                # 開始任務前若平台需要保持連線則先進行此動作
                if getattr(platform, 'KEEP_LOGIN', False):
                    res = platform.activation_token(
                        cf=self.parent.cf,
                        url=self.parent.cf['url'],
                        timeout=self.parent.cf['timeout']
                    )
                    if res['IsSuccess'] is False:
                        platform.session.login = False
                        continue

                # 檢查遊戲清單設置是否異動
                checklist1 = EndlessList(list(filter(lambda g: g.monitor == 1 or g.deposit == 1, self.parent.cf['game_setting'].values())))
                if checklist1 != checklist:
                    checklist = checklist1
                if self.task is False:
                    # 索取任務
                    try:
                        mod = next(checklist)
                    except IndexError as e:
                        self.parent.run_robot.set('机器人暂停中')
                        self.parent.style.configure('CL.TButton', foreground='black')
                        self.parent.switch_input()
                        self.parent.upd_btn_msg()
                        self.pause()
                        self.parent.msg_box('错误', '子系统清单请至少任选一项启用。')
                        continue
                    # 索取任務
                    logger.info(f"===索取任務[{mod['name']}]==========================")
                    item = Apps.get_task(mod, self.parent.cf)
                    # 沒有任務要處理, 休息一秒後重試
                    if item is True:
                        time.sleep(int(self.parent.cf['update_times']))
                        continue
                    # 連線異常處理
                    if '重试超过上限' in item:
                        self.parent.upd_btn_msg()
                        time.sleep(int(self.parent.cf['update_times']))
                        continue
                    # 異常狀態
                    if type(item) == str:
                        if '机器人帐号已在其他地方登入' in item or '连线逾时' in item:
                            self.parent.logout_backend()
                        self.parent.run_robot.set('机器人暂停中')
                        self.parent.style.configure('CL.TButton', foreground='black')
                        self.parent.switch_input()
                        self.parent.upd_btn_msg()
                        self.pause()
                        self.parent.msg_box('错误', f'【{mod["name"]}】{item}')
                        continue
                    # 清除錯誤訊息, 正式開始新一輪任務
                    self.parent.cf['error_msg'].clear()

                # 取得任務目標功能
                self.task = True
                action = getattr(platform.mission, mod['func'])
                # 檢查是否為監控任務、是否設定為啟用
                if item['data']['Action'] == 'chkbbin' and mod['monitor'] != 1:
                    if mod['monitor'] == -1:
                        monitor = '不支援'
                    elif not hasattr(action, 'audit'):
                        monitor = '无'
                    else:
                        monitor = '关闭'
                    self.parent.msg_box('错误', f"任务目标[{mod['name']}] 监控状态为:{monitor}")
                    self.parent.run_robot.set('机器人暂停中')
                    self.parent.style.configure('CL.TButton', foreground='black')
                    self.parent.switch_input()
                    self.parent.upd_btn_msg()
                    self.pause()
                    continue
                if item['data']["Action"] == 'chkpoint' and mod['deposit'] != 1:
                    deposit = '不支援' if mod['deposit'] == -1 else '关闭'
                    self.parent.msg_box('错误', f"任务目标[{mod['name']}] 充值状态为:{deposit}")
                    self.parent.run_robot.set('机器人暂停中')
                    self.parent.style.configure('CL.TButton', foreground='black')
                    self.parent.switch_input()
                    self.parent.upd_btn_msg()
                    self.pause()
                    continue
                # 檢查資料內容不能含有item、cf、timeout、url、extra
                error_keys = {'item', 'cf', 'timeout', 'url', 'extra', 'suport', 'liveTime'} & item["data"].keys()
                if error_keys:
                    self.parent.msg_box('错误', f'參數錯誤, data內不能含有{error_keys}參數')
                    self.parent.run_robot.set('机器人暂停中')
                    self.parent.style.configure('CL.TButton', foreground='black')
                    self.parent.switch_input()
                    self.parent.upd_btn_msg()
                    self.pause()
                    continue

                # 執行任務
                logger.info(f"===執行任務[{mod['name']}][{self.parent.cf['backend_username']}]=====================")
                cf = {
                    k: v for k, v in self.parent.cf.items()
                    if k != 'game_setting'
                }
                if item['data']['Action'] == 'chkbbin':
                    func = action.audit
                else:
                    func = action.deposit
                result = func(**dict(
                    cf,
                    item=item["data"],
                    cf=self.parent.cf,
                    extra=mod.get('extra', {}),
                    suport=getattr(action.Meta, 'suport', {}),
                    mod_key=mod['mod'],
                    mod_name=mod['name'],
                    **item["data"]
                ))
                logger.info(f'機器人[執行任務] 獲得: {result}')
                # 任務回傳為文字代表需要中斷機器人的錯誤
                if type(result) == str:
                    self.parent.run_robot.set('机器人暂停中')
                    self.parent.style.configure('CL.TButton', foreground='black')
                    self.parent.switch_input()
                    self.parent.upd_btn_msg()
                    self.pause()
                    self.parent.msg_box('错误', f'【{mod["name"]}】執行任务失败：{result}')
                    self.task = False
                    continue
                # 連線異常, 中斷後再次嘗試
                if result['ErrorCode'] == platforms.CODE_DICT['CONNECTION_CODE']['code']:
                    if item['data']['Action'] == 'chkpoint':
                        self.task = False
                    self.parent.cf['connect'] = False
                    time.sleep(int(self.parent.cf['update_times']))
                    continue
                # 被登出, 返回開頭重試
                if result['ErrorCode'] == platforms.CODE_DICT['SIGN_OUT_CODE']['code']:
                    if item['data']['Action'] == 'chkpoint':
                        self.task = False
                    platform.session.login = False
                    continue
                # 發生連線異常情況, 不回應PR6后台直接進行下一筆任務
                if result['ErrorCode'] == platforms.CODE_DICT['IGNORE_CODE']['code']:
                    time.sleep(int(self.parent.cf['update_times']))
                    self.task = False
                    continue
                # 發生充值失敗情況(WG平台回应失败：dcy328981839:已禁止领取，请联系客服), 不回應PR6后台直接進行下一筆任務(暫時)
                if result['ErrorCode'] == platforms.CODE_DICT['DEPOSIT_FAIL']['code'] and '已禁止领取' in result['ErrorMessage']:
                    time.sleep(int(self.parent.cf['update_times']))
                    self.task = False
                    continue

                # 任務回傳
                logger.info(f"===任務回傳[{mod['name']}]==========================")
                end = Apps.return_task(mod, item['data']['Action'], result, self.parent.cf)
                self.task = False
                if type(end) == str:
                    if end.endswith('重试超过上限'):
                        self.parent.upd_btn_msg()
                        time.sleep(int(self.parent.cf['update_times']))
                        continue
                    else:
                        if '机器人帐号已在其他地方登入' in end or '连线逾时' in end:
                            self.parent.logout_backend()
                        self.parent.run_robot.set('机器人暂停中')
                        self.parent.style.configure('CL.TButton', foreground='black')
                        self.parent.switch_input()
                        self.parent.upd_btn_msg()
                        self.pause()
                        self.parent.msg_box('错误', f'【{mod["name"]}】{end}')
                        continue

                # 暫停
                time.sleep(int(self.parent.cf['update_times']))
            except Exception as e:
                werdsazxc.log_trackback()
                self.task = False
                self.parent.msg_box('错误', '发生未知异常, 请联系开发团队')
                self.parent.run_robot.set('机器人暂停中')
                self.parent.style.configure('CL.TButton', foreground='black')
                self.parent.switch_input()
                self.parent.upd_btn_msg()
                self.pause()
