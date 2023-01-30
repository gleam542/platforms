from gui import MainPage
from tkinter import messagebox
from pathlib import Path
import tkinter as tk
import werdsazxc
import logging
import psutil
import log
werdsazxc.load_dotenv()
logger = logging.getLogger('robot')


# 設定預設圖示
class Tk(tk.Tk):
    def __init__(self, *args, **kwargs):
        tk.Tk.__init__(self, *args, **kwargs)
        tk.Tk.iconbitmap(self,default='gui/favicon.ico')


def check_opening():
    cnt = 0
    dir_path = Path('.').absolute()
    for p in psutil.process_iter():
        try:
            _dir_path = Path(p.cwd()).absolute()
            _filename = Path(p.cmdline()[-1]).name
            if (
                dir_path == _dir_path and
                'main.exe' == _filename and
                # 使用版本控制機器人會產生cmd執行main.exe,
                # 因此會產生兩條執行續, 只檢查其中一條
                len(p.cmdline()) == 1
            ):
                logger.info(str(p.cwd()))
                logger.info(str(p.cmdline()))
                cnt += 1
        except IndexError as e:
            continue
        except psutil.AccessDenied as e:
            continue

    if cnt > 1:
        root = tk.Tk()
        root.withdraw()
        messagebox.showerror(title='機器人重複開啟錯誤', message='請勿重複開啟機器人')
        logger.info(f'**機器人重複開啟, 請勿重複開啟機器人**')
    return cnt <= 1

def main():
    # 初始化機器人
    root = Tk()
    main_page = MainPage(root)
    # 執行機器人迴圈, 正式啟動機器人
    root.mainloop()

if __name__ == '__main__':
    check_opening() and main()
