from logging.handlers import TimedRotatingFileHandler, RotatingFileHandler
from pathlib import Path
import traceback
import threading
import datetime
import logging
import py7zr



class ArchiveFileHandler(TimedRotatingFileHandler):
    def is_log_file(self, filename):
        try:
            datetime.datetime.strptime(filename, r'紀錄.log.%Y-%m-%d_%H-%M-%S')
            return True
        except ValueError as e:
            return False
        return False

    def should_delete(self, filename):
        try:
            now = datetime.datetime.now()
            old = datetime.datetime.strptime(filename, r'紀錄.log.%Y-%m-%d_%H-%M-%S.7z')
            if (now - old).days >= 90:
                return True
        except ValueError as e:
            return False
        return False

    def doRollover(self):
        super().doRollover()

        # 每周一進行壓縮
        if datetime.datetime.now().weekday() == 0:
            threading.Thread(target=self.doArchive).start()

    def doArchive(self):
        p = Path('config/log')

        # 壓縮並刪除文字檔
        log_files = [f for f in p.iterdir() if self.is_log_file(f.name)]
        for f in log_files:
            with py7zr.SevenZipFile(f'config/log/{f.name}.7z', 'w') as z:
                z.write(str(f), f.name)
                f.unlink()
        # 刪除90天以上壓縮檔
        zip_files = [f for f in p.iterdir() if self.should_delete(f.name)]
        for f in zip_files:
            f.unlink()


def load_logger():
    global logger
    # 檢查目錄存在
    newdir =  'config/log/紀錄.log'
    if not Path('config/log').exists():
        Path('config/log').mkdir()

    # 設定log檔格式
    fmt = logging.Formatter(
        '%(asctime)s %(levelname)7s [%(filename)10s:%(lineno)4s - %(funcName)15s()] %(message)s',
        datefmt=r'%Y-%m-%d %H:%M:%S'
    )

    # 檔案輸出
    filehdlr = ArchiveFileHandler(f'config/log/紀錄.log', when='midnight', backupCount=90, encoding='utf-8')
    filehdlr.suffix = r'%Y-%m-%d_%H-%M-%S'
    filehdlr.setFormatter(fmt)

    # 標準輸出
    cnshdlr = logging.StreamHandler()
    cnshdlr.setFormatter(fmt)

    # 獲取logger
    logger = logging.getLogger('robot')

    # 增加輸出
    logger.addHandler(filehdlr)
    logger.addHandler(cnshdlr)

    # 設定層級
    logger.setLevel(logging.DEBUG)
    cnshdlr.setLevel(logging.DEBUG)
    filehdlr.setLevel(logging.DEBUG)


if not globals().get('logger'):
    load_logger()

