from dotenv import load_dotenv
from pathlib import Path
from ftplib import FTP
import requests_html
import subprocess
import traceback
import logging
import ftplib
import yaml
import log
import os
import io
logger = logging.getLogger('robot')
load_dotenv(encoding='utf-8')  # 載入 .env 檔案


class Deploy:
    download_url = 'https://testcdn.test998.com/botdownload/'
    ftp_url = '18.163.192.24'
    ftp_account = 'taiwinner'
    ftp_password = 'taiwinner999'

    PLATFORM = os.getenv('PLATFORM', '')
    APP_NAME = os.getenv('PLATFORM', f'整合機器人')
    VERSION_NUMBER = os.getenv('VERSION')
    VERSION_IMPACT_SYSTEM = input((
        '請輸入此版本影響子系統：\n'
        'eg. 【BBIN】注單、喜上喜\n'
    ))
    VERSION_DESCRIPT = input((
        '請輸入此版本影響調整內容：\n'
        'eg. 更新遇到gzip无法解析时自动重试\n'
    ))

    @classmethod
    def FILENAME(cls):
        return f'{cls.APP_NAME}V{cls.VERSION_NUMBER}'

    @classmethod
    def FTP_PATH(cls):
        return f'/pr6/public_html/botdownload/{cls.APP_NAME}'

    @classmethod
    def REMOTE_PATH(cls):
        return f'{cls.APP_NAME}'

    # 打包
    @classmethod
    def pyinstaller(cls):
        try:
            subprocess.check_output('pyinstaller -y main.spec')
            logger.info("打包成功")
            return True
        except Exception as e:
            logger.critical('\n' + traceback.format_exc())
            logger.info("打包失敗,請重新確認檔案")
            return False

    # 建立自解壓縮檔.exe
    @classmethod
    def auto_7z(cls):
        path = Path('.').absolute()
        try:
            path_7z = str(path / '7Z' / '7z.exe') # 7z壓縮檔位置
            path_file = str(path / 'dist' / f'{cls.FILENAME()}') # 要執行壓縮的檔案
            path_exe = str(path / 'dist' / f'{cls.FILENAME()}.exe') # 完成的路徑與檔名
            subprocess.check_output(f'"{path_7z}" a -sfx7z.sfx "{path_exe}" "{path_file}"')
            logger.info(f"壓縮成功，檔案路徑:{str(path / 'dist' / f'{cls.FILENAME()}.exe')}")
            return True
        except Exception as e:
            logger.critical('\n' + traceback.format_exc())
            logger.info("壓縮失敗,請手動壓縮並上傳FTP")
            return False

    # 上傳FTP
    @classmethod
    def update_ftp(cls):
        while True:
            try:
                ftp = FTP()
                ftp.connect(cls.ftp_url)
                ftp.login(cls.ftp_account, cls.ftp_password)
                ftp.encoding = 'utf-8'

                # 下載自動更新設定檔
                ftp.cwd(cls.FTP_PATH())
                with io.BytesIO() as f:
                    ftp.retrbinary('RETR config.yaml', callback=f.write)
                    f.seek(0)
                    config = yaml.load(f.read(), yaml.SafeLoader)
                    config['VERSIONS'][cls.VERSION_NUMBER] = {
                        'INFO': cls.VERSION_DESCRIPT,
                        'SYSTEM': cls.VERSION_IMPACT_SYSTEM
                    }
                    url = config['LASTEST']['DATA_URL'].split('/', 3)
                    url[-1] = f"botdownload/{cls.APP_NAME}/history/{cls.FILENAME()}.exe"
                    url = '/'.join(url)
                    config['LASTEST']['DATA_URL'] = url
                    config['LASTEST']['DIR_PATH'] = cls.FILENAME()
                    config['LASTEST']['VERSION'] = cls.VERSION_NUMBER
                # 修改後上傳自動更新設定檔
                with io.StringIO() as f:
                    yaml.dump(config, f, allow_unicode=True)
                    f.seek(0)
                    ftp.storbinary(f"STOR config.yaml", io.BytesIO(f.read().encode()), 1024)

                # 上傳檔案
                ftp.cwd('history')
                local_filename = Path(f'dist/{cls.FILENAME()}.exe')
                with local_filename.open('rb') as f:
                    ftp.storbinary(f"STOR {cls.FILENAME()}.exe", f, 1024)

                logger.info(f"上傳FTP成功，檔案路徑:{cls.download_url}{cls.REMOTE_PATH()}/history/{cls.FILENAME()}.exe")
                return True
            except ftplib.error_temp as e:
                logger.info(f'{e.__class__.__name__}: {e}')
                continue
            except Exception as e:
                logger.critical('\n' + traceback.format_exc())
                logger.info("上傳FTP失敗,請手動上傳FTP")
                return False


def main():
    # 自動打包
    result_install = Deploy.pyinstaller()
    if not result_install:
        return

    # 自動壓縮
    result_7z = Deploy.auto_7z()
    if not result_7z:
        return

    # 自動上傳FTP
    result_ftp = Deploy.update_ftp()
    if not result_ftp:
        return

    logger.info("載點更新成功!!!")


if __name__ == '__main__':
    result = main()
