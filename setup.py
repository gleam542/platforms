from setuptools import setup, find_packages
import werdsazxc
werdsazxc.load_dotenv()


setup(
    name="platforms",
    version=VERSION,
    description="業主平台功能",
    author="William",
    url="http://gitea.2dev.us/Python/platforms",
    # packages=['platforms', 'platforms.cd', 'platforms.bbin', 'platforms.lebo'],
    packages=find_packages(where='.'),
    py_modules=['platforms', 'gui'],
    install_requires=[
        'pytz==2021.1',
        'requests_html==0.10.0',
        'beautifulsoup4==4.9.1',
        'pyppeteer==0.2.2',
        'py7zr==0.13.0',
        'pycryptodome==3.9.8',
        'werdsazxc @ git+https://gitea.2dev.us/Python/utils',
    ]
)