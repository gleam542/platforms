# -*- mode: python ; coding: utf-8 -*-
block_cipher = None
from pathlib import Path

p = Path('.env')
if not p.exists():
    raise FileNotFoundError('找不到檔案：.env')
for line in p.read_text(encoding='utf-8').split('\n'):
    if '=' not in line:
        continue
    line = line.split('=')
    key = line[0]
    value = '='.join(line[1:])
    globals()[key] = value
if not Path('dist').exists():
    Path('dist').mkdir()

with open('整合機器人.bat', 'r', encoding='big5') as f:
    content = f.read()
with open('dist/整合機器人.bat', 'w', encoding='big5') as f:
    f.write(content.format(version=VERSION))

with open('log.bat', 'r', encoding='big5') as f:
    content = f.read()
with open('dist/log.bat', 'w', encoding='big5') as f:
    f.write(content.format(version=VERSION))


a = Analysis(['main.py'],
             pathex=[Path(SPECPATH).absolute()],
             binaries=[],
             datas=[
                 ('gui\\favicon.ico', 'gui'),
                 ('config\\pure', 'config'),
                 ('.env', '.')
             ],
             hiddenimports=[
                 'pkg_resources.py2_warn'
             ],
             hookspath=[],
             runtime_hooks=[],
             excludes=[],
             win_no_prefer_redirects=False,
             win_private_assemblies=False,
             cipher=block_cipher,
             noarchive=False)
pyz = PYZ(a.pure, a.zipped_data,
             cipher=block_cipher)
exe = EXE(pyz,
          a.scripts,
          [],
          exclude_binaries=True,
          name='main',
          debug=False,
          bootloader_ignore_signals=False,
          strip=False,
          upx=True,
          console=False )
coll = COLLECT(exe,
               a.binaries,
               a.zipfiles,
               a.datas,
               strip=False,
               upx=True,
               upx_exclude=['vcruntime140.dll'],
               name=f'整合機器人V{VERSION}')
