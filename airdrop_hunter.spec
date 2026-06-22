# PyInstaller 打包配置
# 使用: pip install pyinstaller; pyinstaller airdrop_hunter.spec

# -*- mode: python ; coding: utf-8 -*-

a = Analysis(
    ['backend/server.py'],
    pathex=[],
    binaries=[],
    datas=[
        ('frontend/index.html', 'frontend'),
    ],
    hiddenimports=['json','os','datetime','sqlite3','urllib.request','http.server','threading','time'],
    hookspath=[],
    hooksconfig={},
    runtime_hooks=[],
    excludes=['tkinter','matplotlib','numpy','pandas','PIL'],
)

pyz = PYZ(a.pure)

exe = EXE(
    pyz,
    a.scripts,
    a.binaries,
    a.datas,
    [],
    name='AirdropHunter',
    debug=False,
    bootloader_ignore_signals=False,
    strip=False,
    upx=True,
    upx_exclude=[],
    runtime_tmpdir=None,
    console=True,
    disable_windowed_traceback=False,
    argv_emulation=False,
    target_arch=None,
    codesign_identity=None,
    entitlements_file=None,
    icon=None,
)
