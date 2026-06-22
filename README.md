# Airdrop Hunter — 链发现 & 空投追踪系统

## 项目结构
```
airdrop-hunter/
├── backend/
│   └── server.py          # Python 后端 API (FastAPI-free, stdlib only)
├── frontend/
│   ├── index.html         # 仪表盘 UI (桌面+手机通用)
│   ├── manifest.json      # PWA 配置
│   └── sw.js              # Service Worker (离线缓存)
├── mobile/
│   └── (APK 构建配置)
├── airdrop_hunter.spec    # PyInstaller 打包配置
└── start.bat              # Windows 一键启动
```

## 桌面 EXE 构建

```bash
# 1. 安装 PyInstaller
pip install pyinstaller

# 2. 打包成单文件 EXE
cd C:\tmp\airdrop-hunter
pyinstaller --onefile --name AirdropHunter --add-data "frontend/index.html;frontend" backend/server.py

# 输出: dist/AirdropHunter.exe
```

## 手机 APK 构建

### 方式一: PWA (推荐，最简单)
手机浏览器打开 `http://你电脑IP:8899` → 添加到主屏幕 → 就像原生 App

### 方式二: WebView APK
```
# 1. 安装 Capacitor
npm install -g @capacitor/cli @capacitor/core @capacitor/android

# 2. 初始化
npx cap init AirdropHunter com.airdrop.hunter
npx cap add android

# 3. 构建 APK
npx cap open android  # 打开 Android Studio
# Build → Build Bundle(s) / APK(s) → Build APK(s)
```

## 运行 (开发模式)
```bash
python backend/server.py
# 浏览器打开 http://127.0.0.1:8899
```

## API 端口
默认 `8899`，修改 server.py 底部 `port` 变量
