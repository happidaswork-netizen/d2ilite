# D2I Lite

D2I Lite 是一个本地桌面看图工具，目标是：

- 本地快速看图（单张打开、同目录上一张/下一张）
- 支持把图片或文件夹直接拖到窗口中打开
- 全量查看元数据（XMP / EXIF / IPTC / PNG text）
- 新增“全部”标签页：合并展示 basic + XMP + EXIF + IPTC + PNG + 结构化字段
- 默认显示原始读取结果，不在打开时自动填空
- 支持手动触发“自动填空(手动)”按钮（仅预填空字段，不自动保存）
- 支持在“原图链接”行尾点击“下载修复”：一键走 d2ilite 内置稳定链路下载并替换当前文件（自动备份 + 元数据回写，会在修复成功后询问是否删除备份旧图）
- 支持“直连修复”作为备用：程序直接请求 URL 下载并替换当前文件
- “直连修复”成功后会询问是否删除备份原图（手动确认，防误删）
- 结构化编辑并保存元数据
- 高级 JSON 方式直接覆盖 XMP/EXIF/IPTC（谨慎使用）
- 支持命令行传入图片路径（可用于系统“默认打开方式”）

## 1. 安装依赖

```powershell
cd d:\bugemini\d2ilite
python -m venv .venv
.\.venv\Scripts\activate
pip install -r requirements.txt
```

> `pyexiv2` 是写入元数据的关键依赖。若安装失败，可先仅做读取。
> `tkinterdnd2` 用于拖拽打开；未安装时不影响基础功能，但拖拽不可用。
> `playwright` 用于“下载修复”的真实浏览器自动下载后备通道（无人工介入）。
> 可选：`undetected_chromedriver` + `selenium` 可作为更强的反爬回退链路。
> 若本机没有可用 Edge/Chromium，可执行 `playwright install chromium` 补齐浏览器内核。

## 2. 启动

双击：

- `启动 D2I Lite.bat`

或命令行：

```powershell
cd d:\bugemini\d2ilite
.\.venv\Scripts\python app.py
```

直接打开指定图片：

```powershell
.\.venv\Scripts\python app.py "D:\images\sample.jpg"
```

## 3. 注册为图片打开器（Windows）

```powershell
cd d:\bugemini\d2ilite
powershell -ExecutionPolicy Bypass -File .\register_windows_file_assoc.ps1
```

脚本会把 D2I Lite 注册到 Open With 列表。

如果要尝试写默认值（Win10/11 不一定生效）：

```powershell
powershell -ExecutionPolicy Bypass -File .\register_windows_file_assoc.ps1 -TrySetDefault
```

如果系统仍未切换默认查看器，请在 Windows 设置里手动选择 D2I Lite 对应的启动命令。

## 4. 打包 EXE（可选）

```powershell
cd d:\bugemini\d2ilite
build_exe.bat
```

输出文件：

- `dist\D2ILite\D2ILite.exe`
