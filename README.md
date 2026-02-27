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
- 已并入旧版 D2I 批量下载器能力（Excel/CSV 读取、下载进度、断点续跑、单条重下、Markdown 导出）
- 已并入旧版队列引擎（`queue_manager.py`），便于后续做多队列并发下载
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

启动后可在顶部工具栏点击 `批量下载器(旧版)`，打开旧版本地下载窗口。

若启动脚本报错想看详细日志，可使用：

```powershell
.\启动 D2I Lite.bat --console
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

## 5. 公开资料抓取入库（实验）

新增了 `scraper/` 模块，支持低速、可恢复、带复核队列的抓取流程：

- 列表页 -> 详情页抓取（Scrapy + JOBDIR 断点续跑）
- 403/429 自动退避（写入 backoff 状态，下次到时再跑）
- 必填字段校验（缺失项写入 `review_queue.jsonl`）
- 图片下载与 `sha256` 去重
- 生成 `metadata_queue.jsonl`，并默认自动写回下载图片元数据（可用 `--skip-metadata` 跳过）
- 成品图片默认按姓名重命名并集中输出到单目录（`downloads/named/`）

快速开始：

```powershell
cd d:\soft\gemini-business2api-workspace\d2ilite
copy .\scraper\config.example.json .\scraper\config.local.json
.\.venv\Scripts\python .\scraper\run_public_scraper.py --config .\scraper\config.local.json
```

定时夜间慢速跑：

```powershell
.\.venv\Scripts\python .\scraper\run_scheduler.py --config .\scraper\config.local.json --time 02:30
```

天同律师团队模板（已预配）：

```powershell
.\.venv\Scripts\python .\scraper\run_public_scraper.py --config .\scraper\config.tiantonglaw.team.json
```

说明：

- 默认遵守 `robots.txt`，并发默认 `1`，间隔与重试可在配置中调整
- `gender` 默认 `unknown`，只在页面文字明确时映射，不做人脸推断
- 建议仅抓取公开且允许抓取的内容，且保留来源与许可信息
