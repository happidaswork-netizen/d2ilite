# D2I Lite Next

`desktop-next/` 现在按 D2I Lite 新版主工程维护，技术栈为 `React + TypeScript + Vite + Tauri 2`。

旧 Tk 版本继续作为规格基线保留；后续新能力默认优先落到这里，而不是继续双轨扩写旧界面。

## 当前能力

1. 加载本地图片目录
2. 读取单张图片元数据
3. 编辑并保存结构化元数据
4. 开发模式下直接预览本地图片
5. 开发模式下通过 Python CLI bridge 访问真实后端
6. `src-tauri/` 已初始化，可通过 Tauri 命令直接调用 Python bridge
7. 编辑区已支持 `Profile / TITI / XMP / EXIF / IPTC / Match` 视图切换
8. 已支持单图“图片原角色名 + 扮演角色名列表”结构化编辑
9. 已支持目录级“原角色名 / 扮演角色名”筛选、勾选集维护和批量角色编辑
10. 已补目录角色摘要索引 / 缓存，以及批量执行进度、跳过统计和失败项反馈

## 运行方式

### 1. 启动前提

在仓库根目录准备好 Python 环境：

```powershell
python -m venv .venv
.\.venv\Scripts\activate
pip install -r requirements.txt
```

### 2. 启动前端开发模式

```powershell
cd desktop-next
npm install
npm run dev
```

开发服务器会自动暴露以下开发期 bridge 路由：

- `GET /api/bridge/ping`
- `GET /api/bridge/list`
- `GET /api/bridge/read`
- `POST /api/bridge/save`
- `GET /api/bridge/preview`

这些路由会在后台调用：

```powershell
.\.venv\Scripts\python.exe scripts\desktop_bridge_cli.py
```

### 3. 启动 Tauri 开发壳

```powershell
cd desktop-next
npm install
npm run tauri:dev
```

当前 Tauri 壳会：

1. 启动现有 Vite dev server
2. 通过 `bridge_ping / bridge_list_images / bridge_read_metadata / bridge_save_metadata` 命令调用 `scripts\desktop_bridge_cli.py`
3. 通过 `convertFileSrc` 渲染本地图片预览

## 冒烟检查

```powershell
cd desktop-next
npm run smoke:provider
npm run smoke:roles
cd ..
.\.venv\Scripts\python.exe scripts\desktop_tauri_startup_smoke.py
.\.venv\Scripts\python.exe scripts\desktop_tauri_roundtrip_smoke.py
```

当前覆盖：

1. provider 选择规则
2. 批量角色编辑纯逻辑：设置 / 追加 / 替换 / 清空 / 去重 / 匹配条件 / 变更判定
3. `tauri:dev` 启动链路（Vite + cargo run + Tauri 二进制启动）
4. Tauri 壳内前端已切到 `tauri` provider，并完成启动期 `ping`
5. Tauri 壳内完整 roundtrip：`ping/list/read/save/preview`

## Provider 说明

1. `vite-python-cli`
   - 仅在 `vite dev` 下可用
   - 走真实 Python CLI bridge
   - 支持真实图片预览与真实元数据读写

2. `tauri`
   - 由 `src-tauri` 中的自定义命令承接
   - 直接调用 Python CLI bridge
   - 图片预览通过 Tauri `convertFileSrc`

3. `mock`
   - 非开发模式、又没有 Tauri runtime 时的兜底

## 现阶段边界

1. `src-tauri/` 已初始化，并已补上基础 smoke
2. 当前 Tauri 命令仍依赖仓库内的 `scripts/desktop_bridge_cli.py` 和本地 `.venv`
3. 当前已确认 Tauri 壳内前端会切到 `tauri` provider，并已补完整 `list/read/save/preview` 端到端 smoke
4. 编辑区已能查看 bridge 返回的 `titi_json / other_xmp / other_exif / other_iptc / matched_row` 原始内容
5. 角色元数据当前已支持单图结构化编辑，以及目录级筛选、勾选和批量编辑
6. 目录角色摘要索引 / 缓存与批量执行反馈已完成第一轮强化
7. 生产构建产物虽然已可被 Tauri 消费，但还没进入正式打包和发布阶段
