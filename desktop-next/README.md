# D2I Lite Next

`desktop-next/` 是 `Phase 1` 的现代前端工作台原型，当前基于 `React + TypeScript + Vite`。

## 当前能力

1. 加载本地图片目录
2. 读取单张图片元数据
3. 编辑并保存结构化元数据
4. 开发模式下直接预览本地图片
5. 开发模式下通过 Python CLI bridge 访问真实后端

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
python scripts\desktop_bridge_cli.py
```

## Provider 说明

1. `vite-python-cli`
   - 仅在 `vite dev` 下可用
   - 走真实 Python CLI bridge
   - 支持真实图片预览与真实元数据读写

2. `tauri`
   - 预留给后续 `src-tauri` 接线

3. `mock`
   - 非开发模式、又没有 Tauri runtime 时的兜底

## 现阶段边界

1. 当前还没有 `src-tauri/`
2. 生产构建产物仍是前端静态资源，不是最终桌面端
3. `Phase 1` 当前目标是先把前端工作台与 Python bridge 联通，而不是立即完成 Tauri 壳
