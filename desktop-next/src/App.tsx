import './App.css'

function App() {
  return (
    <div className="workspace">
      <header className="topbar">
        <div className="titleblock">
          <p className="eyebrow">D2I Lite Next</p>
          <h1>本地看图与元数据工作台</h1>
        </div>
        <div className="toolbar">
          <button>打开图片</button>
          <button>打开文件夹</button>
          <button>上一张</button>
          <button>下一张</button>
          <button className="primary">保存元数据</button>
        </div>
      </header>

      <main className="main">
        <section className="preview">
          <div className="preview-head">预览区</div>
          <div className="preview-canvas">
            <div className="placeholder">图片预览</div>
          </div>
          <div className="preview-actions">
            <button>系统打开文件</button>
            <button>打开所在目录</button>
          </div>
        </section>

        <section className="editor">
          <nav className="tabbar">
            <button className="tab active">编辑</button>
            <button className="tab">结构化</button>
            <button className="tab">XMP</button>
            <button className="tab">EXIF</button>
            <button className="tab">IPTC</button>
            <button className="tab">全部</button>
          </nav>

          <div className="panel">
            <div className="field-grid">
              <label>标题</label>
              <input value="龚云龙 - 姓名：龚云龙" readOnly />
              <label>人物</label>
              <input value="龚云龙" readOnly />
              <label>来源</label>
              <input value="https://www.mps.gov.cn/..." readOnly />
              <label>原图链接</label>
              <input value="https://www.mps.gov.cn/.../pic.jpg" readOnly />
            </div>

            <div className="bio">
              <p className="bio-title">人物小传</p>
              <textarea
                readOnly
                value="龚云龙，男，汉族，1968年11月出生。长期奋战在公安一线，事迹公开可查。这里用于保留可读版人物小传，避免重复结构化字段。"
              />
            </div>
          </div>
        </section>
      </main>

      <footer className="statusbar">
        <span>状态：就绪</span>
        <span>pyexiv2：已启用</span>
        <span>任务：1 / 498</span>
      </footer>
    </div>
  )
}

export default App
