import './App.css'

import { DesktopWorkspace } from './app/DesktopWorkspace'
import { MetadataInspectorWindow } from './app/MetadataInspectorWindow'

function App() {
  const view = new URLSearchParams(window.location.search).get('view')
  if (view === 'metadata-inspector') {
    return <MetadataInspectorWindow />
  }
  return <DesktopWorkspace />
}

export default App
