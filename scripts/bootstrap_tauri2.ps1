param(
  [switch]$ForceInit
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

function Write-Info([string]$msg) {
  Write-Host "[INFO] $msg" -ForegroundColor Cyan
}

function Write-WarnText([string]$msg) {
  Write-Host "[WARN] $msg" -ForegroundColor Yellow
}

function Write-Err([string]$msg) {
  Write-Host "[ERROR] $msg" -ForegroundColor Red
}

$repoRoot = Split-Path -Parent $PSScriptRoot
$webRoot = Join-Path $repoRoot "desktop-next"

if (-not (Test-Path $webRoot)) {
  Write-Err "desktop-next not found: $webRoot"
  exit 2
}

Write-Info "Checking Node/npm..."
try {
  node -v | Out-Null
  npm -v | Out-Null
} catch {
  Write-Err "Node/npm not found. Please install Node.js LTS first."
  exit 2
}

Write-Info "Checking Rust toolchain (cargo)..."
$hasCargo = $true
try {
  cargo -V | Out-Null
} catch {
  $hasCargo = $false
}

if (-not $hasCargo) {
  Write-WarnText "cargo not found. Tauri init requires Rust toolchain."
  Write-Host ""
  Write-Host "Install Rust (recommended):"
  Write-Host "  winget install Rustlang.Rustup"
  Write-Host ""
  Write-Host "Then reopen terminal and run:"
  Write-Host "  powershell -ExecutionPolicy Bypass -File .\scripts\bootstrap_tauri2.ps1"
  exit 3
}

Push-Location $webRoot
try {
  Write-Info "Installing web dependencies..."
  npm install

  Write-Info "Installing Tauri CLI..."
  npm install -D @tauri-apps/cli

  $tauriConf = Join-Path $webRoot "src-tauri\tauri.conf.json"
  if ((-not $ForceInit) -and (Test-Path $tauriConf)) {
    Write-Info "src-tauri already exists, skipping init."
  } else {
    Write-Info "Initializing Tauri project..."
    npx tauri init --ci --app-name "D2I Lite Next" --window-title "D2I Lite Next"
  }

  Write-Host ""
  Write-Info "Done. Next steps:"
  Write-Host "  cd desktop-next"
  Write-Host "  npm run tauri dev"
} finally {
  Pop-Location
}

