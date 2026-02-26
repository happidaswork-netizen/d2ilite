param(
    [string]$PythonExe = "pythonw.exe",
    [string]$AppPath = "",
    [switch]$TrySetDefault
)

$ErrorActionPreference = "Stop"

$scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
if ([string]::IsNullOrWhiteSpace($AppPath)) {
    $AppPath = Join-Path $scriptDir "app.py"
}

if (-not (Test-Path $AppPath)) {
    throw "app.py 不存在: $AppPath"
}

$pythonPath = (Get-Command $PythonExe -ErrorAction Stop).Source
$progId = "D2ILite.Image"
$command = "`"$pythonPath`" `"$AppPath`" `"%1`""

function Ensure-Key {
    param([string]$Path)
    if (-not (Test-Path $Path)) {
        New-Item -Path $Path -Force | Out-Null
    }
}

function Set-DefaultValue {
    param([string]$Path, [string]$Value)
    Ensure-Key -Path $Path
    Set-ItemProperty -Path $Path -Name "(default)" -Value $Value -Force
}

$base = "HKCU:\Software\Classes"

# ProgID 注册
Set-DefaultValue -Path (Join-Path $base $progId) -Value "D2I Lite Image"
Set-DefaultValue -Path (Join-Path $base "$progId\DefaultIcon") -Value "$pythonPath,0"
Set-DefaultValue -Path (Join-Path $base "$progId\shell\open\command") -Value $command

# 关联常见图片扩展名
$exts = @(".jpg", ".jpeg", ".png", ".webp", ".bmp", ".tif", ".tiff")
foreach ($ext in $exts) {
    $extKey = Join-Path $base $ext
    Ensure-Key -Path $extKey

    $openWith = Join-Path $extKey "OpenWithProgids"
    Ensure-Key -Path $openWith
    New-ItemProperty -Path $openWith -Name $progId -PropertyType String -Value "" -Force | Out-Null

    if ($TrySetDefault) {
        # 注意：Win10/11 的 UserChoice 哈希机制可能阻止脚本直接改默认打开。
        Set-ItemProperty -Path $extKey -Name "(default)" -Value $progId -Force
    }
}

Write-Host "已注册 D2I Lite 到 Open With 列表。" -ForegroundColor Green
Write-Host "命令: $command"
Write-Host ""
Write-Host "如果系统仍未变成默认查看器，请在 Windows 设置中手动选择:" -ForegroundColor Yellow
Write-Host "设置 -> 应用 -> 默认应用 -> 按文件类型选择默认应用。"
