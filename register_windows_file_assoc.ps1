param(
    [string]$PythonExe = "pythonw.exe",
    [string]$AppPath = "",
    [string]$ExePath = "",
    [switch]$TrySetDefault
)

$ErrorActionPreference = "Stop"

$scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path

if ([string]::IsNullOrWhiteSpace($ExePath)) {
    $candidateExe = Join-Path $scriptDir "dist\D2ILite\D2ILite.exe"
    if (Test-Path $candidateExe) {
        $ExePath = $candidateExe
    }
}

$progId = "D2ILite.Image"
if (-not [string]::IsNullOrWhiteSpace($ExePath)) {
    if (-not (Test-Path $ExePath)) {
        throw "EXE not found: $ExePath"
    }
    $appCommandPath = (Resolve-Path $ExePath).Path
    $command = "`"$appCommandPath`" `"%1`""
    $icon = "$appCommandPath,0"
} else {
    if ([string]::IsNullOrWhiteSpace($AppPath)) {
        $AppPath = Join-Path $scriptDir "app.py"
    }
    if (-not (Test-Path $AppPath)) {
        throw "app.py not found: $AppPath"
    }
    $pythonPath = (Get-Command $PythonExe -ErrorAction Stop).Source
    $appCommandPath = (Resolve-Path $AppPath).Path
    $command = "`"$pythonPath`" `"$appCommandPath`" `"%1`""
    $icon = "$pythonPath,0"
}

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
Set-DefaultValue -Path (Join-Path $base "$progId\DefaultIcon") -Value $icon
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

Write-Host "D2I Lite has been registered in the Windows Open With list." -ForegroundColor Green
Write-Host "Command: $command"
Write-Host ""
Write-Host "If it is not selected as the default viewer, choose it manually in Windows Settings:" -ForegroundColor Yellow
Write-Host "Settings -> Apps -> Default apps -> Choose defaults by file type."
