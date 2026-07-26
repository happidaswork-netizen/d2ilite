param(
    [string]$Name = "",
    [string]$OutputDir = "",
    [string]$Format = "",
    [string]$OutputName = "",
    [switch]$NoMessage,
    [switch]$NoReveal,
    [Parameter(ValueFromRemainingArguments = $true)]
    [string[]]$InputPaths
)

$ErrorActionPreference = "Stop"

function Resolve-RepoRoot {
    return (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
}

function Get-FirstExistingImagePath {
    param([string[]]$Candidates)
    $imageExts = @(".jpg", ".jpeg", ".png", ".webp", ".bmp", ".tif", ".tiff")
    foreach ($candidate in $Candidates) {
        $path = [string]$candidate
        if ([string]::IsNullOrWhiteSpace($path)) { continue }
        $path = $path.Trim('"')
        if ((Test-Path -LiteralPath $path -PathType Leaf) -and ($imageExts -contains ([IO.Path]::GetExtension($path).ToLowerInvariant()))) {
            return (Resolve-Path -LiteralPath $path).Path
        }
    }
    return ""
}

function Get-ExplorerSelectedFiles {
    $paths = New-Object System.Collections.Generic.List[string]
    try {
        $shell = New-Object -ComObject Shell.Application
        foreach ($window in @($shell.Windows())) {
            try {
                $fullName = [string]$window.FullName
                if (-not $fullName.ToLowerInvariant().EndsWith("explorer.exe")) { continue }
                foreach ($item in @($window.Document.SelectedItems())) {
                    $p = [string]$item.Path
                    if (-not [string]::IsNullOrWhiteSpace($p)) {
                        [void]$paths.Add($p)
                    }
                }
            } catch {
            }
        }
    } catch {
    }
    return $paths.ToArray()
}

function Get-ClipboardFiles {
    $paths = New-Object System.Collections.Generic.List[string]
    try {
        Add-Type -AssemblyName System.Windows.Forms | Out-Null
        $dropList = [System.Windows.Forms.Clipboard]::GetFileDropList()
        foreach ($item in $dropList) {
            if (-not [string]::IsNullOrWhiteSpace([string]$item)) {
                [void]$paths.Add([string]$item)
            }
        }
    } catch {
    }
    return $paths.ToArray()
}

function Select-ImageFile {
    try {
        Add-Type -AssemblyName System.Windows.Forms | Out-Null
        $dialog = New-Object System.Windows.Forms.OpenFileDialog
        $dialog.Title = "Select image for D2I name bar"
        $dialog.Filter = "Image files|*.jpg;*.jpeg;*.png;*.webp;*.bmp;*.tif;*.tiff|All files|*.*"
        $dialog.Multiselect = $false
        if ($dialog.ShowDialog() -eq [System.Windows.Forms.DialogResult]::OK) {
            return $dialog.FileName
        }
    } catch {
    }
    return ""
}

function Read-NameInput {
    param([string]$ImagePath)
    $defaultName = [IO.Path]::GetFileNameWithoutExtension($ImagePath)
    $defaultName = $defaultName -replace '[_-]+named$', ''
    try {
        Add-Type -AssemblyName Microsoft.VisualBasic | Out-Null
        return [Microsoft.VisualBasic.Interaction]::InputBox("Name to show on the white bar:", "D2I Name Bar", $defaultName).Trim()
    } catch {
        Write-Host "Name to show on the white bar (default $defaultName):"
        $typed = [Console]::ReadLine()
        if ([string]::IsNullOrWhiteSpace($typed)) { return $defaultName }
        return $typed.Trim()
    }
}

function Resolve-PythonExe {
    param([string]$RepoRoot)
    $venvPython = Join-Path $RepoRoot ".venv\Scripts\python.exe"
    if (Test-Path -LiteralPath $venvPython -PathType Leaf) {
        return $venvPython
    }
    $python = Get-Command python -ErrorAction SilentlyContinue
    if ($python) {
        return $python.Source
    }
    $py = Get-Command py -ErrorAction SilentlyContinue
    if ($py) {
        return $py.Source
    }
    return ""
}

$repoRoot = Resolve-RepoRoot
$imagePath = Get-FirstExistingImagePath $InputPaths
if (-not $imagePath) {
    $imagePath = Get-FirstExistingImagePath (Get-ExplorerSelectedFiles)
}
if (-not $imagePath) {
    $imagePath = Get-FirstExistingImagePath (Get-ClipboardFiles)
}
if (-not $imagePath) {
    $imagePath = Select-ImageFile
}
if (-not $imagePath) {
    Add-Type -AssemblyName System.Windows.Forms | Out-Null
    if (-not $NoMessage) {
        [System.Windows.Forms.MessageBox]::Show("No image file found. Select an image in Explorer, copy an image file, or pass a file path.", "D2I Name Bar") | Out-Null
    }
    exit 2
}

$name = [string]$Name
if ([string]::IsNullOrWhiteSpace($name)) {
    $name = Read-NameInput $imagePath
}
if ([string]::IsNullOrWhiteSpace($name)) {
    exit 3
}

$pythonExe = Resolve-PythonExe $repoRoot
if (-not $pythonExe) {
    Add-Type -AssemblyName System.Windows.Forms | Out-Null
    if (-not $NoMessage) {
        [System.Windows.Forms.MessageBox]::Show("Python was not found. Make sure python or py is available.", "D2I Name Bar") | Out-Null
    }
    exit 4
}

$appPath = Join-Path $repoRoot "app.py"
$args = @($appPath, "--action", "name-bar", "--name", $name)
if (-not [string]::IsNullOrWhiteSpace($Format)) {
    $args += @("--format", $Format)
}
if (-not [string]::IsNullOrWhiteSpace($OutputName)) {
    $args += @("--output-name", $OutputName)
}
if (-not [string]::IsNullOrWhiteSpace($OutputDir)) {
    $args += @("--output-dir", $OutputDir)
}
if (-not $NoReveal) {
    $args += "--reveal"
}
$args += $imagePath
if ([IO.Path]::GetFileName($pythonExe).ToLowerInvariant() -eq "py.exe") {
    $args = @("-3") + $args
}

$output = ""
$exitCode = 0
try {
    Push-Location $repoRoot
    $output = & $pythonExe @args 2>&1 | Out-String
    $exitCode = $LASTEXITCODE
} finally {
    Pop-Location
}

Add-Type -AssemblyName System.Windows.Forms | Out-Null
if ($exitCode -eq 0) {
    if ($NoMessage) {
        Write-Host ($output.Trim())
    } else {
        [System.Windows.Forms.MessageBox]::Show(($output.Trim()), "D2I Name Bar") | Out-Null
    }
} else {
    if ($NoMessage) {
        Write-Error ("Failed:`n" + $output.Trim())
    } else {
        [System.Windows.Forms.MessageBox]::Show(("Failed:`n" + $output.Trim()), "D2I Name Bar") | Out-Null
    }
}
exit $exitCode
