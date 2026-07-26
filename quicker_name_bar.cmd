@echo off
setlocal
set "ROOT=%~dp0"
where pwsh.exe >nul 2>nul
if %ERRORLEVEL%==0 (
  pwsh.exe -NoProfile -ExecutionPolicy Bypass -File "%ROOT%tools\quicker_name_bar.ps1" %*
) else (
  powershell.exe -NoProfile -ExecutionPolicy Bypass -File "%ROOT%tools\quicker_name_bar.ps1" %*
)
endlocal
