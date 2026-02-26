@echo off
setlocal
cd /d "%~dp0"

if exist ".venv\Scripts\python.exe" (
  set "PY=.venv\Scripts\python.exe"
) else (
  set "PY=python"
)

%PY% -m pip install pyinstaller
if errorlevel 1 goto :fail

%PY% -m PyInstaller --noconfirm --clean --name D2ILite --windowed app.py
if errorlevel 1 goto :fail

echo.
echo Build finished: dist\D2ILite\D2ILite.exe
goto :eof

:fail
echo.
echo Build failed.
exit /b 1
