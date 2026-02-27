@echo off
setlocal EnableExtensions DisableDelayedExpansion
cd /d "%~dp0"

set "RC=1"
set "VENV_DIR=.venv"
set "VENV_PY=%VENV_DIR%\Scripts\python.exe"
set "VENV_PYW=%VENV_DIR%\Scripts\pythonw.exe"
set "FORCE_CONSOLE=0"

if /I "%~1"=="--console" (
    set "FORCE_CONSOLE=1"
    shift
)

echo =======================================
echo D2I Lite Launcher
echo =======================================

call :find_host_python || goto :failed
call :ensure_venv || goto :failed
call :ensure_core_deps || goto :failed

if "%FORCE_CONSOLE%"=="1" goto :run_console

if exist "%VENV_PYW%" (
    "%VENV_PYW%" "app.py" %*
    set "RC=%ERRORLEVEL%"
    if "%RC%"=="0" goto :done
    echo [ERROR] GUI start failed, code %RC%.
    echo [HINT] Run with console mode for details:
    echo [HINT]    启动 D2I Lite.bat --console
    goto :failed
)

echo [WARN] pythonw not found in venv, switching to console mode...

:run_console
"%VENV_PY%" "app.py" %*
set "RC=%ERRORLEVEL%"
if "%RC%"=="0" goto :done

echo.
echo [ERROR] D2I Lite exited with code %RC%.
goto :failed

:find_host_python
set "HOST_PY="
py -3 --version >nul 2>&1 && set "HOST_PY=py -3"
if not defined HOST_PY (
    python --version >nul 2>&1 && set "HOST_PY=python"
)
if not defined HOST_PY (
    pythonw --version >nul 2>&1 && set "HOST_PY=pythonw"
)
if not defined HOST_PY (
    echo [ERROR] Python 3 not found in PATH.
    echo [HINT] Install Python from https://www.python.org/downloads/
    exit /b 1
)
echo [OK] Host Python: %HOST_PY%
exit /b 0

:ensure_venv
if exist "%VENV_PY%" (
    "%VENV_PY%" --version >nul 2>&1
    if not errorlevel 1 goto :venv_ready
    echo [WARN] Broken %VENV_DIR% detected, recreating...
    rmdir /s /q "%VENV_DIR%"
)
echo [INFO] Creating %VENV_DIR% ...
%HOST_PY% -m venv "%VENV_DIR%"
if errorlevel 1 (
    echo [ERROR] Failed to create %VENV_DIR%.
    exit /b 1
)

:venv_ready
"%VENV_PY%" -m pip --version >nul 2>&1
if errorlevel 1 (
    echo [ERROR] pip is unavailable in %VENV_DIR%.
    exit /b 1
)
echo [OK] Virtual environment ready.
exit /b 0

:ensure_core_deps
"%VENV_PY%" -c "import tkinter,ttkbootstrap,PIL,piexif,requests" >nul 2>&1
if not errorlevel 1 (
    echo [OK] Core dependencies are ready.
    exit /b 0
)

echo [INFO] Installing dependencies...
if exist "requirements.txt" (
    "%VENV_PY%" -m pip install --disable-pip-version-check -r requirements.txt
    if not errorlevel 1 goto :deps_recheck
    echo [WARN] Full requirements install failed, retrying with core packages...
)
"%VENV_PY%" -m pip install --disable-pip-version-check ttkbootstrap pillow piexif requests
if errorlevel 1 (
    echo [ERROR] Dependency installation failed.
    exit /b 1
)

:deps_recheck
"%VENV_PY%" -c "import tkinter,ttkbootstrap,PIL,piexif,requests" >nul 2>&1
if errorlevel 1 (
    echo [ERROR] Core dependency check failed after installation.
    exit /b 1
)
echo [OK] Dependencies installed.
exit /b 0

:failed
echo.
echo [FAILED] Launcher stopped.
echo.
echo Press any key to close.
pause >nul
goto :end

:done
:end
endlocal & exit /b %RC%
