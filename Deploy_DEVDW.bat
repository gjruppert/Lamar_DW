@echo off
REM Deploy to DEVDW: run from client Silver root (C:\Lamar.QP2.Reporting\OracleIngestion\Silver).
REM Optional: pass DB name, e.g. Deploy.bat Oracle_Reporting_P2 (sync copies this as Deploy.bat).
set SERVER=DEVDW
set "DB=%~1"
if "%DB%"=="" set "DB=DW_BronzeSilver_DEV1"

set "BATDIR=%~dp0"
set "CLIENTROOT=%BATDIR%"
if not exist "%CLIENTROOT%Deploy.sql" (
  echo Deploy.sql not found. Expected: %CLIENTROOT%Deploy.sql
  echo "Run from client Silver directory (or run Sync_To_Client.ps1 from Code first)."
  exit /b 1
)
cd /d "%CLIENTROOT%"

echo === Deploying to DEVDW - %DB% ===
sqlcmd -S %SERVER% -d %DB% -i "%CLIENTROOT%Deploy.sql"
if errorlevel 1 (
  echo Deploy to %DB% failed with error %ERRORLEVEL%
  exit /b 1
)
echo Deploy to %DB% completed successfully.
