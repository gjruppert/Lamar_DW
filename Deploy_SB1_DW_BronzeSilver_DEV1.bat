@echo off
REM Deploy to database DW_BronzeSilver_DEV1 on server Sandbox1 (SANDBOX1)
REM Run from Code directory (or from Lamar_Index; script finds Deploy.sql).
set SERVER=SANDBOX1
set DB=DW_BronzeSilver_DEV1
 
set "BATDIR=%~dp0"
set "CODEDIR=%BATDIR%"
if not exist "%CODEDIR%Deploy.sql" set "CODEDIR=%BATDIR%..\"
cd /d "%CODEDIR%"
set "SCRIPT=%CODEDIR%Deploy.sql"
if not exist "%SCRIPT%" (
  echo Deploy.sql not found. Expected: %SCRIPT%
  exit /b 1
)

echo === Deploying to SB1 - DW_BronzeSilver_DEV1 ===
sqlcmd -S %SERVER% -d %DB% -i "%SCRIPT%"
if %ERRORLEVEL% neq 0 (
  echo Deploy to %DB% failed with error %ERRORLEVEL%
  exit /b %ERRORLEVEL%
)
echo Deploy to %DB% completed successfully.
