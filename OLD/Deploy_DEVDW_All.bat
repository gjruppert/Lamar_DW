@echo off
REM Deploy to all three databases on server DEVDW:
REM   DW_BronzeSilver_DEV1, DW_BronzeSilver_PROD, Oracle_Reporting_P2
REM Run from Code directory (or any dir; script changes to Code).
set SERVER=DEVDW
cd /d "%~dp0"
set SCRIPT=%~dp0Deploy.sql

echo === Deploying to DEVDW - DW_BronzeSilver_DEV1 ===
sqlcmd -S %SERVER% -d DW_BronzeSilver_DEV1 -i "%SCRIPT%"
if %ERRORLEVEL% neq 0 (
  echo Deploy to DW_BronzeSilver_DEV1 failed with error %ERRORLEVEL%
  exit /b %ERRORLEVEL%
)
echo --- 06_SL ---
sqlcmd -S %SERVER% -d DW_BronzeSilver_DEV1 -i "%~dp0Lamar_Procedures\06_SL\30_usp_Load_F_SL_JOURNAL_DISTRIBUTION.sql"
if %ERRORLEVEL% neq 0 (
  echo 06_SL procedure deploy failed with error %ERRORLEVEL%
  exit /b %ERRORLEVEL%
)
echo.

echo === Deploying to DEVDW - DW_BronzeSilver_PROD ===
sqlcmd -S %SERVER% -d DW_BronzeSilver_PROD -i "%SCRIPT%"
if %ERRORLEVEL% neq 0 (
  echo Deploy to DW_BronzeSilver_PROD failed with error %ERRORLEVEL%
  exit /b %ERRORLEVEL%
)
echo --- 06_SL ---
sqlcmd -S %SERVER% -d DW_BronzeSilver_PROD -i "%~dp0Lamar_Procedures\06_SL\30_usp_Load_F_SL_JOURNAL_DISTRIBUTION.sql"
if %ERRORLEVEL% neq 0 (
  echo 06_SL procedure deploy failed with error %ERRORLEVEL%
  exit /b %ERRORLEVEL%
)
echo.

echo === Deploying to DEVDW - Oracle_Reporting_P2 ===
sqlcmd -S %SERVER% -d Oracle_Reporting_P2 -i "%SCRIPT%"
if %ERRORLEVEL% neq 0 (
  echo Deploy to Oracle_Reporting_P2 failed with error %ERRORLEVEL%
  exit /b %ERRORLEVEL%
)
echo --- 06_SL ---
sqlcmd -S %SERVER% -d Oracle_Reporting_P2 -i "%~dp0Lamar_Procedures\06_SL\30_usp_Load_F_SL_JOURNAL_DISTRIBUTION.sql"
if %ERRORLEVEL% neq 0 (
  echo 06_SL procedure deploy failed with error %ERRORLEVEL%
  exit /b %ERRORLEVEL%
)
echo.
echo All three databases on DEVDW deployed successfully.
