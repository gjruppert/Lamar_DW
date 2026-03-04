@echo off
REM Deploy to database DW_BronzeSilver_DEV1
REM Edit -S server name if not SANDBOX1. Run from Code directory or use full path to Deploy.sql.
set SERVER=DEVDW
set DB=DW_BronzeSilver_DEV1
cd /d "%~dp0"
set SCRIPT=%~dp0Deploy.sql
sqlcmd -S %SERVER% -d %DB% -i "%SCRIPT%"
if %ERRORLEVEL% neq 0 (
  echo Deploy failed with error %ERRORLEVEL%
  exit /b %ERRORLEVEL%
)
echo --- 06_SL ---
sqlcmd -S %SERVER% -d %DB% -i "%~dp0Lamar_Procedures\06_SL\30_usp_Load_F_SL_JOURNAL_DISTRIBUTION.sql"
if %ERRORLEVEL% neq 0 (
  echo 06_SL procedure deploy failed with error %ERRORLEVEL%
  exit /b %ERRORLEVEL%
)
echo Deploy to %DB% completed successfully.
