@echo off
REM Deploy to database Oracle_Reporting_P2 on server DEVDW
REM Run from Code directory or use full path to Deploy.sql.
set SERVER=DEVDW
set DB=Oracle_Reporting_P2
cd /d "%~dp0"
set SCRIPT=%~dp0Deploy.sql
sqlcmd -S %SERVER% -d %DB% -i "%SCRIPT%"
if %ERRORLEVEL% neq 0 (
  echo Deploy to %DB% failed with error %ERRORLEVEL%
  exit /b %ERRORLEVEL%
)
echo --- 06_SL ---
sqlcmd -S %SERVER% -d %DB% -i "%~dp0Lamar_Procedures\06_SL\30_usp_Load_F_SL_JOURNAL_DISTRIBUTION.sql"
if %ERRORLEVEL% neq 0 (
  echo 06_SL procedure deploy failed with error %ERRORLEVEL%
  exit /b %ERRORLEVEL%
)
echo Deploy to %DB% completed successfully.
