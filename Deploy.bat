@echo off
REM Interactive deploy - pick server and database.
REM Run from Code directory. Uses Deploy.sql (Lamar_Procedures paths).
cd /d "%~dp0"
if not exist "Deploy.sql" (
  echo Deploy.sql not found. Run from Code directory.
  exit /b 1
)

:server
echo.
echo === Deploy - Select Server ===
echo   1^) DEVDW
echo   2^) SANDBOX1
set SERVER=
set /p SERVERCHOICE="Enter choice (1-2): "
if "%SERVERCHOICE%"=="1" set SERVER=DEVDW
if "%SERVERCHOICE%"=="2" set SERVER=SANDBOX1
if not defined SERVER goto server

:database
echo.
echo === Deploy - Select Database ===
echo   1^) DW_BronzeSilver_DEV1
echo   2^) DW_BronzeSilver_PROD
echo   3^) Oracle_Reporting_P2
echo   4^) DW_BronzeSilver_TEST
set DB=
set /p DBCHOICE="Enter choice (1-4): "
if "%DBCHOICE%"=="1" set DB=DW_BronzeSilver_DEV1
if "%DBCHOICE%"=="2" set DB=DW_BronzeSilver_PROD
if "%DBCHOICE%"=="3" set DB=Oracle_Reporting_P2
if "%DBCHOICE%"=="4" set DB=DW_BronzeSilver_TEST
if not defined DB goto database

echo.
echo === Deploying to %SERVER% - %DB% ===
sqlcmd -S %SERVER% -d %DB% -i "%~dp0Deploy.sql"
if %ERRORLEVEL% neq 0 (
  echo Deploy to %DB% failed with error %ERRORLEVEL%
  exit /b %ERRORLEVEL%
)
echo Deploy to %DB% completed successfully.
