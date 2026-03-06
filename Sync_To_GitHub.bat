@echo off
cd /d "%~dp0"
echo === Git Sync to GitHub ===

git status --short
if %ERRORLEVEL% neq 0 (
  echo Not a git repository or git not found.
  exit /b 1
)

set /p MSG="Commit message (or press Enter for 'Sync local changes'): "
if "%MSG%"=="" set MSG=Sync local changes

git add -A
git status --short
echo.
echo Committing and pushing...
git commit -m "%MSG%"
if %ERRORLEVEL% neq 0 (
  echo Commit failed - nothing to commit or error occurred.
  pause
  exit /b %ERRORLEVEL%
)
git push
if %ERRORLEVEL% neq 0 (
  echo Push failed - check network, credentials, or remote.
  pause
  exit /b %ERRORLEVEL%
)
echo.
echo Sync complete.
pause
