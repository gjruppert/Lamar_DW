<#
.SYNOPSIS
  Syncs Lamar_Procedures and Lamar_Index from dev (Code) to client file source.
.DESCRIPTION
  Copies procedure folders (00_Prerequisites through 10_SM) into client StoredProcedures,
  syncs Lamar_Index to client Silver\Lamar_Index, and copies client deploy scripts.
  Does not delete client-only folders (e.g. DEV) or files (e.g. Lamar_DW_Setup_and_Run.docx).
.PARAMETER ClientRoot
  Client "Code" root (parent of StoredProcedures). Default: C:\Lamar.QP2.Reporting\OracleIngestion\Silver
.EXAMPLE
  .\Sync_To_Client.ps1
  .\Sync_To_Client.ps1 -ClientRoot "D:\ClientSilver"
#>
param(
  [string]$ClientRoot = "C:\Lamar.QP2.Reporting\OracleIngestion\Silver"
)

$ErrorActionPreference = "Stop"
$CodeDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$StoredProceduresDest = Join-Path $ClientRoot "StoredProcedures"
$LamarIndexDest = Join-Path $ClientRoot "Lamar_Index"
$ProcedureFolders = @(
  "00_Prerequisites", "01_Common", "02_AP", "03_GL", "04_OS", "05_SF",
  "06_SL", "07_RM", "08_OM", "09_AR", "10_SM"
)

if (-not (Test-Path $CodeDir)) {
  Write-Error "Code directory not found: $CodeDir"
}
if (-not (Test-Path (Join-Path $CodeDir "Lamar_Procedures"))) {
  Write-Error "Lamar_Procedures not found under $CodeDir"
}

Write-Host "=== Sync to client ===" -ForegroundColor Cyan
Write-Host "Source (Code): $CodeDir"
Write-Host "Client root:   $ClientRoot"
Write-Host ""

# Create client destinations if missing
foreach ($folder in $ProcedureFolders) {
  $dest = Join-Path $StoredProceduresDest $folder
  if (-not (Test-Path $dest)) {
    New-Item -ItemType Directory -Path $dest -Force | Out-Null
    Write-Host "Created $dest"
  }
}
if (-not (Test-Path $LamarIndexDest)) {
  New-Item -ItemType Directory -Path $LamarIndexDest -Force | Out-Null
  Write-Host "Created $LamarIndexDest"
}

# Sync each procedure folder (subdirs and files; do not purge so client DEV etc. stay)
foreach ($folder in $ProcedureFolders) {
  $src = Join-Path (Join-Path $CodeDir "Lamar_Procedures") $folder
  if (-not (Test-Path $src)) {
    Write-Warning "Skip (missing): $src"
    continue
  }
  $dest = Join-Path $StoredProceduresDest $folder
  Write-Host "Syncing $folder ..."
  robocopy $src $dest /E /NFL /NDL /NJH /NJS /NC /NS | Out-Null
  if ($LASTEXITCODE -ge 8) {
    Write-Error "robocopy failed for $folder (exit $LASTEXITCODE)"
  }
}

# Sync Lamar_Index
Write-Host "Syncing Lamar_Index ..."
robocopy (Join-Path $CodeDir "Lamar_Index") $LamarIndexDest /E /NFL /NDL /NJH /NJS /NC /NS | Out-Null
if ($LASTEXITCODE -ge 8) {
  Write-Error "robocopy failed for Lamar_Index (exit $LASTEXITCODE)"
}

# Copy DEVDW deploy scripts to client root (source: Deploy_DEVDW.* -> client: Deploy.*)
$devdwDeploySql = Join-Path $CodeDir "Deploy_DEVDW.sql"
$devdwDeployBat = Join-Path $CodeDir "Deploy_DEVDW.bat"
if (Test-Path $devdwDeploySql) {
  Copy-Item -Path $devdwDeploySql -Destination (Join-Path $ClientRoot "Deploy.sql") -Force
  Write-Host "Copied Deploy_DEVDW.sql to $ClientRoot\Deploy.sql"
}
if (Test-Path $devdwDeployBat) {
  Copy-Item -Path $devdwDeployBat -Destination (Join-Path $ClientRoot "Deploy.bat") -Force
  Write-Host "Copied Deploy_DEVDW.bat to $ClientRoot\Deploy.bat"
}

Write-Host ""
Write-Host "=== Sync complete ===" -ForegroundColor Green
Write-Host "Deploy from client: cd to $ClientRoot, then run Deploy.bat (or sqlcmd -S DEVDW -d <DB> -i Deploy.sql)"
