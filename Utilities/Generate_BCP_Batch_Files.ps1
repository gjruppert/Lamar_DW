# Generates In.bat and Out.bat files by running Generate_BCP_Batch_Files.sql.
# Run from: C:\Users\gerard.ruppert\Documents\Lamar_DW\Utilities (or Code\Utilities)
# Requires: sqlcmd in PATH
#
# Config: Server=DEVDW, Database=DW_BronzeSilver_PROD, Schema=bzo
# Out commands -> DEVDW | In commands -> SANDBOX1

$ErrorActionPreference = 'Stop'
$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$SqlFile  = Join-Path $ScriptDir 'Generate_BCP_Batch_Files.sql'
$TempFile = Join-Path $env:TEMP 'bcp_batch_output.txt'

$Server   = 'DEVDW'
$Database = 'DW_BronzeSilver_PROD'  # Must have bzo schema for table list

if (-not (Test-Path $SqlFile)) {
    Write-Error "SQL script not found: $SqlFile"
}

Write-Host "Running SQL script against $Server\$Database..." -ForegroundColor Cyan
sqlcmd -S $Server -d $Database -i $SqlFile -W -h-1 -o $TempFile -m 1

if ($LASTEXITCODE -ne 0) {
    Write-Error "sqlcmd failed. Check your connection and database."
}

$rows = Get-Content $TempFile | Where-Object { $_.Trim() -ne '' -and $_ -match '<<>>' }
Remove-Item $TempFile -ErrorAction SilentlyContinue

# Only these 6 batch files are valid; ignore any other output
$validFiles = @(
    'DW_BronzeSilver_DEV1_Out.bat', 'DW_BronzeSilver_DEV1_In.bat',
    'Oracle_Reporting_P2_Out.bat',  'Oracle_Reporting_P2_In.bat',
    'DW_BronzeSilver_PROD_Out.bat', 'DW_BronzeSilver_PROD_In.bat'
)

$groups = @{}
foreach ($row in $rows) {
    $parts = $row -split '<<>>', 3
    if ($parts.Count -ge 3) {
        $outFile = $parts[0].Trim()
        $lineNum = $parts[1].Trim()
        $line    = $parts[2].Trim()
        if ($outFile -in $validFiles) {
            if (-not $groups[$outFile]) { $groups[$outFile] = [ordered]@{} }
            $groups[$outFile][$lineNum] = $line
        }
    }
}

foreach ($outFile in $validFiles) {
    if (-not $groups[$outFile]) { continue }
    $path = Join-Path $ScriptDir $outFile
    $lines = $groups[$outFile].GetEnumerator() | Sort-Object { [int]$_.Key } | ForEach-Object { $_.Value }
    $lines | Set-Content -Path $path -Encoding ASCII
    Write-Host "Created: $outFile" -ForegroundColor Green
}

Write-Host "Done. Batch files written to: $ScriptDir" -ForegroundColor Cyan
