param(
  [string]$Root = "C:\JDA\Lamar\Code\Lamar_DW\01_Common\",
  [switch]$WhatIf
)

if (-not (Test-Path $Root)) { throw "Root path does not exist: $Root" }

$patterns = @(
  # Your current "bad" pattern: schema src + object-name prefix bzo_
  @{ Name="src.bzo_";   Regex='(?i)\bsrc\s*\.\s*bzo_';   Replace='bzo.' },
  @{ Name="src.stage_"; Regex='(?i)\bsrc\s*\.\s*stage_'; Replace='stage.' },
  @{ Name="src.svo_";   Regex='(?i)\bsrc\s*\.\s*svo_';   Replace='svo.' },

  # Optional: if you ever see src.bzo.[Object] (less common)
  @{ Name="src.[bzo]_";   Regex='(?i)\bsrc\s*\.\s*\[\s*bzo\s*\]_';   Replace='bzo.' },
  @{ Name="src.[stage]_"; Regex='(?i)\bsrc\s*\.\s*\[\s*stage\s*\]_'; Replace='stage.' },
  @{ Name="src.[svo]_";   Regex='(?i)\bsrc\s*\.\s*\[\s*svo\s*\]_';   Replace='svo.' }
)

$files = Get-ChildItem -Path $Root -Recurse -File |
         Where-Object { $_.Extension -in ".sql", ".sal" }

Write-Host "Files scanned: $($files.Count)"

$totalChanged = 0

foreach ($f in $files) {
  $text = Get-Content $f.FullName -Raw
  $orig = $text

  foreach ($p in $patterns) {
    $text = [regex]::Replace($text, $p.Regex, $p.Replace)
  }

  if ($text -ne $orig) {
    $totalChanged++
    if ($WhatIf) {
      Write-Host "WOULD UPDATE: $($f.FullName)"
    } else {
      Set-Content -Path $f.FullName -Value $text -Encoding UTF8
      Write-Host "UPDATED:      $($f.FullName)"
    }
  }
}

Write-Host ""
Write-Host "Files changed: $totalChanged"
Write-Host ("Mode: " + ($(if ($WhatIf) { "WhatIf (no writes)" } else { "Write" })))
