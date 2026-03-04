-- Generates In.bat and Out.bat content for each database.
-- Run via Generate_BCP_Batch_Files.ps1 to create the .bat files.
-- Output server: DEVDW | Import target: SANDBOX1 | Schema: bzo

SET NOCOUNT ON;

DECLARE @Schema     sysname       = 'bzo';
DECLARE @Prefix     nvarchar(50)  = '%';
DECLARE @SourceSrv  sysname       = 'DEVDW';
DECLARE @TargetSrv  sysname       = 'SANDBOX1';

-- Per-database config: DB name, Out path (export), In path (import)
DECLARE @DBConfig TABLE (
    DBName   sysname,
    OutPath  nvarchar(4000),
    InPath   nvarchar(4000)
);

INSERT INTO @DBConfig (DBName, OutPath, InPath) VALUES
    ('DW_BronzeSilver_DEV1',  'C:\Users\gerard.ruppert\Documents\Lamar_DW\Extracts\DEV1',   'W:\bcp\DEV1'   ),
    ('Oracle_Reporting_P2',   'C:\Users\gerard.ruppert\Documents\Lamar_DW\Extracts\P2',    'W:\bcp\P2'     ),
    ('DW_BronzeSilver_PROD',  'C:\Users\gerard.ruppert\Documents\Lamar_DW\Extracts\PROD',  'W:\bcp\PROD'   );

-- Must be connected to a DB that has the bzo schema.
DECLARE @Tables TABLE (name sysname PRIMARY KEY);
INSERT INTO @Tables (name)
SELECT t.name
FROM sys.tables t
JOIN sys.schemas s ON s.schema_id = t.schema_id
WHERE s.name = @Schema
  AND t.name LIKE @Prefix
  AND t.name NOT LIKE '%_matt_bk%';

-- Single column with delimiter so PowerShell parsing is reliable
SELECT [OutputFile] + '<<>>' + CAST(LineNum AS varchar(20)) + '<<>>' + Line AS Data
FROM (
    -- OUT.bat: header
    SELECT c.DBName + '_Out.bat' AS OutputFile, 0 AS LineNum,
           CAST('@echo off' AS nvarchar(max)) AS Line
    FROM @DBConfig c

    UNION ALL

    -- OUT.bat: export commands (source: DEVDW)
    SELECT c.DBName + '_Out.bat',
           100 + ROW_NUMBER() OVER (PARTITION BY c.DBName ORDER BY tbl.name),
           'bcp "' + @Schema + '.' + tbl.name + '" out "' + c.OutPath + '\imp_' + @Schema + '_' + tbl.name + '.bcp"'
           + ' -S "' + @SourceSrv + '" -d "' + c.DBName + '" -T -n -E -b 100000 -m 0 -h "TABLOCK"'
           + ' -e "' + c.OutPath + '\imp_' + @Schema + '_' + tbl.name + '_out.err"'
    FROM @DBConfig c
    CROSS JOIN @Tables tbl

    UNION ALL

    -- OUT.bat: footer
    SELECT c.DBName + '_Out.bat', 999,
           CAST('echo Done. & pause' AS nvarchar(max))
    FROM @DBConfig c

    UNION ALL

    -- IN.bat: header
    SELECT c.DBName + '_In.bat', 0,
           CAST('@echo off' AS nvarchar(max))
    FROM @DBConfig c

    UNION ALL

    -- IN.bat: import commands (target: SANDBOX1)
    SELECT c.DBName + '_In.bat',
           100 + ROW_NUMBER() OVER (PARTITION BY c.DBName ORDER BY tbl.name),
           'bcp "' + c.DBName + '.' + @Schema + '.' + tbl.name + '" in "' + c.InPath + '\imp_' + @Schema + '_' + tbl.name + '.bcp"'
           + ' -S "' + @TargetSrv + '" -T -n -E -b 100000 -m 0 -h "TABLOCK"'
           + ' -e "' + c.InPath + '\imp_' + @Schema + '_' + tbl.name + '_in.err"'
    FROM @DBConfig c
    CROSS JOIN @Tables tbl

    UNION ALL

    -- IN.bat: footer
    SELECT c.DBName + '_In.bat', 999,
           CAST('echo Done. & pause' AS nvarchar(max))
    FROM @DBConfig c
) x
ORDER BY OutputFile, LineNum;
