USE [Oracle_Reporting_P2];
GO

/* =====================================================================
   DIM: svo.D_PARTY_CONTACT_POINT (Hybrid SCD2)
   BK : CONTACT_POINT_ID

   v11 Fix:
   - Root cause of OWNER_TABLE_ID NULL: source column is OwnerTableId while target is OWNER_TABLE_ID.
     Previous versions pulled OwnerTableId into #src WITHOUT aliasing, so the insert intersection
     skipped OWNER_TABLE_ID, leaving it NULL.
   - This version builds a column map by matching target vs source columns after removing underscores
     and comparing case-insensitively. It then SELECTs source columns AS target column names.
   - Also keeps NOT NULL fix-up on #src for any NOT NULL target columns.
   - No CONCAT() arg limit; hashing uses nvarchar(max) + concatenation.
   - All temp tables are created/used inside the procedure batch (no scope surprises).
   ===================================================================== */

-------------------------------------------------------------------------------
-- 0) Logging (create once)
-------------------------------------------------------------------------------
IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'etl')
    EXEC('CREATE SCHEMA etl AUTHORIZATION dbo;');
GO

IF OBJECT_ID(N'etl.ETL_RUN', 'U') IS NULL
BEGIN
    CREATE TABLE etl.ETL_RUN
    (
          RUN_ID          bigint IDENTITY(1,1) NOT NULL CONSTRAINT PK_ETL_RUN PRIMARY KEY
        , PROC_NAME       sysname              NOT NULL
        , TARGET_OBJECT   sysname              NOT NULL
        , ASOF_DATE       date                 NULL
        , START_DTTM      datetime2(0)         NOT NULL
        , END_DTTM        datetime2(0)         NULL
        , STATUS          varchar(20)          NOT NULL
        , ROW_INSERTED    int                  NULL
        , ROW_EXPIRED     int                  NULL
        , ROW_UPDATED_T1  int                  NULL
        , ERROR_MESSAGE   nvarchar(4000)       NULL
    );
END
GO

IF COL_LENGTH('etl.ETL_RUN', 'ROW_UPDATED_T1') IS NULL
    ALTER TABLE etl.ETL_RUN ADD ROW_UPDATED_T1 int NULL;
GO

-------------------------------------------------------------------------------
-- 1) Ensure standard SCD columns exist (add only if missing)
-------------------------------------------------------------------------------
IF COL_LENGTH('svo.D_PARTY_CONTACT_POINT', 'EFF_DATE') IS NULL
    ALTER TABLE svo.D_PARTY_CONTACT_POINT ADD EFF_DATE date NOT NULL
        CONSTRAINT DF_D_PARTY_CONTACT_POINT_EFF_DATE DEFAULT (CAST('0001-01-01' AS date));
GO
IF COL_LENGTH('svo.D_PARTY_CONTACT_POINT', 'END_DATE') IS NULL
    ALTER TABLE svo.D_PARTY_CONTACT_POINT ADD END_DATE date NOT NULL
        CONSTRAINT DF_D_PARTY_CONTACT_POINT_END_DATE DEFAULT (CAST('9999-12-31' AS date));
GO
IF COL_LENGTH('svo.D_PARTY_CONTACT_POINT', 'CRE_DATE') IS NULL
    ALTER TABLE svo.D_PARTY_CONTACT_POINT ADD CRE_DATE datetime2(0) NOT NULL
        CONSTRAINT DF_D_PARTY_CONTACT_POINT_CRE_DATE DEFAULT (SYSDATETIME());
GO
IF COL_LENGTH('svo.D_PARTY_CONTACT_POINT', 'UDT_DATE') IS NULL
    ALTER TABLE svo.D_PARTY_CONTACT_POINT ADD UDT_DATE datetime2(0) NOT NULL
        CONSTRAINT DF_D_PARTY_CONTACT_POINT_UDT_DATE DEFAULT (SYSDATETIME());
GO
IF COL_LENGTH('svo.D_PARTY_CONTACT_POINT', 'CURR_IND') IS NULL
    ALTER TABLE svo.D_PARTY_CONTACT_POINT ADD CURR_IND char(1) NOT NULL
        CONSTRAINT DF_D_PARTY_CONTACT_POINT_CURR_IND DEFAULT ('Y');
GO

-------------------------------------------------------------------------------
-- 2) Current-row unique index for BK (drop legacy, create filtered)
-------------------------------------------------------------------------------
IF EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'UX_D_PARTY_CONTACT_POINT' AND object_id = OBJECT_ID('svo.D_PARTY_CONTACT_POINT'))
    DROP INDEX UX_D_PARTY_CONTACT_POINT ON svo.D_PARTY_CONTACT_POINT;
GO

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'UX_D_PARTY_CONTACT_POINT_ID_CURR' AND object_id = OBJECT_ID('svo.D_PARTY_CONTACT_POINT'))
BEGIN
    CREATE UNIQUE NONCLUSTERED INDEX UX_D_PARTY_CONTACT_POINT_ID_CURR
        ON svo.D_PARTY_CONTACT_POINT(CONTACT_POINT_ID)
        WHERE CURR_IND = 'Y' AND END_DATE = '9999-12-31'
        ON FG_SilverDim;
END
GO

-------------------------------------------------------------------------------
-- 3) Loader procedure
-------------------------------------------------------------------------------
CREATE OR ALTER PROCEDURE svo.usp_Load_D_PARTY_CONTACT_POINT_SCD2
(
      @AsOfDate date = NULL
)
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    IF @AsOfDate IS NULL
        SET @AsOfDate = CAST(GETDATE() AS date);

    DECLARE @HighDate date = '9999-12-31';

    DECLARE
          @RunId        bigint
        , @ProcName     sysname = OBJECT_SCHEMA_NAME(@@PROCID) + '.' + OBJECT_NAME(@@PROCID)
        , @TargetObject sysname = 'svo.D_PARTY_CONTACT_POINT';

    DECLARE
          @Inserted   int = 0
        , @Expired    int = 0
        , @UpdatedT1  int = 0;

    INSERT INTO etl.ETL_RUN (PROC_NAME, TARGET_OBJECT, ASOF_DATE, START_DTTM, STATUS)
    VALUES (@ProcName, @TargetObject, @AsOfDate, SYSDATETIME(), 'STARTED');

    SET @RunId = SCOPE_IDENTITY();

    BEGIN TRY
        BEGIN TRAN;

        --------------------------------------------------------------------
        -- Plug row (minimal, do NOT assume SK column name)
        --------------------------------------------------------------------
        IF NOT EXISTS (SELECT 1 FROM svo.D_PARTY_CONTACT_POINT WHERE CONTACT_POINT_ID = -1)
        BEGIN
            DECLARE @SkColPlug sysname =
            (
                SELECT TOP(1) name
                FROM sys.identity_columns
                WHERE object_id = OBJECT_ID('svo.D_PARTY_CONTACT_POINT')
                ORDER BY column_id
            );

            DECLARE @PlugCols nvarchar(max) = N'';
            DECLARE @PlugVals nvarchar(max) = N'';

            ;WITH c AS
            (
                SELECT
                      sc.name
                    , sc.is_nullable
                    , sc.column_id
                    , t.name AS typ
                FROM sys.columns sc
                INNER JOIN sys.types t ON sc.user_type_id = t.user_type_id
                WHERE sc.object_id = OBJECT_ID('svo.D_PARTY_CONTACT_POINT')
                  AND sc.name NOT IN (ISNULL(@SkColPlug, N'<<no_sk>>'))
            )
            SELECT
                @PlugCols = STRING_AGG(QUOTENAME(name), N', ') WITHIN GROUP (ORDER BY column_id),
                @PlugVals = STRING_AGG(
                    CASE
                        WHEN name = 'CONTACT_POINT_ID' THEN N'-1'
                        WHEN name IN ('BZ_LOAD_DATE','SV_LOAD_DATE') THEN N'CAST(GETDATE() AS date)'
                        WHEN name IN ('EFF_DATE') THEN N'CAST(GETDATE() AS date)'
                        WHEN name IN ('END_DATE') THEN N'CAST(''9999-12-31'' AS date)'
                        WHEN name IN ('CRE_DATE','UDT_DATE') THEN N'SYSDATETIME()'
                        WHEN name IN ('CURR_IND') THEN N'''Y'''
                        WHEN typ IN ('bigint','int','smallint','tinyint','decimal','numeric','float','real','money','smallmoney','bit') THEN N'-1'
                        WHEN typ IN ('date') THEN N'CAST(''0001-01-01'' AS date)'
                        WHEN typ IN ('datetime','datetime2','smalldatetime') THEN N'SYSDATETIME()'
                        ELSE N'''UNK'''
                    END,
                    N', '
                ) WITHIN GROUP (ORDER BY column_id)
            FROM c
            WHERE (is_nullable = 0) OR name IN ('CONTACT_POINT_ID','BZ_LOAD_DATE','SV_LOAD_DATE','EFF_DATE','END_DATE','CRE_DATE','UDT_DATE','CURR_IND');

            EXEC (N'INSERT INTO svo.D_PARTY_CONTACT_POINT (' + @PlugCols + N') VALUES (' + @PlugVals + N');');
        END

        --------------------------------------------------------------------
        -- Discover source + build target<->source map (underscore-insensitive)
        --------------------------------------------------------------------
        IF OBJECT_ID('tempdb..#raw') IS NOT NULL DROP TABLE #raw;
        SELECT TOP (0) * INTO #raw FROM src.bzo_AR_PartyContactPointExtractPVO;

        IF NOT EXISTS (SELECT 1 FROM tempdb.sys.columns WHERE object_id = OBJECT_ID('tempdb..#raw') AND name = 'ContactPointId')
            THROW 51000, 'Source src.bzo_AR_PartyContactPointExtractPVO does not have ContactPointId.', 1;

        DECLARE @HasAddDateTime bit =
            CASE WHEN EXISTS (SELECT 1 FROM tempdb.sys.columns WHERE object_id = OBJECT_ID('tempdb..#raw') AND name = 'AddDateTime')
                 THEN 1 ELSE 0 END;

        DECLARE @SkCol sysname =
        (
            SELECT TOP(1) name
            FROM sys.identity_columns
            WHERE object_id = OBJECT_ID('svo.D_PARTY_CONTACT_POINT')
            ORDER BY column_id
        );

        IF OBJECT_ID('tempdb..#map') IS NOT NULL DROP TABLE #map;
        CREATE TABLE #map
        (
              TargetCol sysname NOT NULL
            , SourceCol sysname NOT NULL
        );

        -- Map: for each target col, find a source col where names match after removing underscores.
        INSERT INTO #map (TargetCol, SourceCol)
        SELECT
              t.name AS TargetCol
            , s.name AS SourceCol
        FROM sys.columns t
        JOIN tempdb.sys.columns s
          ON UPPER(REPLACE(t.name, '_', '')) = UPPER(REPLACE(s.name, '_', ''))
        WHERE t.object_id = OBJECT_ID('svo.D_PARTY_CONTACT_POINT')
          AND s.object_id = OBJECT_ID('tempdb..#raw')
          AND t.name NOT IN (ISNULL(@SkCol, N'<<no_sk>>'),
                             N'EFF_DATE',N'END_DATE',N'CRE_DATE',N'UDT_DATE',N'CURR_IND',
                             N'BZ_LOAD_DATE',N'SV_LOAD_DATE')
          AND t.name <> N'CONTACT_POINT_ID'; -- handled explicitly as BK

        -- We MUST have OWNER_TABLE_ID mapped or this will fail again.
        IF NOT EXISTS (SELECT 1 FROM #map WHERE TargetCol = 'OWNER_TABLE_ID')
            THROW 51000, 'Could not map target OWNER_TABLE_ID to a source column (expected OwnerTableId).', 1;

        --------------------------------------------------------------------
        -- Build SELECT list into #src using aliases to TARGET column names.
        --------------------------------------------------------------------
        DECLARE @SelectMapped nvarchar(max) = N'';
        SELECT @SelectMapped =
            STRING_AGG(N', src.' + QUOTENAME(m.SourceCol) + N' AS ' + QUOTENAME(m.TargetCol), N'')
            WITHIN GROUP (ORDER BY m.TargetCol)
        FROM #map m;

        --------------------------------------------------------------------
        -- Build NOT NULL fix-up statement for columns that are NOT NULL in TARGET
        -- and exist in #src (i.e., are mapped).
        --------------------------------------------------------------------
        DECLARE @NNFix nvarchar(max) = N'';

        ;WITH tgt AS
        (
            SELECT c.name, c.column_id, ty.name AS typ
            FROM sys.columns c
            JOIN sys.types ty ON c.user_type_id = ty.user_type_id
            WHERE c.object_id = OBJECT_ID('svo.D_PARTY_CONTACT_POINT')
              AND c.is_nullable = 0
              AND c.name NOT IN (ISNULL(@SkCol, N'<<no_sk>>'),
                                 N'CONTACT_POINT_ID',
                                 N'EFF_DATE',N'END_DATE',N'CRE_DATE',N'UDT_DATE',N'CURR_IND',
                                 N'BZ_LOAD_DATE',N'SV_LOAD_DATE')
        )
        SELECT @NNFix = STRING_AGG(
            N's.' + QUOTENAME(t.name) + N' = COALESCE(s.' + QUOTENAME(t.name) + N', ' +
            CASE
                WHEN t.typ IN ('bigint','int','smallint','tinyint','decimal','numeric','float','real','money','smallmoney','bit') THEN N'-1'
                WHEN t.typ IN ('date') THEN N'CAST(''0001-01-01'' AS date)'
                WHEN t.typ IN ('datetime','datetime2','smalldatetime') THEN N'SYSDATETIME()'
                ELSE N'''UNK'''
            END + N')'
        , N', ')
        FROM tgt t
        WHERE EXISTS (SELECT 1 FROM #map m WHERE m.TargetCol = t.name);

        --------------------------------------------------------------------
        -- Hybrid Type1 list (optional): only update if columns exist in BOTH target and mapped
        --------------------------------------------------------------------
        DECLARE @Type1List nvarchar(max) = N'';
        SELECT @Type1List = STRING_AGG(v.Col, N'|')
        FROM (VALUES (N'PHONE_NUMBER'), (N'EMAIL_ADDRESS'), (N'EMAIL'), (N'URL'), (N'WEB_URL')) v(Col)
        WHERE COL_LENGTH('svo.D_PARTY_CONTACT_POINT', v.Col) IS NOT NULL
          AND EXISTS (SELECT 1 FROM #map WHERE TargetCol = v.Col);

        --------------------------------------------------------------------
        -- Type2 list = mapped cols minus Type1 and technical
        --------------------------------------------------------------------
        DECLARE @Type2List nvarchar(max) = N'';
        SELECT @Type2List = STRING_AGG(m.TargetCol, N'|')
        FROM #map m
        WHERE (NULLIF(@Type1List, N'') IS NULL OR CHARINDEX(N'|' + m.TargetCol + N'|', N'|' + @Type1List + N'|') = 0);

        --------------------------------------------------------------------
        -- Build hash concatenation expression using + (no CONCAT arg limit)
        --------------------------------------------------------------------
        DECLARE @HashConcatSrc nvarchar(max) = N'CAST(COALESCE(CONVERT(nvarchar(4000), s.CONTACT_POINT_ID), N'''') AS nvarchar(max))';
        DECLARE @HashConcatTgt nvarchar(max) = N'CAST(COALESCE(CONVERT(nvarchar(4000), t.CONTACT_POINT_ID), N'''') AS nvarchar(max))';

        IF NULLIF(@Type2List, N'') IS NOT NULL
        BEGIN
            DECLARE @HashTailSrc nvarchar(max) = N'';
            DECLARE @HashTailTgt nvarchar(max) = N'';

            SELECT @HashTailSrc = STRING_AGG(N' + N''|'' + CAST(COALESCE(CONVERT(nvarchar(4000), s.' + QUOTENAME(value) + N'), N'''') AS nvarchar(max))', N'')
            FROM string_split(@Type2List, N'|')
            WHERE LTRIM(RTRIM(value)) <> N'';

            SELECT @HashTailTgt = STRING_AGG(N' + N''|'' + CAST(COALESCE(CONVERT(nvarchar(4000), t.' + QUOTENAME(value) + N'), N'''') AS nvarchar(max))', N'')
            FROM string_split(@Type2List, N'|')
            WHERE LTRIM(RTRIM(value)) <> N'';

            SET @HashConcatSrc = @HashConcatSrc + ISNULL(@HashTailSrc, N'');
            SET @HashConcatTgt = @HashConcatTgt + ISNULL(@HashTailTgt, N'');
        END

        --------------------------------------------------------------------
        -- Dynamic batch for #src/#tgt/#delta and DML
        --------------------------------------------------------------------
        DECLARE @Sql nvarchar(max) = N'
            SET NOCOUNT ON;

            IF OBJECT_ID(''tempdb..#src'') IS NOT NULL DROP TABLE #src;

            SELECT
                  CAST(src.ContactPointId AS bigint) AS CONTACT_POINT_ID'
                  + ISNULL(@SelectMapped, N'') + N',
                  ' + CASE WHEN @HasAddDateTime = 1
                           THEN N'COALESCE(CAST(src.AddDateTime AS date), CAST(GETDATE() AS date))'
                           ELSE N'CAST(GETDATE() AS date)'
                      END + N' AS BZ_LOAD_DATE,
                  CAST(GETDATE() AS date) AS SV_LOAD_DATE,
                  CAST(NULL AS varbinary(32)) AS HASH_T2
            INTO #src
            FROM src.bzo_AR_PartyContactPointExtractPVO src;

            DELETE FROM #src WHERE CONTACT_POINT_ID IS NULL OR CONTACT_POINT_ID = -1;

            ' + CASE WHEN NULLIF(@NNFix, N'') IS NOT NULL
                     THEN N'UPDATE s SET ' + @NNFix + N' FROM #src s;'
                     ELSE N''
                END + N'

            UPDATE s
                SET s.HASH_T2 = HASHBYTES(''SHA2_256'', ' + @HashConcatSrc + N')
            FROM #src s;

            IF OBJECT_ID(''tempdb..#tgt'') IS NOT NULL DROP TABLE #tgt;

            SELECT
                  t.CONTACT_POINT_ID,
                  HASHBYTES(''SHA2_256'', ' + @HashConcatTgt + N') AS HASH_T2
            INTO #tgt
            FROM svo.D_PARTY_CONTACT_POINT t
            WHERE t.CURR_IND = ''Y'' AND t.END_DATE = @HighDate AND t.CONTACT_POINT_ID <> -1;

            ----------------------------------------------------------------
            -- Type1 update (optional)
            ----------------------------------------------------------------
            DECLARE @Type1 nvarchar(max) = @Type1List;

            IF NULLIF(@Type1, N'''') IS NOT NULL
            BEGIN
                DECLARE @SetT1 nvarchar(max) = N'''';
                DECLARE @PredT1 nvarchar(max) = N'''';

                ;WITH parts AS
                (
                    SELECT value AS Col
                    FROM string_split(@Type1, N''|'')
                    WHERE LTRIM(RTRIM(value)) <> N''''
                )
                SELECT
                    @SetT1  = STRING_AGG(N''tgt.'' + QUOTENAME(Col) + N'' = src.'' + QUOTENAME(Col), N'', ''),
                    @PredT1 = STRING_AGG(N''ISNULL(tgt.'' + QUOTENAME(Col) + N'','''''''') <> ISNULL(src.'' + QUOTENAME(Col) + N'','''''''')'', N'' OR '')
                FROM parts;

                IF NULLIF(@SetT1, N'''') IS NOT NULL
                BEGIN
                    DECLARE @T1Sql nvarchar(max) = N''
                        UPDATE tgt
                            SET '' + @SetT1 + N'',
                                tgt.UDT_DATE = SYSDATETIME(),
                                tgt.SV_LOAD_DATE = CAST(GETDATE() AS date),
                                tgt.BZ_LOAD_DATE = COALESCE(tgt.BZ_LOAD_DATE, src.BZ_LOAD_DATE, CAST(GETDATE() AS date))
                        FROM svo.D_PARTY_CONTACT_POINT tgt
                        INNER JOIN #tgt cur ON cur.CONTACT_POINT_ID = tgt.CONTACT_POINT_ID
                        INNER JOIN #src src ON src.CONTACT_POINT_ID = cur.CONTACT_POINT_ID
                        WHERE tgt.CURR_IND = ''''Y''''
                          AND tgt.END_DATE = @HighDate
                          AND cur.HASH_T2 = src.HASH_T2
                          AND ('' + @PredT1 + N'');'';

                    EXEC sp_executesql @T1Sql, N''@HighDate date'', @HighDate;
                    SET @UpdatedT1 = @@ROWCOUNT;
                END
            END

            ----------------------------------------------------------------
            -- Type2 delta
            ----------------------------------------------------------------
            IF OBJECT_ID(''tempdb..#delta_t2'') IS NOT NULL DROP TABLE #delta_t2;

            SELECT s.*
            INTO #delta_t2
            FROM #src s
            LEFT JOIN #tgt t ON t.CONTACT_POINT_ID = s.CONTACT_POINT_ID
            WHERE t.CONTACT_POINT_ID IS NULL
               OR t.HASH_T2 <> s.HASH_T2;

            ----------------------------------------------------------------
            -- Expire changed current rows
            ----------------------------------------------------------------
            UPDATE tgt
                SET
                      tgt.END_DATE     = DATEADD(day, -1, @AsOfDate)
                    , tgt.CURR_IND     = ''N''
                    , tgt.UDT_DATE     = SYSDATETIME()
                    , tgt.SV_LOAD_DATE = CAST(GETDATE() AS date)
            FROM svo.D_PARTY_CONTACT_POINT tgt
            INNER JOIN #delta_t2 d ON d.CONTACT_POINT_ID = tgt.CONTACT_POINT_ID
            WHERE tgt.CURR_IND = ''Y''
              AND tgt.END_DATE = @HighDate
              AND tgt.CONTACT_POINT_ID <> -1
              AND EXISTS (SELECT 1 FROM #tgt cur WHERE cur.CONTACT_POINT_ID = d.CONTACT_POINT_ID);

            SET @Expired = @@ROWCOUNT;

            ----------------------------------------------------------------
            -- Insert new current rows: insert only columns present in #delta_t2
            -- (which are TARGET-named due to mapping)
            ----------------------------------------------------------------
            DECLARE @SkCol sysname =
            (
                SELECT TOP(1) name
                FROM sys.identity_columns
                WHERE object_id = OBJECT_ID(''svo.D_PARTY_CONTACT_POINT'')
                ORDER BY column_id
            );

            DECLARE @InsCols nvarchar(max) = N'''';
            DECLARE @SelCols nvarchar(max) = N'''';

            ;WITH tgt AS
            (
                SELECT c.name, c.column_id
                FROM sys.columns c
                WHERE c.object_id = OBJECT_ID(''svo.D_PARTY_CONTACT_POINT'')
            )
            SELECT
                @InsCols = STRING_AGG(QUOTENAME(t.name), N'', '') WITHIN GROUP (ORDER BY t.column_id),
                @SelCols = STRING_AGG(N''d.'' + QUOTENAME(t.name), N'', '') WITHIN GROUP (ORDER BY t.column_id)
            FROM tgt t
            WHERE t.name NOT IN (ISNULL(@SkCol, N''<<no_sk>>''), N''EFF_DATE'',N''END_DATE'',N''CRE_DATE'',N''UDT_DATE'',N''CURR_IND'')
              AND EXISTS (SELECT 1 FROM tempdb.sys.columns WHERE object_id = OBJECT_ID(''tempdb..#delta_t2'') AND name = t.name);

            IF NULLIF(@InsCols, N'''') IS NOT NULL
            BEGIN
                DECLARE @InsertSql nvarchar(max) = N''
                    INSERT INTO svo.D_PARTY_CONTACT_POINT
                    ('' + @InsCols + N'', EFF_DATE, END_DATE, CRE_DATE, UDT_DATE, CURR_IND)
                    SELECT
                        '' + @SelCols + N'',
                        @AsOfDate, @HighDate, SYSDATETIME(), SYSDATETIME(), ''''Y''''
                    FROM #delta_t2 d;'';

                EXEC sp_executesql @InsertSql, N''@AsOfDate date, @HighDate date'', @AsOfDate, @HighDate;
                SET @Inserted = @@ROWCOUNT;
            END
            ELSE
            BEGIN
                SET @Inserted = 0;
            END
        ';

        EXEC sp_executesql
              @Sql
            , N'@AsOfDate date, @HighDate date, @Type1List nvarchar(max), @Inserted int OUTPUT, @Expired int OUTPUT, @UpdatedT1 int OUTPUT'
            , @AsOfDate = @AsOfDate
            , @HighDate = @HighDate
            , @Type1List = @Type1List
            , @Inserted = @Inserted OUTPUT
            , @Expired = @Expired OUTPUT
            , @UpdatedT1 = @UpdatedT1 OUTPUT;

        COMMIT;

        UPDATE etl.ETL_RUN
            SET
                  END_DTTM       = SYSDATETIME()
                , STATUS         = 'SUCCESS'
                , ROW_INSERTED   = @Inserted
                , ROW_EXPIRED    = @Expired
                , ROW_UPDATED_T1 = @UpdatedT1
                , ERROR_MESSAGE  = NULL
        WHERE RUN_ID = @RunId;

    END TRY
    BEGIN CATCH
        IF @@TRANCOUNT > 0 ROLLBACK;

        DECLARE @Err nvarchar(4000) = ERROR_MESSAGE();

        UPDATE etl.ETL_RUN
            SET
                  END_DTTM       = SYSDATETIME()
                , STATUS         = 'FAILED'
                , ROW_INSERTED   = @Inserted
                , ROW_EXPIRED    = @Expired
                , ROW_UPDATED_T1 = @UpdatedT1
                , ERROR_MESSAGE  = @Err
        WHERE RUN_ID = @RunId;

        THROW;
    END CATCH
END
GO

