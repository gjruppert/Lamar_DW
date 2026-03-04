/* =====================================================================================
   D_COST_CENTER (Dim, Type 1, hash-based incremental; optional full reload)
   - Assumes src synonyms are already set:
       src.bzo_GL_SegmentValueHierarchyExtractPVO
       src.bzo_GL_FndTreeAndVersionVO
       src.bzo_GL_ValueSetValuesPVO
   - Assumes logging table exists: svo.DW_LOAD_LOG
   - Load date rules:
       BZ_LOAD_DATE = CAST(AddDateTime AS DATE)
       SV_LOAD_DATE = CAST(GETDATE() AS DATE)
   ===================================================================================== */

USE Oracle_Reporting_P2;
GO

/* =======
   TABLE
   ======= */
IF OBJECT_ID(N'svo.D_COST_CENTER', 'U') IS NULL
BEGIN
    CREATE TABLE svo.D_COST_CENTER
    (
        COST_CENTER_SK            BIGINT IDENTITY(1,1) NOT NULL PRIMARY KEY,
        COST_CENTER_ID            VARCHAR(100) NOT NULL,
        COST_CENTER_LVL1_CODE     VARCHAR(100) NULL,
        COST_CENTER_LVL1_DESC     VARCHAR(500) NULL,
        COST_CENTER_LVL2_CODE     VARCHAR(25)  NULL,
        COST_CENTER_LVL2_DESC     VARCHAR(500) NULL,
        COST_CENTER_LVL3_CODE     VARCHAR(25)  NULL,
        COST_CENTER_LVL3_DESC     VARCHAR(500) NULL,
        COST_CENTER_LVL4_CODE     VARCHAR(150) NULL,
        COST_CENTER_LVL4_DESC     VARCHAR(500) NULL,
        COST_CENTER_DISTANCE      SMALLINT     NULL,
        COST_CENTER_CATEGORY      VARCHAR(60)  NULL,
        COST_CENTER_ENABLED_FLAG  VARCHAR(4)   NULL,
        START_DATE_ACTIVE         DATE         NOT NULL,
        END_DATE_ACTIVE           DATE         NOT NULL,
        CREATED_BY                VARCHAR(64)  NULL,
        CREATION_DATE             DATE         NULL,
        BZ_LOAD_DATE              DATE         NULL,
        SV_LOAD_DATE              DATE         NULL
    ) ON FG_SilverDim;
END
GO

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'UX_D_COST_CENTER_ID' AND object_id = OBJECT_ID('svo.D_COST_CENTER'))
BEGIN
    CREATE UNIQUE NONCLUSTERED INDEX UX_D_COST_CENTER_ID
    ON svo.D_COST_CENTER(COST_CENTER_ID)
    ON FG_SilverDim;
END
GO

/* =========
   PROCEDURE
   ========= */
IF OBJECT_ID('svo.usp_Load_D_COST_CENTER','P') IS NOT NULL
    DROP PROCEDURE svo.usp_Load_D_COST_CENTER;
GO

CREATE PROCEDURE svo.usp_Load_D_COST_CENTER
(
      @FullReload BIT = 0
    , @Debug      BIT = 0
)
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    DECLARE
          @ProcName   SYSNAME = OBJECT_SCHEMA_NAME(@@PROCID) + '.' + OBJECT_NAME(@@PROCID)
        , @Target     SYSNAME = 'svo.D_COST_CENTER'
        , @LoadLogId  BIGINT
        , @RowsSource BIGINT = 0
        , @RowsIns    BIGINT = 0
        , @RowsUpd    BIGINT = 0
        , @RowsDel    BIGINT = 0;

    BEGIN TRY
        INSERT INTO svo.DW_LOAD_LOG (PROC_NAME, TARGET_OBJECT, FULL_RELOAD_FLAG, DEBUG_INFO)
        VALUES (@ProcName, @Target, @FullReload, CASE WHEN @Debug = 1 THEN N'Debug enabled' ELSE NULL END);

        SET @LoadLogId = SCOPE_IDENTITY();

        IF @Debug = 1
            PRINT CONCAT(@ProcName, ' starting. FullReload=', @FullReload);

        /* ---------------------------
           Source set (with hash)
           --------------------------- */
        IF OBJECT_ID('tempdb..#src') IS NOT NULL DROP TABLE #src;

        SELECT
              COST_CENTER_ID               = ISNULL(LTRIM(RTRIM(lvl4.VALUE)),'-1')
            , COST_CENTER_LVL1_CODE        = COALESCE(h1.DEP31PK1VALUE,h1.DEP30PK1VALUE,h1.DEP29PK1VALUE,lvl4.VALUE)
            , COST_CENTER_LVL1_DESC        = COALESCE(lvl1.DESCRIPTION,lvl2.DESCRIPTION,lvl3.DESCRIPTION,lvl4.DESCRIPTION)
            , COST_CENTER_LVL2_CODE        = COALESCE(h1.DEP30PK1VALUE,h1.DEP29PK1VALUE,lvl4.VALUE)
            , COST_CENTER_LVL2_DESC        = COALESCE(lvl2.DESCRIPTION,lvl3.DESCRIPTION,lvl4.DESCRIPTION)
            , COST_CENTER_LVL3_CODE        = COALESCE(h1.DEP29PK1VALUE,lvl4.VALUE)
            , COST_CENTER_LVL3_DESC        = COALESCE(lvl3.DESCRIPTION,lvl4.DESCRIPTION)
            , COST_CENTER_LVL4_CODE        = lvl4.VALUE
            , COST_CENTER_LVL4_DESC        = lvl4.DESCRIPTION
            , COST_CENTER_DISTANCE         = CAST(0 AS SMALLINT)
            , COST_CENTER_CATEGORY         = lvl4.ATTRIBUTECATEGORY
            , COST_CENTER_ENABLED_FLAG     = lvl4.ENABLEDFLAG
            , START_DATE_ACTIVE            = ISNULL(lvl4.STARTDATEACTIVE, CAST('0001-01-01' AS DATE))
            , END_DATE_ACTIVE              = ISNULL(lvl4.ENDDATEACTIVE,  CAST('9999-12-31' AS DATE))
            , CREATED_BY                   = lvl4.CREATEDBY
            , CREATION_DATE                = CAST(lvl4.CREATIONDATE AS DATE)

            /* Per your rules */
            , BZ_LOAD_DATE                 = COALESCE(CAST(h1.AddDateTime AS DATE), CAST('0001-01-01' AS DATE))
            , SV_LOAD_DATE                 = CAST(GETDATE() AS DATE)

            , ROW_HASH = HASHBYTES(
                'SHA2_256',
                CONCAT(
                      ISNULL(LTRIM(RTRIM(lvl4.VALUE)),'-1'), '|'
                    , ISNULL(COALESCE(h1.DEP31PK1VALUE,h1.DEP30PK1VALUE,h1.DEP29PK1VALUE,lvl4.VALUE),''), '|'
                    , ISNULL(COALESCE(lvl1.DESCRIPTION,lvl2.DESCRIPTION,lvl3.DESCRIPTION,lvl4.DESCRIPTION),''), '|'
                    , ISNULL(COALESCE(h1.DEP30PK1VALUE,h1.DEP29PK1VALUE,lvl4.VALUE),''), '|'
                    , ISNULL(COALESCE(lvl2.DESCRIPTION,lvl3.DESCRIPTION,lvl4.DESCRIPTION),''), '|'
                    , ISNULL(COALESCE(h1.DEP29PK1VALUE,lvl4.VALUE),''), '|'
                    , ISNULL(COALESCE(lvl3.DESCRIPTION,lvl4.DESCRIPTION),''), '|'
                    , ISNULL(lvl4.VALUE,''), '|'
                    , ISNULL(lvl4.DESCRIPTION,''), '|'
                    , ISNULL(lvl4.ATTRIBUTECATEGORY,''), '|'
                    , ISNULL(lvl4.ENABLEDFLAG,''), '|'
                    , CONVERT(VARCHAR(10), ISNULL(lvl4.STARTDATEACTIVE, CAST('0001-01-01' AS DATE)), 120), '|'
                    , CONVERT(VARCHAR(10), ISNULL(lvl4.ENDDATEACTIVE,  CAST('9999-12-31' AS DATE)), 120), '|'
                    , ISNULL(lvl4.CREATEDBY,''), '|'
                    , CONVERT(VARCHAR(10), CAST(lvl4.CREATIONDATE AS DATE), 120)
                )
            )
        INTO #src
        FROM src.bzo_GL_SegmentValueHierarchyExtractPVO h1
        INNER JOIN src.bzo_GL_FndTreeAndVersionVO ver1
            ON ver1.TREEVERSIONID = h1.TREEVERSIONID
           AND ver1.TREENAME LIKE 'CENTER LAMAR'
        INNER JOIN src.bzo_GL_ValueSetValuesPVO lvl1
            ON lvl1.VALUE = h1.DEP31PK1VALUE
           AND lvl1.ATTRIBUTECATEGORY = 'CENTER LAMAR'
        INNER JOIN src.bzo_GL_ValueSetValuesPVO lvl2
            ON lvl2.VALUE = h1.DEP30PK1VALUE
           AND lvl2.ATTRIBUTECATEGORY = 'CENTER LAMAR'
        INNER JOIN src.bzo_GL_ValueSetValuesPVO lvl3
            ON lvl3.VALUE = h1.DEP29PK1VALUE
           AND lvl3.ATTRIBUTECATEGORY = 'CENTER LAMAR'
        RIGHT JOIN src.bzo_GL_ValueSetValuesPVO lvl4
            ON lvl4.VALUE = h1.DEP0PK1VALUE
           AND lvl4.ATTRIBUTECATEGORY = 'CENTER LAMAR'
        WHERE lvl4.ATTRIBUTECATEGORY = 'CENTER LAMAR'
          AND lvl4.SUMMARYFLAG = 'N';

        /* De-dupe safety (if source ever produces duplicates for same natural key) */
        ;WITH d AS
        (
            SELECT COST_CENTER_ID, rn = ROW_NUMBER() OVER (PARTITION BY COST_CENTER_ID ORDER BY COST_CENTER_ID)
            FROM #src
        )
        DELETE s
        FROM #src s
        INNER JOIN d
            ON d.COST_CENTER_ID = s.COST_CENTER_ID
           AND d.rn > 1;

        SELECT @RowsSource = COUNT(*) FROM #src;

        IF @Debug = 1
            PRINT CONCAT('Source rows: ', @RowsSource);

        BEGIN TRAN;

        /* Ensure plug row exists before any deletes */
        IF NOT EXISTS (SELECT 1 FROM svo.D_COST_CENTER WHERE COST_CENTER_SK = 0)
        BEGIN
            SET IDENTITY_INSERT svo.D_COST_CENTER ON;

            INSERT INTO svo.D_COST_CENTER
            (
                  COST_CENTER_SK
                , COST_CENTER_ID
                , COST_CENTER_LVL1_CODE, COST_CENTER_LVL1_DESC
                , COST_CENTER_LVL2_CODE, COST_CENTER_LVL2_DESC
                , COST_CENTER_LVL3_CODE, COST_CENTER_LVL3_DESC
                , COST_CENTER_LVL4_CODE, COST_CENTER_LVL4_DESC
                , COST_CENTER_DISTANCE
                , COST_CENTER_CATEGORY
                , COST_CENTER_ENABLED_FLAG
                , START_DATE_ACTIVE
                , END_DATE_ACTIVE
                , CREATED_BY
                , CREATION_DATE
                , BZ_LOAD_DATE
                , SV_LOAD_DATE
            )
            VALUES
            (
                  0
                , '-1'
                , '-1', 'Unknown'
                , '-1', 'Unknown'
                , '-1', 'Unknown'
                , '-1', 'Unknown'
                , 0
                , 'Missing'
                , NULL                    -- fixed: was literal 'NULL'
                , CAST('0001-01-01' AS DATE)
                , CAST('9999-12-31' AS DATE)
                , 'UNK'
                , CAST('2025-10-18' AS DATE)
                , CAST('0001-01-01' AS DATE)
                , CAST(GETDATE() AS DATE)
            );

            SET IDENTITY_INSERT svo.D_COST_CENTER OFF;
        END

        /* Full reload option */
        IF @FullReload = 1
        BEGIN
            DELETE FROM svo.D_COST_CENTER
            WHERE COST_CENTER_SK <> 0;

            INSERT INTO svo.D_COST_CENTER
            (
                  COST_CENTER_ID
                , COST_CENTER_LVL1_CODE, COST_CENTER_LVL1_DESC
                , COST_CENTER_LVL2_CODE, COST_CENTER_LVL2_DESC
                , COST_CENTER_LVL3_CODE, COST_CENTER_LVL3_DESC
                , COST_CENTER_LVL4_CODE, COST_CENTER_LVL4_DESC
                , COST_CENTER_DISTANCE
                , COST_CENTER_CATEGORY
                , COST_CENTER_ENABLED_FLAG
                , START_DATE_ACTIVE
                , END_DATE_ACTIVE
                , CREATED_BY
                , CREATION_DATE
                , BZ_LOAD_DATE
                , SV_LOAD_DATE
            )
            SELECT
                  s.COST_CENTER_ID
                , s.COST_CENTER_LVL1_CODE, s.COST_CENTER_LVL1_DESC
                , s.COST_CENTER_LVL2_CODE, s.COST_CENTER_LVL2_DESC
                , s.COST_CENTER_LVL3_CODE, s.COST_CENTER_LVL3_DESC
                , s.COST_CENTER_LVL4_CODE, s.COST_CENTER_LVL4_DESC
                , s.COST_CENTER_DISTANCE
                , s.COST_CENTER_CATEGORY
                , s.COST_CENTER_ENABLED_FLAG
                , s.START_DATE_ACTIVE
                , s.END_DATE_ACTIVE
                , s.CREATED_BY
                , s.CREATION_DATE
                , s.BZ_LOAD_DATE
                , CAST(GETDATE() AS DATE)
            FROM #src s;

            SET @RowsIns = @@ROWCOUNT;
        END
        ELSE
        BEGIN
            /* Hash-based incremental update/insert */

            IF OBJECT_ID('tempdb..#tgt') IS NOT NULL DROP TABLE #tgt;

            SELECT
                  COST_CENTER_ID
                , ROW_HASH = HASHBYTES(
                    'SHA2_256',
                    CONCAT(
                          ISNULL(COST_CENTER_ID,'-1'), '|'
                        , ISNULL(COST_CENTER_LVL1_CODE,''), '|'
                        , ISNULL(COST_CENTER_LVL1_DESC,''), '|'
                        , ISNULL(COST_CENTER_LVL2_CODE,''), '|'
                        , ISNULL(COST_CENTER_LVL2_DESC,''), '|'
                        , ISNULL(COST_CENTER_LVL3_CODE,''), '|'
                        , ISNULL(COST_CENTER_LVL3_DESC,''), '|'
                        , ISNULL(COST_CENTER_LVL4_CODE,''), '|'
                        , ISNULL(COST_CENTER_LVL4_DESC,''), '|'
                        , ISNULL(COST_CENTER_CATEGORY,''), '|'
                        , ISNULL(COST_CENTER_ENABLED_FLAG,''), '|'
                        , CONVERT(VARCHAR(10), ISNULL(START_DATE_ACTIVE, CAST('0001-01-01' AS DATE)), 120), '|'
                        , CONVERT(VARCHAR(10), ISNULL(END_DATE_ACTIVE,   CAST('9999-12-31' AS DATE)), 120), '|'
                        , ISNULL(CREATED_BY,''), '|'
                        , CONVERT(VARCHAR(10), ISNULL(CREATION_DATE, CAST('0001-01-01' AS DATE)), 120)
                    )
                  )
            INTO #tgt
            FROM svo.D_COST_CENTER
            WHERE COST_CENTER_SK <> 0;

            UPDATE t
                SET
                      t.COST_CENTER_LVL1_CODE        = s.COST_CENTER_LVL1_CODE
                    , t.COST_CENTER_LVL1_DESC        = s.COST_CENTER_LVL1_DESC
                    , t.COST_CENTER_LVL2_CODE        = s.COST_CENTER_LVL2_CODE
                    , t.COST_CENTER_LVL2_DESC        = s.COST_CENTER_LVL2_DESC
                    , t.COST_CENTER_LVL3_CODE        = s.COST_CENTER_LVL3_CODE
                    , t.COST_CENTER_LVL3_DESC        = s.COST_CENTER_LVL3_DESC
                    , t.COST_CENTER_LVL4_CODE        = s.COST_CENTER_LVL4_CODE
                    , t.COST_CENTER_LVL4_DESC        = s.COST_CENTER_LVL4_DESC
                    , t.COST_CENTER_DISTANCE         = s.COST_CENTER_DISTANCE
                    , t.COST_CENTER_CATEGORY         = s.COST_CENTER_CATEGORY
                    , t.COST_CENTER_ENABLED_FLAG     = s.COST_CENTER_ENABLED_FLAG
                    , t.START_DATE_ACTIVE            = s.START_DATE_ACTIVE
                    , t.END_DATE_ACTIVE              = s.END_DATE_ACTIVE
                    , t.CREATED_BY                   = s.CREATED_BY
                    , t.CREATION_DATE                = s.CREATION_DATE
                    , t.BZ_LOAD_DATE                 = s.BZ_LOAD_DATE
                    , t.SV_LOAD_DATE                 = CAST(GETDATE() AS DATE)
            FROM svo.D_COST_CENTER t
            INNER JOIN #src s
                ON s.COST_CENTER_ID = t.COST_CENTER_ID
            INNER JOIN #tgt h
                ON h.COST_CENTER_ID = t.COST_CENTER_ID
            WHERE h.ROW_HASH <> s.ROW_HASH;

            SET @RowsUpd = @@ROWCOUNT;

            INSERT INTO svo.D_COST_CENTER
            (
                  COST_CENTER_ID
                , COST_CENTER_LVL1_CODE, COST_CENTER_LVL1_DESC
                , COST_CENTER_LVL2_CODE, COST_CENTER_LVL2_DESC
                , COST_CENTER_LVL3_CODE, COST_CENTER_LVL3_DESC
                , COST_CENTER_LVL4_CODE, COST_CENTER_LVL4_DESC
                , COST_CENTER_DISTANCE
                , COST_CENTER_CATEGORY
                , COST_CENTER_ENABLED_FLAG
                , START_DATE_ACTIVE
                , END_DATE_ACTIVE
                , CREATED_BY
                , CREATION_DATE
                , BZ_LOAD_DATE
                , SV_LOAD_DATE
            )
            SELECT
                  s.COST_CENTER_ID
                , s.COST_CENTER_LVL1_CODE, s.COST_CENTER_LVL1_DESC
                , s.COST_CENTER_LVL2_CODE, s.COST_CENTER_LVL2_DESC
                , s.COST_CENTER_LVL3_CODE, s.COST_CENTER_LVL3_DESC
                , s.COST_CENTER_LVL4_CODE, s.COST_CENTER_LVL4_DESC
                , s.COST_CENTER_DISTANCE
                , s.COST_CENTER_CATEGORY
                , s.COST_CENTER_ENABLED_FLAG
                , s.START_DATE_ACTIVE
                , s.END_DATE_ACTIVE
                , s.CREATED_BY
                , s.CREATION_DATE
                , s.BZ_LOAD_DATE
                , CAST(GETDATE() AS DATE)
            FROM #src s
            LEFT JOIN svo.D_COST_CENTER t
                ON t.COST_CENTER_ID = s.COST_CENTER_ID
            WHERE t.COST_CENTER_ID IS NULL;

            SET @RowsIns = @@ROWCOUNT;
        END

        COMMIT;

        UPDATE svo.DW_LOAD_LOG
            SET
                  LOAD_END_DT   = SYSUTCDATETIME()
                , STATUS        = 'SUCCESS'
                , ROWS_SOURCE   = @RowsSource
                , ROWS_INSERTED = @RowsIns
                , ROWS_UPDATED  = @RowsUpd
                , ROWS_DELETED  = @RowsDel
        WHERE LOAD_LOG_ID = @LoadLogId;

        IF @Debug = 1
            PRINT CONCAT('Done. Inserted=', @RowsIns, ' Updated=', @RowsUpd);

    END TRY
    BEGIN CATCH
        IF XACT_STATE() <> 0 ROLLBACK;

        UPDATE svo.DW_LOAD_LOG
            SET
                  LOAD_END_DT     = SYSUTCDATETIME()
                , STATUS          = 'FAILED'
                , ERROR_NUMBER    = ERROR_NUMBER()
                , ERROR_SEVERITY  = ERROR_SEVERITY()
                , ERROR_STATE     = ERROR_STATE()
                , ERROR_LINE      = ERROR_LINE()
                , ERROR_MESSAGE   = LEFT(ERROR_MESSAGE(), 4000)
        WHERE LOAD_LOG_ID = @LoadLogId;

        THROW;
    END CATCH
END
GO


