/* =====================================================================================
   D_BUSINESS_OFFERING (Dim, Type 1, hash-based incremental; optional full reload)
   - Uses src synonyms:
       src.bzo_GL_SegmentValueHierarchyExtractPVO
       src.bzo_GL_FndTreeAndVersionVO
       src.bzo_GL_ValueSetValuesPVO
   - Logging: svo.DW_LOAD_LOG
   - Error rethrow: RAISERROR (no THROW)
   - Load date rules:
       BZ_LOAD_DATE = CAST(AddDateTime AS DATE)
       SV_LOAD_DATE = CAST(GETDATE() AS DATE)
   ===================================================================================== */

USE Oracle_Reporting_P2;
GO

/* =======
   TABLE
   ======= */
IF OBJECT_ID(N'svo.D_BUSINESS_OFFERING', 'U') IS NULL
BEGIN
    CREATE TABLE svo.D_BUSINESS_OFFERING
    (
        BUSINESS_OFFERING_SK            BIGINT IDENTITY(1,1) NOT NULL PRIMARY KEY,
        BUSINESS_OFFERING_ID            VARCHAR(100) NOT NULL,
        BUSINESS_OFFERING_LVL1_CODE     VARCHAR(100) NULL,
        BUSINESS_OFFERING_LVL1_DESC     VARCHAR(500) NULL,
        BUSINESS_OFFERING_LVL2_CODE     VARCHAR(25)  NULL,
        BUSINESS_OFFERING_LVL2_DESC     VARCHAR(500) NULL,
        BUSINESS_OFFERING_LVL3_CODE     VARCHAR(25)  NULL,
        BUSINESS_OFFERING_LVL3_DESC     VARCHAR(500) NULL,
        BUSINESS_OFFERING_LVL4_CODE     VARCHAR(150) NULL,
        BUSINESS_OFFERING_LVL4_DESC     VARCHAR(500) NULL,
        BUSINESS_OFFERING_LVL5_CODE     VARCHAR(100) NULL,
        BUSINESS_OFFERING_LVL5_DESC     VARCHAR(500) NULL,
        BUSINESS_OFFERING_DISTANCE      SMALLINT     NULL,
        BUSINESS_OFFERING_CATEGORY      VARCHAR(60)  NULL,
        BUSINESS_OFFERING_ENABLED_FLAG  VARCHAR(4)   NULL,
        START_DATE_ACTIVE               DATE         NOT NULL,
        END_DATE_ACTIVE                 DATE         NOT NULL,
        CREATED_BY                      VARCHAR(64)  NULL,
        CREATION_DATE                   DATE         NULL,
        BZ_LOAD_DATE                    DATE         NULL,
        SV_LOAD_DATE                    DATE         NULL
    ) ON FG_SilverDim;
END
GO

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'UX_D_BUSINESS_OFFERING_ID' AND object_id = OBJECT_ID('svo.D_BUSINESS_OFFERING'))
BEGIN
    CREATE UNIQUE NONCLUSTERED INDEX UX_D_BUSINESS_OFFERING_ID
    ON svo.D_BUSINESS_OFFERING (BUSINESS_OFFERING_ID)
    ON FG_SilverDim;
END
GO

/* =========
   PROCEDURE
   ========= */
IF OBJECT_ID('svo.usp_Load_D_BUSINESS_OFFERING','P') IS NOT NULL
    DROP PROCEDURE svo.usp_Load_D_BUSINESS_OFFERING;
GO

CREATE PROCEDURE svo.usp_Load_D_BUSINESS_OFFERING
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
        , @Target     SYSNAME = 'svo.D_BUSINESS_OFFERING'
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
              BUSINESS_OFFERING_ID            = ISNULL(TRIM(lvl5.VALUE),'-1')
            , BUSINESS_OFFERING_LVL1_CODE     = COALESCE(h1.DEP31PK1VALUE,h1.DEP30PK1VALUE,h1.DEP29PK1VALUE,h1.DEP28PK1VALUE,lvl5.VALUE)
            , BUSINESS_OFFERING_LVL1_DESC     = COALESCE(lvl1.DESCRIPTION,lvl2.DESCRIPTION,lvl3.DESCRIPTION,lvl4.DESCRIPTION,lvl5.DESCRIPTION)
            , BUSINESS_OFFERING_LVL2_CODE     = COALESCE(h1.DEP30PK1VALUE,h1.DEP29PK1VALUE,h1.DEP28PK1VALUE,lvl5.VALUE)
            , BUSINESS_OFFERING_LVL2_DESC     = COALESCE(lvl2.DESCRIPTION,lvl3.DESCRIPTION,lvl4.DESCRIPTION,lvl5.DESCRIPTION)
            , BUSINESS_OFFERING_LVL3_CODE     = COALESCE(h1.DEP29PK1VALUE,h1.DEP28PK1VALUE,lvl5.VALUE)
            , BUSINESS_OFFERING_LVL3_DESC     = COALESCE(lvl3.DESCRIPTION,lvl4.DESCRIPTION,lvl5.DESCRIPTION)
            , BUSINESS_OFFERING_LVL4_CODE     = COALESCE(h1.DEP28PK1VALUE,lvl5.VALUE)
            , BUSINESS_OFFERING_LVL4_DESC     = COALESCE(lvl4.DESCRIPTION,lvl5.DESCRIPTION)
            , BUSINESS_OFFERING_LVL5_CODE     = lvl5.VALUE
            , BUSINESS_OFFERING_LVL5_DESC     = lvl5.DESCRIPTION
            , BUSINESS_OFFERING_DISTANCE      = CAST(0 AS SMALLINT)
            , BUSINESS_OFFERING_CATEGORY      = lvl5.ATTRIBUTECATEGORY
            , BUSINESS_OFFERING_ENABLED_FLAG  = lvl5.ENABLEDFLAG
            , START_DATE_ACTIVE               = ISNULL(lvl5.STARTDATEACTIVE, '0001-01-01')
            , END_DATE_ACTIVE                 = ISNULL(lvl5.ENDDATEACTIVE,  '9999-12-31')
            , CREATED_BY                      = lvl5.CREATEDBY
            , CREATION_DATE                   = CAST(lvl5.CREATIONDATE AS DATE)
            , BZ_LOAD_DATE                    = COALESCE(CAST(h1.AddDateTime AS DATE), CAST('0001-01-01' AS DATE))
            , SV_LOAD_DATE                    = CAST(GETDATE() AS DATE)

            , ROW_HASH = HASHBYTES(
                'SHA2_256',
                CONCAT(
                      ISNULL(TRIM(lvl5.VALUE),'-1'), '|'
                    , ISNULL(COALESCE(h1.DEP31PK1VALUE,h1.DEP30PK1VALUE,h1.DEP29PK1VALUE,h1.DEP28PK1VALUE,lvl5.VALUE),''), '|'
                    , ISNULL(COALESCE(lvl1.DESCRIPTION,lvl2.DESCRIPTION,lvl3.DESCRIPTION,lvl4.DESCRIPTION,lvl5.DESCRIPTION),''), '|'
                    , ISNULL(COALESCE(h1.DEP30PK1VALUE,h1.DEP29PK1VALUE,h1.DEP28PK1VALUE,lvl5.VALUE),''), '|'
                    , ISNULL(COALESCE(lvl2.DESCRIPTION,lvl3.DESCRIPTION,lvl4.DESCRIPTION,lvl5.DESCRIPTION),''), '|'
                    , ISNULL(COALESCE(h1.DEP29PK1VALUE,h1.DEP28PK1VALUE,lvl5.VALUE),''), '|'
                    , ISNULL(COALESCE(lvl3.DESCRIPTION,lvl4.DESCRIPTION,lvl5.DESCRIPTION),''), '|'
                    , ISNULL(COALESCE(h1.DEP28PK1VALUE,lvl5.VALUE),''), '|'
                    , ISNULL(COALESCE(lvl4.DESCRIPTION,lvl5.DESCRIPTION),''), '|'
                    , ISNULL(lvl5.VALUE,''), '|'
                    , ISNULL(lvl5.DESCRIPTION,''), '|'
                    , ISNULL(lvl5.ATTRIBUTECATEGORY,''), '|'
                    , ISNULL(lvl5.ENABLEDFLAG,''), '|'
                    , ISNULL(CONVERT(VARCHAR(10), ISNULL(lvl5.STARTDATEACTIVE,'0001-01-01'), 120),''), '|'
                    , ISNULL(CONVERT(VARCHAR(10), ISNULL(lvl5.ENDDATEACTIVE,'9999-12-31'), 120),''), '|'
                    , ISNULL(lvl5.CREATEDBY,''), '|'
                    , ISNULL(CONVERT(VARCHAR(10), CAST(lvl5.CREATIONDATE AS DATE), 120),'')
                )
            )
        INTO #src
        FROM src.bzo_GL_SegmentValueHierarchyExtractPVO h1
        INNER JOIN src.bzo_GL_FndTreeAndVersionVO ver1
            ON ver1.TREEVERSIONID = h1.TREEVERSIONID
           AND ver1.TREENAME LIKE 'BUSINESS OFFERING LAMAR'
        INNER JOIN src.bzo_GL_ValueSetValuesPVO lvl1
            ON lvl1.VALUE = h1.DEP31PK1VALUE
           AND lvl1.ATTRIBUTECATEGORY = 'BUSINESS OFFERING LAMAR'
        INNER JOIN src.bzo_GL_ValueSetValuesPVO lvl2
            ON lvl2.VALUE = h1.DEP30PK1VALUE
           AND lvl2.ATTRIBUTECATEGORY = 'BUSINESS OFFERING LAMAR'
        INNER JOIN src.bzo_GL_ValueSetValuesPVO lvl3
            ON lvl3.VALUE = h1.DEP29PK1VALUE
           AND lvl3.ATTRIBUTECATEGORY = 'BUSINESS OFFERING LAMAR'
        INNER JOIN src.bzo_GL_ValueSetValuesPVO lvl4
            ON lvl4.VALUE = h1.DEP28PK1VALUE
           AND lvl4.ATTRIBUTECATEGORY = 'BUSINESS OFFERING LAMAR'
        RIGHT JOIN src.bzo_GL_ValueSetValuesPVO lvl5
            ON lvl5.VALUE = h1.DEP0PK1VALUE
           AND lvl5.ATTRIBUTECATEGORY = 'BUSINESS OFFERING LAMAR'
        WHERE lvl5.ATTRIBUTECATEGORY = 'BUSINESS OFFERING LAMAR'
          AND lvl5.SUMMARYFLAG = 'N';

        /* Do NOT keep dev/test deletes. We reserve BUSINESS_OFFERING_SK = 0 as the plug row. */

        /* De-dupe safety on BK */
        ;WITH d AS
        (
            SELECT BUSINESS_OFFERING_ID, rn = ROW_NUMBER() OVER (PARTITION BY BUSINESS_OFFERING_ID ORDER BY BUSINESS_OFFERING_ID)
            FROM #src
        )
        DELETE s
        FROM #src s
        INNER JOIN d
            ON d.BUSINESS_OFFERING_ID = s.BUSINESS_OFFERING_ID
           AND d.rn > 1;

        SELECT @RowsSource = COUNT(*) FROM #src;

        IF @Debug = 1
            PRINT CONCAT('Source rows: ', @RowsSource);

        BEGIN TRAN;

        /* Ensure SK=0 plug row exists (BK='-1') */
        IF NOT EXISTS (SELECT 1 FROM svo.D_BUSINESS_OFFERING WHERE BUSINESS_OFFERING_SK = 0)
        BEGIN
            SET IDENTITY_INSERT svo.D_BUSINESS_OFFERING ON;

            INSERT INTO svo.D_BUSINESS_OFFERING
            (
                  BUSINESS_OFFERING_SK
                , BUSINESS_OFFERING_ID
                , BUSINESS_OFFERING_LVL1_CODE
                , BUSINESS_OFFERING_LVL1_DESC
                , BUSINESS_OFFERING_LVL2_CODE
                , BUSINESS_OFFERING_LVL2_DESC
                , BUSINESS_OFFERING_LVL3_CODE
                , BUSINESS_OFFERING_LVL3_DESC
                , BUSINESS_OFFERING_LVL4_CODE
                , BUSINESS_OFFERING_LVL4_DESC
                , BUSINESS_OFFERING_LVL5_CODE
                , BUSINESS_OFFERING_LVL5_DESC
                , BUSINESS_OFFERING_DISTANCE
                , BUSINESS_OFFERING_CATEGORY
                , BUSINESS_OFFERING_ENABLED_FLAG
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
                , '-1'
                , 'Unknown'
                , '-1'
                , 'Unknown'
                , '-1'
                , 'Unknown'
                , '-1'
                , 'Unknown'
                , '-1'
                , 'Unknown'
                , 0
                , 'Missing'
                , NULL
                , CAST('0001-01-01' AS DATE)
                , CAST('9999-12-31' AS DATE)
                , 'UNK'
                , CAST('2025-10-18' AS DATE)
                , CAST('0001-01-01' AS DATE)
                , CAST(GETDATE() AS DATE)
            );

            SET IDENTITY_INSERT svo.D_BUSINESS_OFFERING OFF;
        END

        IF @FullReload = 1
        BEGIN
            DELETE FROM svo.D_BUSINESS_OFFERING
            WHERE BUSINESS_OFFERING_SK <> 0;

            INSERT INTO svo.D_BUSINESS_OFFERING
            (
                  BUSINESS_OFFERING_ID
                , BUSINESS_OFFERING_LVL1_CODE
                , BUSINESS_OFFERING_LVL1_DESC
                , BUSINESS_OFFERING_LVL2_CODE
                , BUSINESS_OFFERING_LVL2_DESC
                , BUSINESS_OFFERING_LVL3_CODE
                , BUSINESS_OFFERING_LVL3_DESC
                , BUSINESS_OFFERING_LVL4_CODE
                , BUSINESS_OFFERING_LVL4_DESC
                , BUSINESS_OFFERING_LVL5_CODE
                , BUSINESS_OFFERING_LVL5_DESC
                , BUSINESS_OFFERING_DISTANCE
                , BUSINESS_OFFERING_CATEGORY
                , BUSINESS_OFFERING_ENABLED_FLAG
                , START_DATE_ACTIVE
                , END_DATE_ACTIVE
                , CREATED_BY
                , CREATION_DATE
                , BZ_LOAD_DATE
                , SV_LOAD_DATE
            )
            SELECT
                  s.BUSINESS_OFFERING_ID
                , s.BUSINESS_OFFERING_LVL1_CODE
                , s.BUSINESS_OFFERING_LVL1_DESC
                , s.BUSINESS_OFFERING_LVL2_CODE
                , s.BUSINESS_OFFERING_LVL2_DESC
                , s.BUSINESS_OFFERING_LVL3_CODE
                , s.BUSINESS_OFFERING_LVL3_DESC
                , s.BUSINESS_OFFERING_LVL4_CODE
                , s.BUSINESS_OFFERING_LVL4_DESC
                , s.BUSINESS_OFFERING_LVL5_CODE
                , s.BUSINESS_OFFERING_LVL5_DESC
                , s.BUSINESS_OFFERING_DISTANCE
                , s.BUSINESS_OFFERING_CATEGORY
                , s.BUSINESS_OFFERING_ENABLED_FLAG
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
                  BUSINESS_OFFERING_ID
                , ROW_HASH = HASHBYTES(
                    'SHA2_256',
                    CONCAT(
                          ISNULL(BUSINESS_OFFERING_ID,''), '|'
                        , ISNULL(BUSINESS_OFFERING_LVL1_CODE,''), '|'
                        , ISNULL(BUSINESS_OFFERING_LVL1_DESC,''), '|'
                        , ISNULL(BUSINESS_OFFERING_LVL2_CODE,''), '|'
                        , ISNULL(BUSINESS_OFFERING_LVL2_DESC,''), '|'
                        , ISNULL(BUSINESS_OFFERING_LVL3_CODE,''), '|'
                        , ISNULL(BUSINESS_OFFERING_LVL3_DESC,''), '|'
                        , ISNULL(BUSINESS_OFFERING_LVL4_CODE,''), '|'
                        , ISNULL(BUSINESS_OFFERING_LVL4_DESC,''), '|'
                        , ISNULL(BUSINESS_OFFERING_LVL5_CODE,''), '|'
                        , ISNULL(BUSINESS_OFFERING_LVL5_DESC,''), '|'
                        , ISNULL(CONVERT(VARCHAR(10), START_DATE_ACTIVE, 120),''), '|'
                        , ISNULL(CONVERT(VARCHAR(10), END_DATE_ACTIVE, 120),''), '|'
                        , ISNULL(BUSINESS_OFFERING_CATEGORY,''), '|'
                        , ISNULL(BUSINESS_OFFERING_ENABLED_FLAG,''), '|'
                        , ISNULL(CREATED_BY,''), '|'
                        , ISNULL(CONVERT(VARCHAR(10), CREATION_DATE, 120),'')
                    )
                  )
            INTO #tgt
            FROM svo.D_BUSINESS_OFFERING
            WHERE BUSINESS_OFFERING_SK <> 0;

            UPDATE t
                SET
                      t.BUSINESS_OFFERING_LVL1_CODE     = s.BUSINESS_OFFERING_LVL1_CODE
                    , t.BUSINESS_OFFERING_LVL1_DESC     = s.BUSINESS_OFFERING_LVL1_DESC
                    , t.BUSINESS_OFFERING_LVL2_CODE     = s.BUSINESS_OFFERING_LVL2_CODE
                    , t.BUSINESS_OFFERING_LVL2_DESC     = s.BUSINESS_OFFERING_LVL2_DESC
                    , t.BUSINESS_OFFERING_LVL3_CODE     = s.BUSINESS_OFFERING_LVL3_CODE
                    , t.BUSINESS_OFFERING_LVL3_DESC     = s.BUSINESS_OFFERING_LVL3_DESC
                    , t.BUSINESS_OFFERING_LVL4_CODE     = s.BUSINESS_OFFERING_LVL4_CODE
                    , t.BUSINESS_OFFERING_LVL4_DESC     = s.BUSINESS_OFFERING_LVL4_DESC
                    , t.BUSINESS_OFFERING_LVL5_CODE     = s.BUSINESS_OFFERING_LVL5_CODE
                    , t.BUSINESS_OFFERING_LVL5_DESC     = s.BUSINESS_OFFERING_LVL5_DESC
                    , t.BUSINESS_OFFERING_DISTANCE      = s.BUSINESS_OFFERING_DISTANCE
                    , t.BUSINESS_OFFERING_CATEGORY      = s.BUSINESS_OFFERING_CATEGORY
                    , t.BUSINESS_OFFERING_ENABLED_FLAG  = s.BUSINESS_OFFERING_ENABLED_FLAG
                    , t.START_DATE_ACTIVE               = s.START_DATE_ACTIVE
                    , t.END_DATE_ACTIVE                 = s.END_DATE_ACTIVE
                    , t.CREATED_BY                      = s.CREATED_BY
                    , t.CREATION_DATE                   = s.CREATION_DATE
                    , t.BZ_LOAD_DATE                    = s.BZ_LOAD_DATE
                    , t.SV_LOAD_DATE                    = CAST(GETDATE() AS DATE)
            FROM svo.D_BUSINESS_OFFERING t
            INNER JOIN #src s
                ON s.BUSINESS_OFFERING_ID = t.BUSINESS_OFFERING_ID
            INNER JOIN #tgt h
                ON h.BUSINESS_OFFERING_ID = t.BUSINESS_OFFERING_ID
            WHERE h.ROW_HASH <> s.ROW_HASH;

            SET @RowsUpd = @@ROWCOUNT;

            INSERT INTO svo.D_BUSINESS_OFFERING
            (
                  BUSINESS_OFFERING_ID
                , BUSINESS_OFFERING_LVL1_CODE
                , BUSINESS_OFFERING_LVL1_DESC
                , BUSINESS_OFFERING_LVL2_CODE
                , BUSINESS_OFFERING_LVL2_DESC
                , BUSINESS_OFFERING_LVL3_CODE
                , BUSINESS_OFFERING_LVL3_DESC
                , BUSINESS_OFFERING_LVL4_CODE
                , BUSINESS_OFFERING_LVL4_DESC
                , BUSINESS_OFFERING_LVL5_CODE
                , BUSINESS_OFFERING_LVL5_DESC
                , BUSINESS_OFFERING_DISTANCE
                , BUSINESS_OFFERING_CATEGORY
                , BUSINESS_OFFERING_ENABLED_FLAG
                , START_DATE_ACTIVE
                , END_DATE_ACTIVE
                , CREATED_BY
                , CREATION_DATE
                , BZ_LOAD_DATE
                , SV_LOAD_DATE
            )
            SELECT
                  s.BUSINESS_OFFERING_ID
                , s.BUSINESS_OFFERING_LVL1_CODE
                , s.BUSINESS_OFFERING_LVL1_DESC
                , s.BUSINESS_OFFERING_LVL2_CODE
                , s.BUSINESS_OFFERING_LVL2_DESC
                , s.BUSINESS_OFFERING_LVL3_CODE
                , s.BUSINESS_OFFERING_LVL3_DESC
                , s.BUSINESS_OFFERING_LVL4_CODE
                , s.BUSINESS_OFFERING_LVL4_DESC
                , s.BUSINESS_OFFERING_LVL5_CODE
                , s.BUSINESS_OFFERING_LVL5_DESC
                , s.BUSINESS_OFFERING_DISTANCE
                , s.BUSINESS_OFFERING_CATEGORY
                , s.BUSINESS_OFFERING_ENABLED_FLAG
                , s.START_DATE_ACTIVE
                , s.END_DATE_ACTIVE
                , s.CREATED_BY
                , s.CREATION_DATE
                , s.BZ_LOAD_DATE
                , CAST(GETDATE() AS DATE)
            FROM #src s
            LEFT JOIN svo.D_BUSINESS_OFFERING t
                ON t.BUSINESS_OFFERING_ID = s.BUSINESS_OFFERING_ID
            WHERE t.BUSINESS_OFFERING_ID IS NULL;

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

        DECLARE
              @ErrMsg NVARCHAR(4000) = ERROR_MESSAGE()
            , @ErrNum INT            = ERROR_NUMBER()
            , @ErrSev INT            = ERROR_SEVERITY()
            , @ErrSta INT            = ERROR_STATE()
            , @ErrLin INT            = ERROR_LINE();

        UPDATE svo.DW_LOAD_LOG
            SET
                  LOAD_END_DT     = SYSUTCDATETIME()
                , STATUS          = 'FAILED'
                , ERROR_NUMBER    = @ErrNum
                , ERROR_SEVERITY  = @ErrSev
                , ERROR_STATE     = @ErrSta
                , ERROR_LINE      = @ErrLin
                , ERROR_MESSAGE   = LEFT(@ErrMsg, 4000)
        WHERE LOAD_LOG_ID = @LoadLogId;

        RAISERROR('%s', @ErrSev, @ErrSta, @ErrMsg);
        RETURN;
    END CATCH
END
GO

/* Run:
EXEC svo.usp_Load_D_BUSINESS_OFFERING @FullReload = 1, @Debug = 1;  -- initial rebuild
EXEC svo.usp_Load_D_BUSINESS_OFFERING @FullReload = 0, @Debug = 0;  -- incremental
*/


