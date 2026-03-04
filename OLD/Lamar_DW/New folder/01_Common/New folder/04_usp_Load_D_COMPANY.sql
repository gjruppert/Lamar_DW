/* =====================================================================================
   D_COMPANY (Dim, Type 1, hash-based incremental; optional full reload)
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
IF OBJECT_ID(N'svo.D_COMPANY', 'U') IS NULL
BEGIN
    CREATE TABLE svo.D_COMPANY
    (
        COMPANY_SK           BIGINT IDENTITY(1,1) NOT NULL PRIMARY KEY,
        COMPANY_ID           VARCHAR(100) NOT NULL,
        COMPANY_LVL1_CODE    VARCHAR(100) NULL,
        COMPANY_LVL1_DESC    VARCHAR(500) NULL,
        COMPANY_LVL2_CODE    VARCHAR(25)  NULL,
        COMPANY_LVL2_DESC    VARCHAR(500) NULL,
        COMPANY_LVL3_CODE    VARCHAR(25)  NULL,
        COMPANY_LVL3_DESC    VARCHAR(500) NULL,
        COMPANY_LVL4_CODE    VARCHAR(150) NULL,
        COMPANY_LVL4_DESC    VARCHAR(500) NULL,
        COMPANY_LVL5_CODE    VARCHAR(100) NULL,
        COMPANY_LVL5_DESC    VARCHAR(500) NULL,
        COMPANY_LVL6_CODE    VARCHAR(25)  NULL,
        COMPANY_LVL6_DESC    VARCHAR(500) NULL,
        COMPANY_LVL7_CODE    VARCHAR(25)  NULL,
        COMPANY_LVL7_DESC    VARCHAR(500) NULL,
        COMPANY_DISTANCE     SMALLINT     NULL,
        COMPANY_CATEGORY     VARCHAR(60)  NULL,
        COMPANY_ENABLED_FLAG VARCHAR(4)   NULL,
        START_DATE_ACTIVE    DATE         NOT NULL,
        END_DATE_ACTIVE      DATE         NOT NULL,
        CREATED_BY           VARCHAR(64)  NULL,
        CREATION_DATE        DATE         NULL,
        BZ_LOAD_DATE         DATE         NULL,
        SV_LOAD_DATE         DATE         NULL
    ) ON FG_SilverDim;
END
GO

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'UX_D_COMPANY_ID' AND object_id = OBJECT_ID('svo.D_COMPANY'))
BEGIN
    CREATE UNIQUE NONCLUSTERED INDEX UX_D_COMPANY_ID
    ON svo.D_COMPANY (COMPANY_ID)
    ON FG_SilverDim;
END
GO

/* =========
   PROCEDURE
   ========= */
IF OBJECT_ID('svo.usp_Load_D_COMPANY','P') IS NOT NULL
    DROP PROCEDURE svo.usp_Load_D_COMPANY;
GO

CREATE PROCEDURE svo.usp_Load_D_COMPANY
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
        , @Target     SYSNAME = 'svo.D_COMPANY'
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
              COMPANY_ID           = ISNULL(TRIM(lvl7.VALUE),'-1')
            , COMPANY_LVL1_CODE    = COALESCE(h1.DEP31PK1VALUE,h1.DEP30PK1VALUE,h1.DEP29PK1VALUE,h1.DEP28PK1VALUE,h1.DEP27PK1VALUE,h1.DEP26PK1VALUE,lvl7.VALUE)
            , COMPANY_LVL1_DESC    = COALESCE(lvl1.DESCRIPTION,lvl2.DESCRIPTION,lvl3.DESCRIPTION,lvl4.DESCRIPTION,lvl5.DESCRIPTION,lvl6.DESCRIPTION,lvl7.DESCRIPTION)
            , COMPANY_LVL2_CODE    = COALESCE(h1.DEP30PK1VALUE,h1.DEP29PK1VALUE,h1.DEP28PK1VALUE,h1.DEP27PK1VALUE,h1.DEP26PK1VALUE,lvl7.VALUE)
            , COMPANY_LVL2_DESC    = COALESCE(lvl2.DESCRIPTION,lvl3.DESCRIPTION,lvl4.DESCRIPTION,lvl5.DESCRIPTION,lvl6.DESCRIPTION,lvl7.DESCRIPTION)
            , COMPANY_LVL3_CODE    = COALESCE(h1.DEP29PK1VALUE,h1.DEP28PK1VALUE,h1.DEP27PK1VALUE,h1.DEP26PK1VALUE,lvl7.VALUE)
            , COMPANY_LVL3_DESC    = COALESCE(lvl3.DESCRIPTION,lvl4.DESCRIPTION,lvl5.DESCRIPTION,lvl6.DESCRIPTION,lvl7.DESCRIPTION)
            , COMPANY_LVL4_CODE    = COALESCE(h1.DEP28PK1VALUE,h1.DEP27PK1VALUE,h1.DEP26PK1VALUE,lvl7.VALUE)
            , COMPANY_LVL4_DESC    = COALESCE(lvl4.DESCRIPTION,lvl5.DESCRIPTION,lvl6.DESCRIPTION,lvl7.DESCRIPTION)
            , COMPANY_LVL5_CODE    = COALESCE(h1.DEP27PK1VALUE,h1.DEP26PK1VALUE,lvl7.VALUE)
            , COMPANY_LVL5_DESC    = COALESCE(lvl5.DESCRIPTION,lvl6.DESCRIPTION,lvl7.DESCRIPTION)
            , COMPANY_LVL6_CODE    = COALESCE(h1.DEP26PK1VALUE,lvl7.VALUE)
            , COMPANY_LVL6_DESC    = COALESCE(lvl6.DESCRIPTION,lvl7.DESCRIPTION)
            , COMPANY_LVL7_CODE    = lvl7.VALUE
            , COMPANY_LVL7_DESC    = lvl7.DESCRIPTION
            , COMPANY_DISTANCE     = CAST(0 AS SMALLINT)
            , COMPANY_CATEGORY     = lvl7.ATTRIBUTECATEGORY
            , COMPANY_ENABLED_FLAG = lvl7.ENABLEDFLAG
            , START_DATE_ACTIVE    = ISNULL(lvl7.STARTDATEACTIVE, '0001-01-01')
            , END_DATE_ACTIVE      = ISNULL(lvl7.ENDDATEACTIVE,  '9999-12-31')
            , CREATED_BY           = lvl7.CREATEDBY
            , CREATION_DATE        = CAST(lvl7.CREATIONDATE AS DATE)
            , BZ_LOAD_DATE         = COALESCE(CAST(h1.AddDateTime AS DATE), CAST('0001-01-01' AS DATE))
            , SV_LOAD_DATE         = CAST(GETDATE() AS DATE)

            , ROW_HASH = HASHBYTES(
                'SHA2_256',
                CONCAT(
                      ISNULL(TRIM(lvl7.VALUE),'-1'), '|'
                    , ISNULL(COALESCE(h1.DEP31PK1VALUE,h1.DEP30PK1VALUE,h1.DEP29PK1VALUE,h1.DEP28PK1VALUE,h1.DEP27PK1VALUE,h1.DEP26PK1VALUE,lvl7.VALUE),''), '|'
                    , ISNULL(COALESCE(lvl1.DESCRIPTION,lvl2.DESCRIPTION,lvl3.DESCRIPTION,lvl4.DESCRIPTION,lvl5.DESCRIPTION,lvl6.DESCRIPTION,lvl7.DESCRIPTION),''), '|'
                    , ISNULL(COALESCE(h1.DEP30PK1VALUE,h1.DEP29PK1VALUE,h1.DEP28PK1VALUE,h1.DEP27PK1VALUE,h1.DEP26PK1VALUE,lvl7.VALUE),''), '|'
                    , ISNULL(COALESCE(lvl2.DESCRIPTION,lvl3.DESCRIPTION,lvl4.DESCRIPTION,lvl5.DESCRIPTION,lvl6.DESCRIPTION,lvl7.DESCRIPTION),''), '|'
                    , ISNULL(COALESCE(h1.DEP29PK1VALUE,h1.DEP28PK1VALUE,h1.DEP27PK1VALUE,h1.DEP26PK1VALUE,lvl7.VALUE),''), '|'
                    , ISNULL(COALESCE(lvl3.DESCRIPTION,lvl4.DESCRIPTION,lvl5.DESCRIPTION,lvl6.DESCRIPTION,lvl7.DESCRIPTION),''), '|'
                    , ISNULL(COALESCE(h1.DEP28PK1VALUE,h1.DEP27PK1VALUE,h1.DEP26PK1VALUE,lvl7.VALUE),''), '|'
                    , ISNULL(COALESCE(lvl4.DESCRIPTION,lvl5.DESCRIPTION,lvl6.DESCRIPTION,lvl7.DESCRIPTION),''), '|'
                    , ISNULL(COALESCE(h1.DEP27PK1VALUE,h1.DEP26PK1VALUE,lvl7.VALUE),''), '|'
                    , ISNULL(COALESCE(lvl5.DESCRIPTION,lvl6.DESCRIPTION,lvl7.DESCRIPTION),''), '|'
                    , ISNULL(COALESCE(h1.DEP26PK1VALUE,lvl7.VALUE),''), '|'
                    , ISNULL(COALESCE(lvl6.DESCRIPTION,lvl7.DESCRIPTION),''), '|'
                    , ISNULL(lvl7.VALUE,''), '|'
                    , ISNULL(lvl7.DESCRIPTION,''), '|'
                    , ISNULL(lvl7.ATTRIBUTECATEGORY,''), '|'
                    , ISNULL(lvl7.ENABLEDFLAG,''), '|'
                    , ISNULL(CONVERT(VARCHAR(10), ISNULL(lvl7.STARTDATEACTIVE,'0001-01-01'), 120),''), '|'
                    , ISNULL(CONVERT(VARCHAR(10), ISNULL(lvl7.ENDDATEACTIVE,'9999-12-31'), 120),''), '|'
                    , ISNULL(lvl7.CREATEDBY,''), '|'
                    , ISNULL(CONVERT(VARCHAR(10), CAST(lvl7.CREATIONDATE AS DATE), 120),'')
                )
            )
        INTO #src
        FROM src.bzo_GL_SegmentValueHierarchyExtractPVO h1
        INNER JOIN src.bzo_GL_FndTreeAndVersionVO ver1
            ON ver1.TREEVERSIONID = h1.TREEVERSIONID
           AND ver1.TREENAME LIKE 'COMPANY LAMAR'
        INNER JOIN src.bzo_GL_ValueSetValuesPVO lvl1
            ON lvl1.VALUE = h1.DEP31PK1VALUE
           AND lvl1.ATTRIBUTECATEGORY = 'COMPANY LAMAR'
        INNER JOIN src.bzo_GL_ValueSetValuesPVO lvl2
            ON lvl2.VALUE = h1.DEP30PK1VALUE
           AND lvl2.ATTRIBUTECATEGORY = 'COMPANY LAMAR'
        INNER JOIN src.bzo_GL_ValueSetValuesPVO lvl3
            ON lvl3.VALUE = h1.DEP29PK1VALUE
           AND lvl3.ATTRIBUTECATEGORY = 'COMPANY LAMAR'
        INNER JOIN src.bzo_GL_ValueSetValuesPVO lvl4
            ON lvl4.VALUE = h1.DEP28PK1VALUE
           AND lvl4.ATTRIBUTECATEGORY = 'COMPANY LAMAR'
        INNER JOIN src.bzo_GL_ValueSetValuesPVO lvl5
            ON lvl5.VALUE = h1.DEP27PK1VALUE
           AND lvl5.ATTRIBUTECATEGORY = 'COMPANY LAMAR'
        INNER JOIN src.bzo_GL_ValueSetValuesPVO lvl6
            ON lvl6.VALUE = h1.DEP26PK1VALUE
           AND lvl6.ATTRIBUTECATEGORY = 'COMPANY LAMAR'
        RIGHT JOIN src.bzo_GL_ValueSetValuesPVO lvl7
            ON lvl7.VALUE = h1.DEP0PK1VALUE
           AND lvl7.ATTRIBUTECATEGORY = 'COMPANY LAMAR'
        WHERE lvl7.ATTRIBUTECATEGORY = 'COMPANY LAMAR'
          AND lvl7.SUMMARYFLAG = 'N';

        /* De-dupe safety on BK */
        ;WITH d AS
        (
            SELECT COMPANY_ID, rn = ROW_NUMBER() OVER (PARTITION BY COMPANY_ID ORDER BY COMPANY_ID)
            FROM #src
        )
        DELETE s
        FROM #src s
        INNER JOIN d
            ON d.COMPANY_ID = s.COMPANY_ID
           AND d.rn > 1;

        SELECT @RowsSource = COUNT(*) FROM #src;

        IF @Debug = 1
            PRINT CONCAT('Source rows: ', @RowsSource);

        BEGIN TRAN;

        /* Ensure SK=0 plug row exists (BK='-1') */
        IF NOT EXISTS (SELECT 1 FROM svo.D_COMPANY WHERE COMPANY_SK = 0)
        BEGIN
            SET IDENTITY_INSERT svo.D_COMPANY ON;

            INSERT INTO svo.D_COMPANY
            (
                  COMPANY_SK
                , COMPANY_ID
                , COMPANY_LVL1_CODE
                , COMPANY_LVL1_DESC
                , COMPANY_LVL2_CODE
                , COMPANY_LVL2_DESC
                , COMPANY_LVL3_CODE
                , COMPANY_LVL3_DESC
                , COMPANY_LVL4_CODE
                , COMPANY_LVL4_DESC
                , COMPANY_LVL5_CODE
                , COMPANY_LVL5_DESC
                , COMPANY_LVL6_CODE
                , COMPANY_LVL6_DESC
                , COMPANY_LVL7_CODE
                , COMPANY_LVL7_DESC
                , COMPANY_DISTANCE
                , COMPANY_CATEGORY
                , COMPANY_ENABLED_FLAG
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

            SET IDENTITY_INSERT svo.D_COMPANY OFF;
        END

        IF @FullReload = 1
        BEGIN
            DELETE FROM svo.D_COMPANY
            WHERE COMPANY_SK <> 0;

            INSERT INTO svo.D_COMPANY
            (
                  COMPANY_ID
                , COMPANY_LVL1_CODE
                , COMPANY_LVL1_DESC
                , COMPANY_LVL2_CODE
                , COMPANY_LVL2_DESC
                , COMPANY_LVL3_CODE
                , COMPANY_LVL3_DESC
                , COMPANY_LVL4_CODE
                , COMPANY_LVL4_DESC
                , COMPANY_LVL5_CODE
                , COMPANY_LVL5_DESC
                , COMPANY_LVL6_CODE
                , COMPANY_LVL6_DESC
                , COMPANY_LVL7_CODE
                , COMPANY_LVL7_DESC
                , COMPANY_DISTANCE
                , COMPANY_CATEGORY
                , COMPANY_ENABLED_FLAG
                , START_DATE_ACTIVE
                , END_DATE_ACTIVE
                , CREATED_BY
                , CREATION_DATE
                , BZ_LOAD_DATE
                , SV_LOAD_DATE
            )
            SELECT
                  s.COMPANY_ID
                , s.COMPANY_LVL1_CODE
                , s.COMPANY_LVL1_DESC
                , s.COMPANY_LVL2_CODE
                , s.COMPANY_LVL2_DESC
                , s.COMPANY_LVL3_CODE
                , s.COMPANY_LVL3_DESC
                , s.COMPANY_LVL4_CODE
                , s.COMPANY_LVL4_DESC
                , s.COMPANY_LVL5_CODE
                , s.COMPANY_LVL5_DESC
                , s.COMPANY_LVL6_CODE
                , s.COMPANY_LVL6_DESC
                , s.COMPANY_LVL7_CODE
                , s.COMPANY_LVL7_DESC
                , s.COMPANY_DISTANCE
                , s.COMPANY_CATEGORY
                , s.COMPANY_ENABLED_FLAG
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
            IF OBJECT_ID('tempdb..#tgt') IS NOT NULL DROP TABLE #tgt;

            SELECT
                  COMPANY_ID
                , ROW_HASH = HASHBYTES(
                    'SHA2_256',
                    CONCAT(
                          ISNULL(COMPANY_ID,''), '|'
                        , ISNULL(COMPANY_LVL1_CODE,''), '|'
                        , ISNULL(COMPANY_LVL1_DESC,''), '|'
                        , ISNULL(COMPANY_LVL2_CODE,''), '|'
                        , ISNULL(COMPANY_LVL2_DESC,''), '|'
                        , ISNULL(COMPANY_LVL3_CODE,''), '|'
                        , ISNULL(COMPANY_LVL3_DESC,''), '|'
                        , ISNULL(COMPANY_LVL4_CODE,''), '|'
                        , ISNULL(COMPANY_LVL4_DESC,''), '|'
                        , ISNULL(COMPANY_LVL5_CODE,''), '|'
                        , ISNULL(COMPANY_LVL5_DESC,''), '|'
                        , ISNULL(COMPANY_LVL6_CODE,''), '|'
                        , ISNULL(COMPANY_LVL6_DESC,''), '|'
                        , ISNULL(COMPANY_LVL7_CODE,''), '|'
                        , ISNULL(COMPANY_LVL7_DESC,''), '|'
                        , ISNULL(CONVERT(VARCHAR(10), START_DATE_ACTIVE, 120),''), '|'
                        , ISNULL(CONVERT(VARCHAR(10), END_DATE_ACTIVE, 120),''), '|'
                        , ISNULL(COMPANY_CATEGORY,''), '|'
                        , ISNULL(COMPANY_ENABLED_FLAG,''), '|'
                        , ISNULL(CREATED_BY,''), '|'
                        , ISNULL(CONVERT(VARCHAR(10), CREATION_DATE, 120),'')
                    )
                  )
            INTO #tgt
            FROM svo.D_COMPANY
            WHERE COMPANY_SK <> 0;

            UPDATE t
                SET
                      t.COMPANY_LVL1_CODE    = s.COMPANY_LVL1_CODE
                    , t.COMPANY_LVL1_DESC    = s.COMPANY_LVL1_DESC
                    , t.COMPANY_LVL2_CODE    = s.COMPANY_LVL2_CODE
                    , t.COMPANY_LVL2_DESC    = s.COMPANY_LVL2_DESC
                    , t.COMPANY_LVL3_CODE    = s.COMPANY_LVL3_CODE
                    , t.COMPANY_LVL3_DESC    = s.COMPANY_LVL3_DESC
                    , t.COMPANY_LVL4_CODE    = s.COMPANY_LVL4_CODE
                    , t.COMPANY_LVL4_DESC    = s.COMPANY_LVL4_DESC
                    , t.COMPANY_LVL5_CODE    = s.COMPANY_LVL5_CODE
                    , t.COMPANY_LVL5_DESC    = s.COMPANY_LVL5_DESC
                    , t.COMPANY_LVL6_CODE    = s.COMPANY_LVL6_CODE
                    , t.COMPANY_LVL6_DESC    = s.COMPANY_LVL6_DESC
                    , t.COMPANY_LVL7_CODE    = s.COMPANY_LVL7_CODE
                    , t.COMPANY_LVL7_DESC    = s.COMPANY_LVL7_DESC
                    , t.COMPANY_DISTANCE     = s.COMPANY_DISTANCE
                    , t.COMPANY_CATEGORY     = s.COMPANY_CATEGORY
                    , t.COMPANY_ENABLED_FLAG = s.COMPANY_ENABLED_FLAG
                    , t.START_DATE_ACTIVE    = s.START_DATE_ACTIVE
                    , t.END_DATE_ACTIVE      = s.END_DATE_ACTIVE
                    , t.CREATED_BY           = s.CREATED_BY
                    , t.CREATION_DATE        = s.CREATION_DATE
                    , t.BZ_LOAD_DATE         = s.BZ_LOAD_DATE
                    , t.SV_LOAD_DATE         = CAST(GETDATE() AS DATE)
            FROM svo.D_COMPANY t
            INNER JOIN #src s
                ON s.COMPANY_ID = t.COMPANY_ID
            INNER JOIN #tgt h
                ON h.COMPANY_ID = t.COMPANY_ID
            WHERE h.ROW_HASH <> s.ROW_HASH;

            SET @RowsUpd = @@ROWCOUNT;

            INSERT INTO svo.D_COMPANY
            (
                  COMPANY_ID
                , COMPANY_LVL1_CODE
                , COMPANY_LVL1_DESC
                , COMPANY_LVL2_CODE
                , COMPANY_LVL2_DESC
                , COMPANY_LVL3_CODE
                , COMPANY_LVL3_DESC
                , COMPANY_LVL4_CODE
                , COMPANY_LVL4_DESC
                , COMPANY_LVL5_CODE
                , COMPANY_LVL5_DESC
                , COMPANY_LVL6_CODE
                , COMPANY_LVL6_DESC
                , COMPANY_LVL7_CODE
                , COMPANY_LVL7_DESC
                , COMPANY_DISTANCE
                , COMPANY_CATEGORY
                , COMPANY_ENABLED_FLAG
                , START_DATE_ACTIVE
                , END_DATE_ACTIVE
                , CREATED_BY
                , CREATION_DATE
                , BZ_LOAD_DATE
                , SV_LOAD_DATE
            )
            SELECT
                  s.COMPANY_ID
                , s.COMPANY_LVL1_CODE
                , s.COMPANY_LVL1_DESC
                , s.COMPANY_LVL2_CODE
                , s.COMPANY_LVL2_DESC
                , s.COMPANY_LVL3_CODE
                , s.COMPANY_LVL3_DESC
                , s.COMPANY_LVL4_CODE
                , s.COMPANY_LVL4_DESC
                , s.COMPANY_LVL5_CODE
                , s.COMPANY_LVL5_DESC
                , s.COMPANY_LVL6_CODE
                , s.COMPANY_LVL6_DESC
                , s.COMPANY_LVL7_CODE
                , s.COMPANY_LVL7_DESC
                , s.COMPANY_DISTANCE
                , s.COMPANY_CATEGORY
                , s.COMPANY_ENABLED_FLAG
                , s.START_DATE_ACTIVE
                , s.END_DATE_ACTIVE
                , s.CREATED_BY
                , s.CREATION_DATE
                , s.BZ_LOAD_DATE
                , CAST(GETDATE() AS DATE)
            FROM #src s
            LEFT JOIN svo.D_COMPANY t
                ON t.COMPANY_ID = s.COMPANY_ID
            WHERE t.COMPANY_ID IS NULL;

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
EXEC svo.usp_Load_D_COMPANY @FullReload = 1, @Debug = 1;  -- initial rebuild
EXEC svo.usp_Load_D_COMPANY @FullReload = 0, @Debug = 0;  -- incremental
*/


