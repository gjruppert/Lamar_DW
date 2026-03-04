/* =====================================================================================
   D_INTERCOMPANY (Dim, Type 1, hash-based incremental; optional full reload)
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
IF OBJECT_ID(N'svo.D_INTERCOMPANY', 'U') IS NULL
BEGIN
    CREATE TABLE svo.D_INTERCOMPANY
    (
        INTERCOMPANY_SK           BIGINT IDENTITY(1,1) NOT NULL PRIMARY KEY,
        INTERCOMPANY_ID           VARCHAR(100) NOT NULL,
        INTERCOMPANY_LVL1_CODE    VARCHAR(100) NULL,
        INTERCOMPANY_LVL1_DESC    VARCHAR(500) NULL,
        INTERCOMPANY_LVL2_CODE    VARCHAR(25)  NULL,
        INTERCOMPANY_LVL2_DESC    VARCHAR(500) NULL,
        INTERCOMPANY_LVL3_CODE    VARCHAR(25)  NULL,
        INTERCOMPANY_LVL3_DESC    VARCHAR(500) NULL,
        INTERCOMPANY_DISTANCE     SMALLINT     NULL,
        INTERCOMPANY_CATEGORY     VARCHAR(60)  NULL,
        INTERCOMPANY_ENABLED_FLAG VARCHAR(4)   NULL,
        START_DATE_ACTIVE         DATE         NOT NULL,
        END_DATE_ACTIVE           DATE         NOT NULL,
        CREATED_BY                VARCHAR(64)  NULL,
        CREATION_DATE             DATE         NULL,
        BZ_LOAD_DATE              DATE         NULL,
        SV_LOAD_DATE              DATE         NULL
    ) ON FG_SilverDim;
END
GO

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'UX_D_INTERCOMPANY_ID' AND object_id = OBJECT_ID('svo.D_INTERCOMPANY'))
BEGIN
    CREATE UNIQUE NONCLUSTERED INDEX UX_D_INTERCOMPANY_ID
    ON svo.D_INTERCOMPANY (INTERCOMPANY_ID)
    ON FG_SilverDim;
END
GO

/* =========
   PROCEDURE
   ========= */
IF OBJECT_ID('svo.usp_Load_D_INTERCOMPANY','P') IS NOT NULL
    DROP PROCEDURE svo.usp_Load_D_INTERCOMPANY;
GO

CREATE PROCEDURE svo.usp_Load_D_INTERCOMPANY
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
        , @Target     SYSNAME = 'svo.D_INTERCOMPANY'
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
              INTERCOMPANY_ID           = ISNULL(TRIM(lvl3.VALUE),'-1')
            , INTERCOMPANY_LVL1_CODE    = COALESCE(h1.DEP31PK1VALUE,h1.DEP30PK1VALUE,lvl3.VALUE)
            , INTERCOMPANY_LVL1_DESC    = COALESCE(lvl1.DESCRIPTION,lvl2.DESCRIPTION,lvl3.DESCRIPTION)
            , INTERCOMPANY_LVL2_CODE    = COALESCE(h1.DEP30PK1VALUE,lvl3.VALUE)
            , INTERCOMPANY_LVL2_DESC    = COALESCE(lvl2.DESCRIPTION,lvl3.DESCRIPTION)
            , INTERCOMPANY_LVL3_CODE    = lvl3.VALUE
            , INTERCOMPANY_LVL3_DESC    = lvl3.DESCRIPTION
            , INTERCOMPANY_DISTANCE     = CAST(0 AS SMALLINT)
            , INTERCOMPANY_CATEGORY     = lvl3.ATTRIBUTECATEGORY
            , INTERCOMPANY_ENABLED_FLAG = lvl3.ENABLEDFLAG
            , START_DATE_ACTIVE         = ISNULL(lvl3.STARTDATEACTIVE, '0001-01-01')
            , END_DATE_ACTIVE           = ISNULL(lvl3.ENDDATEACTIVE,  '9999-12-31')
            , CREATED_BY                = lvl3.CREATEDBY
            , CREATION_DATE             = CAST(lvl3.CREATIONDATE AS DATE)
            , BZ_LOAD_DATE              = COALESCE(CAST(h1.AddDateTime AS DATE), CAST('0001-01-01' AS DATE))
            , SV_LOAD_DATE              = CAST(GETDATE() AS DATE)

            , ROW_HASH = HASHBYTES(
                'SHA2_256',
                CONCAT(
                      ISNULL(TRIM(lvl3.VALUE),'-1'), '|'
                    , ISNULL(COALESCE(h1.DEP31PK1VALUE,h1.DEP30PK1VALUE,lvl3.VALUE),''), '|'
                    , ISNULL(COALESCE(lvl1.DESCRIPTION,lvl2.DESCRIPTION,lvl3.DESCRIPTION),''), '|'
                    , ISNULL(COALESCE(h1.DEP30PK1VALUE,lvl3.VALUE),''), '|'
                    , ISNULL(COALESCE(lvl2.DESCRIPTION,lvl3.DESCRIPTION),''), '|'
                    , ISNULL(lvl3.VALUE,''), '|'
                    , ISNULL(lvl3.DESCRIPTION,''), '|'
                    , ISNULL(lvl3.ATTRIBUTECATEGORY,''), '|'
                    , ISNULL(lvl3.ENABLEDFLAG,''), '|'
                    , ISNULL(CONVERT(VARCHAR(10), ISNULL(lvl3.STARTDATEACTIVE,'0001-01-01'), 120),''), '|'
                    , ISNULL(CONVERT(VARCHAR(10), ISNULL(lvl3.ENDDATEACTIVE,'9999-12-31'), 120),''), '|'
                    , ISNULL(lvl3.CREATEDBY,''), '|'
                    , ISNULL(CONVERT(VARCHAR(10), CAST(lvl3.CREATIONDATE AS DATE), 120),'')
                )
            )
        INTO #src
        FROM src.bzo_GL_SegmentValueHierarchyExtractPVO h1
        INNER JOIN src.bzo_GL_FndTreeAndVersionVO ver1
            ON ver1.TREEVERSIONID = h1.TREEVERSIONID
           AND ver1.TREENAME LIKE 'INTERCOMPANY LAMAR'
        INNER JOIN src.bzo_GL_ValueSetValuesPVO lvl1
            ON lvl1.VALUE = h1.DEP31PK1VALUE
           AND lvl1.ATTRIBUTECATEGORY = 'INTERCOMPANY LAMAR'
        INNER JOIN src.bzo_GL_ValueSetValuesPVO lvl2
            ON lvl2.VALUE = h1.DEP30PK1VALUE
           AND lvl2.ATTRIBUTECATEGORY = 'INTERCOMPANY LAMAR'
        RIGHT JOIN src.bzo_GL_ValueSetValuesPVO lvl3
            ON lvl3.VALUE = h1.DEP0PK1VALUE
           AND lvl3.ATTRIBUTECATEGORY = 'INTERCOMPANY LAMAR'
        WHERE lvl3.ATTRIBUTECATEGORY = 'INTERCOMPANY LAMAR'
          AND lvl3.SUMMARYFLAG = 'N';

        /* De-dupe safety on BK */
        ;WITH d AS
        (
            SELECT INTERCOMPANY_ID, rn = ROW_NUMBER() OVER (PARTITION BY INTERCOMPANY_ID ORDER BY INTERCOMPANY_ID)
            FROM #src
        )
        DELETE s
        FROM #src s
        INNER JOIN d
            ON d.INTERCOMPANY_ID = s.INTERCOMPANY_ID
           AND d.rn > 1;

        SELECT @RowsSource = COUNT(*) FROM #src;

        IF @Debug = 1
            PRINT CONCAT('Source rows: ', @RowsSource);

        BEGIN TRAN;

        /* Ensure SK=0 plug row exists (BK='-1') */
        IF NOT EXISTS (SELECT 1 FROM svo.D_INTERCOMPANY WHERE INTERCOMPANY_SK = 0)
        BEGIN
            SET IDENTITY_INSERT svo.D_INTERCOMPANY ON;

            INSERT INTO svo.D_INTERCOMPANY
            (
                  INTERCOMPANY_SK
                , INTERCOMPANY_ID
                , INTERCOMPANY_LVL1_CODE
                , INTERCOMPANY_LVL1_DESC
                , INTERCOMPANY_LVL2_CODE
                , INTERCOMPANY_LVL2_DESC
                , INTERCOMPANY_LVL3_CODE
                , INTERCOMPANY_LVL3_DESC
                , INTERCOMPANY_DISTANCE
                , INTERCOMPANY_CATEGORY
                , INTERCOMPANY_ENABLED_FLAG
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

            SET IDENTITY_INSERT svo.D_INTERCOMPANY OFF;
        END

        IF @FullReload = 1
        BEGIN
            DELETE FROM svo.D_INTERCOMPANY
            WHERE INTERCOMPANY_SK <> 0;

            INSERT INTO svo.D_INTERCOMPANY
            (
                  INTERCOMPANY_ID
                , INTERCOMPANY_LVL1_CODE
                , INTERCOMPANY_LVL1_DESC
                , INTERCOMPANY_LVL2_CODE
                , INTERCOMPANY_LVL2_DESC
                , INTERCOMPANY_LVL3_CODE
                , INTERCOMPANY_LVL3_DESC
                , INTERCOMPANY_DISTANCE
                , INTERCOMPANY_CATEGORY
                , INTERCOMPANY_ENABLED_FLAG
                , START_DATE_ACTIVE
                , END_DATE_ACTIVE
                , CREATED_BY
                , CREATION_DATE
                , BZ_LOAD_DATE
                , SV_LOAD_DATE
            )
            SELECT
                  s.INTERCOMPANY_ID
                , s.INTERCOMPANY_LVL1_CODE
                , s.INTERCOMPANY_LVL1_DESC
                , s.INTERCOMPANY_LVL2_CODE
                , s.INTERCOMPANY_LVL2_DESC
                , s.INTERCOMPANY_LVL3_CODE
                , s.INTERCOMPANY_LVL3_DESC
                , s.INTERCOMPANY_DISTANCE
                , s.INTERCOMPANY_CATEGORY
                , s.INTERCOMPANY_ENABLED_FLAG
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
                  INTERCOMPANY_ID
                , ROW_HASH = HASHBYTES(
                    'SHA2_256',
                    CONCAT(
                          ISNULL(INTERCOMPANY_ID,''), '|'
                        , ISNULL(INTERCOMPANY_LVL1_CODE,''), '|'
                        , ISNULL(INTERCOMPANY_LVL1_DESC,''), '|'
                        , ISNULL(INTERCOMPANY_LVL2_CODE,''), '|'
                        , ISNULL(INTERCOMPANY_LVL2_DESC,''), '|'
                        , ISNULL(INTERCOMPANY_LVL3_CODE,''), '|'
                        , ISNULL(INTERCOMPANY_LVL3_DESC,''), '|'
                        , ISNULL(CONVERT(VARCHAR(10), START_DATE_ACTIVE, 120),''), '|'
                        , ISNULL(CONVERT(VARCHAR(10), END_DATE_ACTIVE, 120),''), '|'
                        , ISNULL(INTERCOMPANY_CATEGORY,''), '|'
                        , ISNULL(INTERCOMPANY_ENABLED_FLAG,''), '|'
                        , ISNULL(CREATED_BY,''), '|'
                        , ISNULL(CONVERT(VARCHAR(10), CREATION_DATE, 120),'')
                    )
                  )
            INTO #tgt
            FROM svo.D_INTERCOMPANY
            WHERE INTERCOMPANY_SK <> 0;

            UPDATE t
                SET
                      t.INTERCOMPANY_LVL1_CODE    = s.INTERCOMPANY_LVL1_CODE
                    , t.INTERCOMPANY_LVL1_DESC    = s.INTERCOMPANY_LVL1_DESC
                    , t.INTERCOMPANY_LVL2_CODE    = s.INTERCOMPANY_LVL2_CODE
                    , t.INTERCOMPANY_LVL2_DESC    = s.INTERCOMPANY_LVL2_DESC
                    , t.INTERCOMPANY_LVL3_CODE    = s.INTERCOMPANY_LVL3_CODE
                    , t.INTERCOMPANY_LVL3_DESC    = s.INTERCOMPANY_LVL3_DESC
                    , t.INTERCOMPANY_DISTANCE     = s.INTERCOMPANY_DISTANCE
                    , t.INTERCOMPANY_CATEGORY     = s.INTERCOMPANY_CATEGORY
                    , t.INTERCOMPANY_ENABLED_FLAG = s.INTERCOMPANY_ENABLED_FLAG
                    , t.START_DATE_ACTIVE         = s.START_DATE_ACTIVE
                    , t.END_DATE_ACTIVE           = s.END_DATE_ACTIVE
                    , t.CREATED_BY                = s.CREATED_BY
                    , t.CREATION_DATE             = s.CREATION_DATE
                    , t.BZ_LOAD_DATE              = s.BZ_LOAD_DATE
                    , t.SV_LOAD_DATE              = CAST(GETDATE() AS DATE)
            FROM svo.D_INTERCOMPANY t
            INNER JOIN #src s
                ON s.INTERCOMPANY_ID = t.INTERCOMPANY_ID
            INNER JOIN #tgt h
                ON h.INTERCOMPANY_ID = t.INTERCOMPANY_ID
            WHERE h.ROW_HASH <> s.ROW_HASH;

            SET @RowsUpd = @@ROWCOUNT;

            INSERT INTO svo.D_INTERCOMPANY
            (
                  INTERCOMPANY_ID
                , INTERCOMPANY_LVL1_CODE
                , INTERCOMPANY_LVL1_DESC
                , INTERCOMPANY_LVL2_CODE
                , INTERCOMPANY_LVL2_DESC
                , INTERCOMPANY_LVL3_CODE
                , INTERCOMPANY_LVL3_DESC
                , INTERCOMPANY_DISTANCE
                , INTERCOMPANY_CATEGORY
                , INTERCOMPANY_ENABLED_FLAG
                , START_DATE_ACTIVE
                , END_DATE_ACTIVE
                , CREATED_BY
                , CREATION_DATE
                , BZ_LOAD_DATE
                , SV_LOAD_DATE
            )
            SELECT
                  s.INTERCOMPANY_ID
                , s.INTERCOMPANY_LVL1_CODE
                , s.INTERCOMPANY_LVL1_DESC
                , s.INTERCOMPANY_LVL2_CODE
                , s.INTERCOMPANY_LVL2_DESC
                , s.INTERCOMPANY_LVL3_CODE
                , s.INTERCOMPANY_LVL3_DESC
                , s.INTERCOMPANY_DISTANCE
                , s.INTERCOMPANY_CATEGORY
                , s.INTERCOMPANY_ENABLED_FLAG
                , s.START_DATE_ACTIVE
                , s.END_DATE_ACTIVE
                , s.CREATED_BY
                , s.CREATION_DATE
                , s.BZ_LOAD_DATE
                , CAST(GETDATE() AS DATE)
            FROM #src s
            LEFT JOIN svo.D_INTERCOMPANY t
                ON t.INTERCOMPANY_ID = s.INTERCOMPANY_ID
            WHERE t.INTERCOMPANY_ID IS NULL;

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
EXEC svo.usp_Load_D_INTERCOMPANY @FullReload = 1, @Debug = 1;  -- initial rebuild
EXEC svo.usp_Load_D_INTERCOMPANY @FullReload = 0, @Debug = 0;  -- incremental
*/


