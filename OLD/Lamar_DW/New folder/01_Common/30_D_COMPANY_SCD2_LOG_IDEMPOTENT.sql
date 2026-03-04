USE Oracle_Reporting_P2;
GO

/* =====================================================================
   COA DIM: svo.D_COMPANY (SCD2)
   Source: synonym-based bzo.* objects (DB independent)
   BK: COMPANY_ID

   Locked rules applied:
     - Stored proc in svo schema
     - BZ_LOAD_DATE never NULL: COALESCE(CAST(AddDateTime AS date), CAST(GETDATE() AS date))
     - Preserve synonym-based source query inside procedure
     - SCD2: EFF_DATE, END_DATE, CRE_DATE, UDT_DATE, CURR_IND
     - ETL run logging (etl.ETL_RUN)
     - Idempotent + transactional

   Index:
     - Drops legacy unique index UX_D_COMPANY_ID if present (breaks SCD2 history)
     - Creates filtered unique index UX_D_COMPANY_ID_CURR for current rows only
   ===================================================================== */

/* =========================
   0) Logging (create once)
   ========================= */
IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'etl')
    EXEC('CREATE SCHEMA etl AUTHORIZATION dbo;');
GO

IF OBJECT_ID(N'etl.ETL_RUN', 'U') IS NULL
BEGIN
    CREATE TABLE etl.ETL_RUN
    (
          RUN_ID        bigint IDENTITY(1,1) NOT NULL CONSTRAINT PK_ETL_RUN PRIMARY KEY
        , PROC_NAME     sysname              NOT NULL
        , TARGET_OBJECT sysname              NOT NULL
        , ASOF_DATE     date                 NULL
        , START_DTTM    datetime2(0)         NOT NULL
        , END_DTTM      datetime2(0)         NULL
        , STATUS        varchar(20)          NOT NULL  -- STARTED/SUCCESS/FAILED
        , ROW_INSERTED  int                  NULL
        , ROW_EXPIRED   int                  NULL
        , ERROR_MESSAGE nvarchar(4000)       NULL
    );
END
GO

/* ==============================================
   1) Drop old UNIQUE index that breaks SCD2
   ============================================== */
IF EXISTS
(
    SELECT 1
    FROM sys.indexes
    WHERE name = 'UX_D_COMPANY_ID'
      AND object_id = OBJECT_ID('svo.D_COMPANY')
)
BEGIN
    DROP INDEX UX_D_COMPANY_ID ON svo.D_COMPANY;
END
GO

/* ==============================================
   2) Create filtered unique index for current rows
   ============================================== */
IF NOT EXISTS
(
    SELECT 1
    FROM sys.indexes
    WHERE name = 'UX_D_COMPANY_ID_CURR'
      AND object_id = OBJECT_ID('svo.D_COMPANY')
)
BEGIN
    CREATE UNIQUE NONCLUSTERED INDEX UX_D_COMPANY_ID_CURR
        ON svo.D_COMPANY(COMPANY_ID)
        WHERE CURR_IND = 'Y' AND END_DATE = '9999-12-31'
        ON FG_SilverDim;
END
GO

/* ================================
   3) SCD2 loader stored procedure
   ================================ */
CREATE OR ALTER PROCEDURE svo.usp_Load_D_COMPANY_SCD2
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
        , @TargetObject sysname = 'svo.D_COMPANY';

    DECLARE
          @Inserted int = 0
        , @Expired  int = 0;

    INSERT INTO etl.ETL_RUN (PROC_NAME, TARGET_OBJECT, ASOF_DATE, START_DTTM, STATUS)
    VALUES (@ProcName, @TargetObject, @AsOfDate, SYSDATETIME(), 'STARTED');

    SET @RunId = SCOPE_IDENTITY();

    BEGIN TRY
        BEGIN TRAN;

        /* =========================================================
           A) Plug row (SK=0, BK='-1') - keep stable, not SCD-managed
           ========================================================= */
        IF NOT EXISTS (SELECT 1 FROM svo.D_COMPANY WHERE COMPANY_SK = 0)
        BEGIN
            SET IDENTITY_INSERT svo.D_COMPANY ON;

            INSERT INTO svo.D_COMPANY (COMPANY_SK, COMPANY_ID, COMPANY_LVL1_CODE, COMPANY_LVL1_DESC, COMPANY_LVL2_CODE, COMPANY_LVL2_DESC, COMPANY_LVL3_CODE, COMPANY_LVL3_DESC, COMPANY_LVL4_CODE, COMPANY_LVL4_DESC, COMPANY_LVL5_CODE, COMPANY_LVL5_DESC, COMPANY_LVL6_CODE, COMPANY_LVL6_DESC, COMPANY_LVL7_CODE, COMPANY_LVL7_DESC, COMPANY_DISTANCE, COMPANY_CATEGORY, COMPANY_ENABLED_FLAG, START_DATE_ACTIVE, END_DATE_ACTIVE, CREATED_BY, CREATION_DATE, BZ_LOAD_DATE, SV_LOAD_DATE, EFF_DATE, END_DATE, CRE_DATE, UDT_DATE, CURR_IND)
            VALUES (0, '-1', '-1', 'Unknown', '-1', 'Unknown', '-1', 'Unknown', '-1', 'Unknown', '-1', 'Unknown', '-1', 'Unknown', '-1', 'Unknown', 0, 'Missing', 'N', '0001-01-01', '9999-12-31', 'UNK', CAST(GETDATE() AS date), CAST(GETDATE() AS date), CAST(GETDATE() AS date), CAST(GETDATE() AS date), '9999-12-31', SYSDATETIME(), SYSDATETIME(), 'Y');

            SET IDENTITY_INSERT svo.D_COMPANY OFF;
        END

        /* =========================================================
           B) Source snapshot (original SELECT preserved; BZ_LOAD_DATE hardened)
           ========================================================= */
        IF OBJECT_ID('tempdb..#src') IS NOT NULL DROP TABLE #src;

SELECT
    ISNULL(TRIM(lvl7.VALUE),'-1') AS COMPANY_ID,
    COALESCE(h1.DEP31PK1VALUE,h1.DEP30PK1VALUE, h1.DEP29PK1VALUE,h1.DEP28PK1VALUE,h1.DEP27PK1VALUE,h1.DEP26PK1VALUE,lvl7.VALUE) AS COMPANY_LVL1_CODE,
    COALESCE(lvl1.DESCRIPTION,lvl2.DESCRIPTION,lvl3.DESCRIPTION,lvl4.DESCRIPTION,lvl5.DESCRIPTION,lvl6.DESCRIPTION,lvl7.DESCRIPTION) AS COMPANY_LVL1_DESC,
    COALESCE(h1.DEP30PK1VALUE,h1.DEP29PK1VALUE,h1.DEP28PK1VALUE,h1.DEP27PK1VALUE,h1.DEP26PK1VALUE,lvl7.VALUE) AS COMPANY_LVL2_CODE,
    COALESCE(lvl2.DESCRIPTION,lvl3.DESCRIPTION,lvl4.DESCRIPTION,lvl5.DESCRIPTION,lvl6.DESCRIPTION,lvl7.DESCRIPTION) AS COMPANY_LVL2_DESC,
    COALESCE(h1.DEP29PK1VALUE,h1.DEP28PK1VALUE,h1.DEP27PK1VALUE,h1.DEP26PK1VALUE,lvl7.VALUE) AS COMPANY_LVL3_CODE,
    COALESCE(lvl3.DESCRIPTION,lvl4.DESCRIPTION,lvl5.DESCRIPTION,lvl6.DESCRIPTION,lvl7.DESCRIPTION) AS COMPANY_LVL3_DESC,
    COALESCE(h1.DEP28PK1VALUE,h1.DEP27PK1VALUE,h1.DEP26PK1VALUE,lvl7.VALUE) AS COMPANY_LVL4_CODE,
    COALESCE(lvl4.DESCRIPTION,lvl5.DESCRIPTION,lvl6.DESCRIPTION,lvl7.DESCRIPTION) AS COMPANY_LVL4_DESC,
    COALESCE(h1.DEP27PK1VALUE,h1.DEP26PK1VALUE,lvl7.VALUE) AS COMPANY_LVL5_CODE,
    COALESCE(lvl5.DESCRIPTION,lvl6.DESCRIPTION,lvl7.DESCRIPTION) AS COMPANY_LVL5_DESC,
    COALESCE(h1.DEP26PK1VALUE,lvl7.VALUE) AS COMPANY_LVL6_CODE,
    COALESCE(lvl6.DESCRIPTION,lvl7.DESCRIPTION) AS COMPANY_LVL6_DESC,
    lvl7.VALUE       AS COMPANY_LVL7_CODE,
    lvl7.DESCRIPTION AS COMPANY_LVL7_DESC,
    0                  AS COMPANY_DISTANCE,
    lvl7.ATTRIBUTECATEGORY AS COMPANY_CATEGORY,
    lvl7.ENABLEDFLAG AS COMPANY_ENABLED_FLAG,
    ISNULL(lvl7.STARTDATEACTIVE, '0001-01-01') AS START_DATE_ACTIVE,
    ISNULL(lvl7.ENDDATEACTIVE,  '9999-12-31') AS END_DATE_ACTIVE,
    lvl7.CREATEDBY AS CREATED_BY,
    CAST(lvl7.CREATIONDATE AS DATE) AS CREATION_DATE,
    COALESCE(CAST(h1.AddDateTime AS DATE), CAST(GETDATE() AS DATE)) AS BZ_LOAD_DATE,
    CAST(GETDATE() AS DATE) AS SV_LOAD_DATE
INTO #src
FROM src.bzo_GL_SegmentValueHierarchyExtractPVO h1 
INNER JOIN src.bzo_GL_FndTreeAndVersionVO ver1 
    ON ver1.TREEVERSIONID = h1.TREEVERSIONID AND ver1.TREENAME LIKE 'COMPANY LAMAR'
INNER JOIN src.bzo_GL_ValueSetValuesPVO lvl1 ON lvl1.VALUE = h1.DEP31PK1VALUE AND lvl1.ATTRIBUTECATEGORY = 'COMPANY LAMAR'
INNER JOIN src.bzo_GL_ValueSetValuesPVO lvl2 ON lvl2.VALUE = h1.DEP30PK1VALUE AND lvl2.ATTRIBUTECATEGORY = 'COMPANY LAMAR'
INNER JOIN src.bzo_GL_ValueSetValuesPVO lvl3 ON lvl3.VALUE = h1.DEP29PK1VALUE AND lvl3.ATTRIBUTECATEGORY = 'COMPANY LAMAR'
INNER JOIN src.bzo_GL_ValueSetValuesPVO lvl4 ON lvl4.VALUE = h1.DEP28PK1VALUE AND lvl4.ATTRIBUTECATEGORY = 'COMPANY LAMAR'
INNER JOIN src.bzo_GL_ValueSetValuesPVO lvl5 ON lvl5.VALUE = h1.DEP27PK1VALUE AND lvl5.ATTRIBUTECATEGORY = 'COMPANY LAMAR'
INNER JOIN src.bzo_GL_ValueSetValuesPVO lvl6 ON lvl6.VALUE = h1.DEP26PK1VALUE AND lvl6.ATTRIBUTECATEGORY = 'COMPANY LAMAR'
RIGHT JOIN src.bzo_GL_ValueSetValuesPVO lvl7 ON lvl7.VALUE = h1.DEP0PK1VALUE AND lvl7.ATTRIBUTECATEGORY = 'COMPANY LAMAR'
WHERE lvl7.ATTRIBUTECATEGORY = 'COMPANY LAMAR' AND lvl7.SUMMARYFLAG = 'N';
        ;

        /* Remove plug BK from SCD processing */
        DELETE FROM #src WHERE COMPANY_ID = '-1';

        /* Build source hash for SCD compare */
        IF COL_LENGTH('tempdb..#src', 'SRC_HASH') IS NULL
            ALTER TABLE #src ADD SRC_HASH varbinary(32) NULL;

        UPDATE s
            SET s.SRC_HASH = HASHBYTES('SHA2_256', CONCAT(COALESCE(CONVERT(nvarchar(500), s.COMPANY_ID), N''), N'|', COALESCE(CONVERT(nvarchar(500), s.COMPANY_LVL1_CODE), N''), N'|', COALESCE(CONVERT(nvarchar(500), s.COMPANY_LVL1_DESC), N''), N'|', COALESCE(CONVERT(nvarchar(500), s.COMPANY_LVL2_CODE), N''), N'|', COALESCE(CONVERT(nvarchar(500), s.COMPANY_LVL2_DESC), N''), N'|', COALESCE(CONVERT(nvarchar(500), s.COMPANY_LVL3_CODE), N''), N'|', COALESCE(CONVERT(nvarchar(500), s.COMPANY_LVL3_DESC), N''), N'|', COALESCE(CONVERT(nvarchar(500), s.COMPANY_LVL4_CODE), N''), N'|', COALESCE(CONVERT(nvarchar(500), s.COMPANY_LVL4_DESC), N''), N'|', COALESCE(CONVERT(nvarchar(500), s.COMPANY_LVL5_CODE), N''), N'|', COALESCE(CONVERT(nvarchar(500), s.COMPANY_LVL5_DESC), N''), N'|', COALESCE(CONVERT(nvarchar(500), s.COMPANY_LVL6_CODE), N''), N'|', COALESCE(CONVERT(nvarchar(500), s.COMPANY_LVL6_DESC), N''), N'|', COALESCE(CONVERT(nvarchar(500), s.COMPANY_LVL7_CODE), N''), N'|', COALESCE(CONVERT(nvarchar(500), s.COMPANY_LVL7_DESC), N''), N'|', CONVERT(nvarchar(10), s.COMPANY_DISTANCE), N'|', COALESCE(CONVERT(nvarchar(500), s.COMPANY_CATEGORY), N''), N'|', COALESCE(CONVERT(nvarchar(500), s.COMPANY_ENABLED_FLAG), N''), N'|', CONVERT(nvarchar(10), s.START_DATE_ACTIVE, 120), N'|', CONVERT(nvarchar(10), s.END_DATE_ACTIVE, 120), N'|', COALESCE(CONVERT(nvarchar(500), s.CREATED_BY), N''), N'|', CONVERT(nvarchar(10), s.CREATION_DATE, 120)))
        FROM #src s;

        /* =========================================================
           C) Current target snapshot + hash
           ========================================================= */
        IF OBJECT_ID('tempdb..#tgt') IS NOT NULL DROP TABLE #tgt;

        SELECT
              t.COMPANY_ID
            , HASHBYTES('SHA2_256', CONCAT(COALESCE(CONVERT(nvarchar(500), t.COMPANY_ID), N''), N'|', COALESCE(CONVERT(nvarchar(500), t.COMPANY_LVL1_CODE), N''), N'|', COALESCE(CONVERT(nvarchar(500), t.COMPANY_LVL1_DESC), N''), N'|', COALESCE(CONVERT(nvarchar(500), t.COMPANY_LVL2_CODE), N''), N'|', COALESCE(CONVERT(nvarchar(500), t.COMPANY_LVL2_DESC), N''), N'|', COALESCE(CONVERT(nvarchar(500), t.COMPANY_LVL3_CODE), N''), N'|', COALESCE(CONVERT(nvarchar(500), t.COMPANY_LVL3_DESC), N''), N'|', COALESCE(CONVERT(nvarchar(500), t.COMPANY_LVL4_CODE), N''), N'|', COALESCE(CONVERT(nvarchar(500), t.COMPANY_LVL4_DESC), N''), N'|', COALESCE(CONVERT(nvarchar(500), t.COMPANY_LVL5_CODE), N''), N'|', COALESCE(CONVERT(nvarchar(500), t.COMPANY_LVL5_DESC), N''), N'|', COALESCE(CONVERT(nvarchar(500), t.COMPANY_LVL6_CODE), N''), N'|', COALESCE(CONVERT(nvarchar(500), t.COMPANY_LVL6_DESC), N''), N'|', COALESCE(CONVERT(nvarchar(500), t.COMPANY_LVL7_CODE), N''), N'|', COALESCE(CONVERT(nvarchar(500), t.COMPANY_LVL7_DESC), N''), N'|', CONVERT(nvarchar(10), t.COMPANY_DISTANCE), N'|', COALESCE(CONVERT(nvarchar(500), t.COMPANY_CATEGORY), N''), N'|', COALESCE(CONVERT(nvarchar(500), t.COMPANY_ENABLED_FLAG), N''), N'|', CONVERT(nvarchar(10), t.START_DATE_ACTIVE, 120), N'|', CONVERT(nvarchar(10), t.END_DATE_ACTIVE, 120), N'|', COALESCE(CONVERT(nvarchar(500), t.CREATED_BY), N''), N'|', CONVERT(nvarchar(10), t.CREATION_DATE, 120))) AS TGT_HASH
        INTO #tgt
        FROM svo.D_COMPANY t
        WHERE t.CURR_IND = 'Y'
          AND t.END_DATE = @HighDate
          AND t.COMPANY_ID <> '-1';

        /* =========================================================
           D) Delta: NEW or CHANGED
           ========================================================= */
        IF OBJECT_ID('tempdb..#delta') IS NOT NULL DROP TABLE #delta;

        SELECT s.*
        INTO #delta
        FROM #src s
        LEFT JOIN #tgt t
            ON t.COMPANY_ID = s.COMPANY_ID
        WHERE t.COMPANY_ID IS NULL
           OR t.TGT_HASH <> s.SRC_HASH;

        /* =========================================================
           E) Expire current rows for changed BKs
           ========================================================= */
        UPDATE tgt
            SET
                  tgt.END_DATE     = DATEADD(day, -1, @AsOfDate)
                , tgt.CURR_IND     = 'N'
                , tgt.UDT_DATE     = SYSDATETIME()
                , tgt.SV_LOAD_DATE = CAST(GETDATE() AS date)
        FROM svo.D_COMPANY tgt
        INNER JOIN #delta d
            ON d.COMPANY_ID = tgt.COMPANY_ID
        WHERE tgt.CURR_IND = 'Y'
          AND tgt.END_DATE = @HighDate
          AND tgt.COMPANY_ID <> '-1';

        SET @Expired = @@ROWCOUNT;

        /* =========================================================
           F) Insert new current rows
           ========================================================= */
        INSERT INTO svo.D_COMPANY
        (
              COMPANY_ID, COMPANY_LVL1_CODE, COMPANY_LVL1_DESC, COMPANY_LVL2_CODE, COMPANY_LVL2_DESC, COMPANY_LVL3_CODE, COMPANY_LVL3_DESC, COMPANY_LVL4_CODE, COMPANY_LVL4_DESC, COMPANY_LVL5_CODE, COMPANY_LVL5_DESC, COMPANY_LVL6_CODE, COMPANY_LVL6_DESC, COMPANY_LVL7_CODE, COMPANY_LVL7_DESC, COMPANY_DISTANCE, COMPANY_CATEGORY, COMPANY_ENABLED_FLAG, START_DATE_ACTIVE, END_DATE_ACTIVE, CREATED_BY, CREATION_DATE, BZ_LOAD_DATE, SV_LOAD_DATE
            , EFF_DATE
            , END_DATE
            , CRE_DATE
            , UDT_DATE
            , CURR_IND
        )
        SELECT
              d.COMPANY_ID, d.COMPANY_LVL1_CODE, d.COMPANY_LVL1_DESC, d.COMPANY_LVL2_CODE, d.COMPANY_LVL2_DESC, d.COMPANY_LVL3_CODE, d.COMPANY_LVL3_DESC, d.COMPANY_LVL4_CODE, d.COMPANY_LVL4_DESC, d.COMPANY_LVL5_CODE, d.COMPANY_LVL5_DESC, d.COMPANY_LVL6_CODE, d.COMPANY_LVL6_DESC, d.COMPANY_LVL7_CODE, d.COMPANY_LVL7_DESC, d.COMPANY_DISTANCE, d.COMPANY_CATEGORY, d.COMPANY_ENABLED_FLAG, d.START_DATE_ACTIVE, d.END_DATE_ACTIVE, d.CREATED_BY, d.CREATION_DATE, d.BZ_LOAD_DATE, d.SV_LOAD_DATE
            , @AsOfDate
            , @HighDate
            , SYSDATETIME()
            , SYSDATETIME()
            , 'Y'
        FROM #delta d;

        SET @Inserted = @@ROWCOUNT;

        COMMIT;

        UPDATE etl.ETL_RUN
            SET
                  END_DTTM      = SYSDATETIME()
                , STATUS        = 'SUCCESS'
                , ROW_INSERTED  = @Inserted
                , ROW_EXPIRED   = @Expired
                , ERROR_MESSAGE = NULL
        WHERE RUN_ID = @RunId;
    END TRY
    BEGIN CATCH
        IF @@TRANCOUNT > 0 ROLLBACK;

        DECLARE @Err nvarchar(4000) = ERROR_MESSAGE();

        UPDATE etl.ETL_RUN
            SET
                  END_DTTM      = SYSDATETIME()
                , STATUS        = 'FAILED'
                , ROW_INSERTED  = @Inserted
                , ROW_EXPIRED   = @Expired
                , ERROR_MESSAGE = @Err
        WHERE RUN_ID = @RunId;

        ;THROW;
    END CATCH
END
GO

