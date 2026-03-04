USE Oracle_Reporting_P2;
GO

/* =====================================================================
   COA DIM: svo.D_COST_CENTER (SCD2)
   Source: synonym-based bzo.* objects (DB independent)
   BK: COST_CENTER_ID

   Locked rules applied:
     - Stored proc in svo schema
     - BZ_LOAD_DATE never NULL: COALESCE(CAST(AddDateTime AS date), CAST(GETDATE() AS date))
     - Preserve synonym-based source query inside procedure
     - SCD2: EFF_DATE, END_DATE, CRE_DATE, UDT_DATE, CURR_IND
     - ETL run logging (etl.ETL_RUN)
     - Idempotent + transactional

   Index:
     - Drops legacy unique index UX_D_COST_CENTER_ID if present (breaks SCD2 history)
     - Creates filtered unique index UX_D_COST_CENTER_ID_CURR for current rows only
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
    WHERE name = 'UX_D_COST_CENTER_ID'
      AND object_id = OBJECT_ID('svo.D_COST_CENTER')
)
BEGIN
    DROP INDEX UX_D_COST_CENTER_ID ON svo.D_COST_CENTER;
END
GO

/* ==============================================
   2) Create filtered unique index for current rows
   ============================================== */
IF NOT EXISTS
(
    SELECT 1
    FROM sys.indexes
    WHERE name = 'UX_D_COST_CENTER_ID_CURR'
      AND object_id = OBJECT_ID('svo.D_COST_CENTER')
)
BEGIN
    CREATE UNIQUE NONCLUSTERED INDEX UX_D_COST_CENTER_ID_CURR
        ON svo.D_COST_CENTER(COST_CENTER_ID)
        WHERE CURR_IND = 'Y' AND END_DATE = '9999-12-31'
        ON FG_SilverDim;
END
GO

/* ================================
   3) SCD2 loader stored procedure
   ================================ */
CREATE OR ALTER PROCEDURE svo.usp_Load_D_COST_CENTER_SCD2
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
        , @TargetObject sysname = 'svo.D_COST_CENTER';

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
        IF NOT EXISTS (SELECT 1 FROM svo.D_COST_CENTER WHERE COST_CENTER_SK = 0)
        BEGIN
            SET IDENTITY_INSERT svo.D_COST_CENTER ON;

            INSERT INTO svo.D_COST_CENTER (COST_CENTER_SK, COST_CENTER_ID, COST_CENTER_LVL1_CODE, COST_CENTER_LVL1_DESC, COST_CENTER_LVL2_CODE, COST_CENTER_LVL2_DESC, COST_CENTER_LVL3_CODE, COST_CENTER_LVL3_DESC, COST_CENTER_LVL4_CODE, COST_CENTER_LVL4_DESC, COST_CENTER_DISTANCE, COST_CENTER_CATEGORY, COST_CENTER_ENABLED_FLAG, START_DATE_ACTIVE, END_DATE_ACTIVE, CREATED_BY, CREATION_DATE, BZ_LOAD_DATE, SV_LOAD_DATE, EFF_DATE, END_DATE, CRE_DATE, UDT_DATE, CURR_IND)
            VALUES (0, '-1', '-1', 'Unknown', '-1', 'Unknown', '-1', 'Unknown', '-1', 'Unknown', 0, 'Missing', 'N', '0001-01-01', '9999-12-31', 'UNK', CAST(GETDATE() AS date), CAST(GETDATE() AS date), CAST(GETDATE() AS date), CAST(GETDATE() AS date), '9999-12-31', SYSDATETIME(), SYSDATETIME(), 'Y');

            SET IDENTITY_INSERT svo.D_COST_CENTER OFF;
        END

        /* =========================================================
           B) Source snapshot (original SELECT preserved; BZ_LOAD_DATE hardened)
           ========================================================= */
        IF OBJECT_ID('tempdb..#src') IS NOT NULL DROP TABLE #src;

SELECT
    ISNULL(TRIM(lvl4.VALUE),'-1') AS COST_CENTER_ID,
    COALESCE(h1.DEP31PK1VALUE,h1.DEP30PK1VALUE,h1.DEP29PK1VALUE,lvl4.VALUE) AS COST_CENTER_LVL1_CODE,
    COALESCE(lvl1.DESCRIPTION,lvl2.DESCRIPTION,lvl3.DESCRIPTION,lvl4.DESCRIPTION) AS COST_CENTER_LVL1_DESC,
    COALESCE(h1.DEP30PK1VALUE,h1.DEP29PK1VALUE,lvl4.VALUE) AS COST_CENTER_LVL2_CODE,
    COALESCE(lvl2.DESCRIPTION,lvl3.DESCRIPTION,lvl4.DESCRIPTION) AS COST_CENTER_LVL2_DESC,
    COALESCE(h1.DEP29PK1VALUE,lvl4.VALUE) AS COST_CENTER_LVL3_CODE,
    COALESCE(lvl3.DESCRIPTION,lvl4.DESCRIPTION) AS COST_CENTER_LVL3_DESC,
    lvl4.VALUE       AS COST_CENTER_LVL4_CODE,
    lvl4.DESCRIPTION AS COST_CENTER_LVL4_DESC,
    0                  AS COST_CENTER_DISTANCE,
    lvl4.ATTRIBUTECATEGORY AS COST_CENTER_CATEGORY,
    lvl4.ENABLEDFLAG AS COST_CENTER_ENABLED_FLAG,
    ISNULL(lvl4.STARTDATEACTIVE, '0001-01-01') AS START_DATE_ACTIVE,
    ISNULL(lvl4.ENDDATEACTIVE,  '9999-12-31') AS END_DATE_ACTIVE,
    lvl4.CREATEDBY AS CREATED_BY,
    CAST(lvl4.CREATIONDATE AS DATE) AS CREATION_DATE,
    COALESCE(CAST(h1.AddDateTime AS DATE), CAST(GETDATE() AS DATE)) AS BZ_LOAD_DATE,
    CAST(GETDATE() AS DATE) AS SV_LOAD_DATE
INTO #src
FROM src.bzo_GL_SegmentValueHierarchyExtractPVO h1 
INNER JOIN src.bzo_GL_FndTreeAndVersionVO ver1 
    ON ver1.TREEVERSIONID = h1.TREEVERSIONID AND ver1.TREENAME LIKE 'CENTER LAMAR'
INNER JOIN src.bzo_GL_ValueSetValuesPVO lvl1 ON lvl1.VALUE = h1.DEP31PK1VALUE AND lvl1.ATTRIBUTECATEGORY = 'CENTER LAMAR'
INNER JOIN src.bzo_GL_ValueSetValuesPVO lvl2 ON lvl2.VALUE = h1.DEP30PK1VALUE AND lvl2.ATTRIBUTECATEGORY = 'CENTER LAMAR'
INNER JOIN src.bzo_GL_ValueSetValuesPVO lvl3 ON lvl3.VALUE = h1.DEP29PK1VALUE AND lvl3.ATTRIBUTECATEGORY = 'CENTER LAMAR'
RIGHT JOIN src.bzo_GL_ValueSetValuesPVO lvl4 ON lvl4.VALUE = h1.DEP0PK1VALUE AND lvl4.ATTRIBUTECATEGORY = 'CENTER LAMAR'
WHERE lvl4.ATTRIBUTECATEGORY = 'CENTER LAMAR' AND lvl4.SUMMARYFLAG = 'N';
        ;

        /* Remove plug BK from SCD processing */
        DELETE FROM #src WHERE COST_CENTER_ID = '-1';

        /* Build source hash for SCD compare */
        IF COL_LENGTH('tempdb..#src', 'SRC_HASH') IS NULL
            ALTER TABLE #src ADD SRC_HASH varbinary(32) NULL;

        UPDATE s
            SET s.SRC_HASH = HASHBYTES('SHA2_256', CONCAT(COALESCE(CONVERT(nvarchar(500), s.COST_CENTER_ID), N''), N'|', COALESCE(CONVERT(nvarchar(500), s.COST_CENTER_LVL1_CODE), N''), N'|', COALESCE(CONVERT(nvarchar(500), s.COST_CENTER_LVL1_DESC), N''), N'|', COALESCE(CONVERT(nvarchar(500), s.COST_CENTER_LVL2_CODE), N''), N'|', COALESCE(CONVERT(nvarchar(500), s.COST_CENTER_LVL2_DESC), N''), N'|', COALESCE(CONVERT(nvarchar(500), s.COST_CENTER_LVL3_CODE), N''), N'|', COALESCE(CONVERT(nvarchar(500), s.COST_CENTER_LVL3_DESC), N''), N'|', COALESCE(CONVERT(nvarchar(500), s.COST_CENTER_LVL4_CODE), N''), N'|', COALESCE(CONVERT(nvarchar(500), s.COST_CENTER_LVL4_DESC), N''), N'|', CONVERT(nvarchar(10), s.COST_CENTER_DISTANCE), N'|', COALESCE(CONVERT(nvarchar(500), s.COST_CENTER_CATEGORY), N''), N'|', COALESCE(CONVERT(nvarchar(500), s.COST_CENTER_ENABLED_FLAG), N''), N'|', CONVERT(nvarchar(10), s.START_DATE_ACTIVE, 120), N'|', CONVERT(nvarchar(10), s.END_DATE_ACTIVE, 120), N'|', COALESCE(CONVERT(nvarchar(500), s.CREATED_BY), N''), N'|', CONVERT(nvarchar(10), s.CREATION_DATE, 120)))
        FROM #src s;

        /* =========================================================
           C) Current target snapshot + hash
           ========================================================= */
        IF OBJECT_ID('tempdb..#tgt') IS NOT NULL DROP TABLE #tgt;

        SELECT
              t.COST_CENTER_ID
            , HASHBYTES('SHA2_256', CONCAT(COALESCE(CONVERT(nvarchar(500), t.COST_CENTER_ID), N''), N'|', COALESCE(CONVERT(nvarchar(500), t.COST_CENTER_LVL1_CODE), N''), N'|', COALESCE(CONVERT(nvarchar(500), t.COST_CENTER_LVL1_DESC), N''), N'|', COALESCE(CONVERT(nvarchar(500), t.COST_CENTER_LVL2_CODE), N''), N'|', COALESCE(CONVERT(nvarchar(500), t.COST_CENTER_LVL2_DESC), N''), N'|', COALESCE(CONVERT(nvarchar(500), t.COST_CENTER_LVL3_CODE), N''), N'|', COALESCE(CONVERT(nvarchar(500), t.COST_CENTER_LVL3_DESC), N''), N'|', COALESCE(CONVERT(nvarchar(500), t.COST_CENTER_LVL4_CODE), N''), N'|', COALESCE(CONVERT(nvarchar(500), t.COST_CENTER_LVL4_DESC), N''), N'|', CONVERT(nvarchar(10), t.COST_CENTER_DISTANCE), N'|', COALESCE(CONVERT(nvarchar(500), t.COST_CENTER_CATEGORY), N''), N'|', COALESCE(CONVERT(nvarchar(500), t.COST_CENTER_ENABLED_FLAG), N''), N'|', CONVERT(nvarchar(10), t.START_DATE_ACTIVE, 120), N'|', CONVERT(nvarchar(10), t.END_DATE_ACTIVE, 120), N'|', COALESCE(CONVERT(nvarchar(500), t.CREATED_BY), N''), N'|', CONVERT(nvarchar(10), t.CREATION_DATE, 120))) AS TGT_HASH
        INTO #tgt
        FROM svo.D_COST_CENTER t
        WHERE t.CURR_IND = 'Y'
          AND t.END_DATE = @HighDate
          AND t.COST_CENTER_ID <> '-1';

        /* =========================================================
           D) Delta: NEW or CHANGED
           ========================================================= */
        IF OBJECT_ID('tempdb..#delta') IS NOT NULL DROP TABLE #delta;

        SELECT s.*
        INTO #delta
        FROM #src s
        LEFT JOIN #tgt t
            ON t.COST_CENTER_ID = s.COST_CENTER_ID
        WHERE t.COST_CENTER_ID IS NULL
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
        FROM svo.D_COST_CENTER tgt
        INNER JOIN #delta d
            ON d.COST_CENTER_ID = tgt.COST_CENTER_ID
        WHERE tgt.CURR_IND = 'Y'
          AND tgt.END_DATE = @HighDate
          AND tgt.COST_CENTER_ID <> '-1';

        SET @Expired = @@ROWCOUNT;

        /* =========================================================
           F) Insert new current rows
           ========================================================= */
        INSERT INTO svo.D_COST_CENTER
        (
              COST_CENTER_ID, COST_CENTER_LVL1_CODE, COST_CENTER_LVL1_DESC, COST_CENTER_LVL2_CODE, COST_CENTER_LVL2_DESC, COST_CENTER_LVL3_CODE, COST_CENTER_LVL3_DESC, COST_CENTER_LVL4_CODE, COST_CENTER_LVL4_DESC, COST_CENTER_DISTANCE, COST_CENTER_CATEGORY, COST_CENTER_ENABLED_FLAG, START_DATE_ACTIVE, END_DATE_ACTIVE, CREATED_BY, CREATION_DATE, BZ_LOAD_DATE, SV_LOAD_DATE
            , EFF_DATE
            , END_DATE
            , CRE_DATE
            , UDT_DATE
            , CURR_IND
        )
        SELECT
              d.COST_CENTER_ID, d.COST_CENTER_LVL1_CODE, d.COST_CENTER_LVL1_DESC, d.COST_CENTER_LVL2_CODE, d.COST_CENTER_LVL2_DESC, d.COST_CENTER_LVL3_CODE, d.COST_CENTER_LVL3_DESC, d.COST_CENTER_LVL4_CODE, d.COST_CENTER_LVL4_DESC, d.COST_CENTER_DISTANCE, d.COST_CENTER_CATEGORY, d.COST_CENTER_ENABLED_FLAG, d.START_DATE_ACTIVE, d.END_DATE_ACTIVE, d.CREATED_BY, d.CREATION_DATE, d.BZ_LOAD_DATE, d.SV_LOAD_DATE
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

