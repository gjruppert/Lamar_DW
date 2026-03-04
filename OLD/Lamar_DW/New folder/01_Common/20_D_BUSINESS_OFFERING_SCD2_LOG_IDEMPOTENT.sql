USE Oracle_Reporting_P2;
GO

/* =====================================================================
   COA DIM: svo.D_BUSINESS_OFFERING (SCD2)
   Source: synonym-based bzo.* objects (DB independent)
   BK: BUSINESS_OFFERING_ID

   Locked rules applied:
     - Stored proc in svo schema
     - BZ_LOAD_DATE never NULL: COALESCE(CAST(AddDateTime AS date), CAST(GETDATE() AS date))
     - SCD2 fields: EFF_DATE, END_DATE, CRE_DATE, UDT_DATE, CURR_IND (defaults assumed present)
     - ETL run logging (etl.ETL_RUN)
     - Idempotent + transactional

   Note: Existing UNIQUE index on BUSINESS_OFFERING_ID must be dropped for SCD2 history.
         Replaced with filtered unique index for current rows only.
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
   1)d) Drop old UNIQUE index that breaks SCD2
   ============================================== */
IF EXISTS
(
    SELECT 1
    FROM sys.indexes
    WHERE name = 'UX_D_BUSINESS_OFFERING_ID'
      AND object_id = OBJECT_ID('svo.D_BUSINESS_OFFERING')
)
BEGIN
    DROP INDEX UX_D_BUSINESS_OFFERING_ID ON svo.D_BUSINESS_OFFERING;
END
GO

/* ==============================================
   1e) Create filtered unique index for current rows
   ============================================== */
IF NOT EXISTS
(
    SELECT 1
    FROM sys.indexes
    WHERE name = 'UX_D_BUSINESS_OFFERING_ID_CURR'
      AND object_id = OBJECT_ID('svo.D_BUSINESS_OFFERING')
)
BEGIN
    CREATE UNIQUE NONCLUSTERED INDEX UX_D_BUSINESS_OFFERING_ID_CURR
        ON svo.D_BUSINESS_OFFERING(BUSINESS_OFFERING_ID)
        WHERE CURR_IND = 'Y' AND END_DATE = '9999-12-31'
        ON FG_SilverDim;
END
GO

/* ================================
   2) SCD2 loader stored procedure
   ================================ */
CREATE OR ALTER PROCEDURE svo.usp_Load_D_BUSINESS_OFFERING_SCD2
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
        , @TargetObject sysname = 'svo.D_BUSINESS_OFFERING';

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
        IF NOT EXISTS (SELECT 1 FROM svo.D_BUSINESS_OFFERING WHERE BUSINESS_OFFERING_SK = 0)
        BEGIN
            SET IDENTITY_INSERT svo.D_BUSINESS_OFFERING ON;

            INSERT INTO svo.D_BUSINESS_OFFERING
            (
                  BUSINESS_OFFERING_SK
                , BUSINESS_OFFERING_ID
                , BUSINESS_OFFERING_LVL1_CODE, BUSINESS_OFFERING_LVL1_DESC
                , BUSINESS_OFFERING_LVL2_CODE, BUSINESS_OFFERING_LVL2_DESC
                , BUSINESS_OFFERING_LVL3_CODE, BUSINESS_OFFERING_LVL3_DESC
                , BUSINESS_OFFERING_LVL4_CODE, BUSINESS_OFFERING_LVL4_DESC
                , BUSINESS_OFFERING_LVL5_CODE, BUSINESS_OFFERING_LVL5_DESC
                , BUSINESS_OFFERING_DISTANCE
                , BUSINESS_OFFERING_CATEGORY
                , BUSINESS_OFFERING_ENABLED_FLAG
                , START_DATE_ACTIVE
                , END_DATE_ACTIVE
                , CREATED_BY
                , CREATION_DATE
                , EFF_DATE
                , END_DATE
                , CRE_DATE
                , UDT_DATE
                , CURR_IND
                , BZ_LOAD_DATE
                , SV_LOAD_DATE
            )
            VALUES
            (
                  0
                , '-1'
                , '-1','Unknown'
                , '-1','Unknown'
                , '-1','Unknown'
                , '-1','Unknown'
                , '-1','Unknown'
                , 0
                , 'Missing'
                , 'N'
                , '0001-01-01'
                , '9999-12-31'
                , 'UNK'
                , CAST(GETDATE() AS date)
                , CAST(GETDATE() AS date)
                , '9999-12-31'
                , SYSDATETIME()
                , SYSDATETIME()
                , 'Y'
                , CAST(GETDATE() AS date)
                , CAST(GETDATE() AS date)
            );

            SET IDENTITY_INSERT svo.D_BUSINESS_OFFERING OFF;
        END

        /* =========================================================
           B) Source snapshot (your existing SELECT preserved, with
              BZ_LOAD_DATE hardened to never be NULL)
           ========================================================= */
        IF OBJECT_ID('tempdb..#src') IS NOT NULL DROP TABLE #src;

SELECT
    ISNULL(TRIM(lvl5.VALUE),'-1') AS BUSINESS_OFFERING_ID,
    COALESCE(h1.DEP31PK1VALUE,h1.DEP30PK1VALUE,h1.DEP29PK1VALUE,h1.DEP28PK1VALUE,lvl5.VALUE) AS BUSINESS_OFFERING_LVL1_CODE,
    COALESCE(lvl1.DESCRIPTION,lvl2.DESCRIPTION,lvl3.DESCRIPTION,lvl4.DESCRIPTION,lvl5.DESCRIPTION) AS BUSINESS_OFFERING_LVL1_DESC,
    COALESCE(h1.DEP30PK1VALUE,h1.DEP29PK1VALUE,h1.DEP28PK1VALUE,lvl5.VALUE) AS BUSINESS_OFFERING_LVL2_CODE,
    COALESCE(lvl2.DESCRIPTION,lvl3.DESCRIPTION,lvl4.DESCRIPTION,lvl5.DESCRIPTION) AS BUSINESS_OFFERING_LVL2_DESC,
    COALESCE(h1.DEP29PK1VALUE,h1.DEP28PK1VALUE,lvl5.VALUE) AS BUSINESS_OFFERING_LVL3_CODE,
    COALESCE(lvl3.DESCRIPTION,lvl4.DESCRIPTION,lvl5.DESCRIPTION) AS BUSINESS_OFFERING_LVL3_DESC,
    COALESCE(h1.DEP28PK1VALUE,lvl5.VALUE) AS BUSINESS_OFFERING_LVL4_CODE,
    COALESCE(lvl4.DESCRIPTION,lvl5.DESCRIPTION) AS BUSINESS_OFFERING_LVL4_DESC,
    lvl5.VALUE       AS BUSINESS_OFFERING_LVL5_CODE,
    lvl5.DESCRIPTION AS BUSINESS_OFFERING_LVL5_DESC,
    0                  AS BUSINESS_OFFERING_DISTANCE,
    lvl5.ATTRIBUTECATEGORY AS BUSINESS_OFFERING_CATEGORY,
    lvl5.ENABLEDFLAG AS BUSINESS_OFFERING_ENABLED_FLAG,
    ISNULL(lvl5.STARTDATEACTIVE, '0001-01-01') AS START_DATE_ACTIVE,
    ISNULL(lvl5.ENDDATEACTIVE,  '9999-12-31') AS END_DATE_ACTIVE,
    lvl5.CREATEDBY AS CREATED_BY,
    CAST(lvl5.CREATIONDATE AS DATE) AS CREATION_DATE,
    COALESCE(CAST(h1.AddDateTime AS DATE), CAST(GETDATE() AS DATE)) AS BZ_LOAD_DATE,
    CAST(GETDATE() AS DATE) AS SV_LOAD_DATE
INTO #src
FROM src.bzo_GL_SegmentValueHierarchyExtractPVO h1 
INNER JOIN src.bzo_GL_FndTreeAndVersionVO ver1 
    ON ver1.TREEVERSIONID = h1.TREEVERSIONID AND ver1.TREENAME LIKE 'BUSINESS OFFERING LAMAR'
INNER JOIN src.bzo_GL_ValueSetValuesPVO lvl1 ON lvl1.VALUE = h1.DEP31PK1VALUE AND lvl1.ATTRIBUTECATEGORY = 'BUSINESS OFFERING LAMAR'
INNER JOIN src.bzo_GL_ValueSetValuesPVO lvl2 ON lvl2.VALUE = h1.DEP30PK1VALUE AND lvl2.ATTRIBUTECATEGORY = 'BUSINESS OFFERING LAMAR'
INNER JOIN src.bzo_GL_ValueSetValuesPVO lvl3 ON lvl3.VALUE = h1.DEP29PK1VALUE AND lvl3.ATTRIBUTECATEGORY = 'BUSINESS OFFERING LAMAR'
INNER JOIN src.bzo_GL_ValueSetValuesPVO lvl4 ON lvl4.VALUE = h1.DEP28PK1VALUE AND lvl4.ATTRIBUTECATEGORY = 'BUSINESS OFFERING LAMAR'
RIGHT JOIN src.bzo_GL_ValueSetValuesPVO lvl5 ON lvl5.VALUE = h1.DEP0PK1VALUE AND lvl5.ATTRIBUTECATEGORY = 'BUSINESS OFFERING LAMAR'
WHERE lvl5.ATTRIBUTECATEGORY = 'BUSINESS OFFERING LAMAR' AND lvl5.SUMMARYFLAG = 'N'
--and ISNULL(TRIM(lvl5.VALUE),'-1') = '214'
;
        /* Remove plug BK from SCD processing */
        DELETE FROM #src WHERE BUSINESS_OFFERING_ID = '-1';

        /* Add deterministic source hash for SCD compare */
        ALTER TABLE #src ADD SRC_HASH varbinary(32) NULL;

        UPDATE s
            SET s.SRC_HASH =
                HASHBYTES('SHA2_256', CONCAT(
                      COALESCE(CONVERT(nvarchar(200), s.BUSINESS_OFFERING_ID), N''), N'|'
                    , COALESCE(CONVERT(nvarchar(200), s.BUSINESS_OFFERING_LVL1_CODE), N''), N'|'
                    , COALESCE(CONVERT(nvarchar(500), s.BUSINESS_OFFERING_LVL1_DESC), N''), N'|'
                    , COALESCE(CONVERT(nvarchar(200), s.BUSINESS_OFFERING_LVL2_CODE), N''), N'|'
                    , COALESCE(CONVERT(nvarchar(500), s.BUSINESS_OFFERING_LVL2_DESC), N''), N'|'
                    , COALESCE(CONVERT(nvarchar(200), s.BUSINESS_OFFERING_LVL3_CODE), N''), N'|'
                    , COALESCE(CONVERT(nvarchar(500), s.BUSINESS_OFFERING_LVL3_DESC), N''), N'|'
                    , COALESCE(CONVERT(nvarchar(200), s.BUSINESS_OFFERING_LVL4_CODE), N''), N'|'
                    , COALESCE(CONVERT(nvarchar(500), s.BUSINESS_OFFERING_LVL4_DESC), N''), N'|'
                    , COALESCE(CONVERT(nvarchar(200), s.BUSINESS_OFFERING_LVL5_CODE), N''), N'|'
                    , COALESCE(CONVERT(nvarchar(500), s.BUSINESS_OFFERING_LVL5_DESC), N''), N'|'
                    , COALESCE(CONVERT(nvarchar(10),  s.BUSINESS_OFFERING_DISTANCE), N''), N'|'
                    , COALESCE(CONVERT(nvarchar(60),  s.BUSINESS_OFFERING_CATEGORY), N''), N'|'
                    , COALESCE(CONVERT(nvarchar(10),  s.BUSINESS_OFFERING_ENABLED_FLAG), N''), N'|'
                    , CONVERT(nvarchar(10), s.START_DATE_ACTIVE, 120), N'|'
                    , CONVERT(nvarchar(10), s.END_DATE_ACTIVE, 120), N'|'
                    , COALESCE(CONVERT(nvarchar(64),  s.CREATED_BY), N''), N'|'
                    , CONVERT(nvarchar(10), s.CREATION_DATE, 120)
                ))
        FROM #src s;

        /* =========================================================
           C) Current target snapshot + hash
           ========================================================= */
        IF OBJECT_ID('tempdb..#tgt') IS NOT NULL DROP TABLE #tgt;

        SELECT
              t.BUSINESS_OFFERING_ID
            , HASHBYTES('SHA2_256', CONCAT(
                  COALESCE(CONVERT(nvarchar(200), t.BUSINESS_OFFERING_ID), N''), N'|'
                , COALESCE(CONVERT(nvarchar(200), t.BUSINESS_OFFERING_LVL1_CODE), N''), N'|'
                , COALESCE(CONVERT(nvarchar(500), t.BUSINESS_OFFERING_LVL1_DESC), N''), N'|'
                , COALESCE(CONVERT(nvarchar(200), t.BUSINESS_OFFERING_LVL2_CODE), N''), N'|'
                , COALESCE(CONVERT(nvarchar(500), t.BUSINESS_OFFERING_LVL2_DESC), N''), N'|'
                , COALESCE(CONVERT(nvarchar(200), t.BUSINESS_OFFERING_LVL3_CODE), N''), N'|'
                , COALESCE(CONVERT(nvarchar(500), t.BUSINESS_OFFERING_LVL3_DESC), N''), N'|'
                , COALESCE(CONVERT(nvarchar(200), t.BUSINESS_OFFERING_LVL4_CODE), N''), N'|'
                , COALESCE(CONVERT(nvarchar(500), t.BUSINESS_OFFERING_LVL4_DESC), N''), N'|'
                , COALESCE(CONVERT(nvarchar(200), t.BUSINESS_OFFERING_LVL5_CODE), N''), N'|'
                , COALESCE(CONVERT(nvarchar(500), t.BUSINESS_OFFERING_LVL5_DESC), N''), N'|'
                , COALESCE(CONVERT(nvarchar(10),  t.BUSINESS_OFFERING_DISTANCE), N''), N'|'
                , COALESCE(CONVERT(nvarchar(60),  t.BUSINESS_OFFERING_CATEGORY), N''), N'|'
                , COALESCE(CONVERT(nvarchar(10),  t.BUSINESS_OFFERING_ENABLED_FLAG), N''), N'|'
                , CONVERT(nvarchar(10), t.START_DATE_ACTIVE, 120), N'|'
                , CONVERT(nvarchar(10), t.END_DATE_ACTIVE, 120), N'|'
                , COALESCE(CONVERT(nvarchar(64),  t.CREATED_BY), N''), N'|'
                , CONVERT(nvarchar(10), t.CREATION_DATE, 120)
            )) AS TGT_HASH
        INTO #tgt
        FROM svo.D_BUSINESS_OFFERING t
        WHERE t.CURR_IND = 'Y'
          AND t.END_DATE = @HighDate
          AND t.BUSINESS_OFFERING_ID <> '-1';

        /* =========================================================
           D) Delta: NEW or CHANGED
           ========================================================= */
        IF OBJECT_ID('tempdb..#delta') IS NOT NULL DROP TABLE #delta;

        SELECT s.*
        INTO #delta
        FROM #src s
        LEFT JOIN #tgt t
            ON t.BUSINESS_OFFERING_ID = s.BUSINESS_OFFERING_ID
        WHERE t.BUSINESS_OFFERING_ID IS NULL
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
        FROM svo.D_BUSINESS_OFFERING tgt
        INNER JOIN #delta d
            ON d.BUSINESS_OFFERING_ID = tgt.BUSINESS_OFFERING_ID
        WHERE tgt.CURR_IND = 'Y'
          AND tgt.END_DATE = @HighDate
          AND tgt.BUSINESS_OFFERING_ID <> '-1';

        SET @Expired = @@ROWCOUNT;

        /* =========================================================
           F) Insert new current rows
           ========================================================= */
        INSERT INTO svo.D_BUSINESS_OFFERING
        (
              BUSINESS_OFFERING_ID
            , BUSINESS_OFFERING_LVL1_CODE, BUSINESS_OFFERING_LVL1_DESC
            , BUSINESS_OFFERING_LVL2_CODE, BUSINESS_OFFERING_LVL2_DESC
            , BUSINESS_OFFERING_LVL3_CODE, BUSINESS_OFFERING_LVL3_DESC
            , BUSINESS_OFFERING_LVL4_CODE, BUSINESS_OFFERING_LVL4_DESC
            , BUSINESS_OFFERING_LVL5_CODE, BUSINESS_OFFERING_LVL5_DESC
            , BUSINESS_OFFERING_DISTANCE
            , BUSINESS_OFFERING_CATEGORY
            , BUSINESS_OFFERING_ENABLED_FLAG
            , START_DATE_ACTIVE
            , END_DATE_ACTIVE
            , CREATED_BY
            , CREATION_DATE
            , EFF_DATE
            , END_DATE
            , CRE_DATE
            , UDT_DATE
            , CURR_IND
            , BZ_LOAD_DATE
            , SV_LOAD_DATE
        )
        SELECT
              d.BUSINESS_OFFERING_ID
            , d.BUSINESS_OFFERING_LVL1_CODE, d.BUSINESS_OFFERING_LVL1_DESC
            , d.BUSINESS_OFFERING_LVL2_CODE, d.BUSINESS_OFFERING_LVL2_DESC
            , d.BUSINESS_OFFERING_LVL3_CODE, d.BUSINESS_OFFERING_LVL3_DESC
            , d.BUSINESS_OFFERING_LVL4_CODE, d.BUSINESS_OFFERING_LVL4_DESC
            , d.BUSINESS_OFFERING_LVL5_CODE, d.BUSINESS_OFFERING_LVL5_DESC
            , d.BUSINESS_OFFERING_DISTANCE
            , d.BUSINESS_OFFERING_CATEGORY
            , d.BUSINESS_OFFERING_ENABLED_FLAG
            , d.START_DATE_ACTIVE
            , d.END_DATE_ACTIVE
            , d.CREATED_BY
            , d.CREATION_DATE
            , @AsOfDate
            , @HighDate
            , SYSDATETIME()
            , SYSDATETIME()
            , 'Y'
            , COALESCE(d.BZ_LOAD_DATE, CAST(GETDATE() AS date))
            , CAST(GETDATE() AS date)
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

