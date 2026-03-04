USE [Oracle_Reporting_P2];
GO

/* =====================================================================
   DIM: svo.D_LEDGER (Hybrid Type1/Type2 SCD)
   BK: LEDGER_ID

   Source (synonym-based / DB independent):
     - src.bzo_GL_LedgerExtractPVO (as in original script)

   Hybrid approach (default):
     - Type 1 (in-place update on current row): LEDGER_NAME, LEDGER_DESCRIPTION, LEDGER_SHORT_NAME
     - Type 2 (versioned): all other non-audit attributes in the target list

   Locked rules applied:
     - Stored proc in svo schema
     - BZ_LOAD_DATE never NULL: COALESCE(CAST(AddDateTime AS date), CAST(GETDATE() AS date))
     - SCD2 fields: EFF_DATE, END_DATE, CRE_DATE, UDT_DATE, CURR_IND
     - ETL run logging (etl.ETL_RUN)
     - Idempotent + transactional

   Index:
     - Drops legacy unique index UX_D_LEDGER_ID if present (breaks SCD2 history)
     - Creates filtered unique index UX_D_LEDGER_ID_CURR for current rows only
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

/* ==============================================
   1) Ensure standard SCD columns exist (add only if needed)
   ============================================== */
IF COL_LENGTH('svo.D_LEDGER', 'EFF_DATE') IS NULL
    ALTER TABLE svo.D_LEDGER ADD EFF_DATE date NOT NULL
        CONSTRAINT DF_D_LEDGER_EFF_DATE DEFAULT (CAST('0001-01-01' AS date));
GO
IF COL_LENGTH('svo.D_LEDGER', 'END_DATE') IS NULL
    ALTER TABLE svo.D_LEDGER ADD END_DATE date NOT NULL
        CONSTRAINT DF_D_LEDGER_END_DATE DEFAULT (CAST('9999-12-31' AS date));
GO
IF COL_LENGTH('svo.D_LEDGER', 'CRE_DATE') IS NULL
    ALTER TABLE svo.D_LEDGER ADD CRE_DATE datetime2(0) NOT NULL
        CONSTRAINT DF_D_LEDGER_CRE_DATE DEFAULT (SYSDATETIME());
GO
IF COL_LENGTH('svo.D_LEDGER', 'UDT_DATE') IS NULL
    ALTER TABLE svo.D_LEDGER ADD UDT_DATE datetime2(0) NOT NULL
        CONSTRAINT DF_D_LEDGER_UDT_DATE DEFAULT (SYSDATETIME());
GO
IF COL_LENGTH('svo.D_LEDGER', 'CURR_IND') IS NULL
    ALTER TABLE svo.D_LEDGER ADD CURR_IND char(1) NOT NULL
        CONSTRAINT DF_D_LEDGER_CURR_IND DEFAULT ('Y');
GO

/* ==============================================
   2) Replace legacy unique index that breaks SCD2
   ============================================== */
IF EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'UX_D_LEDGER_ID' AND object_id = OBJECT_ID('svo.D_LEDGER'))
    DROP INDEX UX_D_LEDGER_ID ON svo.D_LEDGER;
GO

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'UX_D_LEDGER_ID_CURR' AND object_id = OBJECT_ID('svo.D_LEDGER'))
BEGIN
    CREATE UNIQUE NONCLUSTERED INDEX UX_D_LEDGER_ID_CURR
        ON svo.D_LEDGER(LEDGER_ID)
        WHERE CURR_IND = 'Y' AND END_DATE = '9999-12-31'
        ON FG_SilverDim;
END
GO

/* ================================
   3) Hybrid SCD loader stored proc
   ================================ */
CREATE OR ALTER PROCEDURE svo.usp_Load_D_LEDGER_SCD2
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
        , @TargetObject sysname = 'svo.D_LEDGER';

    DECLARE
          @Inserted   int = 0
        , @Expired    int = 0
        , @UpdatedT1  int = 0;

    INSERT INTO etl.ETL_RUN (PROC_NAME, TARGET_OBJECT, ASOF_DATE, START_DTTM, STATUS)
    VALUES (@ProcName, @TargetObject, @AsOfDate, SYSDATETIME(), 'STARTED');

    SET @RunId = SCOPE_IDENTITY();

    BEGIN TRY
        BEGIN TRAN;

        /* Plug row (SK=0, BK=-1) */
        IF NOT EXISTS (SELECT 1 FROM svo.D_LEDGER WHERE LEDGER_SK = 0)
        BEGIN
            SET IDENTITY_INSERT svo.D_LEDGER ON;

            INSERT INTO svo.D_LEDGER
            (
                  LEDGER_SK
                , LEDGER_ID, LEDGER_NAME, LEDGER_ACCOUNTED_PERIOD_TYPE, LEDGER_CHART_OF_ACCOUNTS_ID, LEDGER_CURRENCY_CODE, LEDGER_DESCRIPTION, LEDGER_CATEGORY_CODE, LEDGER_PERIOD_SET_NAME, LEDGER_SHORT_NAME, BZ_LOAD_DATE, SV_LOAD_DATE
                , EFF_DATE
                , END_DATE
                , CRE_DATE
                , UDT_DATE
                , CURR_IND
            )
            VALUES
            (
                  0
                , -1, 'Unknown', 'Unknown', -1, 'U', 'Unknown', 'U', 'Unknown', 'Unknown', CAST(GETDATE() AS date), CAST(GETDATE() AS date)
                , CAST(GETDATE() AS date)
                , @HighDate
                , SYSDATETIME()
                , SYSDATETIME()
                , 'Y'
            );

            SET IDENTITY_INSERT svo.D_LEDGER OFF;
        END

        /* Source snapshot */
        IF OBJECT_ID('tempdb..#src') IS NOT NULL DROP TABLE #src;
SELECT
      LedgerLedgerId AS LEDGER_ID,
      LedgerName AS LEDGER_NAME,
      LedgerAccountedPeriodType AS LEDGER_ACCOUNTED_PERIOD_TYPE,
      LedgerChartOfAccountsId AS LEDGER_CHART_OF_ACCOUNTS_ID,
      LedgerCurrencyCode AS LEDGER_CURRENCY_CODE,
      LedgerDescription AS LEDGER_DESCRIPTION,
      LedgerLedgerCategoryCode AS LEDGER_CATEGORY_CODE,
      LedgerPeriodSetName AS LEDGER_PERIOD_SET_NAME,
      LedgerShortName AS LEDGER_SHORT_NAME,
      COALESCE(CAST(AddDateTime AS date), CAST(GETDATE() AS date)) AS BZ_LOAD_DATE,
      CAST(GETDATE() AS date) AS SV_LOAD_DATE
INTO #src
FROM src.bzo_GL_LedgerExtractPVO;

        DELETE FROM #src WHERE LEDGER_ID IS NULL OR LEDGER_ID = -1;

        ALTER TABLE #src ADD HASH_T2 varbinary(32) NULL;

        UPDATE s
            SET s.HASH_T2 = HASHBYTES('SHA2_256', CONCAT(COALESCE(CONVERT(nvarchar(500), s.LEDGER_ID), N''), N'|', COALESCE(CONVERT(nvarchar(500), s.LEDGER_ACCOUNTED_PERIOD_TYPE), N''), N'|', COALESCE(CONVERT(nvarchar(500), s.LEDGER_CHART_OF_ACCOUNTS_ID), N''), N'|', COALESCE(CONVERT(nvarchar(500), s.LEDGER_CURRENCY_CODE), N''), N'|', COALESCE(CONVERT(nvarchar(500), s.LEDGER_CATEGORY_CODE), N''), N'|', COALESCE(CONVERT(nvarchar(500), s.LEDGER_PERIOD_SET_NAME), N'')))
        FROM #src s;

        /* Current target snapshot */
        IF OBJECT_ID('tempdb..#tgt') IS NOT NULL DROP TABLE #tgt;

        SELECT
              t.LEDGER_ID
            , t.LEDGER_NAME
            , t.LEDGER_DESCRIPTION
            , t.LEDGER_SHORT_NAME
            , HASHBYTES('SHA2_256', CONCAT(COALESCE(CONVERT(nvarchar(500), t.LEDGER_ID), N''), N'|', COALESCE(CONVERT(nvarchar(500), t.LEDGER_ACCOUNTED_PERIOD_TYPE), N''), N'|', COALESCE(CONVERT(nvarchar(500), t.LEDGER_CHART_OF_ACCOUNTS_ID), N''), N'|', COALESCE(CONVERT(nvarchar(500), t.LEDGER_CURRENCY_CODE), N''), N'|', COALESCE(CONVERT(nvarchar(500), t.LEDGER_CATEGORY_CODE), N''), N'|', COALESCE(CONVERT(nvarchar(500), t.LEDGER_PERIOD_SET_NAME), N''))) AS HASH_T2
        INTO #tgt
        FROM svo.D_LEDGER t
        WHERE t.CURR_IND = 'Y'
          AND t.END_DATE = @HighDate
          AND t.LEDGER_ID <> -1;

        /* Type 1 updates (only when Type2 unchanged) */
        UPDATE tgt
            SET
                  tgt.LEDGER_NAME        = src.LEDGER_NAME
                , tgt.LEDGER_DESCRIPTION = src.LEDGER_DESCRIPTION
                , tgt.LEDGER_SHORT_NAME  = src.LEDGER_SHORT_NAME
                , tgt.UDT_DATE           = SYSDATETIME()
                , tgt.SV_LOAD_DATE       = CAST(GETDATE() AS date)
                , tgt.BZ_LOAD_DATE       = COALESCE(tgt.BZ_LOAD_DATE, src.BZ_LOAD_DATE, CAST(GETDATE() AS date))
        FROM svo.D_LEDGER tgt
        INNER JOIN #tgt cur
            ON cur.LEDGER_ID = tgt.LEDGER_ID
        INNER JOIN #src src
            ON src.LEDGER_ID = cur.LEDGER_ID
        WHERE tgt.CURR_IND = 'Y'
          AND tgt.END_DATE = @HighDate
          AND cur.HASH_T2 = src.HASH_T2
          AND (
                ISNULL(tgt.LEDGER_NAME,'')        <> ISNULL(src.LEDGER_NAME,'')
             OR ISNULL(tgt.LEDGER_DESCRIPTION,'') <> ISNULL(src.LEDGER_DESCRIPTION,'')
             OR ISNULL(tgt.LEDGER_SHORT_NAME,'')  <> ISNULL(src.LEDGER_SHORT_NAME,'')
          );

        SET @UpdatedT1 = @@ROWCOUNT;

        /* Delta for Type2 (new or changed) */
        IF OBJECT_ID('tempdb..#delta_t2') IS NOT NULL DROP TABLE #delta_t2;

        SELECT s.*
        INTO #delta_t2
        FROM #src s
        LEFT JOIN #tgt t
            ON t.LEDGER_ID = s.LEDGER_ID
        WHERE t.LEDGER_ID IS NULL
           OR t.HASH_T2 <> s.HASH_T2;

        /* Expire current rows for changed keys */
        UPDATE tgt
            SET
                  tgt.END_DATE     = DATEADD(day, -1, @AsOfDate)
                , tgt.CURR_IND     = 'N'
                , tgt.UDT_DATE     = SYSDATETIME()
                , tgt.SV_LOAD_DATE = CAST(GETDATE() AS date)
        FROM svo.D_LEDGER tgt
        INNER JOIN #delta_t2 d
            ON d.LEDGER_ID = tgt.LEDGER_ID
        WHERE tgt.CURR_IND = 'Y'
          AND tgt.END_DATE = @HighDate
          AND tgt.LEDGER_ID <> -1
          AND EXISTS (SELECT 1 FROM #tgt cur WHERE cur.LEDGER_ID = d.LEDGER_ID);

        SET @Expired = @@ROWCOUNT;

        /* Insert new current rows */
        INSERT INTO svo.D_LEDGER
        (
              LEDGER_ID, LEDGER_NAME, LEDGER_ACCOUNTED_PERIOD_TYPE, LEDGER_CHART_OF_ACCOUNTS_ID, LEDGER_CURRENCY_CODE, LEDGER_DESCRIPTION, LEDGER_CATEGORY_CODE, LEDGER_PERIOD_SET_NAME, LEDGER_SHORT_NAME, BZ_LOAD_DATE, SV_LOAD_DATE
            , EFF_DATE
            , END_DATE
            , CRE_DATE
            , UDT_DATE
            , CURR_IND
        )
        SELECT
              d.LEDGER_ID, d.LEDGER_NAME, d.LEDGER_ACCOUNTED_PERIOD_TYPE, d.LEDGER_CHART_OF_ACCOUNTS_ID, d.LEDGER_CURRENCY_CODE, d.LEDGER_DESCRIPTION, d.LEDGER_CATEGORY_CODE, d.LEDGER_PERIOD_SET_NAME, d.LEDGER_SHORT_NAME, d.BZ_LOAD_DATE, d.SV_LOAD_DATE
            , @AsOfDate
            , @HighDate
            , SYSDATETIME()
            , SYSDATETIME()
            , 'Y'
        FROM #delta_t2 d;

        SET @Inserted = @@ROWCOUNT;

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

