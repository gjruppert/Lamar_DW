USE [Oracle_Reporting_P2];
GO

/* =====================================================================
   DIM: svo.D_CUSTOMER_ACCOUNT_SITE (Hybrid Type1/Type2 SCD)
   BK: CUSTOMER_SITE (CustAcctSiteId)

   Source (synonym-based / DB independent):
     - src.bzo_AR_CustomerAccountSiteExtractPVO

   Hybrid approach (sane default for this table):
     - Type 1 (in-place update on current row): LANGUAGE
     - Type 2 (versioned): CUSTOMER_ACCOUNT, PARTY_SITE, STATUS, START_DATE_ACTIVE, END_DATE_ACTIVE

   Locked rules applied:
     - Stored proc in svo schema
     - BZ_LOAD_DATE never NULL: COALESCE(CAST(AddDateTime AS date), CAST(GETDATE() AS date))
     - SCD2 fields: EFF_DATE, END_DATE, CRE_DATE, UDT_DATE, CURR_IND
     - ETL run logging (etl.ETL_RUN)
     - Idempotent + transactional

   IMPORTANT:
     - Your existing table script uses columns START_DATE / END_DATE for business-active dates.
       END_DATE collides with the standard SCD2 END_DATE column.
     - This script will rename:
         START_DATE -> START_DATE_ACTIVE
         END_DATE   -> END_DATE_ACTIVE
       (guarded; only if the ACTIVE columns do not already exist)
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
        , STATUS          varchar(20)          NOT NULL  -- STARTED/SUCCESS/FAILED
        , ROW_INSERTED    int                  NULL
        , ROW_EXPIRED     int                  NULL
        , ROW_UPDATED_T1  int                  NULL
        , ERROR_MESSAGE   nvarchar(4000)       NULL
    );
END
GO

/* Ensure ETL_RUN has ROW_UPDATED_T1 (older runs may not) */
IF COL_LENGTH('etl.ETL_RUN', 'ROW_UPDATED_T1') IS NULL
    ALTER TABLE etl.ETL_RUN ADD ROW_UPDATED_T1 int NULL;
GO

/* ==============================================
   1) Resolve START_DATE/END_DATE naming collision
   ============================================== */
IF COL_LENGTH('svo.D_CUSTOMER_ACCOUNT_SITE', 'START_DATE_ACTIVE') IS NULL
   AND COL_LENGTH('svo.D_CUSTOMER_ACCOUNT_SITE', 'START_DATE') IS NOT NULL
BEGIN
    EXEC sp_rename 'svo.D_CUSTOMER_ACCOUNT_SITE.START_DATE', 'START_DATE_ACTIVE', 'COLUMN';
END
GO

IF COL_LENGTH('svo.D_CUSTOMER_ACCOUNT_SITE', 'END_DATE_ACTIVE') IS NULL
   AND COL_LENGTH('svo.D_CUSTOMER_ACCOUNT_SITE', 'END_DATE') IS NOT NULL
BEGIN
    EXEC sp_rename 'svo.D_CUSTOMER_ACCOUNT_SITE.END_DATE', 'END_DATE_ACTIVE', 'COLUMN';
END
GO

/* ==============================================
   2) Ensure standard SCD columns exist (add only if needed)
   ============================================== */
IF COL_LENGTH('svo.D_CUSTOMER_ACCOUNT_SITE', 'EFF_DATE') IS NULL
    ALTER TABLE svo.D_CUSTOMER_ACCOUNT_SITE ADD EFF_DATE date NOT NULL
        CONSTRAINT DF_D_CUSTOMER_ACCOUNT_SITE_EFF_DATE DEFAULT (CAST('0001-01-01' AS date));
GO
IF COL_LENGTH('svo.D_CUSTOMER_ACCOUNT_SITE', 'END_DATE') IS NULL
    ALTER TABLE svo.D_CUSTOMER_ACCOUNT_SITE ADD END_DATE date NOT NULL
        CONSTRAINT DF_D_CUSTOMER_ACCOUNT_SITE_END_DATE DEFAULT (CAST('9999-12-31' AS date));
GO
IF COL_LENGTH('svo.D_CUSTOMER_ACCOUNT_SITE', 'CRE_DATE') IS NULL
    ALTER TABLE svo.D_CUSTOMER_ACCOUNT_SITE ADD CRE_DATE datetime2(0) NOT NULL
        CONSTRAINT DF_D_CUSTOMER_ACCOUNT_SITE_CRE_DATE DEFAULT (SYSDATETIME());
GO
IF COL_LENGTH('svo.D_CUSTOMER_ACCOUNT_SITE', 'UDT_DATE') IS NULL
    ALTER TABLE svo.D_CUSTOMER_ACCOUNT_SITE ADD UDT_DATE datetime2(0) NOT NULL
        CONSTRAINT DF_D_CUSTOMER_ACCOUNT_SITE_UDT_DATE DEFAULT (SYSDATETIME());
GO
IF COL_LENGTH('svo.D_CUSTOMER_ACCOUNT_SITE', 'CURR_IND') IS NULL
    ALTER TABLE svo.D_CUSTOMER_ACCOUNT_SITE ADD CURR_IND char(1) NOT NULL
        CONSTRAINT DF_D_CUSTOMER_ACCOUNT_SITE_CURR_IND DEFAULT ('Y');
GO

/* ==============================================
   3) Drop legacy UNIQUE index that breaks SCD2
   ============================================== */
IF EXISTS
(
    SELECT 1
    FROM sys.indexes
    WHERE name = 'UX_D_CUSTOMER_ACCOUNT_SITE'
      AND object_id = OBJECT_ID('svo.D_CUSTOMER_ACCOUNT_SITE')
)
BEGIN
    DROP INDEX UX_D_CUSTOMER_ACCOUNT_SITE ON svo.D_CUSTOMER_ACCOUNT_SITE;
END
GO

/* ==============================================
   4) Create filtered unique index for current rows (BK)
   ============================================== */
IF NOT EXISTS
(
    SELECT 1
    FROM sys.indexes
    WHERE name = 'UX_D_CUSTOMER_ACCOUNT_SITE_CURR'
      AND object_id = OBJECT_ID('svo.D_CUSTOMER_ACCOUNT_SITE')
)
BEGIN
    CREATE UNIQUE NONCLUSTERED INDEX UX_D_CUSTOMER_ACCOUNT_SITE_CURR
        ON svo.D_CUSTOMER_ACCOUNT_SITE(CUSTOMER_SITE)
        WHERE CURR_IND = 'Y' AND END_DATE = '9999-12-31'
        ON FG_SilverDim;
END
GO

/* ================================
   5) Hybrid SCD loader stored proc
   ================================ */
CREATE OR ALTER PROCEDURE svo.usp_Load_D_CUSTOMER_ACCOUNT_SITE_SCD2
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
        , @TargetObject sysname = 'svo.D_CUSTOMER_ACCOUNT_SITE';

    DECLARE
          @Inserted   int = 0
        , @Expired    int = 0
        , @UpdatedT1  int = 0;

    INSERT INTO etl.ETL_RUN (PROC_NAME, TARGET_OBJECT, ASOF_DATE, START_DTTM, STATUS)
    VALUES (@ProcName, @TargetObject, @AsOfDate, SYSDATETIME(), 'STARTED');

    SET @RunId = SCOPE_IDENTITY();

    BEGIN TRY
        BEGIN TRAN;

        /* =========================================================
           A) Plug row (SK=0, BK=-1) - stable, not SCD-managed
           ========================================================= */
        IF NOT EXISTS (SELECT 1 FROM svo.D_CUSTOMER_ACCOUNT_SITE WHERE CUSTOMER_SITE_SK = 0)
        BEGIN
            SET IDENTITY_INSERT svo.D_CUSTOMER_ACCOUNT_SITE ON;

            INSERT INTO svo.D_CUSTOMER_ACCOUNT_SITE
            (
                  CUSTOMER_SITE_SK
                , CUSTOMER_SITE
                , CUSTOMER_ACCOUNT
                , PARTY_SITE
                , STATUS
                , LANGUAGE
                , START_DATE_ACTIVE
                , END_DATE_ACTIVE
                , BZ_LOAD_DATE
                , SV_LOAD_DATE
                , EFF_DATE
                , END_DATE
                , CRE_DATE
                , UDT_DATE
                , CURR_IND
            )
            VALUES
            (
                  0
                , -1
                , -1
                , -1
                , 'U'
                , NULL
                , NULL
                , NULL
                , CAST(GETDATE() AS date)
                , CAST(GETDATE() AS date)
                , CAST(GETDATE() AS date)
                , @HighDate
                , SYSDATETIME()
                , SYSDATETIME()
                , 'Y'
            );

            SET IDENTITY_INSERT svo.D_CUSTOMER_ACCOUNT_SITE OFF;
        END

        /* =========================================================
           B) Source snapshot (synonym-based; BZ_LOAD_DATE hardened)
           ========================================================= */
        IF OBJECT_ID('tempdb..#src') IS NOT NULL DROP TABLE #src;

        SELECT
              S.CustAcctSiteId                           AS CUSTOMER_SITE
            , S.CustAccountId                            AS CUSTOMER_ACCOUNT
            , S.PartySiteId                              AS PARTY_SITE
            , S.Status                                   AS STATUS
            , S.Language                                 AS LANGUAGE
            , CAST(S.StartDate AS date)                  AS START_DATE_ACTIVE
            , CAST(S.EndDate   AS date)                  AS END_DATE_ACTIVE
            , COALESCE(CAST(S.AddDateTime AS date), CAST(GETDATE() AS date)) AS BZ_LOAD_DATE
            , CAST(GETDATE() AS date)                    AS SV_LOAD_DATE
        INTO #src
        FROM src.bzo_AR_CustomerAccountSiteExtractPVO S
        WHERE S.CustAcctSiteId IS NOT NULL;

        DELETE FROM #src WHERE CUSTOMER_SITE = -1;

        /* Hash for Type2 compare (exclude LANGUAGE which is Type1) */
        IF COL_LENGTH('tempdb..#src', 'HASH_T2') IS NULL
            ALTER TABLE #src ADD HASH_T2 varbinary(32) NULL;

        UPDATE s
            SET s.HASH_T2 = HASHBYTES('SHA2_256', CONCAT(
                  COALESCE(CONVERT(nvarchar(50), s.CUSTOMER_SITE),      N''), N'|'
                , COALESCE(CONVERT(nvarchar(50), s.CUSTOMER_ACCOUNT),   N''), N'|'
                , COALESCE(CONVERT(nvarchar(50), s.PARTY_SITE),         N''), N'|'
                , COALESCE(CONVERT(nvarchar(10), s.STATUS),             N''), N'|'
                , COALESCE(CONVERT(nvarchar(10), s.START_DATE_ACTIVE,120), N''), N'|'
                , COALESCE(CONVERT(nvarchar(10), s.END_DATE_ACTIVE,  120), N'')
            ))
        FROM #src s;

        /* =========================================================
           C) Current target snapshot + T2 hash
           ========================================================= */
        IF OBJECT_ID('tempdb..#tgt') IS NOT NULL DROP TABLE #tgt;

        SELECT
              t.CUSTOMER_SITE
            , t.LANGUAGE
            , HASHBYTES('SHA2_256', CONCAT(
                  COALESCE(CONVERT(nvarchar(50), t.CUSTOMER_SITE),      N''), N'|'
                , COALESCE(CONVERT(nvarchar(50), t.CUSTOMER_ACCOUNT),   N''), N'|'
                , COALESCE(CONVERT(nvarchar(50), t.PARTY_SITE),         N''), N'|'
                , COALESCE(CONVERT(nvarchar(10), t.STATUS),             N''), N'|'
                , COALESCE(CONVERT(nvarchar(10), t.START_DATE_ACTIVE,120), N''), N'|'
                , COALESCE(CONVERT(nvarchar(10), t.END_DATE_ACTIVE,  120), N'')
            )) AS HASH_T2
        INTO #tgt
        FROM svo.D_CUSTOMER_ACCOUNT_SITE t
        WHERE t.CURR_IND = 'Y'
          AND t.END_DATE = @HighDate
          AND t.CUSTOMER_SITE <> -1;

        /* =========================================================
           D) Type 1 updates (LANGUAGE only) when Type2 unchanged
           ========================================================= */
        UPDATE tgt
            SET
                  tgt.LANGUAGE     = src.LANGUAGE
                , tgt.UDT_DATE     = SYSDATETIME()
                , tgt.SV_LOAD_DATE = CAST(GETDATE() AS date)
                , tgt.BZ_LOAD_DATE = COALESCE(tgt.BZ_LOAD_DATE, src.BZ_LOAD_DATE, CAST(GETDATE() AS date))
        FROM svo.D_CUSTOMER_ACCOUNT_SITE tgt
        INNER JOIN #tgt cur
            ON cur.CUSTOMER_SITE = tgt.CUSTOMER_SITE
        INNER JOIN #src src
            ON src.CUSTOMER_SITE = cur.CUSTOMER_SITE
        WHERE tgt.CURR_IND = 'Y'
          AND tgt.END_DATE = @HighDate
          AND cur.HASH_T2 = src.HASH_T2
          AND ISNULL(tgt.LANGUAGE, '') <> ISNULL(src.LANGUAGE, '');

        SET @UpdatedT1 = @@ROWCOUNT;

        /* =========================================================
           E) Type 2 delta: NEW or CHANGED (by T2 hash)
           ========================================================= */
        IF OBJECT_ID('tempdb..#delta_t2') IS NOT NULL DROP TABLE #delta_t2;

        SELECT src.*
        INTO #delta_t2
        FROM #src src
        LEFT JOIN #tgt cur
            ON cur.CUSTOMER_SITE = src.CUSTOMER_SITE
        WHERE cur.CUSTOMER_SITE IS NULL
           OR cur.HASH_T2 <> src.HASH_T2;

        /* =========================================================
           F) Expire current rows for CHANGED BKs only (not NEW)
           ========================================================= */
        UPDATE tgt
            SET
                  tgt.END_DATE     = DATEADD(day, -1, @AsOfDate)
                , tgt.CURR_IND     = 'N'
                , tgt.UDT_DATE     = SYSDATETIME()
                , tgt.SV_LOAD_DATE = CAST(GETDATE() AS date)
        FROM svo.D_CUSTOMER_ACCOUNT_SITE tgt
        INNER JOIN #delta_t2 d
            ON d.CUSTOMER_SITE = tgt.CUSTOMER_SITE
        WHERE tgt.CURR_IND = 'Y'
          AND tgt.END_DATE = @HighDate
          AND tgt.CUSTOMER_SITE <> -1
          AND EXISTS (SELECT 1 FROM #tgt cur WHERE cur.CUSTOMER_SITE = d.CUSTOMER_SITE);

        SET @Expired = @@ROWCOUNT;

        /* =========================================================
           G) Insert new current rows (NEW + CHANGED)
           ========================================================= */
        INSERT INTO svo.D_CUSTOMER_ACCOUNT_SITE
        (
              CUSTOMER_SITE
            , CUSTOMER_ACCOUNT
            , PARTY_SITE
            , STATUS
            , LANGUAGE
            , START_DATE_ACTIVE
            , END_DATE_ACTIVE
            , BZ_LOAD_DATE
            , SV_LOAD_DATE
            , EFF_DATE
            , END_DATE
            , CRE_DATE
            , UDT_DATE
            , CURR_IND
        )
        SELECT
              d.CUSTOMER_SITE
            , d.CUSTOMER_ACCOUNT
            , d.PARTY_SITE
            , d.STATUS
            , d.LANGUAGE
            , d.START_DATE_ACTIVE
            , d.END_DATE_ACTIVE
            , COALESCE(d.BZ_LOAD_DATE, CAST(GETDATE() AS date))
            , CAST(GETDATE() AS date)
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

        ;THROW;
    END CATCH
END
GO

