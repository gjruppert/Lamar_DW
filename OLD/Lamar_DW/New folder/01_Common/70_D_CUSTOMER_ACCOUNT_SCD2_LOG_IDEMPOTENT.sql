USE [Oracle_Reporting_P2];
GO

/* =====================================================================
   DIM: svo.D_CUSTOMER_ACCOUNT (Hybrid Type1/Type2 SCD)
   BK: CUSTOMER_ACCOUNT_ID (CustAccountId)

   Sources (synonym-based / DB independent):
     - src.bzo_AR_CustomerAccountExtractPVO A
     - src.bzo_AR_PartyExtractPVO P  (optional; not required for current columns)

   Hybrid approach (sane default):
     - Type 1 (in-place update on current row): ACCOUNT_NUMBER, ACCOUNT_NAME
     - Type 2 (versioned): STATUS_CODE, CUSTOMER_TYPE, PARTY_ID

   Locked rules applied:
     - Stored proc in svo schema
     - BZ_LOAD_DATE never NULL: COALESCE(CAST(AddDateTime AS date), CAST(GETDATE() AS date))
     - SCD2 fields: EFF_DATE, END_DATE, CRE_DATE, UDT_DATE, CURR_IND
     - ETL run logging (etl.ETL_RUN)
     - Idempotent + transactional
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
        , ROW_UPDATED_T1 int                 NULL
        , ERROR_MESSAGE nvarchar(4000)       NULL
    );
END
GO

/* =========================
   0b) Ensure ETL_RUN has ROW_UPDATED_T1 (older runs may not)
   ========================= */
IF COL_LENGTH('etl.ETL_RUN', 'ROW_UPDATED_T1') IS NULL
    ALTER TABLE etl.ETL_RUN ADD ROW_UPDATED_T1 int NULL;
GO


/* ==============================================
   1) Ensure SCD columns exist (add only if needed)
   ============================================== */
IF COL_LENGTH('svo.D_CUSTOMER_ACCOUNT', 'EFF_DATE') IS NULL
    ALTER TABLE svo.D_CUSTOMER_ACCOUNT ADD EFF_DATE date NOT NULL CONSTRAINT DF_D_CUSTOMER_ACCOUNT_EFF_DATE DEFAULT (CAST('0001-01-01' AS date));
GO
IF COL_LENGTH('svo.D_CUSTOMER_ACCOUNT', 'END_DATE') IS NULL
    ALTER TABLE svo.D_CUSTOMER_ACCOUNT ADD END_DATE date NOT NULL CONSTRAINT DF_D_CUSTOMER_ACCOUNT_END_DATE DEFAULT (CAST('9999-12-31' AS date));
GO
IF COL_LENGTH('svo.D_CUSTOMER_ACCOUNT', 'CRE_DATE') IS NULL
    ALTER TABLE svo.D_CUSTOMER_ACCOUNT ADD CRE_DATE datetime2(0) NOT NULL CONSTRAINT DF_D_CUSTOMER_ACCOUNT_CRE_DATE DEFAULT (SYSDATETIME());
GO
IF COL_LENGTH('svo.D_CUSTOMER_ACCOUNT', 'UDT_DATE') IS NULL
    ALTER TABLE svo.D_CUSTOMER_ACCOUNT ADD UDT_DATE datetime2(0) NOT NULL CONSTRAINT DF_D_CUSTOMER_ACCOUNT_UDT_DATE DEFAULT (SYSDATETIME());
GO
IF COL_LENGTH('svo.D_CUSTOMER_ACCOUNT', 'CURR_IND') IS NULL
    ALTER TABLE svo.D_CUSTOMER_ACCOUNT ADD CURR_IND char(1) NOT NULL CONSTRAINT DF_D_CUSTOMER_ACCOUNT_CURR_IND DEFAULT ('Y');
GO

/* ==============================================
   2) Filtered unique index for current rows (BK)
   ============================================== */
IF NOT EXISTS
(
    SELECT 1
    FROM sys.indexes
    WHERE name = 'UX_D_CUSTOMER_ACCOUNT_ID_CURR'
      AND object_id = OBJECT_ID('svo.D_CUSTOMER_ACCOUNT')
)
BEGIN
    CREATE UNIQUE NONCLUSTERED INDEX UX_D_CUSTOMER_ACCOUNT_ID_CURR
        ON svo.D_CUSTOMER_ACCOUNT(CUSTOMER_ACCOUNT_ID)
        WHERE CURR_IND = 'Y' AND END_DATE = '9999-12-31'
        ON FG_SilverDim;
END
GO

/* ================================
   3) Hybrid SCD loader stored proc
   ================================ */
CREATE OR ALTER PROCEDURE svo.usp_Load_D_CUSTOMER_ACCOUNT_SCD2
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
        , @TargetObject sysname = 'svo.D_CUSTOMER_ACCOUNT';

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
        IF NOT EXISTS (SELECT 1 FROM svo.D_CUSTOMER_ACCOUNT WHERE CUSTOMER_SK = 0)
        BEGIN
            SET IDENTITY_INSERT svo.D_CUSTOMER_ACCOUNT ON;

            INSERT INTO svo.D_CUSTOMER_ACCOUNT
            (
                  CUSTOMER_SK
                , CUSTOMER_ACCOUNT_ID
                , ACCOUNT_NUMBER
                , ACCOUNT_NAME
                , STATUS_CODE
                , CUSTOMER_TYPE
                , PARTY_ID
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
                , '-1'
                , 'Unknown'
                , 'Unknown'
                , 'Unknown'
                , '-1'
                , CAST(GETDATE() AS date)
                , CAST(GETDATE() AS date)
                , CAST(GETDATE() AS date)
                , @HighDate
                , SYSDATETIME()
                , SYSDATETIME()
                , 'Y'
            );

            SET IDENTITY_INSERT svo.D_CUSTOMER_ACCOUNT OFF;
        END

        /* =========================================================
           B) Source snapshot (synonym-based; BZ_LOAD_DATE hardened)
              NOTE: Party PVO is available, but not required for the
                    current column set in your table DDL.
           ========================================================= */
        IF OBJECT_ID('tempdb..#src') IS NOT NULL DROP TABLE #src;

        SELECT
              A.CustAccountId                              AS CUSTOMER_ACCOUNT_ID
            , A.AccountNumber                              AS ACCOUNT_NUMBER
            , A.AccountName                                AS ACCOUNT_NAME
            , A.Status                                     AS STATUS_CODE
            , A.CustomerType                               AS CUSTOMER_TYPE
            , A.PartyId                                    AS PARTY_ID
            , COALESCE(CAST(A.AddDateTime AS date), CAST(GETDATE() AS date)) AS BZ_LOAD_DATE
            , CAST(GETDATE() AS date)                      AS SV_LOAD_DATE
        INTO #src
        FROM src.bzo_AR_CustomerAccountExtractPVO A
        WHERE A.CustAccountId IS NOT NULL;

        /* Exclude plug BK from SCD processing */
        DELETE FROM #src WHERE CUSTOMER_ACCOUNT_ID = -1;

        /* Hashes for Type2 compare (exclude Type1 columns) */
        IF COL_LENGTH('tempdb..#src', 'HASH_T2') IS NULL
            ALTER TABLE #src ADD HASH_T2 varbinary(32) NULL;

        UPDATE s
            SET s.HASH_T2 = HASHBYTES('SHA2_256', CONCAT(
                  COALESCE(CONVERT(nvarchar(50),  s.CUSTOMER_ACCOUNT_ID), N''), N'|'
                , COALESCE(CONVERT(nvarchar(30),  s.STATUS_CODE),         N''), N'|'
                , COALESCE(CONVERT(nvarchar(60),  s.CUSTOMER_TYPE),       N''), N'|'
                , COALESCE(CONVERT(nvarchar(60),  s.PARTY_ID),            N'')
            ))
        FROM #src s;

        /* =========================================================
           C) Current target snapshot + hashes
           ========================================================= */
        IF OBJECT_ID('tempdb..#tgt') IS NOT NULL DROP TABLE #tgt;

        SELECT
              t.CUSTOMER_ACCOUNT_ID
            , t.ACCOUNT_NUMBER
            , t.ACCOUNT_NAME
            , t.STATUS_CODE
            , t.CUSTOMER_TYPE
            , t.PARTY_ID
            , HASHBYTES('SHA2_256', CONCAT(
                  COALESCE(CONVERT(nvarchar(50),  t.CUSTOMER_ACCOUNT_ID), N''), N'|'
                , COALESCE(CONVERT(nvarchar(30),  t.STATUS_CODE),         N''), N'|'
                , COALESCE(CONVERT(nvarchar(60),  t.CUSTOMER_TYPE),       N''), N'|'
                , COALESCE(CONVERT(nvarchar(60),  t.PARTY_ID),            N'')
            )) AS HASH_T2
        INTO #tgt
        FROM svo.D_CUSTOMER_ACCOUNT t
        WHERE t.CURR_IND = 'Y'
          AND t.END_DATE = @HighDate
          AND t.CUSTOMER_ACCOUNT_ID <> -1;

        /* =========================================================
           D) Type 1 updates (ACCOUNT_NUMBER, ACCOUNT_NAME)
              Update ONLY when T2 is same (no version) and values changed
           ========================================================= */
        UPDATE tgt
            SET
                  tgt.ACCOUNT_NUMBER = src.ACCOUNT_NUMBER
                , tgt.ACCOUNT_NAME   = src.ACCOUNT_NAME
                , tgt.UDT_DATE       = SYSDATETIME()
                , tgt.SV_LOAD_DATE   = CAST(GETDATE() AS date)
                , tgt.BZ_LOAD_DATE   = COALESCE(tgt.BZ_LOAD_DATE, src.BZ_LOAD_DATE, CAST(GETDATE() AS date)) -- safety
        FROM svo.D_CUSTOMER_ACCOUNT tgt
        INNER JOIN #tgt cur
            ON cur.CUSTOMER_ACCOUNT_ID = tgt.CUSTOMER_ACCOUNT_ID
        INNER JOIN #src src
            ON src.CUSTOMER_ACCOUNT_ID = cur.CUSTOMER_ACCOUNT_ID
        WHERE tgt.CURR_IND = 'Y'
          AND tgt.END_DATE = @HighDate
          AND cur.HASH_T2 = src.HASH_T2
          AND (
                ISNULL(tgt.ACCOUNT_NUMBER, '') <> ISNULL(src.ACCOUNT_NUMBER, '')
             OR ISNULL(tgt.ACCOUNT_NAME,   '') <> ISNULL(src.ACCOUNT_NAME,   '')
          );

        SET @UpdatedT1 = @@ROWCOUNT;

        /* =========================================================
           E) Type 2 delta: NEW or CHANGED (by T2 hash)
           ========================================================= */
        IF OBJECT_ID('tempdb..#delta_t2') IS NOT NULL DROP TABLE #delta_t2;

        SELECT src.*
        INTO #delta_t2
        FROM #src src
        LEFT JOIN #tgt cur
            ON cur.CUSTOMER_ACCOUNT_ID = src.CUSTOMER_ACCOUNT_ID
        WHERE cur.CUSTOMER_ACCOUNT_ID IS NULL
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
        FROM svo.D_CUSTOMER_ACCOUNT tgt
        INNER JOIN #delta_t2 d
            ON d.CUSTOMER_ACCOUNT_ID = tgt.CUSTOMER_ACCOUNT_ID
        WHERE tgt.CURR_IND = 'Y'
          AND tgt.END_DATE = @HighDate
          AND tgt.CUSTOMER_ACCOUNT_ID <> -1
          AND EXISTS (SELECT 1 FROM #tgt cur WHERE cur.CUSTOMER_ACCOUNT_ID = d.CUSTOMER_ACCOUNT_ID);

        SET @Expired = @@ROWCOUNT;

        /* =========================================================
           G) Insert new current rows for NEW + CHANGED
           ========================================================= */
        INSERT INTO svo.D_CUSTOMER_ACCOUNT
        (
              CUSTOMER_ACCOUNT_ID
            , ACCOUNT_NUMBER
            , ACCOUNT_NAME
            , STATUS_CODE
            , CUSTOMER_TYPE
            , PARTY_ID
            , BZ_LOAD_DATE
            , SV_LOAD_DATE
            , EFF_DATE
            , END_DATE
            , CRE_DATE
            , UDT_DATE
            , CURR_IND
        )
        SELECT
              d.CUSTOMER_ACCOUNT_ID
            , d.ACCOUNT_NUMBER
            , d.ACCOUNT_NAME
            , d.STATUS_CODE
            , d.CUSTOMER_TYPE
            , d.PARTY_ID
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

