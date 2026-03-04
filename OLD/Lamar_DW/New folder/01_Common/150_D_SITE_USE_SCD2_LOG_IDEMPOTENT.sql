USE [Oracle_Reporting_P2];
GO

/* =====================================================================
   DIM: svo.D_SITE_USE
   BK : SITE_USE  (Source: SiteUseId)

   Settings locked in:
   - Stored procedure schema: svo
   - BZ_LOAD_DATE never NULL:
       COALESCE(CAST(AddDateTime AS date), CAST(GETDATE() AS date))
   - SCD2 fields: EFF_DATE, END_DATE, CRE_DATE, UDT_DATE, CURR_IND
     (added if missing, with defaults)
   - Idempotent / transactional / logged (etl.ETL_RUN)
   - Preserves synonym-based, DB-independent source query (bzo.*)

   Notes:
   - Hybrid mode implemented as "Type2 for all descriptive attributes"
     (Type1 list intentionally empty for SITE_USE unless you tell me otherwise).
   ===================================================================== */

-------------------------------------------------------------------------------
-- 0) Logging (create once)
-------------------------------------------------------------------------------
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

-------------------------------------------------------------------------------
-- 1) Ensure standard SCD columns exist (add only if missing)
-------------------------------------------------------------------------------
IF COL_LENGTH('svo.D_SITE_USE', 'EFF_DATE') IS NULL
    ALTER TABLE svo.D_SITE_USE ADD EFF_DATE date NOT NULL
        CONSTRAINT DF_D_SITE_USE_EFF_DATE DEFAULT (CAST('0001-01-01' AS date));
GO
IF COL_LENGTH('svo.D_SITE_USE', 'END_DATE') IS NULL
    ALTER TABLE svo.D_SITE_USE ADD END_DATE date NOT NULL
        CONSTRAINT DF_D_SITE_USE_END_DATE DEFAULT (CAST('9999-12-31' AS date));
GO
IF COL_LENGTH('svo.D_SITE_USE', 'CRE_DATE') IS NULL
    ALTER TABLE svo.D_SITE_USE ADD CRE_DATE datetime2(0) NOT NULL
        CONSTRAINT DF_D_SITE_USE_CRE_DATE DEFAULT (SYSDATETIME());
GO
IF COL_LENGTH('svo.D_SITE_USE', 'UDT_DATE') IS NULL
    ALTER TABLE svo.D_SITE_USE ADD UDT_DATE datetime2(0) NOT NULL
        CONSTRAINT DF_D_SITE_USE_UDT_DATE DEFAULT (SYSDATETIME());
GO
IF COL_LENGTH('svo.D_SITE_USE', 'CURR_IND') IS NULL
    ALTER TABLE svo.D_SITE_USE ADD CURR_IND char(1) NOT NULL
        CONSTRAINT DF_D_SITE_USE_CURR_IND DEFAULT ('Y');
GO

-------------------------------------------------------------------------------
-- 2) Current-row unique index for BK (drop legacy, create filtered)
-------------------------------------------------------------------------------
IF EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'UX_D_SITE_USE' AND object_id = OBJECT_ID('svo.D_SITE_USE'))
    DROP INDEX UX_D_SITE_USE ON svo.D_SITE_USE;
GO

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'UX_D_SITE_USE_ID_CURR' AND object_id = OBJECT_ID('svo.D_SITE_USE'))
BEGIN
    CREATE UNIQUE NONCLUSTERED INDEX UX_D_SITE_USE_ID_CURR
        ON svo.D_SITE_USE(SITE_USE)
        WHERE CURR_IND = 'Y' AND END_DATE = '9999-12-31'
        ON FG_SilverDim;
END
GO

-------------------------------------------------------------------------------
-- 3) Loader procedure
-------------------------------------------------------------------------------
CREATE OR ALTER PROCEDURE svo.usp_Load_D_SITE_USE_SCD2
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
        , @TargetObject sysname = 'svo.D_SITE_USE';

    DECLARE
          @Inserted   int = 0
        , @Expired    int = 0
        , @UpdatedT1  int = 0;

    INSERT INTO etl.ETL_RUN (PROC_NAME, TARGET_OBJECT, ASOF_DATE, START_DTTM, STATUS)
    VALUES (@ProcName, @TargetObject, @AsOfDate, SYSDATETIME(), 'STARTED');

    SET @RunId = SCOPE_IDENTITY();

    BEGIN TRY
        BEGIN TRAN;

        --------------------------------------------------------------------
        -- Plug row (no assumptions besides BK name; does not assume SK name)
        --------------------------------------------------------------------
        IF NOT EXISTS (SELECT 1 FROM svo.D_SITE_USE WHERE SITE_USE = -1)
        BEGIN
            DECLARE @SkColPlug sysname =
            (
                SELECT TOP(1) name
                FROM sys.identity_columns
                WHERE object_id = OBJECT_ID('svo.D_SITE_USE')
                ORDER BY column_id
            );

            DECLARE @PlugCols nvarchar(max) = N'';
            DECLARE @PlugVals nvarchar(max) = N'';

            ;WITH c AS
            (
                SELECT
                      sc.name
                    , sc.is_nullable
                    , sc.column_id
                    , t.name AS typ
                FROM sys.columns sc
                INNER JOIN sys.types t ON sc.user_type_id = t.user_type_id
                WHERE sc.object_id = OBJECT_ID('svo.D_SITE_USE')
                  AND sc.name NOT IN (ISNULL(@SkColPlug, N'<<no_sk>>'))
            )
            SELECT
                @PlugCols = STRING_AGG(QUOTENAME(name), N', ') WITHIN GROUP (ORDER BY column_id),
                @PlugVals = STRING_AGG(
                    CASE
                        WHEN name = 'SITE_USE' THEN N'-1'
                        WHEN name IN ('BZ_LOAD_DATE','SV_LOAD_DATE') THEN N'CAST(GETDATE() AS date)'
                        WHEN name = 'EFF_DATE' THEN N'CAST(''0001-01-01'' AS date)'
                        WHEN name = 'END_DATE' THEN N'CAST(''9999-12-31'' AS date)'
                        WHEN name IN ('CRE_DATE','UDT_DATE') THEN N'SYSDATETIME()'
                        WHEN name = 'CURR_IND' THEN N'''Y'''
                        WHEN typ IN ('bigint','int','smallint','tinyint','decimal','numeric','float','real','money','smallmoney','bit') THEN N'-1'
                        WHEN typ IN ('date') THEN N'CAST(''0001-01-01'' AS date)'
                        WHEN typ IN ('datetime','datetime2','smalldatetime') THEN N'SYSDATETIME()'
                        ELSE N'''UNK'''
                    END,
                    N', '
                ) WITHIN GROUP (ORDER BY column_id)
            FROM c
            WHERE (is_nullable = 0) OR name IN ('SITE_USE','BZ_LOAD_DATE','SV_LOAD_DATE','EFF_DATE','END_DATE','CRE_DATE','UDT_DATE','CURR_IND');

            EXEC (N'INSERT INTO svo.D_SITE_USE (' + @PlugCols + N') VALUES (' + @PlugVals + N');');
        END

        --------------------------------------------------------------------
        -- Build source set (preserve your bzo.* source query)
        --------------------------------------------------------------------
        IF OBJECT_ID('tempdb..#src') IS NOT NULL DROP TABLE #src;

        ;WITH src0 AS
        (
            SELECT
                SiteUseId      AS SITE_USE,
                CustAcctSiteId AS CUSTOMER_SITE,
                SiteUseCode    AS SITE_USE_CODE,
                Location       AS LOCATION,
                PrimaryFlag    AS PRIMARY_FLAG,
                PaymentTermId  AS PAYMENT_TERM_ID,
                Status         AS STATUS,
                AddDateTime    AS AddDateTime
            FROM src.bzo_AR_CustomerAcctSiteUseExtractPVO
        ),
        src1 AS
        (
            SELECT
                SITE_USE,
                CUSTOMER_SITE,
                SITE_USE_CODE,
                LOCATION,
                PRIMARY_FLAG,
                PAYMENT_TERM_ID,
                STATUS,
                COALESCE(CAST(AddDateTime AS date), CAST(GETDATE() AS date)) AS BZ_LOAD_DATE,
                CAST(GETDATE() AS date) AS SV_LOAD_DATE,
                ROW_NUMBER() OVER
                (
                    PARTITION BY SITE_USE
                    ORDER BY COALESCE(AddDateTime, SYSDATETIME()) DESC
                ) AS rn
            FROM src0
            WHERE SITE_USE IS NOT NULL
              AND SITE_USE <> -1
        )
        SELECT
            SITE_USE,
            CUSTOMER_SITE,
            SITE_USE_CODE,
            LOCATION,
            PRIMARY_FLAG,
            PAYMENT_TERM_ID,
            STATUS,
            BZ_LOAD_DATE,
            SV_LOAD_DATE,
            CAST(NULL AS varbinary(32)) AS HASH_T2
        INTO #src
        FROM src1
        WHERE rn = 1;

        --------------------------------------------------------------------
        -- NOT NULL guardrails (only for columns that are NOT NULL in target)
        --------------------------------------------------------------------
        -- CUSTOMER_SITE / SITE_USE_CODE are typically NOT NULL in your DDL.
        UPDATE s SET
              s.CUSTOMER_SITE = COALESCE(s.CUSTOMER_SITE, -1)
            , s.SITE_USE_CODE = COALESCE(s.SITE_USE_CODE, 'UNK')
        FROM #src s;

        --------------------------------------------------------------------
        -- Hash for Type2 compare (all descriptive attrs)
        --------------------------------------------------------------------
        UPDATE s
            SET s.HASH_T2 = HASHBYTES('SHA2_256',
                  CAST(COALESCE(CONVERT(nvarchar(4000), s.SITE_USE), N'') AS nvarchar(max))
                + N'|' + CAST(COALESCE(CONVERT(nvarchar(4000), s.CUSTOMER_SITE), N'') AS nvarchar(max))
                + N'|' + CAST(COALESCE(CONVERT(nvarchar(4000), s.SITE_USE_CODE), N'') AS nvarchar(max))
                + N'|' + CAST(COALESCE(CONVERT(nvarchar(4000), s.LOCATION), N'') AS nvarchar(max))
                + N'|' + CAST(COALESCE(CONVERT(nvarchar(4000), s.PRIMARY_FLAG), N'') AS nvarchar(max))
                + N'|' + CAST(COALESCE(CONVERT(nvarchar(4000), s.PAYMENT_TERM_ID), N'') AS nvarchar(max))
                + N'|' + CAST(COALESCE(CONVERT(nvarchar(4000), s.STATUS), N'') AS nvarchar(max))
            )
        FROM #src s;

        --------------------------------------------------------------------
        -- Current target hashes
        --------------------------------------------------------------------
        IF OBJECT_ID('tempdb..#tgt') IS NOT NULL DROP TABLE #tgt;

        SELECT
              t.SITE_USE,
              HASHBYTES('SHA2_256',
                  CAST(COALESCE(CONVERT(nvarchar(4000), t.SITE_USE), N'') AS nvarchar(max))
                + N'|' + CAST(COALESCE(CONVERT(nvarchar(4000), t.CUSTOMER_SITE), N'') AS nvarchar(max))
                + N'|' + CAST(COALESCE(CONVERT(nvarchar(4000), t.SITE_USE_CODE), N'') AS nvarchar(max))
                + N'|' + CAST(COALESCE(CONVERT(nvarchar(4000), t.LOCATION), N'') AS nvarchar(max))
                + N'|' + CAST(COALESCE(CONVERT(nvarchar(4000), t.PRIMARY_FLAG), N'') AS nvarchar(max))
                + N'|' + CAST(COALESCE(CONVERT(nvarchar(4000), t.PAYMENT_TERM_ID), N'') AS nvarchar(max))
                + N'|' + CAST(COALESCE(CONVERT(nvarchar(4000), t.STATUS), N'') AS nvarchar(max))
              ) AS HASH_T2
        INTO #tgt
        FROM svo.D_SITE_USE t
        WHERE t.CURR_IND = 'Y'
          AND t.END_DATE = @HighDate
          AND t.SITE_USE <> -1;

        --------------------------------------------------------------------
        -- Delta (new or changed)
        --------------------------------------------------------------------
        IF OBJECT_ID('tempdb..#delta_t2') IS NOT NULL DROP TABLE #delta_t2;

        SELECT s.*
        INTO #delta_t2
        FROM #src s
        LEFT JOIN #tgt t ON t.SITE_USE = s.SITE_USE
        WHERE t.SITE_USE IS NULL
           OR t.HASH_T2 <> s.HASH_T2;

        --------------------------------------------------------------------
        -- Expire changed rows (only where a current row exists)
        --------------------------------------------------------------------
        UPDATE tgt
            SET
                  tgt.END_DATE     = DATEADD(day, -1, @AsOfDate)
                , tgt.CURR_IND     = 'N'
                , tgt.UDT_DATE     = SYSDATETIME()
                , tgt.SV_LOAD_DATE = CAST(GETDATE() AS date)
        FROM svo.D_SITE_USE tgt
        INNER JOIN #delta_t2 d ON d.SITE_USE = tgt.SITE_USE
        WHERE tgt.CURR_IND = 'Y'
          AND tgt.END_DATE = @HighDate
          AND tgt.SITE_USE <> -1
          AND EXISTS (SELECT 1 FROM #tgt cur WHERE cur.SITE_USE = d.SITE_USE);

        SET @Expired = @@ROWCOUNT;

        --------------------------------------------------------------------
        -- Insert new current rows
        --------------------------------------------------------------------
        INSERT INTO svo.D_SITE_USE
        (
              SITE_USE
            , CUSTOMER_SITE
            , SITE_USE_CODE
            , LOCATION
            , PRIMARY_FLAG
            , PAYMENT_TERM_ID
            , STATUS
            , BZ_LOAD_DATE
            , SV_LOAD_DATE
            , EFF_DATE
            , END_DATE
            , CRE_DATE
            , UDT_DATE
            , CURR_IND
        )
        SELECT
              d.SITE_USE
            , d.CUSTOMER_SITE
            , d.SITE_USE_CODE
            , d.LOCATION
            , d.PRIMARY_FLAG
            , d.PAYMENT_TERM_ID
            , d.STATUS
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

        THROW;
    END CATCH
END
GO

