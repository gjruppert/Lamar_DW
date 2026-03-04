USE [Oracle_Reporting_P2];
GO

/* =====================================================================
   DIM: svo.D_PAYMENT_METHOD
   BK : PAYMENT_METHOD_ID  (Source BK: ArReceiptMethodReceiptMethodId)
   Strategy: Hybrid (Type2 for descriptive attrs; no Type1 list by default)

   Locked standards:
   - SPs in schema: svo
   - BZ_LOAD_DATE never NULL:
       COALESCE(CAST(src.AddDateTime AS date), CAST(GETDATE() AS date))
   - Standard SCD2 columns (add if missing) with defaults:
       EFF_DATE (date), END_DATE (date), CRE_DATE (datetime2), UDT_DATE (datetime2), CURR_IND (char(1))
   - Idempotent, transactional, ETL run logging in etl.ETL_RUN
   - Source query uses synonyms (bzo.*) to remain DB-independent
   - Plug row: SK=0, BK='-1'
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
IF COL_LENGTH('svo.D_PAYMENT_METHOD', 'EFF_DATE') IS NULL
    ALTER TABLE svo.D_PAYMENT_METHOD ADD EFF_DATE date NOT NULL
        CONSTRAINT DF_D_PAYMENT_METHOD_EFF_DATE DEFAULT (CAST('0001-01-01' AS date));
GO
IF COL_LENGTH('svo.D_PAYMENT_METHOD', 'END_DATE') IS NULL
    ALTER TABLE svo.D_PAYMENT_METHOD ADD END_DATE date NOT NULL
        CONSTRAINT DF_D_PAYMENT_METHOD_END_DATE DEFAULT (CAST('9999-12-31' AS date));
GO
IF COL_LENGTH('svo.D_PAYMENT_METHOD', 'CRE_DATE') IS NULL
    ALTER TABLE svo.D_PAYMENT_METHOD ADD CRE_DATE datetime2(0) NOT NULL
        CONSTRAINT DF_D_PAYMENT_METHOD_CRE_DATE DEFAULT (SYSDATETIME());
GO
IF COL_LENGTH('svo.D_PAYMENT_METHOD', 'UDT_DATE') IS NULL
    ALTER TABLE svo.D_PAYMENT_METHOD ADD UDT_DATE datetime2(0) NOT NULL
        CONSTRAINT DF_D_PAYMENT_METHOD_UDT_DATE DEFAULT (SYSDATETIME());
GO
IF COL_LENGTH('svo.D_PAYMENT_METHOD', 'CURR_IND') IS NULL
    ALTER TABLE svo.D_PAYMENT_METHOD ADD CURR_IND char(1) NOT NULL
        CONSTRAINT DF_D_PAYMENT_METHOD_CURR_IND DEFAULT ('Y');
GO

-------------------------------------------------------------------------------
-- 2) Current-row unique index for BK (drop legacy, create filtered)
-------------------------------------------------------------------------------
IF EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'UX_D_PAYMENT_METHOD' AND object_id = OBJECT_ID('svo.D_PAYMENT_METHOD'))
    DROP INDEX UX_D_PAYMENT_METHOD ON svo.D_PAYMENT_METHOD;
GO

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'UX_D_PAYMENT_METHOD_ID_CURR' AND object_id = OBJECT_ID('svo.D_PAYMENT_METHOD'))
BEGIN
    CREATE UNIQUE NONCLUSTERED INDEX UX_D_PAYMENT_METHOD_ID_CURR
        ON svo.D_PAYMENT_METHOD(PAYMENT_METHOD_ID)
        WHERE CURR_IND = 'Y' AND END_DATE = '9999-12-31'
        ON FG_SilverDim;
END
GO

-------------------------------------------------------------------------------
-- 3) Loader procedure
-------------------------------------------------------------------------------
CREATE OR ALTER PROCEDURE svo.usp_Load_D_PAYMENT_METHOD_SCD2
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
        , @TargetObject sysname = 'svo.D_PAYMENT_METHOD';

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
        -- Plug row (SK = 0)
        --------------------------------------------------------------------
        IF NOT EXISTS (SELECT 1 FROM svo.D_PAYMENT_METHOD WHERE PAYMENT_METHOD_ID = '-1')
        BEGIN
            DECLARE @SkCol sysname =
            (
                SELECT TOP(1) name
                FROM sys.identity_columns
                WHERE object_id = OBJECT_ID('svo.D_PAYMENT_METHOD')
                ORDER BY column_id
            );

            IF @SkCol IS NULL
                THROW 51000, 'No IDENTITY column found on svo.D_PAYMENT_METHOD; cannot insert plug row with SK=0.', 1;

            DECLARE @PlugSql nvarchar(max) = N'
                SET IDENTITY_INSERT svo.D_PAYMENT_METHOD ON;

                INSERT INTO svo.D_PAYMENT_METHOD
                (
                      ' + QUOTENAME(@SkCol) + N'
                    , PAYMENT_METHOD_ID
                    , PAYMENT_METHOD_NAME
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
                    , ''-1''
                    , ''Unknown''
                    , CAST(''0001-01-01'' AS date)
                    , CAST(GETDATE() AS date)
                    , CAST(''0001-01-01'' AS date)
                    , CAST(''9999-12-31'' AS date)
                    , SYSDATETIME()
                    , SYSDATETIME()
                    , ''Y''
                );

                SET IDENTITY_INSERT svo.D_PAYMENT_METHOD OFF;
            ';

            EXEC sys.sp_executesql @PlugSql;
        END

        --------------------------------------------------------------------
        -- Source set (single source; synonyms preserved)
        --------------------------------------------------------------------
        IF OBJECT_ID('tempdb..#src') IS NOT NULL DROP TABLE #src;

        SELECT
              CAST(src.ArReceiptMethodReceiptMethodId AS varchar(30)) AS PAYMENT_METHOD_ID
            , CAST(src.ArReceiptMethodName AS varchar(30))            AS PAYMENT_METHOD_NAME
            , COALESCE(CAST(src.AddDateTime AS date), CAST(GETDATE() AS date)) AS BZ_LOAD_DATE
            , CAST(GETDATE() AS date)                                 AS SV_LOAD_DATE
            , CAST(NULL AS varbinary(32))                              AS HASH_T2
        INTO #src
        FROM src.bzo_AR_ReceiptMethodExtractPVO src;

        -- Guardrails
        DELETE FROM #src
        WHERE PAYMENT_METHOD_ID IS NULL
           OR LTRIM(RTRIM(PAYMENT_METHOD_ID)) = ''
           OR PAYMENT_METHOD_ID = '-1';

        UPDATE s SET
              s.PAYMENT_METHOD_NAME = COALESCE(NULLIF(LTRIM(RTRIM(s.PAYMENT_METHOD_NAME)), ''), 'UNK')
            , s.BZ_LOAD_DATE = COALESCE(s.BZ_LOAD_DATE, CAST(GETDATE() AS date))
        FROM #src s;

        --------------------------------------------------------------------
        -- Hash for Type2 compare (PAYMENT_METHOD_NAME drives history)
        --------------------------------------------------------------------
        UPDATE s
            SET s.HASH_T2 = HASHBYTES('SHA2_256',
                  CAST(COALESCE(CONVERT(nvarchar(4000), s.PAYMENT_METHOD_ID), N'') AS nvarchar(max))
                + N'|' + CAST(COALESCE(CONVERT(nvarchar(4000), s.PAYMENT_METHOD_NAME), N'') AS nvarchar(max))
            )
        FROM #src s;

        --------------------------------------------------------------------
        -- Current target hashes
        --------------------------------------------------------------------
        IF OBJECT_ID('tempdb..#tgt') IS NOT NULL DROP TABLE #tgt;

        SELECT
              t.PAYMENT_METHOD_ID,
              HASHBYTES('SHA2_256',
                  CAST(COALESCE(CONVERT(nvarchar(4000), t.PAYMENT_METHOD_ID), N'') AS nvarchar(max))
                + N'|' + CAST(COALESCE(CONVERT(nvarchar(4000), t.PAYMENT_METHOD_NAME), N'') AS nvarchar(max))
              ) AS HASH_T2
        INTO #tgt
        FROM svo.D_PAYMENT_METHOD t
        WHERE t.CURR_IND = 'Y'
          AND t.END_DATE = @HighDate
          AND t.PAYMENT_METHOD_ID <> '-1';

        --------------------------------------------------------------------
        -- Delta (new or changed)
        --------------------------------------------------------------------
        IF OBJECT_ID('tempdb..#delta_t2') IS NOT NULL DROP TABLE #delta_t2;

        SELECT s.*
        INTO #delta_t2
        FROM #src s
        LEFT JOIN #tgt t ON t.PAYMENT_METHOD_ID = s.PAYMENT_METHOD_ID
        WHERE t.PAYMENT_METHOD_ID IS NULL
           OR t.HASH_T2 <> s.HASH_T2;

        --------------------------------------------------------------------
        -- Expire changed rows
        --------------------------------------------------------------------
        UPDATE tgt
            SET
                  tgt.END_DATE     = DATEADD(day, -1, @AsOfDate)
                , tgt.CURR_IND     = 'N'
                , tgt.UDT_DATE     = SYSDATETIME()
                , tgt.SV_LOAD_DATE = CAST(GETDATE() AS date)
                , tgt.BZ_LOAD_DATE = COALESCE(tgt.BZ_LOAD_DATE, CAST(GETDATE() AS date))  -- enforce never NULL
        FROM svo.D_PAYMENT_METHOD tgt
        INNER JOIN #delta_t2 d ON d.PAYMENT_METHOD_ID = tgt.PAYMENT_METHOD_ID
        WHERE tgt.CURR_IND = 'Y'
          AND tgt.END_DATE = @HighDate
          AND tgt.PAYMENT_METHOD_ID <> '-1'
          AND EXISTS (SELECT 1 FROM #tgt cur WHERE cur.PAYMENT_METHOD_ID = d.PAYMENT_METHOD_ID);

        SET @Expired = @@ROWCOUNT;

        --------------------------------------------------------------------
        -- Insert new current rows
        --------------------------------------------------------------------
        INSERT INTO svo.D_PAYMENT_METHOD
        (
              PAYMENT_METHOD_ID
            , PAYMENT_METHOD_NAME
            , BZ_LOAD_DATE
            , SV_LOAD_DATE
            , EFF_DATE
            , END_DATE
            , CRE_DATE
            , UDT_DATE
            , CURR_IND
        )
        SELECT
              d.PAYMENT_METHOD_ID
            , d.PAYMENT_METHOD_NAME
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

