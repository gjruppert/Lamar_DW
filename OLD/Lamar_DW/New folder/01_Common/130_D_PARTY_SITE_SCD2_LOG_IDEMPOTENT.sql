USE [Oracle_Reporting_P2];
GO

/* =====================================================================
   DIM: svo.D_PARTY_SITE  (Hybrid SCD2)
   BK: PARTY_SITE_ID

   Type 1 (in-place on current row when Type2 unchanged):
     - PARTY_SITE_NAME
     - PARTY_SITE_NUMBER

   Type 2 (versioned):
     - PARTY_ID, LOCATION_ID, OVERALL_PRIMARY_FLAG, ACTUAL_CONTENT_SOURCE
     - START_DATE_ACTIVE, END_DATE_ACTIVE, STATUS
     - CREATED_BY, LAST_UPDATE_BY, LAST_UPDATE_LOGIN

   Business active dates (do NOT confuse with SCD END_DATE):
     - START_DATE_ACTIVE / END_DATE_ACTIVE = source business dates
     - EFF_DATE / END_DATE = warehouse SCD2 window

   Locked rules:
     - proc in svo
     - BZ_LOAD_DATE never NULL: COALESCE(CAST(AddDateTime AS date), CAST(GETDATE() AS date))
     - synonym-based sources (no DB prefix)
     - logging + idempotent + transactional
   ===================================================================== */

--------------------------------------------------------------------------------
-- 0) Logging (create once)
--------------------------------------------------------------------------------
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

IF COL_LENGTH('etl.ETL_RUN', 'ROW_UPDATED_T1') IS NULL
    ALTER TABLE etl.ETL_RUN ADD ROW_UPDATED_T1 int NULL;
GO

--------------------------------------------------------------------------------
-- 1) Ensure standard SCD columns exist (add only if needed)
--------------------------------------------------------------------------------
IF COL_LENGTH('svo.D_PARTY_SITE', 'EFF_DATE') IS NULL
    ALTER TABLE svo.D_PARTY_SITE ADD EFF_DATE date NOT NULL
        CONSTRAINT DF_D_PARTY_SITE_EFF_DATE DEFAULT (CAST('0001-01-01' AS date));
GO
IF COL_LENGTH('svo.D_PARTY_SITE', 'END_DATE') IS NULL
    ALTER TABLE svo.D_PARTY_SITE ADD END_DATE date NOT NULL
        CONSTRAINT DF_D_PARTY_SITE_END_DATE DEFAULT (CAST('9999-12-31' AS date));
GO
IF COL_LENGTH('svo.D_PARTY_SITE', 'CRE_DATE') IS NULL
    ALTER TABLE svo.D_PARTY_SITE ADD CRE_DATE datetime2(0) NOT NULL
        CONSTRAINT DF_D_PARTY_SITE_CRE_DATE DEFAULT (SYSDATETIME());
GO
IF COL_LENGTH('svo.D_PARTY_SITE', 'UDT_DATE') IS NULL
    ALTER TABLE svo.D_PARTY_SITE ADD UDT_DATE datetime2(0) NOT NULL
        CONSTRAINT DF_D_PARTY_SITE_UDT_DATE DEFAULT (SYSDATETIME());
GO
IF COL_LENGTH('svo.D_PARTY_SITE', 'CURR_IND') IS NULL
    ALTER TABLE svo.D_PARTY_SITE ADD CURR_IND char(1) NOT NULL
        CONSTRAINT DF_D_PARTY_SITE_CURR_IND DEFAULT ('Y');
GO

--------------------------------------------------------------------------------
-- 2) Replace legacy unique index that breaks SCD2 history
--------------------------------------------------------------------------------
IF EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'UX_D_PARTY_SITE' AND object_id = OBJECT_ID('svo.D_PARTY_SITE'))
    DROP INDEX UX_D_PARTY_SITE ON svo.D_PARTY_SITE;
GO

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'UX_D_PARTY_SITE_ID_CURR' AND object_id = OBJECT_ID('svo.D_PARTY_SITE'))
BEGIN
    CREATE UNIQUE NONCLUSTERED INDEX UX_D_PARTY_SITE_ID_CURR
        ON svo.D_PARTY_SITE(PARTY_SITE_ID)
        WHERE CURR_IND = 'Y' AND END_DATE = '9999-12-31'
        ON FG_SilverDim;
END
GO

--------------------------------------------------------------------------------
-- 3) Loader procedure
--------------------------------------------------------------------------------
CREATE OR ALTER PROCEDURE svo.usp_Load_D_PARTY_SITE_SCD2
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
        , @TargetObject sysname = 'svo.D_PARTY_SITE';

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
        -- A) Plug row (SK=0, BK=-1) - stable
        --------------------------------------------------------------------
        IF NOT EXISTS (SELECT 1 FROM svo.D_PARTY_SITE WHERE PARTY_SITE_SK = 0)
        BEGIN
            SET IDENTITY_INSERT svo.D_PARTY_SITE ON;

            INSERT INTO svo.D_PARTY_SITE
            (
                  PARTY_SITE_SK
                , PARTY_SITE_ID
                , PARTY_ID
                , PARTY_SITE_NAME
                , PARTY_SITE_NUMBER
                , LOCATION_ID
                , OVERALL_PRIMARY_FLAG
                , ACTUAL_CONTENT_SOURCE
                , START_DATE_ACTIVE
                , END_DATE_ACTIVE
                , STATUS
                , CREATED_BY
                , LAST_UPDATE_BY
                , LAST_UPDATE_LOGIN
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
                , 'Unknown Party Site'
                , 'UNK'
                , -1
                , 'N'
                , 'UNK'
                , CAST('0001-01-01' AS date)
                , CAST('0001-01-01' AS date)
                , 'U'
                , 'SYSTEM'
                , 'SYSTEM'
                , 'UNK'
                , CAST('0001-01-01' AS date)
                , CAST(GETDATE() AS date)
                , CAST(GETDATE() AS date)
                , @HighDate
                , SYSDATETIME()
                , SYSDATETIME()
                , 'Y'
            );

            SET IDENTITY_INSERT svo.D_PARTY_SITE OFF;
        END

        --------------------------------------------------------------------
        -- B) Source snapshot (preserves your logic, DB-independent)
        --------------------------------------------------------------------
        IF OBJECT_ID('tempdb..#src') IS NOT NULL DROP TABLE #src;

        SELECT
              CAST(PartySiteId AS bigint)                 AS PARTY_SITE_ID
            , CAST(PartyId AS bigint)                     AS PARTY_ID
            , ISNULL(CAST(PartySiteName AS varchar(240)), 'UNK') AS PARTY_SITE_NAME
            , CAST(PartySiteNumber AS varchar(30))        AS PARTY_SITE_NUMBER
            , CAST(LocationId AS bigint)                  AS LOCATION_ID
            , CAST(OverallPrimaryFlag AS varchar(1))      AS OVERALL_PRIMARY_FLAG
            , CAST(ActualContentSource AS varchar(30))    AS ACTUAL_CONTENT_SOURCE
            , CAST(StartDateActive AS date)               AS START_DATE_ACTIVE
            , CAST(EndDateActive AS date)                 AS END_DATE_ACTIVE
            , CAST(Status AS varchar(1))                  AS STATUS
            , CAST(CreatedBy AS varchar(64))              AS CREATED_BY
            , CAST(LastUpdatedBy AS varchar(64))          AS LAST_UPDATE_BY
            , CAST(LastUpdateLogin AS varchar(64))        AS LAST_UPDATE_LOGIN
            , COALESCE(CAST(AddDateTime AS date), CAST(GETDATE() AS date)) AS BZ_LOAD_DATE
            , CAST(GETDATE() AS date)                     AS SV_LOAD_DATE
        INTO #src
        FROM src.bzo_AR_PartySiteExtractPVO;

        DELETE FROM #src WHERE PARTY_SITE_ID IS NULL OR PARTY_SITE_ID = -1;

        --------------------------------------------------------------------
        -- C) Type2 hash (exclude Type1: name, number; exclude load dates)
        --------------------------------------------------------------------
        ALTER TABLE #src ADD HASH_T2 varbinary(32) NULL;

        UPDATE s
            SET s.HASH_T2 = HASHBYTES('SHA2_256', CONCAT(
                  COALESCE(CONVERT(nvarchar(100), s.PARTY_SITE_ID), N''), N'|'
                , COALESCE(CONVERT(nvarchar(100), s.PARTY_ID), N''), N'|'
                , COALESCE(CONVERT(nvarchar(100), s.LOCATION_ID), N''), N'|'
                , COALESCE(CONVERT(nvarchar(50),  s.OVERALL_PRIMARY_FLAG), N''), N'|'
                , COALESCE(CONVERT(nvarchar(100), s.ACTUAL_CONTENT_SOURCE), N''), N'|'
                , COALESCE(CONVERT(nvarchar(10),  s.START_DATE_ACTIVE, 120), N''), N'|'
                , COALESCE(CONVERT(nvarchar(10),  s.END_DATE_ACTIVE, 120), N''), N'|'
                , COALESCE(CONVERT(nvarchar(50),  s.STATUS), N''), N'|'
                , COALESCE(CONVERT(nvarchar(200), s.CREATED_BY), N''), N'|'
                , COALESCE(CONVERT(nvarchar(200), s.LAST_UPDATE_BY), N''), N'|'
                , COALESCE(CONVERT(nvarchar(200), s.LAST_UPDATE_LOGIN), N'')
            ))
        FROM #src s;

        --------------------------------------------------------------------
        -- D) Current target snapshot + Type2 hash
        --------------------------------------------------------------------
        IF OBJECT_ID('tempdb..#tgt') IS NOT NULL DROP TABLE #tgt;

        SELECT
              t.PARTY_SITE_ID
            , t.PARTY_SITE_NAME
            , t.PARTY_SITE_NUMBER
            , HASHBYTES('SHA2_256', CONCAT(
                  COALESCE(CONVERT(nvarchar(100), t.PARTY_SITE_ID), N''), N'|'
                , COALESCE(CONVERT(nvarchar(100), t.PARTY_ID), N''), N'|'
                , COALESCE(CONVERT(nvarchar(100), t.LOCATION_ID), N''), N'|'
                , COALESCE(CONVERT(nvarchar(50),  t.OVERALL_PRIMARY_FLAG), N''), N'|'
                , COALESCE(CONVERT(nvarchar(100), t.ACTUAL_CONTENT_SOURCE), N''), N'|'
                , COALESCE(CONVERT(nvarchar(10),  t.START_DATE_ACTIVE, 120), N''), N'|'
                , COALESCE(CONVERT(nvarchar(10),  t.END_DATE_ACTIVE, 120), N''), N'|'
                , COALESCE(CONVERT(nvarchar(50),  t.STATUS), N''), N'|'
                , COALESCE(CONVERT(nvarchar(200), t.CREATED_BY), N''), N'|'
                , COALESCE(CONVERT(nvarchar(200), t.LAST_UPDATE_BY), N''), N'|'
                , COALESCE(CONVERT(nvarchar(200), t.LAST_UPDATE_LOGIN), N'')
            )) AS HASH_T2
        INTO #tgt
        FROM svo.D_PARTY_SITE t
        WHERE t.CURR_IND = 'Y'
          AND t.END_DATE = @HighDate
          AND t.PARTY_SITE_ID <> -1;

        --------------------------------------------------------------------
        -- E) Type 1 update (name/number) when Type2 unchanged
        --------------------------------------------------------------------
        UPDATE tgt
            SET
                  tgt.PARTY_SITE_NAME   = src.PARTY_SITE_NAME
                , tgt.PARTY_SITE_NUMBER = src.PARTY_SITE_NUMBER
                , tgt.UDT_DATE          = SYSDATETIME()
                , tgt.SV_LOAD_DATE      = CAST(GETDATE() AS date)
                , tgt.BZ_LOAD_DATE      = COALESCE(tgt.BZ_LOAD_DATE, src.BZ_LOAD_DATE, CAST(GETDATE() AS date))
        FROM svo.D_PARTY_SITE tgt
        INNER JOIN #tgt cur ON cur.PARTY_SITE_ID = tgt.PARTY_SITE_ID
        INNER JOIN #src src ON src.PARTY_SITE_ID = cur.PARTY_SITE_ID
        WHERE tgt.CURR_IND = 'Y'
          AND tgt.END_DATE = @HighDate
          AND cur.HASH_T2 = src.HASH_T2
          AND (
                ISNULL(tgt.PARTY_SITE_NAME, '')   <> ISNULL(src.PARTY_SITE_NAME, '')
             OR ISNULL(tgt.PARTY_SITE_NUMBER, '') <> ISNULL(src.PARTY_SITE_NUMBER, '')
          );

        SET @UpdatedT1 = @@ROWCOUNT;

        --------------------------------------------------------------------
        -- F) Type 2 delta: NEW or CHANGED
        --------------------------------------------------------------------
        IF OBJECT_ID('tempdb..#delta_t2') IS NOT NULL DROP TABLE #delta_t2;

        SELECT s.*
        INTO #delta_t2
        FROM #src s
        LEFT JOIN #tgt t ON t.PARTY_SITE_ID = s.PARTY_SITE_ID
        WHERE t.PARTY_SITE_ID IS NULL
           OR t.HASH_T2 <> s.HASH_T2;

        --------------------------------------------------------------------
        -- G) Expire current rows for CHANGED keys only
        --------------------------------------------------------------------
        UPDATE tgt
            SET
                  tgt.END_DATE     = DATEADD(day, -1, @AsOfDate)
                , tgt.CURR_IND     = 'N'
                , tgt.UDT_DATE     = SYSDATETIME()
                , tgt.SV_LOAD_DATE = CAST(GETDATE() AS date)
        FROM svo.D_PARTY_SITE tgt
        INNER JOIN #delta_t2 d ON d.PARTY_SITE_ID = tgt.PARTY_SITE_ID
        WHERE tgt.CURR_IND = 'Y'
          AND tgt.END_DATE = @HighDate
          AND tgt.PARTY_SITE_ID <> -1
          AND EXISTS (SELECT 1 FROM #tgt cur WHERE cur.PARTY_SITE_ID = d.PARTY_SITE_ID);

        SET @Expired = @@ROWCOUNT;

        --------------------------------------------------------------------
        -- H) Insert new current rows (NEW + CHANGED)
        --------------------------------------------------------------------
        INSERT INTO svo.D_PARTY_SITE
        (
              PARTY_SITE_ID
            , PARTY_ID
            , PARTY_SITE_NAME
            , PARTY_SITE_NUMBER
            , LOCATION_ID
            , OVERALL_PRIMARY_FLAG
            , ACTUAL_CONTENT_SOURCE
            , START_DATE_ACTIVE
            , END_DATE_ACTIVE
            , STATUS
            , CREATED_BY
            , LAST_UPDATE_BY
            , LAST_UPDATE_LOGIN
            , BZ_LOAD_DATE
            , SV_LOAD_DATE
            , EFF_DATE
            , END_DATE
            , CRE_DATE
            , UDT_DATE
            , CURR_IND
        )
        SELECT
              d.PARTY_SITE_ID
            , d.PARTY_ID
            , d.PARTY_SITE_NAME
            , d.PARTY_SITE_NUMBER
            , d.LOCATION_ID
            , d.OVERALL_PRIMARY_FLAG
            , d.ACTUAL_CONTENT_SOURCE
            , d.START_DATE_ACTIVE
            , d.END_DATE_ACTIVE
            , d.STATUS
            , d.CREATED_BY
            , d.LAST_UPDATE_BY
            , d.LAST_UPDATE_LOGIN
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

