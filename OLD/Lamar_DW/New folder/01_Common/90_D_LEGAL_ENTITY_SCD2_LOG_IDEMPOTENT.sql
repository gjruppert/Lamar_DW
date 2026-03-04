USE [Oracle_Reporting_P2];
GO

/* ============================================================
   svo.D_LEGAL_ENTITY - Hybrid SCD2
   BK: LEGAL_ENTITY_ID
   Type 1: LEGAL_ENTITY_IDENTIFIER, LEGAL_ENTITY_NAME
   Type 2: all other attributes in the original target list
   Source: src.bzo_GL_LegalEntityExtractPVO  (synonym-based)
   BZ_LOAD_DATE never NULL: COALESCE(CAST(AddDateTime AS date), CAST(GETDATE() AS date))
   ============================================================ */

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

/* Ensure standard SCD columns exist (add only if missing) */
IF COL_LENGTH('svo.D_LEGAL_ENTITY', 'EFF_DATE') IS NULL
    ALTER TABLE svo.D_LEGAL_ENTITY ADD EFF_DATE date NOT NULL
        CONSTRAINT DF_D_LEGAL_ENTITY_EFF_DATE DEFAULT (CAST('0001-01-01' AS date));
GO
IF COL_LENGTH('svo.D_LEGAL_ENTITY', 'END_DATE') IS NULL
    ALTER TABLE svo.D_LEGAL_ENTITY ADD END_DATE date NOT NULL
        CONSTRAINT DF_D_LEGAL_ENTITY_END_DATE DEFAULT (CAST('9999-12-31' AS date));
GO
IF COL_LENGTH('svo.D_LEGAL_ENTITY', 'CRE_DATE') IS NULL
    ALTER TABLE svo.D_LEGAL_ENTITY ADD CRE_DATE datetime2(0) NOT NULL
        CONSTRAINT DF_D_LEGAL_ENTITY_CRE_DATE DEFAULT (SYSDATETIME());
GO
IF COL_LENGTH('svo.D_LEGAL_ENTITY', 'UDT_DATE') IS NULL
    ALTER TABLE svo.D_LEGAL_ENTITY ADD UDT_DATE datetime2(0) NOT NULL
        CONSTRAINT DF_D_LEGAL_ENTITY_UDT_DATE DEFAULT (SYSDATETIME());
GO
IF COL_LENGTH('svo.D_LEGAL_ENTITY', 'CURR_IND') IS NULL
    ALTER TABLE svo.D_LEGAL_ENTITY ADD CURR_IND char(1) NOT NULL
        CONSTRAINT DF_D_LEGAL_ENTITY_CURR_IND DEFAULT ('Y');
GO

/* Replace legacy unique index (breaks history) with filtered current-row unique index */
IF EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'UX_D_LEGAL_ENTITY_ID' AND object_id = OBJECT_ID('svo.D_LEGAL_ENTITY'))
    DROP INDEX UX_D_LEGAL_ENTITY_ID ON svo.D_LEGAL_ENTITY;
GO

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'UX_D_LEGAL_ENTITY_ID_CURR' AND object_id = OBJECT_ID('svo.D_LEGAL_ENTITY'))
BEGIN
    CREATE UNIQUE NONCLUSTERED INDEX UX_D_LEGAL_ENTITY_ID_CURR
        ON svo.D_LEGAL_ENTITY(LEGAL_ENTITY_ID)
        WHERE CURR_IND = 'Y' AND END_DATE = '9999-12-31'
        ON FG_SilverDim;
END
GO

CREATE OR ALTER PROCEDURE svo.usp_Load_D_LEGAL_ENTITY_SCD2
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
        , @TargetObject sysname = 'svo.D_LEGAL_ENTITY';

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
        IF NOT EXISTS (SELECT 1 FROM svo.D_LEGAL_ENTITY WHERE LEGAL_ENTITY_SK = 0)
        BEGIN
            SET IDENTITY_INSERT svo.D_LEGAL_ENTITY ON;

            INSERT INTO svo.D_LEGAL_ENTITY
            (
                  LEGAL_ENTITY_SK
                , LEGAL_ENTITY_ID
                , LEGAL_ENTITY_ENTERPRISE_ID
                , LEGAL_ENTITY_GEOGRAPHY_ID
                , LEGAL_ENTITY_LEGAL_EMPLOYER_FLAG
                , LEGAL_ENTITY_IDENTIFIER
                , LEGAL_ENTITY_NAME
                , LEGAL_ENTITY_OBJECT_VERSION_NUMBER
                , LEGAL_ENTITY_PARTY_ID
                , LEGAL_ENTITY_PSU_FLAG
                , LEGAL_ENTITY_TRANSACTING_ENTITY_FLAG
                , LEGAL_ENTITY_LAST_UPDATE_DATE
                , LEGAL_ENTITY_LAST_UPDATE_LOGIN
                , LEGAL_ENTITY_LAST_UPDATED_BY
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
                , 'Unknown'
                , 'Unknown'
                , -1
                , -1
                , 'U'
                , 'U'
                , CAST('0001-01-01' AS date)
                , 'Unknown'
                , 'Unknown'
                , CAST(GETDATE() AS date)
                , CAST(GETDATE() AS date)
                , CAST(GETDATE() AS date)
                , @HighDate
                , SYSDATETIME()
                , SYSDATETIME()
                , 'Y'
            );

            SET IDENTITY_INSERT svo.D_LEGAL_ENTITY OFF;
        END

        /* Source snapshot (every column explicitly named; BZ hardened) */
        IF OBJECT_ID('tempdb..#src') IS NOT NULL DROP TABLE #src;

        SELECT
              LEGALENTITYLEGALENTITYID            AS LEGAL_ENTITY_ID
            , LEGALENTITYENTERPRISEID             AS LEGAL_ENTITY_ENTERPRISE_ID
            , LEGALENTITYGEOGRAPHYID              AS LEGAL_ENTITY_GEOGRAPHY_ID
            , LEGALENTITYLEGALEMPLOYERFLAG        AS LEGAL_ENTITY_LEGAL_EMPLOYER_FLAG
            , LEGALENTITYLEGALENTITYIDENTIFIER    AS LEGAL_ENTITY_IDENTIFIER
            , LEGALENTITYNAME                     AS LEGAL_ENTITY_NAME
            , LEGALENTITYOBJECTVERSIONNUMBER      AS LEGAL_ENTITY_OBJECT_VERSION_NUMBER
            , LEGALENTITYPARTYID                  AS LEGAL_ENTITY_PARTY_ID
            , LEGALENTITYPSUFLAG                  AS LEGAL_ENTITY_PSU_FLAG
            , LEGALENTITYTRANSACTINGENTITYFLAG    AS LEGAL_ENTITY_TRANSACTING_ENTITY_FLAG
            , COALESCE(CAST(LEGALENTITYLASTUPDATEDATE AS date), CAST('0001-01-01' AS date)) AS LEGAL_ENTITY_LAST_UPDATE_DATE
            , LEGALENTITYLASTUPDATELOGIN          AS LEGAL_ENTITY_LAST_UPDATE_LOGIN
            , LEGALENTITYLASTUPDATEDBY            AS LEGAL_ENTITY_LAST_UPDATED_BY
            , COALESCE(CAST(AddDateTime AS date), CAST(GETDATE() AS date)) AS BZ_LOAD_DATE
            , CAST(GETDATE() AS date)             AS SV_LOAD_DATE
        INTO #src
        FROM src.bzo_GL_LegalEntityExtractPVO;

        DELETE FROM #src WHERE LEGAL_ENTITY_ID IS NULL OR LEGAL_ENTITY_ID = -1;

        /* Type2 hash (exclude Type1 cols) */
        ALTER TABLE #src ADD HASH_T2 varbinary(32) NULL;

        UPDATE s
            SET s.HASH_T2 = HASHBYTES('SHA2_256', CONCAT(
                  COALESCE(CONVERT(nvarchar(100), s.LEGAL_ENTITY_ID), N''), N'|'
                , COALESCE(CONVERT(nvarchar(100), s.LEGAL_ENTITY_ENTERPRISE_ID), N''), N'|'
                , COALESCE(CONVERT(nvarchar(100), s.LEGAL_ENTITY_GEOGRAPHY_ID), N''), N'|'
                , COALESCE(CONVERT(nvarchar(100), s.LEGAL_ENTITY_LEGAL_EMPLOYER_FLAG), N''), N'|'
                , COALESCE(CONVERT(nvarchar(100), s.LEGAL_ENTITY_OBJECT_VERSION_NUMBER), N''), N'|'
                , COALESCE(CONVERT(nvarchar(100), s.LEGAL_ENTITY_PARTY_ID), N''), N'|'
                , COALESCE(CONVERT(nvarchar(100), s.LEGAL_ENTITY_PSU_FLAG), N''), N'|'
                , COALESCE(CONVERT(nvarchar(100), s.LEGAL_ENTITY_TRANSACTING_ENTITY_FLAG), N''), N'|'
                , CONVERT(nvarchar(10), s.LEGAL_ENTITY_LAST_UPDATE_DATE, 120), N'|'
                , COALESCE(CONVERT(nvarchar(100), s.LEGAL_ENTITY_LAST_UPDATE_LOGIN), N''), N'|'
                , COALESCE(CONVERT(nvarchar(200), s.LEGAL_ENTITY_LAST_UPDATED_BY), N'')
            ))
        FROM #src s;

        /* Current target snapshot */
        IF OBJECT_ID('tempdb..#tgt') IS NOT NULL DROP TABLE #tgt;

        SELECT
              t.LEGAL_ENTITY_ID
            , t.LEGAL_ENTITY_IDENTIFIER
            , t.LEGAL_ENTITY_NAME
            , HASHBYTES('SHA2_256', CONCAT(
                  COALESCE(CONVERT(nvarchar(100), t.LEGAL_ENTITY_ID), N''), N'|'
                , COALESCE(CONVERT(nvarchar(100), t.LEGAL_ENTITY_ENTERPRISE_ID), N''), N'|'
                , COALESCE(CONVERT(nvarchar(100), t.LEGAL_ENTITY_GEOGRAPHY_ID), N''), N'|'
                , COALESCE(CONVERT(nvarchar(100), t.LEGAL_ENTITY_LEGAL_EMPLOYER_FLAG), N''), N'|'
                , COALESCE(CONVERT(nvarchar(100), t.LEGAL_ENTITY_OBJECT_VERSION_NUMBER), N''), N'|'
                , COALESCE(CONVERT(nvarchar(100), t.LEGAL_ENTITY_PARTY_ID), N''), N'|'
                , COALESCE(CONVERT(nvarchar(100), t.LEGAL_ENTITY_PSU_FLAG), N''), N'|'
                , COALESCE(CONVERT(nvarchar(100), t.LEGAL_ENTITY_TRANSACTING_ENTITY_FLAG), N''), N'|'
                , CONVERT(nvarchar(10), t.LEGAL_ENTITY_LAST_UPDATE_DATE, 120), N'|'
                , COALESCE(CONVERT(nvarchar(100), t.LEGAL_ENTITY_LAST_UPDATE_LOGIN), N''), N'|'
                , COALESCE(CONVERT(nvarchar(200), t.LEGAL_ENTITY_LAST_UPDATED_BY), N'')
            )) AS HASH_T2
        INTO #tgt
        FROM svo.D_LEGAL_ENTITY t
        WHERE t.CURR_IND = 'Y'
          AND t.END_DATE = @HighDate
          AND t.LEGAL_ENTITY_ID <> -1;

        /* Type 1 updates (only if Type2 unchanged) */
        UPDATE tgt
            SET
                  tgt.LEGAL_ENTITY_IDENTIFIER = src.LEGAL_ENTITY_IDENTIFIER
                , tgt.LEGAL_ENTITY_NAME       = src.LEGAL_ENTITY_NAME
                , tgt.UDT_DATE                = SYSDATETIME()
                , tgt.SV_LOAD_DATE            = CAST(GETDATE() AS date)
                , tgt.BZ_LOAD_DATE            = COALESCE(tgt.BZ_LOAD_DATE, src.BZ_LOAD_DATE, CAST(GETDATE() AS date))
        FROM svo.D_LEGAL_ENTITY tgt
        INNER JOIN #tgt cur ON cur.LEGAL_ENTITY_ID = tgt.LEGAL_ENTITY_ID
        INNER JOIN #src src ON src.LEGAL_ENTITY_ID = cur.LEGAL_ENTITY_ID
        WHERE tgt.CURR_IND = 'Y'
          AND tgt.END_DATE = @HighDate
          AND cur.HASH_T2 = src.HASH_T2
          AND (
                ISNULL(tgt.LEGAL_ENTITY_IDENTIFIER, '') <> ISNULL(src.LEGAL_ENTITY_IDENTIFIER, '')
             OR ISNULL(tgt.LEGAL_ENTITY_NAME, '')       <> ISNULL(src.LEGAL_ENTITY_NAME, '')
          );

        SET @UpdatedT1 = @@ROWCOUNT;

        /* Delta for Type2 (new or changed) */
        IF OBJECT_ID('tempdb..#delta_t2') IS NOT NULL DROP TABLE #delta_t2;

        SELECT s.*
        INTO #delta_t2
        FROM #src s
        LEFT JOIN #tgt t ON t.LEGAL_ENTITY_ID = s.LEGAL_ENTITY_ID
        WHERE t.LEGAL_ENTITY_ID IS NULL
           OR t.HASH_T2 <> s.HASH_T2;

        /* Expire current rows for changed keys */
        UPDATE tgt
            SET
                  tgt.END_DATE     = DATEADD(day, -1, @AsOfDate)
                , tgt.CURR_IND     = 'N'
                , tgt.UDT_DATE     = SYSDATETIME()
                , tgt.SV_LOAD_DATE = CAST(GETDATE() AS date)
        FROM svo.D_LEGAL_ENTITY tgt
        INNER JOIN #delta_t2 d ON d.LEGAL_ENTITY_ID = tgt.LEGAL_ENTITY_ID
        WHERE tgt.CURR_IND = 'Y'
          AND tgt.END_DATE = @HighDate
          AND tgt.LEGAL_ENTITY_ID <> -1
          AND EXISTS (SELECT 1 FROM #tgt cur WHERE cur.LEGAL_ENTITY_ID = d.LEGAL_ENTITY_ID);

        SET @Expired = @@ROWCOUNT;

        /* Insert new current rows */
        INSERT INTO svo.D_LEGAL_ENTITY
        (
              LEGAL_ENTITY_ID
            , LEGAL_ENTITY_ENTERPRISE_ID
            , LEGAL_ENTITY_GEOGRAPHY_ID
            , LEGAL_ENTITY_LEGAL_EMPLOYER_FLAG
            , LEGAL_ENTITY_IDENTIFIER
            , LEGAL_ENTITY_NAME
            , LEGAL_ENTITY_OBJECT_VERSION_NUMBER
            , LEGAL_ENTITY_PARTY_ID
            , LEGAL_ENTITY_PSU_FLAG
            , LEGAL_ENTITY_TRANSACTING_ENTITY_FLAG
            , LEGAL_ENTITY_LAST_UPDATE_DATE
            , LEGAL_ENTITY_LAST_UPDATE_LOGIN
            , LEGAL_ENTITY_LAST_UPDATED_BY
            , BZ_LOAD_DATE
            , SV_LOAD_DATE
            , EFF_DATE
            , END_DATE
            , CRE_DATE
            , UDT_DATE
            , CURR_IND
        )
        SELECT
              d.LEGAL_ENTITY_ID
            , d.LEGAL_ENTITY_ENTERPRISE_ID
            , d.LEGAL_ENTITY_GEOGRAPHY_ID
            , d.LEGAL_ENTITY_LEGAL_EMPLOYER_FLAG
            , d.LEGAL_ENTITY_IDENTIFIER
            , d.LEGAL_ENTITY_NAME
            , d.LEGAL_ENTITY_OBJECT_VERSION_NUMBER
            , d.LEGAL_ENTITY_PARTY_ID
            , d.LEGAL_ENTITY_PSU_FLAG
            , d.LEGAL_ENTITY_TRANSACTING_ENTITY_FLAG
            , d.LEGAL_ENTITY_LAST_UPDATE_DATE
            , d.LEGAL_ENTITY_LAST_UPDATE_LOGIN
            , d.LEGAL_ENTITY_LAST_UPDATED_BY
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

