USE [Oracle_Reporting_P2];
GO
/* =====================================================================
   GL SUBJECT AREA - D_GL_HEADER
   - SCD2 + Logging + Idempotent loader
   - Stored procedure in svo schema
   - Preserves bzo.* source references (synonym-friendly)
   - SCD2 fields: EFF_DATE, END_DATE, CRE_DATE, UDT_DATE, CURR_IND
   - Load dates:
       BZ_LOAD_DATE = COALESCE(CAST(AddDateTime AS date), CAST(GETDATE() AS date))  (never NULL)
       SV_LOAD_DATE = CAST(GETDATE() AS date)
   ===================================================================== */

-------------------------------------------------------------------------------
-- 0) Logging objects (create once)
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
-- 1) DDL: svo.D_GL_HEADER  (SCD2)
-------------------------------------------------------------------------------
IF OBJECT_ID(N'svo.D_GL_HEADER', 'U') IS NOT NULL
    DROP TABLE svo.D_GL_HEADER;
GO

CREATE TABLE svo.D_GL_HEADER
(
    GL_HEADER_SK                BIGINT IDENTITY(1,1) NOT NULL,

    -- BK
    JE_HEADER_ID                BIGINT        NOT NULL,

    -- FKs / attributes
    LEDGER_SK                   BIGINT        NOT NULL,
    POSTED_DATE_SK              INT           NOT NULL,
    DEFAULT_EFFECTIVE_DATE_SK   INT           NOT NULL,

    GL_HEADER_NAME              VARCHAR(100)  NOT NULL,
    GL_HEADER_STATUS            VARCHAR(4)    NOT NULL,
    GL_HEADER_DESCRIPTION       VARCHAR(240)  NOT NULL,
    ACCRUAL_REV_CHANGESIGN_FLAG VARCHAR(4)    NOT NULL,
    ACTUAL_FLAG                 VARCHAR(4)    NOT NULL,
    BALANCED_JE_FLAG            VARCHAR(4)    NOT NULL,

    CURRENCY_CODE               VARCHAR(15)   NOT NULL,
    CURRENCY_CONVERSION_DATE    DATE          NOT NULL,
    CURRENCY_CONVERSION_RATE    FLOAT         NOT NULL,
    CURRENCY_CONVERSION_TYPE    VARCHAR(30)   NOT NULL,

    EXTERNAL_REFERENCE          VARCHAR(80)   NOT NULL,

    JE_BATCH_ID                 BIGINT        NOT NULL,
    CATEGORY                    VARCHAR(25)   NOT NULL,
    JE_SOURCE                   VARCHAR(25)   NOT NULL,

    MULTI_CURRENCY_FLAG         VARCHAR(4)    NOT NULL,
    POST_CURRENCY_CODE          VARCHAR(10)   NOT NULL,
    POST_MULTI_CURRENCY_FLAG    VARCHAR(4)    NOT NULL,

    RUNNING_TOTAL_CR            FLOAT         NOT NULL,
    RUNNING_TOTAL_DR            FLOAT         NOT NULL,
    RUNNING_TOTAL_ACCOUNTED_CR  FLOAT         NOT NULL,
    RUNNING_TOTAL_ACCOUNTED_DR  FLOAT         NOT NULL,
    CR_DR_VARIANCE              FLOAT         NOT NULL,

    DATE_CREATED                DATE          NOT NULL,
    CREATED_BY                  VARCHAR(32)   NOT NULL,
    CREATION_DATE               DATE          NOT NULL,
    LAST_UPDATE_DATE            DATE          NOT NULL,
    LAST_UPDATE_LOGIN           VARCHAR(32)   NOT NULL,
    LAST_UPDATED_BY             VARCHAR(64)   NOT NULL,

    -- SCD2 fields
    EFF_DATE                    DATE          NOT NULL CONSTRAINT DF_D_GL_HEADER_EFF_DATE  DEFAULT (CAST(GETDATE() AS date)),
    END_DATE                    DATE          NOT NULL CONSTRAINT DF_D_GL_HEADER_END_DATE  DEFAULT (CONVERT(date,'9999-12-31')),
    CRE_DATE                    DATETIME2(0)  NOT NULL CONSTRAINT DF_D_GL_HEADER_CRE_DATE  DEFAULT (SYSDATETIME()),
    UDT_DATE                    DATETIME2(0)  NOT NULL CONSTRAINT DF_D_GL_HEADER_UDT_DATE  DEFAULT (SYSDATETIME()),
    CURR_IND                    CHAR(1)       NOT NULL CONSTRAINT DF_D_GL_HEADER_CURR_IND  DEFAULT ('Y'),

    -- Load dates
    BZ_LOAD_DATE                DATE          NOT NULL CONSTRAINT DF_D_GL_HEADER_BZ_LOAD_DATE DEFAULT (CAST(GETDATE() AS date)),
    SV_LOAD_DATE                DATE          NOT NULL CONSTRAINT DF_D_GL_HEADER_SV_LOAD_DATE DEFAULT (CAST(GETDATE() AS date)),

    CONSTRAINT PK_D_GL_HEADER PRIMARY KEY CLUSTERED (GL_HEADER_SK) ON FG_SilverDim
) ON FG_SilverDim;
GO

-- One current row per BK
CREATE UNIQUE NONCLUSTERED INDEX UX_D_GL_HEADER_JE_HEADER_ID_CURR
ON svo.D_GL_HEADER (JE_HEADER_ID)
WHERE CURR_IND = 'Y'
ON FG_SilverDim;
GO

-- Helpful lookups
CREATE NONCLUSTERED INDEX IX_D_GL_HEADER_LEDGER_SK_CURR
ON svo.D_GL_HEADER (LEDGER_SK, CURR_IND)
ON FG_SilverDim;
GO

-- Plug row (SK=0)
IF NOT EXISTS (SELECT 1 FROM svo.D_GL_HEADER WHERE GL_HEADER_SK = 0)
BEGIN
    SET IDENTITY_INSERT svo.D_GL_HEADER ON;

    INSERT INTO svo.D_GL_HEADER
    (
        GL_HEADER_SK,
        JE_HEADER_ID,
        LEDGER_SK,
        POSTED_DATE_SK,
        DEFAULT_EFFECTIVE_DATE_SK,
        GL_HEADER_NAME,
        GL_HEADER_STATUS,
        GL_HEADER_DESCRIPTION,
        ACCRUAL_REV_CHANGESIGN_FLAG,
        ACTUAL_FLAG,
        BALANCED_JE_FLAG,
        CURRENCY_CODE,
        CURRENCY_CONVERSION_DATE,
        CURRENCY_CONVERSION_RATE,
        CURRENCY_CONVERSION_TYPE,
        EXTERNAL_REFERENCE,
        JE_BATCH_ID,
        CATEGORY,
        JE_SOURCE,
        MULTI_CURRENCY_FLAG,
        POST_CURRENCY_CODE,
        POST_MULTI_CURRENCY_FLAG,
        RUNNING_TOTAL_CR,
        RUNNING_TOTAL_DR,
        RUNNING_TOTAL_ACCOUNTED_CR,
        RUNNING_TOTAL_ACCOUNTED_DR,
        CR_DR_VARIANCE,
        DATE_CREATED,
        CREATED_BY,
        CREATION_DATE,
        LAST_UPDATE_DATE,
        LAST_UPDATE_LOGIN,
        LAST_UPDATED_BY,
        EFF_DATE,
        END_DATE,
        CRE_DATE,
        UDT_DATE,
        CURR_IND,
        BZ_LOAD_DATE,
        SV_LOAD_DATE
    )
    VALUES
    (
        0,
        -1,
        0,
        0,
        0,
        'UNKNOWN',
        'UNK',
        'Unknown GL Header',
        'UNK',
        'UNK',
        'UNK',
        'UNK',
        CONVERT(date,'0001-01-01'),
        0,
        'UNK',
        'UNK',
        0,
        'UNK',
        'UNK',
        'UNK',
        'UNK',
        'UNK',
        0,
        0,
        0,
        0,
        0,
        CONVERT(date,'0001-01-01'),
        'SYSTEM',
        CONVERT(date,'0001-01-01'),
        CONVERT(date,'0001-01-01'),
        'UNK',
        'SYSTEM',
        CONVERT(date,'0001-01-01'),
        CONVERT(date,'9999-12-31'),
        SYSDATETIME(),
        SYSDATETIME(),
        'Y',
        CONVERT(date,'0001-01-01'),
        CAST(GETDATE() AS date)
    );

    SET IDENTITY_INSERT svo.D_GL_HEADER OFF;
END
GO

-------------------------------------------------------------------------------
-- 2) Loader Proc: svo.usp_Load_D_GL_HEADER_SCD2
-------------------------------------------------------------------------------
CREATE OR ALTER PROCEDURE svo.usp_Load_D_GL_HEADER_SCD2
(
      @AsOfDate date = NULL
)
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    IF @AsOfDate IS NULL
        SET @AsOfDate = CAST(GETDATE() AS date);

    DECLARE
          @HighDate     date    = CONVERT(date,'9999-12-31')
        , @RunId        bigint
        , @ProcName     sysname = OBJECT_SCHEMA_NAME(@@PROCID) + '.' + OBJECT_NAME(@@PROCID)
        , @TargetObject sysname = 'svo.D_GL_HEADER'
        , @Inserted     int     = 0
        , @Expired      int     = 0;

    INSERT INTO etl.ETL_RUN (PROC_NAME, TARGET_OBJECT, ASOF_DATE, START_DTTM, STATUS)
    VALUES (@ProcName, @TargetObject, @AsOfDate, SYSDATETIME(), 'STARTED');

    SET @RunId = SCOPE_IDENTITY();

    BEGIN TRY
        BEGIN TRAN;

        IF OBJECT_ID('tempdb..#src') IS NOT NULL DROP TABLE #src;
        IF OBJECT_ID('tempdb..#chg') IS NOT NULL DROP TABLE #chg;

        /* -----------------------
           Source query (preserved)
           ----------------------- */
        SELECT
              ISNULL(H.JEHEADERID, 0) AS JE_HEADER_ID
            , ISNULL(LGR.LEDGER_SK, 0) AS LEDGER_SK
            , ISNULL(CAST(CONVERT(char(8), TRY_CONVERT(date, H.GLJEHEADERSPOSTEDDATE), 112) AS int), 0) AS POSTED_DATE_SK
            , ISNULL(CAST(CONVERT(char(8), TRY_CONVERT(date, H.GLJEHEADERSDEFAULTEFFECTIVEDATE), 112) AS int), 0) AS DEFAULT_EFFECTIVE_DATE_SK
            , ISNULL(H.GLJEHEADERSNAME, 'NULL') AS GL_HEADER_NAME
            , ISNULL(H.GLJEHEADERSSTATUS, 'NULL') AS GL_HEADER_STATUS
            , ISNULL(H.GLJEHEADERSDESCRIPTION, 'NULL') AS GL_HEADER_DESCRIPTION
            , ISNULL(H.GLJEHEADERSACCRUALREVCHANGESIGNFLAG, 'NULL') AS ACCRUAL_REV_CHANGESIGN_FLAG
            , ISNULL(H.GLJEHEADERSACTUALFLAG, 'NULL') AS ACTUAL_FLAG
            , ISNULL(H.GLJEHEADERSBALANCEDJEFLAG, 'NULL') AS BALANCED_JE_FLAG
            , ISNULL(H.GLJEHEADERSCURRENCYCODE, 'NULL') AS CURRENCY_CODE
            , CAST(ISNULL(H.GLJEHEADERSCURRENCYCONVERSIONDATE, '1900-01-01') AS date) AS CURRENCY_CONVERSION_DATE
            , ISNULL(H.GLJEHEADERSCURRENCYCONVERSIONRATE, 0) AS CURRENCY_CONVERSION_RATE
            , ISNULL(H.GLJEHEADERSCURRENCYCONVERSIONTYPE, 'NULL') AS CURRENCY_CONVERSION_TYPE
            , ISNULL(H.GLJEHEADERSEXTERNALREFERENCE, 'NULL') AS EXTERNAL_REFERENCE
            , ISNULL(H.GLJEHEADERSJEBATCHID, 0) AS JE_BATCH_ID
            , ISNULL(cat.JournalCategoryJeCategoryKey,'NULL') AS CATEGORY
            , ISNULL(src.JournalSourceJeSourceKey,'NULL') AS JE_SOURCE
            , ISNULL(H.GLJEHEADERSMULTICURRENCYFLAG, 'NULL') AS MULTI_CURRENCY_FLAG
            , ISNULL(H.GLJEHEADERSPOSTCURRENCYCODE, 'NULL') AS POST_CURRENCY_CODE
            , ISNULL(H.GLJEHEADERSPOSTMULTICURRENCYFLAG, 'NULL') AS POST_MULTI_CURRENCY_FLAG
            , CAST(ISNULL(H.GLJEHEADERSRUNNINGTOTALCR, 0) AS float) AS RUNNING_TOTAL_CR
            , CAST(ISNULL(H.GLJEHEADERSRUNNINGTOTALDR, 0) AS float) AS RUNNING_TOTAL_DR
            , CAST(ISNULL(H.GLJEHEADERSRUNNINGTOTALACCOUNTEDCR, 0) AS float) AS RUNNING_TOTAL_ACCOUNTED_CR
            , CAST(ISNULL(H.GLJEHEADERSRUNNINGTOTALACCOUNTEDDR, 0) AS float) AS RUNNING_TOTAL_ACCOUNTED_DR
            , CAST(ISNULL(H.GLJEHEADERSRUNNINGTOTALACCOUNTEDCR, 0) AS float)
              - CAST(ISNULL(H.GLJEHEADERSRUNNINGTOTALACCOUNTEDDR, 0) AS float) AS CR_DR_VARIANCE
            , CAST(ISNULL(H.GLJEHEADERSDATECREATED, '1900-01-01') AS date) AS DATE_CREATED
            , ISNULL(H.GLJEHEADERSCREATEDBY, 'NULL') AS CREATED_BY
            , CAST(ISNULL(H.GLJEHEADERSCREATIONDATE, '1900-01-01') AS date) AS CREATION_DATE
            , CAST(ISNULL(H.GLJEHEADERSLASTUPDATEDATE, '1900-01-01') AS date) AS LAST_UPDATE_DATE
            , ISNULL(H.GLJEHEADERSLASTUPDATELOGIN, 'NULL') AS LAST_UPDATE_LOGIN
            , ISNULL(H.GLJEHEADERSLASTUPDATEDBY, 'NULL') AS LAST_UPDATED_BY
            , COALESCE(CAST(H.AddDateTime AS date), CAST(GETDATE() AS date)) AS BZ_LOAD_DATE
            , CAST(GETDATE() AS date) AS SV_LOAD_DATE

            , HASHBYTES('SHA2_256',
                CONCAT_WS('|',
                    CAST(ISNULL(LGR.LEDGER_SK,0) AS varchar(20)),
                    CAST(ISNULL(CAST(CONVERT(char(8), TRY_CONVERT(date, H.GLJEHEADERSPOSTEDDATE), 112) AS int),0) AS varchar(20)),
                    CAST(ISNULL(CAST(CONVERT(char(8), TRY_CONVERT(date, H.GLJEHEADERSDEFAULTEFFECTIVEDATE), 112) AS int),0) AS varchar(20)),
                    ISNULL(H.GLJEHEADERSNAME,'NULL'),
                    ISNULL(H.GLJEHEADERSSTATUS,'NULL'),
                    ISNULL(H.GLJEHEADERSDESCRIPTION,'NULL'),
                    ISNULL(H.GLJEHEADERSACCRUALREVCHANGESIGNFLAG,'NULL'),
                    ISNULL(H.GLJEHEADERSACTUALFLAG,'NULL'),
                    ISNULL(H.GLJEHEADERSBALANCEDJEFLAG,'NULL'),
                    ISNULL(H.GLJEHEADERSCURRENCYCODE,'NULL'),
                    CONVERT(char(10), CAST(ISNULL(H.GLJEHEADERSCURRENCYCONVERSIONDATE,'1900-01-01') AS date), 120),
                    CAST(ISNULL(H.GLJEHEADERSCURRENCYCONVERSIONRATE,0) AS varchar(64)),
                    ISNULL(H.GLJEHEADERSCURRENCYCONVERSIONTYPE,'NULL'),
                    ISNULL(H.GLJEHEADERSEXTERNALREFERENCE,'NULL'),
                    CAST(ISNULL(H.GLJEHEADERSJEBATCHID,0) AS varchar(20)),
                    ISNULL(cat.JournalCategoryJeCategoryKey,'NULL'),
                    ISNULL(src.JournalSourceJeSourceKey,'NULL'),
                    ISNULL(H.GLJEHEADERSMULTICURRENCYFLAG,'NULL'),
                    ISNULL(H.GLJEHEADERSPOSTCURRENCYCODE,'NULL'),
                    ISNULL(H.GLJEHEADERSPOSTMULTICURRENCYFLAG,'NULL'),
                    CAST(ISNULL(H.GLJEHEADERSRUNNINGTOTALCR,0) AS varchar(64)),
                    CAST(ISNULL(H.GLJEHEADERSRUNNINGTOTALDR,0) AS varchar(64)),
                    CAST(ISNULL(H.GLJEHEADERSRUNNINGTOTALACCOUNTEDCR,0) AS varchar(64)),
                    CAST(ISNULL(H.GLJEHEADERSRUNNINGTOTALACCOUNTEDDR,0) AS varchar(64)),
                    CONVERT(char(10), CAST(ISNULL(H.GLJEHEADERSDATECREATED,'1900-01-01') AS date), 120),
                    ISNULL(H.GLJEHEADERSCREATEDBY,'NULL'),
                    CONVERT(char(10), CAST(ISNULL(H.GLJEHEADERSCREATIONDATE,'1900-01-01') AS date), 120),
                    CONVERT(char(10), CAST(ISNULL(H.GLJEHEADERSLASTUPDATEDATE,'1900-01-01') AS date), 120),
                    ISNULL(H.GLJEHEADERSLASTUPDATELOGIN,'NULL'),
                    ISNULL(H.GLJEHEADERSLASTUPDATEDBY,'NULL')
                )
            ) AS ROW_HASH
        INTO #src
        FROM src.bzo_GL_JournalHeaderExtractPVO AS H
        LEFT JOIN svo.D_LEDGER AS LGR
               ON LGR.LEDGER_ID = H.GLJEHEADERSLEDGERID
              AND LGR.CURR_IND = 'Y'
        LEFT JOIN src.bzo_GL_JournalCategoryExtractPVO AS cat
               ON H.GlJeHeadersJeCategory = cat.JournalCategoryJeCategoryName
        LEFT JOIN src.bzo_GL_JournalSourceExtractPVO AS src
               ON H.GlJeHeadersJeSource = src.JournalSourceJeSourceName
        WHERE H.JEHEADERID IS NOT NULL;

        /* Identify BKs that are new or changed vs current row */
        SELECT
              s.JE_HEADER_ID
            , s.ROW_HASH
        INTO #chg
        FROM #src s
        LEFT JOIN (
            SELECT
                  d.JE_HEADER_ID
                , HASHBYTES('SHA2_256',
                    CONCAT_WS('|',
                        CAST(d.LEDGER_SK AS varchar(20)),
                        CAST(d.POSTED_DATE_SK AS varchar(20)),
                        CAST(d.DEFAULT_EFFECTIVE_DATE_SK AS varchar(20)),
                        d.GL_HEADER_NAME,
                        d.GL_HEADER_STATUS,
                        d.GL_HEADER_DESCRIPTION,
                        d.ACCRUAL_REV_CHANGESIGN_FLAG,
                        d.ACTUAL_FLAG,
                        d.BALANCED_JE_FLAG,
                        d.CURRENCY_CODE,
                        CONVERT(char(10), d.CURRENCY_CONVERSION_DATE, 120),
                        CAST(d.CURRENCY_CONVERSION_RATE AS varchar(64)),
                        d.CURRENCY_CONVERSION_TYPE,
                        d.EXTERNAL_REFERENCE,
                        CAST(d.JE_BATCH_ID AS varchar(20)),
                        d.CATEGORY,
                        d.JE_SOURCE,
                        d.MULTI_CURRENCY_FLAG,
                        d.POST_CURRENCY_CODE,
                        d.POST_MULTI_CURRENCY_FLAG,
                        CAST(d.RUNNING_TOTAL_CR AS varchar(64)),
                        CAST(d.RUNNING_TOTAL_DR AS varchar(64)),
                        CAST(d.RUNNING_TOTAL_ACCOUNTED_CR AS varchar(64)),
                        CAST(d.RUNNING_TOTAL_ACCOUNTED_DR AS varchar(64)),
                        CONVERT(char(10), d.DATE_CREATED, 120),
                        d.CREATED_BY,
                        CONVERT(char(10), d.CREATION_DATE, 120),
                        CONVERT(char(10), d.LAST_UPDATE_DATE, 120),
                        d.LAST_UPDATE_LOGIN,
                        d.LAST_UPDATED_BY
                    )
                ) AS CURR_HASH
            FROM svo.D_GL_HEADER d
            WHERE d.CURR_IND = 'Y'
              AND d.END_DATE = @HighDate
        ) cur
            ON cur.JE_HEADER_ID = s.JE_HEADER_ID
        WHERE cur.JE_HEADER_ID IS NULL
           OR cur.CURR_HASH <> s.ROW_HASH;

        /* Expire changed current rows (idempotent: don't expire rows effective today) */
        UPDATE d
            SET
                  d.END_DATE = DATEADD(day, -1, @AsOfDate)
                , d.CURR_IND = 'N'
                , d.UDT_DATE = SYSDATETIME()
        FROM svo.D_GL_HEADER d
        INNER JOIN #chg c
            ON c.JE_HEADER_ID = d.JE_HEADER_ID
        WHERE d.CURR_IND = 'Y'
          AND d.END_DATE = @HighDate
          AND d.EFF_DATE < @AsOfDate;

        SET @Expired = @@ROWCOUNT;

        /* Insert new current rows (skip if same-day already inserted) */
        INSERT INTO svo.D_GL_HEADER
        (
            JE_HEADER_ID,
            LEDGER_SK,
            POSTED_DATE_SK,
            DEFAULT_EFFECTIVE_DATE_SK,
            GL_HEADER_NAME,
            GL_HEADER_STATUS,
            GL_HEADER_DESCRIPTION,
            ACCRUAL_REV_CHANGESIGN_FLAG,
            ACTUAL_FLAG,
            BALANCED_JE_FLAG,
            CURRENCY_CODE,
            CURRENCY_CONVERSION_DATE,
            CURRENCY_CONVERSION_RATE,
            CURRENCY_CONVERSION_TYPE,
            EXTERNAL_REFERENCE,
            JE_BATCH_ID,
            CATEGORY,
            JE_SOURCE,
            MULTI_CURRENCY_FLAG,
            POST_CURRENCY_CODE,
            POST_MULTI_CURRENCY_FLAG,
            RUNNING_TOTAL_CR,
            RUNNING_TOTAL_DR,
            RUNNING_TOTAL_ACCOUNTED_CR,
            RUNNING_TOTAL_ACCOUNTED_DR,
            CR_DR_VARIANCE,
            DATE_CREATED,
            CREATED_BY,
            CREATION_DATE,
            LAST_UPDATE_DATE,
            LAST_UPDATE_LOGIN,
            LAST_UPDATED_BY,
            EFF_DATE,
            END_DATE,
            CRE_DATE,
            UDT_DATE,
            CURR_IND,
            BZ_LOAD_DATE,
            SV_LOAD_DATE
        )
        SELECT
              s.JE_HEADER_ID
            , s.LEDGER_SK
            , s.POSTED_DATE_SK
            , s.DEFAULT_EFFECTIVE_DATE_SK
            , s.GL_HEADER_NAME
            , s.GL_HEADER_STATUS
            , s.GL_HEADER_DESCRIPTION
            , s.ACCRUAL_REV_CHANGESIGN_FLAG
            , s.ACTUAL_FLAG
            , s.BALANCED_JE_FLAG
            , s.CURRENCY_CODE
            , s.CURRENCY_CONVERSION_DATE
            , s.CURRENCY_CONVERSION_RATE
            , s.CURRENCY_CONVERSION_TYPE
            , s.EXTERNAL_REFERENCE
            , s.JE_BATCH_ID
            , s.CATEGORY
            , s.JE_SOURCE
            , s.MULTI_CURRENCY_FLAG
            , s.POST_CURRENCY_CODE
            , s.POST_MULTI_CURRENCY_FLAG
            , s.RUNNING_TOTAL_CR
            , s.RUNNING_TOTAL_DR
            , s.RUNNING_TOTAL_ACCOUNTED_CR
            , s.RUNNING_TOTAL_ACCOUNTED_DR
            , s.CR_DR_VARIANCE
            , s.DATE_CREATED
            , s.CREATED_BY
            , s.CREATION_DATE
            , s.LAST_UPDATE_DATE
            , s.LAST_UPDATE_LOGIN
            , s.LAST_UPDATED_BY
            , @AsOfDate
            , @HighDate
            , SYSDATETIME()
            , SYSDATETIME()
            , 'Y'
            , s.BZ_LOAD_DATE
            , CAST(GETDATE() AS date)
        FROM #src s
        INNER JOIN #chg c
            ON c.JE_HEADER_ID = s.JE_HEADER_ID
        WHERE NOT EXISTS
        (
            SELECT 1
            FROM svo.D_GL_HEADER d
            WHERE d.JE_HEADER_ID = s.JE_HEADER_ID
              AND d.CURR_IND = 'Y'
              AND d.EFF_DATE = @AsOfDate
        );

        SET @Inserted = @@ROWCOUNT;

        COMMIT;

        UPDATE etl.ETL_RUN
            SET
                  END_DTTM      = SYSDATETIME()
                , STATUS        = 'SUCCESS'
                , ROW_INSERTED  = @Inserted
                , ROW_EXPIRED   = @Expired
                , ROW_UPDATED_T1 = 0
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
                , ERROR_MESSAGE = @Err
        WHERE RUN_ID = @RunId;

        THROW;
    END CATCH
END
GO

