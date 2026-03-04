USE [Oracle_Reporting_P2];
GO

/* =====================================================================
   DIM: svo.D_CALENDAR
   Grain: 1 row per calendar date (Type 0 / generated)
   Fiscal enrichment: src.bzo_GL_FiscalDayPVO filtered to FiscalPeriodSetName = 'LAMAR'

   Notes / fixes vs v1:
   - Do NOT assume the identity SK column is named CALENDAR_SK (detect it)
   - SQL Server does not support DATEPART(isoyear); ISO year computed safely
   - Optional: @Rebuild = 1 will DROP/CREATE the dimension (safe for calendar)

   Standards:
   - Stored procedure in schema: svo
   - Transactional + etl.ETL_RUN logging
   - BZ_LOAD_DATE never NULL:
       COALESCE(CAST(fd.AddDateTime AS date), CAST(GETDATE() AS date))
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
-- 1) DDL (create if missing) - Type 0, no SCD2 columns
-------------------------------------------------------------------------------
IF OBJECT_ID(N'svo.D_CALENDAR', 'U') IS NULL
BEGIN
    CREATE TABLE svo.D_CALENDAR
    (
          CALENDAR_SK                 bigint IDENTITY(1,1) NOT NULL CONSTRAINT PK_D_CALENDAR PRIMARY KEY
        , CALENDAR_DATE               date        NOT NULL
        , DATE_KEY                    int         NOT NULL  -- YYYYMMDD
        , DAY_OF_WEEK_NUM             tinyint     NOT NULL  -- 1=Mon..7=Sun (ISO when DATEFIRST=1)
        , DAY_OF_WEEK_NAME            varchar(10) NOT NULL
        , DAY_OF_MONTH_NUM            tinyint     NOT NULL
        , DAY_OF_YEAR_NUM             smallint    NOT NULL
        , WEEK_OF_YEAR_NUM            tinyint     NOT NULL  -- ISO week
        , ISO_YEAR_NUM                smallint    NOT NULL
        , MONTH_NUM                   tinyint     NOT NULL
        , MONTH_NAME                  varchar(10) NOT NULL
        , MONTH_START_DATE            date        NOT NULL
        , MONTH_END_DATE              date        NOT NULL
        , QUARTER_NUM                 tinyint     NOT NULL
        , QUARTER_START_DATE          date        NOT NULL
        , QUARTER_END_DATE            date        NOT NULL
        , YEAR_NUM                    smallint    NOT NULL
        , YEAR_START_DATE             date        NOT NULL
        , YEAR_END_DATE               date        NOT NULL
        , IS_WEEKEND                  char(1)     NOT NULL  -- Y/N

        -- Fiscal (LAMAR)
        , FISC_PERIOD_SET_NAME        varchar(15) NULL
        , FISC_USER_PERIOD_SET_NAME   varchar(15) NULL
        , FISC_CALENDAR_ID            bigint      NULL
        , FISC_PERIOD_NAME            varchar(15) NULL
        , FISC_PERIOD_NUMBER          bigint      NULL
        , FISC_PERIOD_TYPE            varchar(15) NULL
        , FISC_PERIOD_START_DATE      date        NULL
        , FISC_PERIOD_END_DATE        date        NULL
        , FISC_ADJUSTMENT_PERIOD_FLAG varchar(1)  NULL
        , FISC_QUARTER_NUMBER         bigint      NULL
        , FISC_QUARTER_START_DATE     date        NULL
        , FISC_QUARTER_END_DATE       date        NULL
        , FISC_YEAR_NUMBER            bigint      NULL
        , FISC_YEAR_START_DATE        date        NULL
        , FISC_YEAR_END_DATE          date        NULL
        , FISC_DAY_OF_WEEK            bigint      NULL
        , JULIAN_DATE                 bigint      NULL

        , BZ_LOAD_DATE                date        NOT NULL CONSTRAINT DF_D_CALENDAR_BZ_LOAD_DATE DEFAULT (CAST(GETDATE() AS date))
        , SV_LOAD_DATE                date        NOT NULL CONSTRAINT DF_D_CALENDAR_SV_LOAD_DATE DEFAULT (CAST(GETDATE() AS date))

        , CONSTRAINT UX_D_CALENDAR_DATE UNIQUE (CALENDAR_DATE)
    ) ON FG_SilverDim;

    CREATE UNIQUE NONCLUSTERED INDEX UX_D_CALENDAR_DATE_KEY
        ON svo.D_CALENDAR(DATE_KEY)
        ON FG_SilverDim;
END
GO

-------------------------------------------------------------------------------
-- 2) Loader procedure (generated calendar + fiscal enrichment)
-------------------------------------------------------------------------------
CREATE OR ALTER PROCEDURE svo.usp_Load_D_CALENDAR
(
      @StartDate     date = NULL
    , @EndDate       date = NULL
    , @PeriodSetName varchar(15) = 'LAMAR'
    , @Rebuild       bit = 0
)
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    DECLARE @AsOfDate date = CAST(GETDATE() AS date);

    IF @StartDate IS NULL SET @StartDate = DATEFROMPARTS(YEAR(@AsOfDate) - 5, 1, 1);
    IF @EndDate   IS NULL SET @EndDate   = DATEFROMPARTS(YEAR(@AsOfDate) + 2, 12, 31);

    IF @EndDate < @StartDate
        THROW 51010, '@EndDate must be >= @StartDate', 1;

    DECLARE
          @RunId        bigint
        , @ProcName     sysname = OBJECT_SCHEMA_NAME(@@PROCID) + '.' + OBJECT_NAME(@@PROCID)
        , @TargetObject sysname = 'svo.D_CALENDAR';

    DECLARE
          @Inserted   int = 0
        , @UpdatedT1  int = 0;

    INSERT INTO etl.ETL_RUN (PROC_NAME, TARGET_OBJECT, ASOF_DATE, START_DTTM, STATUS)
    VALUES (@ProcName, @TargetObject, @AsOfDate, SYSDATETIME(), 'STARTED');

    SET @RunId = SCOPE_IDENTITY();

    BEGIN TRY
        BEGIN TRAN;

        --------------------------------------------------------------------
        -- Optional rebuild (safe for calendar)
        --------------------------------------------------------------------
        IF @Rebuild = 1
        BEGIN
            IF OBJECT_ID(N'svo.D_CALENDAR', 'U') IS NOT NULL
                DROP TABLE svo.D_CALENDAR;

            CREATE TABLE svo.D_CALENDAR
            (
                  CALENDAR_SK                 bigint IDENTITY(1,1) NOT NULL CONSTRAINT PK_D_CALENDAR PRIMARY KEY
                , CALENDAR_DATE               date        NOT NULL
                , DATE_KEY                    int         NOT NULL
                , DAY_OF_WEEK_NUM             tinyint     NOT NULL
                , DAY_OF_WEEK_NAME            varchar(10) NOT NULL
                , DAY_OF_MONTH_NUM            tinyint     NOT NULL
                , DAY_OF_YEAR_NUM             smallint    NOT NULL
                , WEEK_OF_YEAR_NUM            tinyint     NOT NULL
                , ISO_YEAR_NUM                smallint    NOT NULL
                , MONTH_NUM                   tinyint     NOT NULL
                , MONTH_NAME                  varchar(10) NOT NULL
                , MONTH_START_DATE            date        NOT NULL
                , MONTH_END_DATE              date        NOT NULL
                , QUARTER_NUM                 tinyint     NOT NULL
                , QUARTER_START_DATE          date        NOT NULL
                , QUARTER_END_DATE            date        NOT NULL
                , YEAR_NUM                    smallint    NOT NULL
                , YEAR_START_DATE             date        NOT NULL
                , YEAR_END_DATE               date        NOT NULL
                , IS_WEEKEND                  char(1)     NOT NULL

                , FISC_PERIOD_SET_NAME        varchar(15) NULL
                , FISC_USER_PERIOD_SET_NAME   varchar(15) NULL
                , FISC_CALENDAR_ID            bigint      NULL
                , FISC_PERIOD_NAME            varchar(15) NULL
                , FISC_PERIOD_NUMBER          bigint      NULL
                , FISC_PERIOD_TYPE            varchar(15) NULL
                , FISC_PERIOD_START_DATE      date        NULL
                , FISC_PERIOD_END_DATE        date        NULL
                , FISC_ADJUSTMENT_PERIOD_FLAG varchar(1)  NULL
                , FISC_QUARTER_NUMBER         bigint      NULL
                , FISC_QUARTER_START_DATE     date        NULL
                , FISC_QUARTER_END_DATE       date        NULL
                , FISC_YEAR_NUMBER            bigint      NULL
                , FISC_YEAR_START_DATE        date        NULL
                , FISC_YEAR_END_DATE          date        NULL
                , FISC_DAY_OF_WEEK            bigint      NULL
                , JULIAN_DATE                 bigint      NULL

                , BZ_LOAD_DATE                date        NOT NULL CONSTRAINT DF_D_CALENDAR_BZ_LOAD_DATE DEFAULT (CAST(GETDATE() AS date))
                , SV_LOAD_DATE                date        NOT NULL CONSTRAINT DF_D_CALENDAR_SV_LOAD_DATE DEFAULT (CAST(GETDATE() AS date))

                , CONSTRAINT UX_D_CALENDAR_DATE UNIQUE (CALENDAR_DATE)
            ) ON FG_SilverDim;

            CREATE UNIQUE NONCLUSTERED INDEX UX_D_CALENDAR_DATE_KEY
                ON svo.D_CALENDAR(DATE_KEY)
                ON FG_SilverDim;
        END

        --------------------------------------------------------------------
        -- Ensure plug row (SK=0) using detected identity column name
        --------------------------------------------------------------------
        DECLARE @SkCol sysname =
        (
            SELECT TOP(1) name
            FROM sys.identity_columns
            WHERE object_id = OBJECT_ID(N'svo.D_CALENDAR')
        );

        IF @SkCol IS NULL
        BEGIN
            -- Existing table was created without an IDENTITY surrogate key.
            -- Calendar loads do not require the SK for MERGE (we join on CALENDAR_DATE).
            -- If you want a plug row with SK=0, run this proc with @Rebuild = 1 to recreate the table with CALENDAR_SK IDENTITY.
            SET @SkCol = NULL;
        END
        IF @SkCol IS NOT NULL
        BEGIN
        DECLARE @sql nvarchar(max) =
N'IF NOT EXISTS (SELECT 1 FROM svo.D_CALENDAR WHERE ' + QUOTENAME(@SkCol) + N' = 0)
BEGIN
    SET IDENTITY_INSERT svo.D_CALENDAR ON;

    INSERT INTO svo.D_CALENDAR
    (
          ' + QUOTENAME(@SkCol) + N', CALENDAR_DATE, DATE_KEY
        , DAY_OF_WEEK_NUM, DAY_OF_WEEK_NAME, DAY_OF_MONTH_NUM, DAY_OF_YEAR_NUM, WEEK_OF_YEAR_NUM, ISO_YEAR_NUM
        , MONTH_NUM, MONTH_NAME, MONTH_START_DATE, MONTH_END_DATE
        , QUARTER_NUM, QUARTER_START_DATE, QUARTER_END_DATE
        , YEAR_NUM, YEAR_START_DATE, YEAR_END_DATE
        , IS_WEEKEND
        , FISC_PERIOD_SET_NAME, FISC_USER_PERIOD_SET_NAME, FISC_CALENDAR_ID, FISC_PERIOD_NAME, FISC_PERIOD_NUMBER, FISC_PERIOD_TYPE
        , FISC_PERIOD_START_DATE, FISC_PERIOD_END_DATE, FISC_ADJUSTMENT_PERIOD_FLAG
        , FISC_QUARTER_NUMBER, FISC_QUARTER_START_DATE, FISC_QUARTER_END_DATE
        , FISC_YEAR_NUMBER, FISC_YEAR_START_DATE, FISC_YEAR_END_DATE
        , FISC_DAY_OF_WEEK, JULIAN_DATE
        , BZ_LOAD_DATE, SV_LOAD_DATE
    )
    VALUES
    (
          0, CAST(''0001-01-01'' AS date), 10101
        , 1, ''Unknown'', 1, 1, 1, 1
        , 1, ''Unknown'', CAST(''0001-01-01'' AS date), CAST(''0001-01-01'' AS date)
        , 1, CAST(''0001-01-01'' AS date), CAST(''0001-01-01'' AS date)
        , 1, CAST(''0001-01-01'' AS date), CAST(''0001-01-01'' AS date)
        , ''N''
        , ''LAMAR'', ''LAMAR'', NULL, ''UNK'', NULL, ''UNK''
        , CAST(''0001-01-01'' AS date), CAST(''0001-01-01'' AS date), ''N''
        , NULL, CAST(''0001-01-01'' AS date), CAST(''0001-01-01'' AS date)
        , NULL, CAST(''0001-01-01'' AS date), CAST(''0001-01-01'' AS date)
        , NULL, NULL
        , CAST(''0001-01-01'' AS date), CAST(GETDATE() AS date)
    );

    SET IDENTITY_INSERT svo.D_CALENDAR OFF;
END;';

        EXEC sys.sp_executesql @sql;
        END -- plug row block

        --------------------------------------------------------------------
        -- Build date set
        --------------------------------------------------------------------
        IF OBJECT_ID('tempdb..#dates') IS NOT NULL DROP TABLE #dates;

        ;WITH n AS
        (
            SELECT TOP (DATEDIFF(day, @StartDate, @EndDate) + 1)
                ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) - 1 AS i
            FROM sys.all_objects a
            CROSS JOIN sys.all_objects b
        )
        SELECT DATEADD(day, n.i, @StartDate) AS CALENDAR_DATE
        INTO #dates
        FROM n;

        --------------------------------------------------------------------
        -- Fiscal day mapping (LAMAR)
        --------------------------------------------------------------------
        IF OBJECT_ID('tempdb..#fisc') IS NOT NULL DROP TABLE #fisc;

        ;WITH fd0 AS
        (
            SELECT
                  fd.ReportDate                                AS CALENDAR_DATE
                , fd.FiscalPeriodSetName                       AS FISC_PERIOD_SET_NAME
                , fd.GlCalendarsUserPeriodSetName              AS FISC_USER_PERIOD_SET_NAME
                , fd.GlCalendarsCalendarId                     AS FISC_CALENDAR_ID
                , fd.FiscalPeriodName                          AS FISC_PERIOD_NAME
                , fd.FiscalPeriodNumber                        AS FISC_PERIOD_NUMBER
                , fd.FiscalPeriodType                          AS FISC_PERIOD_TYPE
                , fd.FiscalPeriodStartDate                     AS FISC_PERIOD_START_DATE
                , fd.FiscalPeriodEndDate                       AS FISC_PERIOD_END_DATE
                , fd.FiscalPeriodAdjustmentPeriodFlag          AS FISC_ADJUSTMENT_PERIOD_FLAG
                , fd.FiscalQuarterNumber                       AS FISC_QUARTER_NUMBER
                , fd.FiscalQuarterStartDate                    AS FISC_QUARTER_START_DATE
                , fd.FiscalQuarterEndDate                      AS FISC_QUARTER_END_DATE
                , fd.FiscalYearNumber                          AS FISC_YEAR_NUMBER
                , fd.FiscalYearStartDate                       AS FISC_YEAR_START_DATE
                , fd.FiscalYearEndDate                         AS FISC_YEAR_END_DATE
                , fd.DayOfWeek                                 AS FISC_DAY_OF_WEEK
                , fd.JulianDate                                AS JULIAN_DATE
                , COALESCE(CAST(fd.AddDateTime AS date), CAST(GETDATE() AS date)) AS BZ_LOAD_DATE
                , ROW_NUMBER() OVER
                  (
                      PARTITION BY fd.ReportDate
                      ORDER BY
                          CASE
                              WHEN UPPER(fd.FiscalPeriodType) IN ('MONTH','M') THEN 1
                              WHEN UPPER(fd.FiscalPeriodType) IN ('STANDARD','CALENDAR') THEN 2
                              ELSE 3
                          END,
                          CASE WHEN fd.FiscalPeriodAdjustmentPeriodFlag = 'N' THEN 1 ELSE 2 END,
                          fd.FiscalPeriodNumber DESC
                  ) AS rn
            FROM src.bzo_GL_FiscalDayPVO fd
            WHERE fd.ReportDate BETWEEN @StartDate AND @EndDate
              AND fd.FiscalPeriodSetName = @PeriodSetName
        )
        SELECT
              CALENDAR_DATE
            , FISC_PERIOD_SET_NAME
            , FISC_USER_PERIOD_SET_NAME
            , FISC_CALENDAR_ID
            , FISC_PERIOD_NAME
            , FISC_PERIOD_NUMBER
            , FISC_PERIOD_TYPE
            , FISC_PERIOD_START_DATE
            , FISC_PERIOD_END_DATE
            , FISC_ADJUSTMENT_PERIOD_FLAG
            , FISC_QUARTER_NUMBER
            , FISC_QUARTER_START_DATE
            , FISC_QUARTER_END_DATE
            , FISC_YEAR_NUMBER
            , FISC_YEAR_START_DATE
            , FISC_YEAR_END_DATE
            , FISC_DAY_OF_WEEK
            , JULIAN_DATE
            , BZ_LOAD_DATE
        INTO #fisc
        FROM fd0
        WHERE rn = 1;

IF OBJECT_ID('tempdb..#src') IS NOT NULL DROP TABLE #src;

        -- Ensure ISO calculations are stable
        DECLARE @PrevDateFirst int = @@DATEFIRST;
        SET DATEFIRST 1; -- Monday

        SELECT
              d.CALENDAR_DATE
            , (YEAR(d.CALENDAR_DATE) * 10000) + (MONTH(d.CALENDAR_DATE) * 100) + DAY(d.CALENDAR_DATE) AS DATE_KEY

            , CAST(DATEPART(weekday, d.CALENDAR_DATE) AS tinyint) AS DAY_OF_WEEK_NUM
            , DATENAME(weekday, d.CALENDAR_DATE) AS DAY_OF_WEEK_NAME
            , CAST(DAY(d.CALENDAR_DATE) AS tinyint) AS DAY_OF_MONTH_NUM
            , CAST(DATEPART(dayofyear, d.CALENDAR_DATE) AS smallint) AS DAY_OF_YEAR_NUM

            , CAST(DATEPART(ISO_WEEK, d.CALENDAR_DATE) AS tinyint) AS WEEK_OF_YEAR_NUM
            , CAST(YEAR(DATEADD(day, 26 - DATEPART(ISO_WEEK, d.CALENDAR_DATE), d.CALENDAR_DATE)) AS smallint) AS ISO_YEAR_NUM

            , CAST(MONTH(d.CALENDAR_DATE) AS tinyint) AS MONTH_NUM
            , DATENAME(month, d.CALENDAR_DATE) AS MONTH_NAME
            , DATEFROMPARTS(YEAR(d.CALENDAR_DATE), MONTH(d.CALENDAR_DATE), 1) AS MONTH_START_DATE
            , EOMONTH(d.CALENDAR_DATE) AS MONTH_END_DATE

            , CAST(DATEPART(quarter, d.CALENDAR_DATE) AS tinyint) AS QUARTER_NUM
            , DATEFROMPARTS(YEAR(d.CALENDAR_DATE), ((DATEPART(quarter, d.CALENDAR_DATE)-1)*3)+1, 1) AS QUARTER_START_DATE
            , EOMONTH(DATEADD(month, 2, DATEFROMPARTS(YEAR(d.CALENDAR_DATE), ((DATEPART(quarter, d.CALENDAR_DATE)-1)*3)+1, 1))) AS QUARTER_END_DATE

            , CAST(YEAR(d.CALENDAR_DATE) AS smallint) AS YEAR_NUM
            , DATEFROMPARTS(YEAR(d.CALENDAR_DATE), 1, 1) AS YEAR_START_DATE
            , DATEFROMPARTS(YEAR(d.CALENDAR_DATE), 12, 31) AS YEAR_END_DATE

            , CASE WHEN DATEPART(weekday, d.CALENDAR_DATE) IN (6,7) THEN 'Y' ELSE 'N' END AS IS_WEEKEND

            , f.FISC_PERIOD_SET_NAME
            , f.FISC_USER_PERIOD_SET_NAME
            , f.FISC_CALENDAR_ID
            , f.FISC_PERIOD_NAME
            , f.FISC_PERIOD_NUMBER
            , f.FISC_PERIOD_TYPE
            , f.FISC_PERIOD_START_DATE
            , f.FISC_PERIOD_END_DATE
            , f.FISC_ADJUSTMENT_PERIOD_FLAG
            , f.FISC_QUARTER_NUMBER
            , f.FISC_QUARTER_START_DATE
            , f.FISC_QUARTER_END_DATE
            , f.FISC_YEAR_NUMBER
            , f.FISC_YEAR_START_DATE
            , f.FISC_YEAR_END_DATE
            , f.FISC_DAY_OF_WEEK
            , f.JULIAN_DATE

            , COALESCE(f.BZ_LOAD_DATE, CAST(GETDATE() AS date)) AS BZ_LOAD_DATE
            , CAST(GETDATE() AS date) AS SV_LOAD_DATE
        INTO #src
        FROM #dates d
        LEFT JOIN #fisc f
          ON f.CALENDAR_DATE = d.CALENDAR_DATE;

        -- restore DATEFIRST (best effort)
        IF @PrevDateFirst BETWEEN 1 AND 7
        DECLARE @sqlDateFirst nvarchar(40) = N'SET DATEFIRST ' + CONVERT(nvarchar(10), @PrevDateFirst) + N';';
        EXEC sys.sp_executesql @sqlDateFirst;

        --------------------------------------------------------------------
        -- Upsert (Type 0 / overwrite)
        --------------------------------------------------------------------
        MERGE svo.D_CALENDAR AS tgt
        USING #src AS src
           ON tgt.CALENDAR_DATE = src.CALENDAR_DATE
        WHEN MATCHED THEN
            UPDATE SET
                  tgt.DATE_KEY                    = src.DATE_KEY
                , tgt.DAY_OF_WEEK_NUM             = src.DAY_OF_WEEK_NUM
                , tgt.DAY_OF_WEEK_NAME            = src.DAY_OF_WEEK_NAME
                , tgt.DAY_OF_MONTH_NUM            = src.DAY_OF_MONTH_NUM
                , tgt.DAY_OF_YEAR_NUM             = src.DAY_OF_YEAR_NUM
                , tgt.WEEK_OF_YEAR_NUM            = src.WEEK_OF_YEAR_NUM
                , tgt.ISO_YEAR_NUM                = src.ISO_YEAR_NUM
                , tgt.MONTH_NUM                   = src.MONTH_NUM
                , tgt.MONTH_NAME                  = src.MONTH_NAME
                , tgt.MONTH_START_DATE            = src.MONTH_START_DATE
                , tgt.MONTH_END_DATE              = src.MONTH_END_DATE
                , tgt.QUARTER_NUM                 = src.QUARTER_NUM
                , tgt.QUARTER_START_DATE          = src.QUARTER_START_DATE
                , tgt.QUARTER_END_DATE            = src.QUARTER_END_DATE
                , tgt.YEAR_NUM                    = src.YEAR_NUM
                , tgt.YEAR_START_DATE             = src.YEAR_START_DATE
                , tgt.YEAR_END_DATE               = src.YEAR_END_DATE
                , tgt.IS_WEEKEND                  = src.IS_WEEKEND

                , tgt.FISC_PERIOD_SET_NAME        = src.FISC_PERIOD_SET_NAME
                , tgt.FISC_USER_PERIOD_SET_NAME   = src.FISC_USER_PERIOD_SET_NAME
                , tgt.FISC_CALENDAR_ID            = src.FISC_CALENDAR_ID
                , tgt.FISC_PERIOD_NAME            = src.FISC_PERIOD_NAME
                , tgt.FISC_PERIOD_NUMBER          = src.FISC_PERIOD_NUMBER
                , tgt.FISC_PERIOD_TYPE            = src.FISC_PERIOD_TYPE
                , tgt.FISC_PERIOD_START_DATE      = src.FISC_PERIOD_START_DATE
                , tgt.FISC_PERIOD_END_DATE        = src.FISC_PERIOD_END_DATE
                , tgt.FISC_ADJUSTMENT_PERIOD_FLAG = src.FISC_ADJUSTMENT_PERIOD_FLAG
                , tgt.FISC_QUARTER_NUMBER         = src.FISC_QUARTER_NUMBER
                , tgt.FISC_QUARTER_START_DATE     = src.FISC_QUARTER_START_DATE
                , tgt.FISC_QUARTER_END_DATE       = src.FISC_QUARTER_END_DATE
                , tgt.FISC_YEAR_NUMBER            = src.FISC_YEAR_NUMBER
                , tgt.FISC_YEAR_START_DATE        = src.FISC_YEAR_START_DATE
                , tgt.FISC_YEAR_END_DATE          = src.FISC_YEAR_END_DATE
                , tgt.FISC_DAY_OF_WEEK            = src.FISC_DAY_OF_WEEK
                , tgt.JULIAN_DATE                 = src.JULIAN_DATE

                , tgt.BZ_LOAD_DATE                = COALESCE(src.BZ_LOAD_DATE, CAST(GETDATE() AS date))
                , tgt.SV_LOAD_DATE                = CAST(GETDATE() AS date)
        WHEN NOT MATCHED BY TARGET THEN
            INSERT
            (
                  CALENDAR_DATE, DATE_KEY
                , DAY_OF_WEEK_NUM, DAY_OF_WEEK_NAME, DAY_OF_MONTH_NUM, DAY_OF_YEAR_NUM, WEEK_OF_YEAR_NUM, ISO_YEAR_NUM
                , MONTH_NUM, MONTH_NAME, MONTH_START_DATE, MONTH_END_DATE
                , QUARTER_NUM, QUARTER_START_DATE, QUARTER_END_DATE
                , YEAR_NUM, YEAR_START_DATE, YEAR_END_DATE
                , IS_WEEKEND
                , FISC_PERIOD_SET_NAME, FISC_USER_PERIOD_SET_NAME, FISC_CALENDAR_ID
                , FISC_PERIOD_NAME, FISC_PERIOD_NUMBER, FISC_PERIOD_TYPE
                , FISC_PERIOD_START_DATE, FISC_PERIOD_END_DATE, FISC_ADJUSTMENT_PERIOD_FLAG
                , FISC_QUARTER_NUMBER, FISC_QUARTER_START_DATE, FISC_QUARTER_END_DATE
                , FISC_YEAR_NUMBER, FISC_YEAR_START_DATE, FISC_YEAR_END_DATE
                , FISC_DAY_OF_WEEK, JULIAN_DATE
                , BZ_LOAD_DATE, SV_LOAD_DATE
            )
            VALUES
            (
                  src.CALENDAR_DATE, src.DATE_KEY
                , src.DAY_OF_WEEK_NUM, src.DAY_OF_WEEK_NAME, src.DAY_OF_MONTH_NUM, src.DAY_OF_YEAR_NUM, src.WEEK_OF_YEAR_NUM, src.ISO_YEAR_NUM
                , src.MONTH_NUM, src.MONTH_NAME, src.MONTH_START_DATE, src.MONTH_END_DATE
                , src.QUARTER_NUM, src.QUARTER_START_DATE, src.QUARTER_END_DATE
                , src.YEAR_NUM, src.YEAR_START_DATE, src.YEAR_END_DATE
                , src.IS_WEEKEND
                , src.FISC_PERIOD_SET_NAME, src.FISC_USER_PERIOD_SET_NAME, src.FISC_CALENDAR_ID
                , src.FISC_PERIOD_NAME, src.FISC_PERIOD_NUMBER, src.FISC_PERIOD_TYPE
                , src.FISC_PERIOD_START_DATE, src.FISC_PERIOD_END_DATE, src.FISC_ADJUSTMENT_PERIOD_FLAG
                , src.FISC_QUARTER_NUMBER, src.FISC_QUARTER_START_DATE, src.FISC_QUARTER_END_DATE
                , src.FISC_YEAR_NUMBER, src.FISC_YEAR_START_DATE, src.FISC_YEAR_END_DATE
                , src.FISC_DAY_OF_WEEK, src.JULIAN_DATE
                , COALESCE(src.BZ_LOAD_DATE, CAST(GETDATE() AS date)), CAST(GETDATE() AS date)
            );

        SET @UpdatedT1 = (SELECT COUNT(*) FROM #src);

        COMMIT;

        UPDATE etl.ETL_RUN
            SET
                  END_DTTM       = SYSDATETIME()
                , STATUS         = 'SUCCESS'
                , ROW_INSERTED   = NULL
                , ROW_EXPIRED    = 0
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
                , ROW_INSERTED   = NULL
                , ROW_EXPIRED    = 0
                , ROW_UPDATED_T1 = @UpdatedT1
                , ERROR_MESSAGE  = @Err
        WHERE RUN_ID = @RunId;

        THROW;
    END CATCH
END
GO
