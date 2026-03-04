CREATE OR ALTER PROCEDURE svo.usp_Load_D_CALENDAR
      @FullReload bit = 0
    , @Debug      bit = 0
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    DECLARE @proc sysname = QUOTENAME(OBJECT_SCHEMA_NAME(@@PROCID)) + '.' + QUOTENAME(OBJECT_NAME(@@PROCID));
    DECLARE @t0 datetime2(3) = SYSDATETIME();

    BEGIN TRY
        IF @Debug = 1
            PRINT 'Starting ' + @proc;

        IF @FullReload = 1
        BEGIN
            IF @Debug = 1
                PRINT 'FullReload requested - truncating svo.D_CALENDAR';

            TRUNCATE TABLE svo.D_CALENDAR;
        END

        ;WITH P1 AS
        (
            -- If the period table has duplicates per period name, keep the latest row per name.
            SELECT
                  p.PeriodPeriodName
                , p.PeriodPeriodYear
                , p.PeriodObjectVersionNumber
                , rn = ROW_NUMBER() OVER
                    (PARTITION BY p.PeriodPeriodName
                     ORDER BY TRY_CONVERT(datetime2(7), p.AddDateTime) DESC, p.PeriodObjectVersionNumber DESC)
            FROM src.bzo_GL_FiscalPeriodExtractPVO p
            WHERE p.PeriodAdjustmentPeriodFlag = 'N'
        ),
        Dedupe AS
        (
            -- GL_FiscalDayPVO can have multiple rows per ReportDate (period set/type/year).
            -- We force exactly one row per ReportDate to prevent DATE_SK duplicates.
            SELECT
                  DATE_SK  = (YEAR(d.ReportDate) * 10000) + (MONTH(d.ReportDate) * 100) + DAY(d.ReportDate)
                , [DATE]   = d.ReportDate
                , DAY_OF_WEEK = TRY_CONVERT(int, d.DayOfWeek)

                , PERIOD_ID = (YEAR(d.ReportDate) * 100) + MONTH(d.ReportDate)

                , PERIOD_NUMBER = TRY_CONVERT(int, d.FiscalPeriodNumber)
                , PERIOD_NAME   = d.FiscalPeriodName
                , PERIOD_START_DATE = d.FiscalPeriodStartDate
                , PERIOD_END_DATE   = d.FiscalPeriodEndDate
                , PERIOD_TYPE       = d.FiscalPeriodType

                , QUARTER_NUMBER     = TRY_CONVERT(int, d.FiscalQuarterNumber)
                , QUARTER_START_DATE = d.FiscalQuarterStartDate
                , QUARTER_END_DATE   = d.FiscalQuarterEndDate

                , YEAR_NUMBER     = TRY_CONVERT(int, ISNULL(p1.PeriodPeriodYear, d.FiscalYearNumber))
                , YEAR_START_DATE = d.FiscalYearStartDate
                , YEAR_END_DATE   = d.FiscalYearEndDate

                , JULIAN_DATE      = TRY_CONVERT(int, d.JulianDate)
                , LAST_UPDATE_DATE = CAST(d.LastUpdateDate AS date)
                , OBJECT_VERSION_NUMBER = TRY_CONVERT(int, p1.PeriodObjectVersionNumber)

                , CALENDAR_ID = TRY_CONVERT(nvarchar(25), d.GlCalendarsCalendarId)
                , GL_CALENDARS_USER_PERIOD_SET_NAME = TRY_CONVERT(nvarchar(25), d.GlCalendarsUserPeriodSetName)

                , BZ_LOAD_DATE = COALESCE(CAST(d.AddDateTime AS date), CAST('0001-01-01' AS date))
                , SV_LOAD_DATE = CAST(GETDATE() AS date)

                , rn = ROW_NUMBER() OVER
                    (
                        PARTITION BY d.ReportDate
                        ORDER BY
                              TRY_CONVERT(datetime2(7), d.AddDateTime) DESC
                            , d.LastUpdateDate DESC
                            , d.FiscalYearNumber DESC
                            , d.FiscalPeriodNumber DESC
                            , d.FiscalPeriodType DESC
                            , d.FiscalPeriodSetName DESC
                    )
            FROM src.bzo_GL_FiscalDayPVO d
            LEFT JOIN (SELECT PeriodPeriodName, PeriodPeriodYear, PeriodObjectVersionNumber FROM P1 WHERE rn = 1) p1
                   ON p1.PeriodPeriodName = d.FiscalPeriodName
            WHERE d.FiscalPeriodAdjustmentPeriodFlag = 'N'
        ),
        Src AS
        (
            SELECT *
            FROM Dedupe
            WHERE rn = 1
        )
        MERGE svo.D_CALENDAR AS T
        USING Src AS S
           ON T.DATE_SK = S.DATE_SK
        WHEN MATCHED AND @FullReload = 0 THEN
            UPDATE SET
                  T.[DATE] = S.[DATE]
                , T.DAY_OF_WEEK = S.DAY_OF_WEEK
                , T.PERIOD_ID = S.PERIOD_ID
                , T.PERIOD_NUMBER = S.PERIOD_NUMBER
                , T.PERIOD_NAME = S.PERIOD_NAME
                , T.PERIOD_START_DATE = S.PERIOD_START_DATE
                , T.PERIOD_END_DATE = S.PERIOD_END_DATE
                , T.PERIOD_TYPE = S.PERIOD_TYPE
                , T.QUARTER_NUMBER = S.QUARTER_NUMBER
                , T.QUARTER_START_DATE = S.QUARTER_START_DATE
                , T.QUARTER_END_DATE = S.QUARTER_END_DATE
                , T.YEAR_NUMBER = S.YEAR_NUMBER
                , T.YEAR_START_DATE = S.YEAR_START_DATE
                , T.YEAR_END_DATE = S.YEAR_END_DATE
                , T.JULIAN_DATE = S.JULIAN_DATE
                , T.LAST_UPDATE_DATE = S.LAST_UPDATE_DATE
                , T.OBJECT_VERSION_NUMBER = S.OBJECT_VERSION_NUMBER
                , T.CALENDAR_ID = S.CALENDAR_ID
                , T.GL_CALENDARS_USER_PERIOD_SET_NAME = S.GL_CALENDARS_USER_PERIOD_SET_NAME
                , T.BZ_LOAD_DATE = S.BZ_LOAD_DATE
                , T.SV_LOAD_DATE = S.SV_LOAD_DATE
        WHEN NOT MATCHED BY TARGET THEN
            INSERT
            (
                  DATE_SK, [DATE], DAY_OF_WEEK, PERIOD_ID
                , PERIOD_NUMBER, PERIOD_NAME, PERIOD_START_DATE, PERIOD_END_DATE, PERIOD_TYPE
                , QUARTER_NUMBER, QUARTER_START_DATE, QUARTER_END_DATE
                , YEAR_NUMBER, YEAR_START_DATE, YEAR_END_DATE
                , JULIAN_DATE, LAST_UPDATE_DATE, OBJECT_VERSION_NUMBER
                , CALENDAR_ID, GL_CALENDARS_USER_PERIOD_SET_NAME
                , BZ_LOAD_DATE, SV_LOAD_DATE
            )
            VALUES
            (
                  S.DATE_SK, S.[DATE], S.DAY_OF_WEEK, S.PERIOD_ID
                , S.PERIOD_NUMBER, S.PERIOD_NAME, S.PERIOD_START_DATE, S.PERIOD_END_DATE, S.PERIOD_TYPE
                , S.QUARTER_NUMBER, S.QUARTER_START_DATE, S.QUARTER_END_DATE
                , S.YEAR_NUMBER, S.YEAR_START_DATE, S.YEAR_END_DATE
                , S.JULIAN_DATE, S.LAST_UPDATE_DATE, S.OBJECT_VERSION_NUMBER
                , S.CALENDAR_ID, S.GL_CALENDARS_USER_PERIOD_SET_NAME
                , S.BZ_LOAD_DATE, S.SV_LOAD_DATE
            )
        ;

        -- Plug rows (0 = Unknown, 99991231 = High Date)
        IF NOT EXISTS (SELECT 1 FROM svo.D_CALENDAR WHERE DATE_SK = 0)
        BEGIN
            INSERT INTO svo.D_CALENDAR
            (
                  DATE_SK, [DATE], PERIOD_NAME, PERIOD_TYPE
                , BZ_LOAD_DATE, SV_LOAD_DATE
            )
            VALUES
            (
                  0, CAST('0001-01-01' AS date), N'Unknown', N'Unknown'
                , CAST('0001-01-01' AS date), CAST(GETDATE() AS date)
            );
        END

        IF NOT EXISTS (SELECT 1 FROM svo.D_CALENDAR WHERE DATE_SK = 99991231)
        BEGIN
            INSERT INTO svo.D_CALENDAR
            (
                  DATE_SK, [DATE], PERIOD_NAME, PERIOD_TYPE
                , BZ_LOAD_DATE, SV_LOAD_DATE
            )
            VALUES
            (
                  99991231, CAST('9999-12-31' AS date), N'Unknown', N'Unknown'
                , CAST('0001-01-01' AS date), CAST(GETDATE() AS date)
            );
        END

        IF @Debug = 1
        BEGIN
            DECLARE @rows bigint = (SELECT COUNT_BIG(*) FROM svo.D_CALENDAR);
            PRINT 'Completed ' + @proc
                + ' | rows=' + CONVERT(varchar(30), @rows)
                + ' | ms=' + CONVERT(varchar(30), DATEDIFF(millisecond, @t0, SYSDATETIME()));
        END
    END TRY
    BEGIN CATCH
        DECLARE @ErrNum int = ERROR_NUMBER();
        DECLARE @ErrSev int = ERROR_SEVERITY();
        DECLARE @ErrSta int = ERROR_STATE();
        DECLARE @ErrLin int = ERROR_LINE();
        DECLARE @ErrMsg nvarchar(4000) = ERROR_MESSAGE();

        IF @Debug = 1
            PRINT 'FAILED ' + @proc
                + ' | line=' + CONVERT(varchar(12), @ErrLin)
                + ' | err=' + CONVERT(varchar(12), @ErrNum)
                + ' | msg=' + @ErrMsg;

        RAISERROR('%s failed. Line %d. Error %d: %s', @ErrSev, @ErrSta, @proc, @ErrLin, @ErrNum, @ErrMsg);
        RETURN;
    END CATCH
END
GO


