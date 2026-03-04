/* =========================================================
   usp_Load_D_CALENDAR
   Incremental Type 1 load. Source: bzo.GL_FiscalDayPVO, bzo.GL_FiscalPeriodExtractPVO
   Batched upsert using AddDateTime watermark.
   ========================================================= */
CREATE OR ALTER PROCEDURE svo.usp_Load_D_CALENDAR
    @batch_size INT = 10000
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    DECLARE
        @ProcName       SYSNAME       = OBJECT_SCHEMA_NAME(@@PROCID) + '.' + OBJECT_NAME(@@PROCID),
        @TargetObject   SYSNAME       = 'svo.D_CALENDAR',
        @RunId          BIGINT        = NULL,
        @LastWatermark  DATETIME2(7),
        @MaxWatermark   DATETIME2(7),
        @RowInserted    INT           = 0,
        @RowUpdated     INT           = 0,
        @BatchInserted  INT           = 0,
        @BatchUpdated   INT           = 0;

    DECLARE @MergeActions TABLE (ActionTaken VARCHAR(10) NOT NULL);

    BEGIN TRY
        SELECT @LastWatermark = w.LAST_WATERMARK FROM etl.ETL_WATERMARK w WHERE w.TABLE_NAME = @TargetObject;
        IF @LastWatermark IS NULL SET @LastWatermark = '1900-01-01';

        INSERT INTO etl.ETL_RUN (PROC_NAME, TARGET_OBJECT, ASOF_DATE, START_DTTM, STATUS)
        VALUES (@ProcName, @TargetObject, CAST(GETDATE() AS DATE), SYSDATETIME(), 'STARTED');
        SET @RunId = SCOPE_IDENTITY();
        IF @RunId IS NULL THROW 50001, 'ETL_RUN insert failed: RUN_ID is NULL.', 1;

        IF NOT EXISTS (SELECT 1 FROM svo.D_CALENDAR WHERE DATE_SK = 10101)
        BEGIN
            INSERT INTO svo.D_CALENDAR (DATE_SK, DATE, DAY_OF_WEEK, PERIOD_NUMBER, PERIOD_NAME, PERIOD_START_DATE, PERIOD_END_DATE, QUARTER_NUMBER, QUARTER_START_DATE, QUARTER_END_DATE, YEAR_NUMBER, YEAR_START_DATE, YEAR_END_DATE, JULIAN_DATE, PERIOD_TYPE, LAST_UPDATE_DATE, OBJECT_VERSION_NUMBER, CALENDAR_ID, GL_CALENDARS_USER_PERIOD_SET_NAME, BZ_LOAD_DATE, SV_LOAD_DATE)
            VALUES (10101, '0001-01-01', NULL, NULL, 'Unknown', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'Unknown', NULL, NULL, NULL, NULL, CAST(GETDATE() AS DATE), CAST(GETDATE() AS DATE));
        END;
        IF NOT EXISTS (SELECT 1 FROM svo.D_CALENDAR WHERE DATE_SK = 99991231)
        BEGIN
            INSERT INTO svo.D_CALENDAR (DATE_SK, DATE, DAY_OF_WEEK, PERIOD_NUMBER, PERIOD_NAME, PERIOD_START_DATE, PERIOD_END_DATE, QUARTER_NUMBER, QUARTER_START_DATE, QUARTER_END_DATE, YEAR_NUMBER, YEAR_START_DATE, YEAR_END_DATE, JULIAN_DATE, PERIOD_TYPE, LAST_UPDATE_DATE, OBJECT_VERSION_NUMBER, CALENDAR_ID, GL_CALENDARS_USER_PERIOD_SET_NAME, BZ_LOAD_DATE, SV_LOAD_DATE)
            VALUES (99991231, '9999-12-31', NULL, NULL, 'Unknown', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'Unknown', NULL, NULL, NULL, NULL, CAST(GETDATE() AS DATE), CAST(GETDATE() AS DATE));
        END;

        IF OBJECT_ID('tempdb..#src') IS NOT NULL DROP TABLE #src;
        CREATE TABLE #src (
            DATE_SK INT NOT NULL,
            DATE DATE NULL,
            DAY_OF_WEEK INT NULL,
            PERIOD_ID INT NULL,
            PERIOD_NUMBER INT NULL,
            PERIOD_NAME NVARCHAR(25) NULL,
            PERIOD_START_DATE DATE NULL,
            PERIOD_END_DATE DATE NULL,
            PERIOD_TYPE NVARCHAR(25) NULL,
            QUARTER_NUMBER INT NULL,
            QUARTER_START_DATE DATE NULL,
            QUARTER_END_DATE DATE NULL,
            YEAR_NUMBER INT NULL,
            YEAR_START_DATE DATE NULL,
            YEAR_END_DATE DATE NULL,
            JULIAN_DATE INT NULL,
            LAST_UPDATE_DATE DATE NULL,
            OBJECT_VERSION_NUMBER INT NULL,
            CALENDAR_ID NVARCHAR(25) NULL,
            GL_CALENDARS_USER_PERIOD_SET_NAME NVARCHAR(25) NULL,
            BZ_LOAD_DATE DATE NULL,
            SV_LOAD_DATE DATE NULL,
            SourceAddDateTime DATETIME2(7) NULL
        );

        WHILE 1 = 1
        BEGIN
            DELETE FROM #src;

            INSERT INTO #src
            SELECT TOP (@batch_size)
                CAST(CONVERT(CHAR(8), TRY_CONVERT(DATE, D.REPORTDATE), 112) AS INT),
                CAST(D.REPORTDATE AS DATE),
                D.DAYOFWEEK,
                CAST(FORMAT(D.REPORTDATE, 'yyyyMM') AS INT),
                D.FISCALPERIODNUMBER,
                D.FISCALPERIODNAME,
                CAST(D.FISCALPERIODSTARTDATE AS DATE),
                CAST(D.FISCALPERIODENDDATE AS DATE),
                D.FISCALPERIODTYPE,
                D.FISCALQUARTERNUMBER,
                CAST(D.FISCALQUARTERSTARTDATE AS DATE),
                CAST(D.FISCALQUARTERENDDATE AS DATE),
                CAST(P.PERIODPERIODYEAR AS INT),
                CAST(D.FISCALYEARSTARTDATE AS DATE),
                CAST(D.FISCALYEARENDDATE AS DATE),
                D.JULIANDATE,
                CAST(D.LASTUPDATEDATE AS DATE),
                P.PERIODOBJECTVERSIONNUMBER,
                D.GLCALENDARSCALENDARID,
                D.GLCALENDARSUSERPERIODSETNAME,
                CAST(D.AddDateTime AS DATE),
                CAST(GETDATE() AS DATE),
                D.AddDateTime
            FROM bzo.GL_FiscalDayPVO D
            JOIN bzo.GL_FiscalPeriodExtractPVO P ON D.FISCALPERIODNAME = P.PERIODPERIODNAME
            WHERE D.FISCALPERIODADJUSTMENTPERIODFLAG = 'N'
              AND P.PERIODADJUSTMENTPERIODFLAG = 'N'
              AND D.GlCalendarsUserPeriodSetName = 'LAMAR CAL'
              AND D.AddDateTime > @LastWatermark
            ORDER BY D.AddDateTime;

            IF @@ROWCOUNT = 0 BREAK;
            SELECT @MaxWatermark = MAX(SourceAddDateTime) FROM #src;
            DELETE FROM @MergeActions;

            MERGE svo.D_CALENDAR AS tgt
            USING #src AS src ON tgt.DATE_SK = src.DATE_SK
            WHEN MATCHED THEN UPDATE SET
                tgt.DATE = src.DATE,
                tgt.DAY_OF_WEEK = src.DAY_OF_WEEK,
                tgt.PERIOD_ID = src.PERIOD_ID,
                tgt.PERIOD_NUMBER = src.PERIOD_NUMBER,
                tgt.PERIOD_NAME = src.PERIOD_NAME,
                tgt.PERIOD_START_DATE = src.PERIOD_START_DATE,
                tgt.PERIOD_END_DATE = src.PERIOD_END_DATE,
                tgt.PERIOD_TYPE = src.PERIOD_TYPE,
                tgt.QUARTER_NUMBER = src.QUARTER_NUMBER,
                tgt.QUARTER_START_DATE = src.QUARTER_START_DATE,
                tgt.QUARTER_END_DATE = src.QUARTER_END_DATE,
                tgt.YEAR_NUMBER = src.YEAR_NUMBER,
                tgt.YEAR_START_DATE = src.YEAR_START_DATE,
                tgt.YEAR_END_DATE = src.YEAR_END_DATE,
                tgt.JULIAN_DATE = src.JULIAN_DATE,
                tgt.LAST_UPDATE_DATE = src.LAST_UPDATE_DATE,
                tgt.OBJECT_VERSION_NUMBER = src.OBJECT_VERSION_NUMBER,
                tgt.CALENDAR_ID = src.CALENDAR_ID,
                tgt.GL_CALENDARS_USER_PERIOD_SET_NAME = src.GL_CALENDARS_USER_PERIOD_SET_NAME,
                tgt.BZ_LOAD_DATE = src.BZ_LOAD_DATE,
                tgt.SV_LOAD_DATE = src.SV_LOAD_DATE
            WHEN NOT MATCHED BY TARGET THEN INSERT (
                DATE_SK, DATE, DAY_OF_WEEK, PERIOD_ID, PERIOD_NUMBER, PERIOD_NAME, PERIOD_START_DATE, PERIOD_END_DATE,
                PERIOD_TYPE, QUARTER_NUMBER, QUARTER_START_DATE, QUARTER_END_DATE, YEAR_NUMBER, YEAR_START_DATE, YEAR_END_DATE,
                JULIAN_DATE, LAST_UPDATE_DATE, OBJECT_VERSION_NUMBER, CALENDAR_ID, GL_CALENDARS_USER_PERIOD_SET_NAME,
                BZ_LOAD_DATE, SV_LOAD_DATE
            ) VALUES (
                src.DATE_SK, src.DATE, src.DAY_OF_WEEK, src.PERIOD_ID, src.PERIOD_NUMBER, src.PERIOD_NAME, src.PERIOD_START_DATE, src.PERIOD_END_DATE,
                src.PERIOD_TYPE, src.QUARTER_NUMBER, src.QUARTER_START_DATE, src.QUARTER_END_DATE, src.YEAR_NUMBER, src.YEAR_START_DATE, src.YEAR_END_DATE,
                src.JULIAN_DATE, src.LAST_UPDATE_DATE, src.OBJECT_VERSION_NUMBER, src.CALENDAR_ID, src.GL_CALENDARS_USER_PERIOD_SET_NAME,
                src.BZ_LOAD_DATE, src.SV_LOAD_DATE
            )
            OUTPUT $action INTO @MergeActions(ActionTaken);

            SELECT @BatchInserted = SUM(CASE WHEN ActionTaken = 'INSERT' THEN 1 ELSE 0 END), @BatchUpdated = SUM(CASE WHEN ActionTaken = 'UPDATE' THEN 1 ELSE 0 END) FROM @MergeActions;
            SET @RowInserted += @BatchInserted; SET @RowUpdated += @BatchUpdated;
            MERGE etl.ETL_WATERMARK AS tgt USING (SELECT @TargetObject AS TABLE_NAME, @MaxWatermark AS LAST_WATERMARK) AS src ON tgt.TABLE_NAME = src.TABLE_NAME WHEN MATCHED THEN UPDATE SET tgt.LAST_WATERMARK = src.LAST_WATERMARK, tgt.UDT_DATE = SYSDATETIME() WHEN NOT MATCHED BY TARGET THEN INSERT (TABLE_NAME, LAST_WATERMARK) VALUES (src.TABLE_NAME, src.LAST_WATERMARK);
            SET @LastWatermark = @MaxWatermark;
            IF (SELECT COUNT(*) FROM #src) < @batch_size BREAK;
        END

        UPDATE etl.ETL_RUN SET END_DTTM = SYSDATETIME(), STATUS = 'SUCCESS', ROW_INSERTED = @RowInserted, ROW_UPDATED = @RowUpdated, ROW_EXPIRED = 0, ERROR_MESSAGE = NULL WHERE RUN_ID = @RunId;
        IF COL_LENGTH('etl.ETL_RUN', 'ROW_UPDATED_T1') IS NOT NULL UPDATE etl.ETL_RUN SET ROW_UPDATED_T1 = @RowUpdated WHERE RUN_ID = @RunId;
    END TRY
    BEGIN CATCH
        DECLARE @ErrMsg NVARCHAR(4000) = CONCAT('Error ', ERROR_NUMBER(), ' (Line ', ERROR_LINE(), '): ', ERROR_MESSAGE());
        IF @RunId IS NOT NULL BEGIN UPDATE etl.ETL_RUN SET END_DTTM = SYSDATETIME(), STATUS = 'FAILURE', ROW_INSERTED = @RowInserted, ROW_UPDATED = @RowUpdated, ROW_EXPIRED = 0, ERROR_MESSAGE = @ErrMsg WHERE RUN_ID = @RunId; IF COL_LENGTH('etl.ETL_RUN', 'ROW_UPDATED_T1') IS NOT NULL UPDATE etl.ETL_RUN SET ROW_UPDATED_T1 = @RowUpdated WHERE RUN_ID = @RunId; END
        ;THROW;
    END CATCH
END;
GO
