/* =========================================================
   usp_Load_D_LEDGER
   SCD2 incremental load. Source: bzo.GL_LedgerExtractPVO
   Watermark filter, expire changed rows, insert new current rows.
   Idempotent: safe to retry same watermark range.
   ========================================================= */
CREATE OR ALTER PROCEDURE svo.usp_Load_D_LEDGER
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    DECLARE
        @ProcName       SYSNAME        = OBJECT_SCHEMA_NAME(@@PROCID) + '.' + OBJECT_NAME(@@PROCID),
        @TargetObject   SYSNAME        = 'svo.D_LEDGER',
        @StartDttm      DATETIME2(0)   = SYSDATETIME(),
        @EndDttm        DATETIME2(0),
        @RunId          BIGINT         = NULL,
        @ErrMsg         NVARCHAR(4000) = NULL,

        @AsOfDate       DATE           = CAST(GETDATE() AS DATE),
        @LoadDttm       DATETIME2(0)   = SYSDATETIME(),
        @HighDate       DATE           = '9999-12-31',

        @LastWatermark  DATETIME2(7),
        @MaxWatermark   DATETIME2(7)   = NULL,

        @RowInserted    INT            = 0,
        @RowExpired     INT            = 0,
        @RowUpdated     INT            = 0;

    BEGIN TRY
        SELECT @LastWatermark = w.LAST_WATERMARK
        FROM etl.ETL_WATERMARK w
        WHERE w.TABLE_NAME = @TargetObject;

        IF @LastWatermark IS NULL
            SET @LastWatermark = '1900-01-01';

        INSERT INTO etl.ETL_RUN (PROC_NAME, TARGET_OBJECT, ASOF_DATE, START_DTTM, STATUS)
        VALUES (@ProcName, @TargetObject, @AsOfDate, @StartDttm, 'STARTED');

        SET @RunId = SCOPE_IDENTITY();

        IF @RunId IS NULL
            THROW 50001, 'ETL_RUN insert failed: RUN_ID is NULL.', 1;

        IF EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'UX_D_LEDGER_ID' AND object_id = OBJECT_ID('svo.D_LEDGER'))
        BEGIN
            DROP INDEX UX_D_LEDGER_ID ON svo.D_LEDGER;
        END;

        IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'UX_D_LEDGER_BK_CURR' AND object_id = OBJECT_ID('svo.D_LEDGER'))
        BEGIN
            CREATE UNIQUE NONCLUSTERED INDEX UX_D_LEDGER_BK_CURR
            ON svo.D_LEDGER (LEDGER_ID)
            WHERE CURR_IND = 'Y'
            ON FG_SilverDim;
        END;

        IF NOT EXISTS (SELECT 1 FROM svo.D_LEDGER WHERE LEDGER_SK = 0)
        BEGIN
            SET IDENTITY_INSERT svo.D_LEDGER ON;

            INSERT INTO svo.D_LEDGER
            (LEDGER_SK, LEDGER_ID, LEDGER_NAME, LEDGER_ACCOUNTED_PERIOD_TYPE, LEDGER_CHART_OF_ACCOUNTS_ID, LEDGER_CURRENCY_CODE,
             LEDGER_DESCRIPTION, LEDGER_CATEGORY_CODE, LEDGER_PERIOD_SET_NAME, LEDGER_SHORT_NAME, BZ_LOAD_DATE, SV_LOAD_DATE,
             EFF_DATE, END_DATE, CRE_DATE, UDT_DATE, CURR_IND)
            VALUES
            (0, -1, 'Unknown', 'NULL', 0, 'NULL', 'NULL', 0, 'NULL', 'NULL', CAST(GETDATE() AS DATE), CAST(GETDATE() AS DATE),
             @AsOfDate, @HighDate, @LoadDttm, @LoadDttm, 'Y');

            SET IDENTITY_INSERT svo.D_LEDGER OFF;
        END;

        IF OBJECT_ID('tempdb..#src') IS NOT NULL DROP TABLE #src;

        SELECT
            s.LEDGER_ID, s.LEDGER_NAME, s.LEDGER_ACCOUNTED_PERIOD_TYPE, s.LEDGER_CHART_OF_ACCOUNTS_ID, s.LEDGER_CURRENCY_CODE,
            s.LEDGER_DESCRIPTION, s.LEDGER_CATEGORY_CODE, s.LEDGER_PERIOD_SET_NAME, s.LEDGER_SHORT_NAME,
            s.BZ_LOAD_DATE, s.SV_LOAD_DATE, s.SourceAddDateTime
        INTO #src
        FROM
        (
            SELECT
                L.LedgerLedgerId AS LEDGER_ID,
                L.LedgerName AS LEDGER_NAME,
                L.LedgerAccountedPeriodType AS LEDGER_ACCOUNTED_PERIOD_TYPE,
                L.LedgerChartOfAccountsId AS LEDGER_CHART_OF_ACCOUNTS_ID,
                L.LedgerCurrencyCode AS LEDGER_CURRENCY_CODE,
                L.LedgerDescription AS LEDGER_DESCRIPTION,
                L.LedgerLedgerCategoryCode AS LEDGER_CATEGORY_CODE,
                L.LedgerPeriodSetName AS LEDGER_PERIOD_SET_NAME,
                L.LedgerShortName AS LEDGER_SHORT_NAME,
                COALESCE(CAST(L.AddDateTime AS DATE), '0001-01-01') AS BZ_LOAD_DATE,
                CAST(GETDATE() AS DATE) AS SV_LOAD_DATE,
                L.AddDateTime AS SourceAddDateTime,
                ROW_NUMBER() OVER (PARTITION BY L.LedgerLedgerId ORDER BY L.AddDateTime DESC) AS rn
            FROM bzo.GL_LedgerExtractPVO L
            WHERE L.AddDateTime > @LastWatermark
        ) s
        WHERE s.rn = 1;

        SELECT @MaxWatermark = MAX(SourceAddDateTime) FROM #src;

        UPDATE tgt
        SET tgt.END_DATE = DATEADD(DAY, -1, @AsOfDate), tgt.CURR_IND = 'N', tgt.UDT_DATE = @LoadDttm
        FROM svo.D_LEDGER tgt
        INNER JOIN #src src ON src.LEDGER_ID = tgt.LEDGER_ID
        WHERE tgt.CURR_IND = 'Y'
          AND (
                ISNULL(tgt.LEDGER_NAME,'') <> ISNULL(src.LEDGER_NAME,'')
             OR ISNULL(tgt.LEDGER_ACCOUNTED_PERIOD_TYPE,'') <> ISNULL(src.LEDGER_ACCOUNTED_PERIOD_TYPE,'')
             OR ISNULL(tgt.LEDGER_CHART_OF_ACCOUNTS_ID, -999) <> ISNULL(src.LEDGER_CHART_OF_ACCOUNTS_ID, -999)
             OR ISNULL(tgt.LEDGER_CURRENCY_CODE,'') <> ISNULL(src.LEDGER_CURRENCY_CODE,'')
             OR ISNULL(tgt.LEDGER_DESCRIPTION,'') <> ISNULL(src.LEDGER_DESCRIPTION,'')
             OR ISNULL(tgt.LEDGER_CATEGORY_CODE,'') <> ISNULL(src.LEDGER_CATEGORY_CODE,'')
             OR ISNULL(tgt.LEDGER_PERIOD_SET_NAME,'') <> ISNULL(src.LEDGER_PERIOD_SET_NAME,'')
             OR ISNULL(tgt.LEDGER_SHORT_NAME,'') <> ISNULL(src.LEDGER_SHORT_NAME,'')
          );

        SET @RowExpired = @@ROWCOUNT;

        INSERT INTO svo.D_LEDGER
        (LEDGER_ID, LEDGER_NAME, LEDGER_ACCOUNTED_PERIOD_TYPE, LEDGER_CHART_OF_ACCOUNTS_ID, LEDGER_CURRENCY_CODE,
         LEDGER_DESCRIPTION, LEDGER_CATEGORY_CODE, LEDGER_PERIOD_SET_NAME, LEDGER_SHORT_NAME, BZ_LOAD_DATE, SV_LOAD_DATE,
         EFF_DATE, END_DATE, CRE_DATE, UDT_DATE, CURR_IND)
        SELECT
            src.LEDGER_ID, src.LEDGER_NAME, src.LEDGER_ACCOUNTED_PERIOD_TYPE, src.LEDGER_CHART_OF_ACCOUNTS_ID, src.LEDGER_CURRENCY_CODE,
            src.LEDGER_DESCRIPTION, src.LEDGER_CATEGORY_CODE, src.LEDGER_PERIOD_SET_NAME, src.LEDGER_SHORT_NAME, src.BZ_LOAD_DATE, src.SV_LOAD_DATE,
            @AsOfDate, @HighDate, @LoadDttm, @LoadDttm, 'Y'
        FROM #src src
        LEFT JOIN svo.D_LEDGER tgt ON tgt.LEDGER_ID = src.LEDGER_ID AND tgt.CURR_IND = 'Y'
        WHERE tgt.LEDGER_ID IS NULL;

        SET @RowInserted = @@ROWCOUNT;

        IF @MaxWatermark IS NOT NULL
        BEGIN
            UPDATE etl.ETL_WATERMARK SET LAST_WATERMARK = @MaxWatermark, UDT_DATE = SYSDATETIME() WHERE TABLE_NAME = @TargetObject;
        END

        SET @EndDttm = SYSDATETIME();
        UPDATE etl.ETL_RUN SET END_DTTM = @EndDttm, STATUS = 'SUCCESS', ROW_INSERTED = @RowInserted, ROW_EXPIRED = @RowExpired, ROW_UPDATED = @RowUpdated, ERROR_MESSAGE = NULL WHERE RUN_ID = @RunId;
    END TRY
    BEGIN CATCH
        SET @EndDttm = SYSDATETIME();
        SET @ErrMsg  = CONCAT('Error ', ERROR_NUMBER(), ' (Line ', ERROR_LINE(), '): ', ERROR_MESSAGE());
        IF @RunId IS NOT NULL
            UPDATE etl.ETL_RUN SET END_DTTM = @EndDttm, STATUS = 'FAILURE', ROW_INSERTED = @RowInserted, ROW_EXPIRED = @RowExpired, ROW_UPDATED = @RowUpdated, ERROR_MESSAGE = @ErrMsg WHERE RUN_ID = @RunId;
        ;THROW;
    END CATCH
END;
GO
