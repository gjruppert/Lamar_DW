/* =========================================================
   usp_Load_D_CURRENCY
   Incremental Type 1 load. Source: bzo.GL_DailyRateExtractPVO
   Batched upsert using AddDateTime watermark. BK: CURRENCY_ID (composite).
   ========================================================= */
CREATE OR ALTER PROCEDURE svo.usp_Load_D_CURRENCY
    @batch_size INT = 10000
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    DECLARE
        @ProcName       SYSNAME       = OBJECT_SCHEMA_NAME(@@PROCID) + '.' + OBJECT_NAME(@@PROCID),
        @TargetObject   SYSNAME       = 'svo.D_CURRENCY',
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

        IF NOT EXISTS (SELECT 1 FROM svo.D_CURRENCY WHERE CURRENCY_SK = 0)
        BEGIN
            SET IDENTITY_INSERT svo.D_CURRENCY ON;
            INSERT INTO svo.D_CURRENCY (CURRENCY_SK, CURRENCY_ID, CURRENCY_CODE_FROM, CURRENCY_CODE_TO, CURRENCY_CONV_DATE, CURRENCY_CONV_RATE, CURRENCY_CONV_TYPE, CURRENCY_CONV_STATUS, BZ_LOAD_DATE, SV_LOAD_DATE)
            VALUES (0, '-1', 'UNK', 'UNK', '0001-01-01', 1, 'UNK', 'U', '0001-01-01', CAST(GETDATE() AS DATE));
            SET IDENTITY_INSERT svo.D_CURRENCY OFF;
        END;

        IF OBJECT_ID('tempdb..#src') IS NOT NULL DROP TABLE #src;
        CREATE TABLE #src (
            CURRENCY_ID VARCHAR(50) NOT NULL,
            CURRENCY_CODE_FROM VARCHAR(5) NOT NULL,
            CURRENCY_CODE_TO VARCHAR(5) NOT NULL,
            CURRENCY_CONV_DATE DATE NOT NULL,
            CURRENCY_CONV_RATE NUMERIC(18,4) NULL,
            CURRENCY_CONV_TYPE VARCHAR(25) NULL,
            CURRENCY_CONV_STATUS VARCHAR(1) NULL,
            BZ_LOAD_DATE DATE NULL,
            SV_LOAD_DATE DATE NULL,
            SourceAddDateTime DATETIME2(7) NULL
        );

        WHILE 1 = 1
        BEGIN
            DELETE FROM #src;

            INSERT INTO #src
            SELECT TOP (@batch_size)
                CONCAT(ISNULL(L.DailyRateFromCurrency,'UNK'), CONVERT(CHAR(8), ISNULL(L.DailyRateConversionDate,'0001-01-01'), 112), ISNULL(TRIM(L.DailyRateConversionType), 'UNK')),
                ISNULL(TRIM(L.DailyRateFromCurrency), 'UNK'),
                ISNULL(TRIM(L.DailyRateToCurrency), 'UNK'),
                CAST(L.DailyRateConversionDate AS DATE),
                CAST(ISNULL(L.DailyRateConversionRate,1) AS NUMERIC(18,4)),
                ISNULL(TRIM(L.DailyRateConversionType), 'UNK'),
                ISNULL(L.DailyRateStatusCode,'U'),
                CAST(L.AddDateTime AS DATE),
                CAST(GETDATE() AS DATE),
                L.AddDateTime
            FROM bzo.GL_DailyRateExtractPVO L
            WHERE L.AddDateTime > @LastWatermark
            ORDER BY L.AddDateTime;

            IF @@ROWCOUNT = 0 BREAK;
            SELECT @MaxWatermark = MAX(SourceAddDateTime) FROM #src;
            DELETE FROM @MergeActions;

            MERGE svo.D_CURRENCY AS tgt
            USING #src AS src ON tgt.CURRENCY_ID = src.CURRENCY_ID
            WHEN MATCHED THEN UPDATE SET
                tgt.CURRENCY_CODE_FROM = src.CURRENCY_CODE_FROM,
                tgt.CURRENCY_CODE_TO = src.CURRENCY_CODE_TO,
                tgt.CURRENCY_CONV_DATE = src.CURRENCY_CONV_DATE,
                tgt.CURRENCY_CONV_RATE = src.CURRENCY_CONV_RATE,
                tgt.CURRENCY_CONV_TYPE = src.CURRENCY_CONV_TYPE,
                tgt.CURRENCY_CONV_STATUS = src.CURRENCY_CONV_STATUS,
                tgt.BZ_LOAD_DATE = src.BZ_LOAD_DATE,
                tgt.SV_LOAD_DATE = src.SV_LOAD_DATE
            WHEN NOT MATCHED BY TARGET THEN INSERT (CURRENCY_ID, CURRENCY_CODE_FROM, CURRENCY_CODE_TO, CURRENCY_CONV_DATE, CURRENCY_CONV_RATE, CURRENCY_CONV_TYPE, CURRENCY_CONV_STATUS, BZ_LOAD_DATE, SV_LOAD_DATE)
            VALUES (src.CURRENCY_ID, src.CURRENCY_CODE_FROM, src.CURRENCY_CODE_TO, src.CURRENCY_CONV_DATE, src.CURRENCY_CONV_RATE, src.CURRENCY_CONV_TYPE, src.CURRENCY_CONV_STATUS, src.BZ_LOAD_DATE, src.SV_LOAD_DATE)
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
