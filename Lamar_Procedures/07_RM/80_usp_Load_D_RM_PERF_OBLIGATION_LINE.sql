/* =========================================================
   usp_Load_D_RM_PERF_OBLIGATION_LINE
   Type 1 incremental load. Source: bzo.VRM_PerfObligationLinesPVO
   Watermark: AddDateTime. Grain: PERF_OBLIGATION_LINE_ID. Plug row SK=0 if missing.
   Idempotent: safe to retry.
   ========================================================= */
CREATE OR ALTER PROCEDURE svo.usp_Load_D_RM_PERF_OBLIGATION_LINE
    @BatchId INT = NULL
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    DECLARE
        @ProcName       SYSNAME        = OBJECT_SCHEMA_NAME(@@PROCID) + '.' + OBJECT_NAME(@@PROCID),
        @TargetObject   SYSNAME        = 'svo.D_RM_PERF_OBLIGATION_LINE',
        @StartDttm      DATETIME2(0)   = SYSDATETIME(),
        @EndDttm        DATETIME2(0),
        @RunId          BIGINT         = NULL,
        @ErrMsg         NVARCHAR(4000) = NULL,
        @AsOfDate       DATE           = CAST(GETDATE() AS DATE),
        @LastWatermark  DATETIME2(7),
        @MaxWatermark   DATETIME2(7)   = NULL,
        @RowInserted    INT            = 0,
        @RowUpdated     INT            = 0,
        @TableBridgeID  INT            = NULL;

    DECLARE @MergeActions TABLE (ActionTaken VARCHAR(10) NOT NULL);

    SELECT @TableBridgeID = TableBridgeID FROM meta.MedallionTableBridge WHERE targettable = N'VRM_PerfObligationLinesPVO';

    BEGIN TRY
        SELECT @LastWatermark = w.LAST_WATERMARK
        FROM etl.ETL_WATERMARK w
        WHERE w.TABLE_NAME = @TargetObject;

        IF @LastWatermark IS NULL
            SET @LastWatermark = '1900-01-01';

        INSERT INTO etl.ETL_RUN (PROC_NAME, TARGET_OBJECT, ASOF_DATE, START_DTTM, STATUS, BATCH_ID, TABLE_BRIDGE_ID)
        VALUES (@ProcName, @TargetObject, @AsOfDate, @StartDttm, 'STARTED', ISNULL(@BatchId, -1), ISNULL(@TableBridgeID, -1));

        SET @RunId = SCOPE_IDENTITY();
        IF @RunId IS NULL
            THROW 50001, 'ETL_RUN insert failed: RUN_ID is NULL.', 1;

        IF NOT EXISTS (SELECT 1 FROM svo.D_RM_PERF_OBLIGATION_LINE WHERE RM_PERF_OBLIGATION_LINE_SK = 0)
        BEGIN
            SET IDENTITY_INSERT svo.D_RM_PERF_OBLIGATION_LINE ON;
            INSERT INTO svo.D_RM_PERF_OBLIGATION_LINE
            (RM_PERF_OBLIGATION_LINE_SK, PERF_OBLIGATION_LINE_ID, COMMENTS, CONTR_CUR_NET_CONSIDER_AMT, CREATED_BY, DOCUMENT_LINE_ID, ENTERED_CUR_NET_CONSIDER_AMT, ENTERED_CUR_RECOG_REV_AMT, LAST_UPDATE_DATE, LAST_UPDATED_BY, LAST_UPDATE_LOGIN, NET_LINE_AMT, PAYMENT_AMOUNT, PERF_OBLIGATION_ID, PERF_OBLIGATION_LINE_NUMBER, REVENUE_END_DATE, REVENUE_START_DATE, PERF_OBLIGATION_DAYS_TOTAL, SOURCE_DOCUMENT_LINE_ID, BZ_LOAD_DATE, SV_LOAD_DATE)
            VALUES (0, -1, 'Unknown', 0, 'Unknown', -1, 0, 0, CAST('1900-01-01' AS DATETIME), 'Unknown', 'Unknown', 0, 0, -1, -1, '1900-01-01', '9999-12-31', 0, -1, CAST(GETDATE() AS DATE), CAST(GETDATE() AS DATE));
            SET IDENTITY_INSERT svo.D_RM_PERF_OBLIGATION_LINE OFF;
        END;

        IF OBJECT_ID('tempdb..#src') IS NOT NULL DROP TABLE #src;

        SELECT
            ISNULL(L.PerfObligationLineId, -1)          AS PERF_OBLIGATION_LINE_ID,
            ISNULL(L.PerfObligationLinesComments, 'Unknown') AS COMMENTS,
            ISNULL(L.PerfObligationLinesContrCurNetConsiderAmt, 0) AS CONTR_CUR_NET_CONSIDER_AMT,
            ISNULL(L.PerfObligationLinesCreatedBy, 'Unknown') AS CREATED_BY,
            ISNULL(L.PerfObligationLinesDocumentLineId, -1) AS DOCUMENT_LINE_ID,
            ISNULL(L.PerfObligationLinesEnteredCurNetConsiderAmt, 0) AS ENTERED_CUR_NET_CONSIDER_AMT,
            ISNULL(L.PerfObligationLinesEnteredCurRecogRevAmt, 0) AS ENTERED_CUR_RECOG_REV_AMT,
            ISNULL(L.PerfObligationLinesLastUpdateDate, '1900-01-01') AS LAST_UPDATE_DATE,
            ISNULL(L.PerfObligationLinesLastUpdatedBy, 'Unknown') AS LAST_UPDATED_BY,
            L.PerfObligationLinesLastUpdateLogin        AS LAST_UPDATE_LOGIN,
            ISNULL(L.PerfObligationLinesNetLineAmt, 0)  AS NET_LINE_AMT,
            ISNULL(L.PerfObligationLinesPaymentAmount, 0) AS PAYMENT_AMOUNT,
            ISNULL(L.PerfObligationLinesPerfObligationId, -1) AS PERF_OBLIGATION_ID,
            ISNULL(L.PerfObligationLinesPerfObligationLineNumber, -1) AS PERF_OBLIGATION_LINE_NUMBER,
            ISNULL(L.PerfObligationLinesRevenueEndDate, '9999-12-31') AS REVENUE_END_DATE,
            ISNULL(L.PerfObligationLinesRevenueStartDate, '1900-01-01') AS REVENUE_START_DATE,
            CASE WHEN L.PerfObligationLinesSatisfactionBaseProportion IS NULL
                THEN DATEDIFF(DAY, ISNULL(L.PerfObligationLinesRevenueStartDate, '1900-01-01'), ISNULL(L.PerfObligationLinesRevenueEndDate, '9999-12-31')) + 1
                ELSE L.PerfObligationLinesSatisfactionBaseProportion END AS PERF_OBLIGATION_DAYS_TOTAL,
            ISNULL(L.SourceDocLinesDocumentLineId, -1)   AS SOURCE_DOCUMENT_LINE_ID,
            CAST(L.AddDateTime AS DATE)                 AS BZ_LOAD_DATE,
            CAST(GETDATE() AS DATE)                     AS SV_LOAD_DATE,
            ISNULL(L.AddDateTime, SYSDATETIME())        AS SourceAddDateTime
        INTO #src
        FROM bzo.VRM_PerfObligationLinesPVO AS L
        WHERE L.AddDateTime > @LastWatermark;

        SELECT @MaxWatermark = MAX(SourceAddDateTime) FROM #src;

        MERGE svo.D_RM_PERF_OBLIGATION_LINE AS tgt
        USING #src AS src ON tgt.PERF_OBLIGATION_LINE_ID = src.PERF_OBLIGATION_LINE_ID
        WHEN MATCHED THEN UPDATE SET
            tgt.COMMENTS = src.COMMENTS,
            tgt.CONTR_CUR_NET_CONSIDER_AMT = src.CONTR_CUR_NET_CONSIDER_AMT,
            tgt.CREATED_BY = src.CREATED_BY,
            tgt.DOCUMENT_LINE_ID = src.DOCUMENT_LINE_ID,
            tgt.ENTERED_CUR_NET_CONSIDER_AMT = src.ENTERED_CUR_NET_CONSIDER_AMT,
            tgt.ENTERED_CUR_RECOG_REV_AMT = src.ENTERED_CUR_RECOG_REV_AMT,
            tgt.LAST_UPDATE_DATE = src.LAST_UPDATE_DATE,
            tgt.LAST_UPDATED_BY = src.LAST_UPDATED_BY,
            tgt.LAST_UPDATE_LOGIN = src.LAST_UPDATE_LOGIN,
            tgt.NET_LINE_AMT = src.NET_LINE_AMT,
            tgt.PAYMENT_AMOUNT = src.PAYMENT_AMOUNT,
            tgt.PERF_OBLIGATION_ID = src.PERF_OBLIGATION_ID,
            tgt.PERF_OBLIGATION_LINE_NUMBER = src.PERF_OBLIGATION_LINE_NUMBER,
            tgt.REVENUE_END_DATE = src.REVENUE_END_DATE,
            tgt.REVENUE_START_DATE = src.REVENUE_START_DATE,
            tgt.PERF_OBLIGATION_DAYS_TOTAL = src.PERF_OBLIGATION_DAYS_TOTAL,
            tgt.SOURCE_DOCUMENT_LINE_ID = src.SOURCE_DOCUMENT_LINE_ID,
            tgt.BZ_LOAD_DATE = src.BZ_LOAD_DATE,
            tgt.SV_LOAD_DATE = src.SV_LOAD_DATE
        WHEN NOT MATCHED BY TARGET THEN INSERT (
            PERF_OBLIGATION_LINE_ID, COMMENTS, CONTR_CUR_NET_CONSIDER_AMT, CREATED_BY, DOCUMENT_LINE_ID, ENTERED_CUR_NET_CONSIDER_AMT, ENTERED_CUR_RECOG_REV_AMT, LAST_UPDATE_DATE, LAST_UPDATED_BY, LAST_UPDATE_LOGIN, NET_LINE_AMT, PAYMENT_AMOUNT, PERF_OBLIGATION_ID, PERF_OBLIGATION_LINE_NUMBER, REVENUE_END_DATE, REVENUE_START_DATE, PERF_OBLIGATION_DAYS_TOTAL, SOURCE_DOCUMENT_LINE_ID, BZ_LOAD_DATE, SV_LOAD_DATE
        ) VALUES (
            src.PERF_OBLIGATION_LINE_ID, src.COMMENTS, src.CONTR_CUR_NET_CONSIDER_AMT, src.CREATED_BY, src.DOCUMENT_LINE_ID, src.ENTERED_CUR_NET_CONSIDER_AMT, src.ENTERED_CUR_RECOG_REV_AMT, src.LAST_UPDATE_DATE, src.LAST_UPDATED_BY, src.LAST_UPDATE_LOGIN, src.NET_LINE_AMT, src.PAYMENT_AMOUNT, src.PERF_OBLIGATION_ID, src.PERF_OBLIGATION_LINE_NUMBER, src.REVENUE_END_DATE, src.REVENUE_START_DATE, src.PERF_OBLIGATION_DAYS_TOTAL, src.SOURCE_DOCUMENT_LINE_ID, src.BZ_LOAD_DATE, src.SV_LOAD_DATE
        )
        OUTPUT $action INTO @MergeActions(ActionTaken);

        SELECT @RowInserted = SUM(CASE WHEN ActionTaken = 'INSERT' THEN 1 ELSE 0 END),
               @RowUpdated  = SUM(CASE WHEN ActionTaken = 'UPDATE' THEN 1 ELSE 0 END)
        FROM @MergeActions;

        IF @MaxWatermark IS NOT NULL
            UPDATE etl.ETL_WATERMARK SET LAST_WATERMARK = @MaxWatermark, UDT_DATE = SYSDATETIME() WHERE TABLE_NAME = @TargetObject;

        SET @EndDttm = SYSDATETIME();
        UPDATE etl.ETL_RUN SET END_DTTM = @EndDttm, STATUS = 'SUCCESS', ROW_INSERTED = @RowInserted, ROW_UPDATED = @RowUpdated, ROW_EXPIRED = 0, ERROR_MESSAGE = NULL WHERE RUN_ID = @RunId;
    END TRY
    BEGIN CATCH
        SET @EndDttm = SYSDATETIME();
        SET @ErrMsg = CONCAT('Error ', ERROR_NUMBER(), ' (Line ', ERROR_LINE(), '): ', ERROR_MESSAGE());
        IF @RunId IS NOT NULL
            UPDATE etl.ETL_RUN SET END_DTTM = @EndDttm, STATUS = 'FAILURE', ROW_INSERTED = @RowInserted, ROW_UPDATED = @RowUpdated, ROW_EXPIRED = 0, ERROR_MESSAGE = @ErrMsg WHERE RUN_ID = @RunId;
        ;THROW;
    END CATCH
END;
GO
