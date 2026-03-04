/* =========================================================
   usp_Load_D_RM_SATISFACTION_EVENT
   Type 1 incremental load. Source: bzo.VRM_PolSatisfactionEventsPVO
   Watermark: AddDateTime. Grain: POL_SATISFACTION_EVENT_ID. Plug row SK=0 if missing.
   Idempotent: safe to retry.
   ========================================================= */
CREATE OR ALTER PROCEDURE svo.usp_Load_D_RM_SATISFACTION_EVENT
    @BatchId INT = NULL
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    DECLARE
        @ProcName       SYSNAME        = OBJECT_SCHEMA_NAME(@@PROCID) + '.' + OBJECT_NAME(@@PROCID),
        @TargetObject   SYSNAME        = 'svo.D_RM_SATISFACTION_EVENT',
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

    SELECT @TableBridgeID = TableBridgeID FROM meta.MedallionTableBridge WHERE targettable = N'VRM_PolSatisfactionEventsPVO';

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

        IF NOT EXISTS (SELECT 1 FROM svo.D_RM_SATISFACTION_EVENT WHERE RM_SATISFACTION_EVENT_SK = 0)
        BEGIN
            SET IDENTITY_INSERT svo.D_RM_SATISFACTION_EVENT ON;
            INSERT INTO svo.D_RM_SATISFACTION_EVENT
            (RM_SATISFACTION_EVENT_SK, POL_SATISFACTION_EVENT_ID, ATTRIBUTE_CATEGORY, COMMENTS, CREATED_BY, CREATED_FROM, CREATION_DATE, DISCARDED_DATE, DISCARDED_FLAG, DOCUMENT_LINE_ID, DOCUMENT_SUB_LINE_ID, HOLD_FLAG, LAST_UPDATE_DATE, LAST_UPDATED_BY, LAST_UPDATE_LOGIN, OBJECT_VERSION_NUMBER, PERF_OBLIGATION_LINE_ID, PROCESSED_AMOUNT, PROCESSED_FLAG, PROCESSED_PERIOD_PROPORTION, SATISFACTION_MEASUREMENT_DATE, SATISFACTION_MEASUREMENT_NUM, SATISFACTION_PERCENT, SATISFACTION_PERIOD_END_DATE, SATISFACTION_PERIOD_PROPORTION, SATISFACTION_PERIOD_START_DATE, SATISFACTION_QUANTITY, SPLIT_FLAG, BZ_LOAD_DATE, SV_LOAD_DATE)
            VALUES (0, -1, 'Unknown', 'Unknown', 'Unknown', 'Unknown', '1900-01-01', '9999-12-31', 'U', -1, -1, 'U', '1900-01-01', 'Unknown', 'Unknown', 0, -1, 0, 'U', 0, '1900-01-01', 0, 0, '1900-01-01', 0, '9999-12-31', 0, 'U', CAST(GETDATE() AS DATE), CAST(GETDATE() AS DATE));
            SET IDENTITY_INSERT svo.D_RM_SATISFACTION_EVENT OFF;
        END;

        IF OBJECT_ID('tempdb..#src') IS NOT NULL DROP TABLE #src;

        SELECT
            ISNULL(E.PolSatisfactionEventId, -1)               AS POL_SATISFACTION_EVENT_ID,
            ISNULL(E.PolSatisfactionEventsAttributeCategory, 'Unknown') AS ATTRIBUTE_CATEGORY,
            ISNULL(E.PolSatisfactionEventsComments, 'Unknown') AS COMMENTS,
            ISNULL(E.PolSatisfactionEventsCreatedBy, 'Unknown') AS CREATED_BY,
            ISNULL(E.PolSatisfactionEventsCreatedFrom, 'Unk')   AS CREATED_FROM,
            ISNULL(CAST(E.PolSatisfactionEventsCreationDate AS DATE), '1900-01-01') AS CREATION_DATE,
            ISNULL(E.PolSatisfactionEventsDiscardedDate, '9999-12-31') AS DISCARDED_DATE,
            ISNULL(E.PolSatisfactionEventsDiscardedFlag, 'U')   AS DISCARDED_FLAG,
            ISNULL(E.PolSatisfactionEventsDocumentLineId, -1)   AS DOCUMENT_LINE_ID,
            ISNULL(E.PolSatisfactionEventsDocumentSubLineId, -1) AS DOCUMENT_SUB_LINE_ID,
            ISNULL(E.PolSatisfactionEventsHoldFlag, 'U')        AS HOLD_FLAG,
            ISNULL(CAST(E.PolSatisfactionEventsLastUpdateDate AS DATE), '1900-01-01') AS LAST_UPDATE_DATE,
            ISNULL(E.PolSatisfactionEventsLastUpdatedBy, 'Unknown') AS LAST_UPDATED_BY,
            E.PolSatisfactionEventsLastUpdateLogin             AS LAST_UPDATE_LOGIN,
            ISNULL(E.PolSatisfactionEventsObjectVersionNumber, 0) AS OBJECT_VERSION_NUMBER,
            ISNULL(E.PolSatisfactionEventsPerfObligationLineId, -1) AS PERF_OBLIGATION_LINE_ID,
            ISNULL(E.PolSatisfactionEventsProcessedAmount, 0)   AS PROCESSED_AMOUNT,
            ISNULL(E.PolSatisfactionEventsProcessedFlag, 'U')  AS PROCESSED_FLAG,
            ISNULL(E.PolSatisfactionEventsProcessedPeriodProportion, 0) AS PROCESSED_PERIOD_PROPORTION,
            ISNULL(E.PolSatisfactionEventsSatisfactionMeasurementDate, '1900-01-01') AS SATISFACTION_MEASUREMENT_DATE,
            ISNULL(E.PolSatisfactionEventsSatisfactionMeasurementNum, 0) AS SATISFACTION_MEASUREMENT_NUM,
            ISNULL(E.PolSatisfactionEventsSatisfactionPercent, 0) AS SATISFACTION_PERCENT,
            ISNULL(E.PolSatisfactionEventsSatisfactionPeriodEndDate, '9999-12-31') AS SATISFACTION_PERIOD_END_DATE,
            ISNULL(E.PolSatisfactionEventsSatisfactionPeriodProportion, 0) AS SATISFACTION_PERIOD_PROPORTION,
            ISNULL(E.PolSatisfactionEventsSatisfactionPeriodStartDate, '1900-01-01') AS SATISFACTION_PERIOD_START_DATE,
            ISNULL(E.PolSatisfactionEventsSatisfactionQuantity, 0) AS SATISFACTION_QUANTITY,
            ISNULL(E.PolSatisfactionEventsSplitFlag, 'U')        AS SPLIT_FLAG,
            CAST(E.AddDateTime AS DATE)                        AS BZ_LOAD_DATE,
            CAST(GETDATE() AS DATE)                            AS SV_LOAD_DATE,
            ISNULL(E.AddDateTime, SYSDATETIME())               AS SourceAddDateTime
        INTO #src
        FROM bzo.VRM_PolSatisfactionEventsPVO AS E
        WHERE E.AddDateTime > @LastWatermark;

        SELECT @MaxWatermark = MAX(SourceAddDateTime) FROM #src;

        MERGE svo.D_RM_SATISFACTION_EVENT AS tgt
        USING #src AS src ON tgt.POL_SATISFACTION_EVENT_ID = src.POL_SATISFACTION_EVENT_ID
        WHEN MATCHED THEN UPDATE SET
            tgt.ATTRIBUTE_CATEGORY = src.ATTRIBUTE_CATEGORY,
            tgt.COMMENTS = src.COMMENTS,
            tgt.CREATED_BY = src.CREATED_BY,
            tgt.CREATED_FROM = src.CREATED_FROM,
            tgt.CREATION_DATE = src.CREATION_DATE,
            tgt.DISCARDED_DATE = src.DISCARDED_DATE,
            tgt.DISCARDED_FLAG = src.DISCARDED_FLAG,
            tgt.DOCUMENT_LINE_ID = src.DOCUMENT_LINE_ID,
            tgt.DOCUMENT_SUB_LINE_ID = src.DOCUMENT_SUB_LINE_ID,
            tgt.HOLD_FLAG = src.HOLD_FLAG,
            tgt.LAST_UPDATE_DATE = src.LAST_UPDATE_DATE,
            tgt.LAST_UPDATED_BY = src.LAST_UPDATED_BY,
            tgt.LAST_UPDATE_LOGIN = src.LAST_UPDATE_LOGIN,
            tgt.OBJECT_VERSION_NUMBER = src.OBJECT_VERSION_NUMBER,
            tgt.PERF_OBLIGATION_LINE_ID = src.PERF_OBLIGATION_LINE_ID,
            tgt.PROCESSED_AMOUNT = src.PROCESSED_AMOUNT,
            tgt.PROCESSED_FLAG = src.PROCESSED_FLAG,
            tgt.PROCESSED_PERIOD_PROPORTION = src.PROCESSED_PERIOD_PROPORTION,
            tgt.SATISFACTION_MEASUREMENT_DATE = src.SATISFACTION_MEASUREMENT_DATE,
            tgt.SATISFACTION_MEASUREMENT_NUM = src.SATISFACTION_MEASUREMENT_NUM,
            tgt.SATISFACTION_PERCENT = src.SATISFACTION_PERCENT,
            tgt.SATISFACTION_PERIOD_END_DATE = src.SATISFACTION_PERIOD_END_DATE,
            tgt.SATISFACTION_PERIOD_PROPORTION = src.SATISFACTION_PERIOD_PROPORTION,
            tgt.SATISFACTION_PERIOD_START_DATE = src.SATISFACTION_PERIOD_START_DATE,
            tgt.SATISFACTION_QUANTITY = src.SATISFACTION_QUANTITY,
            tgt.SPLIT_FLAG = src.SPLIT_FLAG,
            tgt.BZ_LOAD_DATE = src.BZ_LOAD_DATE,
            tgt.SV_LOAD_DATE = src.SV_LOAD_DATE
        WHEN NOT MATCHED BY TARGET THEN INSERT (
            POL_SATISFACTION_EVENT_ID, ATTRIBUTE_CATEGORY, COMMENTS, CREATED_BY, CREATED_FROM, CREATION_DATE, DISCARDED_DATE, DISCARDED_FLAG, DOCUMENT_LINE_ID, DOCUMENT_SUB_LINE_ID, HOLD_FLAG, LAST_UPDATE_DATE, LAST_UPDATED_BY, LAST_UPDATE_LOGIN, OBJECT_VERSION_NUMBER, PERF_OBLIGATION_LINE_ID, PROCESSED_AMOUNT, PROCESSED_FLAG, PROCESSED_PERIOD_PROPORTION, SATISFACTION_MEASUREMENT_DATE, SATISFACTION_MEASUREMENT_NUM, SATISFACTION_PERCENT, SATISFACTION_PERIOD_END_DATE, SATISFACTION_PERIOD_PROPORTION, SATISFACTION_PERIOD_START_DATE, SATISFACTION_QUANTITY, SPLIT_FLAG, BZ_LOAD_DATE, SV_LOAD_DATE
        ) VALUES (
            src.POL_SATISFACTION_EVENT_ID, src.ATTRIBUTE_CATEGORY, src.COMMENTS, src.CREATED_BY, src.CREATED_FROM, src.CREATION_DATE, src.DISCARDED_DATE, src.DISCARDED_FLAG, src.DOCUMENT_LINE_ID, src.DOCUMENT_SUB_LINE_ID, src.HOLD_FLAG, src.LAST_UPDATE_DATE, src.LAST_UPDATED_BY, src.LAST_UPDATE_LOGIN, src.OBJECT_VERSION_NUMBER, src.PERF_OBLIGATION_LINE_ID, src.PROCESSED_AMOUNT, src.PROCESSED_FLAG, src.PROCESSED_PERIOD_PROPORTION, src.SATISFACTION_MEASUREMENT_DATE, src.SATISFACTION_MEASUREMENT_NUM, src.SATISFACTION_PERCENT, src.SATISFACTION_PERIOD_END_DATE, src.SATISFACTION_PERIOD_PROPORTION, src.SATISFACTION_PERIOD_START_DATE, src.SATISFACTION_QUANTITY, src.SPLIT_FLAG, src.BZ_LOAD_DATE, src.SV_LOAD_DATE
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
