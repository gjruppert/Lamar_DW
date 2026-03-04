/* =========================================================
   usp_Load_D_AR_TRANSACTION
   Type 1 incremental. Source: bzo.AR_TransactionHeaderExtractPVO
   Watermark: AddDateTime. Grain: AR_TRANSACTION_ID. Plug row SK=0.
   Idempotent: safe to retry.
   ========================================================= */
CREATE OR ALTER PROCEDURE svo.usp_Load_D_AR_TRANSACTION
    @BatchId INT = NULL
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    DECLARE
        @ProcName       SYSNAME        = OBJECT_SCHEMA_NAME(@@PROCID) + '.' + OBJECT_NAME(@@PROCID),
        @TargetObject   SYSNAME        = 'svo.D_AR_TRANSACTION',
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

    SELECT @TableBridgeID = TableBridgeID FROM meta.MedallionTableBridge WHERE targettable = N'AR_TransactionHeaderExtractPVO';

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

        IF NOT EXISTS (SELECT 1 FROM svo.D_AR_TRANSACTION WHERE AR_TRANSACTION_SK = 0)
        BEGIN
            SET IDENTITY_INSERT svo.D_AR_TRANSACTION ON;
            INSERT INTO svo.D_AR_TRANSACTION
            (AR_TRANSACTION_SK, AR_TRANSACTION_ID, AR_TRANSACTION_NUMBER, AR_REFERENCE, AR_PO_NUMBER, AR_TRANSACTION_CLASS_CODE, AR_TRANSACTION_STATUS_CODE, AR_REASON_CODE, AR_COMPLETE_FLAG, AddDateTime, SV_LOAD_DATE)
            VALUES (0, 0, 'UNKNOWN', NULL, NULL, NULL, NULL, NULL, NULL, GETDATE(), CONVERT(date, GETDATE()));
            SET IDENTITY_INSERT svo.D_AR_TRANSACTION OFF;
        END;

        IF OBJECT_ID('tempdb..#src') IS NOT NULL DROP TABLE #src;

        SELECT
            H.RaCustomerTrxCustomerTrxId    AS AR_TRANSACTION_ID,
            H.RaCustomerTrxTrxNumber        AS AR_TRANSACTION_NUMBER,
            H.RaCustomerTrxCustomerReference AS AR_REFERENCE,
            H.RaCustomerTrxPurchaseOrder   AS AR_PO_NUMBER,
            H.RaCustomerTrxTrxClass        AS AR_TRANSACTION_CLASS_CODE,
            H.RaCustomerTrxStatusTrx       AS AR_TRANSACTION_STATUS_CODE,
            H.RaCustomerTrxReasonCode      AS AR_REASON_CODE,
            H.RaCustomerTrxCompleteFlag    AS AR_COMPLETE_FLAG,
            H.AddDateTime,
            CONVERT(date, GETDATE())        AS SV_LOAD_DATE,
            H.AddDateTime                   AS SourceAddDateTime
        INTO #src
        FROM bzo.AR_TransactionHeaderExtractPVO H
        WHERE H.AddDateTime > @LastWatermark;

        SELECT @MaxWatermark = MAX(SourceAddDateTime) FROM #src;

        MERGE svo.D_AR_TRANSACTION AS D
        USING #src AS S ON D.AR_TRANSACTION_ID = S.AR_TRANSACTION_ID
        WHEN MATCHED THEN
            UPDATE SET
                D.AR_TRANSACTION_NUMBER      = S.AR_TRANSACTION_NUMBER,
                D.AR_REFERENCE               = S.AR_REFERENCE,
                D.AR_PO_NUMBER               = S.AR_PO_NUMBER,
                D.AR_TRANSACTION_CLASS_CODE  = S.AR_TRANSACTION_CLASS_CODE,
                D.AR_TRANSACTION_STATUS_CODE = S.AR_TRANSACTION_STATUS_CODE,
                D.AR_REASON_CODE             = S.AR_REASON_CODE,
                D.AR_COMPLETE_FLAG           = S.AR_COMPLETE_FLAG,
                D.AddDateTime                = S.AddDateTime
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (AR_TRANSACTION_ID, AR_TRANSACTION_NUMBER, AR_REFERENCE, AR_PO_NUMBER, AR_TRANSACTION_CLASS_CODE, AR_TRANSACTION_STATUS_CODE, AR_REASON_CODE, AR_COMPLETE_FLAG, AddDateTime)
            VALUES (S.AR_TRANSACTION_ID, S.AR_TRANSACTION_NUMBER, S.AR_REFERENCE, S.AR_PO_NUMBER, S.AR_TRANSACTION_CLASS_CODE, S.AR_TRANSACTION_STATUS_CODE, S.AR_REASON_CODE, S.AR_COMPLETE_FLAG, S.AddDateTime)
        OUTPUT $action INTO @MergeActions(ActionTaken);

        SELECT @RowInserted = SUM(CASE WHEN ActionTaken = 'INSERT' THEN 1 ELSE 0 END),
               @RowUpdated  = SUM(CASE WHEN ActionTaken = 'UPDATE' THEN 1 ELSE 0 END)
        FROM @MergeActions;

        IF @MaxWatermark IS NOT NULL
            UPDATE etl.ETL_WATERMARK SET LAST_WATERMARK = @MaxWatermark, UDT_DATE = SYSDATETIME() WHERE TABLE_NAME = @TargetObject;

        SET @EndDttm = SYSDATETIME();
        UPDATE etl.ETL_RUN SET END_DTTM = @EndDttm, STATUS = 'SUCCESS', ROW_INSERTED = @RowInserted, ROW_UPDATED = @RowUpdated, ROW_EXPIRED = 0, ERROR_MESSAGE = NULL WHERE RUN_ID = @RunId;
        IF COL_LENGTH('etl.ETL_RUN', 'ROW_UPDATED_T1') IS NOT NULL
            UPDATE etl.ETL_RUN SET ROW_UPDATED_T1 = @RowUpdated WHERE RUN_ID = @RunId;
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
