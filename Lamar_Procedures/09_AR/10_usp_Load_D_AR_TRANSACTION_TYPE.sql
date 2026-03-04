/* =========================================================
   usp_Load_D_AR_TRANSACTION_TYPE
   Type 1 incremental. Source: bzo.AR_TransactionTypeExtractPVO
   Watermark: AddDateTime. Grain: AR_TRANSACTION_TYPE_SEQ_ID. Plug row SK=0.
   Idempotent: safe to retry.
   ========================================================= */
CREATE OR ALTER PROCEDURE svo.usp_Load_D_AR_TRANSACTION_TYPE
    @BatchId INT = NULL
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    DECLARE
        @ProcName       SYSNAME        = OBJECT_SCHEMA_NAME(@@PROCID) + '.' + OBJECT_NAME(@@PROCID),
        @TargetObject   SYSNAME        = 'svo.D_AR_TRANSACTION_TYPE',
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

    SELECT @TableBridgeID = TableBridgeID FROM meta.MedallionTableBridge WHERE targettable = N'AR_TransactionTypeExtractPVO';

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

        IF NOT EXISTS (SELECT 1 FROM svo.D_AR_TRANSACTION_TYPE WHERE AR_TRANSACTION_TYPE_SK = 0)
        BEGIN
            SET IDENTITY_INSERT svo.D_AR_TRANSACTION_TYPE ON;
            INSERT INTO svo.D_AR_TRANSACTION_TYPE (AR_TRANSACTION_TYPE_SK, AR_TRANSACTION_TYPE_SEQ_ID, AR_TRANSACTION_TYPE_NAME, AddDateTime, SV_LOAD_DATE)
            VALUES (0, 0, 'UNKNOWN', GETDATE(), CONVERT(date, GETDATE()));
            SET IDENTITY_INSERT svo.D_AR_TRANSACTION_TYPE OFF;
        END;

        IF OBJECT_ID('tempdb..#src') IS NOT NULL DROP TABLE #src;

        SELECT
            T.RaCustTrxTypeCustTrxTypeSeqId    AS AR_TRANSACTION_TYPE_SEQ_ID,
            T.RaCustTrxTypeName                AS AR_TRANSACTION_TYPE_NAME,
            T.AddDateTime,
            CONVERT(date, GETDATE())            AS SV_LOAD_DATE,
            T.AddDateTime                      AS SourceAddDateTime
        INTO #src
        FROM bzo.AR_TransactionTypeExtractPVO T
        WHERE T.AddDateTime > @LastWatermark;

        SELECT @MaxWatermark = MAX(SourceAddDateTime) FROM #src;

        MERGE svo.D_AR_TRANSACTION_TYPE AS D
        USING #src AS S ON D.AR_TRANSACTION_TYPE_SEQ_ID = S.AR_TRANSACTION_TYPE_SEQ_ID
        WHEN MATCHED THEN
            UPDATE SET
                D.AR_TRANSACTION_TYPE_NAME = S.AR_TRANSACTION_TYPE_NAME,
                D.AddDateTime              = S.AddDateTime
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (AR_TRANSACTION_TYPE_SEQ_ID, AR_TRANSACTION_TYPE_NAME, AddDateTime)
            VALUES (S.AR_TRANSACTION_TYPE_SEQ_ID, S.AR_TRANSACTION_TYPE_NAME, S.AddDateTime)
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
