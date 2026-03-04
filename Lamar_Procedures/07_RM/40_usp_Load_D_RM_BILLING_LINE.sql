/* =========================================================
   usp_Load_D_RM_BILLING_LINE
   Type 1 incremental load. Source: bzo.VRM_BillingLineDetailsPVO
   Watermark: AddDateTime. Grain: BILLING_LINE_DETAIL_ID. Plug row SK=0 if missing.
   Idempotent: safe to retry.
   ========================================================= */
CREATE OR ALTER PROCEDURE svo.usp_Load_D_RM_BILLING_LINE
    @BatchId INT = NULL
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    DECLARE
        @ProcName       SYSNAME        = OBJECT_SCHEMA_NAME(@@PROCID) + '.' + OBJECT_NAME(@@PROCID),
        @TargetObject   SYSNAME        = 'svo.D_RM_BILLING_LINE',
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

    SELECT @TableBridgeID = TableBridgeID FROM meta.MedallionTableBridge WHERE targettable = N'VRM_BillingLineDetailsPVO';

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

        IF NOT EXISTS (SELECT 1 FROM svo.D_RM_BILLING_LINE WHERE RM_BILLING_LINE_SK = 0)
        BEGIN
            SET IDENTITY_INSERT svo.D_RM_BILLING_LINE ON;
            INSERT INTO svo.D_RM_BILLING_LINE
            (RM_BILLING_LINE_SK, BILLING_LINE_DETAIL_ID, BILL_DATE, BILL_ID, BILL_LINE_ID, BILL_LINE_NUMBER, BILL_NUMBER, CREATED_BY, CREATION_DATE, LAST_UPDATE_DATE, LAST_UPDATED_BY, BZ_LOAD_DATE, SV_LOAD_DATE)
            VALUES (0, -1, '1900-01-01', -1, -1, 'Unknown', 'Unknown', 'Unknown', '1900-01-01', '1900-01-01', 'Unknown', CAST(GETDATE() AS DATE), CAST(GETDATE() AS DATE));
            SET IDENTITY_INSERT svo.D_RM_BILLING_LINE OFF;
        END;

        IF OBJECT_ID('tempdb..#src') IS NOT NULL DROP TABLE #src;

        SELECT
            ISNULL(B.BillingLineDetailId, -1)                    AS BILLING_LINE_DETAIL_ID,
            ISNULL(B.BillingLineDetailsBillDate, '1900-01-01')   AS BILL_DATE,
            ISNULL(B.BillingLineDetailsBillId, -1)               AS BILL_ID,
            ISNULL(B.BillingLineDetailsBillLineId, -1)           AS BILL_LINE_ID,
            ISNULL(B.BillingLineDetailsBillLineNumber, 'Unknown') AS BILL_LINE_NUMBER,
            ISNULL(B.BillingLineDetailsBillNumber, 'Unknown')    AS BILL_NUMBER,
            ISNULL(B.BillingLineDetailsCreatedBy, 'Unknown')     AS CREATED_BY,
            ISNULL(CAST(B.BillingLineDetailsCreationDate AS DATE), '1900-01-01') AS CREATION_DATE,
            ISNULL(CAST(B.BillingLineDetailsLastUpdateDate AS DATE), '1900-01-01') AS LAST_UPDATE_DATE,
            ISNULL(B.BillingLineDetailsLastUpdatedBy, 'Unknown') AS LAST_UPDATED_BY,
            CAST(B.AddDateTime AS DATE)                          AS BZ_LOAD_DATE,
            CAST(GETDATE() AS DATE)                              AS SV_LOAD_DATE,
            ISNULL(B.AddDateTime, SYSDATETIME())                 AS SourceAddDateTime
        INTO #src
        FROM bzo.VRM_BillingLineDetailsPVO AS B
        WHERE B.AddDateTime > @LastWatermark;

        SELECT @MaxWatermark = MAX(SourceAddDateTime) FROM #src;

        MERGE svo.D_RM_BILLING_LINE AS tgt
        USING #src AS src ON tgt.BILLING_LINE_DETAIL_ID = src.BILLING_LINE_DETAIL_ID
        WHEN MATCHED THEN UPDATE SET
            tgt.BILL_DATE = src.BILL_DATE,
            tgt.BILL_ID = src.BILL_ID,
            tgt.BILL_LINE_ID = src.BILL_LINE_ID,
            tgt.BILL_LINE_NUMBER = src.BILL_LINE_NUMBER,
            tgt.BILL_NUMBER = src.BILL_NUMBER,
            tgt.CREATED_BY = src.CREATED_BY,
            tgt.CREATION_DATE = src.CREATION_DATE,
            tgt.LAST_UPDATE_DATE = src.LAST_UPDATE_DATE,
            tgt.LAST_UPDATED_BY = src.LAST_UPDATED_BY,
            tgt.BZ_LOAD_DATE = src.BZ_LOAD_DATE,
            tgt.SV_LOAD_DATE = src.SV_LOAD_DATE
        WHEN NOT MATCHED BY TARGET THEN INSERT (
            BILLING_LINE_DETAIL_ID, BILL_DATE, BILL_ID, BILL_LINE_ID, BILL_LINE_NUMBER, BILL_NUMBER, CREATED_BY, CREATION_DATE, LAST_UPDATE_DATE, LAST_UPDATED_BY, BZ_LOAD_DATE, SV_LOAD_DATE
        ) VALUES (
            src.BILLING_LINE_DETAIL_ID, src.BILL_DATE, src.BILL_ID, src.BILL_LINE_ID, src.BILL_LINE_NUMBER, src.BILL_NUMBER, src.CREATED_BY, src.CREATION_DATE, src.LAST_UPDATE_DATE, src.LAST_UPDATED_BY, src.BZ_LOAD_DATE, src.SV_LOAD_DATE
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
