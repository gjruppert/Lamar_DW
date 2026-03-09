/* =========================================================
   usp_Load_F_SM_BILLING
   Incremental INSERT only. Sources: bzo.OSS_SubscriptionBillLineExtractPVO,
   OSS_SubscriptionHeaderExtractPVO, OSS_SubscriptionProductExtractPVO.
   Filter: BL.AddDateTime > @LastWatermark. Dedupe by BILL_LINE_ID.
   Resolve SKs via svo.D_SM_SUBSCRIPTION, D_SM_SUBSCRIPTION_PRODUCT, D_ITEM,
   D_LEGAL_ENTITY, D_CALENDAR.
   ========================================================= */
CREATE OR ALTER PROCEDURE svo.usp_Load_F_SM_BILLING
    @BatchId INT = NULL
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    DECLARE
        @ProcName       SYSNAME        = OBJECT_SCHEMA_NAME(@@PROCID) + '.' + OBJECT_NAME(@@PROCID),
        @TargetObject   SYSNAME        = 'svo.F_SM_BILLING',
        @StartDttm      DATETIME2(0)   = SYSDATETIME(),
        @EndDttm        DATETIME2(0),
        @RunId          BIGINT         = NULL,
        @ErrMsg         NVARCHAR(4000) = NULL,
        @AsOfDate       DATE           = CAST(GETDATE() AS DATE),
        @LastWatermark  DATETIME2(7),
        @MaxWatermark   DATETIME2(7)   = NULL,
        @RowInserted    INT            = 0,
        @TableBridgeID  INT            = NULL;

    SELECT @TableBridgeID = TableBridgeID FROM meta.MedallionTableBridge WHERE targettable = N'OSS_SubscriptionBillLineExtractPVO';

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

        IF OBJECT_ID('tempdb..#lines') IS NOT NULL DROP TABLE #lines;

        SELECT
            BL.BillLineId,
            BL.SubscriptionId,
            BL.SubscriptionProductId,
            BL.ChargeId,
            BL.InvoiceDate,
            BL.DateBilledFrom,
            BL.DateBilledTo,
            BL.Amount,
            H.LegalEntityId,
            P.InventoryItemId,
            BL.AddDateTime AS SourceAddDateTime
        INTO #lines
        FROM bzo.OSS_SubscriptionBillLineExtractPVO BL
        LEFT JOIN bzo.OSS_SubscriptionHeaderExtractPVO H ON H.SubscriptionId = BL.SubscriptionId
        LEFT JOIN bzo.OSS_SubscriptionProductExtractPVO P ON P.SubscriptionProductId = BL.SubscriptionProductId
        WHERE BL.AddDateTime > @LastWatermark;

        SELECT @MaxWatermark = MAX(SourceAddDateTime) FROM #lines;

        INSERT INTO svo.F_SM_BILLING WITH (TABLOCK) (
            BILL_LINE_ID,
            SUBSCRIPTION_SK,
            SUBSCRIPTION_PRODUCT_SK,
            LEGAL_ENTITY_SK,
            ITEM_SK,
            CUSTOMER_ACCOUNT_ID,
            CUSTOMER_SITE_USE_ID,
            CHARGE_DATE_SK,
            DATE_BILLED_FROM_SK,
            DATE_BILLED_TO_SK,
            CHARGE_ID,
            BILL_LINES_AMOUNT,
            BZ_LOAD_DATE,
            SV_LOAD_DATE
        )
        SELECT
            L.BillLineId,
            ISNULL(DS.SUBSCRIPTION_SK, 0),
            ISNULL(DSP.SUBSCRIPTION_PRODUCT_SK, 0),
            ISNULL(DLE.LEGAL_ENTITY_SK, 0),
            ISNULL(DI.ITEM_SK, 0),
            DS.BILL_TO_ACCT_ID,
            DS.BILL_TO_SITE_USE_ID,
            ISNULL(DC1.DATE_SK, 0),
            ISNULL(DC2.DATE_SK, 0),
            ISNULL(DC3.DATE_SK, 0),
            ISNULL(L.ChargeId, 0),
            ISNULL(L.Amount, 0),
            CAST(GETDATE() AS DATE),
            CAST(GETDATE() AS DATE)
        FROM #lines L
        LEFT JOIN svo.D_SM_SUBSCRIPTION         DS  ON DS.SUBSCRIPTION_ID = L.SubscriptionId
        LEFT JOIN svo.D_SM_SUBSCRIPTION_PRODUCT DSP ON DSP.SUBSCRIPTION_PRODUCT_ID = L.SubscriptionProductId
        LEFT JOIN svo.D_ITEM                    DI  ON DI.INVENTORY_ITEM_ID = L.InventoryItemId AND DI.CURR_IND = 'Y'
        LEFT JOIN svo.D_LEGAL_ENTITY            DLE ON DLE.LEGAL_ENTITY_ID = L.LegalEntityId AND DLE.CURR_IND = 'Y'
        LEFT JOIN svo.D_CALENDAR                DC1 ON DC1.DATE = CAST(L.InvoiceDate     AS DATE)
        LEFT JOIN svo.D_CALENDAR                DC2 ON DC2.DATE = CAST(L.DateBilledFrom AS DATE)
        LEFT JOIN svo.D_CALENDAR                DC3 ON DC3.DATE = CAST(L.DateBilledTo    AS DATE)
        WHERE NOT EXISTS (SELECT 1 FROM svo.F_SM_BILLING F WHERE F.BILL_LINE_ID = L.BillLineId);

        SET @RowInserted = @@ROWCOUNT;

        IF @MaxWatermark IS NOT NULL
            UPDATE etl.ETL_WATERMARK SET LAST_WATERMARK = @MaxWatermark, UDT_DATE = SYSDATETIME() WHERE TABLE_NAME = @TargetObject;

        SET @EndDttm = SYSDATETIME();
        UPDATE etl.ETL_RUN SET END_DTTM = @EndDttm, STATUS = 'SUCCESS', ROW_INSERTED = @RowInserted, ROW_UPDATED = 0, ROW_EXPIRED = 0, ERROR_MESSAGE = NULL WHERE RUN_ID = @RunId;
    END TRY
    BEGIN CATCH
        SET @EndDttm = SYSDATETIME();
        SET @ErrMsg = CONCAT('Error ', ERROR_NUMBER(), ' (Line ', ERROR_LINE(), '): ', ERROR_MESSAGE());
        IF @RunId IS NOT NULL
            UPDATE etl.ETL_RUN SET END_DTTM = @EndDttm, STATUS = 'FAILURE', ROW_INSERTED = @RowInserted, ROW_UPDATED = 0, ROW_EXPIRED = 0, ERROR_MESSAGE = @ErrMsg WHERE RUN_ID = @RunId;
        ;THROW;
    END CATCH
END;
GO
