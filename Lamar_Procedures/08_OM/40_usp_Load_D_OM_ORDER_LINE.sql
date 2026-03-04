/* =========================================================
   usp_Load_D_OM_ORDER_LINE
   Type 1 incremental load. Source: bzo.OM_LineExtractPVO + 5F/Reference/Override/FulfillLine
   Watermark: AddDateTime. Grain: ORDER_LINE_ID. Plug row SK=0 if missing.
   Idempotent: safe to retry.
   ========================================================= */
CREATE OR ALTER PROCEDURE svo.usp_Load_D_OM_ORDER_LINE
    @BatchId INT = NULL
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    DECLARE
        @ProcName       SYSNAME        = OBJECT_SCHEMA_NAME(@@PROCID) + '.' + OBJECT_NAME(@@PROCID),
        @TargetObject   SYSNAME        = 'svo.D_OM_ORDER_LINE',
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

    SELECT @TableBridgeID = TableBridgeID FROM meta.MedallionTableBridge WHERE targettable = N'OM_LineExtractPVO';

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

        IF NOT EXISTS (SELECT 1 FROM svo.D_OM_ORDER_LINE WHERE ORDER_LINE_SK = 0)
        BEGIN
            SET IDENTITY_INSERT svo.D_OM_ORDER_LINE ON;
            INSERT INTO svo.D_OM_ORDER_LINE (ORDER_LINE_SK, ORDER_LINE_ID, ORDER_HEADER_ID, DISPLAY_LINE_NUMBER, LINE_NUMBER, LINE_STATUS_CODE, LINE_CATEGORY_CODE, LINE_TYPE_CODE, ITEM_TYPE_CODE, INVENTORY_ITEM_ID, INVENTORY_ORG_ID, BOOKING_TYPE, BUNDLE_CODE, BUNDLE_DESCRIPTION, BUSINESS_OFFERING_RAW, MARKET, MEDIA_TYPE, REFUND_FLAG, AE_CODE, AE_NAME, SALES_TEAM, LOB, INDUSTRY_NAME, DIGITAL_SLOT_NUMBER, DIGITAL_SLOT_TYPE, OVERRIDE_SUPPRESS_SEND, UNIT_LIST_PRICE, UNIT_SELLING_PRICE, ORDERED_QTY, ORDERED_UOM, BZ_LOAD_DATE, SV_LOAD_DATE)
            VALUES (0, -1, -1, 'Unknown', -1, 'Unknown', 'Unknown', 'Unknown', 'Unknown', -1, -1, 'Unknown', 'Unknown', 'Unknown', 'Unknown', 'Unknown', 'Unknown', 'Unknown', 'Unknown', 'Unknown', 'Unknown', 'Unknown', 'Unknown', -1, 'Unknown', 'Unknown', 0, 0, 0, 'UNK', CAST('0001-01-01' AS DATE), CAST('0001-01-01' AS DATE));
            SET IDENTITY_INSERT svo.D_OM_ORDER_LINE OFF;
        END;

        IF OBJECT_ID('tempdb..#src') IS NOT NULL DROP TABLE #src;

        SELECT
            L.LineId AS ORDER_LINE_ID,
            ISNULL(L.LineHeaderId, -1) AS ORDER_HEADER_ID,
            ISNULL(L.LineDisplayLineNumber, -1) AS DISPLAY_LINE_NUMBER,
            ISNULL(L.LineLineNumber, -1) AS LINE_NUMBER,
            ISNULL(L.LineStatusCode, 'UNK') AS LINE_STATUS_CODE,
            ISNULL(L.LineCategoryCode, 'UNK') AS LINE_CATEGORY_CODE,
            ISNULL(L.LineLineTypeCode, 'UNK') AS LINE_TYPE_CODE,
            ISNULL(FL.FulfillLineItemTypeCode, 'UNK') AS ITEM_TYPE_CODE,
            ISNULL(L.LineInventoryItemId, -1) AS INVENTORY_ITEM_ID,
            ISNULL(L.LineInventoryOrganizationId, -1) AS INVENTORY_ORG_ID,
            ISNULL(LP.bookingType, -1) AS BOOKING_TYPE,
            ISNULL(LP.bundleCode, -1) AS BUNDLE_CODE,
            ISNULL(LP.bundleDescription, -1) AS BUNDLE_DESCRIPTION,
            ISNULL(LP.businessOffering, -1) AS BUSINESS_OFFERING_RAW,
            ISNULL(LP.market, -1) AS MARKET,
            COALESCE(R.mediaType, LP.mediaTypeProductType, -1) AS MEDIA_TYPE,
            ISNULL(LP.refund, -1) AS REFUND_FLAG,
            ISNULL(R.aeCode, -1) AS AE_CODE,
            ISNULL(R.aeName, -1) AS AE_NAME,
            ISNULL(R.salesTeam, -1) AS SALES_TEAM,
            ISNULL(R.lob, -1) AS LOB,
            ISNULL(R.industry, -1) AS INDUSTRY_NAME,
            ISNULL(R2.digitalSlotNumber, -1) AS DIGITAL_SLOT_NUMBER,
            ISNULL(R2.digitalSlotType, -1) AS DIGITAL_SLOT_TYPE,
            ISNULL(OVR.suppressSendToRmcs, -1) AS OVERRIDE_SUPPRESS_SEND,
            ISNULL(L.LineUnitListPrice, 0) AS UNIT_LIST_PRICE,
            ISNULL(L.LineUnitSellingPrice, 0) AS UNIT_SELLING_PRICE,
            ISNULL(L.LineOrderedQty, 0) AS ORDERED_QTY,
            L.LineOrderedUom AS ORDERED_UOM,
            CAST(L.AddDateTime AS DATE) AS BZ_LOAD_DATE,
            CAST(GETDATE() AS DATE) AS SV_LOAD_DATE,
            L.AddDateTime AS SourceAddDateTime
        INTO #src
        FROM bzo.OM_LineExtractPVO L
        LEFT JOIN bzo.OM_5FLineprivateVO LP ON LP.EffLineId = L.LineId
        LEFT JOIN bzo.OM_ReferenceprivateVO R ON R.EffLineId = L.LineId
        LEFT JOIN bzo.OM_References2privateVO R2 ON R2.EffLineId = L.LineId
        LEFT JOIN bzo.OM_OverrideprivateVO OVR ON OVR.EffLineId = L.LineId
        LEFT JOIN bzo.OM_FulfillLineExtractPVO FL ON FL.FulfillLineId = L.LineId
        WHERE L.AddDateTime > @LastWatermark;

        SELECT @MaxWatermark = MAX(SourceAddDateTime) FROM #src;

        MERGE svo.D_OM_ORDER_LINE AS tgt
        USING #src AS src ON tgt.ORDER_LINE_ID = src.ORDER_LINE_ID
        WHEN MATCHED THEN UPDATE SET
            tgt.ORDER_HEADER_ID = src.ORDER_HEADER_ID,
            tgt.DISPLAY_LINE_NUMBER = src.DISPLAY_LINE_NUMBER,
            tgt.LINE_NUMBER = src.LINE_NUMBER,
            tgt.LINE_STATUS_CODE = src.LINE_STATUS_CODE,
            tgt.LINE_CATEGORY_CODE = src.LINE_CATEGORY_CODE,
            tgt.LINE_TYPE_CODE = src.LINE_TYPE_CODE,
            tgt.ITEM_TYPE_CODE = src.ITEM_TYPE_CODE,
            tgt.INVENTORY_ITEM_ID = src.INVENTORY_ITEM_ID,
            tgt.INVENTORY_ORG_ID = src.INVENTORY_ORG_ID,
            tgt.BOOKING_TYPE = src.BOOKING_TYPE,
            tgt.BUNDLE_CODE = src.BUNDLE_CODE,
            tgt.BUNDLE_DESCRIPTION = src.BUNDLE_DESCRIPTION,
            tgt.BUSINESS_OFFERING_RAW = src.BUSINESS_OFFERING_RAW,
            tgt.MARKET = src.MARKET,
            tgt.MEDIA_TYPE = src.MEDIA_TYPE,
            tgt.REFUND_FLAG = src.REFUND_FLAG,
            tgt.AE_CODE = src.AE_CODE,
            tgt.AE_NAME = src.AE_NAME,
            tgt.SALES_TEAM = src.SALES_TEAM,
            tgt.LOB = src.LOB,
            tgt.INDUSTRY_NAME = src.INDUSTRY_NAME,
            tgt.DIGITAL_SLOT_NUMBER = src.DIGITAL_SLOT_NUMBER,
            tgt.DIGITAL_SLOT_TYPE = src.DIGITAL_SLOT_TYPE,
            tgt.OVERRIDE_SUPPRESS_SEND = src.OVERRIDE_SUPPRESS_SEND,
            tgt.UNIT_LIST_PRICE = src.UNIT_LIST_PRICE,
            tgt.UNIT_SELLING_PRICE = src.UNIT_SELLING_PRICE,
            tgt.ORDERED_QTY = src.ORDERED_QTY,
            tgt.ORDERED_UOM = src.ORDERED_UOM,
            tgt.BZ_LOAD_DATE = src.BZ_LOAD_DATE,
            tgt.SV_LOAD_DATE = src.SV_LOAD_DATE
        WHEN NOT MATCHED BY TARGET THEN INSERT (
            ORDER_LINE_ID, ORDER_HEADER_ID, DISPLAY_LINE_NUMBER, LINE_NUMBER, LINE_STATUS_CODE, LINE_CATEGORY_CODE, LINE_TYPE_CODE, ITEM_TYPE_CODE, INVENTORY_ITEM_ID, INVENTORY_ORG_ID, BOOKING_TYPE, BUNDLE_CODE, BUNDLE_DESCRIPTION, BUSINESS_OFFERING_RAW, MARKET, MEDIA_TYPE, REFUND_FLAG, AE_CODE, AE_NAME, SALES_TEAM, LOB, INDUSTRY_NAME, DIGITAL_SLOT_NUMBER, DIGITAL_SLOT_TYPE, OVERRIDE_SUPPRESS_SEND, UNIT_LIST_PRICE, UNIT_SELLING_PRICE, ORDERED_QTY, ORDERED_UOM, BZ_LOAD_DATE, SV_LOAD_DATE
        ) VALUES (
            src.ORDER_LINE_ID, src.ORDER_HEADER_ID, src.DISPLAY_LINE_NUMBER, src.LINE_NUMBER, src.LINE_STATUS_CODE, src.LINE_CATEGORY_CODE, src.LINE_TYPE_CODE, src.ITEM_TYPE_CODE, src.INVENTORY_ITEM_ID, src.INVENTORY_ORG_ID, src.BOOKING_TYPE, src.BUNDLE_CODE, src.BUNDLE_DESCRIPTION, src.BUSINESS_OFFERING_RAW, src.MARKET, src.MEDIA_TYPE, src.REFUND_FLAG, src.AE_CODE, src.AE_NAME, src.SALES_TEAM, src.LOB, src.INDUSTRY_NAME, src.DIGITAL_SLOT_NUMBER, src.DIGITAL_SLOT_TYPE, src.OVERRIDE_SUPPRESS_SEND, src.UNIT_LIST_PRICE, src.UNIT_SELLING_PRICE, src.ORDERED_QTY, src.ORDERED_UOM, src.BZ_LOAD_DATE, src.SV_LOAD_DATE
        )
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
