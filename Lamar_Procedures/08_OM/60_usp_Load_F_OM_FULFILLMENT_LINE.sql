/* =========================================================
   usp_Load_F_OM_FULFILLMENT_LINE
   Incremental INSERT only. Source: bzo.OM_FulfillLineExtractPVO + HoldInstance, Header.
   Filter: H.AddDateTime > @LastWatermark. Dedupe by FULFILL_LINE_ID.
   Resolve SKs via svo.D_OM_ORDER_HEADER, D_OM_ORDER_LINE, D_ITEM, D_PARTY, D_PARTY_SITE,
   D_CUSTOMER_ACCOUNT, D_SITE_USE (bill-to site use), D_BUSINESS_UNIT.
   ========================================================= */
CREATE OR ALTER PROCEDURE svo.usp_Load_F_OM_FULFILLMENT_LINE
    @BatchId INT = NULL
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    DECLARE
        @ProcName       SYSNAME        = OBJECT_SCHEMA_NAME(@@PROCID) + '.' + OBJECT_NAME(@@PROCID),
        @TargetObject   SYSNAME        = 'svo.F_OM_FULFILLMENT_LINE',
        @StartDttm      DATETIME2(0)   = SYSDATETIME(),
        @EndDttm        DATETIME2(0),
        @RunId          BIGINT         = NULL,
        @ErrMsg         NVARCHAR(4000) = NULL,
        @AsOfDate       DATE           = CAST(GETDATE() AS DATE),
        @LastWatermark  DATETIME2(7),
        @MaxWatermark   DATETIME2(7)   = NULL,
        @RowInserted    INT            = 0,
        @TableBridgeID  INT            = NULL;

    SELECT @TableBridgeID = TableBridgeID FROM meta.MedallionTableBridge WHERE targettable = N'OM_FulfillLineExtractPVO';

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

        IF OBJECT_ID('tempdb..#fulfill') IS NOT NULL DROP TABLE #fulfill;

        SELECT
            FL.FulfillLineId,
            FL.FulfillLineLineId,
            FL.FulfillLineHeaderId,
            FL.FulfillLineFulfillmentDate,
            FL.FulfillLineActualShipDate,
            FL.FulfillLineScheduleShipDate,
            FL.FulfillLineInventoryItemId,
            FL.FulfillLineOrgId,
            FL.FulfillLineShipToPartyId,
            FL.FulfillLineShipToPartySiteId,
            FL.FulfillLineBillToCustomerId,
            FL.FulfillLineBillToSiteUseId,
            FL.FulfillLineBillToContactId,
            FL.FulfillLineBillToContactPointId,
            FL.FulfillLineShipToPartyContactId,
            FL.FulfillLineFulfillOrgId,
            FL.FulfillLineShipToContactPointId,
            FL.FulfillLinePaymentTermId,
            FL.FulfillLineOrgId AS FulfillLineOrgIdBU,
            FL.FulfillLineCarrierId,
            FL.FulfillLineInventoryOrganizationId,
            FL.FulfillLineStatusCode,
            FL.FulfillLineOnHold,
            HI.HoldInstanceApplyDate,
            HI.HoldInstanceReleaseDate,
            HI.HoldInstanceActiveFlag,
            FL.FulfillLineOrderedQty,
            FL.FulfillLineFulfilledQty,
            FL.FulfillLineShippedQty,
            FL.FulfillLineUnitSellingPrice,
            FL.FulfillLineExtendedAmount,
            H.HeaderTransactionalCurrencyCode,
            H.AddDateTime AS HeaderAddDateTime
        INTO #fulfill
        FROM bzo.OM_FulfillLineExtractPVO FL
        LEFT JOIN bzo.OM_HoldInstance HI ON HI.FulfillLineFulfillLineId = FL.FulfillLineId AND HI.HoldInstanceDeletedFlag = 'N'
        LEFT JOIN bzo.OM_HeaderExtractPVO H ON H.HeaderId = FL.FulfillLineHeaderId
        WHERE H.AddDateTime > @LastWatermark;

        SELECT @MaxWatermark = MAX(HeaderAddDateTime) FROM #fulfill;

        INSERT INTO svo.F_OM_FULFILLMENT_LINE WITH (TABLOCK) (
            FULFILL_LINE_ID,
            ORDER_LINE_SK,
            ORDER_HEADER_SK,
            FULFILL_DATE_SK,
            ACTUAL_SHIP_DATE_SK,
            SCHEDULE_SHIP_DATE_SK,
            INVENTORY_ITEM_SK,
            SHIP_TO_PARTY_SK,
            SHIP_TO_PARTY_SITE_SK,
            BILL_TO_CUSTOMER_SK,
            BILL_TO_SITE_USE_SK,
            BILL_TO_CONTACT_ID,
            BILL_TO_CONTACT_POINT_SK,
            SHIP_TO_PARTY_CONTACT_ID,
            FULFILL_ORG_SK,
            SHIP_TO_CONTACT_POINT_ID,
            PAYMENT_TERM_ID,
            BUSINESS_UNIT_SK,
            CARRIER_ID,
            INVENTORY_ORGANIZATION_ID,
            STATUS_CODE,
            ON_HOLD_FLAG,
            HOLD_APPLY_DATE,
            HOLD_RELEASE_DATE,
            ORDERED_QTY,
            FULFILLED_QTY,
            SHIPPED_QTY,
            UNIT_SELLING_PRICE,
            EXTENDED_AMOUNT,
            CURRENCY_CODE,
            BZ_LOAD_DATE,
            SV_LOAD_DATE
        )
        SELECT
            FL.FulfillLineId,
            ISNULL(DSL.ORDER_LINE_SK, 0),
            ISNULL(DSH.ORDER_HEADER_SK, 0),
            CONVERT(INT, FORMAT(ISNULL(FL.FulfillLineFulfillmentDate, '0001-01-01'), 'yyyyMMdd')),
            CONVERT(INT, FORMAT(ISNULL(FL.FulfillLineActualShipDate, '0001-01-01'), 'yyyyMMdd')),
            CONVERT(INT, FORMAT(ISNULL(FL.FulfillLineScheduleShipDate, '0001-01-01'), 'yyyyMMdd')),
            ISNULL(I.ITEM_SK, 0),
            ISNULL(P.PARTY_SK, 0),
            ISNULL(SS.PARTY_SITE_SK, 0),
            ISNULL(BC.CUSTOMER_SK, 0),
            ISNULL(BCU.SITE_USE_SK, 0),
            ISNULL(FL.FulfillLineBillToContactId, -1),
            ISNULL(FL.FulfillLineBillToContactPointId, -1),
            ISNULL(FL.FulfillLineShipToPartyContactId, -1),
            ISNULL(FBU.BUSINESS_UNIT_SK, 0),
            ISNULL(FL.FulfillLineShipToContactPointId, -1),
            ISNULL(FL.FulfillLinePaymentTermId, -1),
            ISNULL(BU.BUSINESS_UNIT_SK, 0),
            ISNULL(FL.FulfillLineCarrierId, -1),
            ISNULL(FL.FulfillLineInventoryOrganizationId, -1),
            ISNULL(FL.FulfillLineStatusCode, 'UNK'),
            COALESCE(FL.FulfillLineOnHold, FL.HoldInstanceActiveFlag),
            ISNULL(CAST(FL.HoldInstanceApplyDate AS DATE), '0001-01-01'),
            ISNULL(CAST(FL.HoldInstanceReleaseDate AS DATE), '9999-12-31'),
            ISNULL(FL.FulfillLineOrderedQty, 0),
            ISNULL(FL.FulfillLineFulfilledQty, 0),
            ISNULL(FL.FulfillLineShippedQty, 0),
            ISNULL(FL.FulfillLineUnitSellingPrice, 0),
            ISNULL(FL.FulfillLineExtendedAmount, 0),
            ISNULL(FL.HeaderTransactionalCurrencyCode, 'UNK'),
            CAST(FL.HeaderAddDateTime AS DATETIME2(0)),
            SYSDATETIME()
        FROM #fulfill FL
        LEFT JOIN svo.D_OM_ORDER_HEADER DSH ON DSH.ORDER_HEADER_ID = FL.FulfillLineHeaderId
        LEFT JOIN svo.D_OM_ORDER_LINE DSL ON DSL.ORDER_LINE_ID = FL.FulfillLineLineId
        LEFT JOIN svo.D_ITEM I ON FL.FulfillLineInventoryItemId = I.INVENTORY_ITEM_ID AND I.CURR_IND = 'Y'
        LEFT JOIN svo.D_PARTY P ON FL.FulfillLineShipToPartyId = P.PARTY_ID
        LEFT JOIN svo.D_PARTY_SITE SS ON FL.FulfillLineShipToPartySiteId = SS.PARTY_SITE_ID
        LEFT JOIN svo.D_CUSTOMER_ACCOUNT BC ON FL.FulfillLineBillToCustomerId = BC.CUSTOMER_ACCOUNT_ID
        LEFT JOIN svo.D_SITE_USE BCU ON FL.FulfillLineBillToSiteUseId = BCU.SITE_USE AND BCU.CURR_IND = 'Y'
        LEFT JOIN svo.D_BUSINESS_UNIT BU ON FL.FulfillLineOrgId = BU.BUSINESS_UNIT_ID
        LEFT JOIN svo.D_BUSINESS_UNIT FBU ON FL.FulfillLineFulfillOrgId = FBU.BUSINESS_UNIT_ID
        WHERE NOT EXISTS (SELECT 1 FROM svo.F_OM_FULFILLMENT_LINE t WHERE t.FULFILL_LINE_ID = FL.FulfillLineId);

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
