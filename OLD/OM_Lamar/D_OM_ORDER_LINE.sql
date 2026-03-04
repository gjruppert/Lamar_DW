USE [Oracle_Reporting_P2]
GO

-- Initial load D_OM_ORDER_LINE

IF OBJECT_ID('svo.D_OM_ORDER_LINE','U') IS NOT NULL 
    DROP TABLE svo.D_OM_ORDER_LINE;
GO

BEGIN
    CREATE TABLE svo.D_OM_ORDER_LINE
    (
        ORDER_LINE_SK        BIGINT IDENTITY(1,1) NOT NULL PRIMARY KEY,
        ORDER_LINE_ID        BIGINT       NOT NULL,   -- LineId
        ORDER_HEADER_ID      BIGINT       NOT NULL,   -- LineHeaderId
        DISPLAY_LINE_NUMBER        VARCHAR(100) NULL,       -- LineDisplayLineNumber
        LINE_NUMBER                BIGINT       NULL,       -- LineLineNumber
        LINE_STATUS_CODE           VARCHAR(30)  NULL,       -- LineStatusCode
        LINE_CATEGORY_CODE         VARCHAR(30)  NULL,       -- LineCategoryCode
        LINE_TYPE_CODE             VARCHAR(30)  NULL,       -- LineLineTypeCode
        ITEM_TYPE_CODE             VARCHAR(30)  NULL,       -- FulfillLineItemTypeCode (fallback)
        INVENTORY_ITEM_ID          BIGINT       NULL,       -- LineInventoryItemId
        INVENTORY_ORG_ID           BIGINT       NULL,       -- LineInventoryOrganizationId
        BOOKING_TYPE               VARCHAR(150) NULL,
        BUNDLE_CODE                VARCHAR(150) NULL,
        BUNDLE_DESCRIPTION         VARCHAR(150) NULL,
        BUSINESS_OFFERING_RAW      VARCHAR(150) NULL,
        MARKET                     VARCHAR(150) NULL,
        MEDIA_TYPE                 VARCHAR(150) NULL,
        REFUND_FLAG                VARCHAR(150) NULL,
        AE_CODE                    VARCHAR(150) NULL,
        AE_NAME                    VARCHAR(150) NULL,
        SALES_TEAM                 VARCHAR(150) NULL,
        LOB                        VARCHAR(150) NULL,
        INDUSTRY_NAME              VARCHAR(150) NULL,
        DIGITAL_SLOT_NUMBER        BIGINT       NULL,
        DIGITAL_SLOT_TYPE          VARCHAR(150) NULL,
        OVERRIDE_SUPPRESS_SEND     VARCHAR(150) NULL,
        UNIT_LIST_PRICE            DECIMAL(18,4) NULL,
        UNIT_SELLING_PRICE         DECIMAL(18,4) NULL,
        ORDERED_QTY                BIGINT       NULL,
        ORDERED_UOM                VARCHAR(3)   NULL,
        BZ_LOAD_DATE               DATE         NULL,
        SV_LOAD_DATE               DATE         NULL
    ) ON [FG_SilverDim];

    -- plug
    INSERT INTO svo.D_OM_ORDER_LINE (
        ORDER_LINE_ID, ORDER_HEADER_ID, DISPLAY_LINE_NUMBER, LINE_NUMBER, LINE_STATUS_CODE,
        LINE_CATEGORY_CODE, LINE_TYPE_CODE, ITEM_TYPE_CODE, INVENTORY_ITEM_ID, INVENTORY_ORG_ID,
        BOOKING_TYPE, BUNDLE_CODE, BUNDLE_DESCRIPTION, BUSINESS_OFFERING_RAW, MARKET, MEDIA_TYPE, REFUND_FLAG,
        AE_CODE, AE_NAME, SALES_TEAM, LOB, INDUSTRY_NAME, DIGITAL_SLOT_NUMBER, DIGITAL_SLOT_TYPE,
        OVERRIDE_SUPPRESS_SEND, UNIT_LIST_PRICE, UNIT_SELLING_PRICE, ORDERED_QTY, ORDERED_UOM, BZ_LOAD_DATE, SV_LOAD_DATE
    )
    VALUES (
        -1,-1,'Unknown',-1,'Unknown','Unknown','Unknown','Unknown',-1,-1,'Unknown','Unknown','Unknown',
        'Unknown','Unknown','Unknown','Unknown','Unknown','Unknown','Unknown','Unknown','Unknown',-1,'Unknown',
        'Unknown',0,0,0,'UNK',CAST('0001-01-01' AS DATE),CAST('0001-01-01' AS DATE)
    );

    CREATE UNIQUE NONCLUSTERED INDEX UX_D_OM_SOL_ID
        ON svo.D_OM_ORDER_LINE (ORDER_LINE_ID)
        ON [FG_SilverDim];
END;
GO

INSERT INTO svo.D_OM_ORDER_LINE (
    ORDER_LINE_ID,
    ORDER_HEADER_ID,
    DISPLAY_LINE_NUMBER,
    LINE_NUMBER,
    LINE_STATUS_CODE,
    LINE_CATEGORY_CODE,
    LINE_TYPE_CODE,
    ITEM_TYPE_CODE,
    INVENTORY_ITEM_ID,
    INVENTORY_ORG_ID,
    BOOKING_TYPE,
    BUNDLE_CODE,
    BUNDLE_DESCRIPTION,
    BUSINESS_OFFERING_RAW,
    MARKET,
    MEDIA_TYPE,
    REFUND_FLAG,
    AE_CODE,
    AE_NAME,
    SALES_TEAM,
    LOB,
    INDUSTRY_NAME,
    DIGITAL_SLOT_NUMBER,
    DIGITAL_SLOT_TYPE,
    OVERRIDE_SUPPRESS_SEND,
    UNIT_LIST_PRICE,
    UNIT_SELLING_PRICE,
    ORDERED_QTY,
    ORDERED_UOM,
    BZ_LOAD_DATE,
    SV_LOAD_DATE
)
SELECT DISTINCT
    L.LineId                                        AS ORDER_LINE_ID,
    ISNULL(L.LineHeaderId,-1)                       AS ORDER_HEADER_ID,
    ISNULL(L.LineDisplayLineNumber,-1)              AS DISPLAY_LINE_NUMBER,
    ISNULL(L.LineLineNumber,-1)                     AS LINE_NUMBER,
    ISNULL(L.LineStatusCode,'UNK')                  AS LINE_STATUS_CODE,
    ISNULL(L.LineCategoryCode,'UNK')                AS LINE_CATEGORY_CODE,
    ISNULL(L.LineLineTypeCode,'UNK')                AS LINE_TYPE_CODE,
    ISNULL(FL.FulfillLineItemTypeCode,'UNK')        AS ITEM_TYPE_CODE,
    ISNULL(L.LineInventoryItemId,-1)                AS INVENTORY_ITEM_ID,
    ISNULL(L.LineInventoryOrganizationId,-1)        AS INVENTORY_ORG_ID,
    ISNULL(LP.bookingType,-1)                       AS BOOKING_TYPE,
    ISNULL(LP.bundleCode,-1)                        AS BUNDLE_CODE,
    ISNULL(LP.bundleDescription,-1)                 AS BUNDLE_DESCRIPTION,
    ISNULL(LP.businessOffering,-1)                  AS BUSINESS_OFFERING_RAW,
    ISNULL(LP.market,-1)                            AS MARKET,
    COALESCE(R.mediaType, LP.mediaTypeProductType,-1)  AS MEDIA_TYPE,
    ISNULL(LP.refund,-1)                            AS REFUND_FLAG,
    ISNULL(R.aeCode,-1)                             AS AE_CODE,
    ISNULL(R.aeName,-1)                             AS AE_NAME,
    ISNULL(R.salesTeam,-1)                          AS SALES_TEAM,
    ISNULL(R.lob,-1)                                AS LOB,
    ISNULL(R.industry,-1)                           AS INDUSTRY_NAME,
    ISNULL(R2.digitalSlotNumber,-1)                 AS DIGITAL_SLOT_NUMBER,
    ISNULL(R2.digitalSlotType,-1)                   AS DIGITAL_SLOT_TYPE,
    ISNULL(OVR.suppressSendToRmcs,-1)               AS OVERRIDE_SUPPRESS_SEND,
    ISNULL(L.LineUnitListPrice,0)                             AS UNIT_LIST_PRICE,
    ISNULL(L.LineUnitSellingPrice,0)                        AS UNIT_SELLING_PRICE,
    ISNULL(L.LineOrderedQty,0)                                AS ORDERED_QTY,
    L.LineOrderedUom                                AS ORDERED_UOM,
    CAST(L.AddDateTime AS DATE)                     AS BZ_LOAD_DATE,
    CAST(GETDATE() AS DATE)                         AS SV_LOAD_DATE
FROM bzo.OM_LineExtractPVO              L
LEFT JOIN bzo.OM_5FLineprivateVO        LP   ON LP.EffLineId      = L.LineId
LEFT JOIN bzo.OM_ReferenceprivateVO     R    ON R.EffLineId       = L.LineId
LEFT JOIN bzo.OM_References2privateVO   R2   ON R2.EffLineId      = L.LineId
LEFT JOIN bzo.OM_OverrideprivateVO      OVR  ON OVR.EffLineId     = L.LineId
LEFT JOIN bzo.OM_FulfillLineExtractPVO  FL   ON FL.FulfillLineLineId = L.LineId
WHERE L.LineId IS NOT NULL
  AND NOT EXISTS (
        SELECT 1
        FROM svo.D_OM_ORDER_LINE d
        WHERE d.ORDER_LINE_ID = L.LineId
    );
GO
