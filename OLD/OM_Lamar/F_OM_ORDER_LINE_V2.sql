USE Oracle_Reporting_P2;
GO

IF OBJECT_ID('svo.F_OM_FULFILLMENT_LINE','U') IS NOT NULL
    DROP TABLE svo.F_OM_FULFILLMENT_LINE;
GO

SET ANSI_NULLS ON;
GO
SET QUOTED_IDENTIFIER ON;
GO

CREATE TABLE svo.F_OM_FULFILLMENT_LINE
(
    /* =====================================
       Fact surrogate key
       ===================================== */
    F_OM_FULFILLMENT_LINE_PK   BIGINT IDENTITY(1,1) NOT NULL PRIMARY KEY ,

    /* =====================================
       Degenerate header / line keys
       ===================================== */
    ORDER_HEADER_SK            BIGINT        NOT NULL,  -- DOH.SALES_ORDER_HEADER_SK
    ORDER_LINE_SK              BIGINT        NOT NULL,  -- DSL.ORDER_LINE_SK
    HEADER_ORDER_TYPE_CODE     VARCHAR(30)   NOT NULL,  -- H.HeaderOrderTypeCode

    /* =====================================
       Dimension surrogate keys
       ===================================== */
    ITEM_SK                    BIGINT        NOT NULL,
    SHIP_TO_PARTY_SK           BIGINT        NOT NULL,
    SHIP_TO_PARTY_SITE_SK      BIGINT        NOT NULL,
    BILL_TO_CUSTOMER_SK        BIGINT        NOT NULL,
    BILL_TO_SITE_USE_SK        BIGINT        NOT NULL,
    ORDERING_BUSINESS_UNIT_SK  BIGINT        NOT NULL,
    FULFILL_ORG_SK             BIGINT        NOT NULL,
    BILL_TO_CONTACT_SK         BIGINT        NOT NULL,
    SHIP_TO_CONTACT_SK         BIGINT        NOT NULL,

    /* =====================================
       Date surrogate keys
       ===================================== */
    HEADER_ORDERED_DATE_SK     INT           NOT NULL,
    SCHEDULE_SHIP_DATE_SK      INT           NOT NULL,
    SCHEDULE_ARRIVAL_DATE_SK   INT           NOT NULL,
    FULFILLMENT_DATE_SK        INT           NOT NULL,
    ACTUAL_COMPLETION_DATE_SK  INT           NOT NULL,

    /* =====================================
       Measures
       ===================================== */
    ORDERED_QTY                DECIMAL(18,4) NOT NULL,
    UNIT_QUANTITY              DECIMAL(18,4) NOT NULL,
    FULFILLED_QTY              DECIMAL(18,4) NOT NULL,
    RESERVED_QTY               DECIMAL(18,4) NOT NULL,
    UNIT_LIST_PRICE            DECIMAL(18,4) NOT NULL,
    UNIT_SELLING_PRICE         DECIMAL(18,4) NOT NULL,
    EXTENDED_AMOUNT            DECIMAL(18,4) NOT NULL,

    /* =====================================
       Status / type degenerates
       ===================================== */
    STATUS_CODE                VARCHAR(30)   NOT NULL,
    PRODUCT_TYPE               VARCHAR(30)   NOT NULL,
    ITEM_TYPE_CODE             VARCHAR(30)   NOT NULL,
    LINE_TYPE_CODE             VARCHAR(30)   NOT NULL,
    FULFILLMENT_MODE           VARCHAR(30)   NOT NULL,
    ACTION_TYPE_CODE           VARCHAR(30)   NOT NULL,
    INVOICEABLE_ITEM_FLAG      VARCHAR(1)    NOT NULL,
    INVOICE_ENABLED_FLAG       VARCHAR(1)    NOT NULL,

    /* =====================================
       Load metadata
       ===================================== */
    HEADER_BZ_LOAD_DATE        DATE          NOT NULL,
    LINE_BZ_LOAD_DATE          DATE          NOT NULL,
    SV_LOAD_DATE               DATE          NOT NULL DEFAULT (CAST(GETDATE() AS DATE))
)
ON FG_SilverFact;
GO

INSERT INTO svo.F_OM_FULFILLMENT_LINE
(
    ORDER_HEADER_SK,
    ORDER_LINE_SK,
    HEADER_ORDER_TYPE_CODE,

    ITEM_SK,
    SHIP_TO_PARTY_SK,
    SHIP_TO_PARTY_SITE_SK,
    BILL_TO_CUSTOMER_SK,
    BILL_TO_SITE_USE_SK,
    ORDERING_BUSINESS_UNIT_SK,
    FULFILL_ORG_SK,
    BILL_TO_CONTACT_SK,
    SHIP_TO_CONTACT_SK,

    HEADER_ORDERED_DATE_SK,
    SCHEDULE_SHIP_DATE_SK,
    SCHEDULE_ARRIVAL_DATE_SK,
    FULFILLMENT_DATE_SK,
    ACTUAL_COMPLETION_DATE_SK,

    ORDERED_QTY,
    UNIT_QUANTITY,
    FULFILLED_QTY,
    RESERVED_QTY,
    UNIT_LIST_PRICE,
    UNIT_SELLING_PRICE,
    EXTENDED_AMOUNT,

    STATUS_CODE,
    PRODUCT_TYPE,
    ITEM_TYPE_CODE,
    LINE_TYPE_CODE,
    FULFILLMENT_MODE,
    ACTION_TYPE_CODE,
    INVOICEABLE_ITEM_FLAG,
    INVOICE_ENABLED_FLAG,

    HEADER_BZ_LOAD_DATE,
    LINE_BZ_LOAD_DATE,
    SV_LOAD_DATE
)

SELECT 
      -- Degenerate header keys
       DOH.ORDER_HEADER_SK                    AS ORDER_HEADER_SK 
      ,ISNULL(DSL.ORDER_LINE_SK, 0)           AS ORDER_LINE_SK
      ,ISNULL(H.HeaderOrderTypeCode,'UNK')          AS HEADER_ORDER_TYPE_CODE

      -- Dimension SKs
      ,ISNULL(I.ITEM_SK,0)                          AS ITEM_SK
      ,ISNULL(P.PARTY_SK,0)                         AS SHIP_TO_PARTY_SK
      ,ISNULL(SS.PARTY_SITE_SK,0)                   AS SHIP_TO_PARTY_SITE_SK
      ,ISNULL(BC.CUSTOMER_SK,0)                     AS BILL_TO_CUSTOMER_SK
      ,ISNULL(BCU.SITE_USE_SK,0)                    AS BILL_TO_SITE_USE_SK
      ,ISNULL(BU.BUSINESS_UNIT_SK,0)                AS ORDERING_BUSINESS_UNIT_SK
      ,ISNULL(FBU.BUSINESS_UNIT_SK,0)               AS FULFILL_ORG_SK
      ,ISNULL(PCP.PARTY_CONTACT_POINT_SK,0)         AS BILL_TO_CONTACT_SK
      ,ISNULL(SCP.PARTY_CONTACT_POINT_SK,0)         AS SHIP_TO_CONTACT_SK

      -- Date keys
      ,CONVERT(INT, FORMAT(ISNULL(H.HeaderOrderedDate,'0001-01-01'),'yyyyMMdd'))               AS HEADER_ORDERED_DATE_SK
      ,CONVERT(INT, FORMAT(ISNULL(L.FulfillLineScheduleShipDate,'0001-01-01'),'yyyyMMdd'))     AS SCHEDULE_SHIP_DATE_SK
      ,CONVERT(INT, FORMAT(ISNULL(L.FulfillLineScheduleArrivalDate,'0001-01-01'),'yyyyMMdd'))  AS SCHEDULE_ARRIVAL_DATE_SK
      ,CONVERT(INT, FORMAT(ISNULL(L.FulfillLineFulfillmentDate,'0001-01-01'),'yyyyMMdd'))      AS FULFILLMENT_DATE_SK
      ,CONVERT(INT, FORMAT(ISNULL(L.FulfillLineActualCompletionDate,'0001-01-01'),'yyyyMMdd')) AS ACTUAL_COMPLETION_DATE_SK

      -- Measures
      ,ISNULL(L.FulfillLineOrderedQty,0)            AS ORDERED_QTY
      ,ISNULL(L.FulfillLineUnitQuantity,0)          AS UNIT_QUANTITY
      ,ISNULL(L.FulfillLineFulfilledQty,0)          AS FULFILLED_QTY
      ,ISNULL(L.FulfillLineReservedQty,0)           AS RESERVED_QTY
      ,ISNULL(L.FulfillLineUnitListPrice,0)         AS UNIT_LIST_PRICE
      ,ISNULL(L.FulfillLineUnitSellingPrice,0)      AS UNIT_SELLING_PRICE
      ,ISNULL(L.FulfillLineExtendedAmount,0)        AS EXTENDED_AMOUNT

      -- Line status / type degenerates
      ,ISNULL(L.FulfillLineStatusCode,'UNK')        AS STATUS_CODE
      ,ISNULL(L.FulfillLineProductType,'UNK')       AS PRODUCT_TYPE
      ,ISNULL(L.FulfillLineItemTypeCode,'UNK')      AS ITEM_TYPE_CODE
      ,ISNULL(L.FulfillLineLineTypeCode,'UNK')      AS LINE_TYPE_CODE
      ,ISNULL(L.FulfillLineFulfillmentMode,'UNK')   AS FULFILLMENT_MODE
      ,ISNULL(L.FulfillLineActionTypeCode,'UNK')    AS ACTION_TYPE_CODE
      ,ISNULL(L.FulfillLineInvoiceableItemFlag,'U') AS INVOICEABLE_ITEM_FLAG
      ,ISNULL(L.FulfillLineInvoiceEnabledFlag,'U')  AS INVOICE_ENABLED_FLAG

      -- Load dates
      ,CAST(H.AddDateTime AS DATE)                  AS HEADER_BZ_LOAD_DATE
      ,CAST(L.AddDateTime AS DATE)                  AS LINE_BZ_LOAD_DATE
      ,CAST(GETDATE() AS DATE)                      AS SV_LOAD_DATE

FROM bzo.OM_HeaderExtractPVO          AS H
JOIN bzo.OM_FulfillLineExtractPVO     AS L   ON H.HeaderId                        = L.FulfillLineHeaderId
LEFT JOIN bzo.OM_HoldInstance         AS HI  ON HI.FulfillLineFulfillLineId       = L.FulfillLineId 
                                                                                  AND HI.HoldInstanceDeletedFlag = 'N'
LEFT JOIN svo.D_OM_ORDER_HEADER       AS DSH ON DSH.ORDER_HEADER_ID         = H.HeaderId
LEFT JOIN svo.D_OM_ORDER_LINE         AS DSL ON DSL.ORDER_LINE_ID           = L.FulfillLineLineId

LEFT JOIN svo.D_ITEM                  AS I   ON L.FulfillLineInventoryItemId      = I.INVENTORY_ITEM_ID 
                                                                                  AND L.FulfillLineInventoryOrganizationId  = I.ITEM_ORG_ID
LEFT JOIN svo.D_PARTY                 AS P   ON L.FulfillLineShipToPartyId        = P.PARTY_ID
LEFT JOIN svo.D_PARTY_SITE            AS SS  ON L.FulfillLineShipToPartySiteId    = SS.PARTY_SITE_ID
LEFT JOIN svo.D_CUSTOMER_ACCOUNT      AS BC  ON L.FulfillLineBillToCustomerId     = BC.CUSTOMER_ACCOUNT_ID
LEFT JOIN svo.D_CUSTOMER_SITE_USE     AS BCU ON L.FulfillLineBillToSiteUseId      = BCU.SITE_USE
LEFT JOIN svo.D_BUSINESS_UNIT         AS BU  ON L.FulfillLineOrgId                = BU.BUSINESS_UNIT_ID
LEFT JOIN svo.D_BUSINESS_UNIT         AS FBU ON L.FulfillLineFulfillOrgId         = FBU.BUSINESS_UNIT_ID
LEFT JOIN svo.D_PARTY_CONTACT_POINT   AS PCP ON L.FulfillLineBillToContactId      = PCP.CONTACT_POINT_ID
LEFT JOIN svo.D_PARTY_CONTACT_POINT   AS SCP ON L.FulfillLineShipToPartyContactId = SCP.CONTACT_POINT_ID
LEFT JOIN svo.D_OM_ORDER_HEADER       AS DOH ON H.HeaderId                        = DOH.ORDER_HEADER_ID


