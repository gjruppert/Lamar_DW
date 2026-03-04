USE [Oracle_Reporting_P2];
GO

IF OBJECT_ID('svo.F_OM_FULFILLMENT_LINE','U') IS NOT NULL 
    DROP TABLE svo.F_OM_FULFILLMENT_LINE;
GO

CREATE TABLE svo.F_OM_FULFILLMENT_LINE
(
    FULFILLMENT_FACT_PK           BIGINT IDENTITY(1,1) NOT NULL PRIMARY KEY,
    FULFILL_LINE_ID               BIGINT        NOT NULL,  
    ORDER_LINE_SK                 BIGINT        NOT NULL,
    ORDER_HEADER_SK               BIGINT        NOT NULL,
    FULFILL_DATE_SK               INT           NOT NULL,  
    ACTUAL_SHIP_DATE_SK           INT           NOT NULL,
    SCHEDULE_SHIP_DATE_SK         INT           NOT NULL,
    SHIP_TO_PARTY_SK              BIGINT        NOT NULL,
    SHIP_TO_PARTY_SITE_SK         BIGINT        NOT NULL,
    BILL_TO_CUSTOMER_SK           BIGINT        NOT NULL,
    BILL_TO_SITE_USE_SK           BIGINT        NOT NULL,
    BILL_TO_CONTACT_ID            BIGINT        NOT NULL,
    BILL_TO_CONTACT_POINT_SK      BIGINT        NOT NULL,
    SHIP_TO_PARTY_CONTACT_ID      BIGINT        NOT NULL,
    FULFILL_ORG_SK                BIGINT        NOT NULL,
    SHIP_TO_CONTACT_POINT_ID      BIGINT        NOT NULL,
    PAYMENT_TERM_ID               BIGINT        NOT NULL,
    BUSINESS_UNIT_SK              BIGINT        NOT NULL,
    CARRIER_ID                    BIGINT        NOT NULL,
    INVENTORY_ORGANIZATION_ID     BIGINT        NOT NULL,
    INVENTORY_ITEM_SK             BIGINT        NOT NULL,

    STATUS_CODE                   VARCHAR(30)   NULL,    
    ON_HOLD_FLAG                  VARCHAR(1)    NULL,      
    HOLD_APPLY_DATE               DATE          NULL,
    HOLD_RELEASE_DATE             DATE          NULL,

    ORDERED_QTY                   BIGINT        NULL,      
    FULFILLED_QTY                 BIGINT        NULL,     
    SHIPPED_QTY                   BIGINT        NULL,      
    UNIT_SELLING_PRICE            DECIMAL(18,4) NULL,    
    EXTENDED_AMOUNT               DECIMAL(18,4) NULL,     

    CURRENCY_CODE                 VARCHAR(15)   NULL,     
    BZ_LOAD_DATE                  DATE          NULL,
    SV_LOAD_DATE                  DATE          NULL
) ON [FG_SilverFact];
GO

CREATE NONCLUSTERED INDEX IX_F_FULFILLMENT_Date
    ON svo.F_OM_FULFILLMENT_LINE (FULFILL_DATE_SK, ORDER_HEADER_SK, ORDER_LINE_SK)
    ON [FG_SilverFact];

CREATE NONCLUSTERED INDEX IX_F_FULFILLMENT_SO_LINE
    ON svo.F_OM_FULFILLMENT_LINE (ORDER_LINE_SK)
    ON [FG_SilverFact];
GO

INSERT INTO svo.F_OM_FULFILLMENT_LINE
(
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
    FL.FulfillLineId                                                AS FULFILL_LINE_ID,
    ISNULL(DSL.ORDER_LINE_SK, 0)                              AS ORDER_LINE_SK,
    ISNULL(DSH.ORDER_HEADER_SK, 0)                            AS ORDER_HEADER_SK,
    CONVERT(INT, FORMAT(ISNULL(FL.FulfillLineFulfillmentDate,'0001-01-01'),'yyyyMMdd'))   AS FULFILL_DATE_SK,
    CONVERT(INT, FORMAT(ISNULL(FL.FulfillLineActualShipDate,'0001-01-01'),'yyyyMMdd'))    AS ACTUAL_SHIP_DATE_SK,
    CONVERT(INT, FORMAT(ISNULL(FL.FulfillLineScheduleShipDate,'0001-01-01'),'yyyyMMdd'))  AS SCHEDULE_SHIP_DATE_SK,
    ISNULL(I.ITEM_SK, 0)                                            AS INVENTORY_ITEM_SK,
    ISNULL(P.PARTY_SK, 0)                                           AS SHIP_TO_PARTY_SK,
    ISNULL(SS.PARTY_SITE_SK, 0)                                     AS SHIP_TO_PARTY_SITE_SK,
    ISNULL(BC.CUSTOMER_SK, 0)                                       AS BILL_TO_CUSTOMER_SK,
    ISNULL(BCU.SITE_USE_SK, 0)                                      AS BILL_TO_SITE_USE_SK,

    ISNULL(FL.FulfillLineBillToContactId, -1)                       AS BILL_TO_CONTACT_ID,
    ISNULL(FL.FulfillLineBillToContactPointId, -1)                  AS BILL_TO_CONTACT_POINT_SK,
    ISNULL(FL.FulfillLineShipToPartyContactId, -1)                  AS SHIP_TO_PARTY_CONTACT_ID,
    ISNULL(FBU.BUSINESS_UNIT_SK, 0 )                                AS FULFILL_ORG_SK,
    ISNULL(FL.FulfillLineShipToContactPointId, -1)                  AS SHIP_TO_CONTACT_POINT_ID,
    ISNULL(FL.FulfillLinePaymentTermId, -1)                         AS PAYMENT_TERM_ID,
    ISNULL(BU.BUSINESS_UNIT_SK, 0 )                                 AS BUSINESS_UNIT_SK,
    ISNULL(FL.FulfillLineCarrierId, -1)                             AS CARRIER_ID,
    ISNULL(FL.FulfillLineInventoryOrganizationId, -1)               AS INVENTORY_ORGANIZATION_ID,

    ISNULL(FL.FulfillLineStatusCode, 'UNK')                         AS STATUS_CODE,
    COALESCE(FL.FulfillLineOnHold, HI.HoldInstanceActiveFlag)       AS ON_HOLD_FLAG,
    ISNULL(CAST(HI.HoldInstanceApplyDate   AS DATE), '0001-01-01')  AS HOLD_APPLY_DATE,
    ISNULL(CAST(HI.HoldInstanceReleaseDate AS DATE), '9999-12-31')  AS HOLD_RELEASE_DATE,

    ISNULL(FL.FulfillLineOrderedQty, 0)                             AS ORDERED_QTY,
    ISNULL(FL.FulfillLineFulfilledQty, 0)                           AS FULFILLED_QTY,
    ISNULL(FL.FulfillLineShippedQty, 0)                             AS SHIPPED_QTY,
    ISNULL(FL.FulfillLineUnitSellingPrice, 0)                       AS UNIT_SELLING_PRICE,
    ISNULL(FL.FulfillLineExtendedAmount, 0)                         AS EXTENDED_AMOUNT,

    ISNULL(H.HeaderTransactionalCurrencyCode, 'UNK')                AS CURRENCY_CODE,
    CAST(H.AddDateTime AS DATE)                                     AS BZ_LOAD_DATE,
    CAST(GETDATE() AS DATE)                                         AS SV_LOAD_DATE
FROM bzo.OM_FulfillLineExtractPVO FL
LEFT JOIN bzo.OM_HoldInstance HI     ON HI.FulfillLineFulfillLineId = FL.FulfillLineId AND HI.HoldInstanceDeletedFlag = 'N'
LEFT JOIN bzo.OM_HeaderExtractPVO H  ON H.HeaderId = FL.FulfillLineHeaderId
LEFT JOIN svo.D_OM_ORDER_HEADER DSH  ON DSH.ORDER_HEADER_ID = H.HeaderId
LEFT JOIN svo.D_OM_ORDER_LINE DSL    ON DSL.ORDER_LINE_ID = FL.FulfillLineLineId

 LEFT JOIN svo.D_ITEM  AS I        ON FL.FulfillLineInventoryItemId = I.INVENTORY_ITEM_ID  AND FL.FulfillLineOrgId = I.ITEM_ORG_ID
 LEFT JOIN svo.D_PARTY AS P        ON FL.FulfillLineShipToPartyId = P.PARTY_ID 
 LEFT JOIN svo.D_PARTY_SITE AS SS  ON FL.FulfillLineShipToPartySiteId = SS.PARTY_SITE_ID
 LEFT JOIN svo.D_CUSTOMER_ACCOUNT AS BC    ON FL.FulfillLineBillToCustomerId = BC.CUSTOMER_ACCOUNT_ID
 LEFT JOIN svo.D_CUSTOMER_SITE_USE AS BCU ON FL.FulfillLineBillToSiteUseId = BCU.SITE_USE
 LEFT JOIN svo.D_BUSINESS_UNIT     AS BU ON FL.FulfillLineOrgId = BU.BUSINESS_UNIT_ID
 LEFT JOIN svo.D_BUSINESS_UNIT     AS FBU ON FL.FulfillLineFulfillOrgId = FBU.BUSINESS_UNIT_ID
 LEFT JOIN svo.D_PARTY_CONTACT_POINT AS PCP ON FL.FulfillLineBillToContactPointId = PCP.CONTACT_POINT_ID
 LEFT JOIN svo.D_PARTY_CONTACT_POINT AS SCP ON FL.FulfillLineShipToContactPointId = SCP.CONTACT_POINT_ID

--WHERE NOT EXISTS
--(
--    SELECT 1
--    FROM svo.F_OM_FULFILLMENT_LINE F
--    WHERE F.FULFILL_LINE_ID = FL.FulfillLineId
--);
GO
