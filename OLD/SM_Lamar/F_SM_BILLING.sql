USE [Oracle_Reporting_P2];
GO

IF OBJECT_ID('svo.F_SM_BILLING','U') IS NOT NULL 
    DROP TABLE svo.F_SM_BILLING;
GO

CREATE TABLE svo.F_SM_BILLING
(
    BILLING_FACT_PK          BIGINT IDENTITY(1,1) NOT NULL PRIMARY KEY,

    BILL_LINE_ID             BIGINT NOT NULL,
    SUBSCRIPTION_SK          BIGINT NOT NULL,
    SUBSCRIPTION_PRODUCT_SK  BIGINT NOT NULL,
    LEGAL_ENTITY_SK          BIGINT NOT NULL,
    ITEM_SK                  BIGINT NOT NULL,

    -- what you actually have today from the SM subscription header
    CUSTOMER_ACCOUNT_ID      BIGINT NULL,   -- from D_SM_SUBSCRIPTION.BILL_TO_ACCT_ID
    CUSTOMER_SITE_USE_ID     BIGINT NULL,   -- from D_SM_SUBSCRIPTION.BILL_TO_SITE_USE_ID

    -- conformed calendar
    CHARGE_DATE_SK           INT NULL,
    DATE_BILLED_FROM_SK      INT NULL,
    DATE_BILLED_TO_SK        INT NULL,

    CHARGE_ID                BIGINT NULL,
    BILL_LINES_AMOUNT        DECIMAL(18,4) NULL,

    BZ_LOAD_DATE             DATE NULL,
    SV_LOAD_DATE             DATE NULL
) ON [FG_SilverFact];
GO


WITH BASE AS (
    SELECT
        BL.BillLineId,
        BL.SubscriptionId,
        BL.SubscriptionProductId,
        BL.ChargeId,
        BL.InvoiceDate,
        BL.DateBilledFrom,
        BL.DateBilledTo,
        BL.Amount
    FROM bzo.OSS_SubscriptionBillLineExtractPVO BL
),
HDR AS (
    SELECT
        B.*,
        H.LegalEntityId
    FROM BASE B
    LEFT JOIN bzo.OSS_SubscriptionHeaderExtractPVO H
        ON B.SubscriptionId = H.SubscriptionId
),
PROD AS (
    SELECT
        H.*,
        P.InventoryItemId,
        P.DefinitionOrgId
    FROM HDR H
    LEFT JOIN bzo.OSS_SubscriptionProductExtractPVO P
        ON H.SubscriptionProductId = P.SubscriptionProductId
)
INSERT INTO svo.F_SM_BILLING
(
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
    P.BillLineId,
    ISNULL(DS.SUBSCRIPTION_SK,0)     AS SUBSCRIPTION_SK,
    ISNULL(DSP.SUBSCRIPTION_PRODUCT_SK,0)     AS SUBSCRIPTION_PRODUCT_SK,
    ISNULL(DLE.LEGAL_ENTITY_SK,0)     AS LEGAL_ENTITY_SK,
    ISNULL(DI.ITEM_SK,0)   AS ITEM_SK,
    ISNULL(DS.BILL_TO_ACCT_ID,0)      AS CUSTOMER_ACCOUNT_ID,
    ISNULL(DS.BILL_TO_SITE_USE_ID,0)  AS CUSTOMER_SITE_USE_ID,
    ISNULL(DC1.DATE_SK,0)    AS CHARGE_DATE_SK,
    ISNULL(DC2.DATE_SK,0)    AS DATE_BILLED_FROM_SK,
    ISNULL(DC3.DATE_SK,0)    AS DATE_BILLED_TO_SK,
    ISNULL(P.ChargeId,0)     AS CHARGE_ID,
    ISNULL(P.Amount,0)       AS AMOUNT,
    CAST(GETDATE() AS DATE),
    CAST(GETDATE() AS DATE)
FROM PROD P
LEFT JOIN svo.D_SM_SUBSCRIPTION         DS  ON DS.SUBSCRIPTION_ID = P.SubscriptionId
LEFT JOIN svo.D_SM_SUBSCRIPTION_PRODUCT DSP ON DSP.SUBSCRIPTION_PRODUCT_ID = P.SubscriptionProductId
LEFT JOIN svo.D_ITEM                    DI  ON DI.INVENTORY_ITEM_ID = P.InventoryItemId
LEFT JOIN svo.D_LEGAL_ENTITY            DLE ON DLE.LEGAL_ENTITY_ID = P.LegalEntityId
LEFT JOIN svo.D_CALENDAR                DC1 ON DC1.DATE   = CAST(P.InvoiceDate     AS DATE)
LEFT JOIN svo.D_CALENDAR                DC2 ON DC2.DATE   = CAST(P.DateBilledFrom AS DATE)
LEFT JOIN svo.D_CALENDAR                DC3 ON DC3.DATE   = CAST(P.DateBilledTo   AS DATE)
--WHERE NOT EXISTS (
--    SELECT 1 FROM svo.F_SM_BILLING F WHERE F.BILL_LINE_ID = P.BillLineId
--);
GO