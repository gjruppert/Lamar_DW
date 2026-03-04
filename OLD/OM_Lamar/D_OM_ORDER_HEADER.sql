USE [Oracle_Reporting_P2];
GO

IF OBJECT_ID('svo.D_OM_ORDER_HEADER','U') IS NOT NULL 
    DROP TABLE svo.D_OM_ORDER_HEADER;
GO

BEGIN
    CREATE TABLE svo.D_OM_ORDER_HEADER
    (
        ORDER_HEADER_SK        BIGINT IDENTITY(1,1) NOT NULL PRIMARY KEY,
        ORDER_HEADER_ID        BIGINT       NOT NULL,   -- HeaderId
        LEGAL_ENTITY_SK              BIGINT       NULL,       -- HeaderLegalEntityId
        BUSINESS_UNIT_SK             BIGINT       NULL,       -- HeaderOrgId / FulfillOrg
        SOLD_TO_PARTY_SK             BIGINT       NULL,
        SALESPERSON_SK               BIGINT       NULL,       -- HeaderSalespersonId
        ORDER_NUMBER                 VARCHAR(50)  NULL,       -- HeaderOrderNumber
        SOURCE_ORDER_NUMBER          VARCHAR(50)  NULL,       -- HeaderSourceOrderNumber
        SOURCE_SYSTEM                VARCHAR(50)  NULL,       -- HeaderSourceOrderSystem
        STATUS_CODE                  VARCHAR(30)  NULL,       -- HeaderStatusCode
        OPEN_FLAG                    VARCHAR(1)   NULL,       -- HeaderOpenFlag
        ON_HOLD_FLAG                 VARCHAR(1)   NULL,       -- HeaderOnHold
        ORDERED_DATE_SK                 INT          NULL,       -- HeaderOrderedDate
        SUBMITTED_DATE_SK               INT          NULL,       -- HeaderSubmittedDate
        PAYMENT_TERM                 VARCHAR(30)  NULL,       -- HeaderPaymentTerm 
        REVISION_NUMBER              INT          NULL,       -- HeaderSourceRevisionNumber
        TRANSACTIONAL_CURRENCY_CODE  VARCHAR(15)  NULL,       -- HeaderTransactionalCurrencyCode
        APPLIED_CURRENCY_CODE        VARCHAR(15)  NULL,       -- HeaderAppliedCurrencyCode
        CONTRACT_NUMBER              VARCHAR(150) NULL,
        NATIONAL_CONTRACT_NUMBER     VARCHAR(150) NULL,
        OPPORTUNITY_NUMBER           VARCHAR(150) NULL,
        OPPORTUNITY_NAME             VARCHAR(150) NULL,
        CPQ_CONTRACT_NUMBER          VARCHAR(150) NULL,
        CPQ_TRANSACTION_NUMBER       VARCHAR(150) NULL,
        CAMPAIGN_NAME                VARCHAR(150) NULL,
        ADVERTISER                   VARCHAR(150) NULL,
        BRAND                        VARCHAR(150) NULL,
        SALES_CATEGORY               VARCHAR(150) NULL,
        SELLING_TEAM                 VARCHAR(150) NULL,
        SELLING_AE                   VARCHAR(150) NULL,
        ORIGINATING_COMPANY          VARCHAR(150) NULL,
        BZ_LOAD_DATE                 DATE         NULL,
        SV_LOAD_DATE                 DATE         NULL
    ) ON [FG_SilverDim]
END;
GO
    -- Plug row
 
IF NOT EXISTS (SELECT 1 FROM svo.D_HOLD_CODE WHERE HOLD_CODE_SK = 0)
BEGIN
  SET IDENTITY_INSERT svo.D_HOLD_CODE ON;
 INSERT INTO svo.D_OM_ORDER_HEADER (
    ORDER_HEADER_SK,
    ORDER_HEADER_ID,
    LEGAL_ENTITY_SK,
    BUSINESS_UNIT_SK,
    SOLD_TO_PARTY_SK,
    SALESPERSON_SK,
    ORDER_NUMBER,
    SOURCE_ORDER_NUMBER,
    SOURCE_SYSTEM,
    STATUS_CODE,
    OPEN_FLAG,
    ON_HOLD_FLAG,
    ORDERED_DATE_SK,
    PAYMENT_TERM,
    SUBMITTED_DATE_SK,
    REVISION_NUMBER,
    TRANSACTIONAL_CURRENCY_CODE,
    APPLIED_CURRENCY_CODE,
    CONTRACT_NUMBER,
    NATIONAL_CONTRACT_NUMBER,
    OPPORTUNITY_NUMBER,
    OPPORTUNITY_NAME,
    CPQ_CONTRACT_NUMBER,
    CPQ_TRANSACTION_NUMBER,
    CAMPAIGN_NAME,
    ADVERTISER,
    BRAND,
    SALES_CATEGORY,
    SELLING_TEAM,
    SELLING_AE,
    ORIGINATING_COMPANY,
    BZ_LOAD_DATE,
    SV_LOAD_DATE
)
VALUES (
    0,-1,0,0,0,0,-1,-1,'UNK','U','U','U',10101,'UNK',10101,-1,'UNK','UNK',-1,-1,-1,'UNK',-1,-1,'UNK','UNK','UNK','UNK',-1,
    -1,-1,CAST('0001-01-01' AS DATE),CAST(GETDATE() AS DATE)
);
  SET IDENTITY_INSERT svo.D_HOLD_CODE OFF;
END
GO
-- Lookup index on ID for ETL resolution
CREATE UNIQUE NONCLUSTERED INDEX UX_D_OM_SOH_ID
    ON svo.D_OM_ORDER_HEADER (ORDER_HEADER_ID)
    ON [FG_SilverDim];

GO

-- Initial load D_OM_ORDER_HEADER

INSERT INTO svo.D_OM_ORDER_HEADER (
    ORDER_HEADER_ID,
    LEGAL_ENTITY_SK,
    BUSINESS_UNIT_SK,
    SOLD_TO_PARTY_SK,
    SALESPERSON_SK,
    ORDER_NUMBER,
    SOURCE_ORDER_NUMBER,
    SOURCE_SYSTEM,
    STATUS_CODE,
    OPEN_FLAG,
    ON_HOLD_FLAG,
    PAYMENT_TERM,
    ORDERED_DATE_SK,
    SUBMITTED_DATE_SK,
    REVISION_NUMBER,
    TRANSACTIONAL_CURRENCY_CODE,
    APPLIED_CURRENCY_CODE,
    CONTRACT_NUMBER,
    NATIONAL_CONTRACT_NUMBER,
    OPPORTUNITY_NUMBER,
    OPPORTUNITY_NAME,
    CPQ_CONTRACT_NUMBER,
    CPQ_TRANSACTION_NUMBER,
    CAMPAIGN_NAME,
    ADVERTISER,
    BRAND,
    SALES_CATEGORY,
    SELLING_TEAM,
    SELLING_AE,
    ORIGINATING_COMPANY,
    BZ_LOAD_DATE,
    SV_LOAD_DATE
)
SELECT DISTINCT
    H.HeaderId                                        AS ORDER_HEADER_ID,
    ISNULL(LE.LEGAL_ENTITY_SK,0)                      AS LEGAL_ENTITY_SK,
    ISNULL(BU.BUSINESS_UNIT_SK,0)                     AS BUSINESS_UNIT_SK,
    ISNULL(SC.CUSTOMER_SK,0)                          AS SOLD_TO_PARTY_SK,
    ISNULL(SR.SALES_REP_SK,0)                         AS SALESPERSON_SK,
    ISNULL(H.HeaderOrderNumber,-1)                    AS ORDER_NUMBER,
    ISNULL(H.HeaderSourceOrderNumber,-1)              AS SOURCE_ORDER_NUMBER,
    ISNULL(H.HeaderSourceOrderSystem,-1)              AS SOURCE_SYSTEM,
    ISNULL(H.HeaderStatusCode,-1)                     AS STATUS_CODE,
    ISNULL(H.HeaderOpenFlag,-1)                       AS OPEN_FLAG,
    ISNULL(H.HeaderOnHold,-1)                         AS ON_HOLD_FLAG,
    ISNULL(PT.PAYMENT_TERM_NAME,'UNK')                        AS PAYMENT_TERM, -- There are no matches to this data
    ISNULL(CONVERT(INT, FORMAT(H.HeaderOrderedDate ,'yyyyMMdd')),10101) AS ORDERED_DATE_SK,
    ISNULL(CONVERT(INT, FORMAT(H.HeaderSubmittedDate,'yyyyMMdd')),10101) AS SUBMITTED_DATE_SK,
    ISNULL(H.HeaderSourceRevisionNumber,-1)           AS REVISION_NUMBER,
    ISNULL(H.HeaderTransactionalCurrencyCode,'UNK')   AS TRANSACTIONAL_CURRENCY_CODE,
    ISNULL(H.HeaderAppliedCurrencyCode,'UNK')         AS APPLIED_CURRENCY_CODE,
    ISNULL(DP.contractNumber,-1)                      AS CONTRACT_NUMBER,
    ISNULL(DP.nationalContractNumber,-1)              AS NATIONAL_CONTRACT_NUMBER,
    ISNULL(DP.opportunityNumber,-1)                   AS OPPORTUNITY_NUMBER,
    ISNULL(DP.opportunity,-1)                         AS OPPORTUNITY_NAME,
    ISNULL(HP.cpqContractNumber,-1)                   AS CPQ_CONTRACT_NUMBER,
    ISNULL(HP.cpqTransactionNumber,-1)                AS CPQ_TRANSACTION_NUMBER,
    ISNULL(HP.campaignName,-1)                        AS CAMPAIGN_NAME,
    ISNULL(HP.advertiser,-1)                          AS ADVERTISER,
    ISNULL(HP.brand,-1)                               AS BRAND,
    ISNULL(HP.salesCategory,-1)                       AS SALES_CATEGORY,
    ISNULL(HP.sellingTeam,-1)                         AS SELLING_TEAM,
    ISNULL(HP.sellingAeCodeAeName,-1)                 AS SELLING_AE,
    ISNULL(HP.originatingCompany,-1)                  AS ORIGINATING_COMPANY,
    CAST(H.AddDateTime AS DATE)                       AS BZ_LOAD_DATE,
    CAST(GETDATE() AS DATE)                           AS SV_LOAD_DATE
FROM bzo.OM_HeaderExtractPVO            H
LEFT JOIN bzo.OM_5FHeaderprivateVO      HP ON H.HeaderId = HP.HeaderId
LEFT JOIN bzo.OM_DataprivateVO          DP ON H.HeaderId = DP.HeaderId
LEFT JOIN svo.D_CUSTOMER_ACCOUNT        SC ON H.HeaderSoldToPartyId = SC.PARTY_ID
LEFT JOIN svo.D_LEGAL_ENTITY                 LE ON H.HeaderLegalEntityId = LE.LEGAL_ENTITY_ID
LEFT JOIN svo.D_BUSINESS_UNIT           BU ON H.HeaderOrgId = BU.BUSINESS_UNIT_ID
LEFT JOIN svo.D_PAYMENT_TERM            PT ON H.HeaderPaymentTermId = PT.PAYMENT_TERM_ID
LEFT JOIN svo.D_SALES_REP               SR ON H.HeaderSalespersonId = SR.SALES_REP_ID
WHERE 1 = 1 
;
 
--WHERE H.HeaderId IS NOT NULL
--  AND NOT EXISTS (
--        SELECT 1
--        FROM svo.D_OM_ORDER_HEADER d
--        WHERE d.ORDER_HEADER_ID = H.HeaderId
--    );
--GO


