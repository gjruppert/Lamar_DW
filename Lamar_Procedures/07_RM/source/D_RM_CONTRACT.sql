IF OBJECT_ID('svo.D_RM_CONTRACT', 'U') IS NOT NULL
    DROP TABLE svo.D_RM_CONTRACT;
GO

SET ANSI_NULLS ON;
GO
SET QUOTED_IDENTIFIER ON;
GO

CREATE TABLE [svo].[D_RM_CONTRACT]
(
    RM_CONTRACT_SK                   BIGINT IDENTITY(1,1) NOT NULL,

    CUSTOMER_CONTRACT_HEADER_ID      BIGINT         NOT NULL,
    CONTRACT_CURRENCY_CODE           VARCHAR(15)    NOT NULL,
    CUSTOMER_CONTRACT_NUMBER         BIGINT         NOT NULL,
    EFFECTIVITY_PERIOD_ID            BIGINT         NOT NULL,
    LEDGER_ID                        BIGINT         NOT NULL,

    ALLOCATION_PENDING_REASON        VARCHAR(30)    NULL,
    ALLOCATION_REQUEST_ID            BIGINT         NULL,
    ALLOCATION_STATUS                VARCHAR(30)    NULL,
    ATTRIBUTE1                       VARCHAR(150)   NULL,
    ATTRIBUTE_CATEGORY               VARCHAR(30)    NULL,
    CONTRACT_CLASSIFICATION_CODE     VARCHAR(30)    NULL,
    CONTRACT_RULE_ID                 BIGINT         NULL,
    CONTR_TOTAL_BILLED_AMT           DECIMAL(29,4)  NULL,
    CONTR_TOTAL_RECOG_REV_AMT        DECIMAL(29,4)  NULL,
    CONTR_TRANSACTION_PRICE          DECIMAL(29,4)  NULL,

    CREATED_BY                       VARCHAR(64)    NULL,
    CREATED_FROM                     VARCHAR(30)    NOT NULL,
    CREATION_DATE                    DATE           NOT NULL,
    CUSTOMER_CONTRACT_DATE           DATE           NOT NULL,
    CUSTOMER_CONTRACT_FREEZE_DATE    DATE           NULL,

    EXCHANGE_RATE_DATE               DATE           NULL,
    EXCHANGE_RATE_TYPE               VARCHAR(30)    NULL,

    LAST_UPDATE_DATE                 DATE           NOT NULL,
    LAST_UPDATED_BY                  VARCHAR(64)    NULL,
    LAST_UPDATE_LOGIN                VARCHAR(32)    NULL,

    LEGAL_ENTITY_ID                  BIGINT         NULL,
    OBJECT_VERSION_NUMBER            BIGINT         NOT NULL,

    CONTRACT_REFERENCE               VARCHAR(1000)  NULL,
    REVIEW_STATUS                    VARCHAR(30)    NULL,
    SINGLE_OBLIGATION_FLAG           VARCHAR(1)     NULL,
    STANDALONE_SALES_FLAG            VARCHAR(1)     NULL,

    ADJUSTMENT_STATUS_CODE           VARCHAR(30)    NULL,
    CONTRACT_GROUP_NUMBER            VARCHAR(320)   NULL,
    CONTRACT_CREATED_BY              VARCHAR(64)    NOT NULL,
    EXCL_FROM_AUTO_WRITEOFF_FLAG     VARCHAR(1)     NULL,
    FULL_SATISFACTION_DATE           DATE           NULL,
    LAST_ACTIVITY_DATE               DATE           NULL,
    CONTRACT_LAST_UPDATED_BY         VARCHAR(64)    NOT NULL,
    LATEST_IMMATERIAL_CHANGE_CODE    VARCHAR(30)    NULL,
    LATEST_REVISION_INTENT_CODE      VARCHAR(30)    NULL,
    LATEST_VERSION_DATE              DATE           NULL,
    SATISFACTION_STATUS              VARCHAR(30)    NULL,

    BZ_LOAD_DATE                     DATE           NOT NULL,
    SV_LOAD_DATE                     DATE           NOT NULL,

    CONSTRAINT PK_D_RM_CONTRACT
        PRIMARY KEY CLUSTERED (RM_CONTRACT_SK ASC)
) ON [FG_SilverDim];
GO

CREATE UNIQUE NONCLUSTERED INDEX UX_D_RM_CONTRACT_HEADER_ID
ON [svo].[D_RM_CONTRACT] (CUSTOMER_CONTRACT_HEADER_ID)
ON [FG_SilverDim];
GO

-- Plug row
SET IDENTITY_INSERT svo.D_RM_CONTRACT ON;

INSERT INTO svo.D_RM_CONTRACT
(
    RM_CONTRACT_SK,
    CUSTOMER_CONTRACT_HEADER_ID,
    CONTRACT_CURRENCY_CODE,
    CUSTOMER_CONTRACT_NUMBER,
    EFFECTIVITY_PERIOD_ID,
    LEDGER_ID,
    ALLOCATION_PENDING_REASON,
    ALLOCATION_REQUEST_ID,
    ALLOCATION_STATUS,
    ATTRIBUTE1,
    ATTRIBUTE_CATEGORY,
    CONTRACT_CLASSIFICATION_CODE,
    CONTRACT_RULE_ID,
    CONTR_TOTAL_BILLED_AMT,
    CONTR_TOTAL_RECOG_REV_AMT,
    CONTR_TRANSACTION_PRICE,
    CREATED_BY,
    CREATED_FROM,
    CREATION_DATE,
    CUSTOMER_CONTRACT_DATE,
    CUSTOMER_CONTRACT_FREEZE_DATE,
    EXCHANGE_RATE_DATE,
    EXCHANGE_RATE_TYPE,
    LAST_UPDATE_DATE,
    LAST_UPDATED_BY,
    LAST_UPDATE_LOGIN,
    LEGAL_ENTITY_ID,
    OBJECT_VERSION_NUMBER,
    CONTRACT_REFERENCE,
    REVIEW_STATUS,
    SINGLE_OBLIGATION_FLAG,
    STANDALONE_SALES_FLAG,
    ADJUSTMENT_STATUS_CODE,
    CONTRACT_GROUP_NUMBER,
    CONTRACT_CREATED_BY,
    EXCL_FROM_AUTO_WRITEOFF_FLAG,
    FULL_SATISFACTION_DATE,
    LAST_ACTIVITY_DATE,
    CONTRACT_LAST_UPDATED_BY,
    LATEST_IMMATERIAL_CHANGE_CODE,
    LATEST_REVISION_INTENT_CODE,
    LATEST_VERSION_DATE,
    SATISFACTION_STATUS,
    BZ_LOAD_DATE,
    SV_LOAD_DATE
)
VALUES
(
    0,              -- RM_CONTRACT_SK
    0,              -- CUSTOMER_CONTRACT_HEADER_ID
    'UNK',          -- CONTRACT_CURRENCY_CODE
    0,              -- CUSTOMER_CONTRACT_NUMBER
    0,              -- EFFECTIVITY_PERIOD_ID
    0,              -- LEDGER_ID
    'UNKNOWN',      -- ALLOCATION_PENDING_REASON
    0,              -- ALLOCATION_REQUEST_ID
    'UNKNOWN',      -- ALLOCATION_STATUS
    'UNKNOWN',      -- ATTRIBUTE1
    'UNKNOWN',      -- ATTRIBUTE_CATEGORY
    'UNKNOWN',      -- CONTRACT_CLASSIFICATION_CODE
    0,              -- CONTRACT_RULE_ID
    0,              -- CONTR_TOTAL_BILLED_AMT
    0,              -- CONTR_TOTAL_RECOG_REV_AMT
    0,              -- CONTR_TRANSACTION_PRICE
    'UNKNOWN',      -- CREATED_BY
    'UNKNOWN',      -- CREATED_FROM
    '1900-01-01',   -- CREATION_DATE
    '1900-01-01',   -- CUSTOMER_CONTRACT_DATE
    '1900-01-01',   -- CUSTOMER_CONTRACT_FREEZE_DATE
    '1900-01-01',   -- EXCHANGE_RATE_DATE
    'UNKNOWN',      -- EXCHANGE_RATE_TYPE
    '1900-01-01',   -- LAST_UPDATE_DATE
    'UNKNOWN',      -- LAST_UPDATED_BY
    'UNKNOWN',      -- LAST_UPDATE_LOGIN
    0,              -- LEGAL_ENTITY_ID
    0,              -- OBJECT_VERSION_NUMBER
    'UNKNOWN',      -- CONTRACT_REFERENCE
    'UNKNOWN',      -- REVIEW_STATUS
    'N',            -- SINGLE_OBLIGATION_FLAG
    'N',            -- STANDALONE_SALES_FLAG
    'UNKNOWN',      -- ADJUSTMENT_STATUS_CODE
    'UNKNOWN',      -- CONTRACT_GROUP_NUMBER
    'UNKNOWN',      -- CONTRACT_CREATED_BY
    'N',            -- EXCL_FROM_AUTO_WRITEOFF_FLAG
    '1900-01-01',   -- FULL_SATISFACTION_DATE
    '1900-01-01',   -- LAST_ACTIVITY_DATE
    'UNKNOWN',      -- CONTRACT_LAST_UPDATED_BY
    'UNKNOWN',      -- LATEST_IMMATERIAL_CHANGE_CODE
    'UNKNOWN',      -- LATEST_REVISION_INTENT_CODE
    '1900-01-01',   -- LATEST_VERSION_DATE
    'UNKNOWN',      -- SATISFACTION_STATUS
    CAST(GETDATE() AS DATE), -- BZ_LOAD_DATE
    CAST(GETDATE() AS DATE)  -- SV_LOAD_DATE
);

SET IDENTITY_INSERT svo.D_RM_CONTRACT OFF;
GO

INSERT INTO svo.D_RM_CONTRACT
(
    CUSTOMER_CONTRACT_HEADER_ID,
    CONTRACT_CURRENCY_CODE,
    CUSTOMER_CONTRACT_NUMBER,
    EFFECTIVITY_PERIOD_ID,
    LEDGER_ID,
    ALLOCATION_PENDING_REASON,
    ALLOCATION_REQUEST_ID,
    ALLOCATION_STATUS,
    ATTRIBUTE1,
    ATTRIBUTE_CATEGORY,
    CONTRACT_CLASSIFICATION_CODE,
    CONTRACT_RULE_ID,
    CONTR_TOTAL_BILLED_AMT,
    CONTR_TOTAL_RECOG_REV_AMT,
    CONTR_TRANSACTION_PRICE,
    CREATED_BY,
    CREATED_FROM,
    CREATION_DATE,
    CUSTOMER_CONTRACT_DATE,
    CUSTOMER_CONTRACT_FREEZE_DATE,
    EXCHANGE_RATE_DATE,
    EXCHANGE_RATE_TYPE,
    LAST_UPDATE_DATE,
    LAST_UPDATED_BY,
    LAST_UPDATE_LOGIN,
    LEGAL_ENTITY_ID,
    OBJECT_VERSION_NUMBER,
    CONTRACT_REFERENCE,
    REVIEW_STATUS,
    SINGLE_OBLIGATION_FLAG,
    STANDALONE_SALES_FLAG,
    ADJUSTMENT_STATUS_CODE,
    CONTRACT_GROUP_NUMBER,
    CONTRACT_CREATED_BY,
    EXCL_FROM_AUTO_WRITEOFF_FLAG,
    FULL_SATISFACTION_DATE,
    LAST_ACTIVITY_DATE,
    CONTRACT_LAST_UPDATED_BY,
    LATEST_IMMATERIAL_CHANGE_CODE,
    LATEST_REVISION_INTENT_CODE,
    LATEST_VERSION_DATE,
    SATISFACTION_STATUS,
    BZ_LOAD_DATE,
    SV_LOAD_DATE
)
SELECT
    C.CustomerContractHeaderId                              AS CUSTOMER_CONTRACT_HEADER_ID,
    C.CustContHeadersContractCurrencyCode                  AS CONTRACT_CURRENCY_CODE,
    C.CustContHeadersCustomerContractNumber                AS CUSTOMER_CONTRACT_NUMBER,
    C.CustContHeadersEffectivityPeriodId                   AS EFFECTIVITY_PERIOD_ID,
    C.CustContHeadersLedgerId                              AS LEDGER_ID,

    C.CustContHeadersAllocationPendingReason               AS ALLOCATION_PENDING_REASON,
    C.CustContHeadersAllocationRequestId                   AS ALLOCATION_REQUEST_ID,
    C.CustContHeadersAllocationStatus                      AS ALLOCATION_STATUS,
    C.CustContHeadersAttribute1                            AS ATTRIBUTE1,
    C.CustContHeadersAttributeCategory                     AS ATTRIBUTE_CATEGORY,
    C.CustContHeadersContractClassificationCode            AS CONTRACT_CLASSIFICATION_CODE,
    C.CustContHeadersContractRuleId                        AS CONTRACT_RULE_ID,
    C.CustContHeadersContrCurTotalBilledAmt                AS CONTR_TOTAL_BILLED_AMT,
    C.CustContHeadersContrCurTotalRecogRevAmt              AS CONTR_TOTAL_RECOG_REV_AMT,
    C.CustContHeadersContrCurTransactionPrice              AS CONTR_TRANSACTION_PRICE,

    C.CustContHeadersCreatedBy                             AS CREATED_BY,
    C.CustContHeadersCreatedFrom                           AS CREATED_FROM,
    CAST(C.CustContHeadersCreationDate AS DATE)            AS CREATION_DATE,
    C.CustContHeadersCustomerContractDate                  AS CUSTOMER_CONTRACT_DATE,
    C.CustContHeadersCustomerContractFreezeDate            AS CUSTOMER_CONTRACT_FREEZE_DATE,

    C.CustContHeadersExchangeRateDate                      AS EXCHANGE_RATE_DATE,
    C.CustContHeadersExchangeRateType                      AS EXCHANGE_RATE_TYPE,

    CAST(C.CustContHeadersLastUpdateDate AS DATE)          AS LAST_UPDATE_DATE,
    C.CustContHeadersLastUpdatedBy                         AS LAST_UPDATED_BY,
    C.CustContHeadersLastUpdateLogin                       AS LAST_UPDATE_LOGIN,

    C.CustContHeadersLegalEntityId                         AS LEGAL_ENTITY_ID,
    C.CustContHeadersObjectVersionNumber                   AS OBJECT_VERSION_NUMBER,

    C.CustContHeadersReference                             AS CONTRACT_REFERENCE,
    C.CustContHeadersReviewStatus                          AS REVIEW_STATUS,
    C.CustContHeadersSingleObligationFlag                  AS SINGLE_OBLIGATION_FLAG,
    C.CustContHeadersStandaloneSalesFlag                   AS STANDALONE_SALES_FLAG,

    C.CustomerContractHeadersAdjustmentStatusCode          AS ADJUSTMENT_STATUS_CODE,
    C.CustomerContractHeadersContractGroupNumber           AS CONTRACT_GROUP_NUMBER,
    C.CustomerContractHeadersCreatedBy                     AS CONTRACT_CREATED_BY,
    C.CustomerContractHeadersExclFromAutoWriteoffFlag      AS EXCL_FROM_AUTO_WRITEOFF_FLAG,
    C.CustomerContractHeadersFullSatisfactionDate          AS FULL_SATISFACTION_DATE,
    C.CustomerContractHeadersLastActivityDate              AS LAST_ACTIVITY_DATE,
    C.CustomerContractHeadersLastUpdatedBy                 AS CONTRACT_LAST_UPDATED_BY,
    C.CustomerContractHeadersLatestImmaterialChangeCode    AS LATEST_IMMATERIAL_CHANGE_CODE,
    C.CustomerContractHeadersLatestRevisionIntentCode      AS LATEST_REVISION_INTENT_CODE,
    C.CustomerContractHeadersLatestVersionDate             AS LATEST_VERSION_DATE,
    C.CustomerContractHeadersSatisfactionStatus            AS SATISFACTION_STATUS,

    CAST(C.AddDateTime AS DATE)                            AS BZ_LOAD_DATE,
    CAST(GETDATE() AS DATE)                                AS SV_LOAD_DATE
FROM bzo.VRM_CustomerContractHeadersPVO AS C;
GO