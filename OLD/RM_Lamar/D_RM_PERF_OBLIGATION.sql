USE Oracle_Reporting_P2;
GO

IF OBJECT_ID('svo.D_RM_PERF_OBLIGATION','U') IS NOT NULL
    DROP TABLE svo.D_RM_PERF_OBLIGATION;
GO

CREATE TABLE svo.D_RM_PERF_OBLIGATION
(
    RM_PERF_OBLIGATION_SK        BIGINT IDENTITY(1,1) NOT NULL PRIMARY KEY,

    PERF_OBLIGATION_ID           BIGINT       NOT NULL,

    BASE_PRICE                   DECIMAL(29,4) NULL,
    BASE_PRICE_PERCENTAGE        DECIMAL(29,4) NULL,

    -- former CONTR_CUR_*
    CR_LIABILITY_AMT             DECIMAL(29,4) NULL,
    DR_ASSET_AMT                 DECIMAL(29,4) NULL,
    DR_DISCOUNT_AMT              DECIMAL(29,4) NULL,
    NET_LINE_AMT_CONTR           DECIMAL(29,4) NULL,
    TOTAL_BILLED_AMT_CONTR       DECIMAL(29,4) NULL,
    TOT_ALLOC_AMT_CONTR          DECIMAL(29,4) NULL,
    TOTAL_RECOG_REV_AMT_CONTR    DECIMAL(29,4) NULL,
    TOT_CARVE_OUT_AMT_CONTR      DECIMAL(29,4) NULL,
    TOT_NET_CONSIDER_AMT_CONTR   DECIMAL(29,4) NULL,

    COST_AMOUNT                  DECIMAL(29,4) NULL,
    CREATED_BY                   VARCHAR(4000) NULL,
    CREATION_DATE                DATE     NOT NULL,
    CUSTOMER_CONTRACT_HEADER_ID  BIGINT       NULL,
    DISCARD_FLAG                 VARCHAR(1)   NULL,
    DISCOUNT_AMOUNT              DECIMAL(29,4) NULL,
    DISCOUNT_PERCENTAGE          DECIMAL(29,4) NULL,
    EXEMPT_FROM_ALLOCATION_FLAG  VARCHAR(1)   NULL,
    FMV_LINE_ID                  BIGINT       NULL,
    GROSS_MARGIN_PERCENTAGE      DECIMAL(29,4) NULL,
    INITIAL_PERF_EVT_CREATED_FLAG  VARCHAR(1) NULL,
    INITIAL_PERF_EVT_EXPECTED_DATE DATE       NULL,
    INITIAL_PERF_EVT_RECORDED_FLAG VARCHAR(1) NULL,
    LAST_UPDATE_DATE             DATE     NOT NULL,
    LAST_UPDATED_BY              VARCHAR(4000) NULL,
    LATEST_REVISION_INTENT_CODE  VARCHAR(30)  NULL,
    LATEST_VERSION_DATE          DATE         NULL,
    LIST_PRICE_AMOUNT            DECIMAL(29,4) NULL,
    OBLIGATION_REFERENCE         VARCHAR(1000) NULL,

    -- former OBLIG_CUR_*
    FMV_AMT                      DECIMAL(29,4) NULL,
    NET_LINE_AMT_OBLIG           DECIMAL(29,4) NULL,
    NET_SALES_PRICE              DECIMAL(29,4) NULL,
    OBLIG_CURRENCY_CODE          VARCHAR(15)  NULL,
    TOTAL_BILLED_AMT_OBLIG       DECIMAL(29,4) NULL,
    TOT_ALLOC_AMT_OBLIG          DECIMAL(29,4) NULL,
    TOTAL_RECOG_REV_AMT_OBLIG    DECIMAL(29,4) NULL,
    TOT_CARVE_OUT_AMT_OBLIG      DECIMAL(29,4) NULL,
    TOT_NET_CONSIDER_AMT_OBLIG   DECIMAL(29,4) NULL,

    OBLIG_PAYMENT_AMOUNT         DECIMAL(29,4) NULL,
    PERF_OBLIGATION_NUMBER       BIGINT       NULL,
    PERF_OBLIGATION_TYPE         VARCHAR(30)  NULL,
    PERF_OBLIG_CLASSIFICATION_CODE VARCHAR(30) NULL,
    PERF_OBLIG_FREEZE_FLAG       VARCHAR(1)   NULL,
    REMOVED_FLAG                 VARCHAR(1)   NULL,
    SATISFACTION_DATE            DATE         NULL,
    SATISFACTION_METHOD          VARCHAR(30)  NULL,
    SATISFACTION_STATUS          VARCHAR(4000) NULL,
    UNIT_SELLING_PRICE           DECIMAL(29,4) NULL,

    AVERAGE_PRICE                DECIMAL(38,5) NULL,
    FAIR_MARKET_VALUE            DECIMAL(29,4) NULL,
    FMV_TOLERANCE_HIGH_PCT       DECIMAL(29,4) NULL,
    FMV_TOLERANCE_LOW_PCT        DECIMAL(29,4) NULL,
    HIGHEST_PRICE                DECIMAL(29,4) NULL,
    PRICE_LAST_UPDATE_DATE       DATE     NOT NULL,
    LINE_COUNT                   BIGINT       NULL,
    LOWEST_PRICE                 DECIMAL(29,4) NULL,
    STANDARD_DEVIATION           DECIMAL(29,4) NULL,
    TOLERANCE_COVERAGE           DECIMAL(29,4) NULL,
    TOLERANCE_COVERAGE_COUNT     BIGINT       NULL,
    TOLERANCE_RANGE_COVERAGE     DECIMAL(29,4) NULL,
    TOTAL_AMOUNT                 DECIMAL(29,4) NULL,
    TOTAL_QUANTITY               DECIMAL(29,4) NULL,

    BZ_LOAD_DATE                 DATE         NOT NULL DEFAULT (CAST(GETDATE() AS DATE)),
    SV_LOAD_DATE                 DATE         NOT NULL DEFAULT (CAST(GETDATE() AS DATE))
) ON [FG_SilverDim];
GO

SET IDENTITY_INSERT svo.D_RM_PERF_OBLIGATION ON;

INSERT INTO svo.D_RM_PERF_OBLIGATION
(
    RM_PERF_OBLIGATION_SK,
    PERF_OBLIGATION_ID,
    BASE_PRICE,
    BASE_PRICE_PERCENTAGE,
    CR_LIABILITY_AMT,
    DR_ASSET_AMT,
    DR_DISCOUNT_AMT,
    NET_LINE_AMT_CONTR,
    TOTAL_BILLED_AMT_CONTR,
    TOT_ALLOC_AMT_CONTR,
    TOTAL_RECOG_REV_AMT_CONTR,
    TOT_CARVE_OUT_AMT_CONTR,
    TOT_NET_CONSIDER_AMT_CONTR,
    COST_AMOUNT,
    CREATED_BY,
    CREATION_DATE,
    CUSTOMER_CONTRACT_HEADER_ID,
    DISCARD_FLAG,
    DISCOUNT_AMOUNT,
    DISCOUNT_PERCENTAGE,
    EXEMPT_FROM_ALLOCATION_FLAG,
    FMV_LINE_ID,
    GROSS_MARGIN_PERCENTAGE,
    INITIAL_PERF_EVT_CREATED_FLAG,
    INITIAL_PERF_EVT_EXPECTED_DATE,
    INITIAL_PERF_EVT_RECORDED_FLAG,
    LAST_UPDATE_DATE,
    LAST_UPDATED_BY,
    LATEST_REVISION_INTENT_CODE,
    LATEST_VERSION_DATE,
    LIST_PRICE_AMOUNT,
    OBLIGATION_REFERENCE,
    FMV_AMT,
    NET_LINE_AMT_OBLIG,
    NET_SALES_PRICE,
    OBLIG_CURRENCY_CODE,
    TOTAL_BILLED_AMT_OBLIG,
    TOT_ALLOC_AMT_OBLIG,
    TOTAL_RECOG_REV_AMT_OBLIG,
    TOT_CARVE_OUT_AMT_OBLIG,
    TOT_NET_CONSIDER_AMT_OBLIG,
    OBLIG_PAYMENT_AMOUNT,
    PERF_OBLIGATION_NUMBER,
    PERF_OBLIGATION_TYPE,
    PERF_OBLIG_CLASSIFICATION_CODE,
    PERF_OBLIG_FREEZE_FLAG,
    REMOVED_FLAG,
    SATISFACTION_DATE,
    SATISFACTION_METHOD,
    SATISFACTION_STATUS,
    UNIT_SELLING_PRICE,
    AVERAGE_PRICE,
    FAIR_MARKET_VALUE,
    FMV_TOLERANCE_HIGH_PCT,
    FMV_TOLERANCE_LOW_PCT,
    HIGHEST_PRICE,
    PRICE_LAST_UPDATE_DATE,
    LINE_COUNT,
    LOWEST_PRICE,
    STANDARD_DEVIATION,
    TOLERANCE_COVERAGE,
    TOLERANCE_COVERAGE_COUNT,
    TOLERANCE_RANGE_COVERAGE,
    TOTAL_AMOUNT,
    TOTAL_QUANTITY,
    BZ_LOAD_DATE,
    SV_LOAD_DATE
)
VALUES
(
    0,              -- RM_PERF_OBLIGATION_SK
    0,              -- PERF_OBLIGATION_ID
    0,              -- BASE_PRICE
    0,              -- BASE_PRICE_PERCENTAGE
    0,              -- CR_LIABILITY_AMT
    0,              -- DR_ASSET_AMT
    0,              -- DR_DISCOUNT_AMT
    0,              -- NET_LINE_AMT_CONTR
    0,              -- TOTAL_BILLED_AMT_CONTR
    0,              -- TOT_ALLOC_AMT_CONTR
    0,              -- TOTAL_RECOG_REV_AMT_CONTR
    0,              -- TOT_CARVE_OUT_AMT_CONTR
    0,              -- TOT_NET_CONSIDER_AMT_CONTR
    0,              -- COST_AMOUNT
    'UNKNOWN',      -- CREATED_BY
    '0001-01-01',   -- CREATION_DATE
    0,              -- CUSTOMER_CONTRACT_HEADER_ID
    'N',            -- DISCARD_FLAG
    0,              -- DISCOUNT_AMOUNT
    0,              -- DISCOUNT_PERCENTAGE
    'N',            -- EXEMPT_FROM_ALLOCATION_FLAG
    0,              -- FMV_LINE_ID
    0,              -- GROSS_MARGIN_PERCENTAGE
    'N',            -- INITIAL_PERF_EVT_CREATED_FLAG
    '0001-01-01',   -- INITIAL_PERF_EVT_EXPECTED_DATE
    'N',            -- INITIAL_PERF_EVT_RECORDED_FLAG
    '0001-01-01',   -- LAST_UPDATE_DATE
    'UNKNOWN',      -- LAST_UPDATED_BY
    -1,      -- LATEST_REVISION_INTENT_CODE
    '0001-01-01',   -- LATEST_VERSION_DATE
    0,              -- LIST_PRICE_AMOUNT
    -1,      -- OBLIGATION_REFERENCE
    0,              -- FMV_AMT
    0,              -- NET_LINE_AMT_OBLIG
    0,              -- NET_SALES_PRICE
    'UNK',          -- OBLIG_CURRENCY_CODE
    0,              -- TOTAL_BILLED_AMT_OBLIG
    0,              -- TOT_ALLOC_AMT_OBLIG
    0,              -- TOTAL_RECOG_REV_AMT_OBLIG
    0,              -- TOT_CARVE_OUT_AMT_OBLIG
    0,              -- TOT_NET_CONSIDER_AMT_OBLIG
    0,              -- OBLIG_PAYMENT_AMOUNT
    -1,      -- PERF_OBLIGATION_NUMBER
    'UNKNOWN',      -- PERF_OBLIGATION_TYPE
    'N',            -- PERF_OBLIG_CLASSIFICATION_CODE (placeholder)
    'N',            -- PERF_OBLIG_FREEZE_FLAG
    'N',            -- REMOVED_FLAG 
    '0001-01-1',      -- SATISFACTION_DATE
    'UNKNOWN',      -- SATISFACTION_METHOD
    0,              -- SATISFACTION_STATUS
    0,              -- UNIT_SELLING_PRICE
    0,              -- AVERAGE_PRICE
    0,              -- FAIR_MARKET_VALUE
    0,              -- FMV_TOLERANCE_HIGH_PCT
    0,              -- FMV_TOLERANCE_LOW_PCT
    0,              -- HIGHEST_PRICE
    '0001-01-01',   -- PRICE_LAST_UPDATE_DATE
    0,              -- LINE_COUNT
    0,              -- LOWEST_PRICE
    0,              -- STANDARD_DEVIATION
    0,              -- TOLERANCE_COVERAGE
    0,              -- TOLERANCE_COVERAGE_COUNT
    0,              -- TOLERANCE_RANGE_COVERAGE
    0,              -- TOTAL_AMOUNT
    0,              -- TOTAL_QUANTITY
    CAST(GETDATE() AS DATE), -- BZ_LOAD_DATE
    CAST(GETDATE() AS DATE)  -- SV_LOAD_DATE
);

SET IDENTITY_INSERT svo.D_RM_PERF_OBLIGATION OFF;


INSERT INTO svo.D_RM_PERF_OBLIGATION
(
    PERF_OBLIGATION_ID,
    BASE_PRICE,
    BASE_PRICE_PERCENTAGE,

    CR_LIABILITY_AMT,
    DR_ASSET_AMT,
    DR_DISCOUNT_AMT,
    NET_LINE_AMT_CONTR,
    TOTAL_BILLED_AMT_CONTR,
    TOT_ALLOC_AMT_CONTR,
    TOTAL_RECOG_REV_AMT_CONTR,
    TOT_CARVE_OUT_AMT_CONTR,
    TOT_NET_CONSIDER_AMT_CONTR,

    COST_AMOUNT,
    CREATED_BY,
    CREATION_DATE,
    CUSTOMER_CONTRACT_HEADER_ID,
    DISCARD_FLAG,
    DISCOUNT_AMOUNT,
    DISCOUNT_PERCENTAGE,
    EXEMPT_FROM_ALLOCATION_FLAG,
    FMV_LINE_ID,
    GROSS_MARGIN_PERCENTAGE,
    INITIAL_PERF_EVT_CREATED_FLAG,
    INITIAL_PERF_EVT_EXPECTED_DATE,
    INITIAL_PERF_EVT_RECORDED_FLAG,
    LAST_UPDATE_DATE,
    LAST_UPDATED_BY,
    LATEST_REVISION_INTENT_CODE,
    LATEST_VERSION_DATE,
    LIST_PRICE_AMOUNT,
    OBLIGATION_REFERENCE,

    FMV_AMT,
    NET_LINE_AMT_OBLIG,
    NET_SALES_PRICE,
    OBLIG_CURRENCY_CODE,
    TOTAL_BILLED_AMT_OBLIG,
    TOT_ALLOC_AMT_OBLIG,
    TOTAL_RECOG_REV_AMT_OBLIG,
    TOT_CARVE_OUT_AMT_OBLIG,
    TOT_NET_CONSIDER_AMT_OBLIG,

    OBLIG_PAYMENT_AMOUNT,
    PERF_OBLIGATION_NUMBER,
    PERF_OBLIGATION_TYPE,
    PERF_OBLIG_CLASSIFICATION_CODE,
    PERF_OBLIG_FREEZE_FLAG,
    REMOVED_FLAG,
    SATISFACTION_DATE,
    SATISFACTION_METHOD,
    SATISFACTION_STATUS,
    UNIT_SELLING_PRICE,

    AVERAGE_PRICE,
    FAIR_MARKET_VALUE,
    FMV_TOLERANCE_HIGH_PCT,
    FMV_TOLERANCE_LOW_PCT,
    HIGHEST_PRICE,
    PRICE_LAST_UPDATE_DATE,
    LINE_COUNT,
    LOWEST_PRICE,
    STANDARD_DEVIATION,
    TOLERANCE_COVERAGE,
    TOLERANCE_COVERAGE_COUNT,
    TOLERANCE_RANGE_COVERAGE,
    TOTAL_AMOUNT,
    TOTAL_QUANTITY,
    BZ_LOAD_DATE,
    SV_LOAD_DATE
)
SELECT
    PerfObligationId,
    PerfObligationsBasePrice,
    PerfObligationsBasePricePercentage,

    PerfObligationsContrCurCrLiabilityAmt,
    PerfObligationsContrCurDrAssetAmt,
    PerfObligationsContrCurDrDiscountAmt,
    PerfObligationsContrCurNetLineAmt,
    PerfObligationsContrCurTotalBilledAmt,
    PerfObligationsContrCurTotAllocAmt,
    PerfObligationsContrCurTotalRecogRevAmt,
    PerfObligationsContrCurTotCarveOutAmt,
    PerfObligationsContrCurTotNetConsiderAmt,

    PerfObligationsCostAmount,
    PerfObligationsCreatedBy,
    PerfObligationsCreationDate,
    PerfObligationsCustomerContractHeaderId,
    PerfObligationsDiscardFlag,
    PerfObligationsDiscountAmount,
    PerfObligationsDiscountPercentage,
    PerfObligationsExemptFromAllocationFlag,
    PerfObligationsFmvLineId,
    PerfObligationsGrossMarginPercentage,
    PerfObligationsInitialPerfEvtCreatedFlag,
    PerfObligationsInitialPerfEvtExpectedDate,
    PerfObligationsInitialPerfEvtRecordedFlag,
    PerfObligationsLastUpdateDate,
    PerfObligationsLastUpdatedBy,
    PerfObligationsLatestRevisionIntentCode,
    PerfObligationsLatestVersionDate,
    PerfObligationsListPriceAmount,
    PerfObligationsObligationReference,

    PerfObligationsObligCurFmvAmt,
    PerfObligationsObligCurNetLineAmt,
    PerfObligationsObligCurNetSalesPrice,
    PerfObligationsObligCurrencyCode,
    PerfObligationsObligCurTotalBilledAmt,
    PerfObligationsObligCurTotAllocAmt,
    PerfObligationsObligCurTotalRecogRevAmt,
    PerfObligationsObligCurTotCarveOutAmt,
    PerfObligationsObligCurTotNetConsiderAmt,

    PerfObligationsObligPaymentAmount,
    PerfObligationsPerfObligationNumber,
    PerfObligationsPerfObligationType,
    PerfObligationsPerfObligClassificationCode,
    PerfObligationsPerfObligFreezeFlag,
    PerfObligationsRemovedFlag,
    PerfObligationsSatisfactionDate,
    PerfObligationsSatisfactionMethod,
    PerfObligationsSatisfactionStatus,
    PerfObligationsUnitSellingPrice,

    PriceLinePEOAveragePrice,
    PriceLinePEOFairMarketValue,
    PriceLinePEOFmvToleranceHighPct,
    PriceLinePEOFmvToleranceLowPct,
    PriceLinePEOHighestPrice,
    PriceLinePEOLastUpdateDate,
    PriceLinePEOLineCount,
    PriceLinePEOLowestPrice,
    PriceLinePEOStandardDeviation,
    PriceLinePEOToleranceCoverage,
    PriceLinePEOToleranceCoverageCount,
    PriceLinePEOToleranceRangeCoverage,
    PriceLinePEOTotalAmount,
    PriceLinePEOTotalQuantity,
    CAST(ISNULL(AddDateTime, GETDATE()) AS DATE),
    CAST(GETDATE() AS DATE)
FROM bzo.VRM_PerfObligationsPVO;
GO