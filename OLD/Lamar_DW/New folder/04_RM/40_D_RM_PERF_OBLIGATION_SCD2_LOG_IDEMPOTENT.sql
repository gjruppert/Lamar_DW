/* =====================================================================================
   D_RM_PERF_OBLIGATION (DDL) + SCD2 Loader + ETL_RUN + Idempotent
   Source: src.bzo_VRM_PerfObligationsPVO (synonym)
   ===================================================================================== */

IF OBJECT_ID('svo.D_RM_PERF_OBLIGATION','U') IS NOT NULL
    DROP TABLE svo.D_RM_PERF_OBLIGATION;
GO

CREATE TABLE svo.D_RM_PERF_OBLIGATION
(
    RM_PERF_OBLIGATION_SK           BIGINT IDENTITY(1,1) NOT NULL,

    -- Business key
    PERF_OBLIGATION_ID              BIGINT       NOT NULL,

    -- Attributes
    BASE_PRICE                      DECIMAL(29,4) NULL,
    BASE_PRICE_PERCENTAGE           DECIMAL(29,4) NULL,

    CR_LIABILITY_AMT                DECIMAL(29,4) NULL,
    DR_ASSET_AMT                    DECIMAL(29,4) NULL,
    DR_DISCOUNT_AMT                 DECIMAL(29,4) NULL,
    NET_LINE_AMT_CONTR              DECIMAL(29,4) NULL,
    TOTAL_BILLED_AMT_CONTR          DECIMAL(29,4) NULL,
    TOT_ALLOC_AMT_CONTR             DECIMAL(29,4) NULL,
    TOTAL_RECOG_REV_AMT_CONTR       DECIMAL(29,4) NULL,
    TOT_CARVE_OUT_AMT_CONTR         DECIMAL(29,4) NULL,
    TOT_NET_CONSIDER_AMT_CONTR      DECIMAL(29,4) NULL,

    COST_AMOUNT                     DECIMAL(29,4) NULL,
    CREATED_BY                      VARCHAR(4000) NULL,
    CREATION_DATE                   DATE         NOT NULL,
    CUSTOMER_CONTRACT_HEADER_ID     BIGINT       NULL,
    DISCARD_FLAG                    VARCHAR(1)   NULL,
    DISCOUNT_AMOUNT                 DECIMAL(29,4) NULL,
    DISCOUNT_PERCENTAGE             DECIMAL(29,4) NULL,
    EXEMPT_FROM_ALLOCATION_FLAG     VARCHAR(1)   NULL,
    FMV_LINE_ID                     BIGINT       NULL,
    GROSS_MARGIN_PERCENTAGE         DECIMAL(29,4) NULL,
    INITIAL_PERF_EVT_CREATED_FLAG   VARCHAR(1)   NULL,
    INITIAL_PERF_EVT_EXPECTED_DATE  DATE         NULL,
    INITIAL_PERF_EVT_RECORDED_FLAG  VARCHAR(1)   NULL,
    LAST_UPDATE_DATE                DATE         NOT NULL,
    LAST_UPDATED_BY                 VARCHAR(4000) NULL,
    LATEST_REVISION_INTENT_CODE     VARCHAR(30)  NULL,
    LATEST_VERSION_DATE             DATE         NULL,
    LIST_PRICE_AMOUNT               DECIMAL(29,4) NULL,
    OBLIGATION_REFERENCE            VARCHAR(1000) NULL,

    FMV_AMT                         DECIMAL(29,4) NULL,
    NET_LINE_AMT_OBLIG              DECIMAL(29,4) NULL,
    NET_SALES_PRICE                 DECIMAL(29,4) NULL,
    OBLIG_CURRENCY_CODE             VARCHAR(15)  NULL,
    TOTAL_BILLED_AMT_OBLIG          DECIMAL(29,4) NULL,
    TOT_ALLOC_AMT_OBLIG             DECIMAL(29,4) NULL,
    TOTAL_RECOG_REV_AMT_OBLIG       DECIMAL(29,4) NULL,
    TOT_CARVE_OUT_AMT_OBLIG         DECIMAL(29,4) NULL,
    TOT_NET_CONSIDER_AMT_OBLIG      DECIMAL(29,4) NULL,

    OBLIG_PAYMENT_AMOUNT            DECIMAL(29,4) NULL,
    PERF_OBLIGATION_NUMBER          BIGINT       NULL,
    PERF_OBLIGATION_TYPE            VARCHAR(30)  NULL,
    PERF_OBLIG_CLASSIFICATION_CODE  VARCHAR(30)  NULL,
    PERF_OBLIG_FREEZE_FLAG          VARCHAR(1)   NULL,
    REMOVED_FLAG                    VARCHAR(1)   NULL,
    SATISFACTION_DATE               DATE         NULL,
    SATISFACTION_METHOD             VARCHAR(30)  NULL,
    SATISFACTION_STATUS             VARCHAR(4000) NULL,
    UNIT_SELLING_PRICE              DECIMAL(29,4) NULL,

    AVERAGE_PRICE                   DECIMAL(38,5) NULL,
    FAIR_MARKET_VALUE               DECIMAL(29,4) NULL,
    FMV_TOLERANCE_HIGH_PCT          DECIMAL(29,4) NULL,
    FMV_TOLERANCE_LOW_PCT           DECIMAL(29,4) NULL,
    HIGHEST_PRICE                   DECIMAL(29,4) NULL,
    PRICE_LAST_UPDATE_DATE          DATE         NOT NULL,
    LINE_COUNT                      BIGINT       NULL,
    LOWEST_PRICE                    DECIMAL(29,4) NULL,
    STANDARD_DEVIATION              DECIMAL(29,4) NULL,
    TOLERANCE_COVERAGE              DECIMAL(29,4) NULL,
    TOLERANCE_COVERAGE_COUNT        BIGINT       NULL,
    TOLERANCE_RANGE_COVERAGE        DECIMAL(29,4) NULL,
    TOTAL_AMOUNT                    DECIMAL(29,4) NULL,
    TOTAL_QUANTITY                  DECIMAL(29,4) NULL,

    -- SCD2 fields (Option A style)
    EFF_DATE                        DATE         NOT NULL,
    END_DATE                        DATE         NOT NULL,
    CRE_DATE                        DATETIME2(0) NOT NULL,
    UDT_DATE                        DATETIME2(0) NOT NULL,
    CURR_IND                        CHAR(1)      NOT NULL,

    ROW_HASH                        VARBINARY(32) NOT NULL,

    -- Load dates
    BZ_LOAD_DATE                    DATE         NOT NULL,
    SV_LOAD_DATE                    DATE         NOT NULL,

    CONSTRAINT PK_D_RM_PERF_OBLIGATION PRIMARY KEY CLUSTERED (RM_PERF_OBLIGATION_SK)
) ON [FG_SilverDim];
GO

-- One current row per business key
CREATE UNIQUE NONCLUSTERED INDEX UX_D_RM_PERF_OBLIGATION_BK_CURR
ON svo.D_RM_PERF_OBLIGATION (PERF_OBLIGATION_ID, CURR_IND)
ON [FG_SilverDim];
GO

CREATE OR ALTER PROCEDURE svo.usp_Load_D_RM_PERF_OBLIGATION_SCD2
      @FullReload bit = 0
    , @Debug      bit = 0
    , @AsOfDate   date = NULL
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    DECLARE @proc sysname = QUOTENAME(OBJECT_SCHEMA_NAME(@@PROCID)) + '.' + QUOTENAME(OBJECT_NAME(@@PROCID));
    DECLARE @run_id bigint;
    DECLARE @start_dttm datetime2(0) = SYSDATETIME();
    DECLARE @asof date = COALESCE(@AsOfDate, CAST(GETDATE() AS date));

    DECLARE @rows_inserted int = 0;
    DECLARE @rows_expired  int = 0;

    BEGIN TRY
        INSERT INTO etl.ETL_RUN (PROC_NAME, TARGET_OBJECT, ASOF_DATE, START_DTTM, STATUS)
        VALUES (@proc, 'svo.D_RM_PERF_OBLIGATION', @asof, @start_dttm, 'STARTED');

        SET @run_id = SCOPE_IDENTITY();

        IF @Debug = 1
            PRINT 'Starting ' + @proc + ' | RUN_ID=' + CONVERT(varchar(30), @run_id);

        BEGIN TRAN;

        IF @FullReload = 1
        BEGIN
            IF @Debug = 1
                PRINT 'FullReload requested. Rebuilding svo.D_RM_PERF_OBLIGATION';

            TRUNCATE TABLE svo.D_RM_PERF_OBLIGATION;
        END

        /* Build a single latest row per PERF_OBLIGATION_ID */
        IF OBJECT_ID('tempdb..#Src') IS NOT NULL DROP TABLE #Src;

        ;WITH Base AS
        (
            SELECT
                PERF_OBLIGATION_ID = COALESCE(TRY_CONVERT(bigint, PerfObligationId), 0),

                BASE_PRICE = COALESCE(TRY_CONVERT(decimal(29,4), PerfObligationsBasePrice), 0),
                BASE_PRICE_PERCENTAGE = COALESCE(TRY_CONVERT(decimal(29,4), PerfObligationsBasePricePercentage), 0),

                CR_LIABILITY_AMT = COALESCE(TRY_CONVERT(decimal(29,4), PerfObligationsContrCurCrLiabilityAmt), 0),
                DR_ASSET_AMT = COALESCE(TRY_CONVERT(decimal(29,4), PerfObligationsContrCurDrAssetAmt), 0),
                DR_DISCOUNT_AMT = COALESCE(TRY_CONVERT(decimal(29,4), PerfObligationsContrCurDrDiscountAmt), 0),
                NET_LINE_AMT_CONTR = COALESCE(TRY_CONVERT(decimal(29,4), PerfObligationsContrCurNetLineAmt), 0),
                TOTAL_BILLED_AMT_CONTR = COALESCE(TRY_CONVERT(decimal(29,4), PerfObligationsContrCurTotalBilledAmt), 0),
                TOT_ALLOC_AMT_CONTR = COALESCE(TRY_CONVERT(decimal(29,4), PerfObligationsContrCurTotAllocAmt), 0),
                TOTAL_RECOG_REV_AMT_CONTR = COALESCE(TRY_CONVERT(decimal(29,4), PerfObligationsContrCurTotalRecogRevAmt), 0),
                TOT_CARVE_OUT_AMT_CONTR = COALESCE(TRY_CONVERT(decimal(29,4), PerfObligationsContrCurTotCarveOutAmt), 0),
                TOT_NET_CONSIDER_AMT_CONTR = COALESCE(TRY_CONVERT(decimal(29,4), PerfObligationsContrCurTotNetConsiderAmt), 0),

                COST_AMOUNT = COALESCE(TRY_CONVERT(decimal(29,4), PerfObligationsCostAmount), 0),
                CREATED_BY = COALESCE(NULLIF(TRY_CONVERT(varchar(4000), PerfObligationsCreatedBy), ''), 'Unknown'),
                CREATION_DATE = COALESCE(TRY_CONVERT(date, PerfObligationsCreationDate), CAST('0001-01-01' AS date)),
                CUSTOMER_CONTRACT_HEADER_ID = COALESCE(TRY_CONVERT(bigint, PerfObligationsCustomerContractHeaderId), 0),
                DISCARD_FLAG = COALESCE(NULLIF(TRY_CONVERT(varchar(1), PerfObligationsDiscardFlag), ''), 'U'),
                DISCOUNT_AMOUNT = COALESCE(TRY_CONVERT(decimal(29,4), PerfObligationsDiscountAmount), 0),
                DISCOUNT_PERCENTAGE = COALESCE(TRY_CONVERT(decimal(29,4), PerfObligationsDiscountPercentage), 0),
                EXEMPT_FROM_ALLOCATION_FLAG = COALESCE(NULLIF(TRY_CONVERT(varchar(1), PerfObligationsExemptFromAllocationFlag), ''), 'U'),
                FMV_LINE_ID = COALESCE(TRY_CONVERT(bigint, PerfObligationsFmvLineId), 0),
                GROSS_MARGIN_PERCENTAGE = COALESCE(TRY_CONVERT(decimal(29,4), PerfObligationsGrossMarginPercentage), 0),
                INITIAL_PERF_EVT_CREATED_FLAG = COALESCE(NULLIF(TRY_CONVERT(varchar(1), PerfObligationsInitialPerfEvtCreatedFlag), ''), 'U'),
                INITIAL_PERF_EVT_EXPECTED_DATE = COALESCE(TRY_CONVERT(date, PerfObligationsInitialPerfEvtExpectedDate), CAST('0001-01-01' AS date)),
                INITIAL_PERF_EVT_RECORDED_FLAG = COALESCE(NULLIF(TRY_CONVERT(varchar(1), PerfObligationsInitialPerfEvtRecordedFlag), ''), 'U'),
                LAST_UPDATE_DATE = COALESCE(TRY_CONVERT(date, PerfObligationsLastUpdateDate), CAST('0001-01-01' AS date)),
                LAST_UPDATED_BY = COALESCE(NULLIF(TRY_CONVERT(varchar(4000), PerfObligationsLastUpdatedBy), ''), 'Unknown'),
                LATEST_REVISION_INTENT_CODE = COALESCE(NULLIF(TRY_CONVERT(varchar(30), PerfObligationsLatestRevisionIntentCode), ''), 'Unknown'),
                LATEST_VERSION_DATE = COALESCE(TRY_CONVERT(date, PerfObligationsLatestVersionDate), CAST('0001-01-01' AS date)),
                LIST_PRICE_AMOUNT = COALESCE(TRY_CONVERT(decimal(29,4), PerfObligationsListPriceAmount), 0),
                OBLIGATION_REFERENCE = COALESCE(NULLIF(TRY_CONVERT(varchar(1000), PerfObligationsObligationReference), ''), 'Unknown'),

                FMV_AMT = COALESCE(TRY_CONVERT(decimal(29,4), PerfObligationsObligCurFmvAmt), 0),
                NET_LINE_AMT_OBLIG = COALESCE(TRY_CONVERT(decimal(29,4), PerfObligationsObligCurNetLineAmt), 0),
                NET_SALES_PRICE = COALESCE(TRY_CONVERT(decimal(29,4), PerfObligationsObligCurNetSalesPrice), 0),
                OBLIG_CURRENCY_CODE = COALESCE(NULLIF(TRY_CONVERT(varchar(15), PerfObligationsObligCurrencyCode), ''), 'Unk'),
                TOTAL_BILLED_AMT_OBLIG = COALESCE(TRY_CONVERT(decimal(29,4), PerfObligationsObligCurTotalBilledAmt), 0),
                TOT_ALLOC_AMT_OBLIG = COALESCE(TRY_CONVERT(decimal(29,4), PerfObligationsObligCurTotAllocAmt), 0),
                TOTAL_RECOG_REV_AMT_OBLIG = COALESCE(TRY_CONVERT(decimal(29,4), PerfObligationsObligCurTotalRecogRevAmt), 0),
                TOT_CARVE_OUT_AMT_OBLIG = COALESCE(TRY_CONVERT(decimal(29,4), PerfObligationsObligCurTotCarveOutAmt), 0),
                TOT_NET_CONSIDER_AMT_OBLIG = COALESCE(TRY_CONVERT(decimal(29,4), PerfObligationsObligCurTotNetConsiderAmt), 0),

                OBLIG_PAYMENT_AMOUNT = COALESCE(TRY_CONVERT(decimal(29,4), PerfObligationsObligPaymentAmount), 0),
                PERF_OBLIGATION_NUMBER = COALESCE(TRY_CONVERT(bigint, PerfObligationsPerfObligationNumber), 0),
                PERF_OBLIGATION_TYPE = COALESCE(NULLIF(TRY_CONVERT(varchar(30), PerfObligationsPerfObligationType), ''), 'Unknown'),
                PERF_OBLIG_CLASSIFICATION_CODE = COALESCE(NULLIF(TRY_CONVERT(varchar(30), PerfObligationsPerfObligClassificationCode), ''), 'Unknown'),
                PERF_OBLIG_FREEZE_FLAG = COALESCE(NULLIF(TRY_CONVERT(varchar(1), PerfObligationsPerfObligFreezeFlag), ''), 'U'),
                REMOVED_FLAG = COALESCE(NULLIF(TRY_CONVERT(varchar(1), PerfObligationsRemovedFlag), ''), 'U'),
                SATISFACTION_DATE = COALESCE(TRY_CONVERT(date, PerfObligationsSatisfactionDate), CAST('0001-01-01' AS date)),
                SATISFACTION_METHOD = COALESCE(NULLIF(TRY_CONVERT(varchar(30), PerfObligationsSatisfactionMethod), ''), 'Unknown'),
                SATISFACTION_STATUS = COALESCE(NULLIF(TRY_CONVERT(varchar(4000), PerfObligationsSatisfactionStatus), ''), 'Unknown'),
                UNIT_SELLING_PRICE = COALESCE(TRY_CONVERT(decimal(29,4), PerfObligationsUnitSellingPrice), 0),

                AVERAGE_PRICE = COALESCE(TRY_CONVERT(decimal(38,5), PriceLinePEOAveragePrice), 0),
                FAIR_MARKET_VALUE = COALESCE(TRY_CONVERT(decimal(29,4), PriceLinePEOFairMarketValue), 0),
                FMV_TOLERANCE_HIGH_PCT = COALESCE(TRY_CONVERT(decimal(29,4), PriceLinePEOFmvToleranceHighPct), 0),
                FMV_TOLERANCE_LOW_PCT = COALESCE(TRY_CONVERT(decimal(29,4), PriceLinePEOFmvToleranceLowPct), 0),
                HIGHEST_PRICE = COALESCE(TRY_CONVERT(decimal(29,4), PriceLinePEOHighestPrice), 0),
                PRICE_LAST_UPDATE_DATE = COALESCE(TRY_CONVERT(date, PriceLinePEOLastUpdateDate), CAST('0001-01-01' AS date)),
                LINE_COUNT = COALESCE(TRY_CONVERT(bigint, PriceLinePEOLineCount), 0),
                LOWEST_PRICE = COALESCE(TRY_CONVERT(decimal(29,4), PriceLinePEOLowestPrice), 0),
                STANDARD_DEVIATION = COALESCE(TRY_CONVERT(decimal(29,4), PriceLinePEOStandardDeviation), 0),
                TOLERANCE_COVERAGE = COALESCE(TRY_CONVERT(decimal(29,4), PriceLinePEOToleranceCoverage), 0),
                TOLERANCE_COVERAGE_COUNT = COALESCE(TRY_CONVERT(bigint, PriceLinePEOToleranceCoverageCount), 0),
                TOLERANCE_RANGE_COVERAGE = COALESCE(TRY_CONVERT(decimal(29,4), PriceLinePEOToleranceRangeCoverage), 0),
                TOTAL_AMOUNT = COALESCE(TRY_CONVERT(decimal(29,4), PriceLinePEOTotalAmount), 0),
                TOTAL_QUANTITY = COALESCE(TRY_CONVERT(decimal(29,4), PriceLinePEOTotalQuantity), 0),

                BZ_LOAD_DATE = COALESCE(CAST(AddDateTime AS date), CAST(GETDATE() AS date)),
                SV_LOAD_DATE = CAST(GETDATE() AS date),

                rn = ROW_NUMBER() OVER
                     (PARTITION BY COALESCE(TRY_CONVERT(bigint, PerfObligationId), 0)
                      ORDER BY COALESCE(TRY_CONVERT(datetime2(7), AddDateTime), '1900-01-01') DESC,
                               COALESCE(TRY_CONVERT(date, PerfObligationsLastUpdateDate), '1900-01-01') DESC)
            FROM src.bzo_VRM_PerfObligationsPVO
        )
        SELECT
            *
          , ROW_HASH = HASHBYTES('SHA2_256',
                CONCAT(
                    PERF_OBLIGATION_ID,'|',
                    BASE_PRICE,'|',BASE_PRICE_PERCENTAGE,'|',
                    CR_LIABILITY_AMT,'|',DR_ASSET_AMT,'|',DR_DISCOUNT_AMT,'|',NET_LINE_AMT_CONTR,'|',
                    TOTAL_BILLED_AMT_CONTR,'|',TOT_ALLOC_AMT_CONTR,'|',TOTAL_RECOG_REV_AMT_CONTR,'|',
                    TOT_CARVE_OUT_AMT_CONTR,'|',TOT_NET_CONSIDER_AMT_CONTR,'|',
                    COST_AMOUNT,'|',CREATED_BY,'|',CONVERT(char(10),CREATION_DATE,120),'|',
                    CUSTOMER_CONTRACT_HEADER_ID,'|',DISCARD_FLAG,'|',
                    DISCOUNT_AMOUNT,'|',DISCOUNT_PERCENTAGE,'|',EXEMPT_FROM_ALLOCATION_FLAG,'|',
                    FMV_LINE_ID,'|',GROSS_MARGIN_PERCENTAGE,'|',INITIAL_PERF_EVT_CREATED_FLAG,'|',
                    CONVERT(char(10),INITIAL_PERF_EVT_EXPECTED_DATE,120),'|',INITIAL_PERF_EVT_RECORDED_FLAG,'|',
                    CONVERT(char(10),LAST_UPDATE_DATE,120),'|',LAST_UPDATED_BY,'|',
                    LATEST_REVISION_INTENT_CODE,'|',CONVERT(char(10),LATEST_VERSION_DATE,120),'|',
                    LIST_PRICE_AMOUNT,'|',OBLIGATION_REFERENCE,'|',
                    FMV_AMT,'|',NET_LINE_AMT_OBLIG,'|',NET_SALES_PRICE,'|',OBLIG_CURRENCY_CODE,'|',
                    TOTAL_BILLED_AMT_OBLIG,'|',TOT_ALLOC_AMT_OBLIG,'|',TOTAL_RECOG_REV_AMT_OBLIG,'|',
                    TOT_CARVE_OUT_AMT_OBLIG,'|',TOT_NET_CONSIDER_AMT_OBLIG,'|',
                    OBLIG_PAYMENT_AMOUNT,'|',PERF_OBLIGATION_NUMBER,'|',PERF_OBLIGATION_TYPE,'|',
                    PERF_OBLIG_CLASSIFICATION_CODE,'|',PERF_OBLIG_FREEZE_FLAG,'|',REMOVED_FLAG,'|',
                    CONVERT(char(10),SATISFACTION_DATE,120),'|',SATISFACTION_METHOD,'|',SATISFACTION_STATUS,'|',
                    UNIT_SELLING_PRICE,'|',
                    AVERAGE_PRICE,'|',FAIR_MARKET_VALUE,'|',FMV_TOLERANCE_HIGH_PCT,'|',FMV_TOLERANCE_LOW_PCT,'|',
                    HIGHEST_PRICE,'|',CONVERT(char(10),PRICE_LAST_UPDATE_DATE,120),'|',LINE_COUNT,'|',
                    LOWEST_PRICE,'|',STANDARD_DEVIATION,'|',TOLERANCE_COVERAGE,'|',TOLERANCE_COVERAGE_COUNT,'|',
                    TOLERANCE_RANGE_COVERAGE,'|',TOTAL_AMOUNT,'|',TOTAL_QUANTITY
                )
            )
        INTO #Src
        FROM Base
        WHERE rn = 1
          AND PERF_OBLIGATION_ID IS NOT NULL;

        /* Plug row (only after truncate or if missing) */
        IF NOT EXISTS (SELECT 1 FROM svo.D_RM_PERF_OBLIGATION WHERE RM_PERF_OBLIGATION_SK = 0)
        BEGIN
            SET IDENTITY_INSERT svo.D_RM_PERF_OBLIGATION ON;

            INSERT INTO svo.D_RM_PERF_OBLIGATION
            (
                RM_PERF_OBLIGATION_SK,
                PERF_OBLIGATION_ID,
                BASE_PRICE, BASE_PRICE_PERCENTAGE,
                CR_LIABILITY_AMT, DR_ASSET_AMT, DR_DISCOUNT_AMT, NET_LINE_AMT_CONTR,
                TOTAL_BILLED_AMT_CONTR, TOT_ALLOC_AMT_CONTR, TOTAL_RECOG_REV_AMT_CONTR, TOT_CARVE_OUT_AMT_CONTR, TOT_NET_CONSIDER_AMT_CONTR,
                COST_AMOUNT, CREATED_BY, CREATION_DATE, CUSTOMER_CONTRACT_HEADER_ID,
                DISCARD_FLAG, DISCOUNT_AMOUNT, DISCOUNT_PERCENTAGE, EXEMPT_FROM_ALLOCATION_FLAG,
                FMV_LINE_ID, GROSS_MARGIN_PERCENTAGE, INITIAL_PERF_EVT_CREATED_FLAG, INITIAL_PERF_EVT_EXPECTED_DATE, INITIAL_PERF_EVT_RECORDED_FLAG,
                LAST_UPDATE_DATE, LAST_UPDATED_BY, LATEST_REVISION_INTENT_CODE, LATEST_VERSION_DATE,
                LIST_PRICE_AMOUNT, OBLIGATION_REFERENCE,
                FMV_AMT, NET_LINE_AMT_OBLIG, NET_SALES_PRICE, OBLIG_CURRENCY_CODE,
                TOTAL_BILLED_AMT_OBLIG, TOT_ALLOC_AMT_OBLIG, TOTAL_RECOG_REV_AMT_OBLIG, TOT_CARVE_OUT_AMT_OBLIG, TOT_NET_CONSIDER_AMT_OBLIG,
                OBLIG_PAYMENT_AMOUNT, PERF_OBLIGATION_NUMBER, PERF_OBLIGATION_TYPE, PERF_OBLIG_CLASSIFICATION_CODE,
                PERF_OBLIG_FREEZE_FLAG, REMOVED_FLAG, SATISFACTION_DATE, SATISFACTION_METHOD, SATISFACTION_STATUS,
                UNIT_SELLING_PRICE,
                AVERAGE_PRICE, FAIR_MARKET_VALUE, FMV_TOLERANCE_HIGH_PCT, FMV_TOLERANCE_LOW_PCT, HIGHEST_PRICE,
                PRICE_LAST_UPDATE_DATE, LINE_COUNT, LOWEST_PRICE, STANDARD_DEVIATION, TOLERANCE_COVERAGE, TOLERANCE_COVERAGE_COUNT,
                TOLERANCE_RANGE_COVERAGE, TOTAL_AMOUNT, TOTAL_QUANTITY,
                EFF_DATE, END_DATE, CRE_DATE, UDT_DATE, CURR_IND,
                ROW_HASH,
                BZ_LOAD_DATE, SV_LOAD_DATE
            )
            VALUES
            (
                0,
                0,
                0, 0,
                0, 0, 0, 0,
                0, 0, 0, 0, 0,
                0, 'Unknown', '0001-01-01', 0,
                'U', 0, 0, 'U',
                0, 0, 'U', '0001-01-01', 'U',
                '0001-01-01', 'Unknown', 'Unknown', '0001-01-01',
                0, 'Unknown',
                0, 0, 0, 'Unk',
                0, 0, 0, 0, 0,
                0, 0, 'Unknown', 'Unknown',
                'U', 'U', '0001-01-01', 'Unknown', 'Unknown',
                0,
                0, 0, 0, 0, 0,
                '0001-01-01', 0, 0, 0, 0, 0,
                0, 0, 0,
                '0001-01-01', '9999-12-31', SYSDATETIME(), SYSDATETIME(), '1',
                HASHBYTES('SHA2_256', 'PLUG'),
                CAST(GETDATE() AS date), CAST(GETDATE() AS date)
            );

            SET IDENTITY_INSERT svo.D_RM_PERF_OBLIGATION OFF;
        END

        IF @FullReload = 1
        BEGIN
            INSERT INTO svo.D_RM_PERF_OBLIGATION
            (
                PERF_OBLIGATION_ID,
                BASE_PRICE, BASE_PRICE_PERCENTAGE,
                CR_LIABILITY_AMT, DR_ASSET_AMT, DR_DISCOUNT_AMT, NET_LINE_AMT_CONTR,
                TOTAL_BILLED_AMT_CONTR, TOT_ALLOC_AMT_CONTR, TOTAL_RECOG_REV_AMT_CONTR, TOT_CARVE_OUT_AMT_CONTR, TOT_NET_CONSIDER_AMT_CONTR,
                COST_AMOUNT, CREATED_BY, CREATION_DATE, CUSTOMER_CONTRACT_HEADER_ID,
                DISCARD_FLAG, DISCOUNT_AMOUNT, DISCOUNT_PERCENTAGE, EXEMPT_FROM_ALLOCATION_FLAG,
                FMV_LINE_ID, GROSS_MARGIN_PERCENTAGE, INITIAL_PERF_EVT_CREATED_FLAG, INITIAL_PERF_EVT_EXPECTED_DATE, INITIAL_PERF_EVT_RECORDED_FLAG,
                LAST_UPDATE_DATE, LAST_UPDATED_BY, LATEST_REVISION_INTENT_CODE, LATEST_VERSION_DATE,
                LIST_PRICE_AMOUNT, OBLIGATION_REFERENCE,
                FMV_AMT, NET_LINE_AMT_OBLIG, NET_SALES_PRICE, OBLIG_CURRENCY_CODE,
                TOTAL_BILLED_AMT_OBLIG, TOT_ALLOC_AMT_OBLIG, TOTAL_RECOG_REV_AMT_OBLIG, TOT_CARVE_OUT_AMT_OBLIG, TOT_NET_CONSIDER_AMT_OBLIG,
                OBLIG_PAYMENT_AMOUNT, PERF_OBLIGATION_NUMBER, PERF_OBLIGATION_TYPE, PERF_OBLIG_CLASSIFICATION_CODE,
                PERF_OBLIG_FREEZE_FLAG, REMOVED_FLAG, SATISFACTION_DATE, SATISFACTION_METHOD, SATISFACTION_STATUS,
                UNIT_SELLING_PRICE,
                AVERAGE_PRICE, FAIR_MARKET_VALUE, FMV_TOLERANCE_HIGH_PCT, FMV_TOLERANCE_LOW_PCT, HIGHEST_PRICE,
                PRICE_LAST_UPDATE_DATE, LINE_COUNT, LOWEST_PRICE, STANDARD_DEVIATION, TOLERANCE_COVERAGE, TOLERANCE_COVERAGE_COUNT,
                TOLERANCE_RANGE_COVERAGE, TOTAL_AMOUNT, TOTAL_QUANTITY,
                EFF_DATE, END_DATE, CRE_DATE, UDT_DATE, CURR_IND,
                ROW_HASH,
                BZ_LOAD_DATE, SV_LOAD_DATE
            )
            SELECT
                S.PERF_OBLIGATION_ID,
                S.BASE_PRICE, S.BASE_PRICE_PERCENTAGE,
                S.CR_LIABILITY_AMT, S.DR_ASSET_AMT, S.DR_DISCOUNT_AMT, S.NET_LINE_AMT_CONTR,
                S.TOTAL_BILLED_AMT_CONTR, S.TOT_ALLOC_AMT_CONTR, S.TOTAL_RECOG_REV_AMT_CONTR, S.TOT_CARVE_OUT_AMT_CONTR, S.TOT_NET_CONSIDER_AMT_CONTR,
                S.COST_AMOUNT, S.CREATED_BY, S.CREATION_DATE, S.CUSTOMER_CONTRACT_HEADER_ID,
                S.DISCARD_FLAG, S.DISCOUNT_AMOUNT, S.DISCOUNT_PERCENTAGE, S.EXEMPT_FROM_ALLOCATION_FLAG,
                S.FMV_LINE_ID, S.GROSS_MARGIN_PERCENTAGE, S.INITIAL_PERF_EVT_CREATED_FLAG, S.INITIAL_PERF_EVT_EXPECTED_DATE, S.INITIAL_PERF_EVT_RECORDED_FLAG,
                S.LAST_UPDATE_DATE, S.LAST_UPDATED_BY, S.LATEST_REVISION_INTENT_CODE, S.LATEST_VERSION_DATE,
                S.LIST_PRICE_AMOUNT, S.OBLIGATION_REFERENCE,
                S.FMV_AMT, S.NET_LINE_AMT_OBLIG, S.NET_SALES_PRICE, S.OBLIG_CURRENCY_CODE,
                S.TOTAL_BILLED_AMT_OBLIG, S.TOT_ALLOC_AMT_OBLIG, S.TOTAL_RECOG_REV_AMT_OBLIG, S.TOT_CARVE_OUT_AMT_OBLIG, S.TOT_NET_CONSIDER_AMT_OBLIG,
                S.OBLIG_PAYMENT_AMOUNT, S.PERF_OBLIGATION_NUMBER, S.PERF_OBLIGATION_TYPE, S.PERF_OBLIG_CLASSIFICATION_CODE,
                S.PERF_OBLIG_FREEZE_FLAG, S.REMOVED_FLAG, S.SATISFACTION_DATE, S.SATISFACTION_METHOD, S.SATISFACTION_STATUS,
                S.UNIT_SELLING_PRICE,
                S.AVERAGE_PRICE, S.FAIR_MARKET_VALUE, S.FMV_TOLERANCE_HIGH_PCT, S.FMV_TOLERANCE_LOW_PCT, S.HIGHEST_PRICE,
                S.PRICE_LAST_UPDATE_DATE, S.LINE_COUNT, S.LOWEST_PRICE, S.STANDARD_DEVIATION, S.TOLERANCE_COVERAGE, S.TOLERANCE_COVERAGE_COUNT,
                S.TOLERANCE_RANGE_COVERAGE, S.TOTAL_AMOUNT, S.TOTAL_QUANTITY,
                @asof, CAST('9999-12-31' AS date), SYSDATETIME(), SYSDATETIME(), '1',
                S.ROW_HASH,
                S.BZ_LOAD_DATE, CAST(GETDATE() AS date)
            FROM #Src S
            WHERE S.PERF_OBLIGATION_ID <> 0;

            SET @rows_inserted = @@ROWCOUNT;
        END
        ELSE
        BEGIN
            /* Expire changed current rows */
            UPDATE T
               SET T.END_DATE     = DATEADD(day, -1, @asof),
                   T.CURR_IND     = '0',
                   T.UDT_DATE     = SYSDATETIME(),
                   T.SV_LOAD_DATE = CAST(GETDATE() AS date)
            FROM svo.D_RM_PERF_OBLIGATION T
            JOIN #Src S
              ON S.PERF_OBLIGATION_ID = T.PERF_OBLIGATION_ID
            WHERE T.CURR_IND = '1'
              AND T.PERF_OBLIGATION_ID <> 0
              AND T.ROW_HASH <> S.ROW_HASH;

            SET @rows_expired = @@ROWCOUNT;

            /* Insert new keys and changed keys */
            INSERT INTO svo.D_RM_PERF_OBLIGATION
            (
                PERF_OBLIGATION_ID,
                BASE_PRICE, BASE_PRICE_PERCENTAGE,
                CR_LIABILITY_AMT, DR_ASSET_AMT, DR_DISCOUNT_AMT, NET_LINE_AMT_CONTR,
                TOTAL_BILLED_AMT_CONTR, TOT_ALLOC_AMT_CONTR, TOTAL_RECOG_REV_AMT_CONTR, TOT_CARVE_OUT_AMT_CONTR, TOT_NET_CONSIDER_AMT_CONTR,
                COST_AMOUNT, CREATED_BY, CREATION_DATE, CUSTOMER_CONTRACT_HEADER_ID,
                DISCARD_FLAG, DISCOUNT_AMOUNT, DISCOUNT_PERCENTAGE, EXEMPT_FROM_ALLOCATION_FLAG,
                FMV_LINE_ID, GROSS_MARGIN_PERCENTAGE, INITIAL_PERF_EVT_CREATED_FLAG, INITIAL_PERF_EVT_EXPECTED_DATE, INITIAL_PERF_EVT_RECORDED_FLAG,
                LAST_UPDATE_DATE, LAST_UPDATED_BY, LATEST_REVISION_INTENT_CODE, LATEST_VERSION_DATE,
                LIST_PRICE_AMOUNT, OBLIGATION_REFERENCE,
                FMV_AMT, NET_LINE_AMT_OBLIG, NET_SALES_PRICE, OBLIG_CURRENCY_CODE,
                TOTAL_BILLED_AMT_OBLIG, TOT_ALLOC_AMT_OBLIG, TOTAL_RECOG_REV_AMT_OBLIG, TOT_CARVE_OUT_AMT_OBLIG, TOT_NET_CONSIDER_AMT_OBLIG,
                OBLIG_PAYMENT_AMOUNT, PERF_OBLIGATION_NUMBER, PERF_OBLIGATION_TYPE, PERF_OBLIG_CLASSIFICATION_CODE,
                PERF_OBLIG_FREEZE_FLAG, REMOVED_FLAG, SATISFACTION_DATE, SATISFACTION_METHOD, SATISFACTION_STATUS,
                UNIT_SELLING_PRICE,
                AVERAGE_PRICE, FAIR_MARKET_VALUE, FMV_TOLERANCE_HIGH_PCT, FMV_TOLERANCE_LOW_PCT, HIGHEST_PRICE,
                PRICE_LAST_UPDATE_DATE, LINE_COUNT, LOWEST_PRICE, STANDARD_DEVIATION, TOLERANCE_COVERAGE, TOLERANCE_COVERAGE_COUNT,
                TOLERANCE_RANGE_COVERAGE, TOTAL_AMOUNT, TOTAL_QUANTITY,
                EFF_DATE, END_DATE, CRE_DATE, UDT_DATE, CURR_IND,
                ROW_HASH,
                BZ_LOAD_DATE, SV_LOAD_DATE
            )
            SELECT
                S.PERF_OBLIGATION_ID,
                S.BASE_PRICE, S.BASE_PRICE_PERCENTAGE,
                S.CR_LIABILITY_AMT, S.DR_ASSET_AMT, S.DR_DISCOUNT_AMT, S.NET_LINE_AMT_CONTR,
                S.TOTAL_BILLED_AMT_CONTR, S.TOT_ALLOC_AMT_CONTR, S.TOTAL_RECOG_REV_AMT_CONTR, S.TOT_CARVE_OUT_AMT_CONTR, S.TOT_NET_CONSIDER_AMT_CONTR,
                S.COST_AMOUNT, S.CREATED_BY, S.CREATION_DATE, S.CUSTOMER_CONTRACT_HEADER_ID,
                S.DISCARD_FLAG, S.DISCOUNT_AMOUNT, S.DISCOUNT_PERCENTAGE, S.EXEMPT_FROM_ALLOCATION_FLAG,
                S.FMV_LINE_ID, S.GROSS_MARGIN_PERCENTAGE, S.INITIAL_PERF_EVT_CREATED_FLAG, S.INITIAL_PERF_EVT_EXPECTED_DATE, S.INITIAL_PERF_EVT_RECORDED_FLAG,
                S.LAST_UPDATE_DATE, S.LAST_UPDATED_BY, S.LATEST_REVISION_INTENT_CODE, S.LATEST_VERSION_DATE,
                S.LIST_PRICE_AMOUNT, S.OBLIGATION_REFERENCE,
                S.FMV_AMT, S.NET_LINE_AMT_OBLIG, S.NET_SALES_PRICE, S.OBLIG_CURRENCY_CODE,
                S.TOTAL_BILLED_AMT_OBLIG, S.TOT_ALLOC_AMT_OBLIG, S.TOTAL_RECOG_REV_AMT_OBLIG, S.TOT_CARVE_OUT_AMT_OBLIG, S.TOT_NET_CONSIDER_AMT_OBLIG,
                S.OBLIG_PAYMENT_AMOUNT, S.PERF_OBLIGATION_NUMBER, S.PERF_OBLIGATION_TYPE, S.PERF_OBLIG_CLASSIFICATION_CODE,
                S.PERF_OBLIG_FREEZE_FLAG, S.REMOVED_FLAG, S.SATISFACTION_DATE, S.SATISFACTION_METHOD, S.SATISFACTION_STATUS,
                S.UNIT_SELLING_PRICE,
                S.AVERAGE_PRICE, S.FAIR_MARKET_VALUE, S.FMV_TOLERANCE_HIGH_PCT, S.FMV_TOLERANCE_LOW_PCT, S.HIGHEST_PRICE,
                S.PRICE_LAST_UPDATE_DATE, S.LINE_COUNT, S.LOWEST_PRICE, S.STANDARD_DEVIATION, S.TOLERANCE_COVERAGE, S.TOLERANCE_COVERAGE_COUNT,
                S.TOLERANCE_RANGE_COVERAGE, S.TOTAL_AMOUNT, S.TOTAL_QUANTITY,
                @asof, CAST('9999-12-31' AS date), SYSDATETIME(), SYSDATETIME(), '1',
                S.ROW_HASH,
                S.BZ_LOAD_DATE, CAST(GETDATE() AS date)
            FROM #Src S
            LEFT JOIN svo.D_RM_PERF_OBLIGATION T
              ON T.PERF_OBLIGATION_ID = S.PERF_OBLIGATION_ID
             AND T.CURR_IND = '1'
            WHERE S.PERF_OBLIGATION_ID <> 0
              AND (T.PERF_OBLIGATION_ID IS NULL OR T.ROW_HASH <> S.ROW_HASH);

            SET @rows_inserted = @@ROWCOUNT;
        END

        COMMIT;

        UPDATE etl.ETL_RUN
           SET END_DTTM      = SYSDATETIME(),
               STATUS        = 'SUCCESS',
               ROW_INSERTED  = @rows_inserted,
               ROW_EXPIRED   = @rows_expired
         WHERE RUN_ID = @run_id;

        IF @Debug = 1
            PRINT 'Completed ' + @proc
                + ' | RUN_ID=' + CONVERT(varchar(30), @run_id)
                + ' | inserted=' + CONVERT(varchar(30), @rows_inserted)
                + ' | expired=' + CONVERT(varchar(30), @rows_expired);
    END TRY
    BEGIN CATCH
        IF XACT_STATE() <> 0 ROLLBACK;

        DECLARE @ErrMsg nvarchar(4000) = ERROR_MESSAGE();

        IF @run_id IS NOT NULL
        BEGIN
            UPDATE etl.ETL_RUN
               SET END_DTTM = SYSDATETIME(),
                   STATUS = 'FAILED',
                   ERROR_MESSAGE = @ErrMsg
             WHERE RUN_ID = @run_id;
        END

        ;THROW;
    END CATCH
END
GO