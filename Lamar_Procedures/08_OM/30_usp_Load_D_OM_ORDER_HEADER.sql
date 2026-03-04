/* =========================================================
   usp_Load_D_OM_ORDER_HEADER
   Type 1 incremental load. Source: bzo.OM_HeaderExtractPVO + OM_5FHeaderprivateVO, OM_DataprivateVO
   Watermark: AddDateTime. Grain: ORDER_HEADER_ID. Plug row SK=0 if missing.
   Idempotent: safe to retry.
   ========================================================= */
CREATE OR ALTER PROCEDURE svo.usp_Load_D_OM_ORDER_HEADER
    @BatchId INT = NULL
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    DECLARE
        @ProcName       SYSNAME        = OBJECT_SCHEMA_NAME(@@PROCID) + '.' + OBJECT_NAME(@@PROCID),
        @TargetObject   SYSNAME        = 'svo.D_OM_ORDER_HEADER',
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

    SELECT @TableBridgeID = TableBridgeID FROM meta.MedallionTableBridge WHERE targettable = N'OM_HeaderExtractPVO';

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

        IF NOT EXISTS (SELECT 1 FROM svo.D_OM_ORDER_HEADER WHERE ORDER_HEADER_SK = 0)
        BEGIN
            SET IDENTITY_INSERT svo.D_OM_ORDER_HEADER ON;
            INSERT INTO svo.D_OM_ORDER_HEADER (ORDER_HEADER_SK, ORDER_HEADER_ID, LEGAL_ENTITY_SK, BUSINESS_UNIT_SK, SOLD_TO_PARTY_SK, SALESPERSON_SK, ORDER_NUMBER, SOURCE_ORDER_NUMBER, SOURCE_SYSTEM, STATUS_CODE, OPEN_FLAG, ON_HOLD_FLAG, ORDERED_DATE_SK, PAYMENT_TERM, SUBMITTED_DATE_SK, REVISION_NUMBER, TRANSACTIONAL_CURRENCY_CODE, APPLIED_CURRENCY_CODE, CONTRACT_NUMBER, NATIONAL_CONTRACT_NUMBER, OPPORTUNITY_NUMBER, OPPORTUNITY_NAME, CPQ_CONTRACT_NUMBER, CPQ_TRANSACTION_NUMBER, CAMPAIGN_NAME, ADVERTISER, BRAND, SALES_CATEGORY, SELLING_TEAM, SELLING_AE, ORIGINATING_COMPANY, BZ_LOAD_DATE, SV_LOAD_DATE)
            VALUES (0, -1, 0, 0, 0, 0, -1, -1, 'UNK', 'U', 'U', 'U', 10101, 'UNK', 10101, -1, 'UNK', 'UNK', -1, -1, -1, 'UNK', -1, -1, 'UNK', 'UNK', 'UNK', 'UNK', -1, -1, -1, CAST('0001-01-01' AS DATE), CAST(GETDATE() AS DATE));
            SET IDENTITY_INSERT svo.D_OM_ORDER_HEADER OFF;
        END;

        IF OBJECT_ID('tempdb..#src') IS NOT NULL DROP TABLE #src;

        SELECT
            H.HeaderId AS ORDER_HEADER_ID,
            ISNULL(LE.LEGAL_ENTITY_SK, 0) AS LEGAL_ENTITY_SK,
            ISNULL(BU.BUSINESS_UNIT_SK, 0) AS BUSINESS_UNIT_SK,
            ISNULL(SC.CUSTOMER_SK, 0) AS SOLD_TO_PARTY_SK,
            ISNULL(SR.SALES_REP_SK, 0) AS SALESPERSON_SK,
            ISNULL(H.HeaderOrderNumber, -1) AS ORDER_NUMBER,
            ISNULL(H.HeaderSourceOrderNumber, -1) AS SOURCE_ORDER_NUMBER,
            ISNULL(H.HeaderSourceOrderSystem, -1) AS SOURCE_SYSTEM,
            ISNULL(H.HeaderStatusCode, -1) AS STATUS_CODE,
            ISNULL(H.HeaderOpenFlag, -1) AS OPEN_FLAG,
            ISNULL(H.HeaderOnHold, -1) AS ON_HOLD_FLAG,
            ISNULL(PT.PAYMENT_TERM_NAME, 'UNK') AS PAYMENT_TERM,
            ISNULL(CONVERT(INT, FORMAT(H.HeaderOrderedDate, 'yyyyMMdd')), 10101) AS ORDERED_DATE_SK,
            ISNULL(CONVERT(INT, FORMAT(H.HeaderSubmittedDate, 'yyyyMMdd')), 10101) AS SUBMITTED_DATE_SK,
            ISNULL(H.HeaderSourceRevisionNumber, -1) AS REVISION_NUMBER,
            ISNULL(H.HeaderTransactionalCurrencyCode, 'UNK') AS TRANSACTIONAL_CURRENCY_CODE,
            ISNULL(H.HeaderAppliedCurrencyCode, 'UNK') AS APPLIED_CURRENCY_CODE,
            ISNULL(DP.contractNumber, -1) AS CONTRACT_NUMBER,
            ISNULL(DP.nationalContractNumber, -1) AS NATIONAL_CONTRACT_NUMBER,
            ISNULL(DP.opportunityNumber, -1) AS OPPORTUNITY_NUMBER,
            ISNULL(DP.opportunity, -1) AS OPPORTUNITY_NAME,
            ISNULL(HP.cpqContractNumber, -1) AS CPQ_CONTRACT_NUMBER,
            ISNULL(HP.cpqTransactionNumber, -1) AS CPQ_TRANSACTION_NUMBER,
            ISNULL(HP.campaignName, -1) AS CAMPAIGN_NAME,
            ISNULL(HP.advertiser, -1) AS ADVERTISER,
            ISNULL(HP.brand, -1) AS BRAND,
            ISNULL(HP.salesCategory, -1) AS SALES_CATEGORY,
            ISNULL(HP.sellingTeam, -1) AS SELLING_TEAM,
            ISNULL(HP.sellingAeCodeAeName, -1) AS SELLING_AE,
            ISNULL(HP.originatingCompany, -1) AS ORIGINATING_COMPANY,
            CAST(H.AddDateTime AS DATE) AS BZ_LOAD_DATE,
            CAST(GETDATE() AS DATE) AS SV_LOAD_DATE,
            H.AddDateTime AS SourceAddDateTime
        INTO #src
        FROM bzo.OM_HeaderExtractPVO H
        LEFT JOIN bzo.OM_5FHeaderprivateVO HP ON H.HeaderId = HP.HeaderId
        LEFT JOIN bzo.OM_DataprivateVO DP ON H.HeaderId = DP.HeaderId
        LEFT JOIN svo.D_CUSTOMER_ACCOUNT SC ON H.HeaderSoldToPartyId = SC.PARTY_ID
        LEFT JOIN svo.D_LEGAL_ENTITY LE ON H.HeaderLegalEntityId = LE.LEGAL_ENTITY_ID
        LEFT JOIN svo.D_BUSINESS_UNIT BU ON H.HeaderOrgId = BU.BUSINESS_UNIT_ID
        LEFT JOIN svo.D_PAYMENT_TERM PT ON H.HeaderPaymentTermId = PT.PAYMENT_TERM_ID
        LEFT JOIN svo.D_SALES_REP SR ON H.HeaderSalespersonId = SR.SALES_REP_ID
        WHERE H.AddDateTime > @LastWatermark;

        SELECT @MaxWatermark = MAX(SourceAddDateTime) FROM #src;

        MERGE svo.D_OM_ORDER_HEADER AS tgt
        USING #src AS src ON tgt.ORDER_HEADER_ID = src.ORDER_HEADER_ID
        WHEN MATCHED THEN UPDATE SET
            tgt.LEGAL_ENTITY_SK = src.LEGAL_ENTITY_SK,
            tgt.BUSINESS_UNIT_SK = src.BUSINESS_UNIT_SK,
            tgt.SOLD_TO_PARTY_SK = src.SOLD_TO_PARTY_SK,
            tgt.SALESPERSON_SK = src.SALESPERSON_SK,
            tgt.ORDER_NUMBER = src.ORDER_NUMBER,
            tgt.SOURCE_ORDER_NUMBER = src.SOURCE_ORDER_NUMBER,
            tgt.SOURCE_SYSTEM = src.SOURCE_SYSTEM,
            tgt.STATUS_CODE = src.STATUS_CODE,
            tgt.OPEN_FLAG = src.OPEN_FLAG,
            tgt.ON_HOLD_FLAG = src.ON_HOLD_FLAG,
            tgt.PAYMENT_TERM = src.PAYMENT_TERM,
            tgt.ORDERED_DATE_SK = src.ORDERED_DATE_SK,
            tgt.SUBMITTED_DATE_SK = src.SUBMITTED_DATE_SK,
            tgt.REVISION_NUMBER = src.REVISION_NUMBER,
            tgt.TRANSACTIONAL_CURRENCY_CODE = src.TRANSACTIONAL_CURRENCY_CODE,
            tgt.APPLIED_CURRENCY_CODE = src.APPLIED_CURRENCY_CODE,
            tgt.CONTRACT_NUMBER = src.CONTRACT_NUMBER,
            tgt.NATIONAL_CONTRACT_NUMBER = src.NATIONAL_CONTRACT_NUMBER,
            tgt.OPPORTUNITY_NUMBER = src.OPPORTUNITY_NUMBER,
            tgt.OPPORTUNITY_NAME = src.OPPORTUNITY_NAME,
            tgt.CPQ_CONTRACT_NUMBER = src.CPQ_CONTRACT_NUMBER,
            tgt.CPQ_TRANSACTION_NUMBER = src.CPQ_TRANSACTION_NUMBER,
            tgt.CAMPAIGN_NAME = src.CAMPAIGN_NAME,
            tgt.ADVERTISER = src.ADVERTISER,
            tgt.BRAND = src.BRAND,
            tgt.SALES_CATEGORY = src.SALES_CATEGORY,
            tgt.SELLING_TEAM = src.SELLING_TEAM,
            tgt.SELLING_AE = src.SELLING_AE,
            tgt.ORIGINATING_COMPANY = src.ORIGINATING_COMPANY,
            tgt.BZ_LOAD_DATE = src.BZ_LOAD_DATE,
            tgt.SV_LOAD_DATE = src.SV_LOAD_DATE
        WHEN NOT MATCHED BY TARGET THEN INSERT (
            ORDER_HEADER_ID, LEGAL_ENTITY_SK, BUSINESS_UNIT_SK, SOLD_TO_PARTY_SK, SALESPERSON_SK, ORDER_NUMBER, SOURCE_ORDER_NUMBER, SOURCE_SYSTEM, STATUS_CODE, OPEN_FLAG, ON_HOLD_FLAG, PAYMENT_TERM, ORDERED_DATE_SK, SUBMITTED_DATE_SK, REVISION_NUMBER, TRANSACTIONAL_CURRENCY_CODE, APPLIED_CURRENCY_CODE, CONTRACT_NUMBER, NATIONAL_CONTRACT_NUMBER, OPPORTUNITY_NUMBER, OPPORTUNITY_NAME, CPQ_CONTRACT_NUMBER, CPQ_TRANSACTION_NUMBER, CAMPAIGN_NAME, ADVERTISER, BRAND, SALES_CATEGORY, SELLING_TEAM, SELLING_AE, ORIGINATING_COMPANY, BZ_LOAD_DATE, SV_LOAD_DATE
        ) VALUES (
            src.ORDER_HEADER_ID, src.LEGAL_ENTITY_SK, src.BUSINESS_UNIT_SK, src.SOLD_TO_PARTY_SK, src.SALESPERSON_SK, src.ORDER_NUMBER, src.SOURCE_ORDER_NUMBER, src.SOURCE_SYSTEM, src.STATUS_CODE, src.OPEN_FLAG, src.ON_HOLD_FLAG, src.PAYMENT_TERM, src.ORDERED_DATE_SK, src.SUBMITTED_DATE_SK, src.REVISION_NUMBER, src.TRANSACTIONAL_CURRENCY_CODE, src.APPLIED_CURRENCY_CODE, src.CONTRACT_NUMBER, src.NATIONAL_CONTRACT_NUMBER, src.OPPORTUNITY_NUMBER, src.OPPORTUNITY_NAME, src.CPQ_CONTRACT_NUMBER, src.CPQ_TRANSACTION_NUMBER, src.CAMPAIGN_NAME, src.ADVERTISER, src.BRAND, src.SALES_CATEGORY, src.SELLING_TEAM, src.SELLING_AE, src.ORIGINATING_COMPANY, src.BZ_LOAD_DATE, src.SV_LOAD_DATE
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
