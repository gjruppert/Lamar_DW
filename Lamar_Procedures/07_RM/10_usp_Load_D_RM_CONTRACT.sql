/* =========================================================
   usp_Load_D_RM_CONTRACT
   Type 1 incremental load. Source: bzo.VRM_CustomerContractHeadersPVO
   Watermark: AddDateTime. Grain: CUSTOMER_CONTRACT_HEADER_ID. Plug row SK=0 if missing.
   Idempotent: safe to retry.
   ========================================================= */
CREATE OR ALTER PROCEDURE svo.usp_Load_D_RM_CONTRACT
    @BatchId INT = NULL
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    DECLARE
        @ProcName       SYSNAME        = OBJECT_SCHEMA_NAME(@@PROCID) + '.' + OBJECT_NAME(@@PROCID),
        @TargetObject   SYSNAME        = 'svo.D_RM_CONTRACT',
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

    SELECT @TableBridgeID = TableBridgeID FROM meta.MedallionTableBridge WHERE targettable = N'VRM_CustomerContractHeadersPVO';

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

        IF NOT EXISTS (SELECT 1 FROM svo.D_RM_CONTRACT WHERE RM_CONTRACT_SK = 0)
        BEGIN
            SET IDENTITY_INSERT svo.D_RM_CONTRACT ON;
            INSERT INTO svo.D_RM_CONTRACT
            (RM_CONTRACT_SK, CUSTOMER_CONTRACT_HEADER_ID, CONTRACT_CURRENCY_CODE, CUSTOMER_CONTRACT_NUMBER, EFFECTIVITY_PERIOD_ID, LEDGER_ID,
             ALLOCATION_PENDING_REASON, ALLOCATION_REQUEST_ID, ALLOCATION_STATUS, ATTRIBUTE1, ATTRIBUTE_CATEGORY, CONTRACT_CLASSIFICATION_CODE, CONTRACT_RULE_ID,
             CONTR_TOTAL_BILLED_AMT, CONTR_TOTAL_RECOG_REV_AMT, CONTR_TRANSACTION_PRICE, CREATED_BY, CREATED_FROM, CREATION_DATE, CUSTOMER_CONTRACT_DATE, CUSTOMER_CONTRACT_FREEZE_DATE,
             EXCHANGE_RATE_DATE, EXCHANGE_RATE_TYPE, LAST_UPDATE_DATE, LAST_UPDATED_BY, LAST_UPDATE_LOGIN, LEGAL_ENTITY_ID, OBJECT_VERSION_NUMBER,
             CONTRACT_REFERENCE, REVIEW_STATUS, SINGLE_OBLIGATION_FLAG, STANDALONE_SALES_FLAG, ADJUSTMENT_STATUS_CODE, CONTRACT_GROUP_NUMBER, CONTRACT_CREATED_BY,
             EXCL_FROM_AUTO_WRITEOFF_FLAG, FULL_SATISFACTION_DATE, LAST_ACTIVITY_DATE, CONTRACT_LAST_UPDATED_BY, LATEST_IMMATERIAL_CHANGE_CODE, LATEST_REVISION_INTENT_CODE, LATEST_VERSION_DATE, SATISFACTION_STATUS,
             BZ_LOAD_DATE, SV_LOAD_DATE)
            VALUES
            (0, -1, 'Unk', 0, -1, -1, 'Unknown', -1, 'Unknown', 'Unknown', 'Unknown', 'Unknown', 0, 0, 0, 0, 'Unknown', 'Unk', '1900-01-01', '1900-01-01', '9999-12-31', '1900-01-01', 'Unk', '1900-01-01', 'Unknown', 'Unknown', -1, 0, 'Unknown', 'Unknown', 'U', 'U', 'Unknown', 'Unknown', 'Unknown', 'U', '9999-12-31', '1900-01-01', 'Unknown', 'Unknown', 'Unk', '9999-12-31', 'Unknown', CAST(GETDATE() AS DATE), CAST(GETDATE() AS DATE));
            SET IDENTITY_INSERT svo.D_RM_CONTRACT OFF;
        END;

        IF OBJECT_ID('tempdb..#src') IS NOT NULL DROP TABLE #src;

        SELECT
            ISNULL(C.CustomerContractHeaderId, -1)                  AS CUSTOMER_CONTRACT_HEADER_ID,
            ISNULL(C.CustContHeadersContractCurrencyCode, 'Unk')     AS CONTRACT_CURRENCY_CODE,
            ISNULL(C.CustContHeadersCustomerContractNumber, 0)       AS CUSTOMER_CONTRACT_NUMBER,
            ISNULL(C.CustContHeadersEffectivityPeriodId, -1)        AS EFFECTIVITY_PERIOD_ID,
            ISNULL(C.CustContHeadersLedgerId, -1)                   AS LEDGER_ID,
            ISNULL(C.CustContHeadersAllocationPendingReason, 'Unknown') AS ALLOCATION_PENDING_REASON,
            ISNULL(C.CustContHeadersAllocationRequestId, -1)        AS ALLOCATION_REQUEST_ID,
            ISNULL(C.CustContHeadersAllocationStatus, 'Unknown')    AS ALLOCATION_STATUS,
            ISNULL(C.CustContHeadersAttribute1, 'Unknown')          AS ATTRIBUTE1,
            ISNULL(C.CustContHeadersAttributeCategory, 'Unknown')   AS ATTRIBUTE_CATEGORY,
            ISNULL(C.CustContHeadersContractClassificationCode, 'Unknown') AS CONTRACT_CLASSIFICATION_CODE,
            ISNULL(C.CustContHeadersContractRuleId, -1)             AS CONTRACT_RULE_ID,
            ISNULL(C.CustContHeadersContrCurTotalBilledAmt, 0)      AS CONTR_TOTAL_BILLED_AMT,
            ISNULL(C.CustContHeadersContrCurTotalRecogRevAmt, 0)     AS CONTR_TOTAL_RECOG_REV_AMT,
            ISNULL(C.CustContHeadersContrCurTransactionPrice, 0)     AS CONTR_TRANSACTION_PRICE,
            ISNULL(C.CustContHeadersCreatedBy, 'Unknown')           AS CREATED_BY,
            ISNULL(C.CustContHeadersCreatedFrom, 'Unk')              AS CREATED_FROM,
            ISNULL(CAST(C.CustContHeadersCreationDate AS DATE), '1900-01-01') AS CREATION_DATE,
            ISNULL(C.CustContHeadersCustomerContractDate, '1900-01-01') AS CUSTOMER_CONTRACT_DATE,
            ISNULL(C.CustContHeadersCustomerContractFreezeDate, '9999-12-31') AS CUSTOMER_CONTRACT_FREEZE_DATE,
            ISNULL(C.CustContHeadersExchangeRateDate, '1900-01-01') AS EXCHANGE_RATE_DATE,
            ISNULL(C.CustContHeadersExchangeRateType, 'Unk')         AS EXCHANGE_RATE_TYPE,
            ISNULL(CAST(C.CustContHeadersLastUpdateDate AS DATE), '1900-01-01') AS LAST_UPDATE_DATE,
            ISNULL(C.CustContHeadersLastUpdatedBy, 'Unknown')       AS LAST_UPDATED_BY,
            C.CustContHeadersLastUpdateLogin                        AS LAST_UPDATE_LOGIN,
            ISNULL(C.CustContHeadersLegalEntityId, -1)               AS LEGAL_ENTITY_ID,
            ISNULL(C.CustContHeadersObjectVersionNumber, 0)        AS OBJECT_VERSION_NUMBER,
            ISNULL(C.CustContHeadersReference, 'Unknown')           AS CONTRACT_REFERENCE,
            ISNULL(C.CustContHeadersReviewStatus, 'Unknown')        AS REVIEW_STATUS,
            ISNULL(C.CustContHeadersSingleObligationFlag, 'U')        AS SINGLE_OBLIGATION_FLAG,
            ISNULL(C.CustContHeadersStandaloneSalesFlag, 'U')        AS STANDALONE_SALES_FLAG,
            ISNULL(C.CustomerContractHeadersAdjustmentStatusCode, 'Unknown') AS ADJUSTMENT_STATUS_CODE,
            ISNULL(C.CustomerContractHeadersContractGroupNumber, 'Unknown') AS CONTRACT_GROUP_NUMBER,
            ISNULL(C.CustomerContractHeadersCreatedBy, 'Unknown')   AS CONTRACT_CREATED_BY,
            ISNULL(C.CustomerContractHeadersExclFromAutoWriteoffFlag, 'U') AS EXCL_FROM_AUTO_WRITEOFF_FLAG,
            ISNULL(C.CustomerContractHeadersFullSatisfactionDate, '9999-12-31') AS FULL_SATISFACTION_DATE,
            ISNULL(C.CustomerContractHeadersLastActivityDate, '1900-01-01') AS LAST_ACTIVITY_DATE,
            ISNULL(C.CustomerContractHeadersLastUpdatedBy, 'Unknown') AS CONTRACT_LAST_UPDATED_BY,
            ISNULL(C.CustomerContractHeadersLatestImmaterialChangeCode, 'Unk') AS LATEST_IMMATERIAL_CHANGE_CODE,
            ISNULL(C.CustomerContractHeadersLatestRevisionIntentCode, 'Unk') AS LATEST_REVISION_INTENT_CODE,
            ISNULL(C.CustomerContractHeadersLatestVersionDate, '9999-12-31') AS LATEST_VERSION_DATE,
            ISNULL(C.CustomerContractHeadersSatisfactionStatus, 'Unknown') AS SATISFACTION_STATUS,
            ISNULL(CAST(C.AddDateTime AS DATE), CAST(GETDATE() AS DATE)) AS BZ_LOAD_DATE,
            CAST(GETDATE() AS DATE)                                 AS SV_LOAD_DATE,
            ISNULL(C.AddDateTime, SYSDATETIME())                    AS SourceAddDateTime
        INTO #src
        FROM bzo.VRM_CustomerContractHeadersPVO AS C
        WHERE C.AddDateTime > @LastWatermark;

        SELECT @MaxWatermark = MAX(SourceAddDateTime) FROM #src;

        MERGE svo.D_RM_CONTRACT AS tgt
        USING #src AS src ON tgt.CUSTOMER_CONTRACT_HEADER_ID = src.CUSTOMER_CONTRACT_HEADER_ID
        WHEN MATCHED THEN UPDATE SET
            tgt.CONTRACT_CURRENCY_CODE = src.CONTRACT_CURRENCY_CODE,
            tgt.CUSTOMER_CONTRACT_NUMBER = src.CUSTOMER_CONTRACT_NUMBER,
            tgt.EFFECTIVITY_PERIOD_ID = src.EFFECTIVITY_PERIOD_ID,
            tgt.LEDGER_ID = src.LEDGER_ID,
            tgt.ALLOCATION_PENDING_REASON = src.ALLOCATION_PENDING_REASON,
            tgt.ALLOCATION_REQUEST_ID = src.ALLOCATION_REQUEST_ID,
            tgt.ALLOCATION_STATUS = src.ALLOCATION_STATUS,
            tgt.ATTRIBUTE1 = src.ATTRIBUTE1,
            tgt.ATTRIBUTE_CATEGORY = src.ATTRIBUTE_CATEGORY,
            tgt.CONTRACT_CLASSIFICATION_CODE = src.CONTRACT_CLASSIFICATION_CODE,
            tgt.CONTRACT_RULE_ID = src.CONTRACT_RULE_ID,
            tgt.CONTR_TOTAL_BILLED_AMT = src.CONTR_TOTAL_BILLED_AMT,
            tgt.CONTR_TOTAL_RECOG_REV_AMT = src.CONTR_TOTAL_RECOG_REV_AMT,
            tgt.CONTR_TRANSACTION_PRICE = src.CONTR_TRANSACTION_PRICE,
            tgt.CREATED_BY = src.CREATED_BY,
            tgt.CREATED_FROM = src.CREATED_FROM,
            tgt.CREATION_DATE = src.CREATION_DATE,
            tgt.CUSTOMER_CONTRACT_DATE = src.CUSTOMER_CONTRACT_DATE,
            tgt.CUSTOMER_CONTRACT_FREEZE_DATE = src.CUSTOMER_CONTRACT_FREEZE_DATE,
            tgt.EXCHANGE_RATE_DATE = src.EXCHANGE_RATE_DATE,
            tgt.EXCHANGE_RATE_TYPE = src.EXCHANGE_RATE_TYPE,
            tgt.LAST_UPDATE_DATE = src.LAST_UPDATE_DATE,
            tgt.LAST_UPDATED_BY = src.LAST_UPDATED_BY,
            tgt.LAST_UPDATE_LOGIN = src.LAST_UPDATE_LOGIN,
            tgt.LEGAL_ENTITY_ID = src.LEGAL_ENTITY_ID,
            tgt.OBJECT_VERSION_NUMBER = src.OBJECT_VERSION_NUMBER,
            tgt.CONTRACT_REFERENCE = src.CONTRACT_REFERENCE,
            tgt.REVIEW_STATUS = src.REVIEW_STATUS,
            tgt.SINGLE_OBLIGATION_FLAG = src.SINGLE_OBLIGATION_FLAG,
            tgt.STANDALONE_SALES_FLAG = src.STANDALONE_SALES_FLAG,
            tgt.ADJUSTMENT_STATUS_CODE = src.ADJUSTMENT_STATUS_CODE,
            tgt.CONTRACT_GROUP_NUMBER = src.CONTRACT_GROUP_NUMBER,
            tgt.CONTRACT_CREATED_BY = src.CONTRACT_CREATED_BY,
            tgt.EXCL_FROM_AUTO_WRITEOFF_FLAG = src.EXCL_FROM_AUTO_WRITEOFF_FLAG,
            tgt.FULL_SATISFACTION_DATE = src.FULL_SATISFACTION_DATE,
            tgt.LAST_ACTIVITY_DATE = src.LAST_ACTIVITY_DATE,
            tgt.CONTRACT_LAST_UPDATED_BY = src.CONTRACT_LAST_UPDATED_BY,
            tgt.LATEST_IMMATERIAL_CHANGE_CODE = src.LATEST_IMMATERIAL_CHANGE_CODE,
            tgt.LATEST_REVISION_INTENT_CODE = src.LATEST_REVISION_INTENT_CODE,
            tgt.LATEST_VERSION_DATE = src.LATEST_VERSION_DATE,
            tgt.SATISFACTION_STATUS = src.SATISFACTION_STATUS,
            tgt.BZ_LOAD_DATE = src.BZ_LOAD_DATE,
            tgt.SV_LOAD_DATE = src.SV_LOAD_DATE
        WHEN NOT MATCHED BY TARGET THEN INSERT (
            CUSTOMER_CONTRACT_HEADER_ID, CONTRACT_CURRENCY_CODE, CUSTOMER_CONTRACT_NUMBER, EFFECTIVITY_PERIOD_ID, LEDGER_ID,
            ALLOCATION_PENDING_REASON, ALLOCATION_REQUEST_ID, ALLOCATION_STATUS, ATTRIBUTE1, ATTRIBUTE_CATEGORY, CONTRACT_CLASSIFICATION_CODE, CONTRACT_RULE_ID,
            CONTR_TOTAL_BILLED_AMT, CONTR_TOTAL_RECOG_REV_AMT, CONTR_TRANSACTION_PRICE, CREATED_BY, CREATED_FROM, CREATION_DATE, CUSTOMER_CONTRACT_DATE, CUSTOMER_CONTRACT_FREEZE_DATE,
            EXCHANGE_RATE_DATE, EXCHANGE_RATE_TYPE, LAST_UPDATE_DATE, LAST_UPDATED_BY, LAST_UPDATE_LOGIN, LEGAL_ENTITY_ID, OBJECT_VERSION_NUMBER,
            CONTRACT_REFERENCE, REVIEW_STATUS, SINGLE_OBLIGATION_FLAG, STANDALONE_SALES_FLAG, ADJUSTMENT_STATUS_CODE, CONTRACT_GROUP_NUMBER, CONTRACT_CREATED_BY,
            EXCL_FROM_AUTO_WRITEOFF_FLAG, FULL_SATISFACTION_DATE, LAST_ACTIVITY_DATE, CONTRACT_LAST_UPDATED_BY, LATEST_IMMATERIAL_CHANGE_CODE, LATEST_REVISION_INTENT_CODE, LATEST_VERSION_DATE, SATISFACTION_STATUS,
            BZ_LOAD_DATE, SV_LOAD_DATE
        ) VALUES (
            src.CUSTOMER_CONTRACT_HEADER_ID, src.CONTRACT_CURRENCY_CODE, src.CUSTOMER_CONTRACT_NUMBER, src.EFFECTIVITY_PERIOD_ID, src.LEDGER_ID,
            src.ALLOCATION_PENDING_REASON, src.ALLOCATION_REQUEST_ID, src.ALLOCATION_STATUS, src.ATTRIBUTE1, src.ATTRIBUTE_CATEGORY, src.CONTRACT_CLASSIFICATION_CODE, src.CONTRACT_RULE_ID,
            src.CONTR_TOTAL_BILLED_AMT, src.CONTR_TOTAL_RECOG_REV_AMT, src.CONTR_TRANSACTION_PRICE, src.CREATED_BY, src.CREATED_FROM, src.CREATION_DATE, src.CUSTOMER_CONTRACT_DATE, src.CUSTOMER_CONTRACT_FREEZE_DATE,
            src.EXCHANGE_RATE_DATE, src.EXCHANGE_RATE_TYPE, src.LAST_UPDATE_DATE, src.LAST_UPDATED_BY, src.LAST_UPDATE_LOGIN, src.LEGAL_ENTITY_ID, src.OBJECT_VERSION_NUMBER,
            src.CONTRACT_REFERENCE, src.REVIEW_STATUS, src.SINGLE_OBLIGATION_FLAG, src.STANDALONE_SALES_FLAG, src.ADJUSTMENT_STATUS_CODE, src.CONTRACT_GROUP_NUMBER, src.CONTRACT_CREATED_BY,
            src.EXCL_FROM_AUTO_WRITEOFF_FLAG, src.FULL_SATISFACTION_DATE, src.LAST_ACTIVITY_DATE, src.CONTRACT_LAST_UPDATED_BY, src.LATEST_IMMATERIAL_CHANGE_CODE, src.LATEST_REVISION_INTENT_CODE, src.LATEST_VERSION_DATE, src.SATISFACTION_STATUS,
            src.BZ_LOAD_DATE, src.SV_LOAD_DATE
        )
        OUTPUT $action INTO @MergeActions(ActionTaken);

        SELECT @RowInserted = SUM(CASE WHEN ActionTaken = 'INSERT' THEN 1 ELSE 0 END),
               @RowUpdated  = SUM(CASE WHEN ActionTaken = 'UPDATE' THEN 1 ELSE 0 END)
        FROM @MergeActions;

        IF @MaxWatermark IS NOT NULL
            UPDATE etl.ETL_WATERMARK SET LAST_WATERMARK = @MaxWatermark, UDT_DATE = SYSDATETIME() WHERE TABLE_NAME = @TargetObject;

        SET @EndDttm = SYSDATETIME();
        UPDATE etl.ETL_RUN SET END_DTTM = @EndDttm, STATUS = 'SUCCESS', ROW_INSERTED = @RowInserted, ROW_UPDATED = @RowUpdated, ROW_EXPIRED = 0, ERROR_MESSAGE = NULL WHERE RUN_ID = @RunId;
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
