CREATE OR ALTER PROCEDURE svo.usp_Load_D_RM_CONTRACT_SCD2
      @FullReload bit = 0
    , @Debug      bit = 0
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    DECLARE
          @ProcName      sysname       = QUOTENAME(OBJECT_SCHEMA_NAME(@@PROCID)) + '.' + QUOTENAME(OBJECT_NAME(@@PROCID))
        , @Target        sysname       = N'svo.D_RM_CONTRACT'
        , @RunId         bigint
        , @AsOfDate      date          = CAST(GETDATE() AS date)
        , @Now           datetime2(0)   = SYSDATETIME()
        , @RowInserted   int           = 0
        , @RowExpired    int           = 0
        , @RowUpdatedT1  int           = 0;

    BEGIN TRY
        INSERT etl.ETL_RUN (PROC_NAME, TARGET_OBJECT, ASOF_DATE, START_DTTM, STATUS)
        VALUES (@ProcName, @Target, @AsOfDate, @Now, 'STARTED');

        SET @RunId = SCOPE_IDENTITY();

        IF @Debug = 1
            PRINT 'Starting ' + @ProcName + ' | RUN_ID=' + CONVERT(varchar(30), @RunId);

        BEGIN TRAN;

        /* Full reload = rebuild all history as a single current slice (EFF_DATE = 0001-01-01) */
        IF @FullReload = 1
        BEGIN
            IF @Debug = 1
                PRINT 'FullReload requested. Rebuilding ' + @Target;

            TRUNCATE TABLE svo.D_RM_CONTRACT;
        END

        /* Stage source with strong NULL handling + deterministic hash */
        IF OBJECT_ID('tempdb..#Src') IS NOT NULL DROP TABLE #Src;

        SELECT
              CUSTOMER_CONTRACT_HEADER_ID   = C.CustomerContractHeaderId

            , CONTRACT_CURRENCY_CODE        = ISNULL(C.CustContHeadersContractCurrencyCode, 'UNK')
            , CUSTOMER_CONTRACT_NUMBER      = ISNULL(C.CustContHeadersCustomerContractNumber, 0)
            , EFFECTIVITY_PERIOD_ID         = ISNULL(C.CustContHeadersEffectivityPeriodId, 0)
            , LEDGER_ID                     = ISNULL(C.CustContHeadersLedgerId, 0)

            , ALLOCATION_PENDING_REASON     = ISNULL(C.CustContHeadersAllocationPendingReason, 'Unknown')
            , ALLOCATION_REQUEST_ID         = ISNULL(C.CustContHeadersAllocationRequestId, 0)
            , ALLOCATION_STATUS             = ISNULL(C.CustContHeadersAllocationStatus, 'Unknown')
            , ATTRIBUTE1                    = ISNULL(C.CustContHeadersAttribute1, 'Unknown')
            , ATTRIBUTE_CATEGORY            = ISNULL(C.CustContHeadersAttributeCategory, 'Unknown')
            , CONTRACT_CLASSIFICATION_CODE  = ISNULL(C.CustContHeadersContractClassificationCode, 'Unknown')
            , CONTRACT_RULE_ID              = ISNULL(C.CustContHeadersContractRuleId, 0)
            , CONTR_TOTAL_BILLED_AMT        = ISNULL(C.CustContHeadersContrCurTotalBilledAmt, 0)
            , CONTR_TOTAL_RECOG_REV_AMT     = ISNULL(C.CustContHeadersContrCurTotalRecogRevAmt, 0)
            , CONTR_TRANSACTION_PRICE       = ISNULL(C.CustContHeadersContrCurTransactionPrice, 0)

            , CREATED_BY                    = ISNULL(C.CustContHeadersCreatedBy, 'Unknown')
            , CREATED_FROM                  = ISNULL(C.CustContHeadersCreatedFrom, 'Unknown')
            , CREATION_DATE                 = ISNULL(CAST(C.CustContHeadersCreationDate AS date), CAST('0001-01-01' AS date))
            , CUSTOMER_CONTRACT_DATE        = ISNULL(CAST(C.CustContHeadersCustomerContractDate AS date), CAST('0001-01-01' AS date))
            , CUSTOMER_CONTRACT_FREEZE_DATE = ISNULL(CAST(C.CustContHeadersCustomerContractFreezeDate AS date), CAST('0001-01-01' AS date))

            , EXCHANGE_RATE_DATE            = ISNULL(CAST(C.CustContHeadersExchangeRateDate AS date), CAST('0001-01-01' AS date))
            , EXCHANGE_RATE_TYPE            = ISNULL(C.CustContHeadersExchangeRateType, 'Unknown')

            , LAST_UPDATE_DATE              = ISNULL(CAST(C.CustContHeadersLastUpdateDate AS date), CAST('0001-01-01' AS date))
            , LAST_UPDATED_BY               = ISNULL(C.CustContHeadersLastUpdatedBy, 'Unknown')
            , LAST_UPDATE_LOGIN             = ISNULL(C.CustContHeadersLastUpdateLogin, 'Unknown')

            , LEGAL_ENTITY_ID               = ISNULL(C.CustContHeadersLegalEntityId, 0)
            , OBJECT_VERSION_NUMBER         = ISNULL(C.CustContHeadersObjectVersionNumber, 0)

            , CONTRACT_REFERENCE            = ISNULL(C.CustContHeadersReference, 'Unknown')
            , REVIEW_STATUS                 = ISNULL(C.CustContHeadersReviewStatus, 'Unknown')
            , SINGLE_OBLIGATION_FLAG        = ISNULL(C.CustContHeadersSingleObligationFlag, 'U')
            , STANDALONE_SALES_FLAG         = ISNULL(C.CustContHeadersStandaloneSalesFlag, 'U')

            , ADJUSTMENT_STATUS_CODE        = ISNULL(C.CustomerContractHeadersAdjustmentStatusCode, 'Unknown')
            , CONTRACT_GROUP_NUMBER         = ISNULL(C.CustomerContractHeadersContractGroupNumber, 'Unknown')
            , CONTRACT_CREATED_BY           = ISNULL(C.CustomerContractHeadersCreatedBy, 'Unknown')
            , EXCL_FROM_AUTO_WRITEOFF_FLAG  = ISNULL(C.CustomerContractHeadersExclFromAutoWriteoffFlag, 'U')
            , FULL_SATISFACTION_DATE        = ISNULL(CAST(C.CustomerContractHeadersFullSatisfactionDate AS date), CAST('0001-01-01' AS date))
            , LAST_ACTIVITY_DATE            = ISNULL(CAST(C.CustomerContractHeadersLastActivityDate AS date), CAST('0001-01-01' AS date))
            , CONTRACT_LAST_UPDATED_BY      = ISNULL(C.CustomerContractHeadersLastUpdatedBy, 'Unknown')
            , LATEST_IMMATERIAL_CHANGE_CODE = ISNULL(C.CustomerContractHeadersLatestImmaterialChangeCode, 'Unknown')
            , LATEST_REVISION_INTENT_CODE   = ISNULL(C.CustomerContractHeadersLatestRevisionIntentCode, 'Unknown')
            , LATEST_VERSION_DATE           = ISNULL(CAST(C.CustomerContractHeadersLatestVersionDate AS date), CAST('0001-01-01' AS date))
            , SATISFACTION_STATUS           = ISNULL(C.CustomerContractHeadersSatisfactionStatus, 'Unknown')

            , BZ_LOAD_DATE                  = COALESCE(CAST(C.AddDateTime AS date), CAST(GETDATE() AS date))
            , SV_LOAD_DATE                  = CAST(GETDATE() AS date)

            , ROW_HASH = CONVERT(varbinary(32), HASHBYTES
              (
                'SHA2_256',
                CONCAT(
                  ISNULL(CONVERT(varchar(30), C.CustomerContractHeaderId), '-1'), '|',
                  ISNULL(C.CustContHeadersContractCurrencyCode, 'UNK'), '|',
                  ISNULL(CONVERT(varchar(30), C.CustContHeadersCustomerContractNumber), '0'), '|',
                  ISNULL(CONVERT(varchar(30), C.CustContHeadersEffectivityPeriodId), '0'), '|',
                  ISNULL(CONVERT(varchar(30), C.CustContHeadersLedgerId), '0'), '|',
                  ISNULL(C.CustContHeadersAllocationPendingReason, 'Unknown'), '|',
                  ISNULL(CONVERT(varchar(30), C.CustContHeadersAllocationRequestId), '0'), '|',
                  ISNULL(C.CustContHeadersAllocationStatus, 'Unknown'), '|',
                  ISNULL(C.CustContHeadersAttribute1, 'Unknown'), '|',
                  ISNULL(C.CustContHeadersAttributeCategory, 'Unknown'), '|',
                  ISNULL(C.CustContHeadersContractClassificationCode, 'Unknown'), '|',
                  ISNULL(CONVERT(varchar(30), C.CustContHeadersContractRuleId), '0'), '|',
                  ISNULL(CONVERT(varchar(60), C.CustContHeadersContrCurTotalBilledAmt), '0'), '|',
                  ISNULL(CONVERT(varchar(60), C.CustContHeadersContrCurTotalRecogRevAmt), '0'), '|',
                  ISNULL(CONVERT(varchar(60), C.CustContHeadersContrCurTransactionPrice), '0'), '|',
                  ISNULL(C.CustContHeadersCreatedBy, 'Unknown'), '|',
                  ISNULL(C.CustContHeadersCreatedFrom, 'Unknown'), '|',
                  ISNULL(CONVERT(char(10), CAST(C.CustContHeadersCreationDate AS date), 120), '0001-01-01'), '|',
                  ISNULL(CONVERT(char(10), CAST(C.CustContHeadersCustomerContractDate AS date), 120), '0001-01-01'), '|',
                  ISNULL(CONVERT(char(10), CAST(C.CustContHeadersCustomerContractFreezeDate AS date), 120), '0001-01-01'), '|',
                  ISNULL(CONVERT(char(10), CAST(C.CustContHeadersExchangeRateDate AS date), 120), '0001-01-01'), '|',
                  ISNULL(C.CustContHeadersExchangeRateType, 'Unknown'), '|',
                  ISNULL(CONVERT(char(10), CAST(C.CustContHeadersLastUpdateDate AS date), 120), '0001-01-01'), '|',
                  ISNULL(C.CustContHeadersLastUpdatedBy, 'Unknown'), '|',
                  ISNULL(C.CustContHeadersLastUpdateLogin, 'Unknown'), '|',
                  ISNULL(CONVERT(varchar(30), C.CustContHeadersLegalEntityId), '0'), '|',
                  ISNULL(CONVERT(varchar(30), C.CustContHeadersObjectVersionNumber), '0'), '|',
                  ISNULL(C.CustContHeadersReference, 'Unknown'), '|',
                  ISNULL(C.CustContHeadersReviewStatus, 'Unknown'), '|',
                  ISNULL(C.CustContHeadersSingleObligationFlag, 'U'), '|',
                  ISNULL(C.CustContHeadersStandaloneSalesFlag, 'U'), '|',
                  ISNULL(C.CustomerContractHeadersAdjustmentStatusCode, 'Unknown'), '|',
                  ISNULL(C.CustomerContractHeadersContractGroupNumber, 'Unknown'), '|',
                  ISNULL(C.CustomerContractHeadersCreatedBy, 'Unknown'), '|',
                  ISNULL(C.CustomerContractHeadersExclFromAutoWriteoffFlag, 'U'), '|',
                  ISNULL(CONVERT(char(10), CAST(C.CustomerContractHeadersFullSatisfactionDate AS date), 120), '0001-01-01'), '|',
                  ISNULL(CONVERT(char(10), CAST(C.CustomerContractHeadersLastActivityDate AS date), 120), '0001-01-01'), '|',
                  ISNULL(C.CustomerContractHeadersLastUpdatedBy, 'Unknown'), '|',
                  ISNULL(C.CustomerContractHeadersLatestImmaterialChangeCode, 'Unknown'), '|',
                  ISNULL(C.CustomerContractHeadersLatestRevisionIntentCode, 'Unknown'), '|',
                  ISNULL(CONVERT(char(10), CAST(C.CustomerContractHeadersLatestVersionDate AS date), 120), '0001-01-01'), '|',
                  ISNULL(C.CustomerContractHeadersSatisfactionStatus, 'Unknown')
                )
              ))
        INTO #Src
        FROM src.bzo_VRM_CustomerContractHeadersPVO C
        WHERE C.CustomerContractHeaderId IS NOT NULL;

        /* Deduplicate source per business key (keep latest AddDateTime) */
        IF OBJECT_ID('tempdb..#Src1') IS NOT NULL DROP TABLE #Src1;

        ;WITH d AS
        (
            SELECT
                  s.*
                , rn = ROW_NUMBER() OVER
                    (PARTITION BY s.CUSTOMER_CONTRACT_HEADER_ID
                     ORDER BY TRY_CONVERT(datetime2(7), s.BZ_LOAD_DATE) DESC)
            FROM #Src s
        )
        SELECT *
        INTO #Src1
        FROM d
        WHERE rn = 1;

        /* Ensure plug row exists (SK=0) */
        IF NOT EXISTS (SELECT 1 FROM svo.D_RM_CONTRACT WHERE RM_CONTRACT_SK = 0)
        BEGIN
            SET IDENTITY_INSERT svo.D_RM_CONTRACT ON;

            INSERT INTO svo.D_RM_CONTRACT
            (
                  RM_CONTRACT_SK
                , CUSTOMER_CONTRACT_HEADER_ID
                , CONTRACT_CURRENCY_CODE
                , CUSTOMER_CONTRACT_NUMBER
                , EFFECTIVITY_PERIOD_ID
                , LEDGER_ID
                , ALLOCATION_PENDING_REASON
                , ALLOCATION_REQUEST_ID
                , ALLOCATION_STATUS
                , ATTRIBUTE1
                , ATTRIBUTE_CATEGORY
                , CONTRACT_CLASSIFICATION_CODE
                , CONTRACT_RULE_ID
                , CONTR_TOTAL_BILLED_AMT
                , CONTR_TOTAL_RECOG_REV_AMT
                , CONTR_TRANSACTION_PRICE
                , CREATED_BY
                , CREATED_FROM
                , CREATION_DATE
                , CUSTOMER_CONTRACT_DATE
                , CUSTOMER_CONTRACT_FREEZE_DATE
                , EXCHANGE_RATE_DATE
                , EXCHANGE_RATE_TYPE
                , LAST_UPDATE_DATE
                , LAST_UPDATED_BY
                , LAST_UPDATE_LOGIN
                , LEGAL_ENTITY_ID
                , OBJECT_VERSION_NUMBER
                , CONTRACT_REFERENCE
                , REVIEW_STATUS
                , SINGLE_OBLIGATION_FLAG
                , STANDALONE_SALES_FLAG
                , ADJUSTMENT_STATUS_CODE
                , CONTRACT_GROUP_NUMBER
                , CONTRACT_CREATED_BY
                , EXCL_FROM_AUTO_WRITEOFF_FLAG
                , FULL_SATISFACTION_DATE
                , LAST_ACTIVITY_DATE
                , CONTRACT_LAST_UPDATED_BY
                , LATEST_IMMATERIAL_CHANGE_CODE
                , LATEST_REVISION_INTENT_CODE
                , LATEST_VERSION_DATE
                , SATISFACTION_STATUS
                , BZ_LOAD_DATE
                , SV_LOAD_DATE
                , EFF_DATE
                , END_DATE
                , CRE_DATE
                , UDT_DATE
                , CURR_IND
                , ROW_HASH
            )
            VALUES
            (
                  0
                , -1
                , 'UNK'
                , 0
                , 0
                , 0
                , 'Unknown'
                , 0
                , 'Unknown'
                , 'Unknown'
                , 'Unknown'
                , 'Unknown'
                , 0
                , 0
                , 0
                , 0
                , 'Unknown'
                , 'Unknown'
                , CAST('0001-01-01' AS date)
                , CAST('0001-01-01' AS date)
                , CAST('0001-01-01' AS date)
                , CAST('0001-01-01' AS date)
                , 'Unknown'
                , CAST('0001-01-01' AS date)
                , 'Unknown'
                , 'Unknown'
                , 0
                , 0
                , 'Unknown'
                , 'Unknown'
                , 'U'
                , 'U'
                , 'Unknown'
                , 'Unknown'
                , 'Unknown'
                , 'U'
                , CAST('0001-01-01' AS date)
                , CAST('0001-01-01' AS date)
                , 'Unknown'
                , 'Unknown'
                , 'Unknown'
                , CAST('0001-01-01' AS date)
                , 'Unknown'
                , CAST('0001-01-01' AS date)
                , CAST(GETDATE() AS date)
                , CAST('0001-01-01' AS date)
                , CAST('9999-12-31' AS date)
                , @Now
                , @Now
                , 1
                , 0x00
            );

            SET IDENTITY_INSERT svo.D_RM_CONTRACT OFF;
        END

        /* Full reload path: insert all as current in one shot (EFF_DATE = 0001-01-01) */
        IF @FullReload = 1
        BEGIN
            INSERT INTO svo.D_RM_CONTRACT
            (
                  CUSTOMER_CONTRACT_HEADER_ID
                , CONTRACT_CURRENCY_CODE
                , CUSTOMER_CONTRACT_NUMBER
                , EFFECTIVITY_PERIOD_ID
                , LEDGER_ID
                , ALLOCATION_PENDING_REASON
                , ALLOCATION_REQUEST_ID
                , ALLOCATION_STATUS
                , ATTRIBUTE1
                , ATTRIBUTE_CATEGORY
                , CONTRACT_CLASSIFICATION_CODE
                , CONTRACT_RULE_ID
                , CONTR_TOTAL_BILLED_AMT
                , CONTR_TOTAL_RECOG_REV_AMT
                , CONTR_TRANSACTION_PRICE
                , CREATED_BY
                , CREATED_FROM
                , CREATION_DATE
                , CUSTOMER_CONTRACT_DATE
                , CUSTOMER_CONTRACT_FREEZE_DATE
                , EXCHANGE_RATE_DATE
                , EXCHANGE_RATE_TYPE
                , LAST_UPDATE_DATE
                , LAST_UPDATED_BY
                , LAST_UPDATE_LOGIN
                , LEGAL_ENTITY_ID
                , OBJECT_VERSION_NUMBER
                , CONTRACT_REFERENCE
                , REVIEW_STATUS
                , SINGLE_OBLIGATION_FLAG
                , STANDALONE_SALES_FLAG
                , ADJUSTMENT_STATUS_CODE
                , CONTRACT_GROUP_NUMBER
                , CONTRACT_CREATED_BY
                , EXCL_FROM_AUTO_WRITEOFF_FLAG
                , FULL_SATISFACTION_DATE
                , LAST_ACTIVITY_DATE
                , CONTRACT_LAST_UPDATED_BY
                , LATEST_IMMATERIAL_CHANGE_CODE
                , LATEST_REVISION_INTENT_CODE
                , LATEST_VERSION_DATE
                , SATISFACTION_STATUS
                , BZ_LOAD_DATE
                , SV_LOAD_DATE
                , EFF_DATE
                , END_DATE
                , CRE_DATE
                , UDT_DATE
                , CURR_IND
                , ROW_HASH
            )
            SELECT
                  s.CUSTOMER_CONTRACT_HEADER_ID
                , s.CONTRACT_CURRENCY_CODE
                , s.CUSTOMER_CONTRACT_NUMBER
                , s.EFFECTIVITY_PERIOD_ID
                , s.LEDGER_ID
                , s.ALLOCATION_PENDING_REASON
                , s.ALLOCATION_REQUEST_ID
                , s.ALLOCATION_STATUS
                , s.ATTRIBUTE1
                , s.ATTRIBUTE_CATEGORY
                , s.CONTRACT_CLASSIFICATION_CODE
                , s.CONTRACT_RULE_ID
                , s.CONTR_TOTAL_BILLED_AMT
                , s.CONTR_TOTAL_RECOG_REV_AMT
                , s.CONTR_TRANSACTION_PRICE
                , s.CREATED_BY
                , s.CREATED_FROM
                , s.CREATION_DATE
                , s.CUSTOMER_CONTRACT_DATE
                , s.CUSTOMER_CONTRACT_FREEZE_DATE
                , s.EXCHANGE_RATE_DATE
                , s.EXCHANGE_RATE_TYPE
                , s.LAST_UPDATE_DATE
                , s.LAST_UPDATED_BY
                , s.LAST_UPDATE_LOGIN
                , s.LEGAL_ENTITY_ID
                , s.OBJECT_VERSION_NUMBER
                , s.CONTRACT_REFERENCE
                , s.REVIEW_STATUS
                , s.SINGLE_OBLIGATION_FLAG
                , s.STANDALONE_SALES_FLAG
                , s.ADJUSTMENT_STATUS_CODE
                , s.CONTRACT_GROUP_NUMBER
                , s.CONTRACT_CREATED_BY
                , s.EXCL_FROM_AUTO_WRITEOFF_FLAG
                , s.FULL_SATISFACTION_DATE
                , s.LAST_ACTIVITY_DATE
                , s.CONTRACT_LAST_UPDATED_BY
                , s.LATEST_IMMATERIAL_CHANGE_CODE
                , s.LATEST_REVISION_INTENT_CODE
                , s.LATEST_VERSION_DATE
                , s.SATISFACTION_STATUS
                , s.BZ_LOAD_DATE
                , s.SV_LOAD_DATE
                , CAST('0001-01-01' AS date)     AS EFF_DATE
                , CAST('9999-12-31' AS date)     AS END_DATE
                , @Now                           AS CRE_DATE
                , @Now                           AS UDT_DATE
                , 1                              AS CURR_IND
                , s.ROW_HASH
            FROM #Src1 s
            WHERE s.CUSTOMER_CONTRACT_HEADER_ID <> -1;

            SET @RowInserted = @@ROWCOUNT;

            COMMIT TRAN;

            UPDATE etl.ETL_RUN
                SET END_DTTM = SYSDATETIME(),
                    STATUS = 'SUCCEEDED',
                    ROW_INSERTED = @RowInserted,
                    ROW_EXPIRED = 0,
                    ROW_UPDATED_T1 = 0,
                    ERROR_MESSAGE = NULL
            WHERE RUN_ID = @RunId;

            IF @Debug = 1
                PRINT 'Completed ' + @ProcName + ' | inserted=' + CONVERT(varchar(30), @RowInserted);

            RETURN;
        END

        /* Incremental SCD2: expire changed + insert new current */
        IF OBJECT_ID('tempdb..#ToExpire') IS NOT NULL DROP TABLE #ToExpire;
        SELECT
            t.RM_CONTRACT_SK
        INTO #ToExpire
        FROM svo.D_RM_CONTRACT t
        JOIN #Src1 s
          ON s.CUSTOMER_CONTRACT_HEADER_ID = t.CUSTOMER_CONTRACT_HEADER_ID
        WHERE t.CURR_IND = 1
          AND t.CUSTOMER_CONTRACT_HEADER_ID <> -1
          AND t.ROW_HASH <> s.ROW_HASH;

        UPDATE t
            SET t.CURR_IND = 0,
                t.END_DATE = DATEADD(day, -1, @AsOfDate),
                t.UDT_DATE = @Now
        FROM svo.D_RM_CONTRACT t
        JOIN #ToExpire e
          ON e.RM_CONTRACT_SK = t.RM_CONTRACT_SK;

        SET @RowExpired = @@ROWCOUNT;

        IF OBJECT_ID('tempdb..#ToInsert') IS NOT NULL DROP TABLE #ToInsert;
        SELECT s.*
        INTO #ToInsert
        FROM #Src1 s
        LEFT JOIN svo.D_RM_CONTRACT t
          ON t.CUSTOMER_CONTRACT_HEADER_ID = s.CUSTOMER_CONTRACT_HEADER_ID
         AND t.CURR_IND = 1
        WHERE s.CUSTOMER_CONTRACT_HEADER_ID <> -1
          AND (
                t.CUSTOMER_CONTRACT_HEADER_ID IS NULL   -- brand new
             OR t.ROW_HASH <> s.ROW_HASH                -- changed (expired above)
          );

        INSERT INTO svo.D_RM_CONTRACT
        (
              CUSTOMER_CONTRACT_HEADER_ID
            , CONTRACT_CURRENCY_CODE
            , CUSTOMER_CONTRACT_NUMBER
            , EFFECTIVITY_PERIOD_ID
            , LEDGER_ID
            , ALLOCATION_PENDING_REASON
            , ALLOCATION_REQUEST_ID
            , ALLOCATION_STATUS
            , ATTRIBUTE1
            , ATTRIBUTE_CATEGORY
            , CONTRACT_CLASSIFICATION_CODE
            , CONTRACT_RULE_ID
            , CONTR_TOTAL_BILLED_AMT
            , CONTR_TOTAL_RECOG_REV_AMT
            , CONTR_TRANSACTION_PRICE
            , CREATED_BY
            , CREATED_FROM
            , CREATION_DATE
            , CUSTOMER_CONTRACT_DATE
            , CUSTOMER_CONTRACT_FREEZE_DATE
            , EXCHANGE_RATE_DATE
            , EXCHANGE_RATE_TYPE
            , LAST_UPDATE_DATE
            , LAST_UPDATED_BY
            , LAST_UPDATE_LOGIN
            , LEGAL_ENTITY_ID
            , OBJECT_VERSION_NUMBER
            , CONTRACT_REFERENCE
            , REVIEW_STATUS
            , SINGLE_OBLIGATION_FLAG
            , STANDALONE_SALES_FLAG
            , ADJUSTMENT_STATUS_CODE
            , CONTRACT_GROUP_NUMBER
            , CONTRACT_CREATED_BY
            , EXCL_FROM_AUTO_WRITEOFF_FLAG
            , FULL_SATISFACTION_DATE
            , LAST_ACTIVITY_DATE
            , CONTRACT_LAST_UPDATED_BY
            , LATEST_IMMATERIAL_CHANGE_CODE
            , LATEST_REVISION_INTENT_CODE
            , LATEST_VERSION_DATE
            , SATISFACTION_STATUS
            , BZ_LOAD_DATE
            , SV_LOAD_DATE
            , EFF_DATE
            , END_DATE
            , CRE_DATE
            , UDT_DATE
            , CURR_IND
            , ROW_HASH
        )
        SELECT
              s.CUSTOMER_CONTRACT_HEADER_ID
            , s.CONTRACT_CURRENCY_CODE
            , s.CUSTOMER_CONTRACT_NUMBER
            , s.EFFECTIVITY_PERIOD_ID
            , s.LEDGER_ID
            , s.ALLOCATION_PENDING_REASON
            , s.ALLOCATION_REQUEST_ID
            , s.ALLOCATION_STATUS
            , s.ATTRIBUTE1
            , s.ATTRIBUTE_CATEGORY
            , s.CONTRACT_CLASSIFICATION_CODE
            , s.CONTRACT_RULE_ID
            , s.CONTR_TOTAL_BILLED_AMT
            , s.CONTR_TOTAL_RECOG_REV_AMT
            , s.CONTR_TRANSACTION_PRICE
            , s.CREATED_BY
            , s.CREATED_FROM
            , s.CREATION_DATE
            , s.CUSTOMER_CONTRACT_DATE
            , s.CUSTOMER_CONTRACT_FREEZE_DATE
            , s.EXCHANGE_RATE_DATE
            , s.EXCHANGE_RATE_TYPE
            , s.LAST_UPDATE_DATE
            , s.LAST_UPDATED_BY
            , s.LAST_UPDATE_LOGIN
            , s.LEGAL_ENTITY_ID
            , s.OBJECT_VERSION_NUMBER
            , s.CONTRACT_REFERENCE
            , s.REVIEW_STATUS
            , s.SINGLE_OBLIGATION_FLAG
            , s.STANDALONE_SALES_FLAG
            , s.ADJUSTMENT_STATUS_CODE
            , s.CONTRACT_GROUP_NUMBER
            , s.CONTRACT_CREATED_BY
            , s.EXCL_FROM_AUTO_WRITEOFF_FLAG
            , s.FULL_SATISFACTION_DATE
            , s.LAST_ACTIVITY_DATE
            , s.CONTRACT_LAST_UPDATED_BY
            , s.LATEST_IMMATERIAL_CHANGE_CODE
            , s.LATEST_REVISION_INTENT_CODE
            , s.LATEST_VERSION_DATE
            , s.SATISFACTION_STATUS
            , s.BZ_LOAD_DATE
            , s.SV_LOAD_DATE
            , @AsOfDate                    AS EFF_DATE
            , CAST('9999-12-31' AS date)   AS END_DATE
            , @Now                         AS CRE_DATE
            , @Now                         AS UDT_DATE
            , 1                            AS CURR_IND
            , s.ROW_HASH
        FROM #ToInsert s;

        SET @RowInserted = @@ROWCOUNT;

        COMMIT TRAN;

        UPDATE etl.ETL_RUN
            SET END_DTTM = SYSDATETIME(),
                STATUS = 'SUCCEEDED',
                ROW_INSERTED = @RowInserted,
                ROW_EXPIRED = @RowExpired,
                ROW_UPDATED_T1 = @RowUpdatedT1,
                ERROR_MESSAGE = NULL
        WHERE RUN_ID = @RunId;

        IF @Debug = 1
            PRINT 'Completed ' + @ProcName
                + ' | expired=' + CONVERT(varchar(30), @RowExpired)
                + ' | inserted=' + CONVERT(varchar(30), @RowInserted);

    END TRY
    BEGIN CATCH
        IF XACT_STATE() <> 0 ROLLBACK TRAN;

        DECLARE @ErrMsg nvarchar(4000) = ERROR_MESSAGE();

        IF @RunId IS NOT NULL
        BEGIN
            UPDATE etl.ETL_RUN
                SET END_DTTM = SYSDATETIME(),
                    STATUS = 'FAILED',
                    ROW_INSERTED = ISNULL(@RowInserted, 0),
                    ROW_EXPIRED = ISNULL(@RowExpired, 0),
                    ROW_UPDATED_T1 = ISNULL(@RowUpdatedT1, 0),
                    ERROR_MESSAGE = @ErrMsg
            WHERE RUN_ID = @RunId;
        END

        IF @Debug = 1
            PRINT 'FAILED ' + @ProcName + ' | ' + @ErrMsg;

        THROW;
    END CATCH
END
GO