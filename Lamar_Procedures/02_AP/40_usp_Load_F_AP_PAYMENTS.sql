/* =========================================================
   usp_Load_F_AP_PAYMENTS
   Incremental INSERT only. Sources: bzo.AP_InvoiceHeaderExtractPVO,
   AP_PaidDisbursementScheduleExtractPVO, AP_DisbursementHeaderExtractPVO,
   AP_InvoicePaymentScheduleExtractPVO.
   Filter: max(H.AddDateTime, S.AddDateTime, D.AddDateTime) > @LastWatermark.
   Dedupe by AP_INVOICE_PAYMENTS_ALL_INVOICE_PAYMENT_ID.
   Resolve SKs from svo.D_* (CURR_IND='Y' where SCD2), svo.LINES_CODE_COMBO_LOOKUP,
   svo.D_AP_INVOICE_HEADER, svo.D_AP_DISBURSEMENT_HEADER, svo.D_CURRENCY.
   ========================================================= */
CREATE OR ALTER PROCEDURE svo.usp_Load_F_AP_PAYMENTS
    @BatchId INT = NULL
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    DECLARE
        @ProcName       SYSNAME        = OBJECT_SCHEMA_NAME(@@PROCID) + '.' + OBJECT_NAME(@@PROCID),
        @TargetObject   SYSNAME        = 'svo.F_AP_PAYMENTS',
        @StartDttm      DATETIME2(0)   = SYSDATETIME(),
        @EndDttm        DATETIME2(0),
        @RunId          BIGINT         = NULL,
        @ErrMsg         NVARCHAR(4000) = NULL,
        @AsOfDate       DATE           = CAST(GETDATE() AS DATE),
        @LastWatermark  DATETIME2(7),
        @MaxWatermark   DATETIME2(7)   = NULL,
        @RowInserted    INT            = 0,
        @TableBridgeID  INT            = NULL;

    SELECT @TableBridgeID = TableBridgeID FROM meta.MedallionTableBridge WHERE targettable = N'AP_PaidDisbursementScheduleExtractPVO';

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

        IF OBJECT_ID('tempdb..#pay') IS NOT NULL DROP TABLE #pay;

        SELECT *
        INTO #pay
        FROM (
            SELECT D.ApInvoicePaymentsAllCheckId, D.ApInvoicePaymentsAllInvoiceId, D.ApInvoicePaymentsAllPaymentNum,
                D.ApInvoicePaymentsAllInvoicePaymentId, D.ApInvoicePaymentsAllAccountingDate, D.ApInvoicePaymentsAllExchangeDate,
                D.ApInvoicePaymentsAllInvoiceCurrencyCode, D.ApInvoicePaymentsAllPaymentCurrencyCode, D.ApInvoicePaymentsAllOrgId,
                D.ApInvoicePaymentsAllSetOfBooksId, D.ApInvoicePaymentsAllLastUpdateDate, D.ApInvoicePaymentsAllLastUpdatedBy,
                D.ApInvoicePaymentsAllPostedFlag, D.ApInvoicePaymentsAllAmount, D.ApInvoicePaymentsAllAmountInvCurr,
                D.AddDateTime AS PaidAddDateTime, H.AddDateTime AS HeaderAddDateTime, S.AddDateTime AS ScheduleAddDateTime
            FROM bzo.AP_PaidDisbursementScheduleExtractPVO D WITH (NOLOCK)
            LEFT JOIN bzo.AP_DisbursementHeaderExtractPVO H WITH (NOLOCK) ON H.ApChecksAllCheckId = D.ApInvoicePaymentsAllCheckId
            LEFT JOIN bzo.AP_InvoicePaymentScheduleExtractPVO S WITH (NOLOCK) ON S.ApPaymentSchedulesAllInvoiceId = D.ApInvoicePaymentsAllInvoiceId AND S.ApPaymentSchedulesAllPaymentNum = D.ApInvoicePaymentsAllPaymentNum
            WHERE D.AddDateTime > @LastWatermark
            UNION
            SELECT D.ApInvoicePaymentsAllCheckId, D.ApInvoicePaymentsAllInvoiceId, D.ApInvoicePaymentsAllPaymentNum,
                D.ApInvoicePaymentsAllInvoicePaymentId, D.ApInvoicePaymentsAllAccountingDate, D.ApInvoicePaymentsAllExchangeDate,
                D.ApInvoicePaymentsAllInvoiceCurrencyCode, D.ApInvoicePaymentsAllPaymentCurrencyCode, D.ApInvoicePaymentsAllOrgId,
                D.ApInvoicePaymentsAllSetOfBooksId, D.ApInvoicePaymentsAllLastUpdateDate, D.ApInvoicePaymentsAllLastUpdatedBy,
                D.ApInvoicePaymentsAllPostedFlag, D.ApInvoicePaymentsAllAmount, D.ApInvoicePaymentsAllAmountInvCurr,
                D.AddDateTime AS PaidAddDateTime, H.AddDateTime AS HeaderAddDateTime, S.AddDateTime AS ScheduleAddDateTime
            FROM bzo.AP_PaidDisbursementScheduleExtractPVO D WITH (NOLOCK)
            INNER JOIN bzo.AP_DisbursementHeaderExtractPVO H WITH (NOLOCK) ON H.ApChecksAllCheckId = D.ApInvoicePaymentsAllCheckId
            LEFT JOIN bzo.AP_InvoicePaymentScheduleExtractPVO S WITH (NOLOCK) ON S.ApPaymentSchedulesAllInvoiceId = D.ApInvoicePaymentsAllInvoiceId AND S.ApPaymentSchedulesAllPaymentNum = D.ApInvoicePaymentsAllPaymentNum
            WHERE H.AddDateTime > @LastWatermark
            UNION
            SELECT D.ApInvoicePaymentsAllCheckId, D.ApInvoicePaymentsAllInvoiceId, D.ApInvoicePaymentsAllPaymentNum,
                D.ApInvoicePaymentsAllInvoicePaymentId, D.ApInvoicePaymentsAllAccountingDate, D.ApInvoicePaymentsAllExchangeDate,
                D.ApInvoicePaymentsAllInvoiceCurrencyCode, D.ApInvoicePaymentsAllPaymentCurrencyCode, D.ApInvoicePaymentsAllOrgId,
                D.ApInvoicePaymentsAllSetOfBooksId, D.ApInvoicePaymentsAllLastUpdateDate, D.ApInvoicePaymentsAllLastUpdatedBy,
                D.ApInvoicePaymentsAllPostedFlag, D.ApInvoicePaymentsAllAmount, D.ApInvoicePaymentsAllAmountInvCurr,
                D.AddDateTime AS PaidAddDateTime, H.AddDateTime AS HeaderAddDateTime, S.AddDateTime AS ScheduleAddDateTime
            FROM bzo.AP_PaidDisbursementScheduleExtractPVO D WITH (NOLOCK)
            LEFT JOIN bzo.AP_DisbursementHeaderExtractPVO H WITH (NOLOCK) ON H.ApChecksAllCheckId = D.ApInvoicePaymentsAllCheckId
            INNER JOIN bzo.AP_InvoicePaymentScheduleExtractPVO S WITH (NOLOCK) ON S.ApPaymentSchedulesAllInvoiceId = D.ApInvoicePaymentsAllInvoiceId AND S.ApPaymentSchedulesAllPaymentNum = D.ApInvoicePaymentsAllPaymentNum
            WHERE S.AddDateTime > @LastWatermark
        ) x;

        -- Dedupe by invoice payment id (keep one row per payment)
        IF OBJECT_ID('tempdb..#pay_one') IS NOT NULL DROP TABLE #pay_one;
        SELECT * INTO #pay_one FROM (
            SELECT *,
                ROW_NUMBER() OVER (PARTITION BY ApInvoicePaymentsAllInvoicePaymentId ORDER BY (SELECT MAX(v) FROM (VALUES (PaidAddDateTime), (HeaderAddDateTime), (ScheduleAddDateTime)) AS t(v)) DESC) AS rn
            FROM #pay
        ) x WHERE rn = 1;

        SELECT @MaxWatermark = MAX(MaxAddDateTime) FROM (
            SELECT (SELECT MAX(v) FROM (VALUES (PaidAddDateTime), (ISNULL(HeaderAddDateTime, '1900-01-01')), (ISNULL(ScheduleAddDateTime, '1900-01-01'))) AS t(v)) AS MaxAddDateTime
            FROM #pay_one
        ) m;

        INSERT INTO svo.F_AP_PAYMENTS WITH (TABLOCK) (
            AP_INVOICE_PAYMENTS_ALL_CHECK_ID, AP_CHECKS_ALL_CHECK_NUMBER, AP_INVOICE_HEADER_SK, AP_DISBURSEMENT_HEADER_SK,
            LEGAL_ENTITY_SK, VENDOR_SITE_SK, BUSINESS_UNIT_SK, LEDGER_SK, ACCOUNT_SK, BUSINESS_OFFERING_SK, COMPANY_SK, COST_CENTER_SK, INDUSTRY_SK, INTERCOMPANY_SK,
            INV_CURRENCY_SK, PAY_CURRENCY_SK,
            DUE_DATE_SK, ACCOUNTING_DATE_SK, EXCHANGE_DATE_SK, PAYMENT_DOCUMENT_ID,
            AP_PAYMENT_SCHEDULES_ALL_PAYMENT_PRIORITY, AP_PAYMENT_SCHEDULES_ALL_PAYMENT_STATUS_FLAG, AP_INVOICE_PAYMENTS_ALL_POSTED_FLAG,
            AP_CHECKS_ALL_AMOUNT, AP_INVOICE_PAYMENTS_ALL_INVOICE_PAYMENT_ID, AP_PAYMENT_SCHEDULES_ALL_PAYMENT_NUM,
            AP_PAYMENT_SCHEDULES_ALL_GROSS_AMOUNT, AP_PAYMENT_SCHEDULES_ALL_AMOUNT_REMAINING, AP_INVOICE_PAYMENTS_ALL_AMOUNT, AP_INVOICE_PAYMENTS_ALL_AMOUNT_INV_CURR,
            AP_PAYMENT_SCHEDULES_ALL_LAST_UPDATED_BY, AP_INVOICE_PAYMENTS_ALL_LAST_UPDATED_BY,
            PAYMENT_LAST_UPDATE_DATE, SCHEDULE_LAST_UPDATE_DATE,
            BZ_LOAD_DATE_HEADER, BZ_LOAD_DATE_SCHED, BZ_LOAD_DATE_PAID, SV_LOAD_DATE
        )
        SELECT
            ISNULL(D.ApInvoicePaymentsAllCheckId, -1),
            ISNULL(H.ApChecksAllCheckNumber, -1),
            ISNULL(DAPIH.AP_INVOICE_HEADER_SK, 0),
            ISNULL(DAPDH.AP_DISBURSEMENT_HEADER_SK, 0),
            ISNULL(LE.LEGAL_ENTITY_SK, 0),
            ISNULL(V.VENDOR_SITE_SK, 0),
            ISNULL(BU.BUSINESS_UNIT_SK, 0),
            ISNULL(LDG.LEDGER_SK, 0),
            ISNULL(DA.ACCOUNT_SK, 0),
            ISNULL(DBO.BUSINESS_OFFERING_SK, 0),
            ISNULL(DCO.COMPANY_SK, 0),
            ISNULL(DCC.COST_CENTER_SK, 0),
            ISNULL(DI.INDUSTRY_SK, 0),
            ISNULL(DIC.INTERCOMPANY_SK, 0),
            ISNULL(INVC.CURRENCY_SK, 0),
            ISNULL(PAYC.CURRENCY_SK, 0),
            CONVERT(INT, FORMAT(S.ApPaymentSchedulesAllDueDate, 'yyyyMMdd')),
            CONVERT(INT, FORMAT(D.ApInvoicePaymentsAllAccountingDate, 'yyyyMMdd')),
            CONVERT(INT, FORMAT(D.ApInvoicePaymentsAllExchangeDate, 'yyyyMMdd')),
            ISNULL(H.ApChecksAllPaymentDocumentId, 0),
            ISNULL(S.ApPaymentSchedulesAllPaymentPriority, 0),
            ISNULL(S.ApPaymentSchedulesAllPaymentStatusFlag, 'U'),
            ISNULL(D.ApInvoicePaymentsAllPostedFlag, 'U'),
            ISNULL(H.ApChecksAllAmount, 0),
            ISNULL(D.ApInvoicePaymentsAllInvoicePaymentId, -1),
            ISNULL(S.ApPaymentSchedulesAllPaymentNum, 0),
            ISNULL(S.ApPaymentSchedulesAllGrossAmount, 0),
            ISNULL(S.ApPaymentSchedulesAllAmountRemaining, 0),
            ISNULL(D.ApInvoicePaymentsAllAmount, 0),
            ISNULL(D.ApInvoicePaymentsAllAmountInvCurr, 0),
            ISNULL(S.ApPaymentSchedulesAllLastUpdatedBy, 'UNK'),
            ISNULL(D.ApInvoicePaymentsAllLastUpdatedBy, 'UNK'),
            CAST(D.ApInvoicePaymentsAllLastUpdateDate AS DATETIME),
            CAST(S.ApPaymentSchedulesAllLastUpdateDate AS DATETIME),
            H.AddDateTime,
            S.AddDateTime,
            D.AddDateTime,
            SYSDATETIME()
        FROM #pay_one pay_one
        JOIN bzo.AP_PaidDisbursementScheduleExtractPVO D WITH (NOLOCK) ON D.ApInvoicePaymentsAllCheckId = pay_one.ApInvoicePaymentsAllCheckId AND D.ApInvoicePaymentsAllInvoiceId = pay_one.ApInvoicePaymentsAllInvoiceId AND D.ApInvoicePaymentsAllPaymentNum = pay_one.ApInvoicePaymentsAllPaymentNum
        JOIN bzo.AP_InvoiceHeaderExtractPVO IH WITH (NOLOCK) ON IH.ApInvoicesInvoiceId = D.ApInvoicePaymentsAllInvoiceId
        LEFT JOIN bzo.AP_DisbursementHeaderExtractPVO H WITH (NOLOCK) ON H.ApChecksAllCheckId = D.ApInvoicePaymentsAllCheckId
        LEFT JOIN bzo.AP_InvoicePaymentScheduleExtractPVO S WITH (NOLOCK) ON S.ApPaymentSchedulesAllInvoiceId = D.ApInvoicePaymentsAllInvoiceId AND S.ApPaymentSchedulesAllPaymentNum = D.ApInvoicePaymentsAllPaymentNum
        LEFT JOIN svo.D_LEGAL_ENTITY LE ON LE.LEGAL_ENTITY_ID = H.ApChecksAllLegalEntityId AND LE.CURR_IND = 'Y'
        LEFT JOIN svo.D_VENDOR_SITE V ON V.VENDOR_SITE_ID = H.ApChecksAllVendorSiteId AND V.CURR_IND = 'Y'
        LEFT JOIN svo.D_BUSINESS_UNIT BU ON BU.BUSINESS_UNIT_ID = D.ApInvoicePaymentsAllOrgId AND BU.CURR_IND = 'Y'
        LEFT JOIN svo.D_LEDGER LDG ON LDG.LEDGER_ID = D.ApInvoicePaymentsAllSetOfBooksId AND LDG.CURR_IND = 'Y'
        LEFT JOIN svo.LINES_CODE_COMBO_LOOKUP C ON CAST(IH.ApInvoicesAcctsPayCodeCombinationId AS BIGINT) = C.CODE_COMBINATION_BK
        LEFT JOIN svo.D_ACCOUNT DA ON DA.ACCOUNT_ID = C.ACCOUNT_ID AND DA.CURR_IND = 'Y'
        LEFT JOIN svo.D_BUSINESS_OFFERING DBO ON DBO.BUSINESS_OFFERING_ID = C.BUSINESSOFFERING_ID AND DBO.CURR_IND = 'Y'
        LEFT JOIN svo.D_COMPANY DCO ON DCO.COMPANY_ID = C.COMPANY_ID AND DCO.CURR_IND = 'Y'
        LEFT JOIN svo.D_COST_CENTER DCC ON DCC.COST_CENTER_ID = C.COSTCENTER_ID AND DCC.CURR_IND = 'Y'
        LEFT JOIN svo.D_INDUSTRY DI ON DI.INDUSTRY_ID = C.INDUSTRY_ID AND DI.CURR_IND = 'Y'
        LEFT JOIN svo.D_INTERCOMPANY DIC ON DIC.INTERCOMPANY_ID = C.INTERCOMPANY_ID AND DIC.CURR_IND = 'Y'
        LEFT JOIN svo.D_CURRENCY INVC ON INVC.CURRENCY_ID = CONCAT(ISNULL(D.ApInvoicePaymentsAllInvoiceCurrencyCode, 'UNK'), CONVERT(CHAR(8), ISNULL(D.ApInvoicePaymentsAllExchangeDate, '0001-01-01'), 112), 'Corporate')
        LEFT JOIN svo.D_CURRENCY PAYC ON PAYC.CURRENCY_ID = CONCAT(ISNULL(D.ApInvoicePaymentsAllPaymentCurrencyCode, 'UNK'), CONVERT(CHAR(8), ISNULL(D.ApInvoicePaymentsAllExchangeDate, '0001-01-01'), 112), 'Corporate')
        LEFT JOIN svo.D_AP_INVOICE_HEADER DAPIH ON DAPIH.INVOICE_ID = IH.ApInvoicesInvoiceId
        LEFT JOIN svo.D_AP_DISBURSEMENT_HEADER DAPDH ON DAPDH.CHECK_ID = D.ApInvoicePaymentsAllCheckId
        WHERE NOT EXISTS (SELECT 1 FROM svo.F_AP_PAYMENTS t WHERE t.AP_INVOICE_PAYMENTS_ALL_INVOICE_PAYMENT_ID = D.ApInvoicePaymentsAllInvoicePaymentId);

        SET @RowInserted = @@ROWCOUNT;

        IF @MaxWatermark IS NOT NULL
            UPDATE etl.ETL_WATERMARK SET LAST_WATERMARK = @MaxWatermark, UDT_DATE = SYSDATETIME() WHERE TABLE_NAME = @TargetObject;

        IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'IX_F_AP_PAYMENTS_ACCT_DATE' AND object_id = OBJECT_ID('svo.F_AP_PAYMENTS'))
            CREATE NONCLUSTERED INDEX IX_F_AP_PAYMENTS_ACCT_DATE ON svo.F_AP_PAYMENTS
            (ACCOUNTING_DATE_SK, LEGAL_ENTITY_SK, BUSINESS_UNIT_SK, LEDGER_SK)
            INCLUDE (AP_INVOICE_PAYMENTS_ALL_CHECK_ID, AP_CHECKS_ALL_CHECK_NUMBER, AP_INVOICE_PAYMENTS_ALL_AMOUNT, AP_CHECKS_ALL_AMOUNT, INV_CURRENCY_SK, PAY_CURRENCY_SK) ON FG_SilverFact;
        IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'IX_F_AP_PAYMENTS_CHECK' AND object_id = OBJECT_ID('svo.F_AP_PAYMENTS'))
            CREATE NONCLUSTERED INDEX IX_F_AP_PAYMENTS_CHECK ON svo.F_AP_PAYMENTS(AP_INVOICE_PAYMENTS_ALL_CHECK_ID)
            INCLUDE (AP_CHECKS_ALL_CHECK_NUMBER, AP_INVOICE_PAYMENTS_ALL_INVOICE_PAYMENT_ID, ACCOUNTING_DATE_SK, LEGAL_ENTITY_SK, BUSINESS_UNIT_SK, LEDGER_SK, AP_INVOICE_PAYMENTS_ALL_AMOUNT, AP_CHECKS_ALL_AMOUNT) ON FG_SilverFact;
        IF EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'UX_F_AP_PAYMENTS_INVOICE_PAYMENT_ID' AND object_id = OBJECT_ID('svo.F_AP_PAYMENTS'))
            DROP INDEX UX_F_AP_PAYMENTS_INVOICE_PAYMENT_ID ON svo.F_AP_PAYMENTS;
        IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'IX_F_AP_PAYMENTS_INVOICE_PAYMENT_ID' AND object_id = OBJECT_ID('svo.F_AP_PAYMENTS'))
            CREATE NONCLUSTERED INDEX IX_F_AP_PAYMENTS_INVOICE_PAYMENT_ID ON svo.F_AP_PAYMENTS(AP_INVOICE_PAYMENTS_ALL_INVOICE_PAYMENT_ID)
            INCLUDE (AP_INVOICE_HEADER_SK, AP_DISBURSEMENT_HEADER_SK, AP_INVOICE_PAYMENTS_ALL_CHECK_ID, ACCOUNTING_DATE_SK, LEGAL_ENTITY_SK, BUSINESS_UNIT_SK, LEDGER_SK, AP_INVOICE_PAYMENTS_ALL_AMOUNT, AP_CHECKS_ALL_AMOUNT) ON FG_SilverFact;

        SET @EndDttm = SYSDATETIME();
        UPDATE etl.ETL_RUN SET END_DTTM = @EndDttm, STATUS = 'SUCCESS', ROW_INSERTED = @RowInserted, ROW_UPDATED = 0, ROW_EXPIRED = 0, ERROR_MESSAGE = NULL WHERE RUN_ID = @RunId;
    END TRY
    BEGIN CATCH
        SET @EndDttm = SYSDATETIME();
        SET @ErrMsg = CONCAT('Error ', ERROR_NUMBER(), ' (Line ', ERROR_LINE(), '): ', ERROR_MESSAGE());
        IF @RunId IS NOT NULL
            UPDATE etl.ETL_RUN SET END_DTTM = @EndDttm, STATUS = 'FAILURE', ROW_INSERTED = @RowInserted, ROW_UPDATED = 0, ROW_EXPIRED = 0, ERROR_MESSAGE = @ErrMsg WHERE RUN_ID = @RunId;
        IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'IX_F_AP_PAYMENTS_ACCT_DATE' AND object_id = OBJECT_ID('svo.F_AP_PAYMENTS'))
            CREATE NONCLUSTERED INDEX IX_F_AP_PAYMENTS_ACCT_DATE ON svo.F_AP_PAYMENTS
            (ACCOUNTING_DATE_SK, LEGAL_ENTITY_SK, BUSINESS_UNIT_SK, LEDGER_SK)
            INCLUDE (AP_INVOICE_PAYMENTS_ALL_CHECK_ID, AP_CHECKS_ALL_CHECK_NUMBER, AP_INVOICE_PAYMENTS_ALL_AMOUNT, AP_CHECKS_ALL_AMOUNT, INV_CURRENCY_SK, PAY_CURRENCY_SK) ON FG_SilverFact;
        IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'IX_F_AP_PAYMENTS_CHECK' AND object_id = OBJECT_ID('svo.F_AP_PAYMENTS'))
            CREATE NONCLUSTERED INDEX IX_F_AP_PAYMENTS_CHECK ON svo.F_AP_PAYMENTS(AP_INVOICE_PAYMENTS_ALL_CHECK_ID)
            INCLUDE (AP_CHECKS_ALL_CHECK_NUMBER, AP_INVOICE_PAYMENTS_ALL_INVOICE_PAYMENT_ID, ACCOUNTING_DATE_SK, LEGAL_ENTITY_SK, BUSINESS_UNIT_SK, LEDGER_SK, AP_INVOICE_PAYMENTS_ALL_AMOUNT, AP_CHECKS_ALL_AMOUNT) ON FG_SilverFact;
        IF EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'UX_F_AP_PAYMENTS_INVOICE_PAYMENT_ID' AND object_id = OBJECT_ID('svo.F_AP_PAYMENTS'))
            DROP INDEX UX_F_AP_PAYMENTS_INVOICE_PAYMENT_ID ON svo.F_AP_PAYMENTS;
        IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'IX_F_AP_PAYMENTS_INVOICE_PAYMENT_ID' AND object_id = OBJECT_ID('svo.F_AP_PAYMENTS'))
            CREATE NONCLUSTERED INDEX IX_F_AP_PAYMENTS_INVOICE_PAYMENT_ID ON svo.F_AP_PAYMENTS(AP_INVOICE_PAYMENTS_ALL_INVOICE_PAYMENT_ID)
            INCLUDE (AP_INVOICE_HEADER_SK, AP_DISBURSEMENT_HEADER_SK, AP_INVOICE_PAYMENTS_ALL_CHECK_ID, ACCOUNTING_DATE_SK, LEGAL_ENTITY_SK, BUSINESS_UNIT_SK, LEDGER_SK, AP_INVOICE_PAYMENTS_ALL_AMOUNT, AP_CHECKS_ALL_AMOUNT) ON FG_SilverFact;
        ;THROW;
    END CATCH
END;
GO
