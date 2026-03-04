/****** Object:  StoredProcedure [svo].[usp_Load_F_AR_RECEIPTS]    Script Date: 2/19/2026 4:48:05 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
/* =========================================================
   usp_Load_F_AR_RECEIPTS
   Incremental INSERT. Source: bzo.AR_ReceivableApplicationExtractPVO (+ Receipt, PaymentSchedule, Header).
   Filter: A.AddDateTime > @LastWatermark. Dedupe by AR_RECEIVABLE_APPLICATION_ID.
   ========================================================= */
CREATE OR ALTER PROCEDURE [svo].[usp_Load_F_AR_RECEIPTS]
    @BatchId INT = NULL
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    DECLARE
        @ProcName       SYSNAME        = OBJECT_SCHEMA_NAME(@@PROCID) + '.' + OBJECT_NAME(@@PROCID),
        @TargetObject   SYSNAME        = 'svo.F_AR_RECEIPTS',
        @StartDttm      DATETIME2(0)   = SYSDATETIME(),
        @EndDttm        DATETIME2(0),
        @RunId          BIGINT         = NULL,
        @ErrMsg         NVARCHAR(4000) = NULL,
        @AsOfDate       DATE           = CAST(GETDATE() AS DATE),
        @LastWatermark  DATETIME2(7),
        @MaxWatermark   DATETIME2(7)   = NULL,
        @RowInserted    INT            = 0,
        @TableBridgeID  INT            = NULL;

    SELECT @TableBridgeID = TableBridgeID FROM meta.MedallionTableBridge WHERE targettable = N'AR_ReceivableApplicationExtractPVO';

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

        IF OBJECT_ID('tempdb..#app') IS NOT NULL DROP TABLE #app;
        SELECT A.*, A.AddDateTime AS AppAddDateTime
        INTO #app
        FROM (
            SELECT *,
                ROW_NUMBER() OVER (PARTITION BY A.ArReceivableApplicationReceivableApplicationId ORDER BY A.AddDateTime DESC) AS rn
            FROM bzo.AR_ReceivableApplicationExtractPVO A
            WHERE A.AddDateTime > @LastWatermark
        ) A
        WHERE A.rn = 1;

        SELECT @MaxWatermark = MAX(AppAddDateTime) FROM #app;

        INSERT INTO svo.F_AR_RECEIPTS
        (CUSTOMER_SK, CUSTOMER_SITE_USE_SK, BUSINESS_UNIT_SK, LEDGER_SK, LEGAL_ENTITY_SK, RECEIPT_METHOD_SK, AR_COLLECTOR_SK, ACCOUNT_SK, CURRENCY_SK,
         APPLY_DATE_SK, GL_DATE_SK, RECEIPT_DATE_SK, DEPOSIT_DATE_SK, EXCHANGE_DATE_SK, DUE_DATE_SK, PAYMENT_SCHEDULE_GL_DATE_SK, TRX_DATE_SK, BILLING_DATE_SK, TERM_DUE_DATE_SK,
         AR_RECEIVABLE_APPLICATION_ID, AR_CASH_RECEIPT_ID, AR_PAYMENT_SCHEDULE_ID, AR_CUSTOMER_TRX_ID, APPLIED_CUSTOMER_TRX_ID, APPLIED_PAYMENT_SCHEDULE_ID,
         ORG_ID, SET_OF_BOOKS_ID, LEGAL_ENTITY_ID, RECEIPT_METHOD_ID, COLLECTOR_ID, CODE_COMBINATION_ID, TRX_TYPE_SEQ_ID, TERM_ID,
         RECEIPT_NUMBER, RECEIPT_STATUS, RECEIPT_CURRENCY_CODE, TRX_NUMBER, TRX_STATUS, TRX_CLASS, INVOICE_CURRENCY_CODE, PAYMENT_SCHEDULE_STATUS, APPLICATION_STATUS,
         RECEIPT_AMOUNT, RECEIPT_EXCHANGE_RATE, AMOUNT_DUE_ORIGINAL, AMOUNT_DUE_REMAINING, AMOUNT_APPLIED, AMOUNT_APPLIED_FROM, ACCTD_AMOUNT_APPLIED_FROM, BZ_LOAD_DATE)
        SELECT
            ISNULL(DCA.CUSTOMER_SK, 0),
            ISNULL(DSU.SITE_USE_SK, 0),
            ISNULL(DBU.BUSINESS_UNIT_SK, 0),
            ISNULL(DL.LEDGER_SK, 0),
            ISNULL(DLE.LEGAL_ENTITY_SK, 0),
            ISNULL(DRM.RECEIPT_METHOD_SK, 0),
            ISNULL(DC.AR_COLLECTOR_SK, 0),
            ISNULL(DA.ACCOUNT_SK, 0),
            CAST(0 AS bigint),
            ISNULL(CONVERT(int, CONVERT(char(8), A.ArReceivableApplicationApplyDate, 112)), 0),
            ISNULL(CONVERT(int, CONVERT(char(8), A.ArReceivableApplicationGlDate, 112)), 0),
            ISNULL(CONVERT(int, CONVERT(char(8), R.ArCashReceiptReceiptDate, 112)), 0),
            ISNULL(CONVERT(int, CONVERT(char(8), R.ArCashReceiptDepositDate, 112)), 0),
            ISNULL(CONVERT(int, CONVERT(char(8), R.ArCashReceiptExchangeDate, 112)), 0),
            ISNULL(CONVERT(int, CONVERT(char(8), PS.ArPaymentScheduleDueDate, 112)), 0),
            ISNULL(CONVERT(int, CONVERT(char(8), PS.ArPaymentScheduleGlDate, 112)), 0),
            ISNULL(CONVERT(int, CONVERT(char(8), TH.RaCustomerTrxTrxDate, 112)), 0),
            ISNULL(CONVERT(int, CONVERT(char(8), TH.RaCustomerTrxBillingDate, 112)), 0),
            ISNULL(CONVERT(int, CONVERT(char(8), TH.RaCustomerTrxTermDueDate, 112)), 0),
            A.ArReceivableApplicationReceivableApplicationId,
            A.ArReceivableApplicationCashReceiptId,
            A.ArReceivableApplicationAppliedPaymentScheduleId,
            A.ArReceivableApplicationAppliedCustomerTrxId,
            A.ArReceivableApplicationCustomerTrxId,
            PS.ArPaymentSchedulePaymentScheduleId,
            COALESCE(R.ArCashReceiptOrgId, TH.RaCustomerTrxOrgId),
            COALESCE(R.ArCashReceiptSetOfBooksId, TH.RaCustomerTrxSetOfBooksId),
            COALESCE(R.ArCashReceiptLegalEntityId, TH.RaCustomerTrxLegalEntityId),
            R.ArCashReceiptReceiptMethodId,
            R.ArCashReceiptCollectorId,
            A.ArReceivableApplicationCodeCombinationId,
            TH.RaCustomerTrxCustTrxTypeSeqId,
            TH.RaCustomerTrxTermId,
            R.ArCashReceiptReceiptNumber,
            R.ArCashReceiptStatus,
            R.ArCashReceiptCurrencyCode,
            TH.RaCustomerTrxTrxNumber,
            TH.RaCustomerTrxStatusTrx,
            TH.RaCustomerTrxTrxClass,
            TH.RaCustomerTrxInvoiceCurrencyCode,
            PS.ArPaymentScheduleStatus,
            A.ArReceivableApplicationStatus,
            R.ArCashReceiptAmount,
            CAST(R.ArCashReceiptExchangeRate AS decimal(29,8)),
            PS.ArPaymentScheduleAmountDueOriginal,
            PS.ArPaymentScheduleAmountDueRemaining,
            A.ArReceivableApplicationAmountApplied,
            A.ArReceivableApplicationAmountAppliedFrom,
            A.ArReceivableApplicationAcctdAmountAppliedFrom,
            CAST(A.AddDateTime AS date)
        FROM #app A
        LEFT JOIN bzo.AR_ReceiptHeaderExtractPVO R  ON R.ArCashReceiptCashReceiptId = A.ArReceivableApplicationCashReceiptId
        LEFT JOIN bzo.AR_PaymentScheduleExtractPVO PS ON PS.ArPaymentSchedulePaymentScheduleId = A.ArReceivableApplicationAppliedPaymentScheduleId
            OR (A.ArReceivableApplicationAppliedPaymentScheduleId IS NULL AND PS.ArPaymentScheduleCustomerTrxId = COALESCE(A.ArReceivableApplicationAppliedCustomerTrxId, A.ArReceivableApplicationCustomerTrxId))
        LEFT JOIN bzo.AR_TransactionHeaderExtractPVO TH ON TH.RaCustomerTrxCustomerTrxId = COALESCE(A.ArReceivableApplicationAppliedCustomerTrxId, A.ArReceivableApplicationCustomerTrxId)
        LEFT JOIN svo.LINES_CODE_COMBO_LOOKUP C ON A.ArReceivableApplicationCodeCombinationId = C.CODE_COMBINATION_BK
        LEFT JOIN svo.D_CUSTOMER_ACCOUNT   DCA ON DCA.CUSTOMER_ACCOUNT_ID = COALESCE(R.ArCashReceiptPayFromCustomer, TH.RaCustomerTrxBillToCustomerId)
        LEFT JOIN svo.D_SITE_USE  DSU ON DSU.SITE_USE = COALESCE(R.ArCashReceiptCustomerSiteUseId, TH.RaCustomerTrxBillToSiteUseId)
        LEFT JOIN svo.D_BUSINESS_UNIT      DBU ON DBU.BUSINESS_UNIT_ID = COALESCE(R.ArCashReceiptOrgId, TH.RaCustomerTrxOrgId)
        LEFT JOIN svo.D_LEDGER             DL  ON DL.LEDGER_ID = COALESCE(R.ArCashReceiptSetOfBooksId, TH.RaCustomerTrxSetOfBooksId)
        LEFT JOIN svo.D_LEGAL_ENTITY       DLE ON DLE.LEGAL_ENTITY_ID = COALESCE(R.ArCashReceiptLegalEntityId, TH.RaCustomerTrxLegalEntityId)
        LEFT JOIN svo.D_AR_RECEIPT_METHOD  DRM ON DRM.AR_RECEIPT_METHOD_RECEIPT_METHOD_ID = R.ArCashReceiptReceiptMethodId
        LEFT JOIN svo.D_AR_COLLECTOR       DC  ON DC.AR_COLLECTOR_ID = COALESCE(R.ArCashReceiptCollectorId, PS.ArPaymentScheduleCollectorLast)
        LEFT JOIN svo.D_ACCOUNT            DA  ON DA.ACCOUNT_ID = C.ACCOUNT_ID
        WHERE NOT EXISTS (SELECT 1 FROM svo.F_AR_RECEIPTS t WHERE t.AR_RECEIVABLE_APPLICATION_ID = A.ArReceivableApplicationReceivableApplicationId);

        SET @RowInserted = @@ROWCOUNT;

        IF @MaxWatermark IS NOT NULL
            UPDATE etl.ETL_WATERMARK SET LAST_WATERMARK = @MaxWatermark, UDT_DATE = SYSDATETIME() WHERE TABLE_NAME = @TargetObject;

        SET @EndDttm = SYSDATETIME();
        UPDATE etl.ETL_RUN SET END_DTTM = @EndDttm, STATUS = 'SUCCESS', ROW_INSERTED = @RowInserted, ROW_UPDATED = 0, ROW_EXPIRED = 0, ERROR_MESSAGE = NULL WHERE RUN_ID = @RunId;
    END TRY
    BEGIN CATCH
        SET @EndDttm = SYSDATETIME();
        SET @ErrMsg = CONCAT('Error ', ERROR_NUMBER(), ' (Line ', ERROR_LINE(), '): ', ERROR_MESSAGE());
        IF @RunId IS NOT NULL
            UPDATE etl.ETL_RUN SET END_DTTM = @EndDttm, STATUS = 'FAILURE', ROW_INSERTED = @RowInserted, ROW_UPDATED = 0, ROW_EXPIRED = 0, ERROR_MESSAGE = @ErrMsg WHERE RUN_ID = @RunId;
        ;THROW;
    END CATCH
END;
