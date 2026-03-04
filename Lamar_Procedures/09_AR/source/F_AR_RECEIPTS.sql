USE Oracle_Reporting_P2;
GO
SET NOCOUNT ON;
GO

/* ============================================================
   FACT: svo.F_AR_RECEIPTS
   Purpose: Receipt applications (cash applied to invoices/schedules)
   Grain: 1 row per AR_RECEIVABLE_APPLICATION_ID
          (each application of a receipt to something)
   Sources:
     - bzo.AR_ReceivableApplicationExtractPVO   (base grain)
     - bzo.AR_ReceiptHeaderExtractPVO           (receipt header)
     - bzo.AR_PaymentScheduleExtractPVO         (payment schedule)
     - bzo.AR_TransactionHeaderExtractPVO       (invoice/header)
   Notes:
     - No MAP CTEs
     - No AddDateTime stored; BZ_LOAD_DATE derived from source AddDateTime
     - SV_LOAD_DATE = CAST(GETDATE() AS DATE)
     - Uses existing dims you showed: D_CUSTOMER_ACCOUNT, D_CUSTOMER_SITE_USE,
       D_BUSINESS_UNIT, D_LEDGER, D_LEGAL_ENTITY, D_AR_RECEIPT_METHOD, D_AR_COLLECTOR
     - ACCOUNT_SK left as 0 (no CCID->D_ACCOUNT mapping included here)
     ============================================================ */

IF OBJECT_ID('svo.F_AR_RECEIPTS', 'U') IS NOT NULL
    DROP TABLE svo.F_AR_RECEIPTS;
GO

CREATE TABLE svo.F_AR_RECEIPTS
(
    AR_RECEIPTS_FACT_PK                  bigint IDENTITY(1,1) NOT NULL,

    /* Dim keys (resolved where possible) */
    CUSTOMER_SK                          bigint NOT NULL,
    CUSTOMER_SITE_USE_SK                 bigint NOT NULL,
    BUSINESS_UNIT_SK                     bigint NOT NULL,
    LEDGER_SK                            bigint NOT NULL,
    LEGAL_ENTITY_SK                      bigint NOT NULL,
    RECEIPT_METHOD_SK                    bigint NOT NULL,
    AR_COLLECTOR_SK                         bigint NOT NULL,

    ACCOUNT_SK                           bigint NOT NULL,  -- 0 until CCID->D_ACCOUNT confirmed
    CURRENCY_SK                          bigint NOT NULL,  -- 0 (no currency code dim)

    /* Date keys */
    APPLY_DATE_SK                        int    NOT NULL,
    GL_DATE_SK                           int    NOT NULL,
    RECEIPT_DATE_SK                      int    NOT NULL,
    DEPOSIT_DATE_SK                      int    NOT NULL,
    EXCHANGE_DATE_SK                     int    NOT NULL,
    DUE_DATE_SK                          int    NOT NULL,
    PAYMENT_SCHEDULE_GL_DATE_SK          int    NOT NULL,
    TRX_DATE_SK                          int    NOT NULL,
    BILLING_DATE_SK                      int    NOT NULL,
    TERM_DUE_DATE_SK                     int    NOT NULL,

    /* Business identifiers (degenerate keys) */
    AR_RECEIVABLE_APPLICATION_ID         bigint NOT NULL,
    AR_CASH_RECEIPT_ID                   bigint NULL,
    AR_PAYMENT_SCHEDULE_ID               bigint NULL,
    AR_CUSTOMER_TRX_ID                   bigint NULL,   -- application row’s customer trx id (if populated)
    APPLIED_CUSTOMER_TRX_ID              bigint NULL,   -- applied customer trx id (if populated)
    APPLIED_PAYMENT_SCHEDULE_ID          bigint NULL,

    ORG_ID                               bigint NULL,
    SET_OF_BOOKS_ID                      bigint NULL,
    LEGAL_ENTITY_ID                      bigint NULL,

    RECEIPT_METHOD_ID                    bigint NULL,
    COLLECTOR_ID                         bigint NULL,

    CODE_COMBINATION_ID                  bigint NULL,
    TRX_TYPE_SEQ_ID                      bigint NULL,
    TERM_ID                              bigint NULL,

    /* Descriptive degenerate attributes */
    RECEIPT_NUMBER                       varchar(30) NULL,
    RECEIPT_STATUS                       varchar(30) NULL,
    RECEIPT_CURRENCY_CODE                varchar(15) NULL,

    TRX_NUMBER                           varchar(20) NULL,
    TRX_STATUS                           varchar(30) NULL,
    TRX_CLASS                            varchar(20) NULL,
    INVOICE_CURRENCY_CODE                varchar(15) NULL,

    PAYMENT_SCHEDULE_STATUS              varchar(30) NULL,
    APPLICATION_STATUS                   varchar(30) NULL,

    /* Measures */
    RECEIPT_AMOUNT                       decimal(29,4) NULL,
    RECEIPT_EXCHANGE_RATE                decimal(29,8) NULL,

    AMOUNT_DUE_ORIGINAL                  decimal(29,4) NULL,
    AMOUNT_DUE_REMAINING                 decimal(29,4) NULL,

    AMOUNT_APPLIED                       decimal(29,4) NOT NULL,
    AMOUNT_APPLIED_FROM                  decimal(29,4) NULL,
    ACCTD_AMOUNT_APPLIED_FROM            decimal(29,4) NOT NULL,

    /* Audit */
    BZ_LOAD_DATE                         date NOT NULL,
    SV_LOAD_DATE                         date NOT NULL
        CONSTRAINT DF_F_AR_RECEIPTS_SV_LOAD_DATE DEFAULT (CAST(GETDATE() AS date)),

    CONSTRAINT PK_F_AR_RECEIPTS
        PRIMARY KEY CLUSTERED (AR_RECEIPTS_FACT_PK)
        ON FG_SilverFact
)
ON FG_SilverFact;
GO

CREATE UNIQUE NONCLUSTERED INDEX UX_F_AR_RECEIPTS_BK
ON svo.F_AR_RECEIPTS (AR_RECEIVABLE_APPLICATION_ID)
ON FG_SilverFact;
GO

CREATE NONCLUSTERED INDEX IX_F_AR_RECEIPTS_CUST_APPLYDATE
ON svo.F_AR_RECEIPTS (CUSTOMER_SK, APPLY_DATE_SK)
INCLUDE (AMOUNT_APPLIED, APPLICATION_STATUS, RECEIPT_STATUS, TRX_NUMBER)
ON FG_SilverFact;
GO

CREATE NONCLUSTERED INDEX IX_F_AR_RECEIPTS_RECEIPT
ON svo.F_AR_RECEIPTS (AR_CASH_RECEIPT_ID, RECEIPT_DATE_SK)
INCLUDE (AMOUNT_APPLIED, RECEIPT_NUMBER, CUSTOMER_SK)
ON FG_SilverFact;
GO

CREATE NONCLUSTERED INDEX IX_F_AR_RECEIPTS_TRX
ON svo.F_AR_RECEIPTS (AR_CUSTOMER_TRX_ID, TRX_DATE_SK)
INCLUDE (AMOUNT_APPLIED, TRX_NUMBER, CUSTOMER_SK)
ON FG_SilverFact;
GO

/* ============================================================
   Load
   ============================================================ */

MERGE svo.F_AR_RECEIPTS AS F
USING
(
    SELECT
        /* -----------------------------
           Resolve “customer” context:
           Prefer receipt pay-from customer when available,
           else fall back to trx bill-to customer when available.
           ----------------------------- */
        ISNULL(DCA.CUSTOMER_SK, 0) AS CUSTOMER_SK,

        /* Prefer receipt site use id, else trx bill-to site use id */
        ISNULL(DSU.SITE_USE_SK, 0) AS CUSTOMER_SITE_USE_SK,

        /* Org/Ledger/LE from receipt when available, else trx */
        ISNULL(DBU.BUSINESS_UNIT_SK, 0) AS BUSINESS_UNIT_SK,
        ISNULL(DL.LEDGER_SK, 0)         AS LEDGER_SK,
        ISNULL(DLE.LEGAL_ENTITY_SK, 0)  AS LEGAL_ENTITY_SK,

        ISNULL(DRM.RECEIPT_METHOD_SK, 0) AS RECEIPT_METHOD_SK,
        ISNULL(DC.AR_COLLECTOR_SK, 0)    AS AR_COLLECTOR_SK,

        ISNULL(DA.ACCOUNT_SK,0)             AS ACCOUNT_SK,
        CAST(0 AS bigint) AS CURRENCY_SK,

        /* Date keys */
        ISNULL(CONVERT(int, CONVERT(char(8), A.ArReceivableApplicationApplyDate, 112)), 0) AS APPLY_DATE_SK,
        ISNULL(CONVERT(int, CONVERT(char(8), A.ArReceivableApplicationGlDate, 112)), 0)    AS GL_DATE_SK,

        ISNULL(CONVERT(int, CONVERT(char(8), R.ArCashReceiptReceiptDate, 112)), 0)         AS RECEIPT_DATE_SK,
        ISNULL(CONVERT(int, CONVERT(char(8), R.ArCashReceiptDepositDate, 112)), 0)         AS DEPOSIT_DATE_SK,
        ISNULL(CONVERT(int, CONVERT(char(8), R.ArCashReceiptExchangeDate, 112)), 0)        AS EXCHANGE_DATE_SK,

        ISNULL(CONVERT(int, CONVERT(char(8), PS.ArPaymentScheduleDueDate, 112)), 0)        AS DUE_DATE_SK,
        ISNULL(CONVERT(int, CONVERT(char(8), PS.ArPaymentScheduleGlDate, 112)), 0)         AS PAYMENT_SCHEDULE_GL_DATE_SK,

        ISNULL(CONVERT(int, CONVERT(char(8), TH.RaCustomerTrxTrxDate, 112)), 0)            AS TRX_DATE_SK,
        ISNULL(CONVERT(int, CONVERT(char(8), TH.RaCustomerTrxBillingDate, 112)), 0)        AS BILLING_DATE_SK,
        ISNULL(CONVERT(int, CONVERT(char(8), TH.RaCustomerTrxTermDueDate, 112)), 0)        AS TERM_DUE_DATE_SK,

        /* Business identifiers */
        A.ArReceivableApplicationReceivableApplicationId AS AR_RECEIVABLE_APPLICATION_ID,
        A.ArReceivableApplicationCashReceiptId           AS AR_CASH_RECEIPT_ID,
        A.ArReceivableApplicationAppliedPaymentScheduleId AS APPLIED_PAYMENT_SCHEDULE_ID,
        A.ArReceivableApplicationAppliedCustomerTrxId     AS APPLIED_CUSTOMER_TRX_ID,
        A.ArReceivableApplicationCustomerTrxId            AS AR_CUSTOMER_TRX_ID,
        PS.ArPaymentSchedulePaymentScheduleId             AS AR_PAYMENT_SCHEDULE_ID,

        /* Context ids */
        COALESCE(R.ArCashReceiptOrgId, TH.RaCustomerTrxOrgId)          AS ORG_ID,
        COALESCE(R.ArCashReceiptSetOfBooksId, TH.RaCustomerTrxSetOfBooksId) AS SET_OF_BOOKS_ID,
        COALESCE(R.ArCashReceiptLegalEntityId, TH.RaCustomerTrxLegalEntityId) AS LEGAL_ENTITY_ID,

        R.ArCashReceiptReceiptMethodId AS RECEIPT_METHOD_ID,
        R.ArCashReceiptCollectorId     AS COLLECTOR_ID,

        A.ArReceivableApplicationCodeCombinationId AS CODE_COMBINATION_ID,
        TH.RaCustomerTrxCustTrxTypeSeqId           AS TRX_TYPE_SEQ_ID,
        TH.RaCustomerTrxTermId                     AS TERM_ID,

        /* Degenerate attributes */
        R.ArCashReceiptReceiptNumber  AS RECEIPT_NUMBER,
        R.ArCashReceiptStatus         AS RECEIPT_STATUS,
        R.ArCashReceiptCurrencyCode   AS RECEIPT_CURRENCY_CODE,

        TH.RaCustomerTrxTrxNumber     AS TRX_NUMBER,
        TH.RaCustomerTrxStatusTrx     AS TRX_STATUS,
        TH.RaCustomerTrxTrxClass      AS TRX_CLASS,
        TH.RaCustomerTrxInvoiceCurrencyCode AS INVOICE_CURRENCY_CODE,

        PS.ArPaymentScheduleStatus    AS PAYMENT_SCHEDULE_STATUS,
        A.ArReceivableApplicationStatus AS APPLICATION_STATUS,

        /* Measures */
        R.ArCashReceiptAmount         AS RECEIPT_AMOUNT,
        CAST(R.ArCashReceiptExchangeRate AS decimal(29,8)) AS RECEIPT_EXCHANGE_RATE,

        PS.ArPaymentScheduleAmountDueOriginal  AS AMOUNT_DUE_ORIGINAL,
        PS.ArPaymentScheduleAmountDueRemaining AS AMOUNT_DUE_REMAINING,

        A.ArReceivableApplicationAmountApplied        AS AMOUNT_APPLIED,
        A.ArReceivableApplicationAmountAppliedFrom    AS AMOUNT_APPLIED_FROM,
        A.ArReceivableApplicationAcctdAmountAppliedFrom AS ACCTD_AMOUNT_APPLIED_FROM,

        /* Audit: base on application AddDateTime */
        CAST(A.AddDateTime AS date) AS BZ_LOAD_DATE

    FROM bzo.AR_ReceivableApplicationExtractPVO A
    LEFT JOIN bzo.AR_ReceiptHeaderExtractPVO R        ON R.ArCashReceiptCashReceiptId = A.ArReceivableApplicationCashReceiptId
   /* Payment schedule: prefer AppliedPaymentScheduleId, else fallback to schedule keyed by trx id */
    LEFT JOIN bzo.AR_PaymentScheduleExtractPVO PS   ON PS.ArPaymentSchedulePaymentScheduleId = A.ArReceivableApplicationAppliedPaymentScheduleId
        OR (
            A.ArReceivableApplicationAppliedPaymentScheduleId IS NULL
            AND PS.ArPaymentScheduleCustomerTrxId = COALESCE(A.ArReceivableApplicationAppliedCustomerTrxId, A.ArReceivableApplicationCustomerTrxId)
           )
    /* Transaction header: prefer applied trx id, else application trx id */
    LEFT JOIN bzo.AR_TransactionHeaderExtractPVO TH    ON TH.RaCustomerTrxCustomerTrxId = COALESCE(A.ArReceivableApplicationAppliedCustomerTrxId, A.ArReceivableApplicationCustomerTrxId)

    /* -----------------------------
       Dimension joins (direct, no MAPs)
       ----------------------------- */
  	LEFT JOIN stage.LINES_CODE_COMBO_LOOKUP AS C        ON A.ArReceivableApplicationCodeCombinationId = C.CODE_COMBINATION_BK

    LEFT JOIN svo.D_CUSTOMER_ACCOUNT    AS DCA        ON DCA.CUSTOMER_ACCOUNT_ID = COALESCE(R.ArCashReceiptPayFromCustomer, TH.RaCustomerTrxBillToCustomerId)
    LEFT JOIN svo.D_CUSTOMER_SITE_USE   AS DSU        ON DSU.SITE_USE = COALESCE(R.ArCashReceiptCustomerSiteUseId, TH.RaCustomerTrxBillToSiteUseId)
    LEFT JOIN svo.D_BUSINESS_UNIT       AS DBU        ON DBU.BUSINESS_UNIT_ID = COALESCE(R.ArCashReceiptOrgId, TH.RaCustomerTrxOrgId)
    LEFT JOIN svo.D_LEDGER              AS DL         ON DL.LEDGER_ID = COALESCE(R.ArCashReceiptSetOfBooksId, TH.RaCustomerTrxSetOfBooksId)
    LEFT JOIN svo.D_LEGAL_ENTITY        AS DLE        ON DLE.LEGAL_ENTITY_ID = COALESCE(R.ArCashReceiptLegalEntityId, TH.RaCustomerTrxLegalEntityId)
    LEFT JOIN svo.D_AR_RECEIPT_METHOD   AS DRM        ON DRM.AR_RECEIPT_METHOD_RECEIPT_METHOD_ID = R.ArCashReceiptReceiptMethodId
    LEFT JOIN svo.D_AR_COLLECTOR        AS DC         ON DC.AR_COLLECTOR_ID = COALESCE(R.ArCashReceiptCollectorId, PS.ArPaymentScheduleCollectorLast)
    LEFT JOIN svo.D_ACCOUNT             AS DA         ON DA.ACCOUNT_ID = C.ACCOUNT_ID


) AS X
ON F.AR_RECEIVABLE_APPLICATION_ID = X.AR_RECEIVABLE_APPLICATION_ID

WHEN MATCHED THEN
    UPDATE SET
        F.CUSTOMER_SK                 = X.CUSTOMER_SK,
        F.CUSTOMER_SITE_USE_SK        = X.CUSTOMER_SITE_USE_SK,
        F.BUSINESS_UNIT_SK            = X.BUSINESS_UNIT_SK,
        F.LEDGER_SK                   = X.LEDGER_SK,
        F.LEGAL_ENTITY_SK             = X.LEGAL_ENTITY_SK,
        F.RECEIPT_METHOD_SK           = X.RECEIPT_METHOD_SK,
        F.AR_COLLECTOR_SK             = X.AR_COLLECTOR_SK,
        F.ACCOUNT_SK                  = X.ACCOUNT_SK,
        F.CURRENCY_SK                 = X.CURRENCY_SK,

        F.APPLY_DATE_SK               = X.APPLY_DATE_SK,
        F.GL_DATE_SK                  = X.GL_DATE_SK,
        F.RECEIPT_DATE_SK             = X.RECEIPT_DATE_SK,
        F.DEPOSIT_DATE_SK             = X.DEPOSIT_DATE_SK,
        F.EXCHANGE_DATE_SK            = X.EXCHANGE_DATE_SK,
        F.DUE_DATE_SK                 = X.DUE_DATE_SK,
        F.PAYMENT_SCHEDULE_GL_DATE_SK = X.PAYMENT_SCHEDULE_GL_DATE_SK,
        F.TRX_DATE_SK                 = X.TRX_DATE_SK,
        F.BILLING_DATE_SK             = X.BILLING_DATE_SK,
        F.TERM_DUE_DATE_SK            = X.TERM_DUE_DATE_SK,

        F.AR_CASH_RECEIPT_ID          = X.AR_CASH_RECEIPT_ID,
        F.AR_PAYMENT_SCHEDULE_ID      = X.AR_PAYMENT_SCHEDULE_ID,
        F.AR_CUSTOMER_TRX_ID          = X.AR_CUSTOMER_TRX_ID,
        F.APPLIED_CUSTOMER_TRX_ID     = X.APPLIED_CUSTOMER_TRX_ID,
        F.APPLIED_PAYMENT_SCHEDULE_ID = X.APPLIED_PAYMENT_SCHEDULE_ID,

        F.ORG_ID                      = X.ORG_ID,
        F.SET_OF_BOOKS_ID             = X.SET_OF_BOOKS_ID,
        F.LEGAL_ENTITY_ID             = X.LEGAL_ENTITY_ID,

        F.RECEIPT_METHOD_ID           = X.RECEIPT_METHOD_ID,
        F.COLLECTOR_ID                = X.COLLECTOR_ID,
        F.CODE_COMBINATION_ID         = X.CODE_COMBINATION_ID,
        F.TRX_TYPE_SEQ_ID             = X.TRX_TYPE_SEQ_ID,
        F.TERM_ID                     = X.TERM_ID,

        F.RECEIPT_NUMBER              = X.RECEIPT_NUMBER,
        F.RECEIPT_STATUS              = X.RECEIPT_STATUS,
        F.RECEIPT_CURRENCY_CODE       = X.RECEIPT_CURRENCY_CODE,

        F.TRX_NUMBER                  = X.TRX_NUMBER,
        F.TRX_STATUS                  = X.TRX_STATUS,
        F.TRX_CLASS                   = X.TRX_CLASS,
        F.INVOICE_CURRENCY_CODE       = X.INVOICE_CURRENCY_CODE,

        F.PAYMENT_SCHEDULE_STATUS     = X.PAYMENT_SCHEDULE_STATUS,
        F.APPLICATION_STATUS          = X.APPLICATION_STATUS,

        F.RECEIPT_AMOUNT              = X.RECEIPT_AMOUNT,
        F.RECEIPT_EXCHANGE_RATE       = X.RECEIPT_EXCHANGE_RATE,

        F.AMOUNT_DUE_ORIGINAL         = X.AMOUNT_DUE_ORIGINAL,
        F.AMOUNT_DUE_REMAINING        = X.AMOUNT_DUE_REMAINING,

        F.AMOUNT_APPLIED              = X.AMOUNT_APPLIED,
        F.AMOUNT_APPLIED_FROM         = X.AMOUNT_APPLIED_FROM,
        F.ACCTD_AMOUNT_APPLIED_FROM   = X.ACCTD_AMOUNT_APPLIED_FROM,

        F.BZ_LOAD_DATE                = X.BZ_LOAD_DATE

WHEN NOT MATCHED BY TARGET THEN
    INSERT
    (
        CUSTOMER_SK,
        CUSTOMER_SITE_USE_SK,
        BUSINESS_UNIT_SK,
        LEDGER_SK,
        LEGAL_ENTITY_SK,
        RECEIPT_METHOD_SK,
        AR_COLLECTOR_SK,
        ACCOUNT_SK,
        CURRENCY_SK,
        APPLY_DATE_SK,
        GL_DATE_SK,
        RECEIPT_DATE_SK,
        DEPOSIT_DATE_SK,
        EXCHANGE_DATE_SK,
        DUE_DATE_SK,
        PAYMENT_SCHEDULE_GL_DATE_SK,
        TRX_DATE_SK,
        BILLING_DATE_SK,
        TERM_DUE_DATE_SK,
        AR_RECEIVABLE_APPLICATION_ID,
        AR_CASH_RECEIPT_ID,
        AR_PAYMENT_SCHEDULE_ID,
        AR_CUSTOMER_TRX_ID,
        APPLIED_CUSTOMER_TRX_ID,
        APPLIED_PAYMENT_SCHEDULE_ID,
        ORG_ID,
        SET_OF_BOOKS_ID,
        LEGAL_ENTITY_ID,
        RECEIPT_METHOD_ID,
        COLLECTOR_ID,
        CODE_COMBINATION_ID,
        TRX_TYPE_SEQ_ID,
        TERM_ID,
        RECEIPT_NUMBER,
        RECEIPT_STATUS,
        RECEIPT_CURRENCY_CODE,
        TRX_NUMBER,
        TRX_STATUS,
        TRX_CLASS,
        INVOICE_CURRENCY_CODE,
        PAYMENT_SCHEDULE_STATUS,
        APPLICATION_STATUS,
        RECEIPT_AMOUNT,
        RECEIPT_EXCHANGE_RATE,
        AMOUNT_DUE_ORIGINAL,
        AMOUNT_DUE_REMAINING,
        AMOUNT_APPLIED,
        AMOUNT_APPLIED_FROM,
        ACCTD_AMOUNT_APPLIED_FROM,
        BZ_LOAD_DATE
    )
    VALUES
    (
        X.CUSTOMER_SK,
        X.CUSTOMER_SITE_USE_SK,
        X.BUSINESS_UNIT_SK,
        X.LEDGER_SK,
        X.LEGAL_ENTITY_SK,
        X.RECEIPT_METHOD_SK,
        X.AR_COLLECTOR_SK,
        X.ACCOUNT_SK,
        X.CURRENCY_SK,
        X.APPLY_DATE_SK,
        X.GL_DATE_SK,
        X.RECEIPT_DATE_SK,
        X.DEPOSIT_DATE_SK,
        X.EXCHANGE_DATE_SK,
        X.DUE_DATE_SK,
        X.PAYMENT_SCHEDULE_GL_DATE_SK,
        X.TRX_DATE_SK,
        X.BILLING_DATE_SK,
        X.TERM_DUE_DATE_SK,
        X.AR_RECEIVABLE_APPLICATION_ID,
        X.AR_CASH_RECEIPT_ID,
        X.AR_PAYMENT_SCHEDULE_ID,
        X.AR_CUSTOMER_TRX_ID,
        X.APPLIED_CUSTOMER_TRX_ID,
        X.APPLIED_PAYMENT_SCHEDULE_ID,
        X.ORG_ID,
        X.SET_OF_BOOKS_ID,
        X.LEGAL_ENTITY_ID,
        X.RECEIPT_METHOD_ID,
        X.COLLECTOR_ID,
        X.CODE_COMBINATION_ID,
        X.TRX_TYPE_SEQ_ID,
        X.TERM_ID,
        X.RECEIPT_NUMBER,
        X.RECEIPT_STATUS,
        X.RECEIPT_CURRENCY_CODE,
        X.TRX_NUMBER,
        X.TRX_STATUS,
        X.TRX_CLASS,
        X.INVOICE_CURRENCY_CODE,
        X.PAYMENT_SCHEDULE_STATUS,
        X.APPLICATION_STATUS,
        X.RECEIPT_AMOUNT,
        X.RECEIPT_EXCHANGE_RATE,
        X.AMOUNT_DUE_ORIGINAL,
        X.AMOUNT_DUE_REMAINING,
        X.AMOUNT_APPLIED,
        X.AMOUNT_APPLIED_FROM,
        X.ACCTD_AMOUNT_APPLIED_FROM,
        X.BZ_LOAD_DATE
    )

WHEN NOT MATCHED BY SOURCE THEN
    DELETE;
GO
