

--IF OBJECT_ID('svo.F_AP_PAYMENTS','U') IS NOT NULL
--    DROP TABLE svo.F_AP_PAYMENTS;
--GO

--CREATE TABLE svo.F_AP_PAYMENTS
--(
--      AP_PAYMENTS_FACT_PK                     BIGINT IDENTITY(1,1) NOT NULL PRIMARY KEY

--    , AP_INVOICE_PAYMENTS_ALL_CHECK_ID        BIGINT NOT NULL
--    , AP_CHECKS_ALL_CHECK_NUMBER              VARCHAR(60) NULL

--    , AP_INVOICE_HEADER_SK                    BIGINT NOT NULL
--    , AP_DISBURSEMENT_HEADER_SK               BIGINT NOT NULL
--    , LEGAL_ENTITY_SK                         BIGINT NOT NULL
--    , VENDOR_SITE_SK                          BIGINT NOT NULL
--    , BUSINESS_UNIT_SK                        BIGINT NOT NULL
--    , LEDGER_SK                               BIGINT NOT NULL
--    , ACCOUNT_SK                              BIGINT NOT NULL
--    , BUSINESS_OFFERING_SK                    BIGINT NOT NULL
--    , COMPANY_SK                              BIGINT NOT NULL
--    , COST_CENTER_SK                          BIGINT NOT NULL
--    , INDUSTRY_SK                             BIGINT NOT NULL
--    , INTERCOMPANY_SK                         BIGINT NOT NULL
--    , INV_CURRENCY_SK                         BIGINT NOT NULL
--    , PAY_CURRENCY_SK                         BIGINT NOT NULL

--    , DUE_DATE_SK                             INT NULL
--    , ACCOUNTING_DATE_SK                      INT NULL
--    , EXCHANGE_DATE_SK                        INT NULL

--    , PAYMENT_DOCUMENT_ID                     BIGINT NOT NULL

--    , AP_PAYMENT_SCHEDULES_ALL_PAYMENT_PRIORITY     INT NULL
--    , AP_PAYMENT_SCHEDULES_ALL_PAYMENT_STATUS_FLAG  VARCHAR(30) NULL
--    , AP_INVOICE_PAYMENTS_ALL_POSTED_FLAG           VARCHAR(1) NULL

--    , AP_CHECKS_ALL_AMOUNT                    DECIMAL(29,4) NULL
--    , AP_INVOICE_PAYMENTS_ALL_INVOICE_PAYMENT_ID BIGINT NULL
--    , AP_PAYMENT_SCHEDULES_ALL_PAYMENT_NUM    INT NULL
--    , AP_PAYMENT_SCHEDULES_ALL_GROSS_AMOUNT   DECIMAL(29,4) NULL
--    , AP_PAYMENT_SCHEDULES_ALL_AMOUNT_REMAINING DECIMAL(29,4) NULL
--    , AP_INVOICE_PAYMENTS_ALL_AMOUNT          DECIMAL(29,4) NULL
--    , AP_INVOICE_PAYMENTS_ALL_AMOUNT_INV_CURR DECIMAL(29,4) NULL

--    , AP_PAYMENT_SCHEDULES_ALL_LAST_UPDATED_BY VARCHAR(64) NULL
--    , AP_INVOICE_PAYMENTS_ALL_LAST_UPDATED_BY  VARCHAR(64) NULL

--    , PAYMENT_LAST_UPDATE_DATE                DATE NULL
--    , SCHEDULE_LAST_UPDATE_DATE               DATE NULL

--    , BZ_LOAD_DATE_HEADER                     DATE NULL
--    , BZ_LOAD_DATE_SCHED                      DATE NULL
--    , BZ_LOAD_DATE_PAID                       DATE NULL

--    , SV_LOAD_DATE                            DATE NOT NULL DEFAULT CAST(GETDATE() AS DATE)
--)
--ON [FG_SilverFact];
--GO

TRUNCATE TABLE svo.F_AP_PAYMENTS
GO

INSERT INTO svo.F_AP_PAYMENTS
(
      AP_INVOICE_PAYMENTS_ALL_CHECK_ID
    , AP_CHECKS_ALL_CHECK_NUMBER
    , AP_INVOICE_HEADER_SK
    , AP_DISBURSEMENT_HEADER_SK
    , LEGAL_ENTITY_SK
    , VENDOR_SITE_SK
    , BUSINESS_UNIT_SK
    , LEDGER_SK
    , ACCOUNT_SK
    , BUSINESS_OFFERING_SK
    , COMPANY_SK
    , COST_CENTER_SK
    , INDUSTRY_SK
    , INTERCOMPANY_SK
    , INV_CURRENCY_SK
    , PAY_CURRENCY_SK
    , DUE_DATE_SK
    , ACCOUNTING_DATE_SK
    , EXCHANGE_DATE_SK
    , PAYMENT_DOCUMENT_ID
    , AP_PAYMENT_SCHEDULES_ALL_PAYMENT_PRIORITY
    , AP_PAYMENT_SCHEDULES_ALL_PAYMENT_STATUS_FLAG
    , AP_INVOICE_PAYMENTS_ALL_POSTED_FLAG
    , AP_CHECKS_ALL_AMOUNT
    , AP_INVOICE_PAYMENTS_ALL_INVOICE_PAYMENT_ID
    , AP_PAYMENT_SCHEDULES_ALL_PAYMENT_NUM
    , AP_PAYMENT_SCHEDULES_ALL_GROSS_AMOUNT
    , AP_PAYMENT_SCHEDULES_ALL_AMOUNT_REMAINING
    , AP_INVOICE_PAYMENTS_ALL_AMOUNT
    , AP_INVOICE_PAYMENTS_ALL_AMOUNT_INV_CURR
    , AP_PAYMENT_SCHEDULES_ALL_LAST_UPDATED_BY
    , AP_INVOICE_PAYMENTS_ALL_LAST_UPDATED_BY
    , PAYMENT_LAST_UPDATE_DATE
    , SCHEDULE_LAST_UPDATE_DATE
    , BZ_LOAD_DATE_HEADER
    , BZ_LOAD_DATE_SCHED
    , BZ_LOAD_DATE_PAID
    , SV_LOAD_DATE
)
SELECT 

      ISNULL(D.ApInvoicePaymentsAllCheckId,-1)
    , ISNULL(H.ApChecksAllCheckNumber,-1)
    , ISNULL(DAPIH.AP_INVOICE_HEADER_SK,0)
    , ISNULL(DAPDH.AP_DISBURSEMENT_HEADER_SK ,0)
    , ISNULL(LE.LEGAL_ENTITY_SK, 0)
    , ISNULL(V.VENDOR_SITE_SK,0)
    , ISNULL(BU.BUSINESS_UNIT_SK,0)
    , ISNULL(LDG.LEDGER_SK,0)
    , ISNULL(DA.ACCOUNT_SK, 0)
    , ISNULL(DBO.BUSINESS_OFFERING_SK, 0)
    , ISNULL(DCO.COMPANY_SK, 0)
    , ISNULL(DCC.COST_CENTER_SK, 0)
    , ISNULL(DI.INDUSTRY_SK, 0)
    , ISNULL(DIC.INTERCOMPANY_SK,0)
    , ISNULL(INVC.CURRENCY_SK,0)
    , ISNULL(PAYC.CURRENCY_SK,0)

    , CONVERT(INT, FORMAT(S.ApPaymentSchedulesAllDueDate,'yyyyMMdd'))
    , CONVERT(INT, FORMAT(D.ApInvoicePaymentsAllAccountingDate,'yyyyMMdd'))
    , CONVERT(INT, FORMAT(D.ApInvoicePaymentsAllExchangeDate,'yyyyMMdd'))

    , ISNULL(H.ApChecksAllPaymentDocumentId,0)

    , ISNULL(S.ApPaymentSchedulesAllPaymentPriority,0)
    , ISNULL(S.ApPaymentSchedulesAllPaymentStatusFlag,'U')
    , ISNULL(D.ApInvoicePaymentsAllPostedFlag,'U')

    , ISNULL(H.ApChecksAllAmount,0)
    , ISNULL(D.ApInvoicePaymentsAllInvoicePaymentId,-1)
    , ISNULL(S.ApPaymentSchedulesAllPaymentNum,0)
    , ISNULL(S.ApPaymentSchedulesAllGrossAmount,0)
    , ISNULL(S.ApPaymentSchedulesAllAmountRemaining,0)
    , ISNULL(D.ApInvoicePaymentsAllAmount,0)
    , ISNULL(D.ApInvoicePaymentsAllAmountInvCurr,0)

    , ISNULL(S.ApPaymentSchedulesAllLastUpdatedBy,'UNK')
    , ISNULL(D.ApInvoicePaymentsAllLastUpdatedBy,'UNK')
    , CAST(D.ApInvoicePaymentsAllLastUpdateDate AS DATE)
    , CAST(S.ApPaymentSchedulesAllLastUpdateDate AS DATE)

    , CAST(H.AddDateTime AS DATE)
    , CAST(S.AddDateTime AS DATE)
    , CAST(D.AddDateTime AS DATE)
    , CAST(GETDATE() AS DATE)          -- SV_LOAD_DATE
 FROM [bzo].[AP_InvoiceHeaderExtractPVO] IH                       
LEFT JOIN [bzo].[AP_PaidDisbursementScheduleExtractPVO] D       ON  IH.ApInvoicesInvoiceId  = D.ApInvoicePaymentsAllInvoiceId

LEFT JOIN [bzo].[AP_DisbursementHeaderExtractPVO] H             ON H.ApChecksAllCheckId = D.ApInvoicePaymentsAllCheckId
LEFT JOIN [bzo].[AP_InvoicePaymentScheduleExtractPVO] S         ON S.ApPaymentSchedulesAllInvoiceId = D.ApInvoicePaymentsAllInvoiceId
                                                                        AND S.ApPaymentSchedulesAllPaymentNum = D.ApInvoicePaymentsAllPaymentNum
LEFT JOIN svo.D_LEGAL_ENTITY      LE  ON LE.LEGAL_ENTITY_ID      = H.ApChecksAllLegalEntityId
LEFT JOIN svo.D_VENDOR_SITE       V   ON V.VENDOR_SITE_ID        = H.ApChecksAllVendorSiteId
LEFT JOIN svo.D_BUSINESS_UNIT     BU  ON BU.BUSINESS_UNIT_ID     = D.ApInvoicePaymentsAllOrgId
LEFT JOIN svo.D_LEDGER            LDG ON LDG.LEDGER_ID           = D.ApInvoicePaymentsAllSetOfBooksId

LEFT JOIN stage.LINES_CODE_COMBO_LOOKUP C                  ON CAST(IH.ApInvoicesAcctsPayCodeCombinationId AS BIGINT) = C.CODE_COMBINATION_BK
LEFT JOIN svo.D_ACCOUNT           DA  ON DA.ACCOUNT_ID            = C.ACCOUNT_ID
LEFT JOIN svo.D_BUSINESS_OFFERING DBO ON DBO.BUSINESS_OFFERING_ID = C.BUSINESSOFFERING_ID
LEFT JOIN svo.D_COMPANY           DCO ON DCO.COMPANY_ID           = C.COMPANY_ID
LEFT JOIN svo.D_COST_CENTER       DCC ON DCC.COST_CENTER_ID       = C.COSTCENTER_ID
LEFT JOIN svo.D_INDUSTRY          DI  ON DI.INDUSTRY_ID           = C.INDUSTRY_ID
LEFT JOIN svo.D_INTERCOMPANY      DIC ON DIC.INTERCOMPANY_ID      = C.INTERCOMPANY_ID

LEFT JOIN svo.D_CURRENCY INVC
       ON INVC.CURRENCY_ID =
          CONCAT(ISNULL(D.ApInvoicePaymentsAllInvoiceCurrencyCode,'UNK'),
                 CONVERT(CHAR(8), CONVERT(CHAR(8), ISNULL(D.ApInvoicePaymentsAllExchangeDate,'0001-01-01'),112)),
                 'Corporate')

LEFT JOIN svo.D_CURRENCY PAYC
       ON PAYC.CURRENCY_ID =
          CONCAT(ISNULL(D.ApInvoicePaymentsAllPaymentCurrencyCode,'UNK'),
                 CONVERT(CHAR(8), CONVERT(CHAR(8), ISNULL(D.ApInvoicePaymentsAllExchangeDate,'0001-01-01'),112)),
                 'Corporate')
LEFT JOIN svo.D_AP_INVOICE_HEADER DAPIH ON DAPIH.INVOICE_ID = IH.ApInvoicesInvoiceId
LEFT JOIN svo.D_AP_DISBURSEMENT_HEADER DAPDH ON DAPDH.CHECK_ID = D.ApInvoicePaymentsAllCheckId
 
--WHERE IH.ApInvoicesInvoiceId = 122174
 
    

-- Indexing
--CREATE NONCLUSTERED INDEX IX_F_AP_PAYMENTS_ACCT_DATE
--ON svo.F_AP_PAYMENTS
--(
--    ACCOUNTING_DATE_SK,
--    LEGAL_ENTITY_SK,
--    BUSINESS_UNIT_SK,
--    LEDGER_SK
--)
--INCLUDE
--(
--    AP_INVOICE_PAYMENTS_ALL_CHECK_ID,
--    AP_CHECKS_ALL_CHECK_NUMBER,
--    AP_INVOICE_PAYMENTS_ALL_AMOUNT,
--    AP_CHECKS_ALL_AMOUNT,
--    INV_CURRENCY_SK,
--    PAY_CURRENCY_SK
--)
--ON [FG_SilverFact];


--CREATE NONCLUSTERED INDEX IX_F_AP_PAYMENTS_CHECK
--ON svo.F_AP_PAYMENTS
--(
--    AP_INVOICE_PAYMENTS_ALL_CHECK_ID
--)
--INCLUDE
--(
--    AP_CHECKS_ALL_CHECK_NUMBER,
--    AP_INVOICE_PAYMENTS_ALL_INVOICE_PAYMENT_ID,
--    ACCOUNTING_DATE_SK,
--    LEGAL_ENTITY_SK,
--    BUSINESS_UNIT_SK,
--    LEDGER_SK,
--    AP_INVOICE_PAYMENTS_ALL_AMOUNT,
--    AP_CHECKS_ALL_AMOUNT
--)
--ON [FG_SilverFact];
