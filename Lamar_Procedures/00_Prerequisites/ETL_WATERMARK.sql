/* =========================================================
   ETL Watermark (create once)
   Tracks last processed timestamp per table for incremental loads.
   Seed rows inserted only when not present (preserves existing watermarks).
   ========================================================= */
IF SCHEMA_ID('etl') IS NULL
    EXEC('CREATE SCHEMA etl AUTHORIZATION dbo;');
GO

IF OBJECT_ID('etl.ETL_WATERMARK', 'U') IS NOT NULL DROP TABLE etl.ETL_WATERMARK;
BEGIN
    CREATE TABLE etl.ETL_WATERMARK
    (
        TABLE_NAME        SYSNAME        NOT NULL,
        LAST_WATERMARK    DATETIME2(7)   NOT NULL,
        CRE_DATE          DATETIME2(7)   NOT NULL
            CONSTRAINT DF_ETL_WATERMARK_CRE_DATE DEFAULT SYSDATETIME(),
        UDT_DATE          DATETIME2(7)   NOT NULL
            CONSTRAINT DF_ETL_WATERMARK_UDT_DATE DEFAULT SYSDATETIME(),
        CONSTRAINT PK_ETL_WATERMARK PRIMARY KEY CLUSTERED (TABLE_NAME)
    );
END
GO

-- Seed watermarks only if not present (idempotent; does not overwrite existing)
MERGE etl.ETL_WATERMARK AS tgt
USING (
    SELECT TABLE_NAME, LAST_WATERMARK FROM (VALUES
        ('svo.LINES_CODE_COMBO_LOOKUP', '1900-01-01'),
        ('svo.D_ACCOUNT', '1900-01-01'),
        ('svo.D_BUSINESS_OFFERING', '1900-01-01'),
        ('svo.D_COMPANY', '1900-01-01'),
        ('svo.D_COST_CENTER', '1900-01-01'),
        ('svo.D_INDUSTRY', '1900-01-01'),
        ('svo.D_INTERCOMPANY', '1900-01-01'),
        ('svo.D_GL_HEADER', '1900-01-01'),
        ('svo.F_GL_BALANCES', '1900-01-01'),
        ('svo.F_GL_LINES', '1900-01-01'),
        ('svo.D_BUSINESS_UNIT', '1900-01-01'),
        ('svo.D_CALENDAR', '1900-01-01'),
        ('svo.D_CURRENCY', '1900-01-01'),
        ('svo.D_CUSTOMER_ACCOUNT', '1900-01-01'),
        ('svo.D_CUSTOMER_ACCOUNT_SITE', '1900-01-01'),
        ('svo.D_ITEM', '1900-01-01'),
        ('svo.D_LEDGER', '1900-01-01'),
        ('svo.D_LEGAL_ENTITY', '1900-01-01'),
        ('svo.D_ORGANIZATION', '1900-01-01'),
        ('svo.D_PARTY', '1900-01-01'),
        ('svo.D_PARTY_CONTACT_POINT', '1900-01-01'),
        ('svo.D_PARTY_SITE', '1900-01-01'),
        ('svo.D_PAYMENT_METHOD', '1900-01-01'),
        ('svo.D_PAYMENT_TERM', '1900-01-01'),
        ('svo.D_SITE_USE', '1900-01-01'),
        ('svo.D_VENDOR', '1900-01-01'),
        ('svo.D_VENDOR_SITE', '1900-01-01'),
        ('svo.D_AP_DISBURSEMENT_HEADER', '1900-01-01'),
        ('svo.D_AP_INVOICE_HEADER', '1900-01-01'),
        ('svo.F_AP_AGING_SNAPSHOT', '1900-01-01'),
        ('svo.STG_AP_INVOICE_LINE_DISTRIBUTION', '1900-01-01'),
        ('svo.F_AP_INVOICE_LINE_DISTRIBUTION', '1900-01-01'),
        ('svo.F_AP_PAYMENTS', '1900-01-01'),
        ('svo.F_OS_BUDGET', '1900-01-01'),
        ('svo.D_AR_CASH_RECEIPT', '1900-01-01'),
        ('svo.D_AR_COLLECTOR', '1900-01-01'),
        ('svo.D_AR_RECEIPT_METHOD', '1900-01-01'),
        ('svo.D_AR_TRANSACTION_SOURCE', '1900-01-01'),
        ('svo.D_AR_TRANSACTION_TYPE', '1900-01-01'),
        ('svo.D_AR_TRANSACTION', '1900-01-01'),
        ('svo.F_AR_RECEIPTS', '1900-01-01'),
        ('svo.F_AR_TRANSACTION_LINE_DISTRIBUTION', '1900-01-01'),
        ('svo.D_HOLD_CODE', '1900-01-01'),
        ('svo.D_OM_ORDER_HEADER', '1900-01-01'),
        ('svo.D_OM_ORDER_LINE', '1900-01-01'),
        ('svo.D_SALES_REP', '1900-01-01'),
        ('svo.F_OM_FULFILLMENT_LINE', '1900-01-01'),
        ('svo.F_OM_ORDER_LINE', '1900-01-01'),
        ('svo.D_RM_BILLING_LINE', '1900-01-01'),
        ('svo.D_RM_CONTRACT', '1900-01-01'),
        ('svo.D_RM_PERF_OBLIGATION_LINE', '1900-01-01'),
        ('svo.D_RM_PERF_OBLIGATION', '1900-01-01'),
        ('svo.D_RM_RULE', '1900-01-01'),
        ('svo.D_RM_SATISFACTION_EVENT', '1900-01-01'),
        ('svo.D_RM_SATISFACTION_METHOD', '1900-01-01'),
        ('svo.D_RM_SOURCE_DOC_PRICING_LINE', '1900-01-01'),
        ('svo.D_RM_SOURCE_DOCUMENT_LINE', '1900-01-01'),
        ('svo.F_RM_SATISFACTION_EVENTS', '1900-01-01'),
        ('svo.D_SF_OPPORTUNITY', '1900-01-01'),
        ('svo.F_SF_OPPORTUNITY_LINE_ITEM', '1900-01-01'),
        ('svo.D_SM_SUBSCRIPTION_PRODUCT', '1900-01-01'),
        ('svo.D_SM_SUBSCRIPTION', '1900-01-01'),
        ('svo.F_SM_BILLING', '1900-01-01'),
        ('svo.F_SL_JOURNAL_DISTRIBUTION', '1900-01-01')
    ) AS v(TABLE_NAME, LAST_WATERMARK)
) AS src ON tgt.TABLE_NAME = src.TABLE_NAME
WHEN NOT MATCHED BY TARGET THEN
    INSERT (TABLE_NAME, LAST_WATERMARK)
    VALUES (src.TABLE_NAME, src.LAST_WATERMARK);
GO
