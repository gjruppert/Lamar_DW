/* =========================================================
   Run All Common Dimension Procedures
   Execute in dependency order. Run CodeComboTables first, then others.
   Prerequisites: Run 00_Prerequisites/ETL_RUN.sql and ETL_WATERMARK.sql
   Deploy: Run all 01_Common/xx_usp_Load_*.sql scripts before first run.
   ========================================================= */

SET NOCOUNT ON;

PRINT '=== Common Dimensions Load - Start ===';

-- CodeComboTables (run first - LINES_CODE_COMBO_LOOKUP required before D_ACCOUNT)
PRINT '--- CodeComboTables ---';
EXEC svo.usp_Load_LINES_CODE_COMBO_LOOKUP;
EXEC svo.usp_Load_D_ACCOUNT;
EXEC svo.usp_Load_D_BUSINESS_OFFERING;
EXEC svo.usp_Load_D_COMPANY;
EXEC svo.usp_Load_D_COST_CENTER;
EXEC svo.usp_Load_D_INDUSTRY;
EXEC svo.usp_Load_D_INTERCOMPANY;

-- Other common dimensions
PRINT '--- Other Common Dimensions ---';
EXEC svo.usp_Load_D_BUSINESS_UNIT;
EXEC svo.usp_Load_D_CALENDAR;
EXEC svo.usp_Load_D_CURRENCY;
EXEC svo.usp_Load_D_LEDGER;
EXEC svo.usp_Load_D_LEGAL_ENTITY;
EXEC svo.usp_Load_D_PAYMENT_METHOD;
EXEC svo.usp_Load_D_PAYMENT_TERM;
EXEC svo.usp_Load_D_VENDOR_SITE;

EXEC svo.usp_Load_D_SITE_USE;
EXEC svo.usp_Load_D_VENDOR;
EXEC svo.usp_Load_D_ORGANIZATION;
EXEC svo.usp_Load_D_PARTY;
EXEC svo.usp_Load_D_PARTY_CONTACT_POINT;
EXEC svo.usp_Load_D_PARTY_SITE;
EXEC svo.usp_Load_D_CUSTOMER_ACCOUNT;
EXEC svo.usp_Load_D_CUSTOMER_ACCOUNT_SITE;
EXEC svo.usp_Load_D_ITEM;

PRINT '=== Common Dimensions Load - Complete ===';
/* =========================================================
   Run GL incremental load procedures
   Execute in dependency order: D_GL_HEADER first (SCD2 dim),
   then F_GL_LINES (needs header SK), then F_GL_BALANCES.
   Prerequisites: Common dimensions and LINES_CODE_COMBO_LOOKUP
   (Run_Common_Dimensions.sql or equivalent).
   ========================================================= */

SET NOCOUNT ON;

PRINT '=== GL Load - Start ===';

PRINT '--- (1) D_GL_HEADER ---';
EXEC svo.usp_Load_D_GL_HEADER;

PRINT '--- (2) F_GL_LINES ---';
EXEC svo.usp_Load_F_GL_LINES;

PRINT '--- (3) F_GL_BALANCES ---';
EXEC svo.usp_Load_F_GL_BALANCES;

PRINT '=== GL Load - Complete ===';

/* =========================================================
   Run SL (Subledger) incremental load procedures
   Prerequisites: Common dimensions and LINES_CODE_COMBO_LOOKUP
   (Run_Common_Dimensions.sql or equivalent).
   ========================================================= */

SET NOCOUNT ON;

PRINT '=== SL Load - Start ===';

PRINT '--- (1) F_SL_JOURNAL_DISTRIBUTION ---';
EXEC svo.usp_Load_F_SL_JOURNAL_DISTRIBUTION;

PRINT '=== SL Load - Complete ===';


/* =========================================================
   Run AP incremental load procedures
   Execute in dependency order: dimensions first, then facts,
   F_AP_AGING_SNAPSHOT last (derived from F_AP_PAYMENTS).
   Prerequisites: Common dimensions and LINES_CODE_COMBO_LOOKUP
   (Run_Common_Dimensions.sql or equivalent).
   ========================================================= */

SET NOCOUNT ON;

PRINT '=== AP Load - Start ===';

PRINT '--- (1) D_AP_DISBURSEMENT_HEADER ---';
EXEC svo.usp_Load_D_AP_DISBURSEMENT_HEADER;

PRINT '--- (2) D_AP_INVOICE_HEADER ---';
EXEC svo.usp_Load_D_AP_INVOICE_HEADER;

PRINT '--- (3) STG_AP_INVOICE_LINE_DISTRIBUTION ---';
EXEC svo.usp_Load_STG_AP_INVOICE_LINE_DISTRIBUTION;

PRINT '--- (4) F_AP_INVOICE_LINE_DISTRIBUTION ---';
EXEC svo.usp_Load_F_AP_INVOICE_LINE_DISTRIBUTION;

PRINT '--- (5) F_AP_PAYMENTS ---';
EXEC svo.usp_Load_F_AP_PAYMENTS;

PRINT '--- (6) F_AP_AGING_SNAPSHOT ---';
EXEC svo.usp_Load_F_AP_AGING_SNAPSHOT;

PRINT '=== AP Load - Complete ===';
/* =========================================================
   Run OneSource load procedures
   F_OS_BUDGET: full refresh from dbo.OneStreamCSVDetails.
   Prerequisites: Common dimensions (Run_Common_Dimensions.sql or equivalent)
   so D_COMPANY, D_BUSINESS_OFFERING, D_COST_CENTER exist with CURR_IND='Y'.
   ========================================================= */

SET NOCOUNT ON;

PRINT '=== OneSource Load - Start ===';

PRINT '--- (1) F_OS_BUDGET ---';
EXEC svo.usp_Load_F_OS_BUDGET;

PRINT '=== OneSource Load - Complete ===';


/* =========================================================
   Run Salesforce load procedures
   Execute in dependency order: D_SF_OPPORTUNITY first (dim),
   then F_SF_OPPORTUNITY_LINE_ITEM (needs OPPORTUNITY_SK).
   Prerequisites: svo.D_SF_OPPORTUNITY table exists (run DDL once).
   ========================================================= */

SET NOCOUNT ON;

PRINT '=== SF Load - Start ===';

PRINT '--- (1) D_SF_OPPORTUNITY ---';
EXEC svo.usp_Load_D_SF_OPPORTUNITY;

PRINT '--- (2) F_SF_OPPORTUNITY_LINE_ITEM ---';
EXEC svo.usp_Load_F_SF_OPPORTUNITY_LINE_ITEM;

PRINT '=== SF Load - Complete ===';


/* =========================================================
   Run OM (Order Management) incremental load procedures
   Execute in dependency order: dimensions first, then facts.
   Prerequisites: Common dimensions (Run_Common_Dimensions.sql or equivalent).
   ========================================================= */

SET NOCOUNT ON;

PRINT '=== OM Load - Start ===';

PRINT '--- (1) D_HOLD_CODE ---';
EXEC svo.usp_Load_D_HOLD_CODE;

PRINT '--- (2) D_SALES_REP ---';
EXEC svo.usp_Load_D_SALES_REP;

PRINT '--- (3) D_OM_ORDER_HEADER ---';
EXEC svo.usp_Load_D_OM_ORDER_HEADER;

PRINT '--- (4) D_OM_ORDER_LINE ---';
EXEC svo.usp_Load_D_OM_ORDER_LINE;

PRINT '--- (5) F_OM_ORDER_LINE ---';
EXEC svo.usp_Load_F_OM_ORDER_LINE;

PRINT '--- (6) F_OM_FULFILLMENT_LINE ---';
EXEC svo.usp_Load_F_OM_FULFILLMENT_LINE;

PRINT '=== OM Load - Complete ===';

/* =========================================================
   Run RM (Revenue Management) incremental load procedures
   Execute in dependency order: dimensions first, then fact.
   Prerequisites: Common dimensions (Run_Common_Dimensions.sql or equivalent).
   ========================================================= */

SET NOCOUNT ON;

PRINT '=== RM Load - Start ===';

PRINT '--- (1) D_RM_CONTRACT ---';
EXEC svo.usp_Load_D_RM_CONTRACT;

PRINT '--- (2) D_RM_SOURCE_DOCUMENT_LINE ---';
EXEC svo.usp_Load_D_RM_SOURCE_DOCUMENT_LINE;

PRINT '--- (3) D_RM_SOURCE_DOC_PRICING_LINE ---';
EXEC svo.usp_Load_D_RM_SOURCE_DOC_PRICING_LINE;

PRINT '--- (4) D_RM_BILLING_LINE ---';
EXEC svo.usp_Load_D_RM_BILLING_LINE;

PRINT '--- (5) D_RM_RULE ---';
EXEC svo.usp_Load_D_RM_RULE;

PRINT '--- (6) D_RM_SATISFACTION_METHOD ---';
EXEC svo.usp_Load_D_RM_SATISFACTION_METHOD;

PRINT '--- (7) D_RM_PERF_OBLIGATION ---';
EXEC svo.usp_Load_D_RM_PERF_OBLIGATION;

PRINT '--- (8) D_RM_PERF_OBLIGATION_LINE ---';
EXEC svo.usp_Load_D_RM_PERF_OBLIGATION_LINE;

PRINT '--- (9) D_RM_SATISFACTION_EVENT ---';
EXEC svo.usp_Load_D_RM_SATISFACTION_EVENT;

PRINT '--- (10) F_RM_SATISFACTION_EVENTS ---';
EXEC svo.usp_Load_F_RM_SATISFACTION_EVENTS;

PRINT '=== RM Load - Complete ===';

/* =========================================================
   Run AR (Accounts Receivable) incremental load procedures
   Execute in dependency order: dimensions first, then facts.
   Prerequisites: Common dimensions and LINES_CODE_COMBO_LOOKUP
   (Run_Common_Dimensions.sql or equivalent).
   ========================================================= */

SET NOCOUNT ON;

PRINT '=== AR Load - Start ===';

PRINT '--- (1) D_AR_TRANSACTION_TYPE ---';
EXEC svo.usp_Load_D_AR_TRANSACTION_TYPE;

PRINT '--- (2) D_AR_TRANSACTION_SOURCE ---';
EXEC svo.usp_Load_D_AR_TRANSACTION_SOURCE;

PRINT '--- (3) D_AR_RECEIPT_METHOD ---';
EXEC svo.usp_Load_D_AR_RECEIPT_METHOD;

PRINT '--- (4) D_AR_COLLECTOR ---';
EXEC svo.usp_Load_D_AR_COLLECTOR;

PRINT '--- (5) D_AR_CASH_RECEIPT ---';
EXEC svo.usp_Load_D_AR_CASH_RECEIPT;

PRINT '--- (6) D_AR_TRANSACTION ---';
EXEC svo.usp_Load_D_AR_TRANSACTION;

PRINT '--- (7) F_AR_TRANSACTION_LINE_DISTRIBUTION ---';
EXEC svo.usp_Load_F_AR_TRANSACTION_LINE_DISTRIBUTION;

PRINT '--- (8) F_AR_RECEIPTS ---';
EXEC svo.usp_Load_F_AR_RECEIPTS;

PRINT '=== AR Load - Complete ===';

/* =========================================================
   Run SM (Subscription Management) incremental load procedures
   Execute in dependency order: dimensions first, then fact.
   Prerequisites: Common dimensions (Run_Common_Dimensions.sql or equivalent).
   ========================================================= */

SET NOCOUNT ON;

PRINT '=== SM Load - Start ===';

PRINT '--- (1) D_SM_SUBSCRIPTION ---';
EXEC svo.usp_Load_D_SM_SUBSCRIPTION;

PRINT '--- (2) D_SM_SUBSCRIPTION_PRODUCT ---';
EXEC svo.usp_Load_D_SM_SUBSCRIPTION_PRODUCT;

PRINT '--- (3) F_SM_BILLING ---';
EXEC svo.usp_Load_F_SM_BILLING;

PRINT '=== SM Load - Complete ===';
