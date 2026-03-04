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
EXEC svo.usp_Load_LINES_CODE_COMBO_LOOKUP @batch_size = 10000;
EXEC svo.usp_Load_D_ACCOUNT;
EXEC svo.usp_Load_D_BUSINESS_OFFERING;
EXEC svo.usp_Load_D_COMPANY;
EXEC svo.usp_Load_D_COST_CENTER;
EXEC svo.usp_Load_D_INDUSTRY;
EXEC svo.usp_Load_D_INTERCOMPANY;

-- Other common dimensions
PRINT '--- Other Common Dimensions ---';
EXEC svo.usp_Load_D_BUSINESS_UNIT;
EXEC svo.usp_Load_D_CALENDAR @batch_size = 10000;
EXEC svo.usp_Load_D_CURRENCY @batch_size = 10000;
EXEC svo.usp_Load_D_CUSTOMER_ACCOUNT;
EXEC svo.usp_Load_D_CUSTOMER_ACCOUNT_SITE;
EXEC svo.usp_Load_D_ITEM;
EXEC svo.usp_Load_D_LEDGER;
EXEC svo.usp_Load_D_LEGAL_ENTITY;
EXEC svo.usp_Load_D_ORGANIZATION;
EXEC svo.usp_Load_D_PARTY;
EXEC svo.usp_Load_D_PARTY_CONTACT_POINT;
EXEC svo.usp_Load_D_PARTY_SITE;
EXEC svo.usp_Load_D_PAYMENT_METHOD;
EXEC svo.usp_Load_D_PAYMENT_TERM;
EXEC svo.usp_Load_D_SITE_USE;
EXEC svo.usp_Load_D_VENDOR;
EXEC svo.usp_Load_D_VENDOR_SITE;

PRINT '=== Common Dimensions Load - Complete ===';
