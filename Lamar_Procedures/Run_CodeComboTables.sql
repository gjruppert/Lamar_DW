/* =========================================================
   Run CodeComboTables Procedures
   Execute in order: LINES_CODE_COMBO_LOOKUP first, then dimensions.
   Prerequisites: Run 00_Prerequisites/ETL_RUN.sql and ETL_WATERMARK.sql
   Deploy: Run all 01_Common/xx_usp_Load_*.sql scripts before first run.
   ========================================================= */

SET NOCOUNT ON;

PRINT '=== CodeComboTables Load - Start ===';

-- 1. LINES_CODE_COMBO_LOOKUP (required before D_ACCOUNT and other CodeCombo dims)
PRINT '1/7: svo.usp_Load_LINES_CODE_COMBO_LOOKUP';
EXEC svo.usp_Load_LINES_CODE_COMBO_LOOKUP @batch_size = 10000;

-- 2. D_ACCOUNT (from GL value set hierarchy)
PRINT '2/7: svo.usp_Load_D_ACCOUNT';
EXEC svo.usp_Load_D_ACCOUNT @batch_size = 10000;

-- 3. D_BUSINESS_OFFERING
PRINT '3/7: svo.usp_Load_D_BUSINESS_OFFERING';
EXEC svo.usp_Load_D_BUSINESS_OFFERING @batch_size = 10000;

-- 4. D_COMPANY
PRINT '4/7: svo.usp_Load_D_COMPANY';
EXEC svo.usp_Load_D_COMPANY @batch_size = 10000;

-- 5. D_COST_CENTER
PRINT '5/7: svo.usp_Load_D_COST_CENTER';
EXEC svo.usp_Load_D_COST_CENTER @batch_size = 10000;

-- 6. D_INDUSTRY
PRINT '6/7: svo.usp_Load_D_INDUSTRY';
EXEC svo.usp_Load_D_INDUSTRY @batch_size = 10000;

-- 7. D_INTERCOMPANY
PRINT '7/7: svo.usp_Load_D_INTERCOMPANY';
EXEC svo.usp_Load_D_INTERCOMPANY @batch_size = 10000;

PRINT '=== CodeComboTables Load - Complete ===';

-- Optional: Check ETL log and watermarks
-- EXEC sp_executesql N'SELECT TOP 20 * FROM etl.ETL_RUN ORDER BY RUN_ID DESC;';
-- EXEC sp_executesql N'SELECT * FROM etl.ETL_WATERMARK WHERE TABLE_NAME LIKE ''svo.D_%'' OR TABLE_NAME = ''svo.LINES_CODE_COMBO_LOOKUP'';';
