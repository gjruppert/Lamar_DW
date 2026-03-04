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
