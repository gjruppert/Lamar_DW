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
