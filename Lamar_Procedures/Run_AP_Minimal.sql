/* =========================================================
   Run AP load with minimal prerequisites
   F_SL must run first (SL). Use when common dimensions and AP dimensions are already loaded.
   Prerequisites: Common dimensions, D_AP_INVOICE_HEADER (and its deps).
   ========================================================= */

SET NOCOUNT ON;

PRINT '=== AP Minimal Load - Start ===';

PRINT '--- (1) F_SL_JOURNAL_DISTRIBUTION ---';
EXEC svo.usp_Load_F_SL_JOURNAL_DISTRIBUTION;

PRINT '--- (2) D_AP_DISBURSEMENT_HEADER ---';
EXEC svo.usp_Load_D_AP_DISBURSEMENT_HEADER;

PRINT '--- (3) D_AP_INVOICE_HEADER ---';
EXEC svo.usp_Load_D_AP_INVOICE_HEADER;

PRINT '--- (4) STG_AP_INVOICE_LINE_DISTRIBUTION ---';
EXEC svo.usp_Load_STG_AP_INVOICE_LINE_DISTRIBUTION;

PRINT '--- (5) F_AP_INVOICE_LINE_DISTRIBUTION ---';
EXEC svo.usp_Load_F_AP_INVOICE_LINE_DISTRIBUTION;

PRINT '--- (6) F_AP_PAYMENTS ---';
EXEC svo.usp_Load_F_AP_PAYMENTS;

PRINT '--- (7) F_AP_AGING_SNAPSHOT ---';
EXEC svo.usp_Load_F_AP_AGING_SNAPSHOT;

PRINT '=== AP Minimal Load - Complete ===';
