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
