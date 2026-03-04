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
