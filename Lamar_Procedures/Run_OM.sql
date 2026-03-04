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
