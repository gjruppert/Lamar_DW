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
