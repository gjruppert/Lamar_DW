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
