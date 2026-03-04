/* =========================================================
   Run RM (Revenue Management) incremental load procedures
   Execute in dependency order: dimensions first, then fact.
   Prerequisites: Common dimensions (Run_Common_Dimensions.sql or equivalent).
   ========================================================= */

SET NOCOUNT ON;

PRINT '=== RM Load - Start ===';

PRINT '--- (1) D_RM_CONTRACT ---';
EXEC svo.usp_Load_D_RM_CONTRACT;

PRINT '--- (2) D_RM_SOURCE_DOCUMENT_LINE ---';
EXEC svo.usp_Load_D_RM_SOURCE_DOCUMENT_LINE;

PRINT '--- (3) D_RM_SOURCE_DOC_PRICING_LINE ---';
EXEC svo.usp_Load_D_RM_SOURCE_DOC_PRICING_LINE;

PRINT '--- (4) D_RM_BILLING_LINE ---';
EXEC svo.usp_Load_D_RM_BILLING_LINE;

PRINT '--- (5) D_RM_RULE ---';
EXEC svo.usp_Load_D_RM_RULE;

PRINT '--- (6) D_RM_SATISFACTION_METHOD ---';
EXEC svo.usp_Load_D_RM_SATISFACTION_METHOD;

PRINT '--- (7) D_RM_PERF_OBLIGATION ---';
EXEC svo.usp_Load_D_RM_PERF_OBLIGATION;

PRINT '--- (8) D_RM_PERF_OBLIGATION_LINE ---';
EXEC svo.usp_Load_D_RM_PERF_OBLIGATION_LINE;

PRINT '--- (9) D_RM_SATISFACTION_EVENT ---';
EXEC svo.usp_Load_D_RM_SATISFACTION_EVENT;

PRINT '--- (10) F_RM_SATISFACTION_EVENTS ---';
EXEC svo.usp_Load_F_RM_SATISFACTION_EVENTS;

PRINT '=== RM Load - Complete ===';
