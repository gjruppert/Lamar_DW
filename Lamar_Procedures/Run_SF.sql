/* =========================================================
   Run Salesforce load procedures
   Execute in dependency order: D_SF_OPPORTUNITY first (dim),
   then F_SF_OPPORTUNITY_LINE_ITEM (needs OPPORTUNITY_SK).

   First-time deploy: Silver tables (svo.D_SF_OPPORTUNITY, svo.F_SF_OPPORTUNITY_LINE_ITEM)
   are in Lamar_Index/Create_Tables_DDL.sql. Staging (bzo) only:
   1) 01_bzo_Opportunity_DDL.sql, 02_bzo_OpportunityLineItem_DDL.sql
   If not using Create_Tables_DDL.sql: 03_DDL_D_SF_OPPORTUNITY.sql (dim + plug row).
   Then deploy usp_Load_D_SF_OPPORTUNITY.sql, usp_Load_F_SF_OPPORTUNITY_LINE_ITEM.sql.
   Then run this script (or call from Update_or_Load_DW.sql).
   ========================================================= */

SET NOCOUNT ON;

PRINT '=== SF Load - Start ===';

PRINT '--- (1) D_SF_OPPORTUNITY ---';
EXEC svo.usp_Load_D_SF_OPPORTUNITY;

PRINT '--- (2) F_SF_OPPORTUNITY_LINE_ITEM ---';
EXEC svo.usp_Load_F_SF_OPPORTUNITY_LINE_ITEM;

PRINT '=== SF Load - Complete ===';
