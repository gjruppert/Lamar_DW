/* =========================================================
   Deploy for DEVDW / client – same order as Deploy.sql, paths for client layout.
   Client file source: StoredProcedures (not Lamar_Procedures). Run with current directory = client Silver root.
   Server: DEVDW. Databases: DW_BronzeSilver_DEV1, DW_BronzeSilver_PROD, Oracle_Reporting_P2, etc.
   Example: sqlcmd -S DEVDW -d DW_BronzeSilver_DEV1 -i Deploy.sql
   Or run Deploy.bat from the client Silver directory (sync copies this script as Deploy.sql).
   ========================================================= */

SET NOCOUNT ON;

PRINT '=== (1/4) ETL infrastructure ===';
:r StoredProcedures/00_Prerequisites/ETL_RUN.sql
:r StoredProcedures/00_Prerequisites/ETL_WATERMARK.sql

PRINT '=== (2/4) Tables (drops existing svo.*) ===';
:r Lamar_Index/Create_Tables_DDL.sql

PRINT '=== (3/4) Stored procedures ===';
GO
PRINT '--- 01_Common ---';
GO
:r StoredProcedures/01_Common/10_usp_Load_LINES_CODE_COMBO_LOOKUP.sql
:r StoredProcedures/01_Common/20_usp_Load_D_ACCOUNT.sql
:r StoredProcedures/01_Common/30_usp_Load_D_BUSINESS_OFFERING.sql
:r StoredProcedures/01_Common/40_usp_Load_D_COMPANY.sql
:r StoredProcedures/01_Common/50_usp_Load_D_COST_CENTER.sql
:r StoredProcedures/01_Common/60_usp_Load_D_INDUSTRY.sql
:r StoredProcedures/01_Common/70_usp_Load_D_INTERCOMPANY.sql
:r StoredProcedures/01_Common/80_usp_Load_D_BUSINESS_UNIT.sql
:r StoredProcedures/01_Common/90_usp_Load_D_CALENDAR.sql
:r StoredProcedures/01_Common/100_usp_Load_D_CURRENCY.sql
:r StoredProcedures/01_Common/110_usp_Load_D_CUSTOMER_ACCOUNT.sql
:r StoredProcedures/01_Common/120_usp_Load_D_CUSTOMER_ACCOUNT_SITE.sql
:r StoredProcedures/01_Common/130_usp_Load_D_ITEM.sql
:r StoredProcedures/01_Common/140_usp_Load_D_LEDGER.sql
:r StoredProcedures/01_Common/150_usp_Load_D_LEGAL_ENTITY.sql
:r StoredProcedures/01_Common/160_usp_Load_D_ORGANIZATION.sql
:r StoredProcedures/01_Common/170_usp_Load_D_PARTY.sql
:r StoredProcedures/01_Common/180_usp_Load_D_PARTY_CONTACT_POINT.sql
:r StoredProcedures/01_Common/190_usp_Load_D_PARTY_SITE.sql
:r StoredProcedures/01_Common/200_usp_Load_D_PAYMENT_METHOD.sql
:r StoredProcedures/01_Common/210_usp_Load_D_PAYMENT_TERM.sql
:r StoredProcedures/01_Common/220_usp_Load_D_SITE_USE.sql
:r StoredProcedures/01_Common/230_usp_Load_D_VENDOR.sql
:r StoredProcedures/01_Common/240_usp_Load_D_VENDOR_SITE.sql
PRINT '--- 02_AP ---';
GO
:r StoredProcedures/02_AP/10_usp_Load_D_AP_DISBURSEMENT_HEADER.sql
:r StoredProcedures/02_AP/20_usp_Load_D_AP_INVOICE_HEADER.sql
:r StoredProcedures/02_AP/30_usp_Load_STG_AP_INVOICE_LINE_DISTRIBUTION.sql
:r StoredProcedures/02_AP/35_usp_Load_F_AP_INVOICE_LINE_DISTRIBUTION.sql
:r StoredProcedures/02_AP/40_usp_Load_F_AP_PAYMENTS.sql
:r StoredProcedures/02_AP/50_usp_Load_F_AP_AGING_SNAPSHOT.sql
PRINT '--- 03_GL ---';
GO
:r StoredProcedures/03_GL/10_usp_Load_D_GL_HEADER.sql
:r StoredProcedures/03_GL/20_usp_Load_F_GL_LINES.sql
:r StoredProcedures/03_GL/30_usp_Load_F_GL_BALANCES.sql
PRINT '--- 04_OS ---';
GO
:r StoredProcedures/04_OS/10_usp_Load_F_OS_BUDGET.sql
GO
PRINT '--- 05_SF ---';
GO
:r StoredProcedures/05_SF/usp_Load_D_SF_OPPORTUNITY.sql
:r StoredProcedures/05_SF/usp_Load_F_SF_OPPORTUNITY_LINE_ITEM.sql
PRINT '--- 06_SL ---';
GO
:r StoredProcedures/06_SL/30_usp_Load_F_SL_JOURNAL_DISTRIBUTION.sql
PRINT '--- 07_RM ---';
GO
:r StoredProcedures/07_RM/10_usp_Load_D_RM_CONTRACT.sql
GO
:r StoredProcedures/07_RM/20_usp_Load_D_RM_SOURCE_DOCUMENT_LINE.sql
GO
:r StoredProcedures/07_RM/30_usp_Load_D_RM_SOURCE_DOC_PRICING_LINE.sql
GO
:r StoredProcedures/07_RM/40_usp_Load_D_RM_BILLING_LINE.sql
GO
:r StoredProcedures/07_RM/50_usp_Load_D_RM_RULE.sql
GO
:r StoredProcedures/07_RM/60_usp_Load_D_RM_SATISFACTION_METHOD.sql
GO
:r StoredProcedures/07_RM/70_usp_Load_D_RM_PERF_OBLIGATION.sql
GO
:r StoredProcedures/07_RM/80_usp_Load_D_RM_PERF_OBLIGATION_LINE.sql
GO
:r StoredProcedures/07_RM/90_usp_Load_D_RM_SATISFACTION_EVENT.sql
GO
:r StoredProcedures/07_RM/100_usp_Load_F_RM_SATISFACTION_EVENTS.sql
GO

PRINT '--- 08_OM ---';
GO
:r StoredProcedures/08_OM/10_usp_Load_D_HOLD_CODE.sql
:r StoredProcedures/08_OM/20_usp_Load_D_SALES_REP.sql
:r StoredProcedures/08_OM/30_usp_Load_D_OM_ORDER_HEADER.sql
:r StoredProcedures/08_OM/40_usp_Load_D_OM_ORDER_LINE.sql
:r StoredProcedures/08_OM/50_usp_Load_F_OM_ORDER_LINE.sql
:r StoredProcedures/08_OM/60_usp_Load_F_OM_FULFILLMENT_LINE.sql
PRINT '--- 09_AR ---';
GO
:r StoredProcedures/09_AR/10_usp_Load_D_AR_TRANSACTION_TYPE.sql
:r StoredProcedures/09_AR/20_usp_Load_D_AR_TRANSACTION_SOURCE.sql
:r StoredProcedures/09_AR/30_usp_Load_D_AR_RECEIPT_METHOD.sql
:r StoredProcedures/09_AR/40_usp_Load_D_AR_COLLECTOR.sql
:r StoredProcedures/09_AR/50_usp_Load_D_AR_CASH_RECEIPT.sql
:r StoredProcedures/09_AR/60_usp_Load_D_AR_TRANSACTION.sql
:r StoredProcedures/09_AR/70_usp_Load_F_AR_TRANSACTION_LINE_DISTRIBUTION.sql
:r StoredProcedures/09_AR/80_usp_Load_F_AR_RECEIPTS.sql
PRINT '--- 10_SM ---';
GO
:r StoredProcedures/10_SM/10_usp_Load_D_SM_SUBSCRIPTION.sql
:r StoredProcedures/10_SM/20_usp_Load_D_SM_SUBSCRIPTION_PRODUCT.sql
:r StoredProcedures/10_SM/30_usp_Load_F_SM_BILLING.sql

PRINT '=== (4/4) Optional: indexes (uncomment to run after first load) ===';
-- :r Lamar_Index/Create_Indexes.sql

PRINT '=== Deploy complete ===';
