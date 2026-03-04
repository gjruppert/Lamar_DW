# Code Directory Structure

Current layout of the **Code** directory (updated after file moves).

## Active content

### Code root

| Path | Description |
|------|-------------|
| Deploy.sql | Master deploy script: ETL infra → Tables → all SPs in order (run from Code/ via sqlcmd) |

### Lamar_Procedures/

ETL stored procedures and runners. Execute in dependency order per area (Run_*.sql).

| Path | Description |
|------|-------------|
| **00_Prerequisites/** | |
| ETL_RUN.sql | ETL run logging table |
| ETL_WATERMARK.sql | Watermark table for incremental loads |
| **01_Common/** | Dimension and lookup loaders |
| 10_usp_Load_LINES_CODE_COMBO_LOOKUP.sql | Code combination lookup |
| 20_usp_Load_D_ACCOUNT.sql | |
| 30_usp_Load_D_BUSINESS_OFFERING.sql | |
| 40_usp_Load_D_COMPANY.sql | |
| 50_usp_Load_D_COST_CENTER.sql | |
| 60_usp_Load_D_INDUSTRY.sql | |
| 70_usp_Load_D_INTERCOMPANY.sql | |
| 80_usp_Load_D_BUSINESS_UNIT.sql | |
| 90_usp_Load_D_CALENDAR.sql | |
| 100_usp_Load_D_CURRENCY.sql | |
| 110_usp_Load_D_CUSTOMER_ACCOUNT.sql | |
| 120_usp_Load_D_CUSTOMER_ACCOUNT_SITE.sql | |
| 130_usp_Load_D_ITEM.sql | |
| 140_usp_Load_D_LEDGER.sql | |
| 150_usp_Load_D_LEGAL_ENTITY.sql | |
| 160_usp_Load_D_ORGANIZATION.sql | |
| 170_usp_Load_D_PARTY.sql | |
| 180_usp_Load_D_PARTY_CONTACT_POINT.sql | |
| 190_usp_Load_D_PARTY_SITE.sql | |
| 200_usp_Load_D_PAYMENT_METHOD.sql | |
| 210_usp_Load_D_PAYMENT_TERM.sql | |
| 220_usp_Load_D_SITE_USE.sql | |
| 230_usp_Load_D_VENDOR.sql | |
| 240_usp_Load_D_VENDOR_SITE.sql | |
| **02_AP/** | AP (Accounts Payable) |
| 10_usp_Load_D_AP_DISBURSEMENT_HEADER.sql | |
| 20_usp_Load_D_AP_INVOICE_HEADER.sql | |
| 30_usp_Load_F_AP_INVOICE_LINE_DISTRIBUTION.sql | |
| 40_usp_Load_F_AP_PAYMENTS.sql | |
| 50_usp_Load_F_AP_AGING_SNAPSHOT.sql | |
| **03_GL/** | GL (General Ledger) |
| 10_usp_Load_D_GL_HEADER.sql | |
| 20_usp_Load_F_GL_LINES.sql | |
| 30_usp_Load_F_GL_BALANCES.sql | |
| **04_OS/** | OneSource |
| 10_usp_Load_F_OS_BUDGET.sql | |
| **Root** | |
| DEPLOY.md | Deploy order and object list; see also Run_0_to_100_Reload.md |
| Run_0_to_100_Reload.md | Full reload checklist (prereqs, tables, watermarks, deploy, load) |
| Reset_Watermarks_For_Full_Reload.sql | Set watermarks to 1900-01-01 for full reload |
| Run_AP.sql | Run AP load sequence |
| Run_CodeComboTables.sql | Run code combo lookup load |
| Run_Common_Dimensions.sql | Run common dimension load sequence |
| Run_GL.sql | Run GL load sequence (D_GL_HEADER → F_GL_LINES → F_GL_BALANCES) |
| Run_OS.sql | Run OS load sequence |
| Check_ETL_Log.sql | ETL run log check |
| Check_Watermark.sql | Watermark check |

### Lamar_Index/

Table DDL and index definitions (single source for schema).

| Path | Description |
|------|-------------|
| Create_Tables_DDL.sql | Table CREATE scripts (dims + facts) |
| Create_Indexes.sql | Index CREATE scripts |
| g.sql | Generated/export script (reference) |

### Workspace

| Path | Description |
|------|-------------|
| Lamar_DW.code-workspace | VS Code / Cursor workspace file (includes Lamar_Index folder) |

---

## Archived (OLD/)

Legacy or superseded scripts; kept for reference. **Not** the active procedures.

| Folder | Contents |
|--------|----------|
| **OLD/AP_Lamar/** | Old AP DDL/load scripts |
| **OLD/AR_Lamar/** | AR dimensions and facts |
| **OLD/Common_Lamar/** | Old common dims + CodeComboTables |
| **OLD/GL_Lamar/** | Old GL create-dim and load scripts (10_GL_Create_Dim, 20_GL_Load_Balance_Fact, 30_GL_Load_Lines_Fact) |
| **OLD/Lamar_DW/** | Old DW loaders (SCD2/T1, Update_or_Load_DW, etc.) |
| **OLD/OM_Lamar/** | OM dimensions and facts |
| **OLD/OneSource/** | Old OS DDL (F_OS_BUDGET_DDL, OneStreamCSVDetails_DDL) |
| **OLD/RM_Lamar/** | RM dimensions and facts |
| **OLD/SF_Lamar/** | Salesforce opportunity objects |
| **OLD/SM_Lamar/** | Subscription billing objects |

---

## Quick reference

- **GL load order:** `Run_GL.sql` → (1) D_GL_HEADER, (2) F_GL_LINES, (3) F_GL_BALANCES.
- **Table DDL / indexes:** **Code/Lamar_Index/** (Create_Tables_DDL.sql, Create_Indexes.sql).
- **Deploy:** Run `Code/Deploy.sql` from Code/ (sqlcmd) or see `Lamar_Procedures/DEPLOY.md`.
- **Active procedure roots:** `Lamar_Procedures/00_Prerequisites`, `01_Common`, `02_AP`, `03_GL`, `04_OS` plus root `Run_*.sql` and `Check_*.sql`.
