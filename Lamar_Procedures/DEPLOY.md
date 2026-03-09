# Deploy order and object list

Reference for deploying ETL infrastructure, tables, and stored procedures. For full reload steps (including watermarks and data load), see [Run_0_to_100_Reload.md](Run_0_to_100_Reload.md).

## Deploy order

Run in this sequence (or use the master script once):

1. **ETL infrastructure** – `00_Prerequisites/ETL_RUN.sql`, `ETL_WATERMARK.sql`
2. **Tables** – `Lamar_Index/Create_Tables_DDL.sql` (includes svo.D_SF_OPPORTUNITY and svo.F_SF_OPPORTUNITY_LINE_ITEM).  
   For a full reload, then run `Reset_Watermarks_For_Full_Reload.sql`.  
   Staging (bzo) for Salesforce: run `05_SF/01_bzo_Opportunity_DDL.sql` and `02_bzo_OpportunityLineItem_DDL.sql` if not already created.
3. **Stored procedures** – all `*_usp_Load_*.sql` in 01_Common, 02_AP, 03_GL, 04_OS, 05_SF, 06_SL, 07_RM, 08_OM, 09_AR, 10_SM (order within folders does not affect deploy). Each load procedure defines and creates its own indexes (drop before full reload where applicable, create at end and in CATCH).
4. **Indexes** – Indexes are created by running the load procedures. `Lamar_Index/Create_Indexes.sql` is a stub (no longer creates indexes).

## Master deploy script

From the **Code** directory, run:

```bash
Deploy.bat
```

Or run directly:
```bash
sqlcmd -S <server> -d <database> -i Deploy.sql
```

`Deploy.bat` is interactive: you pick a server (DEVDW or SANDBOX1), then a database, then it runs `Deploy.sql`. Databases: `DW_BronzeSilver_DEV1`, `DW_BronzeSilver_PROD`, `Oracle_Reporting_P2`, `DW_BronzeSilver_TEST`.

Or open `Code/Deploy.sql` in SSMS and execute in **sqlcmd mode**. The script runs steps 1–3 above in order; step 4 (indexes) is commented out.

## Client deployment (DEVDW / StoredProcedures)

Dev stays at `C:\JDA\Lamar\Code`; the client uses `C:\Lamar.QP2.Reporting\OracleIngestion\Silver\StoredProcedures` and server **DEVDW** (databases: DW_BronzeSilver_DEV1, DW_BronzeSilver_PROD, Oracle_Reporting_P2, DW_BronzeSilver_TEST). To ship changes to the client:

1. **Sync from Code to client**  
   From the Code directory run:
   ```powershell
   .\Sync_To_Client.ps1
   ```
   Optional: `.\Sync_To_Client.ps1 -ClientRoot "C:\Lamar.QP2.Reporting\OracleIngestion\Silver"` (that path is the default).  
   This syncs `Lamar_Procedures` into `StoredProcedures` (00_Prerequisites … 10_SM), syncs `Lamar_Index` to the client Silver folder, and copies `Deploy_DEVDW.sql` as `Deploy.sql` and `Deploy.bat` (same interactive batch as Code) to the client root.

2. **Deploy on the client**  
   On the client (or from a machine that can reach DEVDW), open the client Silver directory and run:
   ```batch
   Deploy.bat
   ```
   You will be prompted to select a database: `DW_BronzeSilver_DEV1`, `DW_BronzeSilver_PROD`, `Oracle_Reporting_P2`, or `DW_BronzeSilver_TEST`.

When you add or remove procedures, update both `Deploy.sql` and `Deploy_DEVDW.sql` so the client deploy stays in sync.

## Objects deployed

| Group | Count | Location |
|-------|--------|----------|
| ETL infrastructure | 2 | 00_Prerequisites/ |
| Table DDL | 1 script | Lamar_Index/Create_Tables_DDL.sql |
| Common SPs | 24 | 01_Common/ |
| AP SPs | 6 | 02_AP/ |
| GL SPs | 3 | 03_GL/ |
| OS SPs | 1 | 04_OS/ |
| SF (Salesforce) | 2 SPs + 3 DDL | 05_SF/ (staging + D_SF_OPPORTUNITY DDL, then usp_Load_D_SF_OPPORTUNITY, usp_Load_F_SF_OPPORTUNITY_LINE_ITEM) |
| SL SPs | 1 | 06_SL/ (30_usp_Load_F_SL_JOURNAL_DISTRIBUTION) |
| RM SPs | 10 | 07_RM/ |
| OM SPs | 6 | 08_OM/ |
| AR SPs | 8 | 09_AR/ |
| SM SPs | 3 | 10_SM/ |
| Indexes | In load procs | Each load procedure creates its indexes; Create_Indexes.sql is a stub |

## Execution order (load data)

After deploy, run the load in this order each time:

1. **Common dimensions** – `Run_Common_Dimensions.sql` (CodeCombo + all other dims)
2. **GL** – `Run_GL.sql` (D_GL_HEADER → F_GL_LINES → F_GL_BALANCES)
3. **SL (Subledger)** – `Run_SL.sql` (F_SL_JOURNAL_DISTRIBUTION) – must run before AP
4. **AP** – `Run_AP.sql` (D_AP_DISBURSEMENT_HEADER → D_AP_INVOICE_HEADER → STG_AP_INVOICE_LINE_DISTRIBUTION → F_AP_INVOICE_LINE_DISTRIBUTION → F_AP_PAYMENTS → F_AP_AGING_SNAPSHOT)
5. **OneSource** (optional) – `Run_OS.sql`
6. **Salesforce** – `05_SF/Run_SF.sql` (D_SF_OPPORTUNITY → F_SF_OPPORTUNITY_LINE_ITEM)
7. **RM (Revenue Management)** – `Run_RM.sql` (D_RM_* dimensions → F_RM_SATISFACTION_EVENTS)
8. **OM (Order Management)** – `Run_OM.sql` (D_HOLD_CODE → D_SALES_REP → D_OM_ORDER_HEADER → D_OM_ORDER_LINE → F_OM_ORDER_LINE → F_OM_FULFILLMENT_LINE)
9. **AR (Accounts Receivable)** – `Run_AR.sql` (D_AR_TRANSACTION_TYPE → D_AR_TRANSACTION_SOURCE → D_AR_RECEIPT_METHOD → D_AR_COLLECTOR → D_AR_CASH_RECEIPT → D_AR_TRANSACTION → F_AR_TRANSACTION_LINE_DISTRIBUTION → F_AR_RECEIPTS)
10. **SM (Subscription Management)** – `Run_SM.sql` (D_SM_SUBSCRIPTION → D_SM_SUBSCRIPTION_PRODUCT → F_SM_BILLING)