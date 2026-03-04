# Lamar DW

SQL Server data warehouse ETL for Lamar Advertising (JDA/ERP integration). Bronze/Silver layer with staging (`bzo`) and dimensional model (`svo`) schemas.

**Subject areas:** AP (Accounts Payable), AR (Accounts Receivable), GL (General Ledger), OM (Order Management), RM (Revenue Management), SF (Salesforce), SL, SM (Subscription Management), OS (OneSource).

## Prerequisites

- SQL Server (tested against server **DEVDW**)
- `sqlcmd` in PATH (or SSMS with sqlcmd mode for `:r`)
- Target databases: `DW_BronzeSilver_DEV1`, `DW_BronzeSilver_PROD`, `Oracle_Reporting_P2`

## Directory structure

| Path | Description |
|------|-------------|
| `Deploy.sql` | Master deploy script (ETL infra → tables → all SPs) |
| `Lamar_Procedures/` | ETL stored procedures by subject area (00_Prerequisites … 10_SM) |
| `Lamar_Index/` | Table DDL and indexes (`Create_Tables_DDL.sql`) |
| `Deploy_DEVDW_*.bat` | Batch files to deploy to DEVDW databases |

## Deployment

From the **Code** directory:

```batch
sqlcmd -S DEVDW -d DW_BronzeSilver_DEV1 -i Deploy.sql
```

Or use batch files: `Deploy_DEVDW_DW_BronzeSilver_DEV1.bat`, `Deploy_DEVDW_DW_BronzeSilver_PROD.bat`, `Deploy_DEVDW_Oracle_Reporting_P2.bat`, or `Deploy_DEVDW_All.bat`.

## ETL execution order

After deploy, run loads in this order:

1. `Run_Common_Dimensions.sql`
2. `Run_GL.sql`
3. `Run_AP.sql`
4. `Run_OS.sql` (optional)
5. `05_SF/Run_SF.sql`
6. `Run_RM.sql`
7. `Run_OM.sql`
8. `Run_AR.sql`
9. `Run_SM.sql`

## Client sync (optional)

To sync dev code to client at `C:\Lamar.QP2.Reporting\OracleIngestion\Silver`:

```powershell
.\Sync_To_Client.ps1
```

## Documentation

- [Lamar_Procedures/DEPLOY.md](Lamar_Procedures/DEPLOY.md) – Deploy order, object list, client deployment
- [CODE_DIRECTORY.md](CODE_DIRECTORY.md) – Code layout and quick reference
