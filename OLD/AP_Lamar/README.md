# AP_Lamar – legacy reference

The **canonical load** for AP (Accounts Payable) is via **Lamar_Procedures**: run `Run_AP.sql`, which executes the incremental stored procedures in `02_AP/` in order.

- Table DDL lives in **Lamar_Index/Create_Tables_DDL.sql** (and indexes in **Create_Indexes.sql**).
- The scripts in this folder (AP_Lamar) are kept for **reference** only; they perform full reloads and are not used for production load.
