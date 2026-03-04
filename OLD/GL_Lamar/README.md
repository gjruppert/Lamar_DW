# GL_Lamar – legacy reference

The **canonical load** for GL (General Ledger) is via **Lamar_Procedures**: run `Run_GL.sql`, which executes the incremental stored procedures in `03_GL/` in order.

- Table DDL lives in **Lamar_Index/Create_Tables_DDL.sql** (and indexes in **Create_Indexes.sql**).
- The scripts in this folder (GL_Lamar) are kept for **reference** only; they perform full reloads and are not used for production load.
