# 0-to-100 Full Reload Checklist

Use this when loading from empty (nothing) to all tables populated.

---

## 1. Prerequisites (one-time or ensure exist)

- **Database:** Target DB (e.g. DW_BronzeSilver_PROD) with **bzo** and **svo** schemas and source PVOs populated (e.g. `bzo.GL_JournalHeaderExtractPVO`, `bzo.GL_CodeCombinationExtractPVO`, etc.).
- **File groups** (if your DDL uses them): `FG_SilverDim`, `FG_SilverFact`, `FG_SilverMisc` exist, or edit `Lamar_Index/Create_Tables_DDL.sql` to use your file groups / remove `ON ...`.

---

## 2. Run order (execute in this sequence)

### Step A: ETL infrastructure (once per DB)

```sql
-- In Lamar_Procedures/00_Prerequisites/
:r ETL_RUN.sql
:r ETL_WATERMARK.sql
```

Or run the two files manually. This creates `etl.ETL_RUN` and `etl.ETL_WATERMARK` and seeds watermarks at `1900-01-01` for new table names.

### Step B: Create all tables (drops existing svo tables)

```sql
-- In Lamar_Index/
:r Create_Tables_DDL.sql
```

Or run `Create_Tables_DDL.sql` manually. This **drops** all `svo.*` tables and recreates them empty. **etl.ETL_WATERMARK is not dropped**; existing watermark rows remain.

### Step C: Reset watermarks for full reload (required for 0-to-100)

After creating tables, watermarks may still hold the last run time. Set them to `1900-01-01` so every load does a full load (e.g. F_GL_LINES truncates and reloads):

```sql
-- In Lamar_Procedures/
:r Reset_Watermarks_For_Full_Reload.sql
```

Or run `Reset_Watermarks_For_Full_Reload.sql` manually.

### Step D: Deploy stored procedures (once per deploy)

Run every `*_usp_Load_*.sql` under `Lamar_Procedures/01_Common/`, `02_AP/`, `03_GL/`, `04_OS/` so the procedures exist in the database. Order within each folder does not matter for deploy; run order for **execution** is below.

### Step E: Load data (this order)

1. **Common dimensions** (CodeCombo + all other dims; LINES_CODE_COMBO_LOOKUP must run before D_ACCOUNT and other code-combo dims):

   ```sql
   :r Run_Common_Dimensions.sql
   ```

2. **GL** (depends on common dims and LINES_CODE_COMBO_LOOKUP):

   ```sql
   :r Run_GL.sql
   ```

3. **AP** (depends on common dims, including D_VENDOR_SITE, D_PAYMENT_METHOD, D_PAYMENT_TERM):

   ```sql
   :r Run_AP.sql
   ```

4. **OneSource** (optional; depends on D_COMPANY, D_BUSINESS_OFFERING, D_COST_CENTER and source `dbo.OneStreamCSVDetails`):

   ```sql
   :r Run_OS.sql
   ```

### Step F: Create indexes (after load, optional but recommended)

```sql
-- In Lamar_Index/
:r Create_Indexes.sql
```

Run after the first full load so inserts are faster. Can be run before load if you prefer; some SPs (e.g. F_GL_LINES) temporarily drop certain indexes during load and recreate them.

---

## 3. Quick “am I ready?” check

- [ ] **00_Prerequisites**: `ETL_RUN.sql` and `ETL_WATERMARK.sql` have been run (etl.ETL_RUN and etl.ETL_WATERMARK exist).
- [ ] **Tables**: `Create_Tables_DDL.sql` has been run (all svo.* tables exist and are empty or as desired).
- [ ] **Watermarks**: `Reset_Watermarks_For_Full_Reload.sql` has been run so all svo.* watermarks are `1900-01-01` (required for full reload behavior).
- [ ] **Procedures**: All `*_usp_Load_*.sql` scripts in 01_Common, 02_AP, 03_GL, 04_OS have been executed (procedures exist in DB).
- [ ] **Source data**: bzo PVOs (e.g. GL, AP, Calendar, Currency) are populated for the scope you are loading.

Then run: **Run_Common_Dimensions.sql** → **Run_GL.sql** → **Run_AP.sql** (and **Run_OS.sql** if using OneSource).
