USE [Oracle_Reporting_P2];
GO

/* =====================================================================
   DIM: svo.D_ORGANIZATION (Hybrid Type1/Type2 SCD)
   BK: ORGANIZATION_ID
   Source (synonym-based / DB independent):
     - src.bzo_PIM_InvOrgParametersExtractPVO

   Hybrid (sane default for orgs):
     - Type 1: ORGANIZATION_CODE, INVENTORY_FLAG
     - Type 2: BUSINESS_UNIT_ID, LEGAL_ENTITY_ID, MASTER_ORGANIZATION_ID, SOURCE_ORGANIZATION_ID

   Locked rules applied:
     - Stored proc in svo schema
     - BZ_LOAD_DATE never NULL: COALESCE(CAST(AddDateTime AS date), CAST(GETDATE() AS date))
     - SCD2 fields: EFF_DATE, END_DATE, CRE_DATE, UDT_DATE, CURR_IND
     - ETL run logging (etl.ETL_RUN)
     - Idempotent + transactional
   ===================================================================== */

/* =========================
   0) Logging (create once)
   ========================= */
IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'etl')
    EXEC('CREATE SCHEMA etl AUTHORIZATION dbo;');
GO

IF OBJECT_ID(N'etl.ETL_RUN', 'U') IS NULL
BEGIN
    CREATE TABLE etl.ETL_RUN
    (
          RUN_ID          bigint IDENTITY(1,1) NOT NULL CONSTRAINT PK_ETL_RUN PRIMARY KEY
        , PROC_NAME       sysname              NOT NULL
        , TARGET_OBJECT   sysname              NOT NULL
        , ASOF_DATE       date                 NULL
        , START_DTTM      datetime2(0)         NOT NULL
        , END_DTTM        datetime2(0)         NULL
        , STATUS          varchar(20)          NOT NULL  -- STARTED/SUCCESS/FAILED
        , ROW_INSERTED    int                  NULL
        , ROW_EXPIRED     int                  NULL
        , ROW_UPDATED_T1  int                  NULL
        , ERROR_MESSAGE   nvarchar(4000)       NULL
    );
END
GO

IF COL_LENGTH('etl.ETL_RUN', 'ROW_UPDATED_T1') IS NULL
    ALTER TABLE etl.ETL_RUN ADD ROW_UPDATED_T1 int NULL;
GO

/* ==============================================
   1) Ensure standard SCD columns exist (add only if needed)
   ============================================== */
IF COL_LENGTH('svo.D_ORGANIZATION', 'EFF_DATE') IS NULL
    ALTER TABLE svo.D_ORGANIZATION ADD EFF_DATE date NOT NULL
        CONSTRAINT DF_D_ORGANIZATION_EFF_DATE DEFAULT (CAST('0001-01-01' AS date));
GO
IF COL_LENGTH('svo.D_ORGANIZATION', 'END_DATE') IS NULL
    ALTER TABLE svo.D_ORGANIZATION ADD END_DATE date NOT NULL
        CONSTRAINT DF_D_ORGANIZATION_END_DATE DEFAULT (CAST('9999-12-31' AS date));
GO
IF COL_LENGTH('svo.D_ORGANIZATION', 'CRE_DATE') IS NULL
    ALTER TABLE svo.D_ORGANIZATION ADD CRE_DATE datetime2(0) NOT NULL
        CONSTRAINT DF_D_ORGANIZATION_CRE_DATE DEFAULT (SYSDATETIME());
GO
IF COL_LENGTH('svo.D_ORGANIZATION', 'UDT_DATE') IS NULL
    ALTER TABLE svo.D_ORGANIZATION ADD UDT_DATE datetime2(0) NOT NULL
        CONSTRAINT DF_D_ORGANIZATION_UDT_DATE DEFAULT (SYSDATETIME());
GO
IF COL_LENGTH('svo.D_ORGANIZATION', 'CURR_IND') IS NULL
    ALTER TABLE svo.D_ORGANIZATION ADD CURR_IND char(1) NOT NULL
        CONSTRAINT DF_D_ORGANIZATION_CURR_IND DEFAULT ('Y');
GO

/* ==============================================
   2) Drop legacy UNIQUE constraint / index on BK (breaks SCD2 history)
   ============================================== */
IF EXISTS
(
    SELECT 1
    FROM sys.indexes
    WHERE object_id = OBJECT_ID('svo.D_ORGANIZATION')
      AND name IN ('UX_D_ORGANIZATION_NK','UX_D_ORGANIZATION_ID','UX_D_ORGANIZATION_ORGANIZATION_ID')
)
BEGIN
    DECLARE @idx sysname;

    SELECT TOP (1) @idx = name
    FROM sys.indexes
    WHERE object_id = OBJECT_ID('svo.D_ORGANIZATION')
      AND name IN ('UX_D_ORGANIZATION_NK','UX_D_ORGANIZATION_ID','UX_D_ORGANIZATION_ORGANIZATION_ID');

    DECLARE @sql nvarchar(max);
    SET @sql = 'DROP INDEX ' + QUOTENAME(@idx) + ' ON svo.D_ORGANIZATION;';
    EXEC (@sql);
END
GO

/* If BK uniqueness is enforced by a CONSTRAINT, drop it (guarded) */
DECLARE @uk sysname;
SELECT @uk = kc.name
FROM sys.key_constraints kc
WHERE kc.parent_object_id = OBJECT_ID('svo.D_ORGANIZATION')
  AND kc.[type] = 'UQ'
  AND EXISTS
  (
      SELECT 1
      FROM sys.index_columns ic
      JOIN sys.columns c
        ON c.object_id = ic.object_id AND c.column_id = ic.column_id
      WHERE ic.object_id = kc.parent_object_id
        AND ic.index_id = kc.unique_index_id
        AND c.name = 'ORGANIZATION_ID'
  );

IF @uk IS NOT NULL
    DECLARE @sql2 nvarchar(max);
    SET @sql2 = 'ALTER TABLE svo.D_ORGANIZATION DROP CONSTRAINT ' + QUOTENAME(@uk) + ';';
    EXEC (@sql2);
GO

/* ==============================================
   3) Create filtered unique index for current rows
   ============================================== */
IF NOT EXISTS
(
    SELECT 1
    FROM sys.indexes
    WHERE name = 'UX_D_ORGANIZATION_ID_CURR'
      AND object_id = OBJECT_ID('svo.D_ORGANIZATION')
)
BEGIN
    CREATE UNIQUE NONCLUSTERED INDEX UX_D_ORGANIZATION_ID_CURR
        ON svo.D_ORGANIZATION(ORGANIZATION_ID)
        WHERE CURR_IND = 'Y' AND END_DATE = '9999-12-31'
        ON FG_SilverDim;
END
GO

/* ================================
   4) Hybrid SCD loader stored procedure
   ================================ */
CREATE OR ALTER PROCEDURE svo.usp_Load_D_ORGANIZATION_SCD2
(
      @AsOfDate date = NULL
)
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    IF @AsOfDate IS NULL
        SET @AsOfDate = CAST(GETDATE() AS date);

    DECLARE @HighDate date = '9999-12-31';

    DECLARE
          @RunId        bigint
        , @ProcName     sysname = OBJECT_SCHEMA_NAME(@@PROCID) + '.' + OBJECT_NAME(@@PROCID)
        , @TargetObject sysname = 'svo.D_ORGANIZATION';

    DECLARE
          @Inserted   int = 0
        , @Expired    int = 0
        , @UpdatedT1  int = 0;

    INSERT INTO etl.ETL_RUN (PROC_NAME, TARGET_OBJECT, ASOF_DATE, START_DTTM, STATUS)
    VALUES (@ProcName, @TargetObject, @AsOfDate, SYSDATETIME(), 'STARTED');

    SET @RunId = SCOPE_IDENTITY();

    BEGIN TRY
        BEGIN TRAN;

        /* =========================================================
           A) Plug row (SK=0, BK=-1) - stable, not SCD-managed
           ========================================================= */
        IF NOT EXISTS (SELECT 1 FROM svo.D_ORGANIZATION WHERE INVENTORY_ORG_SK = 0)
        BEGIN
            SET IDENTITY_INSERT svo.D_ORGANIZATION ON;

            INSERT INTO svo.D_ORGANIZATION
            (
                  INVENTORY_ORG_SK
                , ORGANIZATION_ID
                , ORGANIZATION_CODE
                , INVENTORY_FLAG
                , BUSINESS_UNIT_ID
                , LEGAL_ENTITY_ID
                , MASTER_ORGANIZATION_ID
                , SOURCE_ORGANIZATION_ID
                , BZ_LOAD_DATE
                , SV_LOAD_DATE
                , EFF_DATE
                , END_DATE
                , CRE_DATE
                , UDT_DATE
                , CURR_IND
            )
            VALUES
            (
                  0
                , -1
                , 'Unknown'
                , 'U'
                , -1
                , -1
                , -1
                , -1
                , CAST(GETDATE() AS date)
                , CAST(GETDATE() AS date)
                , CAST(GETDATE() AS date)
                , @HighDate
                , SYSDATETIME()
                , SYSDATETIME()
                , 'Y'
            );

            SET IDENTITY_INSERT svo.D_ORGANIZATION OFF;
        END

        /* =========================================================
           B) Source snapshot (synonym-based; BZ_LOAD_DATE hardened)
           ========================================================= */
        IF OBJECT_ID('tempdb..#src') IS NOT NULL DROP TABLE #src;

        SELECT
              p.OrganizationId        AS ORGANIZATION_ID
            , p.OrganizationCode      AS ORGANIZATION_CODE
            , p.InventoryFlag         AS INVENTORY_FLAG
            , p.BusinessUnitId        AS BUSINESS_UNIT_ID
            , p.LegalEntityId         AS LEGAL_ENTITY_ID
            , p.MasterOrganizationId  AS MASTER_ORGANIZATION_ID
            , p.SourceOrganizationId  AS SOURCE_ORGANIZATION_ID
            , COALESCE(CAST(p.AddDateTime AS date), CAST(GETDATE() AS date)) AS BZ_LOAD_DATE
            , CAST(GETDATE() AS date) AS SV_LOAD_DATE
        INTO #src
        FROM src.bzo_PIM_InvOrgParametersExtractPVO p
        WHERE p.OrganizationId IS NOT NULL;

        DELETE FROM #src WHERE ORGANIZATION_ID = -1;

        /* Hash for Type2 compare (exclude Type1 cols: ORGANIZATION_CODE, INVENTORY_FLAG) */
        ALTER TABLE #src ADD HASH_T2 varbinary(32) NULL;

        UPDATE s
            SET s.HASH_T2 = HASHBYTES('SHA2_256', CONCAT(
                  COALESCE(CONVERT(nvarchar(50), s.ORGANIZATION_ID), N''), N'|'
                , COALESCE(CONVERT(nvarchar(50), s.BUSINESS_UNIT_ID), N''), N'|'
                , COALESCE(CONVERT(nvarchar(50), s.LEGAL_ENTITY_ID), N''), N'|'
                , COALESCE(CONVERT(nvarchar(50), s.MASTER_ORGANIZATION_ID), N''), N'|'
                , COALESCE(CONVERT(nvarchar(50), s.SOURCE_ORGANIZATION_ID), N'')
            ))
        FROM #src s;

        /* =========================================================
           C) Current target snapshot + T2 hash
           ========================================================= */
        IF OBJECT_ID('tempdb..#tgt') IS NOT NULL DROP TABLE #tgt;

        SELECT
              t.ORGANIZATION_ID
            , t.ORGANIZATION_CODE
            , t.INVENTORY_FLAG
            , HASHBYTES('SHA2_256', CONCAT(
                  COALESCE(CONVERT(nvarchar(50), t.ORGANIZATION_ID), N''), N'|'
                , COALESCE(CONVERT(nvarchar(50), t.BUSINESS_UNIT_ID), N''), N'|'
                , COALESCE(CONVERT(nvarchar(50), t.LEGAL_ENTITY_ID), N''), N'|'
                , COALESCE(CONVERT(nvarchar(50), t.MASTER_ORGANIZATION_ID), N''), N'|'
                , COALESCE(CONVERT(nvarchar(50), t.SOURCE_ORGANIZATION_ID), N'')
            )) AS HASH_T2
        INTO #tgt
        FROM svo.D_ORGANIZATION t
        WHERE t.CURR_IND = 'Y'
          AND t.END_DATE = @HighDate
          AND t.ORGANIZATION_ID <> -1;

        /* =========================================================
           D) Type 1 updates when Type2 unchanged
           ========================================================= */
        UPDATE tgt
            SET
                  tgt.ORGANIZATION_CODE = src.ORGANIZATION_CODE
                , tgt.INVENTORY_FLAG    = src.INVENTORY_FLAG
                , tgt.UDT_DATE          = SYSDATETIME()
                , tgt.SV_LOAD_DATE      = CAST(GETDATE() AS date)
                , tgt.BZ_LOAD_DATE      = COALESCE(tgt.BZ_LOAD_DATE, src.BZ_LOAD_DATE, CAST(GETDATE() AS date))
        FROM svo.D_ORGANIZATION tgt
        INNER JOIN #tgt cur
            ON cur.ORGANIZATION_ID = tgt.ORGANIZATION_ID
        INNER JOIN #src src
            ON src.ORGANIZATION_ID = cur.ORGANIZATION_ID
        WHERE tgt.CURR_IND = 'Y'
          AND tgt.END_DATE = @HighDate
          AND cur.HASH_T2 = src.HASH_T2
          AND (
                ISNULL(tgt.ORGANIZATION_CODE, '') <> ISNULL(src.ORGANIZATION_CODE, '')
             OR ISNULL(tgt.INVENTORY_FLAG, '')    <> ISNULL(src.INVENTORY_FLAG, '')
          );

        SET @UpdatedT1 = @@ROWCOUNT;

        /* =========================================================
           E) Type 2 delta: NEW or CHANGED (by T2 hash)
           ========================================================= */
        IF OBJECT_ID('tempdb..#delta_t2') IS NOT NULL DROP TABLE #delta_t2;

        SELECT src.*
        INTO #delta_t2
        FROM #src src
        LEFT JOIN #tgt cur
            ON cur.ORGANIZATION_ID = src.ORGANIZATION_ID
        WHERE cur.ORGANIZATION_ID IS NULL
           OR cur.HASH_T2 <> src.HASH_T2;

        /* =========================================================
           F) Expire current rows for CHANGED BKs only (not NEW)
           ========================================================= */
        UPDATE tgt
            SET
                  tgt.END_DATE     = DATEADD(day, -1, @AsOfDate)
                , tgt.CURR_IND     = 'N'
                , tgt.UDT_DATE     = SYSDATETIME()
                , tgt.SV_LOAD_DATE = CAST(GETDATE() AS date)
        FROM svo.D_ORGANIZATION tgt
        INNER JOIN #delta_t2 d
            ON d.ORGANIZATION_ID = tgt.ORGANIZATION_ID
        WHERE tgt.CURR_IND = 'Y'
          AND tgt.END_DATE = @HighDate
          AND tgt.ORGANIZATION_ID <> -1
          AND EXISTS (SELECT 1 FROM #tgt cur WHERE cur.ORGANIZATION_ID = d.ORGANIZATION_ID);

        SET @Expired = @@ROWCOUNT;

        /* =========================================================
           G) Insert new current rows (NEW + CHANGED)
           ========================================================= */
        INSERT INTO svo.D_ORGANIZATION
        (
              ORGANIZATION_ID
            , ORGANIZATION_CODE
            , INVENTORY_FLAG
            , BUSINESS_UNIT_ID
            , LEGAL_ENTITY_ID
            , MASTER_ORGANIZATION_ID
            , SOURCE_ORGANIZATION_ID
            , BZ_LOAD_DATE
            , SV_LOAD_DATE
            , EFF_DATE
            , END_DATE
            , CRE_DATE
            , UDT_DATE
            , CURR_IND
        )
        SELECT
              d.ORGANIZATION_ID
            , d.ORGANIZATION_CODE
            , d.INVENTORY_FLAG
            , d.BUSINESS_UNIT_ID
            , d.LEGAL_ENTITY_ID
            , d.MASTER_ORGANIZATION_ID
            , d.SOURCE_ORGANIZATION_ID
            , COALESCE(d.BZ_LOAD_DATE, CAST(GETDATE() AS date))
            , CAST(GETDATE() AS date)
            , @AsOfDate
            , @HighDate
            , SYSDATETIME()
            , SYSDATETIME()
            , 'Y'
        FROM #delta_t2 d;

        SET @Inserted = @@ROWCOUNT;

        COMMIT;

        UPDATE etl.ETL_RUN
            SET
                  END_DTTM       = SYSDATETIME()
                , STATUS         = 'SUCCESS'
                , ROW_INSERTED   = @Inserted
                , ROW_EXPIRED    = @Expired
                , ROW_UPDATED_T1 = @UpdatedT1
                , ERROR_MESSAGE  = NULL
        WHERE RUN_ID = @RunId;

    END TRY
    BEGIN CATCH
        IF @@TRANCOUNT > 0 ROLLBACK;

        DECLARE @Err nvarchar(4000) = ERROR_MESSAGE();

        UPDATE etl.ETL_RUN
            SET
                  END_DTTM       = SYSDATETIME()
                , STATUS         = 'FAILED'
                , ROW_INSERTED   = @Inserted
                , ROW_EXPIRED    = @Expired
                , ROW_UPDATED_T1 = @UpdatedT1
                , ERROR_MESSAGE  = @Err
        WHERE RUN_ID = @RunId;

        THROW;
    END CATCH
END
GO

