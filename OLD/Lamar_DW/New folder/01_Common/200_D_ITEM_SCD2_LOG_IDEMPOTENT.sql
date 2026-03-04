USE [Oracle_Reporting_P2];
GO

/* =====================================================================
   DIM: svo.D_ITEM  (Global Item Master)
   BK : INVENTORY_ITEM_ID  (source: ITEMBASEPEOINVENTORYITEMID)
   Source: src.bzo_PIM_ItemExtractPVO   (via synonym-friendly reference)
   Pattern: Hybrid SCD2 (EFF_DATE/END_DATE/CRE_DATE/UDT_DATE/CURR_IND)
            - 1 current row per BK
            - history preserved
            - idempotent / rerunnable
            - logged to etl.ETL_RUN
   Standards:
     - Procedure in svo schema (NOT dbo)
     - BZ_LOAD_DATE must never be NULL:
         COALESCE(CAST(src.AddDateTime AS date), CAST(GETDATE() AS date))
     - SV_LOAD_DATE = CAST(GETDATE() AS date)
   ===================================================================== */

-------------------------------------------------------------------------------
-- 0) Logging (create once)
-------------------------------------------------------------------------------
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
        , STATUS          varchar(20)          NOT NULL
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

-------------------------------------------------------------------------------
-- 1) Loader (Hybrid SCD2)
-------------------------------------------------------------------------------
CREATE OR ALTER PROCEDURE svo.usp_Load_D_ITEM_SCD2
(
      @AsOfDate date = NULL
)
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    IF @AsOfDate IS NULL
        SET @AsOfDate = CAST(GETDATE() AS date);

    DECLARE @HighDate date = CONVERT(date, '9999-12-31');

    DECLARE
          @RunId        bigint
        , @ProcName     sysname = OBJECT_SCHEMA_NAME(@@PROCID) + '.' + OBJECT_NAME(@@PROCID)
        , @TargetObject sysname = 'svo.D_ITEM';

    DECLARE
          @Inserted   int = 0
        , @Expired    int = 0
        , @UpdatedT1  int = 0;

    INSERT INTO etl.ETL_RUN (PROC_NAME, TARGET_OBJECT, ASOF_DATE, START_DTTM, STATUS)
    VALUES (@ProcName, @TargetObject, @AsOfDate, SYSDATETIME(), 'STARTED');

    SET @RunId = SCOPE_IDENTITY();

    BEGIN TRY
        BEGIN TRAN;

        --------------------------------------------------------------------
        -- Ensure required SCD2 columns exist (only add when needed)
        --------------------------------------------------------------------
        IF COL_LENGTH('svo.D_ITEM','EFF_DATE') IS NULL
        BEGIN
            ALTER TABLE svo.D_ITEM ADD
                  EFF_DATE  date NOT NULL CONSTRAINT DF_D_ITEM_EFF_DATE  DEFAULT (CONVERT(date,'0001-01-01'))
                , END_DATE  date NOT NULL CONSTRAINT DF_D_ITEM_END_DATE  DEFAULT (CONVERT(date,'9999-12-31'))
                , CRE_DATE  datetime2(0) NOT NULL CONSTRAINT DF_D_ITEM_CRE_DATE DEFAULT (SYSDATETIME())
                , UDT_DATE  datetime2(0) NOT NULL CONSTRAINT DF_D_ITEM_UDT_DATE DEFAULT (SYSDATETIME())
                , CURR_IND  char(1) NOT NULL CONSTRAINT DF_D_ITEM_CURR_IND DEFAULT ('Y');
        END

        -- If BZ_LOAD_DATE exists and is nullable, we still guarantee non-NULL values in our writes.
        -- Do not change existing datatype here; keep table as-is.

        --------------------------------------------------------------------
        -- Ensure plug row (SK = 0)
        --------------------------------------------------------------------
        IF NOT EXISTS (SELECT 1 FROM svo.D_ITEM WHERE ITEM_SK = 0)
        BEGIN
            SET IDENTITY_INSERT svo.D_ITEM ON;

            INSERT INTO svo.D_ITEM
            (
                  ITEM_SK
                , INVENTORY_ITEM_ID
                , ITEM_NUMBER
                , ITEM_DESCRIPTION
                , ITEM_TYPE
                , ITEM_STATUS
                , BZ_LOAD_DATE
                , SV_LOAD_DATE
                , INFERRED_FLAG
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
                , 'UNK'
                , 'Unknown Item'
                , 'UNK'
                , 'UNK'
                , CONVERT(date,'0001-01-01')
                , @AsOfDate
                , 'N'
                , CONVERT(date,'0001-01-01')
                , CONVERT(date,'0001-01-01')
                , SYSDATETIME()
                , SYSDATETIME()
                , 'Y'
            );

            SET IDENTITY_INSERT svo.D_ITEM OFF;
        END

        --------------------------------------------------------------------
        -- Source extract (dedupe to 1 row per BK)
        --------------------------------------------------------------------
        IF OBJECT_ID('tempdb..#src') IS NOT NULL DROP TABLE #src;

        ;WITH s0 AS
        (
            SELECT
                  CAST(I.ITEMBASEPEOINVENTORYITEMID AS bigint)        AS INVENTORY_ITEM_ID
                , CAST(I.ITEMBASEPEOITEMNUMBER AS varchar(100))       AS ITEM_NUMBER
                , CAST(I.ITEMTRANSLATIONPEODESCRIPTION AS varchar(1000)) AS ITEM_DESCRIPTION
                , ISNULL(CAST(I.ITEMBASEPEOITEMTYPE AS varchar(200)), 'UNK') AS ITEM_TYPE
                , CAST(I.ItemBasePEOInventoryItemStatusCode AS varchar(100)) AS ITEM_STATUS
                , COALESCE(CAST(I.AddDateTime AS date), CAST(GETDATE() AS date)) AS BZ_LOAD_DATE
                , ROW_NUMBER() OVER
                  (
                      PARTITION BY I.ITEMBASEPEOINVENTORYITEMID
                      ORDER BY COALESCE(I.AddDateTime, GETDATE()) DESC
                  ) AS rn
            FROM src.bzo_PIM_ItemExtractPVO AS I
            WHERE I.ITEMBASEPEOINVENTORYITEMID IS NOT NULL
        )
        SELECT
              INVENTORY_ITEM_ID
            , ISNULL(NULLIF(ITEM_NUMBER,''), 'UNK') AS ITEM_NUMBER
            , ISNULL(NULLIF(ITEM_DESCRIPTION,''), 'UNK') AS ITEM_DESCRIPTION
            , ISNULL(NULLIF(ITEM_TYPE,''), 'UNK') AS ITEM_TYPE
            , ISNULL(NULLIF(ITEM_STATUS,''), 'UNK') AS ITEM_STATUS
            , BZ_LOAD_DATE
            , CAST(GETDATE() AS date) AS SV_LOAD_DATE
            , CAST('N' AS char(1)) AS INFERRED_FLAG
            , CONVERT(varbinary(32), HASHBYTES('SHA2_256',
                    CONCAT(
                        ISNULL(NULLIF(ITEM_NUMBER,''), 'UNK'), '|',
                        ISNULL(NULLIF(ITEM_DESCRIPTION,''), 'UNK'), '|',
                        ISNULL(NULLIF(ITEM_TYPE,''), 'UNK'), '|',
                        ISNULL(NULLIF(ITEM_STATUS,''), 'UNK')
                    )
              )) AS ROW_HASH
        INTO #src
        FROM s0
        WHERE rn = 1;

        --------------------------------------------------------------------
        -- Current target rows
        --------------------------------------------------------------------
        IF OBJECT_ID('tempdb..#tcur') IS NOT NULL DROP TABLE #tcur;

        SELECT
              t.ITEM_SK
            , t.INVENTORY_ITEM_ID
            , t.ITEM_NUMBER
            , t.ITEM_DESCRIPTION
            , t.ITEM_TYPE
            , t.ITEM_STATUS
            , t.INFERRED_FLAG
            , t.EFF_DATE
            , t.END_DATE
            , t.CURR_IND
            , CONVERT(varbinary(32), HASHBYTES('SHA2_256',
                    CONCAT(
                        ISNULL(NULLIF(t.ITEM_NUMBER,''), 'UNK'), '|',
                        ISNULL(NULLIF(t.ITEM_DESCRIPTION,''), 'UNK'), '|',
                        ISNULL(NULLIF(t.ITEM_TYPE,''), 'UNK'), '|',
                        ISNULL(NULLIF(t.ITEM_STATUS,''), 'UNK')
                    )
              )) AS ROW_HASH
        INTO #tcur
        FROM svo.D_ITEM t
        WHERE t.CURR_IND = 'Y';

        --------------------------------------------------------------------
        -- Determine changes (new vs changed vs same)
        --------------------------------------------------------------------
        IF OBJECT_ID('tempdb..#chg') IS NOT NULL DROP TABLE #chg;

        SELECT
              s.*
            , tc.ITEM_SK AS CUR_ITEM_SK
            , tc.ROW_HASH AS CUR_ROW_HASH
            , CASE
                WHEN tc.ITEM_SK IS NULL THEN 'I'       -- insert new BK
                WHEN tc.ROW_HASH <> s.ROW_HASH THEN 'S' -- SCD2 change
                ELSE 'U'                               -- no change (Type1 refresh)
              END AS ACTION_CD
        INTO #chg
        FROM #src s
        LEFT JOIN #tcur tc
          ON tc.INVENTORY_ITEM_ID = s.INVENTORY_ITEM_ID;

        --------------------------------------------------------------------
        -- Expire changed current rows (SCD2)
        --------------------------------------------------------------------
        UPDATE tgt
            SET
                  tgt.END_DATE = DATEADD(day, -1, @AsOfDate)
                , tgt.CURR_IND = 'N'
                , tgt.UDT_DATE = SYSDATETIME()
                , tgt.SV_LOAD_DATE = @AsOfDate
        FROM svo.D_ITEM tgt
        INNER JOIN #chg c
            ON c.ACTION_CD = 'S'
           AND tgt.ITEM_SK = c.CUR_ITEM_SK
        WHERE tgt.CURR_IND = 'Y';

        SET @Expired = @@ROWCOUNT;

        --------------------------------------------------------------------
        -- Insert new current rows (new BKs + changed BKs)
        --------------------------------------------------------------------
        INSERT INTO svo.D_ITEM
        (
              INVENTORY_ITEM_ID
            , ITEM_NUMBER
            , ITEM_DESCRIPTION
            , ITEM_TYPE
            , ITEM_STATUS
            , BZ_LOAD_DATE
            , SV_LOAD_DATE
            , INFERRED_FLAG
            , EFF_DATE
            , END_DATE
            , CRE_DATE
            , UDT_DATE
            , CURR_IND
        )
        SELECT
              c.INVENTORY_ITEM_ID
            , c.ITEM_NUMBER
            , c.ITEM_DESCRIPTION
            , c.ITEM_TYPE
            , c.ITEM_STATUS
            , c.BZ_LOAD_DATE
            , @AsOfDate
            , c.INFERRED_FLAG
            , @AsOfDate
            , @HighDate
            , SYSDATETIME()
            , SYSDATETIME()
            , 'Y'
        FROM #chg c
        WHERE c.ACTION_CD IN ('I','S');

        SET @Inserted = @@ROWCOUNT;

        --------------------------------------------------------------------
        -- Type 1 refresh for unchanged current rows (keep history stable)
        -- (updates load dates only; attributes unchanged)
        --------------------------------------------------------------------
        UPDATE tgt
            SET
                  tgt.BZ_LOAD_DATE = COALESCE(tgt.BZ_LOAD_DATE, c.BZ_LOAD_DATE)
                , tgt.SV_LOAD_DATE = @AsOfDate
                , tgt.UDT_DATE     = SYSDATETIME()
        FROM svo.D_ITEM tgt
        INNER JOIN #chg c
            ON c.ACTION_CD = 'U'
           AND tgt.INVENTORY_ITEM_ID = c.INVENTORY_ITEM_ID
        WHERE tgt.CURR_IND = 'Y';

        SET @UpdatedT1 = @@ROWCOUNT;

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

