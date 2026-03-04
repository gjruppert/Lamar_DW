USE [Oracle_Reporting_P2];
GO

/* =====================================================================
   DIM: svo.D_VENDOR_SITE
   BK : VENDOR_SITE_ID
   Strategy: Hybrid SCD2 (history preserved; 1 current row per BK)

   Locked standards:
   - Stored procedures in schema: svo
   - Preserve synonym-based source query (bzo.*) inside the procedure
   - BZ_LOAD_DATE must never be NULL:
       COALESCE(CAST(AddDateTime AS date), CAST(GETDATE() AS date))
   - Standard SCD2 columns (add if missing) with defaults:
       EFF_DATE (date), END_DATE (date), CRE_DATE (datetime2), UDT_DATE (datetime2), CURR_IND (char(1))
   - Plug row: SK=0, BK=-1
   - Idempotent / transactional / ETL run logging in etl.ETL_RUN
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
-- 1) Ensure standard SCD columns exist (add only if missing)
-------------------------------------------------------------------------------
IF COL_LENGTH('svo.D_VENDOR_SITE', 'EFF_DATE') IS NULL
    ALTER TABLE svo.D_VENDOR_SITE ADD EFF_DATE date NOT NULL
        CONSTRAINT DF_D_VENDOR_SITE_EFF_DATE DEFAULT (CAST('0001-01-01' AS date));
GO
IF COL_LENGTH('svo.D_VENDOR_SITE', 'END_DATE') IS NULL
    ALTER TABLE svo.D_VENDOR_SITE ADD END_DATE date NOT NULL
        CONSTRAINT DF_D_VENDOR_SITE_END_DATE DEFAULT (CAST('9999-12-31' AS date));
GO
IF COL_LENGTH('svo.D_VENDOR_SITE', 'CRE_DATE') IS NULL
    ALTER TABLE svo.D_VENDOR_SITE ADD CRE_DATE datetime2(0) NOT NULL
        CONSTRAINT DF_D_VENDOR_SITE_CRE_DATE DEFAULT (SYSDATETIME());
GO
IF COL_LENGTH('svo.D_VENDOR_SITE', 'UDT_DATE') IS NULL
    ALTER TABLE svo.D_VENDOR_SITE ADD UDT_DATE datetime2(0) NOT NULL
        CONSTRAINT DF_D_VENDOR_SITE_UDT_DATE DEFAULT (SYSDATETIME());
GO
IF COL_LENGTH('svo.D_VENDOR_SITE', 'CURR_IND') IS NULL
    ALTER TABLE svo.D_VENDOR_SITE ADD CURR_IND char(1) NOT NULL
        CONSTRAINT DF_D_VENDOR_SITE_CURR_IND DEFAULT ('Y');
GO

-------------------------------------------------------------------------------
-- 2) Enforce BZ_LOAD_DATE never NULL (backfill existing NULLs once)
-------------------------------------------------------------------------------
IF COL_LENGTH('svo.D_VENDOR_SITE', 'BZ_LOAD_DATE') IS NOT NULL
BEGIN
    UPDATE svo.D_VENDOR_SITE
        SET BZ_LOAD_DATE = CAST(GETDATE() AS date)
    WHERE BZ_LOAD_DATE IS NULL;
END
GO

-------------------------------------------------------------------------------
-- 3) Current-row unique index for BK (drop legacy, create filtered)
-------------------------------------------------------------------------------
IF EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'UX_D_VENDOR_SITE' AND object_id = OBJECT_ID('svo.D_VENDOR_SITE'))
    DROP INDEX UX_D_VENDOR_SITE ON svo.D_VENDOR_SITE;
GO

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'UX_D_VENDOR_SITE_ID_CURR' AND object_id = OBJECT_ID('svo.D_VENDOR_SITE'))
BEGIN
    CREATE UNIQUE NONCLUSTERED INDEX UX_D_VENDOR_SITE_ID_CURR
        ON svo.D_VENDOR_SITE(VENDOR_SITE_ID)
        WHERE CURR_IND = 'Y' AND END_DATE = '9999-12-31'
        ON FG_SilverDim;
END
GO

-------------------------------------------------------------------------------
-- 4) Loader procedure (SCD2, logged, idempotent)
-------------------------------------------------------------------------------
CREATE OR ALTER PROCEDURE svo.usp_Load_D_VENDOR_SITE_SCD2
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
        , @TargetObject sysname = 'svo.D_VENDOR_SITE';

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
        -- Plug row (SK=0, BK=-1) - insert only once
        --------------------------------------------------------------------
        IF NOT EXISTS (SELECT 1 FROM svo.D_VENDOR_SITE WHERE VENDOR_SITE_ID = -1)
        BEGIN
            DECLARE @SkCol sysname =
            (
                SELECT TOP(1) name
                FROM sys.identity_columns
                WHERE object_id = OBJECT_ID('svo.D_VENDOR_SITE')
                ORDER BY column_id
            );

            IF @SkCol IS NULL
                THROW 51000, 'No IDENTITY column found on svo.D_VENDOR_SITE; cannot insert plug row with SK=0.', 1;

            DECLARE @PlugSql nvarchar(max) = N'
                SET IDENTITY_INSERT svo.D_VENDOR_SITE ON;

                INSERT INTO svo.D_VENDOR_SITE
                (
                      ' + QUOTENAME(@SkCol) + N'
                    , VENDOR_SITE_ID
                    , VENDOR_ID
                    , VENDOR_SITE
                    , SUPPLIER_PARTY_ID
                    , VENDOR_TYPE_LOOKUP
                    , PARTY_NAME
                    , ADDRESS_1
                    , ADDRESS_2
                    , ADDRESS_3
                    , CITY
                    , [STATE]
                    , POSTAL_CODE
                    , POSTAL_CODE_4
                    , COUNTRY_CODE
                    , INVOICE_CURRENCY
                    , PAYMENT_CURRENCY
                    , PAY_SITE_FLAG
                    , PRIMARY_PAY_SITE_FLAG
                    , STATUS_FLAG
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
                    , -1
                    , ''UNKNOWN''
                    , -1
                    , ''UNK''
                    , ''UNKNOWN''
                    , ''UNKNOWN''
                    , ''UNKNOWN''
                    , ''UNKNOWN''
                    , ''UNKNOWN''
                    , ''UNKNOWN''
                    , ''00000''
                    , ''0000''
                    , ''UN''
                    , ''UNK''
                    , ''UNK''
                    , ''N''
                    , ''N''
                    , ''U''
                    , CAST(''0001-01-01'' AS date)
                    , CAST(GETDATE() AS date)
                    , CAST(''0001-01-01'' AS date)
                    , CAST(''9999-12-31'' AS date)
                    , SYSDATETIME()
                    , SYSDATETIME()
                    , ''Y''
                );

                SET IDENTITY_INSERT svo.D_VENDOR_SITE OFF;
            ';

            EXEC sys.sp_executesql @PlugSql;
        END

        --------------------------------------------------------------------
        -- Source set (preserved from your script; only BZ_LOAD_DATE rule fixed)
        --------------------------------------------------------------------
        IF OBJECT_ID('tempdb..#src') IS NOT NULL DROP TABLE #src;

        ;WITH S AS
        (
            SELECT
                VendorSiteId                                     AS VENDOR_SITE_ID,
                SupplierVendorId                                 AS VENDOR_ID,
                ISNULL(SupplierSiteVendorSiteCode,'UNK')         AS VENDOR_SITE,
                ISNULL(SuppPartyPartyId,-1)                      AS SUPPLIER_PARTY_ID,
                ISNULL([SupplierVendorTypeLookupCode],'UNK')     AS VENDOR_TYPE_LOOKUP,
                COALESCE([SuppPartyPartyName],[LocationAddress4],'UNK') AS PARTY_NAME,
                [LocationAddress1]                               AS ADDRESS_1,
                [LocationAddress2]                               AS ADDRESS_2,
                [LocationAddress3]                               AS ADDRESS_3,
                ISNULL(LocationCity,'UNK')                       AS CITY,
                ISNULL(LocationState,'UN')                       AS [STATE],
                ISNULL(LocationPostalCode,'00000')               AS POSTAL_CODE,
                ISNULL([LocationPostalPlus4Code],'0000')         AS POSTAL_CODE_4,
                ISNULL(LocationCountry,'UN')                     AS COUNTRY_CODE,
                ISNULL(SupplierSiteInvoiceCurrencyCode,'UNK')    AS INVOICE_CURRENCY,
                ISNULL(SupplierSitePaymentCurrencyCode,'UNK')    AS PAYMENT_CURRENCY,
                ISNULL(SupplierSitePaySiteFlag,'U')              AS PAY_SITE_FLAG,
                ISNULL(SupplierSitePrimaryPaySiteFlag,'U')       AS PRIMARY_PAY_SITE_FLAG,
                ISNULL([LocationStatusFlag],'U')                 AS STATUS_FLAG,
                COALESCE(CAST(AddDateTime AS date), CAST(GETDATE() AS date)) AS BZ_LOAD_DATE
            FROM src.bzo_AP_SupplierSitePVO
            WHERE VendorSiteId IS NOT NULL
        )
        SELECT
              s.*
            , CAST(NULL AS varbinary(32)) AS HASH_T2
        INTO #src
        FROM S s;

        -- Guardrails
        DELETE FROM #src WHERE VENDOR_SITE_ID IS NULL OR VENDOR_SITE_ID = -1;
        UPDATE s SET s.BZ_LOAD_DATE = COALESCE(s.BZ_LOAD_DATE, CAST(GETDATE() AS date)) FROM #src s;

        --------------------------------------------------------------------
        -- Hash (Type2 compare)
        --------------------------------------------------------------------
        UPDATE s
            SET s.HASH_T2 = HASHBYTES('SHA2_256',
                  COALESCE(CONVERT(nvarchar(4000), s.VENDOR_SITE_ID), N'')
                + N'|' + COALESCE(CONVERT(nvarchar(4000), s.VENDOR_ID), N'')
                + N'|' + COALESCE(CONVERT(nvarchar(4000), s.VENDOR_SITE), N'')
                + N'|' + COALESCE(CONVERT(nvarchar(4000), s.SUPPLIER_PARTY_ID), N'')
                + N'|' + COALESCE(CONVERT(nvarchar(4000), s.VENDOR_TYPE_LOOKUP), N'')
                + N'|' + COALESCE(CONVERT(nvarchar(4000), s.PARTY_NAME), N'')
                + N'|' + COALESCE(CONVERT(nvarchar(4000), s.ADDRESS_1), N'')
                + N'|' + COALESCE(CONVERT(nvarchar(4000), s.ADDRESS_2), N'')
                + N'|' + COALESCE(CONVERT(nvarchar(4000), s.ADDRESS_3), N'')
                + N'|' + COALESCE(CONVERT(nvarchar(4000), s.CITY), N'')
                + N'|' + COALESCE(CONVERT(nvarchar(4000), s.[STATE]), N'')
                + N'|' + COALESCE(CONVERT(nvarchar(4000), s.POSTAL_CODE), N'')
                + N'|' + COALESCE(CONVERT(nvarchar(4000), s.POSTAL_CODE_4), N'')
                + N'|' + COALESCE(CONVERT(nvarchar(4000), s.COUNTRY_CODE), N'')
                + N'|' + COALESCE(CONVERT(nvarchar(4000), s.INVOICE_CURRENCY), N'')
                + N'|' + COALESCE(CONVERT(nvarchar(4000), s.PAYMENT_CURRENCY), N'')
                + N'|' + COALESCE(CONVERT(nvarchar(4000), s.PAY_SITE_FLAG), N'')
                + N'|' + COALESCE(CONVERT(nvarchar(4000), s.PRIMARY_PAY_SITE_FLAG), N'')
                + N'|' + COALESCE(CONVERT(nvarchar(4000), s.STATUS_FLAG), N'')
            )
        FROM #src s;

        --------------------------------------------------------------------
        -- Current target hashes
        --------------------------------------------------------------------
        IF OBJECT_ID('tempdb..#tgt') IS NOT NULL DROP TABLE #tgt;

        SELECT
              t.VENDOR_SITE_ID,
              HASHBYTES('SHA2_256',
                  COALESCE(CONVERT(nvarchar(4000), t.VENDOR_SITE_ID), N'')
                + N'|' + COALESCE(CONVERT(nvarchar(4000), t.VENDOR_ID), N'')
                + N'|' + COALESCE(CONVERT(nvarchar(4000), t.VENDOR_SITE), N'')
                + N'|' + COALESCE(CONVERT(nvarchar(4000), t.SUPPLIER_PARTY_ID), N'')
                + N'|' + COALESCE(CONVERT(nvarchar(4000), t.VENDOR_TYPE_LOOKUP), N'')
                + N'|' + COALESCE(CONVERT(nvarchar(4000), t.PARTY_NAME), N'')
                + N'|' + COALESCE(CONVERT(nvarchar(4000), t.ADDRESS_1), N'')
                + N'|' + COALESCE(CONVERT(nvarchar(4000), t.ADDRESS_2), N'')
                + N'|' + COALESCE(CONVERT(nvarchar(4000), t.ADDRESS_3), N'')
                + N'|' + COALESCE(CONVERT(nvarchar(4000), t.CITY), N'')
                + N'|' + COALESCE(CONVERT(nvarchar(4000), t.[STATE]), N'')
                + N'|' + COALESCE(CONVERT(nvarchar(4000), t.POSTAL_CODE), N'')
                + N'|' + COALESCE(CONVERT(nvarchar(4000), t.POSTAL_CODE_4), N'')
                + N'|' + COALESCE(CONVERT(nvarchar(4000), t.COUNTRY_CODE), N'')
                + N'|' + COALESCE(CONVERT(nvarchar(4000), t.INVOICE_CURRENCY), N'')
                + N'|' + COALESCE(CONVERT(nvarchar(4000), t.PAYMENT_CURRENCY), N'')
                + N'|' + COALESCE(CONVERT(nvarchar(4000), t.PAY_SITE_FLAG), N'')
                + N'|' + COALESCE(CONVERT(nvarchar(4000), t.PRIMARY_PAY_SITE_FLAG), N'')
                + N'|' + COALESCE(CONVERT(nvarchar(4000), t.STATUS_FLAG), N'')
              ) AS HASH_T2
        INTO #tgt
        FROM svo.D_VENDOR_SITE t
        WHERE t.CURR_IND = 'Y'
          AND t.END_DATE = @HighDate
          AND t.VENDOR_SITE_ID <> -1;

        --------------------------------------------------------------------
        -- Delta (new or changed)
        --------------------------------------------------------------------
        IF OBJECT_ID('tempdb..#delta_t2') IS NOT NULL DROP TABLE #delta_t2;

        SELECT s.*
        INTO #delta_t2
        FROM #src s
        LEFT JOIN #tgt t ON t.VENDOR_SITE_ID = s.VENDOR_SITE_ID
        WHERE t.VENDOR_SITE_ID IS NULL
           OR t.HASH_T2 <> s.HASH_T2;

        --------------------------------------------------------------------
        -- Expire changed current rows
        --------------------------------------------------------------------
        UPDATE tgt
            SET
                  tgt.END_DATE     = DATEADD(day, -1, @AsOfDate)
                , tgt.CURR_IND     = 'N'
                , tgt.UDT_DATE     = SYSDATETIME()
                , tgt.SV_LOAD_DATE = CAST(GETDATE() AS date)
                , tgt.BZ_LOAD_DATE = COALESCE(tgt.BZ_LOAD_DATE, CAST(GETDATE() AS date))
        FROM svo.D_VENDOR_SITE tgt
        INNER JOIN #delta_t2 d ON d.VENDOR_SITE_ID = tgt.VENDOR_SITE_ID
        WHERE tgt.CURR_IND = 'Y'
          AND tgt.END_DATE = @HighDate
          AND tgt.VENDOR_SITE_ID <> -1
          AND EXISTS (SELECT 1 FROM #tgt cur WHERE cur.VENDOR_SITE_ID = d.VENDOR_SITE_ID);

        SET @Expired = @@ROWCOUNT;

        --------------------------------------------------------------------
        -- Insert new current rows
        --------------------------------------------------------------------
        INSERT INTO svo.D_VENDOR_SITE
        (
              VENDOR_SITE_ID
            , VENDOR_ID
            , VENDOR_SITE
            , SUPPLIER_PARTY_ID
            , VENDOR_TYPE_LOOKUP
            , PARTY_NAME
            , ADDRESS_1
            , ADDRESS_2
            , ADDRESS_3
            , CITY
            , [STATE]
            , POSTAL_CODE
            , POSTAL_CODE_4
            , COUNTRY_CODE
            , INVOICE_CURRENCY
            , PAYMENT_CURRENCY
            , PAY_SITE_FLAG
            , PRIMARY_PAY_SITE_FLAG
            , STATUS_FLAG
            , BZ_LOAD_DATE
            , SV_LOAD_DATE
            , EFF_DATE
            , END_DATE
            , CRE_DATE
            , UDT_DATE
            , CURR_IND
        )
        SELECT
              d.VENDOR_SITE_ID
            , d.VENDOR_ID
            , d.VENDOR_SITE
            , d.SUPPLIER_PARTY_ID
            , d.VENDOR_TYPE_LOOKUP
            , d.PARTY_NAME
            , d.ADDRESS_1
            , d.ADDRESS_2
            , d.ADDRESS_3
            , d.CITY
            , d.[STATE]
            , d.POSTAL_CODE
            , d.POSTAL_CODE_4
            , d.COUNTRY_CODE
            , d.INVOICE_CURRENCY
            , d.PAYMENT_CURRENCY
            , d.PAY_SITE_FLAG
            , d.PRIMARY_PAY_SITE_FLAG
            , d.STATUS_FLAG
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

