USE [Oracle_Reporting_P2];
GO

/* =====================================================================
   DROP-IN PACKAGE: DDL + Logging + Loader Proc (Hybrid SCD2)

   DIM:        svo.D_VENDOR   (Derived from vendor-site source)
   BK:         VENDOR_ID = SupplierVendorId
   Source:     src.bzo_AP_SupplierSitePVO  (only available supplier source)

   SCD2 fields: EFF_DATE, END_DATE, CRE_DATE, UDT_DATE, CURR_IND
   Load dates:  BZ_LOAD_DATE (never NULL), SV_LOAD_DATE (never NULL)

   Idempotent behavior:
     - Rerunnable: expires + inserts only when attributes change
     - 1 current row per BK enforced by filtered unique index
     - Logs to etl.ETL_RUN

   Notes:
     - Because source is site-grain, we dedupe to one row per supplier using ROW_NUMBER().
     - This is a "best available" vendor header until a true supplier header extract exists.
   ===================================================================== */

-------------------------------------------------------------------------------
-- 0) Logging objects (create once)
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
-- 1) DDL: svo.D_VENDOR (Hybrid SCD2)
-------------------------------------------------------------------------------
IF OBJECT_ID(N'svo.D_VENDOR', N'U') IS NOT NULL
    DROP TABLE svo.D_VENDOR;
GO

CREATE TABLE svo.D_VENDOR
(
    -- Surrogate key first
    VENDOR_SK                         BIGINT IDENTITY(1,1) NOT NULL,

    -- Business key
    VENDOR_ID                         BIGINT               NOT NULL,   -- SupplierVendorId (BK)

    -- Attributes (derived from site source)
    VENDOR_NAME                       VARCHAR(360)         NULL,       -- SuppPartyPartyName / SupplierTaxReportingName
    VENDOR_NUMBER                     VARCHAR(30)          NULL,       -- SupplierSegment1
    VENDOR_TYPE_CODE                  VARCHAR(30)          NULL,       -- SupplierVendorTypeLookupCode
    ORG_TYPE_CODE                     VARCHAR(25)          NULL,       -- SupplierOrganizationTypeLookupCode

    START_DATE_ACTIVE                 DATE                 NULL,       -- SupplierStartDateActive
    END_DATE_ACTIVE                   DATE                 NULL,       -- SupplierEndDateActive
    FEDERAL_REPORTABLE_FLAG           VARCHAR(1)           NULL,       -- SupplierFederalReportableFlag
    STATE_REPORTABLE_FLAG             VARCHAR(1)           NULL,       -- SupplierStateReportableFlag
    WITHHOLDING_STATUS_CODE           VARCHAR(25)          NULL,       -- SupplierWithholdingStatusLookupCode
    WITHHOLDING_START_DATE            DATE                 NULL,       -- SupplierWithholdingStartDate

    TAXPAYER_COUNTRY                  VARCHAR(2)           NULL,       -- SupplierTaxpayerCountry
    VAT_CODE                          VARCHAR(15)          NULL,       -- SupplierVatCode

    INFERRED_FLAG                     CHAR(1)              NOT NULL CONSTRAINT DF_D_VENDOR_INFERRED_FLAG DEFAULT ('N'),

    -- SCD2
    EFF_DATE                          DATE                 NOT NULL CONSTRAINT DF_D_VENDOR_EFF_DATE DEFAULT (CONVERT(date,'0001-01-01')),
    END_DATE                          DATE                 NOT NULL CONSTRAINT DF_D_VENDOR_END_DATE DEFAULT (CONVERT(date,'9999-12-31')),
    CRE_DATE                          DATETIME2(0)         NOT NULL CONSTRAINT DF_D_VENDOR_CRE_DATE DEFAULT (SYSDATETIME()),
    UDT_DATE                          DATETIME2(0)         NOT NULL CONSTRAINT DF_D_VENDOR_UDT_DATE DEFAULT (SYSDATETIME()),
    CURR_IND                          CHAR(1)              NOT NULL CONSTRAINT DF_D_VENDOR_CURR_IND DEFAULT ('Y'),

    -- Load dates (DATE, NOT NULL)
    BZ_LOAD_DATE                      DATE                 NOT NULL CONSTRAINT DF_D_VENDOR_BZ_LOAD_DATE DEFAULT (CAST(GETDATE() AS DATE)),
    SV_LOAD_DATE                      DATE                 NOT NULL CONSTRAINT DF_D_VENDOR_SV_LOAD_DATE DEFAULT (CAST(GETDATE() AS DATE)),

    CONSTRAINT PK_D_VENDOR PRIMARY KEY CLUSTERED (VENDOR_SK) ON FG_SilverDim
) ON FG_SilverDim;
GO

-- Enforce 1 current row per BK
CREATE UNIQUE NONCLUSTERED INDEX UX_D_VENDOR_BK_CURR
ON svo.D_VENDOR (VENDOR_ID)
WHERE CURR_IND = 'Y'
ON FG_SilverDim;
GO

-- Helpful for history seeks
CREATE NONCLUSTERED INDEX IX_D_VENDOR_BK_EFF_END
ON svo.D_VENDOR (VENDOR_ID, EFF_DATE, END_DATE)
ON FG_SilverDim;
GO

-- Plug row (VENDOR_SK = 0, VENDOR_ID = -1)
IF NOT EXISTS (SELECT 1 FROM svo.D_VENDOR WHERE VENDOR_SK = 0)
BEGIN
    SET IDENTITY_INSERT svo.D_VENDOR ON;

    INSERT INTO svo.D_VENDOR
    (
        VENDOR_SK, VENDOR_ID,
        VENDOR_NAME, VENDOR_NUMBER, VENDOR_TYPE_CODE, ORG_TYPE_CODE,
        START_DATE_ACTIVE, END_DATE_ACTIVE,
        FEDERAL_REPORTABLE_FLAG, STATE_REPORTABLE_FLAG,
        WITHHOLDING_STATUS_CODE, WITHHOLDING_START_DATE,
        TAXPAYER_COUNTRY, VAT_CODE,
        INFERRED_FLAG,
        EFF_DATE, END_DATE, CRE_DATE, UDT_DATE, CURR_IND,
        BZ_LOAD_DATE, SV_LOAD_DATE
    )
    VALUES
    (
        0, -1,
        'Unknown Vendor', 'UNK', 'UNK', 'UNK',
        CONVERT(date,'0001-01-01'), CONVERT(date,'0001-01-01'),
        'U', 'U',
        'UNK', CONVERT(date,'0001-01-01'),
        'UN', 'UNK',
        'N',
        CONVERT(date,'0001-01-01'), CONVERT(date,'0001-01-01'),
        SYSDATETIME(), SYSDATETIME(), 'Y',
        CONVERT(date,'0001-01-01'), CAST(GETDATE() AS DATE)
    );

    SET IDENTITY_INSERT svo.D_VENDOR OFF;
END
GO

-------------------------------------------------------------------------------
-- 2) Loader Proc: svo.usp_Load_D_VENDOR_SCD2 (Hybrid SCD2, logged, idempotent)
-------------------------------------------------------------------------------
CREATE OR ALTER PROCEDURE svo.usp_Load_D_VENDOR_SCD2
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
        , @TargetObject sysname = 'svo.D_VENDOR';

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
        -- Source extract (dedupe site rows to 1 row per SupplierVendorId)
        --------------------------------------------------------------------
        IF OBJECT_ID('tempdb..#src') IS NOT NULL DROP TABLE #src;

        ;WITH VendorPick AS
        (
            SELECT
                s.SupplierVendorId AS VENDOR_ID,

                COALESCE(s.SuppPartyPartyName, s.SupplierTaxReportingName) AS VENDOR_NAME,
                s.SupplierSegment1 AS VENDOR_NUMBER,
                s.SupplierVendorTypeLookupCode AS VENDOR_TYPE_CODE,
                s.SupplierOrganizationTypeLookupCode AS ORG_TYPE_CODE,

                s.SupplierStartDateActive AS START_DATE_ACTIVE,
                s.SupplierEndDateActive AS END_DATE_ACTIVE,
                s.SupplierFederalReportableFlag AS FEDERAL_REPORTABLE_FLAG,
                s.SupplierStateReportableFlag AS STATE_REPORTABLE_FLAG,
                s.SupplierWithholdingStatusLookupCode AS WITHHOLDING_STATUS_CODE,
                s.SupplierWithholdingStartDate AS WITHHOLDING_START_DATE,

                s.SupplierTaxpayerCountry AS TAXPAYER_COUNTRY,
                s.SupplierVatCode AS VAT_CODE,

                COALESCE(CAST(s.AddDateTime AS date), CAST(GETDATE() AS date)) AS BZ_LOAD_DATE,

                ROW_NUMBER() OVER
                (
                    PARTITION BY s.SupplierVendorId
                    ORDER BY
                        s.SupplierLastUpdateDate DESC,
                        s.SupplierSiteLastUpdateDate DESC,
                        s.AddDateTime DESC,
                        s.VendorSiteId DESC
                ) AS rn
            FROM src.bzo_AP_SupplierSitePVO s
            WHERE s.SupplierVendorId IS NOT NULL
        )
        SELECT
              CAST(v.VENDOR_ID AS bigint) AS VENDOR_ID
            , ISNULL(NULLIF(CAST(v.VENDOR_NAME AS varchar(360)), ''), 'Unknown') AS VENDOR_NAME
            , ISNULL(NULLIF(CAST(v.VENDOR_NUMBER AS varchar(30)), ''), 'UNK') AS VENDOR_NUMBER
            , ISNULL(NULLIF(CAST(v.VENDOR_TYPE_CODE AS varchar(30)), ''), 'UNK') AS VENDOR_TYPE_CODE
            , ISNULL(NULLIF(CAST(v.ORG_TYPE_CODE AS varchar(25)), ''), 'UNK') AS ORG_TYPE_CODE

            , CAST(v.START_DATE_ACTIVE AS date) AS START_DATE_ACTIVE
            , CAST(v.END_DATE_ACTIVE   AS date) AS END_DATE_ACTIVE
            , ISNULL(NULLIF(CAST(v.FEDERAL_REPORTABLE_FLAG AS varchar(1)), ''), 'U') AS FEDERAL_REPORTABLE_FLAG
            , ISNULL(NULLIF(CAST(v.STATE_REPORTABLE_FLAG   AS varchar(1)), ''), 'U') AS STATE_REPORTABLE_FLAG
            , ISNULL(NULLIF(CAST(v.WITHHOLDING_STATUS_CODE AS varchar(25)), ''), 'UNK') AS WITHHOLDING_STATUS_CODE
            , CAST(v.WITHHOLDING_START_DATE AS date) AS WITHHOLDING_START_DATE

            , ISNULL(NULLIF(CAST(v.TAXPAYER_COUNTRY AS varchar(2)), ''), 'UN') AS TAXPAYER_COUNTRY
            , ISNULL(NULLIF(CAST(v.VAT_CODE         AS varchar(15)), ''), 'UNK') AS VAT_CODE

            , v.BZ_LOAD_DATE
            , @AsOfDate AS SV_LOAD_DATE
            , CAST('N' AS char(1)) AS INFERRED_FLAG

            , CONVERT(varbinary(32), HASHBYTES('SHA2_256',
                    CONCAT(
                        ISNULL(NULLIF(CAST(v.VENDOR_NAME AS varchar(360)), ''), 'Unknown'), '|',
                        ISNULL(NULLIF(CAST(v.VENDOR_NUMBER AS varchar(30)), ''), 'UNK'), '|',
                        ISNULL(NULLIF(CAST(v.VENDOR_TYPE_CODE AS varchar(30)), ''), 'UNK'), '|',
                        ISNULL(NULLIF(CAST(v.ORG_TYPE_CODE AS varchar(25)), ''), 'UNK'), '|',
                        ISNULL(CONVERT(varchar(10), CAST(v.START_DATE_ACTIVE AS date), 120), '0001-01-01'), '|',
                        ISNULL(CONVERT(varchar(10), CAST(v.END_DATE_ACTIVE   AS date), 120), '0001-01-01'), '|',
                        ISNULL(NULLIF(CAST(v.FEDERAL_REPORTABLE_FLAG AS varchar(1)), ''), 'U'), '|',
                        ISNULL(NULLIF(CAST(v.STATE_REPORTABLE_FLAG   AS varchar(1)), ''), 'U'), '|',
                        ISNULL(NULLIF(CAST(v.WITHHOLDING_STATUS_CODE AS varchar(25)), ''), 'UNK'), '|',
                        ISNULL(CONVERT(varchar(10), CAST(v.WITHHOLDING_START_DATE AS date), 120), '0001-01-01'), '|',
                        ISNULL(NULLIF(CAST(v.TAXPAYER_COUNTRY AS varchar(2)), ''), 'UN'), '|',
                        ISNULL(NULLIF(CAST(v.VAT_CODE         AS varchar(15)), ''), 'UNK')
                    )
              )) AS ROW_HASH
        INTO #src
        FROM VendorPick v
        WHERE v.rn = 1;

        --------------------------------------------------------------------
        -- Current target rows
        --------------------------------------------------------------------
        IF OBJECT_ID('tempdb..#tcur') IS NOT NULL DROP TABLE #tcur;

        SELECT
              t.VENDOR_SK
            , t.VENDOR_ID
            , CONVERT(varbinary(32), HASHBYTES('SHA2_256',
                    CONCAT(
                        ISNULL(NULLIF(t.VENDOR_NAME, ''), 'Unknown'), '|',
                        ISNULL(NULLIF(t.VENDOR_NUMBER, ''), 'UNK'), '|',
                        ISNULL(NULLIF(t.VENDOR_TYPE_CODE, ''), 'UNK'), '|',
                        ISNULL(NULLIF(t.ORG_TYPE_CODE, ''), 'UNK'), '|',
                        ISNULL(CONVERT(varchar(10), t.START_DATE_ACTIVE, 120), '0001-01-01'), '|',
                        ISNULL(CONVERT(varchar(10), t.END_DATE_ACTIVE,   120), '0001-01-01'), '|',
                        ISNULL(NULLIF(t.FEDERAL_REPORTABLE_FLAG, ''), 'U'), '|',
                        ISNULL(NULLIF(t.STATE_REPORTABLE_FLAG,   ''), 'U'), '|',
                        ISNULL(NULLIF(t.WITHHOLDING_STATUS_CODE, ''), 'UNK'), '|',
                        ISNULL(CONVERT(varchar(10), t.WITHHOLDING_START_DATE, 120), '0001-01-01'), '|',
                        ISNULL(NULLIF(t.TAXPAYER_COUNTRY, ''), 'UN'), '|',
                        ISNULL(NULLIF(t.VAT_CODE,         ''), 'UNK')
                    )
              )) AS ROW_HASH
        INTO #tcur
        FROM svo.D_VENDOR t
        WHERE t.CURR_IND = 'Y';

        --------------------------------------------------------------------
        -- Determine changes
        --------------------------------------------------------------------
        IF OBJECT_ID('tempdb..#chg') IS NOT NULL DROP TABLE #chg;

        SELECT
              s.*
            , tc.VENDOR_SK AS CUR_VENDOR_SK
            , tc.ROW_HASH  AS CUR_ROW_HASH
            , CASE
                WHEN tc.VENDOR_SK IS NULL THEN 'I'
                WHEN tc.ROW_HASH <> s.ROW_HASH THEN 'S'
                ELSE 'U'
              END AS ACTION_CD
        INTO #chg
        FROM #src s
        LEFT JOIN #tcur tc
          ON tc.VENDOR_ID = s.VENDOR_ID;

        --------------------------------------------------------------------
        -- Expire changed current rows
        --------------------------------------------------------------------
        UPDATE tgt
            SET
                  tgt.END_DATE     = DATEADD(day, -1, @AsOfDate)
                , tgt.CURR_IND     = 'N'
                , tgt.UDT_DATE     = SYSDATETIME()
                , tgt.SV_LOAD_DATE = @AsOfDate
        FROM svo.D_VENDOR tgt
        INNER JOIN #chg c
            ON c.ACTION_CD = 'S'
           AND tgt.VENDOR_SK = c.CUR_VENDOR_SK
        WHERE tgt.CURR_IND = 'Y';

        SET @Expired = @@ROWCOUNT;

        --------------------------------------------------------------------
        -- Insert new current rows (new BKs + changed BKs)
        --------------------------------------------------------------------
        INSERT INTO svo.D_VENDOR
        (
              VENDOR_ID
            , VENDOR_NAME
            , VENDOR_NUMBER
            , VENDOR_TYPE_CODE
            , ORG_TYPE_CODE
            , START_DATE_ACTIVE
            , END_DATE_ACTIVE
            , FEDERAL_REPORTABLE_FLAG
            , STATE_REPORTABLE_FLAG
            , WITHHOLDING_STATUS_CODE
            , WITHHOLDING_START_DATE
            , TAXPAYER_COUNTRY
            , VAT_CODE
            , INFERRED_FLAG
            , EFF_DATE
            , END_DATE
            , CRE_DATE
            , UDT_DATE
            , CURR_IND
            , BZ_LOAD_DATE
            , SV_LOAD_DATE
        )
        SELECT
              c.VENDOR_ID
            , c.VENDOR_NAME
            , c.VENDOR_NUMBER
            , c.VENDOR_TYPE_CODE
            , c.ORG_TYPE_CODE
            , c.START_DATE_ACTIVE
            , c.END_DATE_ACTIVE
            , c.FEDERAL_REPORTABLE_FLAG
            , c.STATE_REPORTABLE_FLAG
            , c.WITHHOLDING_STATUS_CODE
            , c.WITHHOLDING_START_DATE
            , c.TAXPAYER_COUNTRY
            , c.VAT_CODE
            , c.INFERRED_FLAG
            , @AsOfDate
            , @HighDate
            , SYSDATETIME()
            , SYSDATETIME()
            , 'Y'
            , c.BZ_LOAD_DATE
            , @AsOfDate
        FROM #chg c
        WHERE c.ACTION_CD IN ('I','S');

        SET @Inserted = @@ROWCOUNT;

        --------------------------------------------------------------------
        -- Type 1 refresh for unchanged current rows (load dates only)
        --------------------------------------------------------------------
        UPDATE tgt
            SET
                  tgt.BZ_LOAD_DATE = COALESCE(tgt.BZ_LOAD_DATE, c.BZ_LOAD_DATE)
                , tgt.SV_LOAD_DATE = @AsOfDate
                , tgt.UDT_DATE     = SYSDATETIME()
        FROM svo.D_VENDOR tgt
        INNER JOIN #chg c
            ON c.ACTION_CD = 'U'
           AND tgt.VENDOR_ID = c.VENDOR_ID
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

