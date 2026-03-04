/* =========================================================
   usp_Load_D_VENDOR_SITE
   SCD2 incremental load. Source: bzo.AP_SupplierSitePVO
   Watermark filter, expire changed rows, insert new current rows.
   Idempotent: safe to retry same watermark range.
   ========================================================= */
CREATE OR ALTER PROCEDURE svo.usp_Load_D_VENDOR_SITE
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    DECLARE
        @ProcName       SYSNAME        = OBJECT_SCHEMA_NAME(@@PROCID) + '.' + OBJECT_NAME(@@PROCID),
        @TargetObject   SYSNAME        = 'svo.D_VENDOR_SITE',
        @StartDttm      DATETIME2(0)   = SYSDATETIME(),
        @EndDttm        DATETIME2(0),
        @RunId          BIGINT         = NULL,
        @ErrMsg         NVARCHAR(4000) = NULL,

        @AsOfDate       DATE           = CAST(GETDATE() AS DATE),
        @LoadDttm       DATETIME2(0)   = SYSDATETIME(),
        @HighDate       DATE           = '9999-12-31',

        @LastWatermark  DATETIME2(7),
        @MaxWatermark   DATETIME2(7)   = NULL,

        @RowInserted    INT            = 0,
        @RowExpired     INT            = 0,
        @RowUpdated     INT            = 0;

    BEGIN TRY
        SELECT @LastWatermark = w.LAST_WATERMARK
        FROM etl.ETL_WATERMARK w
        WHERE w.TABLE_NAME = @TargetObject;

        IF @LastWatermark IS NULL
            SET @LastWatermark = '1900-01-01';

        INSERT INTO etl.ETL_RUN (PROC_NAME, TARGET_OBJECT, ASOF_DATE, START_DTTM, STATUS)
        VALUES (@ProcName, @TargetObject, @AsOfDate, @StartDttm, 'STARTED');

        SET @RunId = SCOPE_IDENTITY();

        IF @RunId IS NULL
            THROW 50001, 'ETL_RUN insert failed: RUN_ID is NULL.', 1;

        IF EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'UX_D_VENDOR_SITE_ID' AND object_id = OBJECT_ID('svo.D_VENDOR_SITE'))
        BEGIN
            DROP INDEX UX_D_VENDOR_SITE_ID ON svo.D_VENDOR_SITE;
        END;

        IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'UX_D_VENDOR_SITE_BK_CURR' AND object_id = OBJECT_ID('svo.D_VENDOR_SITE'))
        BEGIN
            CREATE UNIQUE NONCLUSTERED INDEX UX_D_VENDOR_SITE_BK_CURR
            ON svo.D_VENDOR_SITE (VENDOR_SITE_ID)
            WHERE CURR_IND = 'Y'
            ON FG_SilverDim;
        END;

        IF NOT EXISTS (SELECT 1 FROM svo.D_VENDOR_SITE WHERE VENDOR_SITE_SK = 0)
        BEGIN
            SET IDENTITY_INSERT svo.D_VENDOR_SITE ON;

            INSERT INTO svo.D_VENDOR_SITE
            (VENDOR_SITE_SK, VENDOR_SITE_ID, VENDOR_ID, VENDOR_SITE, SUPPLIER_PARTY_ID, VENDOR_TYPE_LOOKUP, PARTY_NAME, ADDRESS_1, ADDRESS_2, ADDRESS_3, CITY, STATE, POSTAL_CODE, POSTAL_CODE_4, COUNTRY_CODE, INVOICE_CURRENCY, PAYMENT_CURRENCY, PAY_SITE_FLAG, PRIMARY_PAY_SITE_FLAG, STATUS_FLAG, BZ_LOAD_DATE, SV_LOAD_DATE, EFF_DATE, END_DATE, CRE_DATE, UDT_DATE, CURR_IND)
            VALUES
            (0, -1, -1, 'UNKNOWN', -1, 'UNK', 'UNKNOWN', 'UNKNOWN', 'UNKNOWN', 'UNKNOWN', 'UNKNOWN', 'UNKNOWN', 'UNKNOWN', '0000', 'UN', 'UNK', 'UNK', 'N', 'N', 'U', CAST(GETDATE() AS DATE), CAST(GETDATE() AS DATE), @AsOfDate, @HighDate, @LoadDttm, @LoadDttm, 'Y');

            SET IDENTITY_INSERT svo.D_VENDOR_SITE OFF;
        END;

        IF OBJECT_ID('tempdb..#src') IS NOT NULL DROP TABLE #src;

        SELECT
            s.VENDOR_SITE_ID, s.VENDOR_ID, s.VENDOR_SITE, s.SUPPLIER_PARTY_ID, s.VENDOR_TYPE_LOOKUP, s.PARTY_NAME,
            s.ADDRESS_1, s.ADDRESS_2, s.ADDRESS_3, s.CITY, s.STATE, s.POSTAL_CODE, s.POSTAL_CODE_4, s.COUNTRY_CODE,
            s.INVOICE_CURRENCY, s.PAYMENT_CURRENCY, s.PAY_SITE_FLAG, s.PRIMARY_PAY_SITE_FLAG, s.STATUS_FLAG,
            s.BZ_LOAD_DATE, s.SV_LOAD_DATE, s.SourceAddDateTime
        INTO #src
        FROM
        (
            SELECT
                S.VendorSiteId AS VENDOR_SITE_ID,
                S.SupplierVendorId AS VENDOR_ID,
                ISNULL(S.SupplierSiteVendorSiteCode,'UNK') AS VENDOR_SITE,
                ISNULL(S.SuppPartyPartyId,-1) AS SUPPLIER_PARTY_ID,
                ISNULL(S.SupplierVendorTypeLookupCode,'UNK') AS VENDOR_TYPE_LOOKUP,
                COALESCE(S.SuppPartyPartyName,S.LocationAddress4,'UNK') AS PARTY_NAME,
                S.LocationAddress1 AS ADDRESS_1,
                S.LocationAddress2 AS ADDRESS_2,
                S.LocationAddress3 AS ADDRESS_3,
                ISNULL(S.LocationCity,'UNK') AS CITY,
                ISNULL(S.LocationState,'UN') AS STATE,
                ISNULL(S.LocationPostalCode,'00000') AS POSTAL_CODE,
                ISNULL(S.LocationPostalPlus4Code,'0000') AS POSTAL_CODE_4,
                ISNULL(S.LocationCountry,'UN') AS COUNTRY_CODE,
                ISNULL(S.SupplierSiteInvoiceCurrencyCode,'UNK') AS INVOICE_CURRENCY,
                ISNULL(S.SupplierSitePaymentCurrencyCode,'UNK') AS PAYMENT_CURRENCY,
                ISNULL(S.SupplierSitePaySiteFlag,'U') AS PAY_SITE_FLAG,
                ISNULL(S.SupplierSitePrimaryPaySiteFlag,'U') AS PRIMARY_PAY_SITE_FLAG,
                ISNULL(S.LocationStatusFlag,'U') AS STATUS_FLAG,
                CAST(S.AddDateTime AS DATE) AS BZ_LOAD_DATE,
                CAST(GETDATE() AS DATE) AS SV_LOAD_DATE,
                S.AddDateTime AS SourceAddDateTime,
                ROW_NUMBER() OVER (PARTITION BY S.VendorSiteId ORDER BY S.AddDateTime DESC) AS rn
            FROM bzo.AP_SupplierSitePVO S
            WHERE S.VendorSiteId IS NOT NULL
              AND S.AddDateTime > @LastWatermark
        ) s
        WHERE s.rn = 1;

        SELECT @MaxWatermark = MAX(SourceAddDateTime) FROM #src;

        UPDATE tgt
        SET tgt.END_DATE = DATEADD(DAY, -1, @AsOfDate), tgt.CURR_IND = 'N', tgt.UDT_DATE = @LoadDttm
        FROM svo.D_VENDOR_SITE tgt
        INNER JOIN #src src ON src.VENDOR_SITE_ID = tgt.VENDOR_SITE_ID
        WHERE tgt.CURR_IND = 'Y'
          AND (
                ISNULL(tgt.VENDOR_ID, -999) <> ISNULL(src.VENDOR_ID, -999)
             OR ISNULL(tgt.VENDOR_SITE,'') <> ISNULL(src.VENDOR_SITE,'')
             OR ISNULL(tgt.SUPPLIER_PARTY_ID, -999) <> ISNULL(src.SUPPLIER_PARTY_ID, -999)
             OR ISNULL(tgt.VENDOR_TYPE_LOOKUP,'') <> ISNULL(src.VENDOR_TYPE_LOOKUP,'')
             OR ISNULL(tgt.PARTY_NAME,'') <> ISNULL(src.PARTY_NAME,'')
             OR ISNULL(tgt.ADDRESS_1,'') <> ISNULL(src.ADDRESS_1,'')
             OR ISNULL(tgt.ADDRESS_2,'') <> ISNULL(src.ADDRESS_2,'')
             OR ISNULL(tgt.ADDRESS_3,'') <> ISNULL(src.ADDRESS_3,'')
             OR ISNULL(tgt.CITY,'') <> ISNULL(src.CITY,'')
             OR ISNULL(tgt.STATE,'') <> ISNULL(src.STATE,'')
             OR ISNULL(tgt.POSTAL_CODE,'') <> ISNULL(src.POSTAL_CODE,'')
             OR ISNULL(tgt.POSTAL_CODE_4,'') <> ISNULL(src.POSTAL_CODE_4,'')
             OR ISNULL(tgt.COUNTRY_CODE,'') <> ISNULL(src.COUNTRY_CODE,'')
             OR ISNULL(tgt.INVOICE_CURRENCY,'') <> ISNULL(src.INVOICE_CURRENCY,'')
             OR ISNULL(tgt.PAYMENT_CURRENCY,'') <> ISNULL(src.PAYMENT_CURRENCY,'')
             OR ISNULL(tgt.PAY_SITE_FLAG,'') <> ISNULL(src.PAY_SITE_FLAG,'')
             OR ISNULL(tgt.PRIMARY_PAY_SITE_FLAG,'') <> ISNULL(src.PRIMARY_PAY_SITE_FLAG,'')
             OR ISNULL(tgt.STATUS_FLAG,'') <> ISNULL(src.STATUS_FLAG,'')
          );

        SET @RowExpired = @@ROWCOUNT;

        INSERT INTO svo.D_VENDOR_SITE
        (VENDOR_SITE_ID, VENDOR_ID, VENDOR_SITE, SUPPLIER_PARTY_ID, VENDOR_TYPE_LOOKUP, PARTY_NAME, ADDRESS_1, ADDRESS_2, ADDRESS_3, CITY, STATE, POSTAL_CODE, POSTAL_CODE_4, COUNTRY_CODE, INVOICE_CURRENCY, PAYMENT_CURRENCY, PAY_SITE_FLAG, PRIMARY_PAY_SITE_FLAG, STATUS_FLAG, BZ_LOAD_DATE, SV_LOAD_DATE, EFF_DATE, END_DATE, CRE_DATE, UDT_DATE, CURR_IND)
        SELECT
            src.VENDOR_SITE_ID, src.VENDOR_ID, src.VENDOR_SITE, src.SUPPLIER_PARTY_ID, src.VENDOR_TYPE_LOOKUP, src.PARTY_NAME,
            src.ADDRESS_1, src.ADDRESS_2, src.ADDRESS_3, src.CITY, src.STATE, src.POSTAL_CODE, src.POSTAL_CODE_4, src.COUNTRY_CODE,
            src.INVOICE_CURRENCY, src.PAYMENT_CURRENCY, src.PAY_SITE_FLAG, src.PRIMARY_PAY_SITE_FLAG, src.STATUS_FLAG,
            src.BZ_LOAD_DATE, src.SV_LOAD_DATE, @AsOfDate, @HighDate, @LoadDttm, @LoadDttm, 'Y'
        FROM #src src
        LEFT JOIN svo.D_VENDOR_SITE tgt ON tgt.VENDOR_SITE_ID = src.VENDOR_SITE_ID AND tgt.CURR_IND = 'Y'
        WHERE tgt.VENDOR_SITE_ID IS NULL;

        SET @RowInserted = @@ROWCOUNT;

        IF @MaxWatermark IS NOT NULL
        BEGIN
            UPDATE etl.ETL_WATERMARK SET LAST_WATERMARK = @MaxWatermark, UDT_DATE = SYSDATETIME() WHERE TABLE_NAME = @TargetObject;
        END

        SET @EndDttm = SYSDATETIME();
        UPDATE etl.ETL_RUN SET END_DTTM = @EndDttm, STATUS = 'SUCCESS', ROW_INSERTED = @RowInserted, ROW_EXPIRED = @RowExpired, ROW_UPDATED = @RowUpdated, ERROR_MESSAGE = NULL WHERE RUN_ID = @RunId;
    END TRY
    BEGIN CATCH
        SET @EndDttm = SYSDATETIME();
        SET @ErrMsg  = CONCAT('Error ', ERROR_NUMBER(), ' (Line ', ERROR_LINE(), '): ', ERROR_MESSAGE());
        IF @RunId IS NOT NULL
            UPDATE etl.ETL_RUN SET END_DTTM = @EndDttm, STATUS = 'FAILURE', ROW_INSERTED = @RowInserted, ROW_EXPIRED = @RowExpired, ROW_UPDATED = @RowUpdated, ERROR_MESSAGE = @ErrMsg WHERE RUN_ID = @RunId;
        ;THROW;
    END CATCH
END;
GO
