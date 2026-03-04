

--/* D_VENDOR_SITE (Dim, Type 1, no SCD) */
--IF OBJECT_ID('svo.D_VENDOR_SITE','U') IS NOT NULL
--    DROP TABLE svo.D_VENDOR_SITE;
--GO

--CREATE TABLE svo.D_VENDOR_SITE(
--    VENDOR_SITE_SK            BIGINT IDENTITY(1,1) PRIMARY KEY,
--    VENDOR_SITE_ID            BIGINT        NOT NULL,   -- natural key
--    VENDOR_ID                 BIGINT        NULL,
--    VENDOR_SITE               VARCHAR(240)  NULL,
--    SUPPLIER_PARTY_ID         BIGINT        NOT NULL,

--    /* New fields */
--    VENDOR_TYPE_LOOKUP        VARCHAR(60)   NULL,
--    PARTY_NAME                VARCHAR(240)  NULL,
--    ADDRESS_1                 VARCHAR(240)  NULL,
--    ADDRESS_2                 VARCHAR(240)  NULL,
--    ADDRESS_3                 VARCHAR(240)  NULL,

--    CITY                      VARCHAR(60)   NULL,
--    STATE                     VARCHAR(60)   NULL,
--    POSTAL_CODE               VARCHAR(60)   NULL,

--    /* New field */
--    POSTAL_CODE_4             VARCHAR(60)   NULL,

--    COUNTRY_CODE              VARCHAR(3)    NULL,
--    INVOICE_CURRENCY          VARCHAR(15)   NULL,
--    PAYMENT_CURRENCY          VARCHAR(15)   NULL,
--    PAY_SITE_FLAG             VARCHAR(1)    NULL,
--    PRIMARY_PAY_SITE_FLAG     VARCHAR(1)    NULL,

--    /* New field */
--    STATUS_FLAG               VARCHAR(1)    NULL,

--    BZ_LOAD_DATE              DATE          NULL,
--    SV_LOAD_DATE              DATE          NOT NULL DEFAULT CAST(GETDATE() AS DATE)
--) ON [FG_SilverDim];
--GO

TRUNCATE TABLE svo.D_VENDOR_SITE;
GO

/* Plug row */
SET IDENTITY_INSERT [svo].D_VENDOR_SITE ON;

INSERT INTO svo.D_VENDOR_SITE
(
    VENDOR_SITE_SK,
    VENDOR_SITE_ID,
    VENDOR_ID,
    VENDOR_SITE,
    SUPPLIER_PARTY_ID,
    VENDOR_TYPE_LOOKUP,
    PARTY_NAME,
    ADDRESS_1,
    ADDRESS_2,
    ADDRESS_3,
    CITY,
    STATE,
    POSTAL_CODE,
    POSTAL_CODE_4,
    COUNTRY_CODE,
    INVOICE_CURRENCY,
    PAYMENT_CURRENCY,
    PAY_SITE_FLAG,
    PRIMARY_PAY_SITE_FLAG,
    STATUS_FLAG,
    BZ_LOAD_DATE,
    SV_LOAD_DATE
)
VALUES
(
    0,                      -- VENDOR_SITE_SK
    -1,                     -- VENDOR_SITE_ID
    -1,                     -- VENDOR_ID
    'UNKNOWN',              -- VENDOR_SITE
    -1,                     -- SUPPLIER_PARTY_ID
    'UNK',                  -- VENDOR_TYPE_LOOKUP
    'UNKNOWN',              -- PARTY_NAME
    'UNKNOWN',              -- ADDRESS_1
    'UNKNOWN',              -- ADDRESS_2
    'UNKNOWN',              -- ADDRESS_3
    'UNKNOWN',              -- CITY
    'UNKNOWN',              -- STATE
    'UNKNOWN',              -- POSTAL_CODE
    '0000',                 -- POSTAL_CODE_4
    'UN',                   -- COUNTRY_CODE
    'UNK',                  -- INVOICE_CURRENCY
    'UNK',                  -- PAYMENT_CURRENCY
    'N',                    -- PAY_SITE_FLAG
    'N',                    -- PRIMARY_PAY_SITE_FLAG
    'U',                    -- STATUS_FLAG
    CAST(GETDATE() AS DATE),-- BZ_LOAD_DATE
    CAST(GETDATE() AS DATE) -- SV_LOAD_DATE
);

SET IDENTITY_INSERT [svo].D_VENDOR_SITE OFF;
GO

/* Type 1 load from bzo.AP_SupplierSitePVO */
SET XACT_ABORT ON;
BEGIN TRAN;

DECLARE @today DATE = CAST(GETDATE() AS DATE);

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
        ISNULL(SupplierSiteInvoiceCurrencyCode,'UNK')    AS INVOICE_CURR,
        ISNULL(SupplierSitePaymentCurrencyCode,'UNK')    AS PAYMENT_CURR,
        ISNULL(SupplierSitePaySiteFlag,'U')              AS PAY_FLAG,
        ISNULL(SupplierSitePrimaryPaySiteFlag,'U')       AS PRIMARY_PAY_FLAG,
        ISNULL([LocationStatusFlag],'U')                 AS STATUS_FLAG,
        CAST(AddDateTime AS DATE)                        AS BZ_LOAD_DATE
    FROM bzo.AP_SupplierSitePVO
    WHERE VendorSiteId IS NOT NULL
)
MERGE svo.D_VENDOR_SITE AS D
USING S
   ON D.VENDOR_SITE_ID = S.VENDOR_SITE_ID
WHEN MATCHED THEN
    UPDATE SET
        D.VENDOR_ID             = S.VENDOR_ID,
        D.VENDOR_SITE           = S.VENDOR_SITE,
        D.SUPPLIER_PARTY_ID     = S.SUPPLIER_PARTY_ID,
        D.VENDOR_TYPE_LOOKUP    = S.VENDOR_TYPE_LOOKUP,
        D.PARTY_NAME            = S.PARTY_NAME,
        D.ADDRESS_1             = S.ADDRESS_1,
        D.ADDRESS_2             = S.ADDRESS_2,
        D.ADDRESS_3             = S.ADDRESS_3,
        D.CITY                  = S.CITY,
        D.STATE                 = S.STATE,
        D.POSTAL_CODE           = S.POSTAL_CODE,
        D.POSTAL_CODE_4         = S.POSTAL_CODE_4,
        D.COUNTRY_CODE          = S.COUNTRY_CODE,
        D.INVOICE_CURRENCY      = S.INVOICE_CURR,
        D.PAYMENT_CURRENCY      = S.PAYMENT_CURR,
        D.PAY_SITE_FLAG         = S.PAY_FLAG,
        D.PRIMARY_PAY_SITE_FLAG = S.PRIMARY_PAY_FLAG,
        D.STATUS_FLAG           = S.STATUS_FLAG,
        D.BZ_LOAD_DATE          = S.BZ_LOAD_DATE,
        D.SV_LOAD_DATE          = @today
WHEN NOT MATCHED BY TARGET THEN
    INSERT
    (
        VENDOR_SITE_ID,
        VENDOR_ID,
        VENDOR_SITE,
        SUPPLIER_PARTY_ID,
        VENDOR_TYPE_LOOKUP,
        PARTY_NAME,
        ADDRESS_1,
        ADDRESS_2,
        ADDRESS_3,
        CITY,
        STATE,
        POSTAL_CODE,
        POSTAL_CODE_4,
        COUNTRY_CODE,
        INVOICE_CURRENCY,
        PAYMENT_CURRENCY,
        PAY_SITE_FLAG,
        PRIMARY_PAY_SITE_FLAG,
        STATUS_FLAG,
        BZ_LOAD_DATE,
        SV_LOAD_DATE
    )
    VALUES
    (
        S.VENDOR_SITE_ID,
        S.VENDOR_ID,
        S.VENDOR_SITE,
        S.SUPPLIER_PARTY_ID,
        S.VENDOR_TYPE_LOOKUP,
        S.PARTY_NAME,
        S.ADDRESS_1,
        S.ADDRESS_2,
        S.ADDRESS_3,
        S.CITY,
        S.STATE,
        S.POSTAL_CODE,
        S.POSTAL_CODE_4,
        S.COUNTRY_CODE,
        S.INVOICE_CURR,
        S.PAYMENT_CURR,
        S.PAY_FLAG,
        S.PRIMARY_PAY_FLAG,
        S.STATUS_FLAG,
        S.BZ_LOAD_DATE,
        @today
    );

COMMIT;
GO
