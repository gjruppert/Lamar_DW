USE [Oracle_Reporting_P2];
GO

/* =================
   D_VENDOR (derived)
   ================= */
IF OBJECT_ID(N'svo.D_VENDOR', 'U') IS NOT NULL
    DROP TABLE svo.D_VENDOR;
GO

CREATE TABLE svo.D_VENDOR
(
    VENDOR_SK                         BIGINT IDENTITY(1,1) NOT NULL,
    VENDOR_ID                         BIGINT               NOT NULL,   -- SupplierVendorId (BK)

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

    BZ_LOAD_DATE                      DATE                 NULL,
    SV_LOAD_DATE                      DATE                 NOT NULL,

    CONSTRAINT PK_D_VENDOR PRIMARY KEY CLUSTERED (VENDOR_SK) ON FG_SilverDim,
    CONSTRAINT UX_D_VENDOR_VendorId UNIQUE (VENDOR_ID) ON FG_SilverDim
) ON FG_SilverDim;
GO

/* Plug row */
SET IDENTITY_INSERT svo.D_VENDOR ON;

INSERT INTO svo.D_VENDOR
(
    VENDOR_SK, VENDOR_ID, VENDOR_NAME, VENDOR_NUMBER, VENDOR_TYPE_CODE, ORG_TYPE_CODE,
    START_DATE_ACTIVE, END_DATE_ACTIVE, FEDERAL_REPORTABLE_FLAG, STATE_REPORTABLE_FLAG,
    WITHHOLDING_STATUS_CODE, WITHHOLDING_START_DATE, TAXPAYER_COUNTRY, VAT_CODE,
    BZ_LOAD_DATE, SV_LOAD_DATE
)
VALUES
(
    0, 0, 'Unknown Vendor', NULL, NULL, NULL,
    NULL, NULL, NULL, NULL,
    NULL, NULL, NULL, NULL,
    CAST('1900-01-01' AS DATE), CAST(GETDATE() AS DATE)
);

SET IDENTITY_INSERT svo.D_VENDOR OFF;
GO

USE [Oracle_Reporting_P2];
GO

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

        CAST(s.AddDateTime AS DATE) AS BZ_LOAD_DATE,

        ROW_NUMBER() OVER
        (
            PARTITION BY s.SupplierVendorId
            ORDER BY
                s.SupplierLastUpdateDate DESC,
                s.SupplierSiteLastUpdateDate DESC,
                s.AddDateTime DESC,
                s.VendorSiteId DESC
        ) AS rn
    FROM bzo.AP_SupplierSitePVO s
    WHERE s.SupplierVendorId IS NOT NULL
)
MERGE svo.D_VENDOR AS tgt
USING
(
    SELECT *
    FROM VendorPick
    WHERE rn = 1
) AS src
ON (tgt.VENDOR_ID = src.VENDOR_ID)
WHEN MATCHED THEN
    UPDATE SET
        tgt.VENDOR_NAME               = src.VENDOR_NAME,
        tgt.VENDOR_NUMBER             = src.VENDOR_NUMBER,
        tgt.VENDOR_TYPE_CODE          = src.VENDOR_TYPE_CODE,
        tgt.ORG_TYPE_CODE             = src.ORG_TYPE_CODE,
        tgt.START_DATE_ACTIVE         = src.START_DATE_ACTIVE,
        tgt.END_DATE_ACTIVE           = src.END_DATE_ACTIVE,
        tgt.FEDERAL_REPORTABLE_FLAG   = src.FEDERAL_REPORTABLE_FLAG,
        tgt.STATE_REPORTABLE_FLAG     = src.STATE_REPORTABLE_FLAG,
        tgt.WITHHOLDING_STATUS_CODE   = src.WITHHOLDING_STATUS_CODE,
        tgt.WITHHOLDING_START_DATE    = src.WITHHOLDING_START_DATE,
        tgt.TAXPAYER_COUNTRY          = src.TAXPAYER_COUNTRY,
        tgt.VAT_CODE                  = src.VAT_CODE,
        tgt.BZ_LOAD_DATE              = src.BZ_LOAD_DATE,
        tgt.SV_LOAD_DATE              = CAST(GETDATE() AS DATE)
WHEN NOT MATCHED BY TARGET THEN
    INSERT
    (
        VENDOR_ID, VENDOR_NAME, VENDOR_NUMBER, VENDOR_TYPE_CODE, ORG_TYPE_CODE,
        START_DATE_ACTIVE, END_DATE_ACTIVE, FEDERAL_REPORTABLE_FLAG, STATE_REPORTABLE_FLAG,
        WITHHOLDING_STATUS_CODE, WITHHOLDING_START_DATE, TAXPAYER_COUNTRY, VAT_CODE,
        BZ_LOAD_DATE, SV_LOAD_DATE
    )
    VALUES
    (
        src.VENDOR_ID, src.VENDOR_NAME, src.VENDOR_NUMBER, src.VENDOR_TYPE_CODE, src.ORG_TYPE_CODE,
        src.START_DATE_ACTIVE, src.END_DATE_ACTIVE, src.FEDERAL_REPORTABLE_FLAG, src.STATE_REPORTABLE_FLAG,
        src.WITHHOLDING_STATUS_CODE, src.WITHHOLDING_START_DATE, src.TAXPAYER_COUNTRY, src.VAT_CODE,
        src.BZ_LOAD_DATE, CAST(GETDATE() AS DATE)
    );
GO
