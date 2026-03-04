/* =========================================================
   usp_Load_D_VENDOR
   SCD2 incremental load. Source: bzo.AP_SupplierSitePVO (derived, one row per VendorId)
   Watermark filter, expire changed rows, insert new current rows.
   Idempotent: safe to retry same watermark range.
   ========================================================= */
CREATE OR ALTER PROCEDURE svo.usp_Load_D_VENDOR
    @BatchId INT = NULL
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    DECLARE
        @ProcName       SYSNAME        = OBJECT_SCHEMA_NAME(@@PROCID) + '.' + OBJECT_NAME(@@PROCID),
        @TargetObject   SYSNAME        = 'svo.D_VENDOR',
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
        @RowUpdated     INT            = 0,
        @TableBridgeID  INT            = NULL;

    SELECT @TableBridgeID = TableBridgeID FROM meta.MedallionTableBridge WHERE targettable = N'AP_SupplierSitePVO';

    BEGIN TRY
        SELECT @LastWatermark = w.LAST_WATERMARK
        FROM etl.ETL_WATERMARK w
        WHERE w.TABLE_NAME = @TargetObject;

        IF @LastWatermark IS NULL
            SET @LastWatermark = '1900-01-01';

        INSERT INTO etl.ETL_RUN (PROC_NAME, TARGET_OBJECT, ASOF_DATE, START_DTTM, STATUS, BATCH_ID, TABLE_BRIDGE_ID)
        VALUES (@ProcName, @TargetObject, @AsOfDate, @StartDttm, 'STARTED', ISNULL(@BatchId, -1), ISNULL(@TableBridgeID, -1));

        SET @RunId = SCOPE_IDENTITY();

        IF @RunId IS NULL
            THROW 50001, 'ETL_RUN insert failed: RUN_ID is NULL.', 1;

        IF EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'UX_D_VENDOR_ID' AND object_id = OBJECT_ID('svo.D_VENDOR'))
        BEGIN
            DROP INDEX UX_D_VENDOR_ID ON svo.D_VENDOR;
        END;

        IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'UX_D_VENDOR_BK_CURR' AND object_id = OBJECT_ID('svo.D_VENDOR'))
        BEGIN
            CREATE UNIQUE NONCLUSTERED INDEX UX_D_VENDOR_BK_CURR
            ON svo.D_VENDOR (VENDOR_ID)
            WHERE CURR_IND = 'Y'
            ON FG_SilverDim;
        END;

        IF NOT EXISTS (SELECT 1 FROM svo.D_VENDOR WHERE VENDOR_SK = 0)
        BEGIN
            SET IDENTITY_INSERT svo.D_VENDOR ON;

            INSERT INTO svo.D_VENDOR
            (VENDOR_SK, VENDOR_ID, VENDOR_NAME, VENDOR_NUMBER, VENDOR_TYPE_CODE, ORG_TYPE_CODE, START_DATE_ACTIVE, END_DATE_ACTIVE, FEDERAL_REPORTABLE_FLAG, STATE_REPORTABLE_FLAG, WITHHOLDING_STATUS_CODE, WITHHOLDING_START_DATE, TAXPAYER_COUNTRY, VAT_CODE, BZ_LOAD_DATE, SV_LOAD_DATE, EFF_DATE, END_DATE, CRE_DATE, UDT_DATE, CURR_IND)
            VALUES
            (0, -1, 'Unknown Vendor', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, '1900-01-01', GETDATE(), @AsOfDate, @HighDate, @LoadDttm, @LoadDttm, 'Y');

            SET IDENTITY_INSERT svo.D_VENDOR OFF;
        END;

        IF OBJECT_ID('tempdb..#src') IS NOT NULL DROP TABLE #src;

        SELECT
            s.VENDOR_ID, s.VENDOR_NAME, s.VENDOR_NUMBER, s.VENDOR_TYPE_CODE, s.ORG_TYPE_CODE,
            s.START_DATE_ACTIVE, s.END_DATE_ACTIVE, s.FEDERAL_REPORTABLE_FLAG, s.STATE_REPORTABLE_FLAG,
            s.WITHHOLDING_STATUS_CODE, s.WITHHOLDING_START_DATE, s.TAXPAYER_COUNTRY, s.VAT_CODE,
            s.BZ_LOAD_DATE, s.SV_LOAD_DATE, s.SourceAddDateTime
        INTO #src
        FROM
        (
            SELECT
                s.SupplierVendorId AS VENDOR_ID,
                COALESCE(s.SuppPartyPartyName, s.SupplierTaxReportingName) AS VENDOR_NAME,
                s.SupplierSegment1 AS VENDOR_NUMBER,
                s.SupplierVendorTypeLookupCode AS VENDOR_TYPE_CODE,
                s.SupplierOrganizationTypeLookupCode AS ORG_TYPE_CODE,
                CAST(s.SupplierStartDateActive AS DATE) AS START_DATE_ACTIVE,
                CAST(s.SupplierEndDateActive AS DATE) AS END_DATE_ACTIVE,
                s.SupplierFederalReportableFlag AS FEDERAL_REPORTABLE_FLAG,
                s.SupplierStateReportableFlag AS STATE_REPORTABLE_FLAG,
                s.SupplierWithholdingStatusLookupCode AS WITHHOLDING_STATUS_CODE,
                CAST(s.SupplierWithholdingStartDate AS DATE) AS WITHHOLDING_START_DATE,
                s.SupplierTaxpayerCountry AS TAXPAYER_COUNTRY,
                s.SupplierVatCode AS VAT_CODE,
                CAST(s.AddDateTime AS DATETIME) AS BZ_LOAD_DATE,
                GETDATE() AS SV_LOAD_DATE,
                s.AddDateTime AS SourceAddDateTime,
                ROW_NUMBER() OVER (PARTITION BY s.SupplierVendorId ORDER BY s.SupplierLastUpdateDate DESC, s.SupplierSiteLastUpdateDate DESC, s.AddDateTime DESC, s.VendorSiteId DESC) AS rn
            FROM bzo.AP_SupplierSitePVO s
            WHERE s.SupplierVendorId IS NOT NULL
              AND s.AddDateTime > @LastWatermark
        ) s
        WHERE s.rn = 1;

        SELECT @MaxWatermark = MAX(SourceAddDateTime) FROM #src;

        UPDATE tgt
        SET tgt.END_DATE = DATEADD(DAY, -1, @AsOfDate), tgt.CURR_IND = 'N', tgt.UDT_DATE = @LoadDttm
        FROM svo.D_VENDOR tgt
        INNER JOIN #src src ON src.VENDOR_ID = tgt.VENDOR_ID
        WHERE tgt.CURR_IND = 'Y'
          AND (
                ISNULL(tgt.VENDOR_NAME,'') <> ISNULL(src.VENDOR_NAME,'')
             OR ISNULL(tgt.VENDOR_NUMBER,'') <> ISNULL(src.VENDOR_NUMBER,'')
             OR ISNULL(tgt.VENDOR_TYPE_CODE,'') <> ISNULL(src.VENDOR_TYPE_CODE,'')
             OR ISNULL(tgt.ORG_TYPE_CODE,'') <> ISNULL(src.ORG_TYPE_CODE,'')
             OR ISNULL(tgt.START_DATE_ACTIVE,'1900-01-01') <> ISNULL(src.START_DATE_ACTIVE,'1900-01-01')
             OR ISNULL(tgt.END_DATE_ACTIVE,'1900-01-01') <> ISNULL(src.END_DATE_ACTIVE,'1900-01-01')
             OR ISNULL(tgt.FEDERAL_REPORTABLE_FLAG,'') <> ISNULL(src.FEDERAL_REPORTABLE_FLAG,'')
             OR ISNULL(tgt.STATE_REPORTABLE_FLAG,'') <> ISNULL(src.STATE_REPORTABLE_FLAG,'')
             OR ISNULL(tgt.WITHHOLDING_STATUS_CODE,'') <> ISNULL(src.WITHHOLDING_STATUS_CODE,'')
             OR ISNULL(tgt.WITHHOLDING_START_DATE,'1900-01-01') <> ISNULL(src.WITHHOLDING_START_DATE,'1900-01-01')
             OR ISNULL(tgt.TAXPAYER_COUNTRY,'') <> ISNULL(src.TAXPAYER_COUNTRY,'')
             OR ISNULL(tgt.VAT_CODE,'') <> ISNULL(src.VAT_CODE,'')
          );

        SET @RowExpired = @@ROWCOUNT;

        INSERT INTO svo.D_VENDOR
        (VENDOR_ID, VENDOR_NAME, VENDOR_NUMBER, VENDOR_TYPE_CODE, ORG_TYPE_CODE, START_DATE_ACTIVE, END_DATE_ACTIVE, FEDERAL_REPORTABLE_FLAG, STATE_REPORTABLE_FLAG, WITHHOLDING_STATUS_CODE, WITHHOLDING_START_DATE, TAXPAYER_COUNTRY, VAT_CODE, BZ_LOAD_DATE, SV_LOAD_DATE, EFF_DATE, END_DATE, CRE_DATE, UDT_DATE, CURR_IND)
        SELECT
            src.VENDOR_ID, src.VENDOR_NAME, src.VENDOR_NUMBER, src.VENDOR_TYPE_CODE, src.ORG_TYPE_CODE,
            src.START_DATE_ACTIVE, src.END_DATE_ACTIVE, src.FEDERAL_REPORTABLE_FLAG, src.STATE_REPORTABLE_FLAG,
            src.WITHHOLDING_STATUS_CODE, src.WITHHOLDING_START_DATE, src.TAXPAYER_COUNTRY, src.VAT_CODE,
            src.BZ_LOAD_DATE, src.SV_LOAD_DATE, @AsOfDate, @HighDate, @LoadDttm, @LoadDttm, 'Y'
        FROM #src src
        LEFT JOIN svo.D_VENDOR tgt ON tgt.VENDOR_ID = src.VENDOR_ID AND tgt.CURR_IND = 'Y'
        WHERE tgt.VENDOR_ID IS NULL;

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
