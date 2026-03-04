/* =========================================================
   usp_Load_F_AP_INVOICE_LINE_DISTRIBUTION
   Incremental INSERT only. Sources: bzo.AP_InvoiceHeaderExtractPVO,
   AP_InvoiceLineExtractPVO, AP_InvoiceDistributionExtractPVO.
   Filter: D.AddDateTime > @LastWatermark. Dedupe by INVOICE_DISTRIBUTION_ID.
   Resolve SKs via svo.LINES_CODE_COMBO_LOOKUP and svo.D_* (CURR_IND='Y').
   ========================================================= */
CREATE OR ALTER PROCEDURE svo.usp_Load_F_AP_INVOICE_LINE_DISTRIBUTION
    @BatchId INT = NULL
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    DECLARE
        @ProcName       SYSNAME        = OBJECT_SCHEMA_NAME(@@PROCID) + '.' + OBJECT_NAME(@@PROCID),
        @TargetObject   SYSNAME        = 'svo.F_AP_INVOICE_LINE_DISTRIBUTION',
        @StartDttm      DATETIME2(0)   = SYSDATETIME(),
        @EndDttm        DATETIME2(0),
        @RunId          BIGINT         = NULL,
        @ErrMsg         NVARCHAR(4000) = NULL,
        @AsOfDate       DATE           = CAST(GETDATE() AS DATE),
        @LastWatermark  DATETIME2(7),
        @MaxWatermark   DATETIME2(7)   = NULL,
        @RowInserted    INT            = 0,
        @TableBridgeID  INT            = NULL;

    SELECT @TableBridgeID = TableBridgeID FROM meta.MedallionTableBridge WHERE targettable = N'AP_InvoiceDistributionExtractPVO';

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

        IF OBJECT_ID('tempdb..#dist') IS NOT NULL DROP TABLE #dist;
        SELECT
            D.ApInvoiceDistributionsInvoiceId,
            D.ApInvoiceDistributionsInvoiceLineNumber,
            D.ApInvoiceDistributionsInvoiceDistributionId,
            D.ApInvoiceDistributionsDistributionLineNumber,
            D.ApInvoiceDistributionsDistCodeCombinationId,
            D.ApInvoiceDistributionsAccountingDate,
            D.ApInvoiceDistributionsDistributionClass,
            D.ApInvoiceDistributionsDescription,
            D.ApInvoiceDistributionsAmount,
            D.ApInvoiceDistributionsPostedFlag,
            D.ApInvoiceDistributionsType1099,
            D.ApInvoiceDistributionsPJC_CONTRACT_ID,
            D.ApInvoiceDistributionsPJC_CONTRACT_LINE_ID,
            D.ApInvoiceDistributionsPJC_EXPENDITURE_ITEM_DATE,
            D.ApInvoiceDistributionsPJC_EXPENDITURE_TYPE_ID,
            D.ApInvoiceDistributionsPJC_FUNDING_ALLOCATION_ID,
            D.ApInvoiceDistributionsPJC_ORGANIZATION_ID,
            D.ApInvoiceDistributionsPJC_PROJECT_ID,
            D.ApInvoiceDistributionsPJC_TASK_ID,
            D.ApInvoiceDistributionsPoDistributionId,
            D.ApInvoiceDistributionsLastUpdateDate,
            D.ApInvoiceDistributionsLastUpdatedBy,
            D.ApInvoiceDistributionsLastUpdateLogin,
            D.AddDateTime AS DistAddDateTime
        INTO #dist
        FROM (
            SELECT *,
                ROW_NUMBER() OVER (PARTITION BY D.ApInvoiceDistributionsInvoiceDistributionId ORDER BY D.AddDateTime DESC) AS rn
            FROM bzo.AP_InvoiceDistributionExtractPVO D
            WHERE D.AddDateTime > @LastWatermark
        ) D
        WHERE D.rn = 1;

        SELECT @MaxWatermark = MAX(DistAddDateTime) FROM #dist;

        INSERT INTO svo.F_AP_INVOICE_LINE_DISTRIBUTION (
            AP_INVOICE_HEADER_SK, INVOICE_ID, INVOICE_LINE_NUMBER, INVOICE_DISTRIBUTION_ID, DIST_INV_LINE_NUMBER, DISTRIBUTION_LINE_NUMBER,
            DIST_ACCOUNTING_DATE_SK, LINE_ACCOUNTING_DATE_SK,
            ACCOUNT_SK, BUSINESS_OFFERING_SK, COMPANY_SK, COST_CENTER_SK, INDUSTRY_SK, INTERCOMPANY_SK, LEGAL_ENTITY_SK, BUSINESS_UNIT_SK, VENDOR_SITE_SK, LEDGER_SK,
            DISTRIBUTION_CLASS, DIST_DESCRIPTION, LINE_DESCRIPTION, DISTRIBUTION_AMOUNT, POSTED_FLAG, TYPE_1099,
            INV_TAX_JURIDISTION_CODE, INV_TAX_RATE, LINE_TYPE_LOOKUP_CODE,
            PJC_CONTRACT_ID, PJC_CONTRACT_LINE_ID, PJC_EXPENDITURE_ITEM_DATE, PJC_EXPENDITURE_TYPE_ID, PJC_FUNDING_ALLOCATION_ID, PJC_ORGANIZATION_ID, PJC_PROJECT_ID, PJC_TASK_ID,
            PO_DISTRIBUTION_ID, DIST_LAST_UPDATE_DATE, DIST_LAST_UPDATE_BY, DIST_LAST_UPDATE_LOGIN, DIST_CODE_COMBINATION_ID,
            BZ_LOAD_DATE, SV_LOAD_DATE,
            LINES_AMOUNT_debug_only
        )
        SELECT
            ISNULL(H.AP_INVOICE_HEADER_SK, 0),
            ISNULL(I.ApInvoiceLinesAllInvoiceId, -1),
            ISNULL(I.ApInvoiceLinesAllLineNumber, -1),
            ISNULL(D.ApInvoiceDistributionsInvoiceDistributionId, -1),
            ISNULL(D.ApInvoiceDistributionsInvoiceLineNumber, -1),
            ISNULL(D.ApInvoiceDistributionsDistributionLineNumber, -1),
            ISNULL(CONVERT(INT, FORMAT(D.ApInvoiceDistributionsAccountingDate, 'yyyyMMdd')), 10101),
            ISNULL(CONVERT(INT, FORMAT(I.ApInvoiceLinesAllAccountingDate, 'yyyyMMdd')), 10101),
            ISNULL(DA.ACCOUNT_SK, 0),
            ISNULL(DBO.BUSINESS_OFFERING_SK, 0),
            ISNULL(DCO.COMPANY_SK, 0),
            ISNULL(DCC.COST_CENTER_SK, 0),
            ISNULL(DI.INDUSTRY_SK, 0),
            ISNULL(DIC.INTERCOMPANY_SK, 0),
            ISNULL(LE.LEGAL_ENTITY_SK, 0),
            ISNULL(BU.BUSINESS_UNIT_SK, 0),
            ISNULL(VS.VENDOR_SITE_SK, 0),
            ISNULL(LDG.LEDGER_SK, 0),
            ISNULL(D.ApInvoiceDistributionsDistributionClass, 'U'),
            ISNULL(D.ApInvoiceDistributionsDescription, 'UNK'),
            ISNULL(I.ApInvoiceLinesAllDescription, 'UNK'),
            COALESCE(D.ApInvoiceDistributionsAmount, I.ApInvoiceLinesAllAmount, 0),
            ISNULL(D.ApInvoiceDistributionsPostedFlag, 'U'),
            ISNULL(D.ApInvoiceDistributionsType1099, 0),
            ISNULL(I.ApInvoiceLinesAllTaxJurisdictionCode, 'UNK'),
            ISNULL(I.ApInvoiceLinesAllTaxRate, 0),
            I.ApInvoiceLinesAllLineTypeLookupCode,
            ISNULL(D.ApInvoiceDistributionsPJC_CONTRACT_ID, 0),
            ISNULL(D.ApInvoiceDistributionsPJC_CONTRACT_LINE_ID, 0),
            ISNULL(D.ApInvoiceDistributionsPJC_EXPENDITURE_ITEM_DATE, '9999-12-31'),
            ISNULL(D.ApInvoiceDistributionsPJC_EXPENDITURE_TYPE_ID, 0),
            ISNULL(D.ApInvoiceDistributionsPJC_FUNDING_ALLOCATION_ID, 0),
            ISNULL(D.ApInvoiceDistributionsPJC_ORGANIZATION_ID, 0),
            ISNULL(D.ApInvoiceDistributionsPJC_PROJECT_ID, 0),
            ISNULL(D.ApInvoiceDistributionsPJC_TASK_ID, 0),
            ISNULL(D.ApInvoiceDistributionsPoDistributionId, 0),
            ISNULL(CONVERT(DATE, D.ApInvoiceDistributionsLastUpdateDate), '0001-01-01'),
            ISNULL(D.ApInvoiceDistributionsLastUpdatedBy, 'UNK'),
            ISNULL(D.ApInvoiceDistributionsLastUpdateLogin, 'UNK'),
            ISNULL(D.ApInvoiceDistributionsDistCodeCombinationId, -1),
            ISNULL(D.DistAddDateTime, SYSDATETIME()),
            SYSDATETIME(),
            CAST(COALESCE(I.ApInvoiceLinesAllAmount, D.ApInvoiceDistributionsAmount, 0) AS DECIMAL(29,4))
        FROM #dist D
        JOIN bzo.AP_InvoiceLineExtractPVO I
            ON I.ApInvoiceLinesAllInvoiceId = D.ApInvoiceDistributionsInvoiceId
           AND I.ApInvoiceLinesAllLineNumber = D.ApInvoiceDistributionsInvoiceLineNumber
        JOIN bzo.AP_InvoiceHeaderExtractPVO IH
            ON IH.ApInvoicesInvoiceId = D.ApInvoiceDistributionsInvoiceId
        LEFT JOIN svo.LINES_CODE_COMBO_LOOKUP C
            ON CAST(D.ApInvoiceDistributionsDistCodeCombinationId AS BIGINT) = C.CODE_COMBINATION_BK
        LEFT JOIN svo.D_AP_INVOICE_HEADER H ON H.INVOICE_ID = IH.ApInvoicesInvoiceId
        LEFT JOIN svo.D_ACCOUNT DA ON DA.ACCOUNT_ID = C.ACCOUNT_ID AND DA.CURR_IND = 'Y'
        LEFT JOIN svo.D_BUSINESS_OFFERING DBO ON DBO.BUSINESS_OFFERING_ID = C.BUSINESSOFFERING_ID AND DBO.CURR_IND = 'Y'
        LEFT JOIN svo.D_COMPANY DCO ON DCO.COMPANY_ID = C.COMPANY_ID AND DCO.CURR_IND = 'Y'
        LEFT JOIN svo.D_COST_CENTER DCC ON DCC.COST_CENTER_ID = C.COSTCENTER_ID AND DCC.CURR_IND = 'Y'
        LEFT JOIN svo.D_INDUSTRY DI ON DI.INDUSTRY_ID = C.INDUSTRY_ID AND DI.CURR_IND = 'Y'
        LEFT JOIN svo.D_INTERCOMPANY DIC ON DIC.INTERCOMPANY_ID = C.INTERCOMPANY_ID AND DIC.CURR_IND = 'Y'
        LEFT JOIN svo.D_LEGAL_ENTITY LE ON LE.LEGAL_ENTITY_ID = IH.ApInvoicesLegalEntityId AND LE.CURR_IND = 'Y'
        LEFT JOIN svo.D_BUSINESS_UNIT BU ON BU.BUSINESS_UNIT_ID = IH.ApInvoicesOrgId AND BU.CURR_IND = 'Y'
        LEFT JOIN svo.D_VENDOR_SITE VS ON VS.VENDOR_SITE_ID = IH.ApInvoicesVendorSiteId AND VS.CURR_IND = 'Y'
        LEFT JOIN svo.D_LEDGER LDG ON LDG.LEDGER_ID = IH.ApInvoicesSetOfBooksId AND LDG.CURR_IND = 'Y'
        WHERE NOT EXISTS (SELECT 1 FROM svo.F_AP_INVOICE_LINE_DISTRIBUTION t WHERE t.INVOICE_DISTRIBUTION_ID = D.ApInvoiceDistributionsInvoiceDistributionId);

        SET @RowInserted = @@ROWCOUNT;

        IF @MaxWatermark IS NOT NULL
            UPDATE etl.ETL_WATERMARK SET LAST_WATERMARK = @MaxWatermark, UDT_DATE = SYSDATETIME() WHERE TABLE_NAME = @TargetObject;

        SET @EndDttm = SYSDATETIME();
        UPDATE etl.ETL_RUN SET END_DTTM = @EndDttm, STATUS = 'SUCCESS', ROW_INSERTED = @RowInserted, ROW_UPDATED = 0, ROW_EXPIRED = 0, ERROR_MESSAGE = NULL WHERE RUN_ID = @RunId;
    END TRY
    BEGIN CATCH
        SET @EndDttm = SYSDATETIME();
        SET @ErrMsg = CONCAT('Error ', ERROR_NUMBER(), ' (Line ', ERROR_LINE(), '): ', ERROR_MESSAGE());
        IF @RunId IS NOT NULL
            UPDATE etl.ETL_RUN SET END_DTTM = @EndDttm, STATUS = 'FAILURE', ROW_INSERTED = @RowInserted, ROW_UPDATED = 0, ROW_EXPIRED = 0, ERROR_MESSAGE = @ErrMsg WHERE RUN_ID = @RunId;
        ;THROW;
    END CATCH
END;
GO
