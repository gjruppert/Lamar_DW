/* =========================================================
   usp_Load_F_AP_INVOICE_LINE_DISTRIBUTION
   Incremental INSERT only. Sources:
   (1) AP: bzo.AP_InvoiceHeaderExtractPVO, AP_InvoiceLineExtractPVO, AP_InvoiceDistributionExtractPVO
   (2) SLA AP_INV_DIST: svo.F_SL_JOURNAL_DISTRIBUTION (SOURCE_DISTRIBUTION_TYPE = 'AP_INV_DIST')
   (3) SLA OPEXP: svo.F_SL_JOURNAL_DISTRIBUTION - invoice match, OPEXP account, App 200, AP_INVOICES
   Prerequisite: usp_Load_F_SL_JOURNAL_DISTRIBUTION must run before this proc.
   Filter: D.AddDateTime > @LastWatermark (AP) / BZ_LOAD_DATE > @LastWatermark (SLA). Dedupe by INVOICE_DISTRIBUTION_ID.
   ORIG_AP_INVOICE: 1 = from AP, 0 = from SLA (both #dist_sla and #dist_sla_opexp).
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

        /* --- (1) AP source: #dist from bronze --- */
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

        /* --- (2) SLA AP_INV_DIST: #dist_sla from DW fact F_SL_JOURNAL_DISTRIBUTION --- */
        IF OBJECT_ID('tempdb..#dist_sla') IS NOT NULL DROP TABLE #dist_sla;
        SELECT
            D.TRANSACTION_SOURCE_ID_INT1 AS InvoiceId,
            D.SOURCE_DIST_ID_NUM1 AS InvoiceDistributionId,
            D.SOURCE_DIST_ID_NUM2 AS InvoiceLineNumber,
            D.TRANSACTION_ENTITY_CODE AS TransactionEntityEntityCode,
            D.SOURCE_DISTRIBUTION_TYPE AS XladistlinkSourceDistributionType,
            ISNULL(D.ENTERED_CR, 0) AS XladistlinkUnroundedEnteredCr,
            NULL AS XladistlinkUnroundedEnteredDr,
            D.ACCOUNTING_CLASS_CODE AS XlalinesAccountingClassCode,
            D.CODE_COMBINATION_ID AS XlalinesCodeCombinationId,
            CONVERT(DATE, CONVERT(VARCHAR(8), D.ACCOUNTING_DATE_SK), 112) AS XlalinesAccountingDate,
            ISNULL(D.AE_LINE_NUM, -1) AS XlalinesAeLineNum,
            D.LINE_DESCRIPTION AS XlalinesDescription,
            ISNULL(D.ACCOUNTED_DR, 0) AS XladistlinkUnroundedAccountedDr,
            ISNULL(D.ACCOUNTED_CR, 0) AS XladistlinkUnroundedAccountedCr,
            D.LAST_UPDATE_DATE AS XladistlinkLastUpdateDate,
            D.LAST_UPDATED_BY AS XladistlinkLastUpdatedBy,
            D.LAST_UPDATE_LOGIN AS XladistlinkLastUpdateLogin,
            -1 AS XlalinesLedgerId,
            -1 AS XlalinesPartySiteId,
            D.BZ_LOAD_DATE AS DistAddDateTime
        INTO #dist_sla
        FROM (
            SELECT D.*, ROW_NUMBER() OVER (PARTITION BY D.SOURCE_DIST_ID_NUM1 ORDER BY D.BZ_LOAD_DATE DESC) AS rn
            FROM svo.F_SL_JOURNAL_DISTRIBUTION D
            WHERE D.BZ_LOAD_DATE > @LastWatermark
              AND D.SOURCE_DISTRIBUTION_TYPE = 'AP_INV_DIST'
        ) D
        WHERE D.rn = 1
          AND NOT EXISTS (SELECT 1 FROM svo.F_AP_INVOICE_LINE_DISTRIBUTION t WHERE t.INVOICE_DISTRIBUTION_ID = D.SOURCE_DIST_ID_NUM1);

        /* --- (3) SLA OPEXP: #dist_sla_opexp from DW fact F_SL_JOURNAL_DISTRIBUTION --- */
        IF OBJECT_ID('tempdb..#dist_sla_opexp') IS NOT NULL DROP TABLE #dist_sla_opexp;
        SELECT
            D.TRANSACTION_SOURCE_ID_INT1 AS InvoiceId,
            D.SOURCE_DIST_ID_NUM1 AS InvoiceDistributionId,
            D.SOURCE_DIST_ID_NUM2 AS InvoiceLineNumber,
            D.TRANSACTION_ENTITY_CODE AS TransactionEntityEntityCode,
            D.SOURCE_DISTRIBUTION_TYPE AS XladistlinkSourceDistributionType,
            ISNULL(D.ENTERED_CR, 0) AS XladistlinkUnroundedEnteredCr,
            ISNULL(D.ENTERED_DR, 0) AS XladistlinkUnroundedEnteredDr,
            D.ACCOUNTING_CLASS_CODE AS XlalinesAccountingClassCode,
            D.CODE_COMBINATION_ID AS XlalinesCodeCombinationId,
            CONVERT(DATE, CONVERT(VARCHAR(8), D.ACCOUNTING_DATE_SK), 112) AS XlalinesAccountingDate,
            ISNULL(D.AE_LINE_NUM, -1) AS XlalinesAeLineNum,
            D.LINE_DESCRIPTION AS XlalinesDescription,
            ISNULL(D.ACCOUNTED_DR, 0) AS XladistlinkUnroundedAccountedDr,
            ISNULL(D.ACCOUNTED_CR, 0) AS XladistlinkUnroundedAccountedCr,
            D.LAST_UPDATE_DATE AS XladistlinkLastUpdateDate,
            D.LAST_UPDATED_BY AS XladistlinkLastUpdatedBy,
            D.LAST_UPDATE_LOGIN AS XladistlinkLastUpdateLogin,
            -1 AS XlalinesLedgerId,
            -1 AS XlalinesPartySiteId,
            D.BZ_LOAD_DATE AS DistAddDateTime
        INTO #dist_sla_opexp
        FROM (
            SELECT D.*, ROW_NUMBER() OVER (PARTITION BY D.SOURCE_DIST_ID_NUM1 ORDER BY D.BZ_LOAD_DATE DESC) AS rn
            FROM svo.F_SL_JOURNAL_DISTRIBUTION D
            WHERE D.BZ_LOAD_DATE > @LastWatermark
              AND D.APPLICATION_ID = 200
              AND D.TRANSACTION_ENTITY_CODE = 'AP_INVOICES'
              AND (COALESCE(D.ENTERED_DR, 0) <> 0 OR COALESCE(D.ENTERED_CR, 0) <> 0)
              AND CAST(COALESCE(D.ACCOUNTED_DR, 0) - COALESCE(D.ACCOUNTED_CR, 0) AS DECIMAL(29,4)) <> 0
              AND EXISTS (SELECT 1 FROM svo.D_ACCOUNT DA WHERE DA.ACCOUNT_SK = D.ACCOUNT_SK AND DA.CURR_IND = 'Y' AND DA.ACCOUNT_LVL5_CODE = 'OPEXP')
              AND (EXISTS (SELECT 1 FROM svo.F_AP_INVOICE_LINE_DISTRIBUTION t WHERE t.INVOICE_ID = D.TRANSACTION_SOURCE_ID_INT1)
                   OR EXISTS (SELECT 1 FROM #dist d WHERE d.ApInvoiceDistributionsInvoiceId = D.TRANSACTION_SOURCE_ID_INT1))
              AND NOT EXISTS (SELECT 1 FROM svo.F_AP_INVOICE_LINE_DISTRIBUTION t WHERE t.INVOICE_DISTRIBUTION_ID = D.SOURCE_DIST_ID_NUM1)
              AND NOT EXISTS (SELECT 1 FROM #dist d WHERE d.ApInvoiceDistributionsInvoiceDistributionId = D.SOURCE_DIST_ID_NUM1)
              AND NOT EXISTS (SELECT 1 FROM #dist_sla s WHERE s.InvoiceDistributionId = D.SOURCE_DIST_ID_NUM1)
        ) D
        WHERE D.rn = 1;

        /* Watermark from all three sources */
        SELECT @MaxWatermark = (SELECT MAX(d) FROM (
            SELECT MAX(DistAddDateTime) AS d FROM #dist
            UNION ALL
            SELECT MAX(DistAddDateTime) FROM #dist_sla
            UNION ALL
            SELECT MAX(DistAddDateTime) FROM #dist_sla_opexp
        ) x);

        /* --- INSERT (1): AP rows --- */
        INSERT INTO svo.F_AP_INVOICE_LINE_DISTRIBUTION (
            AP_INVOICE_HEADER_SK, INVOICE_ID, INVOICE_LINE_NUMBER, INVOICE_DISTRIBUTION_ID, DIST_INV_LINE_NUMBER, DISTRIBUTION_LINE_NUMBER,
            DIST_ACCOUNTING_DATE_SK, LINE_ACCOUNTING_DATE_SK,
            ACCOUNT_SK, BUSINESS_OFFERING_SK, COMPANY_SK, COST_CENTER_SK, INDUSTRY_SK, INTERCOMPANY_SK, LEGAL_ENTITY_SK, BUSINESS_UNIT_SK, VENDOR_SITE_SK, LEDGER_SK,
            DISTRIBUTION_CLASS, DIST_DESCRIPTION, LINE_DESCRIPTION, DISTRIBUTION_AMOUNT, POSTED_FLAG, TYPE_1099,
            INV_TAX_JURIDISTION_CODE, INV_TAX_RATE, LINE_TYPE_LOOKUP_CODE,
            PO_DISTRIBUTION_ID, DIST_LAST_UPDATE_DATE, DIST_LAST_UPDATE_BY, DIST_LAST_UPDATE_LOGIN, DIST_CODE_COMBINATION_ID,
            BZ_LOAD_DATE, SV_LOAD_DATE,
            LINES_AMOUNT_debug_only,
            SLA_ENTITY_CODE, SLA_DESC, SLA_DIST_TYPE, SLA_INVOICE_ID, SLA_DIST_ID, ENTERED_CR,
            ORIG_AP_INVOICE,
            SLA_ACCTG_CLASS_CODE
        )
        SELECT
            ISNULL(H.AP_INVOICE_HEADER_SK, 0),
            ISNULL(I.ApInvoiceLinesAllInvoiceId, -1),
            ISNULL(I.ApInvoiceLinesAllLineNumber, -1),
            ISNULL(D.ApInvoiceDistributionsInvoiceDistributionId, -1),
            ISNULL(D.ApInvoiceDistributionsInvoiceLineNumber, -1),
            ISNULL(D.ApInvoiceDistributionsDistributionLineNumber, -1),
            ISNULL(CONVERT(INT, FORMAT(D.ApInvoiceDistributionsAccountingDate, 'yyyyMMdd')), 19000101),
            ISNULL(CONVERT(INT, FORMAT(I.ApInvoiceLinesAllAccountingDate, 'yyyyMMdd')), 19000101),
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
            ISNULL(D.ApInvoiceDistributionsPoDistributionId, 0),
            ISNULL(CONVERT(DATE, D.ApInvoiceDistributionsLastUpdateDate), '0001-01-01'),
            ISNULL(D.ApInvoiceDistributionsLastUpdatedBy, 'UNK'),
            ISNULL(D.ApInvoiceDistributionsLastUpdateLogin, 'UNK'),
            ISNULL(D.ApInvoiceDistributionsDistCodeCombinationId, -1),
            ISNULL(D.DistAddDateTime, SYSDATETIME()),
            SYSDATETIME(),
            CAST(COALESCE(I.ApInvoiceLinesAllAmount, D.ApInvoiceDistributionsAmount, 0) AS DECIMAL(29,4)),
            NULL, NULL, NULL, NULL, NULL, NULL,
            1,
            NULL
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

        /* --- INSERT (2): SLA AP_INV_DIST rows --- */
        INSERT INTO svo.F_AP_INVOICE_LINE_DISTRIBUTION (
            AP_INVOICE_HEADER_SK, INVOICE_ID, INVOICE_LINE_NUMBER, INVOICE_DISTRIBUTION_ID, DIST_INV_LINE_NUMBER, DISTRIBUTION_LINE_NUMBER,
            DIST_ACCOUNTING_DATE_SK, LINE_ACCOUNTING_DATE_SK,
            ACCOUNT_SK, BUSINESS_OFFERING_SK, COMPANY_SK, COST_CENTER_SK, INDUSTRY_SK, INTERCOMPANY_SK, LEGAL_ENTITY_SK, BUSINESS_UNIT_SK, VENDOR_SITE_SK, LEDGER_SK,
            DISTRIBUTION_CLASS, DIST_DESCRIPTION, LINE_DESCRIPTION, DISTRIBUTION_AMOUNT, POSTED_FLAG, TYPE_1099,
            INV_TAX_JURIDISTION_CODE, INV_TAX_RATE, LINE_TYPE_LOOKUP_CODE,
            PO_DISTRIBUTION_ID, DIST_LAST_UPDATE_DATE, DIST_LAST_UPDATE_BY, DIST_LAST_UPDATE_LOGIN, DIST_CODE_COMBINATION_ID,
            BZ_LOAD_DATE, SV_LOAD_DATE,
            LINES_AMOUNT_debug_only,
            SLA_ENTITY_CODE, SLA_DESC, SLA_DIST_TYPE, SLA_INVOICE_ID, SLA_DIST_ID, ENTERED_CR,
            ORIG_AP_INVOICE,
            SLA_ACCTG_CLASS_CODE
        )
        SELECT
            ISNULL(H.AP_INVOICE_HEADER_SK, 0),
            ISNULL(D.InvoiceId, -1),
            ISNULL(D.InvoiceLineNumber, -1),
            ISNULL(D.InvoiceDistributionId, -1),
            ISNULL(D.InvoiceLineNumber, -1),
            ISNULL(D.XlalinesAeLineNum, -1),
            ISNULL(CONVERT(INT, FORMAT(D.XlalinesAccountingDate, 'yyyyMMdd')), 19000101),
            ISNULL(CONVERT(INT, FORMAT(D.XlalinesAccountingDate, 'yyyyMMdd')), 19000101),
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
            'U',
            ISNULL(D.XlalinesDescription, 'UNK'),
            ISNULL(D.XlalinesDescription, 'UNK'),
            CAST(COALESCE(D.XladistlinkUnroundedAccountedDr, 0) - COALESCE(D.XladistlinkUnroundedAccountedCr, 0) AS DECIMAL(29,4)),
            'U',
            0,
            'SUBLEDGER ACCTG',
            0,
            NULL,
            0,
            ISNULL(CONVERT(DATE, D.XladistlinkLastUpdateDate), '0001-01-01'),
            ISNULL(D.XladistlinkLastUpdatedBy, 'UNK'),
            ISNULL(D.XladistlinkLastUpdateLogin, 'UNK'),
            ISNULL(D.XlalinesCodeCombinationId, -1),
            ISNULL(D.DistAddDateTime, SYSDATETIME()),
            SYSDATETIME(),
            CAST(COALESCE(D.XladistlinkUnroundedAccountedDr, 0) - COALESCE(D.XladistlinkUnroundedAccountedCr, 0) AS DECIMAL(29,4)),
            D.TransactionEntityEntityCode,
            D.XlalinesDescription,
            D.XladistlinkSourceDistributionType,
            D.InvoiceId,
            D.InvoiceDistributionId,
            D.XladistlinkUnroundedEnteredCr,
            0,
            D.XlalinesAccountingClassCode
        FROM #dist_sla D
        LEFT JOIN bzo.AP_InvoiceHeaderExtractPVO IH ON IH.ApInvoicesInvoiceId = D.InvoiceId
        LEFT JOIN svo.LINES_CODE_COMBO_LOOKUP C ON CAST(D.XlalinesCodeCombinationId AS BIGINT) = C.CODE_COMBINATION_BK
        LEFT JOIN svo.D_AP_INVOICE_HEADER H ON H.INVOICE_ID = IH.ApInvoicesInvoiceId
        LEFT JOIN svo.D_ACCOUNT DA ON DA.ACCOUNT_ID = C.ACCOUNT_ID AND DA.CURR_IND = 'Y'
        LEFT JOIN svo.D_BUSINESS_OFFERING DBO ON DBO.BUSINESS_OFFERING_ID = C.BUSINESSOFFERING_ID AND DBO.CURR_IND = 'Y'
        LEFT JOIN svo.D_COMPANY DCO ON DCO.COMPANY_ID = C.COMPANY_ID AND DCO.CURR_IND = 'Y'
        LEFT JOIN svo.D_COST_CENTER DCC ON DCC.COST_CENTER_ID = C.COSTCENTER_ID AND DCC.CURR_IND = 'Y'
        LEFT JOIN svo.D_INDUSTRY DI ON DI.INDUSTRY_ID = C.INDUSTRY_ID AND DI.CURR_IND = 'Y'
        LEFT JOIN svo.D_INTERCOMPANY DIC ON DIC.INTERCOMPANY_ID = C.INTERCOMPANY_ID AND DIC.CURR_IND = 'Y'
        LEFT JOIN svo.D_LEGAL_ENTITY LE ON LE.LEGAL_ENTITY_ID = IH.ApInvoicesLegalEntityId AND LE.CURR_IND = 'Y'
        LEFT JOIN svo.D_BUSINESS_UNIT BU ON BU.BUSINESS_UNIT_ID = IH.ApInvoicesOrgId AND BU.CURR_IND = 'Y'
        LEFT JOIN svo.D_VENDOR_SITE VS ON VS.VENDOR_SITE_ID = COALESCE(IH.ApInvoicesVendorSiteId, D.XlalinesPartySiteId) AND VS.CURR_IND = 'Y'
        LEFT JOIN svo.D_LEDGER LDG ON LDG.LEDGER_ID = D.XlalinesLedgerId AND LDG.CURR_IND = 'Y';

        SET @RowInserted = @RowInserted + @@ROWCOUNT;

        /* --- INSERT (3): SLA OPEXP rows --- */
        INSERT INTO svo.F_AP_INVOICE_LINE_DISTRIBUTION (
            AP_INVOICE_HEADER_SK, INVOICE_ID, INVOICE_LINE_NUMBER, INVOICE_DISTRIBUTION_ID, DIST_INV_LINE_NUMBER, DISTRIBUTION_LINE_NUMBER,
            DIST_ACCOUNTING_DATE_SK, LINE_ACCOUNTING_DATE_SK,
            ACCOUNT_SK, BUSINESS_OFFERING_SK, COMPANY_SK, COST_CENTER_SK, INDUSTRY_SK, INTERCOMPANY_SK, LEGAL_ENTITY_SK, BUSINESS_UNIT_SK, VENDOR_SITE_SK, LEDGER_SK,
            DISTRIBUTION_CLASS, DIST_DESCRIPTION, LINE_DESCRIPTION, DISTRIBUTION_AMOUNT, POSTED_FLAG, TYPE_1099,
            INV_TAX_JURIDISTION_CODE, INV_TAX_RATE, LINE_TYPE_LOOKUP_CODE,
            PO_DISTRIBUTION_ID, DIST_LAST_UPDATE_DATE, DIST_LAST_UPDATE_BY, DIST_LAST_UPDATE_LOGIN, DIST_CODE_COMBINATION_ID,
            BZ_LOAD_DATE, SV_LOAD_DATE,
            LINES_AMOUNT_debug_only,
            SLA_ENTITY_CODE, SLA_DESC, SLA_DIST_TYPE, SLA_INVOICE_ID, SLA_DIST_ID, ENTERED_CR,
            ORIG_AP_INVOICE,
            SLA_ACCTG_CLASS_CODE
        )
        SELECT
            ISNULL(H.AP_INVOICE_HEADER_SK, 0),
            ISNULL(D.InvoiceId, -1),
            ISNULL(D.InvoiceLineNumber, -1),
            ISNULL(D.InvoiceDistributionId, -1),
            ISNULL(D.InvoiceLineNumber, -1),
            ISNULL(D.XlalinesAeLineNum, -1),
            ISNULL(CONVERT(INT, FORMAT(D.XlalinesAccountingDate, 'yyyyMMdd')), 19000101),
            ISNULL(CONVERT(INT, FORMAT(D.XlalinesAccountingDate, 'yyyyMMdd')), 19000101),
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
            'U',
            ISNULL(D.XlalinesDescription, 'UNK'),
            ISNULL(D.XlalinesDescription, 'UNK'),
            CAST(COALESCE(D.XladistlinkUnroundedAccountedDr, 0) - COALESCE(D.XladistlinkUnroundedAccountedCr, 0) AS DECIMAL(29,4)),
            'U',
            0,
            'SUBLEDGER ACCTG',
            0,
            NULL,
            0,
            ISNULL(CONVERT(DATE, D.XladistlinkLastUpdateDate), '0001-01-01'),
            ISNULL(D.XladistlinkLastUpdatedBy, 'UNK'),
            ISNULL(D.XladistlinkLastUpdateLogin, 'UNK'),
            ISNULL(D.XlalinesCodeCombinationId, -1),
            ISNULL(D.DistAddDateTime, SYSDATETIME()),
            SYSDATETIME(),
            CAST(COALESCE(D.XladistlinkUnroundedAccountedDr, 0) - COALESCE(D.XladistlinkUnroundedAccountedCr, 0) AS DECIMAL(29,4)),
            D.TransactionEntityEntityCode,
            D.XlalinesDescription,
            D.XladistlinkSourceDistributionType,
            D.InvoiceId,
            D.InvoiceDistributionId,
            D.XladistlinkUnroundedEnteredCr,
            0,
            D.XlalinesAccountingClassCode
        FROM #dist_sla_opexp D
        LEFT JOIN bzo.AP_InvoiceHeaderExtractPVO IH ON IH.ApInvoicesInvoiceId = D.InvoiceId
        LEFT JOIN svo.LINES_CODE_COMBO_LOOKUP C ON CAST(D.XlalinesCodeCombinationId AS BIGINT) = C.CODE_COMBINATION_BK
        LEFT JOIN svo.D_AP_INVOICE_HEADER H ON H.INVOICE_ID = IH.ApInvoicesInvoiceId
        LEFT JOIN svo.D_ACCOUNT DA ON DA.ACCOUNT_ID = C.ACCOUNT_ID AND DA.CURR_IND = 'Y'
        LEFT JOIN svo.D_BUSINESS_OFFERING DBO ON DBO.BUSINESS_OFFERING_ID = C.BUSINESSOFFERING_ID AND DBO.CURR_IND = 'Y'
        LEFT JOIN svo.D_COMPANY DCO ON DCO.COMPANY_ID = C.COMPANY_ID AND DCO.CURR_IND = 'Y'
        LEFT JOIN svo.D_COST_CENTER DCC ON DCC.COST_CENTER_ID = C.COSTCENTER_ID AND DCC.CURR_IND = 'Y'
        LEFT JOIN svo.D_INDUSTRY DI ON DI.INDUSTRY_ID = C.INDUSTRY_ID AND DI.CURR_IND = 'Y'
        LEFT JOIN svo.D_INTERCOMPANY DIC ON DIC.INTERCOMPANY_ID = C.INTERCOMPANY_ID AND DIC.CURR_IND = 'Y'
        LEFT JOIN svo.D_LEGAL_ENTITY LE ON LE.LEGAL_ENTITY_ID = IH.ApInvoicesLegalEntityId AND LE.CURR_IND = 'Y'
        LEFT JOIN svo.D_BUSINESS_UNIT BU ON BU.BUSINESS_UNIT_ID = IH.ApInvoicesOrgId AND BU.CURR_IND = 'Y'
        LEFT JOIN svo.D_VENDOR_SITE VS ON VS.VENDOR_SITE_ID = COALESCE(IH.ApInvoicesVendorSiteId, D.XlalinesPartySiteId) AND VS.CURR_IND = 'Y'
        LEFT JOIN svo.D_LEDGER LDG ON LDG.LEDGER_ID = D.XlalinesLedgerId AND LDG.CURR_IND = 'Y'
        WHERE NOT EXISTS (SELECT 1 FROM svo.F_AP_INVOICE_LINE_DISTRIBUTION t WHERE t.INVOICE_DISTRIBUTION_ID = D.InvoiceDistributionId);

        SET @RowInserted = @RowInserted + @@ROWCOUNT;

        IF @MaxWatermark IS NOT NULL
            UPDATE etl.ETL_WATERMARK SET LAST_WATERMARK = @MaxWatermark, UDT_DATE = SYSDATETIME() WHERE TABLE_NAME = @TargetObject;

        IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'IX_F_AP_INV_LINE_COMPANY_SK' AND object_id = OBJECT_ID('svo.F_AP_INVOICE_LINE_DISTRIBUTION'))
            CREATE NONCLUSTERED INDEX IX_F_AP_INV_LINE_COMPANY_SK ON svo.F_AP_INVOICE_LINE_DISTRIBUTION(COMPANY_SK)
            INCLUDE (AP_INVOICE_HEADER_SK, INVOICE_ID, INVOICE_LINE_NUMBER, INVOICE_DISTRIBUTION_ID, DIST_INV_LINE_NUMBER, DISTRIBUTION_LINE_NUMBER, DIST_ACCOUNTING_DATE_SK, LINE_ACCOUNTING_DATE_SK, ACCOUNT_SK, BUSINESS_OFFERING_SK, COST_CENTER_SK, INDUSTRY_SK, INTERCOMPANY_SK, LEGAL_ENTITY_SK, BUSINESS_UNIT_SK, VENDOR_SITE_SK, LEDGER_SK, DISTRIBUTION_CLASS, DIST_DESCRIPTION, LINE_DESCRIPTION, DISTRIBUTION_AMOUNT, POSTED_FLAG, TYPE_1099, INV_TAX_JURIDISTION_CODE, INV_TAX_RATE, LINE_TYPE_LOOKUP_CODE, PO_DISTRIBUTION_ID, SLA_ENTITY_CODE, SLA_DESC, SLA_DIST_TYPE, SLA_INVOICE_ID, SLA_DIST_ID, ENTERED_CR, ORIG_AP_INVOICE, SLA_ACCTG_CLASS_CODE) ON FG_SilverFact;
        IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'IX_F_AP_INV_LINE_ACCOUNT_SK' AND object_id = OBJECT_ID('svo.F_AP_INVOICE_LINE_DISTRIBUTION'))
            CREATE NONCLUSTERED INDEX IX_F_AP_INV_LINE_ACCOUNT_SK ON svo.F_AP_INVOICE_LINE_DISTRIBUTION(ACCOUNT_SK)
            INCLUDE (AP_INVOICE_HEADER_SK, INVOICE_ID, INVOICE_LINE_NUMBER, INVOICE_DISTRIBUTION_ID, DIST_INV_LINE_NUMBER, DISTRIBUTION_LINE_NUMBER, DIST_ACCOUNTING_DATE_SK, LINE_ACCOUNTING_DATE_SK, BUSINESS_OFFERING_SK, COMPANY_SK, COST_CENTER_SK, INDUSTRY_SK, INTERCOMPANY_SK, LEGAL_ENTITY_SK, BUSINESS_UNIT_SK, VENDOR_SITE_SK, LEDGER_SK, DISTRIBUTION_CLASS, DIST_DESCRIPTION, LINE_DESCRIPTION, DISTRIBUTION_AMOUNT, POSTED_FLAG, TYPE_1099, INV_TAX_JURIDISTION_CODE, INV_TAX_RATE, LINE_TYPE_LOOKUP_CODE, PO_DISTRIBUTION_ID, SLA_ENTITY_CODE, SLA_DESC, SLA_DIST_TYPE, SLA_INVOICE_ID, SLA_DIST_ID, ENTERED_CR, ORIG_AP_INVOICE, SLA_ACCTG_CLASS_CODE) ON FG_SilverFact;

        SET @EndDttm = SYSDATETIME();
        UPDATE etl.ETL_RUN SET END_DTTM = @EndDttm, STATUS = 'SUCCESS', ROW_INSERTED = @RowInserted, ROW_UPDATED = 0, ROW_EXPIRED = 0, ERROR_MESSAGE = NULL WHERE RUN_ID = @RunId;
    END TRY
    BEGIN CATCH
        SET @EndDttm = SYSDATETIME();
        SET @ErrMsg = CONCAT('Error ', ERROR_NUMBER(), ' (Line ', ERROR_LINE(), '): ', ERROR_MESSAGE());
        IF @RunId IS NOT NULL
            UPDATE etl.ETL_RUN SET END_DTTM = @EndDttm, STATUS = 'FAILURE', ROW_INSERTED = @RowInserted, ROW_UPDATED = 0, ROW_EXPIRED = 0, ERROR_MESSAGE = @ErrMsg WHERE RUN_ID = @RunId;
        IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'IX_F_AP_INV_LINE_COMPANY_SK' AND object_id = OBJECT_ID('svo.F_AP_INVOICE_LINE_DISTRIBUTION'))
            CREATE NONCLUSTERED INDEX IX_F_AP_INV_LINE_COMPANY_SK ON svo.F_AP_INVOICE_LINE_DISTRIBUTION(COMPANY_SK)
            INCLUDE (AP_INVOICE_HEADER_SK, INVOICE_ID, INVOICE_LINE_NUMBER, INVOICE_DISTRIBUTION_ID, DIST_INV_LINE_NUMBER, DISTRIBUTION_LINE_NUMBER, DIST_ACCOUNTING_DATE_SK, LINE_ACCOUNTING_DATE_SK, ACCOUNT_SK, BUSINESS_OFFERING_SK, COST_CENTER_SK, INDUSTRY_SK, INTERCOMPANY_SK, LEGAL_ENTITY_SK, BUSINESS_UNIT_SK, VENDOR_SITE_SK, LEDGER_SK, DISTRIBUTION_CLASS, DIST_DESCRIPTION, LINE_DESCRIPTION, DISTRIBUTION_AMOUNT, POSTED_FLAG, TYPE_1099, INV_TAX_JURIDISTION_CODE, INV_TAX_RATE, LINE_TYPE_LOOKUP_CODE, PO_DISTRIBUTION_ID, SLA_ENTITY_CODE, SLA_DESC, SLA_DIST_TYPE, SLA_INVOICE_ID, SLA_DIST_ID, ENTERED_CR, ORIG_AP_INVOICE, SLA_ACCTG_CLASS_CODE) ON FG_SilverFact;
        IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'IX_F_AP_INV_LINE_ACCOUNT_SK' AND object_id = OBJECT_ID('svo.F_AP_INVOICE_LINE_DISTRIBUTION'))
            CREATE NONCLUSTERED INDEX IX_F_AP_INV_LINE_ACCOUNT_SK ON svo.F_AP_INVOICE_LINE_DISTRIBUTION(ACCOUNT_SK)
            INCLUDE (AP_INVOICE_HEADER_SK, INVOICE_ID, INVOICE_LINE_NUMBER, INVOICE_DISTRIBUTION_ID, DIST_INV_LINE_NUMBER, DISTRIBUTION_LINE_NUMBER, DIST_ACCOUNTING_DATE_SK, LINE_ACCOUNTING_DATE_SK, BUSINESS_OFFERING_SK, COMPANY_SK, COST_CENTER_SK, INDUSTRY_SK, INTERCOMPANY_SK, LEGAL_ENTITY_SK, BUSINESS_UNIT_SK, VENDOR_SITE_SK, LEDGER_SK, DISTRIBUTION_CLASS, DIST_DESCRIPTION, LINE_DESCRIPTION, DISTRIBUTION_AMOUNT, POSTED_FLAG, TYPE_1099, INV_TAX_JURIDISTION_CODE, INV_TAX_RATE, LINE_TYPE_LOOKUP_CODE, PO_DISTRIBUTION_ID, SLA_ENTITY_CODE, SLA_DESC, SLA_DIST_TYPE, SLA_INVOICE_ID, SLA_DIST_ID, ENTERED_CR, ORIG_AP_INVOICE, SLA_ACCTG_CLASS_CODE) ON FG_SilverFact;
        ;THROW;
    END CATCH
END;
GO
