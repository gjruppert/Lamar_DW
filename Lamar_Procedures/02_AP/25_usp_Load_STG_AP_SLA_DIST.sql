/* =========================================================
   usp_Load_STG_AP_SLA_DIST
   Populates svo.STG_AP_SLA_DIST from svo.F_SL_JOURNAL_DISTRIBUTION.
   Uses dynamic SQL to avoid procedure compile-time column resolution issue.
   Prerequisite: usp_Load_F_SL_JOURNAL_DISTRIBUTION must run before AP load.
   NOTE: usp_Load_F_AP_INVOICE_LINE_DISTRIBUTION now inlines this logic
   (avoids cross-proc temp-table scope). Use this proc for standalone testing.
   ========================================================= */
CREATE OR ALTER PROCEDURE svo.usp_Load_STG_AP_SLA_DIST
    @LastWatermark DATETIME2(7)
AS
BEGIN
    SET NOCOUNT ON;

    TRUNCATE TABLE svo.STG_AP_SLA_DIST;

    EXEC sp_executesql N'
    INSERT INTO svo.STG_AP_SLA_DIST (
        InvoiceId, InvoiceDistributionId, InvoiceLineNumber, TransactionEntityEntityCode,
        XladistlinkSourceDistributionType, XladistlinkUnroundedEnteredCr, XladistlinkUnroundedEnteredDr,
        XlalinesAccountingClassCode, XlalinesCodeCombinationId, XlalinesAccountingDate, XlalinesAeLineNum,
        XlalinesDescription, XladistlinkUnroundedAccountedDr, XladistlinkUnroundedAccountedCr,
        XladistlinkLastUpdateDate, XladistlinkLastUpdatedBy, XladistlinkLastUpdateLogin,
        XlalinesLedgerId, XlalinesPartySiteId, DistAddDateTime, SLA_Source
    )
    SELECT
        D.TRANSACTION_SOURCE_ID_INT1, D.SOURCE_DIST_ID_NUM1, D.SOURCE_DIST_ID_NUM2,
        D.TRANSACTION_ENTITY_CODE, D.SOURCE_DISTRIBUTION_TYPE,
        ISNULL(D.ENTERED_CR, 0), NULL,
        D.ACCOUNTING_CLASS_CODE, D.CODE_COMBINATION_ID,
        CONVERT(DATE, CONVERT(VARCHAR(8), D.ACCOUNTING_DATE_SK), 112),
        ISNULL(D.AE_LINE_NUM, -1), D.LINE_DESCRIPTION,
        ISNULL(D.ACCOUNTED_DR, 0), ISNULL(D.ACCOUNTED_CR, 0),
        D.LAST_UPDATE_DATE, D.LAST_UPDATED_BY, D.LAST_UPDATE_LOGIN,
        -1, -1, D.BZ_LOAD_DATE, ''AP_INV_DIST''
    FROM (
        SELECT D.*, ROW_NUMBER() OVER (PARTITION BY D.SOURCE_DIST_ID_NUM1 ORDER BY D.BZ_LOAD_DATE DESC) AS rn
        FROM svo.F_SL_JOURNAL_DISTRIBUTION D
        WHERE D.BZ_LOAD_DATE > @LastWatermark
          AND D.SOURCE_DISTRIBUTION_TYPE = ''AP_INV_DIST''
    ) D
    WHERE D.rn = 1
      AND NOT EXISTS (SELECT 1 FROM svo.F_AP_INVOICE_LINE_DISTRIBUTION t WHERE t.INVOICE_DISTRIBUTION_ID = D.SOURCE_DIST_ID_NUM1);
    ', N'@LastWatermark DATETIME2(7)', @LastWatermark = @LastWatermark;

    EXEC sp_executesql N'
    INSERT INTO svo.STG_AP_SLA_DIST (
        InvoiceId, InvoiceDistributionId, InvoiceLineNumber, TransactionEntityEntityCode,
        XladistlinkSourceDistributionType, XladistlinkUnroundedEnteredCr, XladistlinkUnroundedEnteredDr,
        XlalinesAccountingClassCode, XlalinesCodeCombinationId, XlalinesAccountingDate, XlalinesAeLineNum,
        XlalinesDescription, XladistlinkUnroundedAccountedDr, XladistlinkUnroundedAccountedCr,
        XladistlinkLastUpdateDate, XladistlinkLastUpdatedBy, XladistlinkLastUpdateLogin,
        XlalinesLedgerId, XlalinesPartySiteId, DistAddDateTime, SLA_Source
    )
    SELECT
        D.TRANSACTION_SOURCE_ID_INT1, D.SOURCE_DIST_ID_NUM1, D.SOURCE_DIST_ID_NUM2,
        D.TRANSACTION_ENTITY_CODE, D.SOURCE_DISTRIBUTION_TYPE,
        ISNULL(D.ENTERED_CR, 0), ISNULL(D.ENTERED_DR, 0),
        D.ACCOUNTING_CLASS_CODE, D.CODE_COMBINATION_ID,
        CONVERT(DATE, CONVERT(VARCHAR(8), D.ACCOUNTING_DATE_SK), 112),
        ISNULL(D.AE_LINE_NUM, -1), D.LINE_DESCRIPTION,
        ISNULL(D.ACCOUNTED_DR, 0), ISNULL(D.ACCOUNTED_CR, 0),
        D.LAST_UPDATE_DATE, D.LAST_UPDATED_BY, D.LAST_UPDATE_LOGIN,
        -1, -1, D.BZ_LOAD_DATE, ''OPEXP''
    FROM (
        SELECT D.*, ROW_NUMBER() OVER (PARTITION BY D.SOURCE_DIST_ID_NUM1 ORDER BY D.BZ_LOAD_DATE DESC) AS rn
        FROM svo.F_SL_JOURNAL_DISTRIBUTION D
        WHERE D.BZ_LOAD_DATE > @LastWatermark
          AND D.APPLICATION_ID = 200
          AND D.TRANSACTION_ENTITY_CODE = ''AP_INVOICES''
          AND (COALESCE(D.ENTERED_DR, 0) <> 0 OR COALESCE(D.ENTERED_CR, 0) <> 0)
          AND CAST(COALESCE(D.ACCOUNTED_DR, 0) - COALESCE(D.ACCOUNTED_CR, 0) AS DECIMAL(29,4)) <> 0
          AND EXISTS (SELECT 1 FROM svo.D_ACCOUNT DA WHERE DA.ACCOUNT_SK = D.ACCOUNT_SK AND DA.CURR_IND = ''Y'' AND DA.ACCOUNT_LVL5_CODE = ''OPEXP'')
          AND (EXISTS (SELECT 1 FROM svo.F_AP_INVOICE_LINE_DISTRIBUTION t WHERE t.INVOICE_ID = D.TRANSACTION_SOURCE_ID_INT1)
               OR EXISTS (SELECT 1 FROM svo.STG_AP_DIST_IDS d WHERE d.InvoiceId = D.TRANSACTION_SOURCE_ID_INT1))
          AND NOT EXISTS (SELECT 1 FROM svo.F_AP_INVOICE_LINE_DISTRIBUTION t WHERE t.INVOICE_DISTRIBUTION_ID = D.SOURCE_DIST_ID_NUM1)
          AND NOT EXISTS (SELECT 1 FROM svo.STG_AP_DIST_IDS d WHERE d.InvoiceDistributionId = D.SOURCE_DIST_ID_NUM1)
          AND NOT EXISTS (SELECT 1 FROM svo.STG_AP_SLA_DIST s WHERE s.SLA_Source = ''AP_INV_DIST'' AND s.InvoiceDistributionId = D.SOURCE_DIST_ID_NUM1)
    ) D
    WHERE D.rn = 1;
    ', N'@LastWatermark DATETIME2(7)', @LastWatermark = @LastWatermark;
END;
GO
