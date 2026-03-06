
GO
CREATE OR ALTER PROCEDURE svo.usp_Load_F_SL_JOURNAL_DISTRIBUTION
    @BatchId INT = NULL
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;
    /* Incremental INSERT only. Source: bzo.SLA_SubledgerJournalDistributionPVO. Dedupe by (RefAeHeaderId, TempLineNum, AeHeaderId). */

    DECLARE
        @ProcName       SYSNAME        = OBJECT_SCHEMA_NAME(@@PROCID) + '.' + OBJECT_NAME(@@PROCID),
        @TargetObject   SYSNAME        = 'svo.F_SL_JOURNAL_DISTRIBUTION',
        @StartDttm      DATETIME2(0)   = SYSDATETIME(),
        @EndDttm        DATETIME2(0),
        @RunId          BIGINT         = NULL,
        @ErrMsg         NVARCHAR(4000) = NULL,
        @AsOfDate       DATE           = CAST(GETDATE() AS DATE),
        @LastWatermark  DATETIME2(7),
        @MaxWatermark   DATETIME2(7)   = NULL,
        @RowInserted    INT            = 0,
        @TableBridgeID  INT            = NULL;

    SELECT @TableBridgeID = TableBridgeID FROM meta.MedallionTableBridge WHERE targettable = N'SLA_SubledgerJournalDistributionPVO';

    BEGIN TRY
        SELECT @LastWatermark = w.LAST_WATERMARK
        FROM etl.ETL_WATERMARK w
        WHERE w.TABLE_NAME = @TargetObject;

        IF @LastWatermark IS NULL
            SET @LastWatermark = '1900-01-01';

        INSERT INTO etl.ETL_RUN (PROC_NAME, TARGET_OBJECT, ASOF_DATE, START_DTTM, STATUS, BATCH_ID, TABLE_BRIDGE_ID)
        VALUES (@ProcName, @TargetObject, @AsOfDate, @StartDttm, 'STARTED', @BatchId, @TableBridgeID);

        SET @RunId = SCOPE_IDENTITY();
        IF @RunId IS NULL
            THROW 50001, 'ETL_RUN insert failed: RUN_ID is NULL.', 1;

        IF OBJECT_ID('tempdb..#dist') IS NOT NULL DROP TABLE #dist;
        SELECT
            D.AeHeaderId,
            D.RefAeHeaderId,
            D.TempLineNum,
            D.TransactionEntityEntityCode,
            D.TransactionEntitySourceIdInt1,
            D.TransactionEntityTransactionNumber,
            D.XladistlinkEventClassCode,
            D.XladistlinkEventId,
            D.XladistlinkEventTypeCode,
            D.XladistlinkSourceDistributionType,
            D.XladistlinkSourceDistributionIdNum1,
            D.XladistlinkSourceDistributionIdNum2,
            D.XlalinesAccountingClassCode,
            D.XlalinesAccountingDate,
            D.XlalinesAeLineNum,
            D.XlalinesApplicationId,
            D.XlalinesCodeCombinationId,
            D.XlalinesCurrencyCode,
            D.XlalinesDescription,
            D.XladistlinkUnroundedAccountedCr,
            D.XladistlinkUnroundedAccountedDr,
            D.XladistlinkUnroundedEnteredCr,
            D.XladistlinkUnroundedEnteredDr,
            D.XladistlinkCreatedBy,
            D.XladistlinkCreationDate,
            D.XladistlinkLastUpdateDate,
            D.XladistlinkLastUpdatedBy,
            D.XladistlinkLastUpdateLogin,
            D.XlalinesLedgerId,
            D.XlalinesPartySiteId,
            D.AddDateTime AS DistAddDateTime
        INTO #dist
        FROM (
            SELECT *,
                ROW_NUMBER() OVER (PARTITION BY D.RefAeHeaderId, D.TempLineNum, D.AeHeaderId ORDER BY D.AddDateTime DESC) AS rn
            FROM bzo.SLA_SubledgerJournalDistributionPVO D
            WHERE D.AddDateTime > @LastWatermark
        ) D
        WHERE D.rn = 1;

        SELECT @MaxWatermark = MAX(DistAddDateTime) FROM #dist;

        INSERT INTO svo.F_SL_JOURNAL_DISTRIBUTION (
            AE_HEADER_ID, REF_AE_HEADER_ID, TEMP_LINE_NUM,
            ACCOUNTING_DATE_SK,
            ACCOUNT_SK, BUSINESS_OFFERING_SK, COMPANY_SK, COST_CENTER_SK, INDUSTRY_SK, INTERCOMPANY_SK, LEGAL_ENTITY_SK, BUSINESS_UNIT_SK, VENDOR_SITE_SK, LEDGER_SK,
            TRANSACTION_ENTITY_CODE, TRANSACTION_SOURCE_ID_INT1, TRANSACTION_NUMBER,
            EVENT_CLASS_CODE, EVENT_ID, EVENT_TYPE_CODE,
            SOURCE_DISTRIBUTION_TYPE, SOURCE_DIST_ID_NUM1, SOURCE_DIST_ID_NUM2,
            ACCOUNTING_CLASS_CODE, AE_LINE_NUM, APPLICATION_ID, CODE_COMBINATION_ID,
            CURRENCY_CODE, LINE_DESCRIPTION,
            ACCOUNTED_CR, ACCOUNTED_DR, ENTERED_CR, ENTERED_DR,
            CREATED_BY, CREATION_DATE, LAST_UPDATE_DATE, LAST_UPDATED_BY, LAST_UPDATE_LOGIN,
            BZ_LOAD_DATE, SV_LOAD_DATE
        )
        SELECT
            ISNULL(D.AeHeaderId, -1),
            ISNULL(D.RefAeHeaderId, -1),
            ISNULL(D.TempLineNum, -1),
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
            ISNULL(D.TransactionEntityEntityCode, 'UNK'),
            D.TransactionEntitySourceIdInt1,
            D.TransactionEntityTransactionNumber,
            D.XladistlinkEventClassCode,
            ISNULL(D.XladistlinkEventId, -1),
            D.XladistlinkEventTypeCode,
            D.XladistlinkSourceDistributionType,
            D.XladistlinkSourceDistributionIdNum1,
            D.XladistlinkSourceDistributionIdNum2,
            ISNULL(D.XlalinesAccountingClassCode, 'UNK'),
            ISNULL(D.XlalinesAeLineNum, -1),
            ISNULL(D.XlalinesApplicationId, -1),
            ISNULL(D.XlalinesCodeCombinationId, -1),
            ISNULL(D.XlalinesCurrencyCode, 'UNK'),
            D.XlalinesDescription,
            ISNULL(D.XladistlinkUnroundedAccountedCr, 0),
            ISNULL(D.XladistlinkUnroundedAccountedDr, 0),
            ISNULL(D.XladistlinkUnroundedEnteredCr, 0),
            ISNULL(D.XladistlinkUnroundedEnteredDr, 0),
            D.XladistlinkCreatedBy,
            D.XladistlinkCreationDate,
            D.XladistlinkLastUpdateDate,
            D.XladistlinkLastUpdatedBy,
            D.XladistlinkLastUpdateLogin,
            ISNULL(D.DistAddDateTime, SYSDATETIME()),
            SYSDATETIME()
        FROM #dist D
        LEFT JOIN bzo.AP_InvoiceHeaderExtractPVO APH
            ON D.TransactionEntitySourceIdInt1 = APH.ApInvoicesInvoiceId
        LEFT JOIN svo.LINES_CODE_COMBO_LOOKUP C
            ON CAST(D.XlalinesCodeCombinationId AS BIGINT) = C.CODE_COMBINATION_BK
        LEFT JOIN svo.D_ACCOUNT DA ON DA.ACCOUNT_ID = C.ACCOUNT_ID AND DA.CURR_IND = 'Y'
        LEFT JOIN svo.D_BUSINESS_OFFERING DBO ON DBO.BUSINESS_OFFERING_ID = C.BUSINESSOFFERING_ID AND DBO.CURR_IND = 'Y'
        LEFT JOIN svo.D_COMPANY DCO ON DCO.COMPANY_ID = C.COMPANY_ID AND DCO.CURR_IND = 'Y'
        LEFT JOIN svo.D_COST_CENTER DCC ON DCC.COST_CENTER_ID = C.COSTCENTER_ID AND DCC.CURR_IND = 'Y'
        LEFT JOIN svo.D_INDUSTRY DI ON DI.INDUSTRY_ID = C.INDUSTRY_ID AND DI.CURR_IND = 'Y'
        LEFT JOIN svo.D_INTERCOMPANY DIC ON DIC.INTERCOMPANY_ID = C.INTERCOMPANY_ID AND DIC.CURR_IND = 'Y'
        LEFT JOIN svo.D_LEGAL_ENTITY LE ON LE.LEGAL_ENTITY_ID = APH.ApInvoicesLegalEntityId AND LE.CURR_IND = 'Y'
        LEFT JOIN svo.D_BUSINESS_UNIT BU ON BU.BUSINESS_UNIT_ID = APH.ApInvoicesOrgId AND BU.CURR_IND = 'Y'
        LEFT JOIN svo.D_VENDOR_SITE VS ON VS.VENDOR_SITE_ID = COALESCE(APH.ApInvoicesVendorSiteId, D.XlalinesPartySiteId) AND VS.CURR_IND = 'Y'
        LEFT JOIN svo.D_LEDGER LDG ON LDG.LEDGER_ID = D.XlalinesLedgerId AND LDG.CURR_IND = 'Y'
        WHERE NOT EXISTS (
            SELECT 1 FROM svo.F_SL_JOURNAL_DISTRIBUTION t
            WHERE t.AE_HEADER_ID = D.AeHeaderId
              AND t.REF_AE_HEADER_ID = D.RefAeHeaderId
              AND t.TEMP_LINE_NUM = D.TempLineNum
        );

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
