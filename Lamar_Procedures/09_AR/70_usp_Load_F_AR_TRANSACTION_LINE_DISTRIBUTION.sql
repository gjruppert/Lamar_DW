/****** Object:  StoredProcedure [svo].[usp_Load_F_AR_TRANSACTION_LINE_DISTRIBUTION]    Script Date: 2/19/2026 4:49:37 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
/* =========================================================
   usp_Load_F_AR_TRANSACTION_LINE_DISTRIBUTION
   Incremental INSERT. Source: bzo.AR_TransactionDistributionExtractPVO (+ Line, Header).
   Filter: TD.AddDateTime > @LastWatermark. Dedupe by AR_TRX_LINE_GL_DIST_ID.
   ========================================================= */
CREATE OR ALTER PROCEDURE [svo].[usp_Load_F_AR_TRANSACTION_LINE_DISTRIBUTION]
    @BatchId INT = NULL
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    DECLARE
        @ProcName       SYSNAME        = OBJECT_SCHEMA_NAME(@@PROCID) + '.' + OBJECT_NAME(@@PROCID),
        @TargetObject   SYSNAME        = 'svo.F_AR_TRANSACTION_LINE_DISTRIBUTION',
        @StartDttm      DATETIME2(0)   = SYSDATETIME(),
        @EndDttm        DATETIME2(0),
        @RunId          BIGINT         = NULL,
        @ErrMsg         NVARCHAR(4000) = NULL,
        @AsOfDate       DATE           = CAST(GETDATE() AS DATE),
        @LastWatermark  DATETIME2(7),
        @MaxWatermark   DATETIME2(7)   = NULL,
        @RowInserted    INT            = 0,
        @TableBridgeID  INT            = NULL;

    SELECT @TableBridgeID = TableBridgeID FROM meta.MedallionTableBridge WHERE targettable = N'AR_TransactionDistributionExtractPVO';

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
        SELECT TD.*, TD.AddDateTime AS DistAddDateTime
        INTO #dist
        FROM bzo.AR_TransactionDistributionExtractPVO TD WITH (NOLOCK)
        WHERE TD.AddDateTime > @LastWatermark;

        CREATE CLUSTERED INDEX IX_dist ON #dist (RaCustTrxLineGlDistCustTrxLineGlDistId, DistAddDateTime DESC);

        IF OBJECT_ID('tempdb..#dist_one') IS NOT NULL DROP TABLE #dist_one;
        SELECT * INTO #dist_one FROM (
            SELECT *,
                ROW_NUMBER() OVER (PARTITION BY RaCustTrxLineGlDistCustTrxLineGlDistId ORDER BY DistAddDateTime DESC) AS rn
            FROM #dist
        ) x WHERE rn = 1;

        DROP TABLE #dist;

        SELECT @MaxWatermark = MAX(DistAddDateTime) FROM #dist_one;

        INSERT INTO svo.F_AR_TRANSACTION_LINE_DISTRIBUTION WITH (TABLOCK)
        (CUSTOMER_SK, CUSTOMER_SITE_USE_SK, BUSINESS_UNIT_SK, LEDGER_SK, ITEM_SK, BUSINESS_OFFERING_SK, COMPANY_SK, COST_CENTER_SK, INDUSTRY_SK, INTERCOMPANY_SK, LEGAL_ENTITY_SK, ACCOUNT_SK, CURRENCY_SK,
         TRX_DATE_SK, BILLING_DATE_SK, TERM_DUE_DATE_SK, GL_DATE_SK, GL_POSTED_DATE_SK, LINE_SALES_ORDER_DATE_SK,
         AR_TRX_LINE_GL_DIST_ID, AR_TRANSACTION_ID, AR_TRANSACTION_LINE_ID, LINE_NUMBER,
         BILL_TO_CUSTOMER_ID, BILL_TO_SITE_USE_ID, TRX_TYPE_SEQ_ID, TERM_ID, INVENTORY_ITEM_ID, ACCOUNT_CLASS,
         TRX_NUMBER, TRX_STATUS, TRX_CLASS, INVOICE_CURRENCY_CODE, LINE_TYPE, LINE_DESCRIPTION,
         GL_DIST_AMOUNT, GL_DIST_ACCTD_AMOUNT, LINE_EXTENDED_AMOUNT, LINE_EXTENDED_ACCTD_AMOUNT, LINE_REVENUE_AMOUNT, LINE_TAXABLE_AMOUNT, LINE_QTY_INVOICED, LINE_UNIT_SELLING_PRICE,
         BZ_LOAD_DATE)
        SELECT
            ISNULL(DCA.CUSTOMER_SK, 0),
            ISNULL(DSU.SITE_USE_SK, 0),
            ISNULL(DBU.BUSINESS_UNIT_SK, 0),
            ISNULL(DL.LEDGER_SK, 0),
            ISNULL(DI.ITEM_SK, 0),
            ISNULL(DBO.BUSINESS_OFFERING_SK, 0),
            ISNULL(DCO.COMPANY_SK, 0),
            ISNULL(DCC.COST_CENTER_SK, 0),
            ISNULL(DIN.INDUSTRY_SK, 0),
            ISNULL(DIC.INTERCOMPANY_SK, 0),
            ISNULL(DLE.LEGAL_ENTITY_SK, 0),
            ISNULL(DA.ACCOUNT_SK, 0),
            CAST(0 AS bigint),
            ISNULL(CONVERT(int, CONVERT(char(8), TH.RaCustomerTrxTrxDate, 112)), 0),
            ISNULL(CONVERT(int, CONVERT(char(8), TH.RaCustomerTrxBillingDate, 112)), 0),
            ISNULL(CONVERT(int, CONVERT(char(8), TH.RaCustomerTrxTermDueDate, 112)), 0),
            ISNULL(CONVERT(int, CONVERT(char(8), TD.RaCustTrxLineGlDistGlDate, 112)), 0),
            ISNULL(CONVERT(int, CONVERT(char(8), TD.RaCustTrxLineGlDistGlPostedDate, 112)), 0),
            ISNULL(CONVERT(int, CONVERT(char(8), TL.RaCustomerTrxLineSalesOrderDate, 112)), 0),
            ISNULL(TD.RaCustTrxLineGlDistCustTrxLineGlDistId, -1),
            ISNULL(TD.RaCustTrxLineGlDistCustomerTrxId, -1),
            ISNULL(TD.RaCustTrxLineGlDistCustomerTrxLineId, -1),
            ISNULL(TL.RaCustomerTrxLineLineNumber, -1),
            ISNULL(TH.RaCustomerTrxBillToCustomerId, -1),
            ISNULL(TH.RaCustomerTrxBillToSiteUseId, -1),
            ISNULL(TH.RaCustomerTrxCustTrxTypeSeqId, -1),
            ISNULL(TH.RaCustomerTrxTermId, -1),
            ISNULL(TL.RaCustomerTrxLineInventoryItemId, -1),
            ISNULL(TD.RaCustTrxLineGlDistAccountClass, 'UNK'),
            ISNULL(TH.RaCustomerTrxTrxNumber, -1),
            ISNULL(TH.RaCustomerTrxStatusTrx, 'U'),
            ISNULL(TH.RaCustomerTrxTrxClass, 'UNK'),
            ISNULL(TH.RaCustomerTrxInvoiceCurrencyCode, 'UNK'),
            ISNULL(TL.RaCustomerTrxLineLineType, 'U'),
            ISNULL(TL.RaCustomerTrxLineDescription, 'UNK'),
            ISNULL(TD.RaCustTrxLineGlDistAmount, 0),
            ISNULL(TD.RaCustTrxLineGlDistAcctdAmount, 0),
            ISNULL(TL.RaCustomerTrxLineExtendedAmount, 0),
            ISNULL(TL.RaCustomerTrxLineExtendedAcctdAmount, 0),
            ISNULL(TL.RaCustomerTrxLineRevenueAmount, 0),
            ISNULL(TL.RaCustomerTrxLineTaxableAmount, 0),
            ISNULL(TL.RaCustomerTrxLineQuantityInvoiced, 0),
            ISNULL(TL.RaCustomerTrxLineRaCustomerTrxLineUnitSellingPrice, 0),
            CAST(TD.AddDateTime AS date)
        FROM #dist_one TD
        LEFT JOIN bzo.AR_TransactionLineExtractPVO    TL WITH (NOLOCK) ON TL.RaCustomerTrxLineCustomerTrxLineId = TD.RaCustTrxLineGlDistCustomerTrxLineId
        LEFT JOIN bzo.AR_TransactionHeaderExtractPVO   TH WITH (NOLOCK) ON TH.RaCustomerTrxCustomerTrxId = TD.RaCustTrxLineGlDistCustomerTrxId
        LEFT JOIN svo.LINES_CODE_COMBO_LOOKUP        C  ON TD.RaCustTrxLineGlDistCodeCombinationId = C.CODE_COMBINATION_BK
        LEFT JOIN svo.D_CUSTOMER_ACCOUNT   DCA ON DCA.CUSTOMER_ACCOUNT_ID = TH.RaCustomerTrxBillToCustomerId
        LEFT JOIN svo.D_SITE_USE           DSU ON DSU.SITE_USE = TH.RaCustomerTrxBillToSiteUseId
        LEFT JOIN svo.D_BUSINESS_UNIT      DBU ON DBU.BUSINESS_UNIT_ID = TH.RaCustomerTrxOrgId
        LEFT JOIN svo.D_LEDGER             DL  ON DL.LEDGER_ID = TH.RaCustomerTrxSetOfBooksId
        LEFT JOIN svo.D_LEGAL_ENTITY       DLE ON DLE.LEGAL_ENTITY_ID = TH.RaCustomerTrxLegalEntityId
        LEFT JOIN svo.D_ITEM               DI  ON DI.INVENTORY_ITEM_ID = TL.RaCustomerTrxLineInventoryItemId --AND DI.ITEM_ORG_ID = 300000004571765
        LEFT JOIN svo.D_ACCOUNT            DA  ON DA.ACCOUNT_ID = C.ACCOUNT_ID
        LEFT JOIN svo.D_BUSINESS_OFFERING  DBO ON DBO.BUSINESS_OFFERING_ID = C.BUSINESSOFFERING_ID
        LEFT JOIN svo.D_COMPANY            DCO ON DCO.COMPANY_ID = C.COMPANY_ID
        LEFT JOIN svo.D_COST_CENTER        DCC ON DCC.COST_CENTER_ID = C.COSTCENTER_ID
        LEFT JOIN svo.D_INTERCOMPANY       DIC ON DIC.INTERCOMPANY_ID = C.INTERCOMPANY_ID
        LEFT JOIN svo.D_INDUSTRY          DIN ON DIN.INDUSTRY_ID = C.INDUSTRY_ID
        WHERE NOT EXISTS (SELECT 1 FROM svo.F_AR_TRANSACTION_LINE_DISTRIBUTION t WHERE t.AR_TRX_LINE_GL_DIST_ID = TD.RaCustTrxLineGlDistCustTrxLineGlDistId);

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
