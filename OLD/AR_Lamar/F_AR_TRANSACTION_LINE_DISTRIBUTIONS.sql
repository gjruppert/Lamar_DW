USE [Oracle_Reporting_P2];
GO
SET NOCOUNT ON;
GO

/* ============================================================
   FACT: svo.F_AR_TRANSACTION_LINE_DISTRIBUTION
   Grain: 1 row per AR_TRX_LINE_GL_DIST_ID (RaCustTrxLineGlDistCustTrxLineGlDistId)
   Sources:
     - bzo.AR_TransactionDistributionExtractPVO  (base grain)
     - bzo.AR_TransactionLineExtractPVO          (line attributes)
     - bzo.AR_TransactionHeaderExtractPVO        (header attributes)
   Notes:
     - No MAP CTEs
     - No AddDateTime stored; BZ_LOAD_DATE derived from distribution AddDateTime
     - SV_LOAD_DATE = CAST(GETDATE() AS DATE)
     - Your D_ITEM BK is INVENTORY_ITEM_ID, so join to [svo].[D_ITEM] on that.
     - ACCOUNT_SK left as 0 until CCID->D_ACCOUNT mapping confirmed (CCID available on distribution).
   ============================================================ */

IF OBJECT_ID('[svo].[F_AR_TRANSACTION_LINE_DISTRIBUTION]', 'U') IS NOT NULL
    DROP TABLE [svo].[F_AR_TRANSACTION_LINE_DISTRIBUTION];
GO

CREATE TABLE [svo].[F_AR_TRANSACTION_LINE_DISTRIBUTION]
(
    AR_TRX_LINE_DIST_PK                bigint IDENTITY(1,1) NOT NULL,

    /* Dimension keys */
    CUSTOMER_SK                         bigint NOT NULL,
    CUSTOMER_SITE_USE_SK                bigint NOT NULL,
    BUSINESS_UNIT_SK                    bigint NOT NULL,
    LEDGER_SK                           bigint NOT NULL,
    ITEM_SK                             bigint NOT NULL,
    BUSINESS_OFFERING_SK                 BIGINT ,--NOT NULL,
    COMPANY_SK                           BIGINT  NULL,
    COST_CENTER_SK                       BIGINT  NULL,
    INDUSTRY_SK                          BIGINT  NULL,
    INTERCOMPANY_SK                      BIGINT  NULL,
    LEGAL_ENTITY_SK                      BIGINT NOT NULL,
    ACCOUNT_SK                          bigint NOT NULL,   
    CURRENCY_SK                         bigint NOT NULL,  -- 0 (no currency-code dim in your list)

    /* Date keys */
    TRX_DATE_SK                         int    NOT NULL,
    BILLING_DATE_SK                     int    NOT NULL,
    TERM_DUE_DATE_SK                    int    NOT NULL,
    GL_DATE_SK                          int    NOT NULL,
    GL_POSTED_DATE_SK                   int    NOT NULL,
    LINE_SALES_ORDER_DATE_SK            int    NOT NULL,

    /* Business identifiers (degenerate keys) */
    AR_TRX_LINE_GL_DIST_ID              bigint NOT NULL,  -- BK
    AR_TRANSACTION_ID                   bigint NOT NULL,  -- RaCustomerTrxCustomerTrxId / DistCustomerTrxId
    AR_TRANSACTION_LINE_ID              bigint NULL,      -- DistCustomerTrxLineId
    LINE_NUMBER                         bigint NULL,

    /* Source ids / attributes */
    BILL_TO_CUSTOMER_ID                 bigint NULL,
    BILL_TO_SITE_USE_ID                 bigint NULL,

    TRX_TYPE_SEQ_ID                     bigint NULL,
    TERM_ID                             bigint NULL,

    INVENTORY_ITEM_ID                   bigint NULL,

    ACCOUNT_CLASS                       varchar(20) NOT NULL,

    TRX_NUMBER                          varchar(20) NULL,
    TRX_STATUS                          varchar(30) NULL,
    TRX_CLASS                           varchar(20) NULL,
    INVOICE_CURRENCY_CODE               varchar(15) NULL,

    LINE_TYPE                           varchar(20) NULL,
    LINE_DESCRIPTION                    varchar(240) NULL,

    /* Measures */
    GL_DIST_AMOUNT                      decimal(29,4) NULL,
    GL_DIST_ACCTD_AMOUNT                decimal(29,4) NULL,

    LINE_EXTENDED_AMOUNT                decimal(29,4) NULL,
    LINE_EXTENDED_ACCTD_AMOUNT          decimal(29,4) NULL,
    LINE_REVENUE_AMOUNT                 decimal(29,4) NULL,
    LINE_TAXABLE_AMOUNT                 decimal(29,4) NULL,
    LINE_QTY_INVOICED                   bigint NULL,
    LINE_UNIT_SELLING_PRICE             decimal(29,4) NULL,

    /* Audit */
    BZ_LOAD_DATE                        date NOT NULL,
    SV_LOAD_DATE                        date NOT NULL
        CONSTRAINT DF_F_AR_TRX_LINE_DIST_SV_LOAD_DATE DEFAULT (CAST(GETDATE() AS date)),

    CONSTRAINT PK_F_AR_TRANSACTION_LINE_DISTRIBUTION
        PRIMARY KEY CLUSTERED (AR_TRX_LINE_DIST_PK)
        ON [FG_SilverFact]
)
ON [FG_SilverFact];
GO

CREATE NONCLUSTERED INDEX UX_F_AR_TRANSACTION_LINE_DISTRIBUTION_ID
ON [svo].[F_AR_TRANSACTION_LINE_DISTRIBUTION] (AR_TRX_LINE_GL_DIST_ID)
ON [FG_SilverFact];
GO

CREATE NONCLUSTERED INDEX IX_F_AR_TRX_LINE_DIST_TRX
ON [svo].[F_AR_TRANSACTION_LINE_DISTRIBUTION] (AR_TRANSACTION_ID, GL_DATE_SK)
INCLUDE (ACCOUNT_CLASS, GL_DIST_AMOUNT, GL_DIST_ACCTD_AMOUNT, CUSTOMER_SK)
ON [FG_SilverFact];
GO

CREATE NONCLUSTERED INDEX IX_F_AR_TRX_LINE_DIST_ITEM
ON [svo].[F_AR_TRANSACTION_LINE_DISTRIBUTION] (ITEM_SK, TRX_DATE_SK)
INCLUDE (GL_DIST_AMOUNT, ACCOUNT_CLASS, AR_TRANSACTION_ID, LINE_NUMBER)
ON [FG_SilverFact];
GO

/* ============================================================
   Load
   ============================================================ */

MERGE [svo].[F_AR_TRANSACTION_LINE_DISTRIBUTION] AS F
USING
(
        SELECT
        /* -----------------------------
           Dimension resolution (direct joins, no MAPs)
           ----------------------------- */
 
        ISNULL(DCA.CUSTOMER_SK, 0)          AS CUSTOMER_SK,
        ISNULL(DSU.SITE_USE_SK, 0)          AS CUSTOMER_SITE_USE_SK,
        ISNULL(DBU.BUSINESS_UNIT_SK,0)      AS BUSINESS_UNIT_SK,
        ISNULL(DL.LEDGER_SK,0)              AS LEDGER_SK,
        ISNULL(DLE.LEGAL_ENTITY_SK,0)       AS LEGAL_ENTITY_SK,
        ISNULL(DI.ITEM_SK, 0)               AS ITEM_SK,
        ISNULL(DBO.BUSINESS_OFFERING_SK,0)  AS BUSINESS_OFFERING_SK,
        ISNULL(DCO.COMPANY_SK,0)            AS COMPANY_SK,
        ISNULL(DCC.COST_CENTER_SK,0)        AS COST_CENTER_SK,
        ISNULL(DIN.INDUSTRY_SK,0)           AS INDUSTRY_SK,
        ISNULL(DIC.INTERCOMPANY_SK,0)       AS INTERCOMPANY_SK,
        ISNULL(DA.ACCOUNT_SK,0)             AS ACCOUNT_SK,
        CAST(0 AS bigint)                   AS CURRENCY_SK,

        /* Date keys */
        ISNULL(CONVERT(int, CONVERT(char(8), TH.RaCustomerTrxTrxDate, 112)), 0)             AS TRX_DATE_SK,
        ISNULL(CONVERT(int, CONVERT(char(8), TH.RaCustomerTrxBillingDate, 112)), 0)         AS BILLING_DATE_SK,
        ISNULL(CONVERT(int, CONVERT(char(8), TH.RaCustomerTrxTermDueDate, 112)), 0)         AS TERM_DUE_DATE_SK,

        ISNULL(CONVERT(int, CONVERT(char(8), TD.RaCustTrxLineGlDistGlDate, 112)), 0)        AS GL_DATE_SK,
        ISNULL(CONVERT(int, CONVERT(char(8), TD.RaCustTrxLineGlDistGlPostedDate, 112)), 0)  AS GL_POSTED_DATE_SK,

        ISNULL(CONVERT(int, CONVERT(char(8), TL.RaCustomerTrxLineSalesOrderDate, 112)), 0)  AS LINE_SALES_ORDER_DATE_SK,

        /* Business identifiers */
        ISNULL(TD.RaCustTrxLineGlDistCustTrxLineGlDistId,-1)                                AS AR_TRX_LINE_GL_DIST_ID,
        ISNULL(TD.RaCustTrxLineGlDistCustomerTrxId,-1)                                      AS AR_TRANSACTION_ID,
        ISNULL(TD.RaCustTrxLineGlDistCustomerTrxLineId,-1)                                  AS AR_TRANSACTION_LINE_ID,

        ISNULL(TL.RaCustomerTrxLineLineNumber,-1)                                           AS LINE_NUMBER,

        /* Header context */
        ISNULL(TH.RaCustomerTrxBillToCustomerId,-1)                                         AS BILL_TO_CUSTOMER_ID,
        ISNULL(TH.RaCustomerTrxBillToSiteUseId,-1)                                          AS BILL_TO_SITE_USE_ID,
        ISNULL(TH.RaCustomerTrxCustTrxTypeSeqId,-1)                                         AS TRX_TYPE_SEQ_ID,
        ISNULL(TH.RaCustomerTrxTermId,-1)                                                   AS TERM_ID,

        /* Line context */
        ISNULL(TL.RaCustomerTrxLineInventoryItemId,-1)                                      AS INVENTORY_ITEM_ID,

        /* Dist attributes */
        ISNULL(TD.RaCustTrxLineGlDistAccountClass,'UNK')                                    AS ACCOUNT_CLASS,

        /* Degenerate descriptors */
        ISNULL(TH.RaCustomerTrxTrxNumber,-1)                                                AS TRX_NUMBER,
        ISNULL(TH.RaCustomerTrxStatusTrx, 'U')                                              AS TRX_STATUS,
        ISNULL(TH.RaCustomerTrxTrxClass, 'UNK')                                             AS TRX_CLASS,
        ISNULL(TH.RaCustomerTrxInvoiceCurrencyCode, 'UNK')                                  AS INVOICE_CURRENCY_CODE,

        ISNULL(TL.RaCustomerTrxLineLineType,'U')                                            AS LINE_TYPE,
        ISNULL(TL.RaCustomerTrxLineDescription,'UNK')                                       AS LINE_DESCRIPTION,

        /* Measures */
        ISNULL(TD.RaCustTrxLineGlDistAmount,0)                                              AS GL_DIST_AMOUNT,
        ISNULL(TD.RaCustTrxLineGlDistAcctdAmount,0)                                         AS GL_DIST_ACCTD_AMOUNT,

        ISNULL(TL.RaCustomerTrxLineExtendedAmount,0)                                        AS LINE_EXTENDED_AMOUNT,
        ISNULL(TL.RaCustomerTrxLineExtendedAcctdAmount,0)                                   AS LINE_EXTENDED_ACCTD_AMOUNT,
        ISNULL(TL.RaCustomerTrxLineRevenueAmount,0)                                         AS LINE_REVENUE_AMOUNT,
        ISNULL(TL.RaCustomerTrxLineTaxableAmount,0)                                         AS LINE_TAXABLE_AMOUNT,
        ISNULL(TL.RaCustomerTrxLineQuantityInvoiced,0)                                      AS LINE_QTY_INVOICED,
        ISNULL(TL.RaCustomerTrxLineRaCustomerTrxLineUnitSellingPrice,0)                     AS LINE_UNIT_SELLING_PRICE,

        /* Audit */
        CAST(TD.AddDateTime AS date)                                                        AS BZ_LOAD_DATE

    FROM [bzo].[AR_TransactionDistributionExtractPVO] AS TD
    LEFT JOIN [bzo].[AR_TransactionLineExtractPVO]    AS TL  ON TL.RaCustomerTrxLineCustomerTrxLineId = TD.RaCustTrxLineGlDistCustomerTrxLineId
    LEFT JOIN [bzo].[AR_TransactionHeaderExtractPVO]  AS TH  ON TH.RaCustomerTrxCustomerTrxId = TD.RaCustTrxLineGlDistCustomerTrxId

    /* Dims */
  	LEFT JOIN stage.LINES_CODE_COMBO_LOOKUP AS C        ON TD.RaCustTrxLineGlDistCodeCombinationId = C.CODE_COMBINATION_BK

    LEFT JOIN svo.D_CUSTOMER_ACCOUNT    AS DCA          ON DCA.CUSTOMER_ACCOUNT_ID  = TH.RaCustomerTrxBillToCustomerId
    LEFT JOIN svo.D_CUSTOMER_SITE_USE   AS DSU          ON DSU.SITE_USE             = TH.RaCustomerTrxBillToSiteUseId
    LEFT JOIN svo.D_BUSINESS_UNIT       AS DBU          ON DBU.BUSINESS_UNIT_ID     = TH.RaCustomerTrxOrgId
    LEFT JOIN svo.D_LEDGER              AS DL           ON DL.LEDGER_ID             = TH.RaCustomerTrxSetOfBooksId
    LEFT JOIN svo.D_LEGAL_ENTITY        AS DLE          ON DLE.LEGAL_ENTITY_ID      = TH.RaCustomerTrxLegalEntityId
    LEFT JOIN svo.D_ITEM                AS DI           ON DI.INVENTORY_ITEM_ID     = TL.RaCustomerTrxLineInventoryItemId AND DI.ITEM_ORG_ID = 300000004571765
    LEFT JOIN svo.D_ACCOUNT             AS DA           ON DA.ACCOUNT_ID            = C.ACCOUNT_ID
    LEFT JOIN svo.D_BUSINESS_OFFERING   AS DBO          ON DBO.BUSINESS_OFFERING_ID = C.BUSINESSOFFERING_ID
    LEFT JOIN svo.D_COMPANY             AS DCO          ON DCO.COMPANY_ID           = C.COMPANY_ID
    LEFT JOIN svo.D_COST_CENTER         AS DCC          ON DCC.COST_CENTER_ID       = C.COSTCENTER_ID
    LEFT JOIN svo.D_INTERCOMPANY        AS DIC          ON DIC.INTERCOMPANY_ID      = C.INTERCOMPANY_ID
    LEFT JOIN svo.D_INDUSTRY            AS DIN          ON DIN.INDUSTRY_ID          = C.INDUSTRY_ID
) AS X
ON F.AR_TRX_LINE_GL_DIST_ID = X.AR_TRX_LINE_GL_DIST_ID

WHEN MATCHED THEN
    UPDATE SET
        F.CUSTOMER_SK                = X.CUSTOMER_SK,
        F.CUSTOMER_SITE_USE_SK       = X.CUSTOMER_SITE_USE_SK,
        F.BUSINESS_UNIT_SK           = X.BUSINESS_UNIT_SK,
        F.LEDGER_SK                  = X.LEDGER_SK,
        F.LEGAL_ENTITY_SK            = X.LEGAL_ENTITY_SK,
        F.ITEM_SK                    = X.ITEM_SK,
        F.BUSINESS_OFFERING_SK       = X.BUSINESS_OFFERING_SK,
        F.COMPANY_SK                 = X.COMPANY_SK,
        F.COST_CENTER_SK             = X.COST_CENTER_SK,
        F.INDUSTRY_SK                = X.INDUSTRY_SK,
        F.INTERCOMPANY_SK            = X.INTERCOMPANY_SK,
        F.ACCOUNT_SK                 = X.ACCOUNT_SK,
        F.CURRENCY_SK                = X.CURRENCY_SK,

        F.TRX_DATE_SK                = X.TRX_DATE_SK,
        F.BILLING_DATE_SK            = X.BILLING_DATE_SK,
        F.TERM_DUE_DATE_SK           = X.TERM_DUE_DATE_SK,
        F.GL_DATE_SK                 = X.GL_DATE_SK,
        F.GL_POSTED_DATE_SK          = X.GL_POSTED_DATE_SK,
        F.LINE_SALES_ORDER_DATE_SK   = X.LINE_SALES_ORDER_DATE_SK,

        F.AR_TRANSACTION_ID          = X.AR_TRANSACTION_ID,
        F.AR_TRANSACTION_LINE_ID     = X.AR_TRANSACTION_LINE_ID,
        F.LINE_NUMBER                = X.LINE_NUMBER,

        F.BILL_TO_CUSTOMER_ID        = X.BILL_TO_CUSTOMER_ID,
        F.BILL_TO_SITE_USE_ID        = X.BILL_TO_SITE_USE_ID,
        F.TRX_TYPE_SEQ_ID            = X.TRX_TYPE_SEQ_ID,
        F.TERM_ID                    = X.TERM_ID,

        F.INVENTORY_ITEM_ID          = X.INVENTORY_ITEM_ID,
        F.ACCOUNT_CLASS              = X.ACCOUNT_CLASS,

        F.TRX_NUMBER                 = X.TRX_NUMBER,
        F.TRX_STATUS                 = X.TRX_STATUS,
        F.TRX_CLASS                  = X.TRX_CLASS,
        F.INVOICE_CURRENCY_CODE      = X.INVOICE_CURRENCY_CODE,

        F.LINE_TYPE                  = X.LINE_TYPE,
        F.LINE_DESCRIPTION           = X.LINE_DESCRIPTION,

        F.GL_DIST_AMOUNT             = X.GL_DIST_AMOUNT,
        F.GL_DIST_ACCTD_AMOUNT       = X.GL_DIST_ACCTD_AMOUNT,

        F.LINE_EXTENDED_AMOUNT       = X.LINE_EXTENDED_AMOUNT,
        F.LINE_EXTENDED_ACCTD_AMOUNT = X.LINE_EXTENDED_ACCTD_AMOUNT,
        F.LINE_REVENUE_AMOUNT        = X.LINE_REVENUE_AMOUNT,
        F.LINE_TAXABLE_AMOUNT        = X.LINE_TAXABLE_AMOUNT,
        F.LINE_QTY_INVOICED          = X.LINE_QTY_INVOICED,
        F.LINE_UNIT_SELLING_PRICE    = X.LINE_UNIT_SELLING_PRICE,

        F.BZ_LOAD_DATE               = X.BZ_LOAD_DATE

WHEN NOT MATCHED BY TARGET THEN
    INSERT
    (
        CUSTOMER_SK,
        CUSTOMER_SITE_USE_SK,
        BUSINESS_UNIT_SK,
        LEDGER_SK,
        LEGAL_ENTITY_SK,
        ITEM_SK,
        BUSINESS_OFFERING_SK,
        COMPANY_SK,
        COST_CENTER_SK,
        INDUSTRY_SK,
        INTERCOMPANY_SK,
        ACCOUNT_SK,
        CURRENCY_SK,
        TRX_DATE_SK,
        BILLING_DATE_SK,
        TERM_DUE_DATE_SK,
        GL_DATE_SK,
        GL_POSTED_DATE_SK,
        LINE_SALES_ORDER_DATE_SK,
        AR_TRX_LINE_GL_DIST_ID,
        AR_TRANSACTION_ID,
        AR_TRANSACTION_LINE_ID,
        LINE_NUMBER,
        BILL_TO_CUSTOMER_ID,
        BILL_TO_SITE_USE_ID,
        TRX_TYPE_SEQ_ID,
        TERM_ID,
        INVENTORY_ITEM_ID,
        ACCOUNT_CLASS,
        TRX_NUMBER,
        TRX_STATUS,
        TRX_CLASS,
        INVOICE_CURRENCY_CODE,
        LINE_TYPE,
        LINE_DESCRIPTION,
        GL_DIST_AMOUNT,
        GL_DIST_ACCTD_AMOUNT,
        LINE_EXTENDED_AMOUNT,
        LINE_EXTENDED_ACCTD_AMOUNT,
        LINE_REVENUE_AMOUNT,
        LINE_TAXABLE_AMOUNT,
        LINE_QTY_INVOICED,
        LINE_UNIT_SELLING_PRICE,
        BZ_LOAD_DATE
    )
    VALUES
    (
        X.CUSTOMER_SK,
        X.CUSTOMER_SITE_USE_SK,
        X.BUSINESS_UNIT_SK,
        X.LEDGER_SK,
        X.LEGAL_ENTITY_SK,
        X.ITEM_SK,
        X.BUSINESS_OFFERING_SK,
        X.COMPANY_SK,
        X.COST_CENTER_SK,
        X.INDUSTRY_SK,
        X.INTERCOMPANY_SK,
        X.ACCOUNT_SK,
        X.CURRENCY_SK,
        X.TRX_DATE_SK,
        X.BILLING_DATE_SK,
        X.TERM_DUE_DATE_SK,
        X.GL_DATE_SK,
        X.GL_POSTED_DATE_SK,
        X.LINE_SALES_ORDER_DATE_SK,
        X.AR_TRX_LINE_GL_DIST_ID,
        X.AR_TRANSACTION_ID,
        X.AR_TRANSACTION_LINE_ID,
        X.LINE_NUMBER,
        X.BILL_TO_CUSTOMER_ID,
        X.BILL_TO_SITE_USE_ID,
        X.TRX_TYPE_SEQ_ID,
        X.TERM_ID,
        X.INVENTORY_ITEM_ID,
        X.ACCOUNT_CLASS,
        X.TRX_NUMBER,
        X.TRX_STATUS,
        X.TRX_CLASS,
        X.INVOICE_CURRENCY_CODE,
        X.LINE_TYPE,
        X.LINE_DESCRIPTION,
        X.GL_DIST_AMOUNT,
        X.GL_DIST_ACCTD_AMOUNT,
        X.LINE_EXTENDED_AMOUNT,
        X.LINE_EXTENDED_ACCTD_AMOUNT,
        X.LINE_REVENUE_AMOUNT,
        X.LINE_TAXABLE_AMOUNT,
        X.LINE_QTY_INVOICED,
        X.LINE_UNIT_SELLING_PRICE,
        X.BZ_LOAD_DATE
    )

WHEN NOT MATCHED BY SOURCE THEN
    DELETE;
GO
