--IF OBJECT_ID('svo.F_AP_INVOICE_LINE_DISTRIBUTION','U') IS NOT NULL 
--    DROP TABLE svo.F_AP_INVOICE_LINE_DISTRIBUTION;
--GO

--CREATE TABLE svo.F_AP_INVOICE_LINE_DISTRIBUTION
--(
--    AP_INVOICE_DIST_FACT_PK      BIGINT IDENTITY(1,1) NOT NULL PRIMARY KEY,

--    AP_INVOICE_HEADER_SK         BIGINT       NOT NULL,
--    INVOICE_ID                   BIGINT       NOT NULL,
--    INVOICE_LINE_NUMBER          BIGINT       NULL,
--    INVOICE_DISTRIBUTION_ID      BIGINT       NOT NULL,
--    DIST_INV_LINE_NUMBER         BIGINT       NULL,
--    DISTRIBUTION_LINE_NUMBER     BIGINT       NOT NULL,

--    DIST_ACCOUNTING_DATE_SK      INT          NOT NULL,
--    LINE_ACCOUNTING_DATE_SK      INT          NOT NULL,

--    ACCOUNT_SK                   BIGINT       NOT NULL,
--    BUSINESS_OFFERING_SK         BIGINT       NOT NULL,
--    COMPANY_SK                   BIGINT       NOT NULL,
--    COST_CENTER_SK               BIGINT       NOT NULL,
--    INDUSTRY_SK                  BIGINT       NOT NULL,
--    INTERCOMPANY_SK              BIGINT       NOT NULL,
--    LEGAL_ENTITY_SK              BIGINT       NOT NULL,
--    BUSINESS_UNIT_SK             BIGINT       NOT NULL,
--    VENDOR_SITE_SK               BIGINT       NOT NULL,
--    LEDGER_SK                    BIGINT       NOT NULL,
--    DISTRIBUTION_CLASS           VARCHAR(30)   NULL,
--    DIST_DESCRIPTION             VARCHAR(1000) NULL,
--    LINE_DESCRIPTION             VARCHAR(1000) NULL,
--    DISTRIBUTION_AMOUNT          DECIMAL(29,4) NOT NULL,
--    POSTED_FLAG                  VARCHAR(1)    NULL,
--    TYPE_1099                    VARCHAR(10)   NULL,

--    INV_TAX_JURIDISTION_CODE     VARCHAR(60)   NULL,
--    INV_TAX_RATE                 DECIMAL(29,4) NULL,
--    LINE_TYPE_LOOKUP_CODE        VARCHAR(25)   NULL,
--    PJC_CONTRACT_ID              BIGINT       NULL,
--    PJC_CONTRACT_LINE_ID         BIGINT       NULL,
--    PJC_EXPENDITURE_ITEM_DATE    DATE         NULL,
--    PJC_EXPENDITURE_TYPE_ID      BIGINT       NULL,
--    PJC_FUNDING_ALLOCATION_ID    BIGINT       NULL,
--    PJC_ORGANIZATION_ID          BIGINT       NULL,
--    PJC_PROJECT_ID               BIGINT       NULL,
--    PJC_TASK_ID                  BIGINT       NULL,

--    PO_DISTRIBUTION_ID           BIGINT       NULL,

--    DIST_LAST_UPDATE_DATE        DATE         NOT NULL,
--    DIST_LAST_UPDATE_BY          VARCHAR(64)  NOT NULL,
--    DIST_LAST_UPDATE_LOGIN       VARCHAR(64)  NOT NULL,

--    DIST_CODE_COMBINATION_ID     BIGINT       NOT NULL,

--    BZ_LOAD_DATE                 DATE         NOT NULL,
--    SV_LOAD_DATE                 DATE         NOT NULL,
--    LINES_AMOUNT_debug_only     DECIMAL(29,4) NOT NULL

--) ON FG_SilverFact;
--GO

TRUNCATE TABLE svo.F_AP_INVOICE_LINE_DISTRIBUTION;
GO

INSERT INTO svo.F_AP_INVOICE_LINE_DISTRIBUTION (
    AP_INVOICE_HEADER_SK,
    INVOICE_ID,
    INVOICE_LINE_NUMBER,
    INVOICE_DISTRIBUTION_ID,
    DIST_INV_LINE_NUMBER,
    DISTRIBUTION_LINE_NUMBER,
    DIST_ACCOUNTING_DATE_SK,
    LINE_ACCOUNTING_DATE_SK,
    ACCOUNT_SK,
    BUSINESS_OFFERING_SK,
    COMPANY_SK,
    COST_CENTER_SK,
    INDUSTRY_SK,
    INTERCOMPANY_SK,
    LEGAL_ENTITY_SK,
    BUSINESS_UNIT_SK,
    VENDOR_SITE_SK,
    LEDGER_SK,
    DISTRIBUTION_CLASS,
    DIST_DESCRIPTION,
    LINE_DESCRIPTION,
    DISTRIBUTION_AMOUNT,
    POSTED_FLAG,
    TYPE_1099,
    INV_TAX_JURIDISTION_CODE,
    INV_TAX_RATE,
    LINE_TYPE_LOOKUP_CODE,
    PJC_CONTRACT_ID,
    PJC_CONTRACT_LINE_ID,
    PJC_EXPENDITURE_ITEM_DATE,
    PJC_EXPENDITURE_TYPE_ID,
    PJC_FUNDING_ALLOCATION_ID,
    PJC_ORGANIZATION_ID,
    PJC_PROJECT_ID,
    PJC_TASK_ID,
    PO_DISTRIBUTION_ID,
    DIST_LAST_UPDATE_DATE,
    DIST_LAST_UPDATE_BY,
    DIST_LAST_UPDATE_LOGIN,
    DIST_CODE_COMBINATION_ID,
    BZ_LOAD_DATE,
    SV_LOAD_DATE,
    LINES_AMOUNT_debug_only

)
SELECT 

    ISNULL(H.AP_INVOICE_HEADER_SK, 0)                                            AS AP_INVOICE_HEADER_SK,
    ISNULL(I.ApInvoiceLinesAllInvoiceId,-1)                                      AS INVOICE_ID,
    ISNULL(I.ApInvoiceLinesAllLineNumber,-1)                                     AS INVOICE_LINE_NUMBER,
    ISNULL(D.ApInvoiceDistributionsInvoiceDistributionId,-1)                     AS INVOICE_DISTRIBUTION_ID,
    ISNULL(D.ApInvoiceDistributionsInvoiceLineNumber,-1)                         AS DIST_INV_LINE_NUMBER,
    ISNULL(D.ApInvoiceDistributionsDistributionLineNumber,-1)                    AS DISTRIBUTION_LINE_NUMBER, 
    ISNULL(CONVERT(INT, FORMAT(D.ApInvoiceDistributionsAccountingDate,'yyyyMMdd')),10101) AS DIST_ACCOUNTING_DATE_SK,
    ISNULL(CONVERT(INT, FORMAT(I.ApInvoiceLinesAllAccountingDate,'yyyyMMdd')),10101)      AS LINE_ACCOUNTING_DATE_SK,
    ISNULL(DA.ACCOUNT_SK, 0)                                                     AS ACCOUNT_SK,
    ISNULL(DBO.BUSINESS_OFFERING_SK, 0)                                          AS BUSINESS_OFFERING_SK,
    ISNULL(DCO.COMPANY_SK, 0)                                                    AS COMPANY_SK,
    ISNULL(DCC.COST_CENTER_SK, 0)                                                AS COST_CENTER_SK,
    ISNULL(DI.INDUSTRY_SK, 0)                                                    AS INDUSTRY_SK,
    ISNULL(DIC.INTERCOMPANY_SK,0)                                                AS INTERCOMPANY_SK,
    ISNULL(LE.LEGAL_ENTITY_SK,0)                                                 AS LEGAL_ENTITY_SK,
    ISNULL(BU.BUSINESS_UNIT_SK,0)                                                AS BUSINESS_UNIT_SK,
    ISNULL(VS.VENDOR_SITE_SK,0)                                                  AS VENDOR_SITE_SK,  
    ISNULL(LDG.LEDGER_SK,0)                                                      AS LEDGER_SK,
    ISNULL(D.ApInvoiceDistributionsDistributionClass,'U')                        AS DISTRIBUTION_CLASS,
    ISNULL(D.ApInvoiceDistributionsDescription,'UNK')                            AS DIST_DESCRIPTION,
    ISNULL(I.ApInvoiceLinesAllDescription,'UNK')                                 AS LINE_DESCRIPTION,
    COALESCE(D.ApInvoiceDistributionsAmount,I.ApInvoiceLinesAllAmount,0)         AS DISTRIBUTION_AMOUNT,
    ISNULL(D.ApInvoiceDistributionsPostedFlag, 'U')                              AS POSTED_FLAG,
    ISNULL(D.ApInvoiceDistributionsType1099,0)                                   AS TYPE_1099,
    ISNULL(I.ApInvoiceLinesAllTaxJurisdictionCode,'UNK')                         AS INV_TAX_JURIDISTION_CODE,
    ISNULL(I.ApInvoiceLinesAllTaxRate,0)                                         AS INV_TAX_RATE,
    I.ApInvoiceLinesAllLineTypeLookupCode,
    ISNULL(D.ApInvoiceDistributionsPJC_CONTRACT_ID,0)                            AS PJC_CONTRACT_ID,
    ISNULL(D.ApInvoiceDistributionsPJC_CONTRACT_LINE_ID,0)                       AS PJC_CONTRACT_LINE_ID,
    ISNULL(D.ApInvoiceDistributionsPJC_EXPENDITURE_ITEM_DATE,'9999-12-31')       AS PJC_EXPENDITURE_ITEM_DATE,
    ISNULL(D.ApInvoiceDistributionsPJC_EXPENDITURE_TYPE_ID,0)                    AS PJC_EXPENDITURE_TYPE_ID,  
    ISNULL(D.ApInvoiceDistributionsPJC_FUNDING_ALLOCATION_ID,0)                  AS PJC_FUNDING_ALLOCATION_ID,
    ISNULL(D.ApInvoiceDistributionsPJC_ORGANIZATION_ID,0)                        AS PJC_ORGANIZATION_ID,
    ISNULL(D.ApInvoiceDistributionsPJC_PROJECT_ID,0)                             AS PJC_PROJECT_ID,
    ISNULL(D.ApInvoiceDistributionsPJC_TASK_ID,0)                                AS PJC_TASK_ID,
    ISNULL(D.ApInvoiceDistributionsPoDistributionId,0)                           AS PO_DISTRIBUTION_ID,
    ISNULL(CONVERT(DATE, D.ApInvoiceDistributionsLastUpdateDate),'0001-01-01')   AS DIST_LAST_UPDATE_DATE,
    ISNULL(D.ApInvoiceDistributionsLastUpdatedBy,'UNK')                          AS DIST_LAST_UPDATE_BY,
    ISNULL(D.ApInvoiceDistributionsLastUpdateLogin,'UNK')                        AS DIST_LAST_UPDATE_LOGIN,
    ISNULL(D.ApInvoiceDistributionsDistCodeCombinationId,-1)                     AS DIST_CODE_COMBINATION_ID,
    ISNULL(CAST(D.AddDateTime AS DATE),'0001-01-01')                             AS BZ_LOAD_DATE,
    CAST(GETDATE() AS DATE)                                                      AS SV_LOAD_DATE,
    ISNULL(I.ApInvoiceLinesAllAmount,0)                                          AS LINES_AMOUNT_debug_only

FROM bzo.AP_InvoiceHeaderExtractPVO IH
LEFT JOIN bzo.AP_InvoiceLineExtractPVO I         ON IH.ApInvoicesInvoiceId = I.ApInvoiceLinesAllInvoiceId 
LEFT JOIN bzo.AP_InvoiceDistributionExtractPVO D ON I.ApInvoiceLinesAllInvoiceId = D.ApInvoiceDistributionsInvoiceId
                                                     AND I.ApInvoiceLinesAllLineNumber = D.ApInvoiceDistributionsInvoiceLineNumber 
LEFT JOIN stage.LINES_CODE_COMBO_LOOKUP   C       ON CAST(D.ApInvoiceDistributionsDistCodeCombinationId AS BIGINT) = C.CODE_COMBINATION_BK
LEFT JOIN svo.D_AP_INVOICE_HEADER      AS H       ON H.INVOICE_ID             = IH.ApInvoicesInvoiceId
LEFT JOIN svo.D_ACCOUNT                AS DA      ON DA.ACCOUNT_ID            = C.ACCOUNT_ID
LEFT JOIN svo.D_BUSINESS_OFFERING      AS DBO     ON DBO.BUSINESS_OFFERING_ID = C.BUSINESSOFFERING_ID
LEFT JOIN svo.D_COMPANY                AS DCO     ON DCO.COMPANY_ID           = C.COMPANY_ID
LEFT JOIN svo.D_COST_CENTER            AS DCC     ON DCC.COST_CENTER_ID       = C.COSTCENTER_ID
LEFT JOIN svo.D_INDUSTRY               AS DI      ON DI.INDUSTRY_ID           = C.INDUSTRY_ID
LEFT JOIN svo.D_INTERCOMPANY           AS DIC     ON DIC.INTERCOMPANY_ID      = C.INTERCOMPANY_ID
LEFT JOIN svo.D_LEGAL_ENTITY           AS LE      ON LE.LEGAL_ENTITY_ID       = IH.ApInvoicesLegalEntityId
LEFT JOIN svo.D_BUSINESS_UNIT          AS BU      ON BU.BUSINESS_UNIT_ID      = IH.ApInvoicesOrgId
LEFT JOIN svo.D_VENDOR_SITE            AS VS      ON VS.VENDOR_SITE_ID        = IH.ApInvoicesVendorSiteId
LEFT JOIN svo.D_LEDGER                 AS LDG     ON LDG.LEDGER_ID            = IH.ApInvoicesSetOfBooksId
WHERE 1 = 1
;

 