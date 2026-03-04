USE [Oracle_Reporting_P2];
GO

IF OBJECT_ID('svo.F_GL_LINES','U') IS NOT NULL DROP TABLE svo.F_GL_LINES;
GO

CREATE TABLE svo.F_GL_LINES
(
    GL_LINE_PK            BIGINT      NOT NULL PRIMARY KEY,
    GL_HEADER_SK          BIGINT      NOT NULL,
    ACCOUNT_SK            BIGINT      NOT NULL,
    BUSINESS_OFFERING_SK  BIGINT      NOT NULL,
    COMPANY_SK            BIGINT      NOT NULL,
    COST_CENTER_SK        BIGINT      NOT NULL,
    CURRENCY_SK           BIGINT      NOT NULL,
    INDUSTRY_SK           BIGINT      NOT NULL,
    INTERCOMPANY_SK       BIGINT      NOT NULL,
    EFFECTIVE_DATE_SK     INT         NOT NULL,   
    LEDGER_SK             BIGINT      NOT NULL,  
    LINE_NUM              BIGINT      NULL,
    [DESCRIPTION]         NVARCHAR(1000) NULL,
    ACCOUNTED_CR          NUMERIC(18,4) NULL,
    ACCOUNTED_DR          NUMERIC(18,4) NULL,
    AMOUNT_USD            NUMERIC(18,4) NULL,
    AMOUNT_LOCAL          NUMERIC(18,4) NULL,
    CREATED_BY            NVARCHAR(32)  NULL,
    LAST_UPDATED_BY       NVARCHAR(64)  NULL,
    LAST_UPDATED_DATE     DATE          NULL,
    CREATION_DATE         DATE          NULL,
    BZ_LOAD_DATE          DATE          NULL,
    SV_LOAD_DATE          DATE          NULL,
    CODE_COMBINATION_ID   BIGINT        NULL
);
GO

INSERT INTO svo.F_GL_LINES (                             
    GL_LINE_PK,
    GL_HEADER_SK,
    ACCOUNT_SK, 
    BUSINESS_OFFERING_SK, 
    COMPANY_SK, 
    COST_CENTER_SK, 
    CURRENCY_SK, 
    INDUSTRY_SK, 
    INTERCOMPANY_SK,
    EFFECTIVE_DATE_SK, 
    LEDGER_SK, 
    LINE_NUM, 
    [DESCRIPTION],
    ACCOUNTED_CR, 
    ACCOUNTED_DR,
    AMOUNT_USD,          
    AMOUNT_LOCAL,          
    CREATED_BY, 
    LAST_UPDATED_BY, 
    LAST_UPDATED_DATE, 
    CREATION_DATE, 
    BZ_LOAD_DATE, 
    SV_LOAD_DATE,
    CODE_COMBINATION_ID
)
SELECT 
    CAST(CONCAT(H.GLJEHEADERSJEBATCHID, H.JEHEADERID, L.JELINENUM) AS BIGINT)          AS GLLINE_PK,
    ISNULL(DH.GL_HEADER_SK, 0)                                                         AS GL_HEADER_SK,
    ISNULL(DA.ACCOUNT_SK, 0)                                                           AS ACCOUNT_SK,
    ISNULL(DBO.BUSINESS_OFFERING_SK, 0)                                                AS BUSINESS_OFFERING_SK,
    ISNULL(DCO.COMPANY_SK, 0)                                                          AS COMPANY_SK,
    ISNULL(DCC.COST_CENTER_SK, 0)                                                      AS COST_CENTER_SK,
    ISNULL(CUR.CURRENCY_SK, 0)                                                         AS CURRENCY_SK,
    ISNULL(DI.INDUSTRY_SK, 0)                                                          AS INDUSTRY_SK,
    ISNULL(DIC.INTERCOMPANY_SK, 0)                                                     AS INTERCOMPANY_SK,
    ISNULL(CAST(CONVERT(CHAR(8), TRY_CONVERT(DATE, L.GLJELINESEFFECTIVEDATE), 112) AS INT), 0) AS EFFECTIVE_DATE_SK,
    ISNULL(LDG.LEDGER_SK, 0)                                                           AS LEDGER_SK,
    CAST(L.JELINENUM  AS BIGINT)                                                       AS LINE_NUM,
    NULLIF(LTRIM(RTRIM(L.GLJELINESDESCRIPTION)), '')                                   AS [DESCRIPTION],
    CAST(ISNULL(L.GLJELINESACCOUNTEDCR,0) AS FLOAT)                                    AS ACCOUNTED_CR,
    CAST(ISNULL(L.GLJELINESACCOUNTEDDR,0) AS FLOAT)                                    AS ACCOUNTED_DR,
    CASE L.GlJeLinesLedgerId WHEN '300000004574005' THEN 0 ELSE
          (CAST(ISNULL(L.GLJELINESACCOUNTEDDR,0) AS FLOAT)-CAST(ISNULL(L.GLJELINESACCOUNTEDCR,0) AS FLOAT))*
          CAST(ISNULL(L.GLJELINESCURRENCYCONVERSIONRATE,1) AS NUMERIC(7,4)) END AS AMOUNT_USD, 
    (CAST(ISNULL(L.GLJELINESACCOUNTEDDR,0) AS FLOAT)-CAST(ISNULL(L.GLJELINESACCOUNTEDCR,0) AS FLOAT)) AS AMOUNT_LOCAL, 
    NULLIF(TRIM(L.GLJELINESCREATEDBY), '')                                             AS CREATED_BY,
    NULLIF(TRIM(L.GLJELINESLASTUPDATEDBY), '')                                         AS LAST_UPDATED_BY,
    CAST(L.GLJELINESLASTUPDATEDATE AS DATE)                                            AS LAST_UPDATED_DATE,
    CAST(L.GLJELINESCREATIONDATE  AS DATE)                                             AS CREATION_DATE,
    CAST(L.AddDateTime AS DATE)                                                        AS BZ_LOAD_DATE,
    CAST(GETDATE()  AS DATE)                                                           AS SV_LOAD_DATE,
    [GlJeLinesCodeCombinationId]                                                       AS CODE_COMBINATION_ID
FROM bzo.GL_JournalLineExtractPVO L
JOIN bzo.GL_JournalHeaderExtractPVO H ON H.JEHEADERID = L.JEHEADERID
LEFT JOIN stage.LINES_CODE_COMBO_LOOKUP C ON CAST(L.GLJELINESCODECOMBINATIONID AS BIGINT) = C.CODE_COMBINATION_BK

-- Surrogate lookups
LEFT JOIN svo.D_GL_HEADER          AS DH  ON DH.JE_HEADER_ID          = L.JEHEADERID
LEFT JOIN svo.D_ACCOUNT            AS DA  ON DA.ACCOUNT_ID            = C.ACCOUNT_ID
LEFT JOIN svo.D_BUSINESS_OFFERING  AS DBO ON DBO.BUSINESS_OFFERING_ID = C.BUSINESSOFFERING_ID
LEFT JOIN svo.D_COMPANY            AS DCO ON DCO.COMPANY_ID           = C.COMPANY_ID
LEFT JOIN svo.D_COST_CENTER        AS DCC ON DCC.COST_CENTER_ID       = C.COSTCENTER_ID
LEFT JOIN svo.D_INDUSTRY           AS DI  ON DI.INDUSTRY_ID           = C.INDUSTRY_ID
LEFT JOIN svo.D_INTERCOMPANY       AS DIC ON DIC.INTERCOMPANY_ID      = C.INTERCOMPANY_ID
LEFT JOIN svo.D_LEDGER             AS LDG ON LDG.LEDGER_ID            = L.GlJeLinesLedgerId
LEFT JOIN svo.D_CURRENCY           AS CUR ON CUR.CURRENCY_ID          =  CONCAT(ISNULL(L.GLJELINESCURRENCYCODE,'UNK'),  
                                                                                CONVERT(CHAR(8), CONVERT(CHAR(8), ISNULL(L.GLJELINESCURRENCYCONVERSIONDATE,'0001-01-01'),112) ),
                                                                                ISNULL(TRIM(L.GLJELINESCURRENCYCONVERSIONTYPE), 'UNK')
                                                                         )                                                           
GO

USE [Oracle_Reporting_P2]
GO

/****** Object:  Index [NonClusteredIndex-20251030-123731]    Script Date: 10/30/2025 1:04:07 PM ******/
CREATE NONCLUSTERED INDEX [F_GL_LINES_EFFDT_ACCTSK_COSK_ETC] ON [svo].[F_GL_LINES]
(
	[EFFECTIVE_DATE_SK] ASC,
	[ACCOUNT_SK] ASC,
	[COMPANY_SK] ASC,
	[BUSINESS_OFFERING_SK] ASC,
	[INDUSTRY_SK] ASC,
	[COST_CENTER_SK] ASC,
	[INTERCOMPANY_SK] ASC
)
INCLUDE([DESCRIPTION],[ACCOUNTED_CR],[ACCOUNTED_DR],[AMOUNT_USD],[AMOUNT_LOCAL]) 
ON FG_SilverFact
GO


