


--/* ================================
--   GL Balances table (SK-based fact)
--   ================================ */
--IF OBJECT_ID('svo.F_GL_BALANCES','U') IS NOT NULL DROP TABLE svo.F_GL_BALANCES;
--GO

--CREATE TABLE [svo].[F_GL_BALANCES](
--    GL_BALANCE_PK         BIGINT IDENTITY(1,1) NOT NULL PRIMARY KEY,
--    ACCOUNT_SK            BIGINT        NOT NULL,
--    BUSINESS_OFFERING_SK  BIGINT        NOT NULL,
--    COMPANY_SK            BIGINT        NOT NULL,
--    COST_CENTER_SK        BIGINT        NOT NULL,
--    INDUSTRY_SK           BIGINT        NOT NULL,
--    INTERCOMPANY_SK       BIGINT        NOT NULL,
--    LEDGER_SK             BIGINT        NOT NULL,
--    BALANCE_DATE_SK       INT           NOT NULL,
--    BEGIN_BALANCE_AMT     DECIMAL(18,4) NULL, 
--    PERIOD_ACTIVITY_AMT   DECIMAL(18,4) NULL,
--    END_BALANCE_AMT       DECIMAL(18,4) NULL, 
--    CURRENCY_CODE         VARCHAR(15)   NOT NULL,
--    PERIOD_NUMBER         INT           NULL,
--    CODE_COMBINATION_ID   BIGINT        NULL,
--    LAST_UPDATE_DATE      DATETIME      NOT NULL, 
--    LAST_UPDATED_BY       VARCHAR(64)   NOT NULL,
--    BZ_LOAD_DATE          DATE          NOT NULL,
--    SV_LOAD_DATE          DATE          NOT NULL
--) ON FG_SilverFact;

TRUNCATE TABLE svo.F_GL_BALANCES;
GO

INSERT INTO svo.F_GL_BALANCES (
    ACCOUNT_SK, 
    BUSINESS_OFFERING_SK, 
    COMPANY_SK, 
    COST_CENTER_SK, 
    INDUSTRY_SK, 
    INTERCOMPANY_SK,
    LEDGER_SK,
    BALANCE_DATE_SK, 
    BEGIN_BALANCE_AMT, 
    PERIOD_ACTIVITY_AMT,
    END_BALANCE_AMT, 
    CURRENCY_CODE,
    PERIOD_NUMBER,
    CODE_COMBINATION_ID,
    LAST_UPDATE_DATE, 
    LAST_UPDATED_BY,
    BZ_LOAD_DATE, 
    SV_LOAD_DATE
)
SELECT
    ISNULL(DA.ACCOUNT_SK,0)                          AS ACCOUNT_SK,
    ISNULL(DBO.BUSINESS_OFFERING_SK,0)               AS BUSINESS_OFFERING_SK,
    ISNULL(DCO.COMPANY_SK,0)                         AS COMPANY_SK,
    ISNULL(DCC.COST_CENTER_SK,0)                     AS COST_CENTER_SK,
    ISNULL(DI.INDUSTRY_SK,0)                         AS INDUSTRY_SK,
    ISNULL(DIC.INTERCOMPANY_SK,0)                    AS INTERCOMPANY_SK,
    ISNULL(LDG.LEDGER_SK,0)                          AS LEDGER_SK,
    ISNULL(CAST(CONVERT(CHAR(8), TRY_CONVERT(DATE, P.[PeriodStartDate]), 112) AS INT),0) AS BALANCE_DATE_SK,
    ISNULL(B.[BalanceBeginBalanceDr],0) - ISNULL(B.[BalanceBeginBalanceCr],0)            AS BEGIN_BALANCE_AMT,
    ISNULL(B.[BalancePeriodNetDr],0) - ISNULL(B.[BalancePeriodNetCr],0)                  AS PERIOD_ACTIVITY_AMT,
    ISNULL(B.[BalanceBeginBalanceDr],0) - ISNULL(B.[BalanceBeginBalanceCr],0) + ISNULL(B.[BalancePeriodNetDr],0) - ISNULL(B.[BalancePeriodNetCr],0) as END_BALANCE_AMT,
    ISNULL([BalanceCurrencyCode],'UNK')              AS CURRENCY_CODE,    
    [BalancePeriodNum]                               AS PERIOD_NUMBER,             
    [BalanceCodeCombinationId]                       AS CODE_COMBINATION_ID,
    [BalanceLastUpdateDate]                          AS LAST_UPDATED_DATE,
    [BalanceLastUpdatedBy]                           AS LAST_UPDATE_DATE,
    CAST(B.AddDateTime AS DATE)                      AS BZ_LOAD_DATE,
    CAST(GETDATE() AS DATE)                          AS SV_LOAD_DATE
FROM  [bzo].[GL_BalanceExtractPVO] AS B
LEFT JOIN [bzo].[GL_FiscalPeriodExtractPVO] P ON B.[BalancePeriodName] = P.[PeriodPeriodName]
LEFT JOIN [stage].[LINES_CODE_COMBO_LOOKUP] AS C ON CAST(B.[BalanceCodeCombinationId] AS BIGINT) = C.[CODE_COMBINATION_BK]
LEFT JOIN svo.D_ACCOUNT            AS DA  ON DA.ACCOUNT_ID            = C.ACCOUNT_ID
LEFT JOIN svo.D_BUSINESS_OFFERING  AS DBO ON DBO.BUSINESS_OFFERING_ID = C.BUSINESSOFFERING_ID
LEFT JOIN svo.D_COMPANY            AS DCO ON DCO.COMPANY_ID           = C.COMPANY_ID
LEFT JOIN svo.D_COST_CENTER        AS DCC ON DCC.COST_CENTER_ID       = C.COSTCENTER_ID
LEFT JOIN svo.D_INDUSTRY           AS DI  ON DI.INDUSTRY_ID           = C.INDUSTRY_ID
LEFT JOIN svo.D_INTERCOMPANY       AS DIC ON DIC.INTERCOMPANY_ID      = C.INTERCOMPANY_ID
LEFT JOIN svo.D_LEDGER             AS LDG ON LDG.LEDGER_ID            = B.BalanceLedgerId
WHERE [BalanceTranslatedFlag] <> 'R'  ; -- This is needed to remove rows that previously converted, including them causes dupes
GO