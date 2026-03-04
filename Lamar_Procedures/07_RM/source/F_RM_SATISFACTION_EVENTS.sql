IF OBJECT_ID('svo.F_RM_SATISFACTION_EVENTS','U') IS NOT NULL
    DROP TABLE svo.F_RM_SATISFACTION_EVENTS;
GO

SET ANSI_NULLS ON;
GO
SET QUOTED_IDENTIFIER ON;
GO

CREATE TABLE [svo].[F_RM_SATISFACTION_EVENTS]
(
    RM_SATISFACTION_EVENT_FACT_PK      BIGINT IDENTITY(1,1) NOT NULL,

    -- RM subject-area dimensions
    RM_SATISFACTION_EVENT_SK           BIGINT NOT NULL,
    RM_PERF_OBLIGATION_LINE_SK         BIGINT NOT NULL,
    RM_PERF_OBLIGATION_SK              BIGINT NOT NULL,
    RM_CONTRACT_SK                     BIGINT NOT NULL,
    RM_SOURCE_DOCUMENT_LINE_SK         BIGINT NOT NULL,
    RM_SOURCE_DOC_PRICING_LINE_SK      BIGINT NOT NULL,

    -- Conformed dimensions
    CUSTOMER_SK                        BIGINT NOT NULL,
    CUSTOMER_SITE_SK                   BIGINT NOT NULL,
    BUSINESS_UNIT_SK                   BIGINT NOT NULL,
    LEDGER_SK                          BIGINT NOT NULL,
    LEGAL_ENTITY_SK                    BIGINT NOT NULL,
    CURRENCY_SK                        BIGINT NOT NULL,
    ITEM_SK                            BIGINT NOT NULL,
    ORDER_HEADER_SK                       BIGINT NOT NULL,   -- D_OM_ORDER_HEADER via HeaderSourceOrderNumber = CustomerContractHeadersContractGroupNumber

    -- Date dimensions (SK, not KEY)
    SATISFACTION_MEASUREMENT_DATE_SK   INT    NOT NULL,
    SATISFACTION_PERIOD_START_DATE_SK  INT    NOT NULL,
    SATISFACTION_PERIOD_END_DATE_SK    INT    NOT NULL,
    EVENT_CREATION_DATE_SK             INT    NOT NULL,
    EVENT_LAST_UPDATE_DATE_SK          INT    NOT NULL,

    -- Measures
    SATISFACTION_MEASUREMENT_NUMBER    INT              NULL,
    SATISFACTION_DAYS_IN_PERIOD        INT              NULL,
    SATISFACTION_PERIOD_PROPORTION     DECIMAL(29,4)    NULL,
    SATISFACTION_PERCENT               DECIMAL(29,4)    NULL,
    SATISFACTION_QUANTITY              DECIMAL(29,4)    NULL,
    SATISFACTION_AMOUNT                DECIMAL(29,4)    NULL,

    BZ_LOAD_DATE                       DATE   NOT NULL,
    SV_LOAD_DATE                       DATE   NOT NULL,

    ROW_TYPE                           CHAR(1) NOT NULL,  -- 'A' actual, 'D' derived

    CONSTRAINT PK_F_RM_SATISFACTION_EVENTS
        PRIMARY KEY CLUSTERED (RM_SATISFACTION_EVENT_FACT_PK ASC)
) ON [FG_SilverFact];
GO

-- Common analysis path: by contract/obligation/event & date
CREATE NONCLUSTERED INDEX IX_F_RM_SAT_EVENTS_CONTRACT
ON svo.F_RM_SATISFACTION_EVENTS
(
    RM_CONTRACT_SK,
    RM_PERF_OBLIGATION_SK,
    RM_PERF_OBLIGATION_LINE_SK,
    RM_SATISFACTION_EVENT_SK,
    SATISFACTION_MEASUREMENT_DATE_SK
)
ON [FG_SilverFact];
GO

-- Business path: by customer / BU / satisfaction date
CREATE NONCLUSTERED INDEX IX_F_RM_SAT_EVENTS_CUST_BU
ON svo.F_RM_SATISFACTION_EVENTS
(
    CUSTOMER_SK,
    BUSINESS_UNIT_SK,
    SATISFACTION_MEASUREMENT_DATE_SK
)
ON [FG_SilverFact];
GO

 ;WITH BaseLines AS
(
    SELECT
          L.PerfObligationLineId                                                                    AS PerfObligationLineId
        , CAST(L.PerfObligationLinesContrCurNetConsiderAmt AS DECIMAL(29,4))                        AS LineAmount
        , COALESCE(L.PerfObligationLinesRevenueStartDate, L.PerfObligationLinesRevenueEndDate,'0001-01-01')   AS RevStart
        , COALESCE(L.PerfObligationLinesRevenueEndDate,L.PerfObligationLinesRevenueStartDate, '0001-01-01')   AS RevEnd
        , DATEDIFF(DAY, COALESCE(L.PerfObligationLinesRevenueStartDate, L.PerfObligationLinesRevenueEndDate,'0001-01-01'),
                        COALESCE(L.PerfObligationLinesRevenueEndDate,L.PerfObligationLinesRevenueStartDate, '0001-01-01')) + 1    AS BaseDays
        , ISNULL(L.SourceDocLinesDocumentLineId,-1)                      AS SourceDocLinesDocumentLineId
        , ISNULL(O.PerfObligationId,-1)                                  AS PerfObligationId
        , ISNULL(C.CustomerContractHeaderId,-1)                          AS CustomerContractHeaderId
        , ISNULL(S.SourceDocumentsOrgId,-1)                              AS SourceDocumentsOrgId
        , ISNULL(P.SourceDocLinesBillToCustomerId,-1)                    AS SourceDocLinesBillToCustomerId
        , ISNULL(P.SourceDocLinesBillToCustomerSiteId,-1)                AS SourceDocLinesBillToCustomerSiteId
        , ISNULL(C.CustContHeadersLedgerId,-1)                           AS CustContHeadersLedgerId
        , ISNULL(C.CustContHeadersLegalEntityId,-1)                      AS CustContHeadersLegalEntityId
        , ISNULL(C.CustContHeadersContractCurrencyCode,'UNK')            AS CustContHeadersContractCurrencyCode
        , CAST(L.AddDateTime AS DATE)                                    AS BZ_LOAD_DATE

        , CASE
              WHEN EXISTS
                   (
                       SELECT 1
                       FROM bzo.VRM_PolSatisfactionEventsPVO E
                       WHERE E.PolSatisfactionEventsPerfObligationLineId = L.PerfObligationLineId
                   )
              THEN 'A'
              ELSE 'D'
          END                                               AS ROW_TYPE
        , ISNULL(P.SourceDocLinesInventoryOrgId,-1)         AS SourceDocLinesInventoryOrgId
        , ISNULL(P.SourceDocLinesItemId,-1)                 AS SourceDocLinesItemId
    FROM bzo.VRM_PerfObligationLinesPVO            AS L
    LEFT JOIN bzo.VRM_PerfObligationsPVO           AS O  ON O.PerfObligationId = L.PerfObligationLinesPerfObligationId
    LEFT JOIN bzo.VRM_CustomerContractHeadersPVO   AS C  ON C.CustomerContractHeaderId = O.PerfObligationsCustomerContractHeaderId
    LEFT JOIN bzo.VRM_SourceDocumentLinesPVO       AS S  ON S.SourceDocLinesDocumentLineId = L.SourceDocLinesDocumentLineId
    LEFT JOIN bzo.VRM_SourceDocLinePricingLinesPVO AS P  ON P.SourceDocLinesDocumentLineId = L.SourceDocLinesDocumentLineId

    -- Optional filter for testing a single line:
     --WHERE L.PerfObligationLineId = 19169
),
Segments AS
(
    -- First segment for each line
    SELECT
          b.PerfObligationLineId
        , b.LineAmount
        , b.RevStart
        , b.RevEnd
        , b.BaseDays
        , b.SourceDocLinesDocumentLineId
        , b.PerfObligationId
        , b.CustomerContractHeaderId
        , b.SourceDocumentsOrgId
        , b.SourceDocLinesBillToCustomerId
        , b.SourceDocLinesBillToCustomerSiteId
        , b.CustContHeadersLedgerId
        , b.CustContHeadersLegalEntityId
        , b.CustContHeadersContractCurrencyCode
        , b.BZ_LOAD_DATE
        , b.ROW_TYPE
        , b.SourceDocLinesInventoryOrgId
        , b.SourceDocLinesItemId
        , CAST(b.RevStart AS date) AS SegmentStart
        , CAST(CASE WHEN EOMONTH(b.RevStart) < b.RevEnd THEN EOMONTH(b.RevStart) ELSE b.RevEnd END AS DATE) AS SegmentEnd
     FROM BaseLines b

    UNION ALL

    -- Subsequent segments
    SELECT
          s.PerfObligationLineId
        , s.LineAmount
        , s.RevStart
        , s.RevEnd
        , s.BaseDays
        , s.SourceDocLinesDocumentLineId
        , s.PerfObligationId
        , s.CustomerContractHeaderId
        , s.SourceDocumentsOrgId
        , s.SourceDocLinesBillToCustomerId
        , s.SourceDocLinesBillToCustomerSiteId
        , s.CustContHeadersLedgerId
        , s.CustContHeadersLegalEntityId
        , s.CustContHeadersContractCurrencyCode
        , s.BZ_LOAD_DATE
        , s.ROW_TYPE
        , s.SourceDocLinesInventoryOrgId
        , s.SourceDocLinesItemId
        , CAST(DATEADD(DAY, 1, s.SegmentEnd) AS date) AS SegmentStart
        , CAST(CASE WHEN EOMONTH(DATEADD(DAY, 1, s.SegmentEnd)) < s.RevEnd THEN EOMONTH(DATEADD(DAY, 1, s.SegmentEnd)) ELSE s.RevEnd END AS DATE) AS SegmentEnd
 
    FROM Segments s
    WHERE DATEADD(DAY, 1, s.SegmentEnd) <= s.RevEnd
),
Calc AS
(
    SELECT
          s.*
        , DATEDIFF(DAY, s.SegmentStart, s.SegmentEnd) + 1    AS DaysInPeriod

        , CAST(
              s.LineAmount
              * CAST(DATEDIFF(DAY, s.SegmentStart, s.SegmentEnd) + 1 AS DECIMAL(29,4))
              / NULLIF(CAST(s.BaseDays AS DECIMAL(29,4)), 0)
              AS DECIMAL(29,4)
          )                                                   AS AmountRaw

        , ROW_NUMBER() OVER
              (PARTITION BY s.PerfObligationLineId
               ORDER BY s.SegmentStart, s.SegmentEnd)         AS rn

        , COUNT(*) OVER
              (PARTITION BY s.PerfObligationLineId)           AS cnt

        , SUM(
              CAST(
                  s.LineAmount
                  * CAST(DATEDIFF(DAY, s.SegmentStart, s.SegmentEnd) + 1 AS DECIMAL(29,4))
                  / NULLIF(CAST(s.BaseDays AS DECIMAL(29,4)), 0)
                  AS DECIMAL(29,4)
              )
          ) OVER
              (
                  PARTITION BY s.PerfObligationLineId
                  ORDER BY s.SegmentStart, s.SegmentEnd
                  ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
              )                                               AS SumPrevAmt
    FROM Segments s
),
FinalRows AS
(
    SELECT

        -- RM subject-area SKs
        ISNULL(SE.RM_SATISFACTION_EVENT_SK, 0)             AS RM_SATISFACTION_EVENT_SK,
        ISNULL(LD.RM_PERF_OBLIGATION_LINE_SK, 0)           AS RM_PERF_OBLIGATION_LINE_SK,
        ISNULL(OD.RM_PERF_OBLIGATION_SK, 0)                AS RM_PERF_OBLIGATION_SK,
        ISNULL(CD.RM_CONTRACT_SK, 0)                       AS RM_CONTRACT_SK,
        ISNULL(SD.RM_SOURCE_DOCUMENT_LINE_SK, 0)           AS RM_SOURCE_DOCUMENT_LINE_SK,
        ISNULL(PD.RM_SOURCE_DOC_PRICING_LINE_SK, 0)        AS RM_SOURCE_DOC_PRICING_LINE_SK,

        -- Conformed dims
        ISNULL(CUST.CUSTOMER_SK, 0)                        AS CUSTOMER_SK,
        ISNULL(PS.CUSTOMER_SITE_SK, 0)                     AS CUSTOMER_SITE_SK,
        ISNULL(BU.BUSINESS_UNIT_SK, 0)                     AS BUSINESS_UNIT_SK,
        ISNULL(LDG.LEDGER_SK, 0)                           AS LEDGER_SK,
        ISNULL(LE.LEGAL_ENTITY_SK, 0)                      AS LEGAL_ENTITY_SK,
        ISNULL(CUR.CURRENCY_SK, 0)                         AS CURRENCY_SK,
        ISNULL(ITM.ITEM_SK,0)                              AS ITEM_SK, 
        -- Date SKs (use segment end for measurement/creation/update)
        COALESCE(CONVERT(INT, CONVERT(CHAR(8), c.SegmentEnd, 112)), 10101)   AS SATISFACTION_MEASUREMENT_DATE_SK,
        COALESCE(CONVERT(INT, CONVERT(CHAR(8), c.SegmentStart, 112)), 10101) AS SATISFACTION_PERIOD_START_DATE_SK,
        COALESCE(CONVERT(INT, CONVERT(CHAR(8), c.SegmentEnd, 112)), 10101)   AS SATISFACTION_PERIOD_END_DATE_SK,
        COALESCE(CONVERT(INT, CONVERT(CHAR(8), c.SegmentEnd, 112)), 10101)   AS EVENT_CREATION_DATE_SK,
        COALESCE(CONVERT(INT, CONVERT(CHAR(8), c.SegmentEnd, 112)), 10101)   AS EVENT_LAST_UPDATE_DATE_SK,
        1                                                                    AS SATISFACTION_MEASUREMENT_NUMBER,
        c.DaysInPeriod                                                       AS SATISFACTION_DAYS_IN_PERIOD,

        -- Final amount for this segment (fix rounding on last segment)
        CAST(CASE WHEN c.rn = c.cnt THEN c.LineAmount - ISNULL(c.SumPrevAmt, 0) ELSE c.AmountRaw END AS DECIMAL(29,4)) AS SATISFACTION_AMOUNT,

        -- Proportion = Amount / LineAmount
        CAST(CASE WHEN c.LineAmount = 0 
                  THEN 0 
                  ELSE (
                        CASE WHEN c.rn = c.cnt 
                             THEN c.LineAmount - ISNULL(c.SumPrevAmt, 0) 
                             ELSE c.AmountRaw 
                         END) / NULLIF(c.LineAmount, 0) END AS DECIMAL(29,4) )   AS SATISFACTION_PERIOD_PROPORTION,

        CAST(CASE WHEN c.LineAmount = 0 
                  THEN 0 
                  ELSE (  
                        CASE WHEN c.rn = c.cnt 
                        THEN c.LineAmount - ISNULL(c.SumPrevAmt, 0)
                        ELSE c.AmountRaw
                        END) / NULLIF(c.LineAmount, 0)  END * 100.0 AS DECIMAL(29,4) ) AS SATISFACTION_PERCENT,
        CAST(0.0 AS DECIMAL(29,4))                       AS SATISFACTION_QUANTITY,
        c.BZ_LOAD_DATE                                   AS BZ_LOAD_DATE,
        CAST(GETDATE() AS DATE)                          AS SV_LOAD_DATE,
        c.ROW_TYPE                                       AS ROW_TYPE
    FROM Calc c

    -- RM dims
    LEFT JOIN svo.D_RM_PERF_OBLIGATION_LINE    AS LD  ON LD.PERF_OBLIGATION_LINE_ID     = c.PerfObligationLineId
    LEFT JOIN svo.D_RM_PERF_OBLIGATION         AS OD  ON OD.PERF_OBLIGATION_ID          = c.PerfObligationId
    LEFT JOIN svo.D_RM_CONTRACT                AS CD  ON CD.CUSTOMER_CONTRACT_HEADER_ID = c.CustomerContractHeaderId
    LEFT JOIN svo.D_RM_SOURCE_DOCUMENT_LINE    AS SD  ON SD.SOURCE_DOCUMENT_LINE_ID     = c.SourceDocLinesDocumentLineId
    LEFT JOIN svo.D_RM_SOURCE_DOC_PRICING_LINE AS PD  ON PD.SOURCE_DOCUMENT_LINE_ID     = c.SourceDocLinesDocumentLineId
    LEFT JOIN svo.D_RM_SATISFACTION_EVENT      AS SE  ON SE.POL_SATISFACTION_EVENT_ID   = c.PerfObligationLineId

    -- Conformed dims
    LEFT JOIN svo.D_CUSTOMER_ACCOUNT      AS CUST ON CUST.CUSTOMER_ACCOUNT_ID = c.SourceDocLinesBillToCustomerId
    LEFT JOIN svo.D_CUSTOMER_ACCOUNT_SITE AS PS   ON PS.CUSTOMER_SITE         = c.SourceDocLinesBillToCustomerSiteId
    LEFT JOIN svo.D_BUSINESS_UNIT         AS BU   ON BU.BUSINESS_UNIT_ID      = c.SourceDocumentsOrgId
    LEFT JOIN svo.D_LEDGER                AS LDG  ON LDG.LEDGER_ID            = c.CustContHeadersLedgerId
    LEFT JOIN svo.D_LEGAL_ENTITY          AS LE   ON LE.LEGAL_ENTITY_ID       = c.CustContHeadersLegalEntityId
    LEFT JOIN svo.D_CURRENCY              AS CUR  ON CUR.CURRENCY_ID          = c.CustContHeadersContractCurrencyCode
    LEFT JOIN svo.D_ITEM                  AS ITM  ON ITM.ITEM_ORG_ID          = c.SourceDocLinesInventoryOrgId AND ITM.INVENTORY_ITEM_ID = c.SourceDocLinesItemId 

)

-- =================================================================
-- FINAL INSERT
-- =================================================================
INSERT INTO svo.F_RM_SATISFACTION_EVENTS
(
    RM_SATISFACTION_EVENT_SK,
    RM_PERF_OBLIGATION_LINE_SK,
    RM_PERF_OBLIGATION_SK,
    RM_CONTRACT_SK,
    RM_SOURCE_DOCUMENT_LINE_SK,
    RM_SOURCE_DOC_PRICING_LINE_SK,
    CUSTOMER_SK,
    CUSTOMER_SITE_SK,
    BUSINESS_UNIT_SK,
    LEDGER_SK,
    LEGAL_ENTITY_SK,
    CURRENCY_SK,
    ITEM_SK, 
    SATISFACTION_MEASUREMENT_DATE_SK,
    SATISFACTION_PERIOD_START_DATE_SK,
    SATISFACTION_PERIOD_END_DATE_SK,
    EVENT_CREATION_DATE_SK,
    EVENT_LAST_UPDATE_DATE_SK,
    SATISFACTION_MEASUREMENT_NUMBER,
    SATISFACTION_DAYS_IN_PERIOD,
    SATISFACTION_PERIOD_PROPORTION,
    SATISFACTION_PERCENT,
    SATISFACTION_QUANTITY,
    SATISFACTION_AMOUNT,
    BZ_LOAD_DATE,
    SV_LOAD_DATE,
    ROW_TYPE
)
SELECT

      RM_SATISFACTION_EVENT_SK
    , RM_PERF_OBLIGATION_LINE_SK
    , RM_PERF_OBLIGATION_SK
    , RM_CONTRACT_SK
    , RM_SOURCE_DOCUMENT_LINE_SK
    , RM_SOURCE_DOC_PRICING_LINE_SK
    , CUSTOMER_SK
    , CUSTOMER_SITE_SK
    , BUSINESS_UNIT_SK
    , LEDGER_SK
    , LEGAL_ENTITY_SK
    , CURRENCY_SK
    , ITEM_SK 
    , SATISFACTION_MEASUREMENT_DATE_SK
    , SATISFACTION_PERIOD_START_DATE_SK
    , SATISFACTION_PERIOD_END_DATE_SK
    , EVENT_CREATION_DATE_SK
    , EVENT_LAST_UPDATE_DATE_SK
    , SATISFACTION_MEASUREMENT_NUMBER
    , SATISFACTION_DAYS_IN_PERIOD
    , SATISFACTION_PERIOD_PROPORTION
    , SATISFACTION_PERCENT
    , SATISFACTION_QUANTITY
    , SATISFACTION_AMOUNT
    , BZ_LOAD_DATE
    , SV_LOAD_DATE
    , ROW_TYPE
FROM FinalRows
OPTION (MAXRECURSION 32767);
GO