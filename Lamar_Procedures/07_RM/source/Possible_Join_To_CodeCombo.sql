SELECT
      L.PerfObligationLineId                                                                    AS PerfObligationLineId
    , CAST(ISNULL(L.PerfObligationLinesContrCurNetConsiderAmt,0) AS DECIMAL(29,4))              AS LineAmount
    , COALESCE(L.PerfObligationLinesRevenueStartDate, L.PerfObligationLinesRevenueEndDate,'1900-01-01') AS RevStart
    , COALESCE(L.PerfObligationLinesRevenueEndDate, L.PerfObligationLinesRevenueStartDate, '9999-12-31') AS RevEnd
    , DATEDIFF(DAY,
        COALESCE(L.PerfObligationLinesRevenueStartDate, L.PerfObligationLinesRevenueEndDate,'1900-01-01'),
        COALESCE(L.PerfObligationLinesRevenueEndDate, L.PerfObligationLinesRevenueStartDate, '9999-12-31')
      ) + 1                                                                                     AS BaseDays
    , ISNULL(L.SourceDocLinesDocumentLineId,-1)                                                 AS SourceDocLinesDocumentLineId
    , ISNULL(O.PerfObligationId,-1)                                                             AS PerfObligationId
    , ISNULL(C.CustomerContractHeaderId,-1)                                                     AS CustomerContractHeaderId
    , ISNULL(S.SourceDocumentsOrgId,-1)                                                         AS SourceDocumentsOrgId
    , ISNULL(P.SourceDocLinesBillToCustomerId,-1)                                               AS SourceDocLinesBillToCustomerId
    , ISNULL(P.SourceDocLinesBillToCustomerSiteId,-1)                                           AS SourceDocLinesBillToCustomerSiteId
    , ISNULL(C.CustContHeadersLedgerId,-1)                                                      AS CustContHeadersLedgerId
    , ISNULL(C.CustContHeadersLegalEntityId,-1)                                                 AS CustContHeadersLegalEntityId
    , ISNULL(C.CustContHeadersContractCurrencyCode,'Unk')                                       AS CustContHeadersContractCurrencyCode
    , ISNULL(C.CustomerContractHeadersContractGroupNumber,'')                                   AS CustomerContractHeadersContractGroupNumber
    , CAST(L.AddDateTime AS DATE)                                                               AS BZ_LOAD_DATE
    , CASE
          WHEN EXISTS (SELECT 1
                       FROM bzo.VRM_PolSatisfactionEventsPVO E
                       WHERE E.PolSatisfactionEventsPerfObligationLineId = L.PerfObligationLineId)
          THEN 'A' ELSE 'D'
      END                                                                                       AS ROW_TYPE
    , ISNULL(P.SourceDocLinesInventoryOrgId,-1)                                                 AS SourceDocLinesInventoryOrgId
    , ISNULL(P.SourceDocLinesItemId,-1)                                                         AS SourceDocLinesItemId

    -- NEW: Revenue account CCID
    , RevDist.PerfObligationLinDistsEOCodeCombinationId                                                               AS RevenueCodeCombinationId
    , RevDist.PerfObligationLinDistsEOConcatenatedSegments                                                             AS RevenueConcatenatedSegments
FROM bzo.VRM_PerfObligationLinesPVO            AS L
LEFT JOIN bzo.VRM_PerfObligationsPVO           AS O  ON O.PerfObligationId = L.PerfObligationLinesPerfObligationId
LEFT JOIN bzo.VRM_CustomerContractHeadersPVO   AS C  ON C.CustomerContractHeaderId = O.PerfObligationsCustomerContractHeaderId
LEFT JOIN bzo.VRM_SourceDocumentLinesPVO       AS S  ON S.SourceDocLinesDocumentLineId = L.SourceDocLinesDocumentLineId
LEFT JOIN bzo.VRM_SourceDocLinePricingLinesPVO AS P  ON P.SourceDocLinesDocumentLineId = L.SourceDocLinesDocumentLineId
OUTER APPLY
(
    SELECT TOP (1)
           D.PerfObligationLinDistsEOCodeCombinationId,
           D.PerfObligationLinDistsEOConcatenatedSegments
    FROM bzo.VRM_PerfObligationLinDistsPVO D
    WHERE D.PerfObligationLineDistId = L.PerfObligationLineId
      AND D.PerfObligationLinDistsEOAccountClass = 'ORA_REV'   -- validate actual value in your data (see check below)
    ORDER BY D.PerfObligationLinDistsEOAccountingDate DESC, D.PerfObligationLineDistId DESC
) AS RevDist;