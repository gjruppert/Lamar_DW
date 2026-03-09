SELECT 
    'OM to RM Line Crosswalk' Q
,   OMH.HEADERID                                OM_HEADER_ID
,   OMH.HEADERORDERNUMBER                       OM_HEADER_ORDER_NUMBER
,   OMFLA.FULFILLLINEID                         OM_FULFILL_LINE_ID
,   RMSDL.SOURCEDOCLINESDOCLINEIDINT1           RM_SOURCE_DOC_LINE_ID
,   POBL.PerfObligationLinesDocumentLineId      RM_POBL_LINE_ID
,   CCH.CustomerContractHeadersContractGroupNumber
                                                RM_CONTRACT_GROUP_NUMBER
FROM 
    bzo.OM_HeaderExtractPVO                             OMH 
        INNER JOIN bzo.OM_LineExtractPVO                OML     ON OMH.HEADERID             =   OML.LINEHEADERID
        INNER JOIN bzo.OM_FulfillLineExtractPVO         OMFLA   ON OML.LINEID               =   OMFLA.FULFILLLINELINEID
                                                                    AND OML.LINEHEADERID    =   OMFLA.FULFILLLINEHEADERID
        INNER JOIN imp.VRM_SourceDocumentLinesPVO       RMSDL   ON OMFLA.FULFILLLINEID      =   RMSDL.SOURCEDOCLINESDOCLINEIDINT1
        LEFT JOIN bzo.VRM_PerfObligationLinesPVO        POBL    ON RMSDL.SOURCEDOCLINESDOCUMENTLINEID
                                                                                            =   POBL.PerfObligationLinesDocumentLineId
        LEFT JOIN bzo.VRM_PerfObligationsPVO            POB     ON POBL.PerfObligationLinesPerfObligationId
                                                                                            =   POB.PerfObligationId
        LEFT JOIN bzo.VRM_CustomerContractHeadersPVO    CCH     ON POB.PerfObligationsCustomerContractHeaderId
                                                                                            =   CCH.CustomerContractHeaderId
WHERE 1=1
    AND CCH.CustomerContractHeadersContractGroupNumber IS NOT NULL  --HOW DO SOME ORDER NUMBERS DON'T HAVE CONTRACTS??
ORDER BY
    CCH.CustomerContractHeadersContractGroupNumber