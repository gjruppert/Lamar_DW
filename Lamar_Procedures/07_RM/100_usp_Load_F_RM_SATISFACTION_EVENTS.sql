/* =========================================================
   usp_Load_F_RM_SATISFACTION_EVENTS
   Full refresh. Source: CTE from bzo.VRM_PerfObligationLinesPVO + joins to RM and conformed dims.
   TRUNCATE then INSERT. ETL_RUN logged; watermark set on success for audit. Run after all D_RM_* dimensions.
   ========================================================= */
CREATE OR ALTER PROCEDURE svo.usp_Load_F_RM_SATISFACTION_EVENTS
    @BatchId INT = NULL
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    DECLARE
        @ProcName       SYSNAME        = OBJECT_SCHEMA_NAME(@@PROCID) + '.' + OBJECT_NAME(@@PROCID),
        @TargetObject   SYSNAME        = 'svo.F_RM_SATISFACTION_EVENTS',
        @StartDttm      DATETIME2(0)   = SYSDATETIME(),
        @EndDttm        DATETIME2(0),
        @RunId          BIGINT         = NULL,
        @ErrMsg         NVARCHAR(4000) = NULL,
        @AsOfDate       DATE           = CAST(GETDATE() AS DATE),
        @RowInserted    INT            = 0,
        @TableBridgeID  INT            = NULL;

    SELECT @TableBridgeID = TableBridgeID FROM meta.MedallionTableBridge WHERE targettable = N'VRM_PerfObligationLinesPVO';

    BEGIN TRY
        INSERT INTO etl.ETL_RUN (PROC_NAME, TARGET_OBJECT, ASOF_DATE, START_DTTM, STATUS, BATCH_ID, TABLE_BRIDGE_ID)
        VALUES (@ProcName, @TargetObject, @AsOfDate, @StartDttm, 'STARTED', ISNULL(@BatchId, -1), ISNULL(@TableBridgeID, -1));

        SET @RunId = SCOPE_IDENTITY();
        IF @RunId IS NULL
            THROW 50001, 'ETL_RUN insert failed: RUN_ID is NULL.', 1;

        TRUNCATE TABLE svo.F_RM_SATISFACTION_EVENTS;

        ;WITH
        d(n) AS (SELECT 0 UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4 UNION ALL SELECT 5 UNION ALL SELECT 6 UNION ALL SELECT 7 UNION ALL SELECT 8 UNION ALL SELECT 9),
        N AS (
            SELECT (d1.n * 100 + d2.n * 10 + d3.n) AS n
            FROM d d1 CROSS JOIN d d2 CROSS JOIN d d3
            WHERE (d1.n * 100 + d2.n * 10 + d3.n) < 1200
        ),
        BaseLines AS
        (
            SELECT
                  L.PerfObligationLineId                                                                    AS PerfObligationLineId
                , CAST(L.PerfObligationLinesContrCurNetConsiderAmt AS DECIMAL(29,4))                        AS LineAmount
                , COALESCE(L.PerfObligationLinesRevenueStartDate, L.PerfObligationLinesRevenueEndDate,'1900-01-01')   AS RevStart
                , COALESCE(L.PerfObligationLinesRevenueEndDate,L.PerfObligationLinesRevenueStartDate, '1900-01-01')   AS RevEnd
                , DATEDIFF(DAY, COALESCE(L.PerfObligationLinesRevenueStartDate, L.PerfObligationLinesRevenueEndDate,'1900-01-01'),
                                COALESCE(L.PerfObligationLinesRevenueEndDate,L.PerfObligationLinesRevenueStartDate, '1900-01-01')) + 1    AS BaseDays
                , ISNULL(L.SourceDocLinesDocumentLineId,-1)                      AS SourceDocLinesDocumentLineId
                , ISNULL(O.PerfObligationId,-1)                                  AS PerfObligationId
                , ISNULL(C.CustomerContractHeaderId,-1)                          AS CustomerContractHeaderId
                , ISNULL(S.SourceDocumentsOrgId,-1)                              AS SourceDocumentsOrgId
                , ISNULL(P.SourceDocLinesBillToCustomerId,-1)                    AS SourceDocLinesBillToCustomerId
                , ISNULL(P.SourceDocLinesBillToCustomerSiteId,-1)                AS SourceDocLinesBillToCustomerSiteId
                , ISNULL(C.CustContHeadersLedgerId,-1)                           AS CustContHeadersLedgerId
                , ISNULL(C.CustContHeadersLegalEntityId,-1)                      AS CustContHeadersLegalEntityId
                , ISNULL(C.CustContHeadersContractCurrencyCode,'Unk')            AS CustContHeadersContractCurrencyCode
                , ISNULL(C.CustomerContractHeadersContractGroupNumber,'')        AS CustomerContractHeadersContractGroupNumber
                , CAST(L.AddDateTime AS DATETIME2(0))                            AS BZ_LOAD_DATE
                , CASE
                      WHEN EXISTS (SELECT 1 FROM bzo.VRM_PolSatisfactionEventsPVO E WHERE E.PolSatisfactionEventsPerfObligationLineId = L.PerfObligationLineId)
                      THEN 'A' ELSE 'D'
                  END                                                           AS ROW_TYPE
                , ISNULL(P.SourceDocLinesInventoryOrgId,-1)                     AS SourceDocLinesInventoryOrgId
                , ISNULL(P.SourceDocLinesItemId,-1)                             AS SourceDocLinesItemId
                , ISNULL(OML.LineId, -1)                                        AS LineId
                , COALESCE(OML.LineHeaderId,S.SourceDocLinesDocLineIdInt1, -1)  AS LineHeaderId
            FROM bzo.VRM_PerfObligationLinesPVO            AS L
            LEFT JOIN bzo.VRM_PerfObligationsPVO           AS O  ON O.PerfObligationId = L.PerfObligationLinesPerfObligationId
            LEFT JOIN bzo.VRM_CustomerContractHeadersPVO   AS C  ON C.CustomerContractHeaderId = O.PerfObligationsCustomerContractHeaderId
            LEFT JOIN bzo.VRM_SourceDocumentLinesPVO       AS S  ON S.SourceDocLinesDocumentLineId = L.SourceDocLinesDocumentLineId
            LEFT JOIN bzo.VRM_SourceDocLinePricingLinesPVO AS P  ON P.SourceDocLinesDocumentLineId = L.SourceDocLinesDocumentLineId
            LEFT JOIN bzo.OM_FulfillLineExtractPVO        AS FL ON FL.FulfillLineId = S.SourceDocLinesDocLineIdInt1
            LEFT JOIN bzo.OM_LineExtractPVO               AS OML ON OML.LineId = FL.FulfillLineLineId
        ),
        Segments AS
        (
            SELECT b.PerfObligationLineId, b.LineAmount, b.RevStart, b.RevEnd, b.BaseDays, b.SourceDocLinesDocumentLineId, b.PerfObligationId, 
                  b.CustomerContractHeaderId, b.SourceDocumentsOrgId, b.SourceDocLinesBillToCustomerId, b.SourceDocLinesBillToCustomerSiteId, 
                  b.CustContHeadersLedgerId, b.CustContHeadersLegalEntityId, b.CustContHeadersContractCurrencyCode, b.CustomerContractHeadersContractGroupNumber, 
                  b.BZ_LOAD_DATE, b.ROW_TYPE, b.SourceDocLinesInventoryOrgId, b.SourceDocLinesItemId, b.LineId, b.LineHeaderId, 
                   CAST(CASE WHEN n.n = 0 THEN b.RevStart ELSE DATEADD(MONTH, n.n, DATEFROMPARTS(YEAR(b.RevStart), MONTH(b.RevStart), 1)) END AS DATE) AS SegmentStart,
                   CAST(CASE
                        WHEN n.n = 0 THEN CASE WHEN EOMONTH(b.RevStart) < b.RevEnd THEN EOMONTH(b.RevStart) ELSE b.RevEnd END
                        ELSE CASE WHEN EOMONTH(DATEADD(MONTH, n.n, DATEFROMPARTS(YEAR(b.RevStart), MONTH(b.RevStart), 1))) < b.RevEnd THEN EOMONTH(DATEADD(MONTH, n.n, DATEFROMPARTS(YEAR(b.RevStart), MONTH(b.RevStart), 1))) ELSE b.RevEnd END
                    END AS DATE) AS SegmentEnd
            FROM BaseLines b
            INNER JOIN N n ON (CASE WHEN n.n = 0 THEN b.RevStart ELSE DATEADD(MONTH, n.n, DATEFROMPARTS(YEAR(b.RevStart), MONTH(b.RevStart), 1)) END) <= b.RevEnd
        ),
        Calc AS
        (
            SELECT s.*,
                   DATEDIFF(DAY, s.SegmentStart, s.SegmentEnd) + 1 AS DaysInPeriod,
                   CAST(s.LineAmount * CAST(DATEDIFF(DAY, s.SegmentStart, s.SegmentEnd) + 1 AS DECIMAL(29,4)) / NULLIF(CAST(s.BaseDays AS DECIMAL(29,4)), 0) AS DECIMAL(29,4)) AS AmountRaw,
                   ROW_NUMBER() OVER (PARTITION BY s.PerfObligationLineId ORDER BY s.SegmentStart, s.SegmentEnd) AS rn,
                   COUNT(*) OVER (PARTITION BY s.PerfObligationLineId) AS cnt,
                   SUM(CAST(s.LineAmount * CAST(DATEDIFF(DAY, s.SegmentStart, s.SegmentEnd) + 1 AS DECIMAL(29,4)) / NULLIF(CAST(s.BaseDays AS DECIMAL(29,4)), 0) AS DECIMAL(29,4))) OVER (PARTITION BY s.PerfObligationLineId ORDER BY s.SegmentStart, s.SegmentEnd ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS SumPrevAmt
            FROM Segments s
        ),
        FinalRows AS
        (
            SELECT
                ISNULL(SE.RM_SATISFACTION_EVENT_SK, 0)             AS RM_SATISFACTION_EVENT_SK,
                ISNULL(LD.RM_PERF_OBLIGATION_LINE_SK, 0)           AS RM_PERF_OBLIGATION_LINE_SK,
                ISNULL(OD.RM_PERF_OBLIGATION_SK, 0)                AS RM_PERF_OBLIGATION_SK,
                ISNULL(CD.RM_CONTRACT_SK, 0)                       AS RM_CONTRACT_SK,
                ISNULL(SD.RM_SOURCE_DOCUMENT_LINE_SK, 0)           AS RM_SOURCE_DOCUMENT_LINE_SK,
                ISNULL(PD.RM_SOURCE_DOC_PRICING_LINE_SK, 0)        AS RM_SOURCE_DOC_PRICING_LINE_SK,
                ISNULL(CUST.CUSTOMER_SK, 0)                        AS CUSTOMER_SK,
                ISNULL(PS.CUSTOMER_SITE_SK, 0)                     AS CUSTOMER_SITE_SK,
                ISNULL(BU.BUSINESS_UNIT_SK, 0)                     AS BUSINESS_UNIT_SK,
                ISNULL(LDG.LEDGER_SK, 0)                           AS LEDGER_SK,
                ISNULL(LE.LEGAL_ENTITY_SK, 0)                      AS LEGAL_ENTITY_SK,
                ISNULL(CUR.CURRENCY_SK, 0)                         AS CURRENCY_SK,
                ISNULL(ITM.ITEM_SK,0)                              AS ITEM_SK,
                ISNULL(OH.ORDER_HEADER_SK, 0)                      AS ORDER_HEADER_SK,
                ISNULL(OL.ORDER_LINE_SK, 0)                        AS ORDER_LINE_SK,
                COALESCE(CONVERT(INT, CONVERT(CHAR(8), c.SegmentEnd, 112)), 10101)   AS SATISFACTION_MEASUREMENT_DATE_SK,
                COALESCE(CONVERT(INT, CONVERT(CHAR(8), c.SegmentStart, 112)), 10101) AS SATISFACTION_PERIOD_START_DATE_SK,
                COALESCE(CONVERT(INT, CONVERT(CHAR(8), c.SegmentEnd, 112)), 10101)   AS SATISFACTION_PERIOD_END_DATE_SK,
                COALESCE(CONVERT(INT, CONVERT(CHAR(8), c.SegmentEnd, 112)), 10101)   AS EVENT_CREATION_DATE_SK,
                COALESCE(CONVERT(INT, CONVERT(CHAR(8), c.SegmentEnd, 112)), 10101)   AS EVENT_LAST_UPDATE_DATE_SK,
                1                                                  AS SATISFACTION_MEASUREMENT_NUMBER,
                c.DaysInPeriod                                     AS SATISFACTION_DAYS_IN_PERIOD,
                CAST(CASE WHEN c.rn = c.cnt THEN c.LineAmount - ISNULL(c.SumPrevAmt, 0) ELSE c.AmountRaw END AS DECIMAL(29,4)) AS SATISFACTION_AMOUNT,
                CAST(CASE WHEN c.LineAmount = 0 THEN 0 ELSE (CASE WHEN c.rn = c.cnt THEN c.LineAmount - ISNULL(c.SumPrevAmt, 0) ELSE c.AmountRaw END) / NULLIF(c.LineAmount, 0) END AS DECIMAL(29,4)) AS SATISFACTION_PERIOD_PROPORTION,
                CAST(CASE WHEN c.LineAmount = 0 THEN 0 ELSE (CASE WHEN c.rn = c.cnt THEN c.LineAmount - ISNULL(c.SumPrevAmt, 0) ELSE c.AmountRaw END) / NULLIF(c.LineAmount, 0) END * 100.0 AS DECIMAL(29,4)) AS SATISFACTION_PERCENT,
                CAST(0.0 AS DECIMAL(29,4))                         AS SATISFACTION_QUANTITY,
                c.BZ_LOAD_DATE                                     AS BZ_LOAD_DATE,
                SYSDATETIME()                                       AS SV_LOAD_DATE,
                c.ROW_TYPE                                         AS ROW_TYPE
            FROM Calc c
            LEFT JOIN svo.D_RM_PERF_OBLIGATION_LINE    AS LD  ON LD.PERF_OBLIGATION_LINE_ID     = c.PerfObligationLineId
            LEFT JOIN svo.D_RM_PERF_OBLIGATION         AS OD  ON OD.PERF_OBLIGATION_ID          = c.PerfObligationId
            LEFT JOIN svo.D_RM_CONTRACT                AS CD  ON CD.CUSTOMER_CONTRACT_HEADER_ID = c.CustomerContractHeaderId
            LEFT JOIN svo.D_RM_SOURCE_DOCUMENT_LINE    AS SD  ON SD.SOURCE_DOCUMENT_LINE_ID     = c.SourceDocLinesDocumentLineId
            LEFT JOIN svo.D_RM_SOURCE_DOC_PRICING_LINE AS PD  ON PD.SOURCE_DOCUMENT_LINE_ID     = c.SourceDocLinesDocumentLineId
            LEFT JOIN svo.D_RM_SATISFACTION_EVENT      AS SE  ON SE.POL_SATISFACTION_EVENT_ID   = c.PerfObligationLineId
            LEFT JOIN svo.D_CUSTOMER_ACCOUNT      AS CUST ON CUST.CUSTOMER_ACCOUNT_ID = c.SourceDocLinesBillToCustomerId
            LEFT JOIN svo.D_CUSTOMER_ACCOUNT_SITE AS PS   ON PS.CUSTOMER_SITE         = c.SourceDocLinesBillToCustomerSiteId
            LEFT JOIN svo.D_BUSINESS_UNIT         AS BU   ON BU.BUSINESS_UNIT_ID      = c.SourceDocumentsOrgId
            LEFT JOIN svo.D_LEDGER                AS LDG  ON LDG.LEDGER_ID            = c.CustContHeadersLedgerId
            LEFT JOIN svo.D_LEGAL_ENTITY          AS LE   ON LE.LEGAL_ENTITY_ID       = c.CustContHeadersLegalEntityId
            LEFT JOIN svo.D_CURRENCY              AS CUR  ON CUR.CURRENCY_ID          = c.CustContHeadersContractCurrencyCode
            LEFT JOIN svo.D_ITEM                  AS ITM  ON ITM.INVENTORY_ITEM_ID    = c.SourceDocLinesItemId -- ITM.ITEM_ORG_ID = AND c.SourceDocLinesInventoryOrgId
            LEFT JOIN svo.D_OM_ORDER_HEADER       AS OH   ON OH.ORDER_HEADER_ID         = CONVERT(VARCHAR(50), C.LineHeaderId)
            LEFT JOIN svo.D_OM_ORDER_LINE        AS OL   ON OL.ORDER_LINE_ID         = c.LineId and OL.ORDER_HEADER_ID = C.LineHeaderID
        )
        INSERT INTO svo.F_RM_SATISFACTION_EVENTS WITH (TABLOCK)
        (RM_SATISFACTION_EVENT_SK, RM_PERF_OBLIGATION_LINE_SK, RM_PERF_OBLIGATION_SK, RM_CONTRACT_SK, RM_SOURCE_DOCUMENT_LINE_SK, RM_SOURCE_DOC_PRICING_LINE_SK, CUSTOMER_SK, CUSTOMER_SITE_SK, BUSINESS_UNIT_SK, LEDGER_SK, LEGAL_ENTITY_SK, CURRENCY_SK, ITEM_SK, ORDER_HEADER_SK, ORDER_LINE_SK, SATISFACTION_MEASUREMENT_DATE_SK, SATISFACTION_PERIOD_START_DATE_SK, SATISFACTION_PERIOD_END_DATE_SK, EVENT_CREATION_DATE_SK, EVENT_LAST_UPDATE_DATE_SK, SATISFACTION_MEASUREMENT_NUMBER, SATISFACTION_DAYS_IN_PERIOD, SATISFACTION_PERIOD_PROPORTION, SATISFACTION_PERCENT, SATISFACTION_QUANTITY, SATISFACTION_AMOUNT, BZ_LOAD_DATE, SV_LOAD_DATE, ROW_TYPE)
        SELECT RM_SATISFACTION_EVENT_SK, RM_PERF_OBLIGATION_LINE_SK, RM_PERF_OBLIGATION_SK, RM_CONTRACT_SK, RM_SOURCE_DOCUMENT_LINE_SK, RM_SOURCE_DOC_PRICING_LINE_SK, CUSTOMER_SK, CUSTOMER_SITE_SK, BUSINESS_UNIT_SK, LEDGER_SK, LEGAL_ENTITY_SK, CURRENCY_SK, ITEM_SK, ORDER_HEADER_SK, ORDER_LINE_SK, SATISFACTION_MEASUREMENT_DATE_SK, SATISFACTION_PERIOD_START_DATE_SK, SATISFACTION_PERIOD_END_DATE_SK, EVENT_CREATION_DATE_SK, EVENT_LAST_UPDATE_DATE_SK, SATISFACTION_MEASUREMENT_NUMBER, SATISFACTION_DAYS_IN_PERIOD, SATISFACTION_PERIOD_PROPORTION, SATISFACTION_PERCENT, SATISFACTION_QUANTITY, SATISFACTION_AMOUNT, BZ_LOAD_DATE, SV_LOAD_DATE, ROW_TYPE
        FROM FinalRows;

        SET @RowInserted = @@ROWCOUNT;

        UPDATE etl.ETL_WATERMARK SET LAST_WATERMARK = CAST(GETDATE() AS DATETIME2(7)), UDT_DATE = SYSDATETIME() WHERE TABLE_NAME = @TargetObject;

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
