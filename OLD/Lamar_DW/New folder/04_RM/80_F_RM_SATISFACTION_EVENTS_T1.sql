/* =====================================================================================
   F_RM_SATISFACTION_EVENTS (DDL) + usp_Load_F_RM_SATISFACTION_EVENTS_T1
   - T1 fact loader
   - Idempotent
   - ETL_RUN logging
   - No MERGE (prevents "update/delete same row more than once")
   ===================================================================================== */

SET ANSI_NULLS ON;
SET QUOTED_IDENTIFIER ON;
GO

/* =========================
   DDL (create if missing)
   ========================= */
IF OBJECT_ID('svo.F_RM_SATISFACTION_EVENTS','U') IS NULL
BEGIN
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

        -- Date dimensions (KEYs as INT yyyymmdd, not SK identity)
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

    CREATE NONCLUSTERED INDEX IX_F_RM_SAT_EVENTS_CUST_BU
    ON svo.F_RM_SATISFACTION_EVENTS
    (
        CUSTOMER_SK,
        BUSINESS_UNIT_SK,
        SATISFACTION_MEASUREMENT_DATE_SK
    )
    ON [FG_SilverFact];
END
GO

/* Recommended: enforce idempotency key so duplicates can’t creep in.
   If you already have duplicates in the table, clean them first before creating this. */
IF NOT EXISTS
(
    SELECT 1
    FROM sys.indexes
    WHERE name = 'UX_F_RM_SAT_EVENTS_BK'
      AND object_id = OBJECT_ID('svo.F_RM_SATISFACTION_EVENTS')
)
BEGIN
    CREATE UNIQUE NONCLUSTERED INDEX UX_F_RM_SAT_EVENTS_BK
    ON svo.F_RM_SATISFACTION_EVENTS
    (
        RM_PERF_OBLIGATION_LINE_SK,
        SATISFACTION_PERIOD_START_DATE_SK,
        SATISFACTION_PERIOD_END_DATE_SK,
        ROW_TYPE
    )
    ON [FG_SilverFact];
END
GO

/* =========================
   Stored Procedure (T1)
   ========================= */
CREATE OR ALTER PROCEDURE [svo].[usp_Load_F_RM_SATISFACTION_EVENTS_T1]
(
      @FullReload BIT = 0
    , @AsofDate   DATE = NULL   -- optional; if NULL, treated as "today"
)
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    DECLARE
          @ProcName     SYSNAME  = OBJECT_SCHEMA_NAME(@@PROCID) + '.' + OBJECT_NAME(@@PROCID)
        , @TargetObject SYSNAME  = 'svo.F_RM_SATISFACTION_EVENTS'
        , @RunId        BIGINT
        , @StartDttm    DATETIME2(0) = SYSDATETIME()
        , @RowInserted  INT = 0
        , @RowUpdatedT1 INT = 0;

    IF @AsofDate IS NULL
        SET @AsofDate = CAST(GETDATE() AS DATE);

    INSERT INTO etl.ETL_RUN (PROC_NAME, TARGET_OBJECT, ASOF_DATE, START_DTTM, STATUS)
    VALUES (@ProcName, @TargetObject, @AsofDate, @StartDttm, 'STARTED');

    SET @RunId = SCOPE_IDENTITY();

    BEGIN TRY
        BEGIN TRAN;

        IF @FullReload = 1
        BEGIN
            PRINT 'FullReload requested. Rebuilding ' + @TargetObject;
            TRUNCATE TABLE svo.F_RM_SATISFACTION_EVENTS;
        END

        /* =========================
           Source build (your logic)
           ========================= */
        ;WITH BaseLines AS
        (
            SELECT
                  L.PerfObligationLineId                                                                    AS PerfObligationLineId
                , CAST(COALESCE(L.PerfObligationLinesContrCurNetConsiderAmt, 0) AS DECIMAL(29,4))            AS LineAmount
                , COALESCE(L.PerfObligationLinesRevenueStartDate, L.PerfObligationLinesRevenueEndDate, CAST('0001-01-01' AS DATE)) AS RevStart
                , COALESCE(L.PerfObligationLinesRevenueEndDate,   L.PerfObligationLinesRevenueStartDate, CAST('0001-01-01' AS DATE)) AS RevEnd
                , DATEDIFF(DAY,
                           COALESCE(L.PerfObligationLinesRevenueStartDate, L.PerfObligationLinesRevenueEndDate, CAST('0001-01-01' AS DATE)),
                           COALESCE(L.PerfObligationLinesRevenueEndDate,   L.PerfObligationLinesRevenueStartDate, CAST('0001-01-01' AS DATE))
                  ) + 1                                                                                      AS BaseDays
                , ISNULL(L.SourceDocLinesDocumentLineId, -1)                                                 AS SourceDocLinesDocumentLineId
                , ISNULL(O.PerfObligationId, -1)                                                             AS PerfObligationId
                , ISNULL(C.CustomerContractHeaderId, -1)                                                     AS CustomerContractHeaderId
                , ISNULL(S.SourceDocumentsOrgId, -1)                                                         AS SourceDocumentsOrgId
                , ISNULL(P.SourceDocLinesBillToCustomerId, -1)                                               AS SourceDocLinesBillToCustomerId
                , ISNULL(P.SourceDocLinesBillToCustomerSiteId, -1)                                           AS SourceDocLinesBillToCustomerSiteId
                , ISNULL(C.CustContHeadersLedgerId, -1)                                                      AS CustContHeadersLedgerId
                , ISNULL(C.CustContHeadersLegalEntityId, -1)                                                 AS CustContHeadersLegalEntityId
                , ISNULL(C.CustContHeadersContractCurrencyCode, 'UNK')                                       AS CustContHeadersContractCurrencyCode
                , COALESCE(CAST(L.AddDateTime AS DATE), CAST(GETDATE() AS DATE))                              AS BZ_LOAD_DATE
                , CASE
                      WHEN EXISTS
                           (
                               SELECT 1
                               FROM bzo.VRM_PolSatisfactionEventsPVO E
                               WHERE E.PolSatisfactionEventsPerfObligationLineId = L.PerfObligationLineId
                           )
                      THEN 'A'
                      ELSE 'D'
                  END                                                                                        AS ROW_TYPE
                , ISNULL(P.SourceDocLinesInventoryOrgId, -1)                                                 AS SourceDocLinesInventoryOrgId
                , ISNULL(P.SourceDocLinesItemId, -1)                                                         AS SourceDocLinesItemId
            FROM bzo.VRM_PerfObligationLinesPVO            AS L
            LEFT JOIN bzo.VRM_PerfObligationsPVO           AS O  ON O.PerfObligationId = L.PerfObligationLinesPerfObligationId
            LEFT JOIN bzo.VRM_CustomerContractHeadersPVO   AS C  ON C.CustomerContractHeaderId = O.PerfObligationsCustomerContractHeaderId
            LEFT JOIN bzo.VRM_SourceDocumentLinesPVO       AS S  ON S.SourceDocLinesDocumentLineId = L.SourceDocLinesDocumentLineId
            LEFT JOIN bzo.VRM_SourceDocLinePricingLinesPVO AS P  ON P.SourceDocLinesDocumentLineId = L.SourceDocLinesDocumentLineId
        ),
        Segments AS
        (
            SELECT
                  b.*
                , CAST(b.RevStart AS DATE) AS SegmentStart
                , CAST(CASE WHEN EOMONTH(b.RevStart) < b.RevEnd THEN EOMONTH(b.RevStart) ELSE b.RevEnd END AS DATE) AS SegmentEnd
            FROM BaseLines b

            UNION ALL

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
                , CAST(DATEADD(DAY, 1, s.SegmentEnd) AS DATE) AS SegmentStart
                , CAST(CASE WHEN EOMONTH(DATEADD(DAY, 1, s.SegmentEnd)) < s.RevEnd THEN EOMONTH(DATEADD(DAY, 1, s.SegmentEnd)) ELSE s.RevEnd END AS DATE) AS SegmentEnd
            FROM Segments s
            WHERE DATEADD(DAY, 1, s.SegmentEnd) <= s.RevEnd
        ),
        Calc AS
        (
            SELECT
                  s.*
                , DATEDIFF(DAY, s.SegmentStart, s.SegmentEnd) + 1 AS DaysInPeriod
                , CAST(
                      s.LineAmount
                      * CAST(DATEDIFF(DAY, s.SegmentStart, s.SegmentEnd) + 1 AS DECIMAL(29,4))
                      / NULLIF(CAST(s.BaseDays AS DECIMAL(29,4)), 0)
                      AS DECIMAL(29,4)
                  ) AS AmountRaw
                , ROW_NUMBER() OVER (PARTITION BY s.PerfObligationLineId ORDER BY s.SegmentStart, s.SegmentEnd) AS rn
                , COUNT(*)    OVER (PARTITION BY s.PerfObligationLineId) AS cnt
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
                  ) AS SumPrevAmt
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
                ISNULL(ITM.ITEM_SK, 0)                             AS ITEM_SK,

                -- Date keys
                COALESCE(CONVERT(INT, CONVERT(CHAR(8), c.SegmentEnd,   112)), 10101) AS SATISFACTION_MEASUREMENT_DATE_SK,
                COALESCE(CONVERT(INT, CONVERT(CHAR(8), c.SegmentStart, 112)), 10101) AS SATISFACTION_PERIOD_START_DATE_SK,
                COALESCE(CONVERT(INT, CONVERT(CHAR(8), c.SegmentEnd,   112)), 10101) AS SATISFACTION_PERIOD_END_DATE_SK,
                COALESCE(CONVERT(INT, CONVERT(CHAR(8), c.SegmentEnd,   112)), 10101) AS EVENT_CREATION_DATE_SK,
                COALESCE(CONVERT(INT, CONVERT(CHAR(8), c.SegmentEnd,   112)), 10101) AS EVENT_LAST_UPDATE_DATE_SK,

                1 AS SATISFACTION_MEASUREMENT_NUMBER,
                c.DaysInPeriod AS SATISFACTION_DAYS_IN_PERIOD,

                CAST(CASE WHEN c.rn = c.cnt THEN c.LineAmount - ISNULL(c.SumPrevAmt, 0) ELSE c.AmountRaw END AS DECIMAL(29,4)) AS SATISFACTION_AMOUNT,

                CAST(
                    CASE WHEN c.LineAmount = 0 THEN 0
                         ELSE (CASE WHEN c.rn = c.cnt THEN c.LineAmount - ISNULL(c.SumPrevAmt, 0) ELSE c.AmountRaw END)
                              / NULLIF(c.LineAmount, 0)
                    END
                    AS DECIMAL(29,4)
                ) AS SATISFACTION_PERIOD_PROPORTION,

                CAST(
                    CASE WHEN c.LineAmount = 0 THEN 0
                         ELSE (CASE WHEN c.rn = c.cnt THEN c.LineAmount - ISNULL(c.SumPrevAmt, 0) ELSE c.AmountRaw END)
                              / NULLIF(c.LineAmount, 0) * 100.0
                    END
                    AS DECIMAL(29,4)
                ) AS SATISFACTION_PERCENT,

                CAST(0.0 AS DECIMAL(29,4)) AS SATISFACTION_QUANTITY,

                COALESCE(c.BZ_LOAD_DATE, CAST(GETDATE() AS DATE)) AS BZ_LOAD_DATE,
                CAST(GETDATE() AS DATE) AS SV_LOAD_DATE,
                c.ROW_TYPE

            FROM Calc c

            -- RM dims
            LEFT JOIN svo.D_RM_PERF_OBLIGATION_LINE    AS LD  ON LD.PERF_OBLIGATION_LINE_ID      = c.PerfObligationLineId
            LEFT JOIN svo.D_RM_PERF_OBLIGATION         AS OD  ON OD.PERF_OBLIGATION_ID           = c.PerfObligationId
            LEFT JOIN svo.D_RM_CONTRACT                AS CD  ON CD.CUSTOMER_CONTRACT_HEADER_ID  = c.CustomerContractHeaderId
            LEFT JOIN svo.D_RM_SOURCE_DOCUMENT_LINE    AS SD  ON SD.SOURCE_DOCUMENT_LINE_ID      = c.SourceDocLinesDocumentLineId
            LEFT JOIN svo.D_RM_SOURCE_DOC_PRICING_LINE AS PD  ON PD.SOURCE_DOCUMENT_LINE_ID      = c.SourceDocLinesDocumentLineId

            /* NOTE: Your original join was:
               SE.POL_SATISFACTION_EVENT_ID = c.PerfObligationLineId
               That looks suspicious (event id != obligation line id). Keeping your behavior
               but this is a likely accuracy issue if the source has real EventIds. */
            LEFT JOIN svo.D_RM_SATISFACTION_EVENT      AS SE  ON SE.POL_SATISFACTION_EVENT_ID    = c.PerfObligationLineId

            -- Conformed dims
            LEFT JOIN svo.D_CUSTOMER_ACCOUNT      AS CUST ON CUST.CUSTOMER_ACCOUNT_ID = c.SourceDocLinesBillToCustomerId
            LEFT JOIN svo.D_CUSTOMER_ACCOUNT_SITE AS PS   ON PS.CUSTOMER_SITE         = c.SourceDocLinesBillToCustomerSiteId
            LEFT JOIN svo.D_BUSINESS_UNIT         AS BU   ON BU.BUSINESS_UNIT_ID      = c.SourceDocumentsOrgId
            LEFT JOIN svo.D_LEDGER                AS LDG  ON LDG.LEDGER_ID            = c.CustContHeadersLedgerId
            LEFT JOIN svo.D_LEGAL_ENTITY          AS LE   ON LE.LEGAL_ENTITY_ID       = c.CustContHeadersLegalEntityId
            LEFT JOIN svo.D_CURRENCY              AS CUR  ON CUR.CURRENCY_ID          = c.CustContHeadersContractCurrencyCode
            LEFT JOIN svo.D_ITEM                  AS ITM  ON ITM.INVENTORY_ITEM_ID    = c.SourceDocLinesItemId
        ),
        Deduped AS
        (
            /* This prevents the MERGE-style multi-match problem:
               choose one row per business key. */
            SELECT *
            FROM
            (
                SELECT
                      fr.*
                    , ROW_NUMBER() OVER
                      (
                          PARTITION BY
                              fr.RM_PERF_OBLIGATION_LINE_SK,
                              fr.SATISFACTION_PERIOD_START_DATE_SK,
                              fr.SATISFACTION_PERIOD_END_DATE_SK,
                              fr.ROW_TYPE
                          ORDER BY
                              fr.BZ_LOAD_DATE DESC,
                              fr.SV_LOAD_DATE DESC
                      ) AS rn
                FROM FinalRows fr
            ) x
            WHERE x.rn = 1
        )
        /* =========================
           T1 Upsert (no MERGE)
           ========================= */

        -- UPDATE existing rows
        UPDATE tgt
            SET
                  tgt.RM_SATISFACTION_EVENT_SK          = src.RM_SATISFACTION_EVENT_SK
                , tgt.RM_PERF_OBLIGATION_SK             = src.RM_PERF_OBLIGATION_SK
                , tgt.RM_CONTRACT_SK                    = src.RM_CONTRACT_SK
                , tgt.RM_SOURCE_DOCUMENT_LINE_SK        = src.RM_SOURCE_DOCUMENT_LINE_SK
                , tgt.RM_SOURCE_DOC_PRICING_LINE_SK     = src.RM_SOURCE_DOC_PRICING_LINE_SK
                , tgt.CUSTOMER_SK                       = src.CUSTOMER_SK
                , tgt.CUSTOMER_SITE_SK                  = src.CUSTOMER_SITE_SK
                , tgt.BUSINESS_UNIT_SK                  = src.BUSINESS_UNIT_SK
                , tgt.LEDGER_SK                         = src.LEDGER_SK
                , tgt.LEGAL_ENTITY_SK                   = src.LEGAL_ENTITY_SK
                , tgt.CURRENCY_SK                       = src.CURRENCY_SK
                , tgt.ITEM_SK                           = src.ITEM_SK
                , tgt.SATISFACTION_MEASUREMENT_DATE_SK  = src.SATISFACTION_MEASUREMENT_DATE_SK
                , tgt.EVENT_CREATION_DATE_SK            = src.EVENT_CREATION_DATE_SK
                , tgt.EVENT_LAST_UPDATE_DATE_SK         = src.EVENT_LAST_UPDATE_DATE_SK
                , tgt.SATISFACTION_MEASUREMENT_NUMBER   = src.SATISFACTION_MEASUREMENT_NUMBER
                , tgt.SATISFACTION_DAYS_IN_PERIOD       = src.SATISFACTION_DAYS_IN_PERIOD
                , tgt.SATISFACTION_PERIOD_PROPORTION    = src.SATISFACTION_PERIOD_PROPORTION
                , tgt.SATISFACTION_PERCENT              = src.SATISFACTION_PERCENT
                , tgt.SATISFACTION_QUANTITY             = src.SATISFACTION_QUANTITY
                , tgt.SATISFACTION_AMOUNT               = src.SATISFACTION_AMOUNT
                , tgt.BZ_LOAD_DATE                      = src.BZ_LOAD_DATE
                , tgt.SV_LOAD_DATE                      = src.SV_LOAD_DATE
        FROM svo.F_RM_SATISFACTION_EVENTS tgt
        INNER JOIN Deduped src
            ON  tgt.RM_PERF_OBLIGATION_LINE_SK        = src.RM_PERF_OBLIGATION_LINE_SK
            AND tgt.SATISFACTION_PERIOD_START_DATE_SK = src.SATISFACTION_PERIOD_START_DATE_SK
            AND tgt.SATISFACTION_PERIOD_END_DATE_SK   = src.SATISFACTION_PERIOD_END_DATE_SK
            AND tgt.ROW_TYPE                          = src.ROW_TYPE;

        SET @RowUpdatedT1 = @@ROWCOUNT;

        -- INSERT new rows
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
              src.RM_SATISFACTION_EVENT_SK
            , src.RM_PERF_OBLIGATION_LINE_SK
            , src.RM_PERF_OBLIGATION_SK
            , src.RM_CONTRACT_SK
            , src.RM_SOURCE_DOCUMENT_LINE_SK
            , src.RM_SOURCE_DOC_PRICING_LINE_SK
            , src.CUSTOMER_SK
            , src.CUSTOMER_SITE_SK
            , src.BUSINESS_UNIT_SK
            , src.LEDGER_SK
            , src.LEGAL_ENTITY_SK
            , src.CURRENCY_SK
            , src.ITEM_SK
            , src.SATISFACTION_MEASUREMENT_DATE_SK
            , src.SATISFACTION_PERIOD_START_DATE_SK
            , src.SATISFACTION_PERIOD_END_DATE_SK
            , src.EVENT_CREATION_DATE_SK
            , src.EVENT_LAST_UPDATE_DATE_SK
            , src.SATISFACTION_MEASUREMENT_NUMBER
            , src.SATISFACTION_DAYS_IN_PERIOD
            , src.SATISFACTION_PERIOD_PROPORTION
            , src.SATISFACTION_PERCENT
            , src.SATISFACTION_QUANTITY
            , src.SATISFACTION_AMOUNT
            , src.BZ_LOAD_DATE
            , src.SV_LOAD_DATE
            , src.ROW_TYPE
        FROM Deduped src
        WHERE NOT EXISTS
        (
            SELECT 1
            FROM svo.F_RM_SATISFACTION_EVENTS tgt
            WHERE tgt.RM_PERF_OBLIGATION_LINE_SK        = src.RM_PERF_OBLIGATION_LINE_SK
              AND tgt.SATISFACTION_PERIOD_START_DATE_SK = src.SATISFACTION_PERIOD_START_DATE_SK
              AND tgt.SATISFACTION_PERIOD_END_DATE_SK   = src.SATISFACTION_PERIOD_END_DATE_SK
              AND tgt.ROW_TYPE                          = src.ROW_TYPE
        )
        OPTION (MAXRECURSION 32767);

        SET @RowInserted = @@ROWCOUNT;

        COMMIT;

        UPDATE etl.ETL_RUN
            SET END_DTTM = SYSDATETIME(),
                STATUS = 'SUCCESS',
                ROW_INSERTED = @RowInserted,
                ROW_UPDATED_T1 = @RowUpdatedT1,
                ROW_EXPIRED = 0
        WHERE RUN_ID = @RunId;

        PRINT 'Completed ' + @ProcName
            + ' | RUN_ID=' + CAST(@RunId AS VARCHAR(20))
            + ' | Inserted=' + CAST(@RowInserted AS VARCHAR(20))
            + ' | Updated=' + CAST(@RowUpdatedT1 AS VARCHAR(20));
    END TRY
    BEGIN CATCH
        IF @@TRANCOUNT > 0 ROLLBACK;

        UPDATE etl.ETL_RUN
            SET END_DTTM = SYSDATETIME(),
                STATUS = 'FAILED',
                ERROR_MESSAGE = LEFT(ERROR_MESSAGE(), 4000)
        WHERE RUN_ID = @RunId;

        DECLARE @Msg NVARCHAR(4000) = 'Load failed in ' + @ProcName + ' | RUN_ID=' + CAST(@RunId AS NVARCHAR(20))
                                   + ' | ' + LEFT(ERROR_MESSAGE(), 3500);
        THROW 50000, @Msg, 1;
    END CATCH
END
GO