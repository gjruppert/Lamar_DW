USE [Oracle_Reporting_P2];
GO

IF OBJECT_ID('svo.D_RM_PERF_OBLIGATION_LINE','U') IS NOT NULL
    DROP TABLE svo.D_RM_PERF_OBLIGATION_LINE;
GO

SET ANSI_NULLS ON;
GO
SET QUOTED_IDENTIFIER ON;
GO

CREATE TABLE [svo].[D_RM_PERF_OBLIGATION_LINE]
(
    RM_PERF_OBLIGATION_LINE_SK     BIGINT IDENTITY(1,1) NOT NULL,
    PERF_OBLIGATION_LINE_ID        BIGINT       NOT NULL,  -- PerfObligationLineId

    COMMENTS                       VARCHAR(1000) NULL,     -- PerfObligationLinesComments
    CONTR_CUR_NET_CONSIDER_AMT     DECIMAL(29,4) NULL,     -- PerfObligationLinesContrCurNetConsiderAmt
    CREATED_BY                     VARCHAR(64)  NOT NULL,  -- PerfObligationLinesCreatedBy
    DOCUMENT_LINE_ID               BIGINT       NOT NULL,  -- PerfObligationLinesDocumentLineId
    ENTERED_CUR_NET_CONSIDER_AMT   DECIMAL(29,4) NULL,     -- PerfObligationLinesEnteredCurNetConsiderAmt
    ENTERED_CUR_RECOG_REV_AMT      DECIMAL(29,4) NULL,     -- PerfObligationLinesEnteredCurRecogRevAmt
    LAST_UPDATE_DATE               DATETIME     NOT NULL,  -- PerfObligationLinesLastUpdateDate
    LAST_UPDATED_BY                VARCHAR(64)  NOT NULL,  -- PerfObligationLinesLastUpdatedBy
    LAST_UPDATE_LOGIN              VARCHAR(32)  NULL,      -- PerfObligationLinesLastUpdateLogin
    NET_LINE_AMT                   DECIMAL(29,4) NULL,     -- PerfObligationLinesNetLineAmt
    PAYMENT_AMOUNT                 DECIMAL(29,4) NULL,     -- PerfObligationLinesPaymentAmount
    PERF_OBLIGATION_ID             BIGINT       NULL,      -- PerfObligationLinesPerfObligationId
    PERF_OBLIGATION_LINE_NUMBER    BIGINT       NULL,      -- PerfObligationLinesPerfObligationLineNumber
    REVENUE_END_DATE               DATE         NULL,      -- PerfObligationLinesRevenueEndDate
    REVENUE_START_DATE             DATE         NULL,      -- PerfObligationLinesRevenueStartDate
    SATISFACTION_BASE_PROPORTION   BIGINT       NULL,      -- PerfObligationLinesSatisfactionBaseProportion
    SOURCE_DOCUMENT_LINE_ID        BIGINT       NOT NULL,  -- SourceDocLinesDocumentLineId

    BZ_LOAD_DATE                   DATE         NOT NULL,
    SV_LOAD_DATE                   DATE         NOT NULL,

    CONSTRAINT PK_D_RM_PERF_OBLIGATION_LINE
        PRIMARY KEY CLUSTERED (RM_PERF_OBLIGATION_LINE_SK ASC)
) ON [FG_SilverDim];
GO

CREATE UNIQUE NONCLUSTERED INDEX UX_D_RM_PERF_OBLIGATION_LINE_ID
ON svo.D_RM_PERF_OBLIGATION_LINE (PERF_OBLIGATION_LINE_ID)
ON [FG_SilverDim];
GO

SET IDENTITY_INSERT svo.D_RM_PERF_OBLIGATION_LINE ON;

INSERT INTO svo.D_RM_PERF_OBLIGATION_LINE
(
    RM_PERF_OBLIGATION_LINE_SK,
    PERF_OBLIGATION_LINE_ID,
    COMMENTS,
    CONTR_CUR_NET_CONSIDER_AMT,
    CREATED_BY,
    DOCUMENT_LINE_ID,
    ENTERED_CUR_NET_CONSIDER_AMT,
    ENTERED_CUR_RECOG_REV_AMT,
    LAST_UPDATE_DATE,
    LAST_UPDATED_BY,
    LAST_UPDATE_LOGIN,
    NET_LINE_AMT,
    PAYMENT_AMOUNT,
    PERF_OBLIGATION_ID,
    PERF_OBLIGATION_LINE_NUMBER,
    REVENUE_END_DATE,
    REVENUE_START_DATE,
    SATISFACTION_BASE_PROPORTION,
    SOURCE_DOCUMENT_LINE_ID,
    BZ_LOAD_DATE,
    SV_LOAD_DATE
)
VALUES
(
    0,
    0,
    'UNKNOWN',
    0,
    'UNKNOWN',
    0,
    0,
    0,
    CAST('1900-01-01' AS DATETIME),  -- was '0001-01-01' (invalid for DATETIME)
    'UNKNOWN',
    'UNKNOWN',
    0,
    0,
    0,
    0,
    CAST('1900-01-01' AS DATE),
    CAST('1900-01-01' AS DATE),
    0,
    0,
    CAST(GETDATE() AS DATE),
    CAST(GETDATE() AS DATE)
);

SET IDENTITY_INSERT svo.D_RM_PERF_OBLIGATION_LINE OFF;
GO

USE [Oracle_Reporting_P2];
GO

INSERT INTO svo.D_RM_PERF_OBLIGATION_LINE
(
    PERF_OBLIGATION_LINE_ID,
    COMMENTS,
    CONTR_CUR_NET_CONSIDER_AMT,
    CREATED_BY,
    DOCUMENT_LINE_ID,
    ENTERED_CUR_NET_CONSIDER_AMT,
    ENTERED_CUR_RECOG_REV_AMT,
    LAST_UPDATE_DATE,
    LAST_UPDATED_BY,
    LAST_UPDATE_LOGIN,
    NET_LINE_AMT,
    PAYMENT_AMOUNT,
    PERF_OBLIGATION_ID,
    PERF_OBLIGATION_LINE_NUMBER,
    REVENUE_END_DATE,
    REVENUE_START_DATE,
    SATISFACTION_BASE_PROPORTION,
    SOURCE_DOCUMENT_LINE_ID,
    BZ_LOAD_DATE,
    SV_LOAD_DATE
)
SELECT
    L.PerfObligationLineId                      AS PERF_OBLIGATION_LINE_ID,
    L.PerfObligationLinesComments               AS COMMENTS,
    L.PerfObligationLinesContrCurNetConsiderAmt AS CONTR_CUR_NET_CONSIDER_AMT,
    L.PerfObligationLinesCreatedBy              AS CREATED_BY,
    L.PerfObligationLinesDocumentLineId         AS DOCUMENT_LINE_ID,
    L.PerfObligationLinesEnteredCurNetConsiderAmt  AS ENTERED_CUR_NET_CONSIDER_AMT,
    L.PerfObligationLinesEnteredCurRecogRevAmt     AS ENTERED_CUR_RECOG_REV_AMT,
    L.PerfObligationLinesLastUpdateDate         AS LAST_UPDATE_DATE,
    L.PerfObligationLinesLastUpdatedBy          AS LAST_UPDATED_BY,
    L.PerfObligationLinesLastUpdateLogin        AS LAST_UPDATE_LOGIN,
    L.PerfObligationLinesNetLineAmt             AS NET_LINE_AMT,
    L.PerfObligationLinesPaymentAmount          AS PAYMENT_AMOUNT,
    L.PerfObligationLinesPerfObligationId       AS PERF_OBLIGATION_ID,
    L.PerfObligationLinesPerfObligationLineNumber AS PERF_OBLIGATION_LINE_NUMBER,
    ISNULL(L.PerfObligationLinesRevenueEndDate,'01-01-0001')        AS REVENUE_END_DATE,
    ISNULL(L.PerfObligationLinesRevenueStartDate,'01-01-0001')      AS REVENUE_START_DATE,
    CASE WHEN L.PerfObligationLinesSatisfactionBaseProportion IS NULL
        THEN DATEDIFF(day, ISNULL(L.PerfObligationLinesRevenueStartDate,'01-01-0001') , ISNULL(L.PerfObligationLinesRevenueEndDate,'01-01-0001') )+1
        ELSE L.PerfObligationLinesSatisfactionBaseProportion END AS SATISFACTION_BASE_PROPORTION,
    L.SourceDocLinesDocumentLineId              AS SOURCE_DOCUMENT_LINE_ID,
    CAST(L.AddDateTime AS DATE)                 AS BZ_LOAD_DATE,
    CAST(GETDATE() AS DATE)                     AS SV_LOAD_DATE
FROM bzo.VRM_PerfObligationLinesPVO AS L
 
GO