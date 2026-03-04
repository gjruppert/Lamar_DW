USE [Oracle_Reporting_P2];
GO

IF OBJECT_ID('svo.D_RM_SATISFACTION_EVENT', 'U') IS NOT NULL
    DROP TABLE svo.D_RM_SATISFACTION_EVENT;
GO

SET ANSI_NULLS ON;
GO
SET QUOTED_IDENTIFIER ON;
GO

CREATE TABLE [svo].[D_RM_SATISFACTION_EVENT]
(
    RM_SATISFACTION_EVENT_SK          BIGINT IDENTITY(1,1) NOT NULL,

    POL_SATISFACTION_EVENT_ID         BIGINT        NOT NULL,   -- PolSatisfactionEventId

    ATTRIBUTE_CATEGORY                VARCHAR(30)   NULL,       -- PolSatisfactionEventsAttributeCategory
    COMMENTS                          VARCHAR(2000) NULL,       -- PolSatisfactionEventsComments

    CREATED_BY                        VARCHAR(64)   NOT NULL,   -- PolSatisfactionEventsCreatedBy
    CREATED_FROM                      VARCHAR(30)   NULL,       -- PolSatisfactionEventsCreatedFrom
    CREATION_DATE                     DATE          NOT NULL,   -- from PolSatisfactionEventsCreationDate

    DISCARDED_DATE                    DATE          NULL,       -- PolSatisfactionEventsDiscardedDate
    DISCARDED_FLAG                    VARCHAR(1)    NULL,       -- PolSatisfactionEventsDiscardedFlag

    DOCUMENT_LINE_ID                  BIGINT        NULL,       -- PolSatisfactionEventsDocumentLineId
    DOCUMENT_SUB_LINE_ID              BIGINT        NULL,       -- PolSatisfactionEventsDocumentSubLineId

    HOLD_FLAG                         VARCHAR(1)    NULL,       -- PolSatisfactionEventsHoldFlag

    LAST_UPDATE_DATE                  DATE          NOT NULL,   -- from PolSatisfactionEventsLastUpdateDate
    LAST_UPDATED_BY                   VARCHAR(64)   NOT NULL,   -- PolSatisfactionEventsLastUpdatedBy
    LAST_UPDATE_LOGIN                 VARCHAR(32)   NULL,       -- PolSatisfactionEventsLastUpdateLogin

    OBJECT_VERSION_NUMBER             BIGINT        NOT NULL,   -- PolSatisfactionEventsObjectVersionNumber

    PERF_OBLIGATION_LINE_ID           BIGINT        NULL,       -- PolSatisfactionEventsPerfObligationLineId

    PROCESSED_AMOUNT                  DECIMAL(29,4) NULL,       -- PolSatisfactionEventsProcessedAmount
    PROCESSED_FLAG                    VARCHAR(1)    NULL,       -- PolSatisfactionEventsProcessedFlag
    PROCESSED_PERIOD_PROPORTION       DECIMAL(29,4) NULL,       -- PolSatisfactionEventsProcessedPeriodProportion

    SATISFACTION_MEASUREMENT_DATE     DATE          NULL,       -- PolSatisfactionEventsSatisfactionMeasurementDate
    SATISFACTION_MEASUREMENT_NUM      BIGINT        NULL,       -- PolSatisfactionEventsSatisfactionMeasurementNum
    SATISFACTION_PERCENT              DECIMAL(29,4) NULL,       -- PolSatisfactionEventsSatisfactionPercent

    SATISFACTION_PERIOD_END_DATE      DATE          NULL,       -- PolSatisfactionEventsSatisfactionPeriodEndDate
    SATISFACTION_PERIOD_PROPORTION    DECIMAL(29,4) NULL,       -- PolSatisfactionEventsSatisfactionPeriodProportion
    SATISFACTION_PERIOD_START_DATE    DATE          NULL,       -- PolSatisfactionEventsSatisfactionPeriodStartDate

    SATISFACTION_QUANTITY             DECIMAL(29,4) NULL,       -- PolSatisfactionEventsSatisfactionQuantity

    SPLIT_FLAG                        VARCHAR(1)    NULL,       -- PolSatisfactionEventsSplitFlag

    BZ_LOAD_DATE                      DATE          NOT NULL,
    SV_LOAD_DATE                      DATE          NOT NULL,

    CONSTRAINT PK_D_RM_SATISFACTION_EVENT
        PRIMARY KEY CLUSTERED (RM_SATISFACTION_EVENT_SK ASC)
) ON [FG_SilverDim];
GO

CREATE UNIQUE NONCLUSTERED INDEX UX_D_RM_SATISFACTION_EVENT_ID
ON [svo].[D_RM_SATISFACTION_EVENT] (POL_SATISFACTION_EVENT_ID)
ON [FG_SilverDim];
GO

-- Plug row
SET IDENTITY_INSERT svo.D_RM_SATISFACTION_EVENT ON;

INSERT INTO svo.D_RM_SATISFACTION_EVENT
(
    RM_SATISFACTION_EVENT_SK,
    POL_SATISFACTION_EVENT_ID,
    ATTRIBUTE_CATEGORY,
    COMMENTS,
    CREATED_BY,
    CREATED_FROM,
    CREATION_DATE,
    DISCARDED_DATE,
    DISCARDED_FLAG,
    DOCUMENT_LINE_ID,
    DOCUMENT_SUB_LINE_ID,
    HOLD_FLAG,
    LAST_UPDATE_DATE,
    LAST_UPDATED_BY,
    LAST_UPDATE_LOGIN,
    OBJECT_VERSION_NUMBER,
    PERF_OBLIGATION_LINE_ID,
    PROCESSED_AMOUNT,
    PROCESSED_FLAG,
    PROCESSED_PERIOD_PROPORTION,
    SATISFACTION_MEASUREMENT_DATE,
    SATISFACTION_MEASUREMENT_NUM,
    SATISFACTION_PERCENT,
    SATISFACTION_PERIOD_END_DATE,
    SATISFACTION_PERIOD_PROPORTION,
    SATISFACTION_PERIOD_START_DATE,
    SATISFACTION_QUANTITY,
    SPLIT_FLAG,
    BZ_LOAD_DATE,
    SV_LOAD_DATE
)
VALUES
(
    0,              -- RM_SATISFACTION_EVENT_SK
    0,              -- POL_SATISFACTION_EVENT_ID
    'UNKNOWN',      -- ATTRIBUTE_CATEGORY
    'UNKNOWN',      -- COMMENTS
    'UNKNOWN',      -- CREATED_BY
    'UNKNOWN',      -- CREATED_FROM
    '1900-01-01',   -- CREATION_DATE
    '1900-01-01',   -- DISCARDED_DATE
    'N',            -- DISCARDED_FLAG
    0,              -- DOCUMENT_LINE_ID
    0,              -- DOCUMENT_SUB_LINE_ID
    'N',            -- HOLD_FLAG
    '1900-01-01',   -- LAST_UPDATE_DATE
    'UNKNOWN',      -- LAST_UPDATED_BY
    'UNKNOWN',      -- LAST_UPDATE_LOGIN
    0,              -- OBJECT_VERSION_NUMBER
    0,              -- PERF_OBLIGATION_LINE_ID
    0,              -- PROCESSED_AMOUNT
    'N',            -- PROCESSED_FLAG
    0,              -- PROCESSED_PERIOD_PROPORTION
    '1900-01-01',   -- SATISFACTION_MEASUREMENT_DATE
    0,              -- SATISFACTION_MEASUREMENT_NUM
    0,              -- SATISFACTION_PERCENT
    '1900-01-01',   -- SATISFACTION_PERIOD_END_DATE
    0,              -- SATISFACTION_PERIOD_PROPORTION
    '1900-01-01',   -- SATISFACTION_PERIOD_START_DATE
    0,              -- SATISFACTION_QUANTITY
    'N',            -- SPLIT_FLAG
    CAST(GETDATE() AS DATE),
    CAST(GETDATE() AS DATE)
);

SET IDENTITY_INSERT svo.D_RM_SATISFACTION_EVENT OFF;
GO

USE [Oracle_Reporting_P2];
GO

INSERT INTO svo.D_RM_SATISFACTION_EVENT
(
    POL_SATISFACTION_EVENT_ID,
    ATTRIBUTE_CATEGORY,
    COMMENTS,
    CREATED_BY,
    CREATED_FROM,
    CREATION_DATE,
    DISCARDED_DATE,
    DISCARDED_FLAG,
    DOCUMENT_LINE_ID,
    DOCUMENT_SUB_LINE_ID,
    HOLD_FLAG,
    LAST_UPDATE_DATE,
    LAST_UPDATED_BY,
    LAST_UPDATE_LOGIN,
    OBJECT_VERSION_NUMBER,
    PERF_OBLIGATION_LINE_ID,
    PROCESSED_AMOUNT,
    PROCESSED_FLAG,
    PROCESSED_PERIOD_PROPORTION,
    SATISFACTION_MEASUREMENT_DATE,
    SATISFACTION_MEASUREMENT_NUM,
    SATISFACTION_PERCENT,
    SATISFACTION_PERIOD_END_DATE,
    SATISFACTION_PERIOD_PROPORTION,
    SATISFACTION_PERIOD_START_DATE,
    SATISFACTION_QUANTITY,
    SPLIT_FLAG,
    BZ_LOAD_DATE,
    SV_LOAD_DATE
)
SELECT
    E.PolSatisfactionEventId                          AS POL_SATISFACTION_EVENT_ID,
    E.PolSatisfactionEventsAttributeCategory          AS ATTRIBUTE_CATEGORY,
    E.PolSatisfactionEventsComments                   AS COMMENTS,
    E.PolSatisfactionEventsCreatedBy                  AS CREATED_BY,
    E.PolSatisfactionEventsCreatedFrom                AS CREATED_FROM,
    CAST(E.PolSatisfactionEventsCreationDate AS DATE) AS CREATION_DATE,
    E.PolSatisfactionEventsDiscardedDate              AS DISCARDED_DATE,
    E.PolSatisfactionEventsDiscardedFlag              AS DISCARDED_FLAG,
    E.PolSatisfactionEventsDocumentLineId             AS DOCUMENT_LINE_ID,
    E.PolSatisfactionEventsDocumentSubLineId          AS DOCUMENT_SUB_LINE_ID,
    E.PolSatisfactionEventsHoldFlag                   AS HOLD_FLAG,
    CAST(E.PolSatisfactionEventsLastUpdateDate AS DATE) AS LAST_UPDATE_DATE,
    E.PolSatisfactionEventsLastUpdatedBy              AS LAST_UPDATED_BY,
    E.PolSatisfactionEventsLastUpdateLogin            AS LAST_UPDATE_LOGIN,
    E.PolSatisfactionEventsObjectVersionNumber        AS OBJECT_VERSION_NUMBER,
    E.PolSatisfactionEventsPerfObligationLineId       AS PERF_OBLIGATION_LINE_ID,
    E.PolSatisfactionEventsProcessedAmount            AS PROCESSED_AMOUNT,
    E.PolSatisfactionEventsProcessedFlag              AS PROCESSED_FLAG,
    E.PolSatisfactionEventsProcessedPeriodProportion  AS PROCESSED_PERIOD_PROPORTION,
    E.PolSatisfactionEventsSatisfactionMeasurementDate    AS SATISFACTION_MEASUREMENT_DATE,
    E.PolSatisfactionEventsSatisfactionMeasurementNum     AS SATISFACTION_MEASUREMENT_NUM,
    E.PolSatisfactionEventsSatisfactionPercent            AS SATISFACTION_PERCENT,
    E.PolSatisfactionEventsSatisfactionPeriodEndDate      AS SATISFACTION_PERIOD_END_DATE,
    E.PolSatisfactionEventsSatisfactionPeriodProportion   AS SATISFACTION_PERIOD_PROPORTION,
    E.PolSatisfactionEventsSatisfactionPeriodStartDate    AS SATISFACTION_PERIOD_START_DATE,
    E.PolSatisfactionEventsSatisfactionQuantity           AS SATISFACTION_QUANTITY,
    E.PolSatisfactionEventsSplitFlag                      AS SPLIT_FLAG,
    CAST(E.AddDateTime AS DATE)                       AS BZ_LOAD_DATE,
    CAST(GETDATE() AS DATE)                           AS SV_LOAD_DATE
FROM bzo.VRM_PolSatisfactionEventsPVO AS E;
GO