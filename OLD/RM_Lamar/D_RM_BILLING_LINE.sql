USE [Oracle_Reporting_P2];
GO

IF OBJECT_ID('svo.D_RM_BILLING_LINE', 'U') IS NOT NULL
    DROP TABLE svo.D_RM_BILLING_LINE;
GO

SET ANSI_NULLS ON;
GO
SET QUOTED_IDENTIFIER ON;
GO

CREATE TABLE [svo].[D_RM_BILLING_LINE]
(
    RM_BILLING_LINE_SK         BIGINT IDENTITY(1,1) NOT NULL,

    BILLING_LINE_DETAIL_ID     BIGINT        NOT NULL,   -- BillingLineDetailId
    BILL_DATE                  DATE          NOT NULL,   -- BillingLineDetailsBillDate
    BILL_ID                    BIGINT        NOT NULL,   -- BillingLineDetailsBillId
    BILL_LINE_ID               BIGINT        NOT NULL,   -- BillingLineDetailsBillLineId
    BILL_LINE_NUMBER           VARCHAR(30)   NOT NULL,   -- BillingLineDetailsBillLineNumber
    BILL_NUMBER                VARCHAR(60)   NOT NULL,   -- BillingLineDetailsBillNumber

    CREATED_BY                 VARCHAR(250)  NULL,       -- BillingLineDetailsCreatedBy
    CREATION_DATE              DATE          NOT NULL,   -- from BillingLineDetailsCreationDate
    LAST_UPDATE_DATE           DATE          NOT NULL,   -- from BillingLineDetailsLastUpdateDate
    LAST_UPDATED_BY            VARCHAR(250)  NULL,       -- BillingLineDetailsLastUpdatedBy

    BZ_LOAD_DATE               DATE          NOT NULL,
    SV_LOAD_DATE               DATE          NOT NULL,

    CONSTRAINT PK_D_RM_BILLING_LINE
        PRIMARY KEY CLUSTERED (RM_BILLING_LINE_SK ASC)
) ON [FG_SilverDim];
GO

CREATE UNIQUE NONCLUSTERED INDEX UX_D_RM_BILLING_LINE_DETAIL_ID
ON [svo].[D_RM_BILLING_LINE] (BILLING_LINE_DETAIL_ID)
ON [FG_SilverDim];
GO

-- Plug row
SET IDENTITY_INSERT svo.D_RM_BILLING_LINE ON;

INSERT INTO svo.D_RM_BILLING_LINE
(
    RM_BILLING_LINE_SK,
    BILLING_LINE_DETAIL_ID,
    BILL_DATE,
    BILL_ID,
    BILL_LINE_ID,
    BILL_LINE_NUMBER,
    BILL_NUMBER,
    CREATED_BY,
    CREATION_DATE,
    LAST_UPDATE_DATE,
    LAST_UPDATED_BY,
    BZ_LOAD_DATE,
    SV_LOAD_DATE
)
VALUES
(
    0,              -- RM_BILLING_LINE_SK
    0,              -- BILLING_LINE_DETAIL_ID
    '1900-01-01',   -- BILL_DATE
    0,              -- BILL_ID
    0,              -- BILL_LINE_ID
    'UNKNOWN',      -- BILL_LINE_NUMBER
    'UNKNOWN',      -- BILL_NUMBER
    'UNKNOWN',      -- CREATED_BY
    '1900-01-01',   -- CREATION_DATE
    '1900-01-01',   -- LAST_UPDATE_DATE
    'UNKNOWN',      -- LAST_UPDATED_BY
    CAST(GETDATE() AS DATE), -- BZ_LOAD_DATE
    CAST(GETDATE() AS DATE)  -- SV_LOAD_DATE
);

SET IDENTITY_INSERT svo.D_RM_BILLING_LINE OFF;
GO

USE [Oracle_Reporting_P2];
GO

INSERT INTO svo.D_RM_BILLING_LINE
(
    BILLING_LINE_DETAIL_ID,
    BILL_DATE,
    BILL_ID,
    BILL_LINE_ID,
    BILL_LINE_NUMBER,
    BILL_NUMBER,
    CREATED_BY,
    CREATION_DATE,
    LAST_UPDATE_DATE,
    LAST_UPDATED_BY,
    BZ_LOAD_DATE,
    SV_LOAD_DATE
)
SELECT
    B.BillingLineDetailId                    AS BILLING_LINE_DETAIL_ID,
    B.BillingLineDetailsBillDate             AS BILL_DATE,
    B.BillingLineDetailsBillId               AS BILL_ID,
    B.BillingLineDetailsBillLineId           AS BILL_LINE_ID,
    B.BillingLineDetailsBillLineNumber       AS BILL_LINE_NUMBER,
    B.BillingLineDetailsBillNumber           AS BILL_NUMBER,
    B.BillingLineDetailsCreatedBy            AS CREATED_BY,
    CAST(B.BillingLineDetailsCreationDate AS DATE)   AS CREATION_DATE,
    CAST(B.BillingLineDetailsLastUpdateDate AS DATE) AS LAST_UPDATE_DATE,
    B.BillingLineDetailsLastUpdatedBy        AS LAST_UPDATED_BY,
    CAST(B.AddDateTime AS DATE)              AS BZ_LOAD_DATE,
    CAST(GETDATE() AS DATE)                  AS SV_LOAD_DATE
FROM bzo.VRM_BillingLineDetailsPVO AS B;
GO