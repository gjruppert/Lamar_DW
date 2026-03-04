IF OBJECT_ID('svo.D_RM_RULE','U') IS NOT NULL
    DROP TABLE svo.D_RM_RULE;
GO

CREATE TABLE svo.D_RM_RULE
(
    RM_RULE_SK                 BIGINT IDENTITY(1,1) NOT NULL PRIMARY KEY,
    RULE_ID                    BIGINT        NOT NULL,    -- natural key
    RULE_NAME                  VARCHAR(240)  NULL,
    RULE_TYPE_CODE             VARCHAR(60)   NULL,
    SSP_RULE_CODE              VARCHAR(60)   NULL,
    ALLOCATION_METHOD_CODE     VARCHAR(60)   NULL,
    SATISFACTION_METHOD_CODE   VARCHAR(60)   NULL,
    REVENUE_METHOD_CODE        VARCHAR(60)   NULL,
    ACTIVE_FLAG                VARCHAR(1)    NULL,
    START_DATE                 DATE          NULL,
    END_DATE                   DATE          NULL,
    CREATED_BY                 VARCHAR(64)   NULL,
    CREATION_DATE              DATE          NULL,
    LAST_UPDATED_BY            VARCHAR(64)   NULL,
    LAST_UPDATE_DATE           DATE          NULL,

    BZ_LOAD_DATE               DATE          NOT NULL DEFAULT (CAST(GETDATE() AS DATE)),
    SV_LOAD_DATE               DATE          NOT NULL DEFAULT (CAST(GETDATE() AS DATE))
) ON [FG_SilverDim];
GO

SET IDENTITY_INSERT svo.D_RM_RULE ON;

INSERT INTO svo.D_RM_RULE
(
    RM_RULE_SK,
    RULE_ID,
    RULE_NAME,
    RULE_TYPE_CODE,
    SSP_RULE_CODE,
    ALLOCATION_METHOD_CODE,
    SATISFACTION_METHOD_CODE,
    REVENUE_METHOD_CODE,
    ACTIVE_FLAG,
    START_DATE,
    END_DATE,
    CREATED_BY,
    CREATION_DATE,
    LAST_UPDATED_BY,
    LAST_UPDATE_DATE,
    BZ_LOAD_DATE,
    SV_LOAD_DATE
)
VALUES
(
    0,
    0,
    'Unknown',
    'Unknown',
    'Unknown',
    'Unknown',
    'Unknown',
    'Unknown',
    'N',
    '0001-01-01',
    '0001-01-01',
    'Unknown',
    '0001-01-01',
    'Unknown',
    '0001-01-01',
    CAST(GETDATE() AS DATE),
    CAST(GETDATE() AS DATE)
);

SET IDENTITY_INSERT svo.D_RM_RULE OFF;
GO