USE [Oracle_Reporting_P2];
GO

-------------------------------------------------------------------------------
-- 1. DDL: Drop and create dimension D_AR_COLLECTOR
-------------------------------------------------------------------------------
IF OBJECT_ID('svo.D_AR_COLLECTOR','U') IS NOT NULL
    DROP TABLE svo.D_AR_COLLECTOR;
GO

CREATE TABLE svo.D_AR_COLLECTOR
(
    -------------------------------------------------------------------------
    -- Surrogate key
    -------------------------------------------------------------------------
    AR_COLLECTOR_SK             BIGINT IDENTITY(1,1) NOT NULL
        CONSTRAINT PK_D_AR_COLLECTOR
            PRIMARY KEY CLUSTERED,

    -------------------------------------------------------------------------
    -- Natural key from AR
    -------------------------------------------------------------------------
    AR_COLLECTOR_ID             BIGINT       NOT NULL,  -- ArCollectorCollectorId

    -------------------------------------------------------------------------
    -- Attributes
    -------------------------------------------------------------------------
    COLLECTOR_NAME              VARCHAR(30)  NOT NULL,  -- ArCollectorName
    COLLECTOR_DESCRIPTION       VARCHAR(240) NULL,      -- ArCollectorDescription

    -------------------------------------------------------------------------
    -- Source audit
    -------------------------------------------------------------------------
    SOURCE_LAST_UPDATE_DATE     DATETIME     NOT NULL,  -- ArCollectorLastUpdateDate
    SOURCE_LAST_UPDATED_BY      VARCHAR(64)  NOT NULL,  -- ArCollectorLastUpdatedBy
    SOURCE_LAST_UPDATE_LOGIN    VARCHAR(32)  NULL,      -- ArCollectorLastUpdateLogin

    -------------------------------------------------------------------------
    -- Warehouse metadata
    -------------------------------------------------------------------------
    BZ_LOAD_DATE                DATE         NOT NULL,
    SV_LOAD_DATE                DATE         NOT NULL
)
ON FG_SilverDim;
GO

CREATE UNIQUE NONCLUSTERED INDEX UX_D_AR_COLLECTOR_ID
    ON svo.D_AR_COLLECTOR (AR_COLLECTOR_ID);
GO

-------------------------------------------------------------------------------
-- 2. Plug row (idempotent)
-------------------------------------------------------------------------------
IF NOT EXISTS
(
    SELECT 1
    FROM   svo.D_AR_COLLECTOR
    WHERE  AR_COLLECTOR_SK = 0
)
BEGIN
    SET IDENTITY_INSERT svo.D_AR_COLLECTOR ON;

    INSERT INTO svo.D_AR_COLLECTOR
    (
        AR_COLLECTOR_SK,
        AR_COLLECTOR_ID,
        COLLECTOR_NAME,
        COLLECTOR_DESCRIPTION,
        SOURCE_LAST_UPDATE_DATE,
        SOURCE_LAST_UPDATED_BY,
        SOURCE_LAST_UPDATE_LOGIN,
        BZ_LOAD_DATE,
        SV_LOAD_DATE
    )
    VALUES
    (
        0,                  -- AR_COLLECTOR_SK
        0,                  -- AR_COLLECTOR_ID
        'UNKNOWN',          -- COLLECTOR_NAME
        'Unknown AR collector',
        '19000101',         -- SOURCE_LAST_UPDATE_DATE (dummy)
        'SYSTEM',           -- SOURCE_LAST_UPDATED_BY (dummy)
        NULL,               -- SOURCE_LAST_UPDATE_LOGIN
        '19000101',         -- BZ_LOAD_DATE
        '19000101'          -- SV_LOAD_DATE
    );

    SET IDENTITY_INSERT svo.D_AR_COLLECTOR OFF;
END;
GO

-------------------------------------------------------------------------------
-- 3. Loader (MERGE, SCD1, no procedure)
--    Source: bzo.AR_CollectorExtractPVO
--    BZ_LOAD_DATE = CAST(AddDateTime AS DATE)
--    SV_LOAD_DATE = CAST(GETDATE() AS DATE)
-------------------------------------------------------------------------------
MERGE svo.D_AR_COLLECTOR AS D
USING
(
    SELECT
        C.ArCollectorCollectorId         AS AR_COLLECTOR_ID,
        C.ArCollectorName                AS COLLECTOR_NAME,
        C.ArCollectorDescription         AS COLLECTOR_DESCRIPTION,

        C.ArCollectorLastUpdateDate      AS SOURCE_LAST_UPDATE_DATE,
        C.ArCollectorLastUpdatedBy       AS SOURCE_LAST_UPDATED_BY,
        C.ArCollectorLastUpdateLogin     AS SOURCE_LAST_UPDATE_LOGIN,

        CAST(ISNULL(C.AddDateTime, GETDATE()) AS DATE) AS BZ_LOAD_DATE,
        CAST(GETDATE() AS DATE)                        AS SV_LOAD_DATE
    FROM bzo.AR_CollectorExtractPVO AS C
) AS S
ON D.AR_COLLECTOR_ID = S.AR_COLLECTOR_ID

WHEN MATCHED AND
(
       ISNULL(D.COLLECTOR_NAME, '')              <> ISNULL(S.COLLECTOR_NAME, '')
    OR ISNULL(D.COLLECTOR_DESCRIPTION, '')       <> ISNULL(S.COLLECTOR_DESCRIPTION, '')
    OR ISNULL(D.SOURCE_LAST_UPDATE_DATE, '19000101') <> ISNULL(S.SOURCE_LAST_UPDATE_DATE, '19000101')
    OR ISNULL(D.SOURCE_LAST_UPDATED_BY, '')      <> ISNULL(S.SOURCE_LAST_UPDATED_BY, '')
    OR ISNULL(D.SOURCE_LAST_UPDATE_LOGIN, '')    <> ISNULL(S.SOURCE_LAST_UPDATE_LOGIN, '')
)
THEN
    UPDATE SET
        D.COLLECTOR_NAME              = S.COLLECTOR_NAME,
        D.COLLECTOR_DESCRIPTION       = S.COLLECTOR_DESCRIPTION,
        D.SOURCE_LAST_UPDATE_DATE     = S.SOURCE_LAST_UPDATE_DATE,
        D.SOURCE_LAST_UPDATED_BY      = S.SOURCE_LAST_UPDATED_BY,
        D.SOURCE_LAST_UPDATE_LOGIN    = S.SOURCE_LAST_UPDATE_LOGIN,
        -- keep original BZ_LOAD_DATE to represent first arrival
        D.SV_LOAD_DATE                = S.SV_LOAD_DATE

WHEN NOT MATCHED BY TARGET
THEN
    INSERT
    (
        AR_COLLECTOR_ID,
        COLLECTOR_NAME,
        COLLECTOR_DESCRIPTION,
        SOURCE_LAST_UPDATE_DATE,
        SOURCE_LAST_UPDATED_BY,
        SOURCE_LAST_UPDATE_LOGIN,
        BZ_LOAD_DATE,
        SV_LOAD_DATE
    )
    VALUES
    (
        S.AR_COLLECTOR_ID,
        S.COLLECTOR_NAME,
        S.COLLECTOR_DESCRIPTION,
        S.SOURCE_LAST_UPDATE_DATE,
        S.SOURCE_LAST_UPDATED_BY,
        S.SOURCE_LAST_UPDATE_LOGIN,
        S.BZ_LOAD_DATE,
        S.SV_LOAD_DATE
    );
GO