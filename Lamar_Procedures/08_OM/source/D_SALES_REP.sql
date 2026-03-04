/* ============================================================
   D_SALES_REP Dimension
   ============================================================ */
IF OBJECT_ID('svo.D_SALES_REP', 'U') IS NOT NULL
    DROP TABLE svo.D_SALES_REP;
GO

CREATE TABLE svo.D_SALES_REP
(
    SALES_REP_SK                     BIGINT IDENTITY(1,1) NOT NULL,
    SALES_REP_ID                     BIGINT NULL,             -- ResourceSalesrepId
    SALES_REP_NUMBER                 VARCHAR(100) NULL,       -- SalesrepNumber
    PARTY_ID                         BIGINT NULL,
    PARTY_NAME                       NVARCHAR(500) NULL,
    PERSON_FIRST_NAME                NVARCHAR(250) NULL,
    PERSON_LAST_NAME                 NVARCHAR(250) NULL,
    EMAIL_ADDRESS                    NVARCHAR(320) NULL,
    RESOURCE_ID                      BIGINT NULL,
    RESOURCE_LAST_UPDATE_DATE        DATE NULL,
    RESOURCE_LAST_UPDATED_BY         NVARCHAR(100) NULL,
    RESOURCE_LAST_UPDATE_LOGIN       NVARCHAR(100) NULL,
    RESOURCE_STATUS                  NVARCHAR(50) NULL,
    START_DATE_ACTIVE                DATE NULL,
    BZ_LOAD_DATE                     DATE NOT NULL,
    SV_LOAD_DATE                     DATE NOT NULL,
    CONSTRAINT PK_D_SALES_REP PRIMARY KEY CLUSTERED (SALES_REP_SK) ON [FG_SilverDim]
) ON [FG_SilverDim];
GO

/* ============================================================
   Populate D_SALES_REP
   ============================================================ */
INSERT INTO svo.D_SALES_REP
(
    SALES_REP_ID,
    SALES_REP_NUMBER,
    PARTY_ID,
    PARTY_NAME,
    PERSON_FIRST_NAME,
    PERSON_LAST_NAME,
    EMAIL_ADDRESS,
    RESOURCE_ID,
    RESOURCE_LAST_UPDATE_DATE,
    RESOURCE_LAST_UPDATED_BY,
    RESOURCE_LAST_UPDATE_LOGIN,
    RESOURCE_STATUS,
    START_DATE_ACTIVE,
    BZ_LOAD_DATE,
    SV_LOAD_DATE
)
SELECT
    SR.ResourceSalesrepId                 AS SALES_REP_ID,
    SR.SalesrepNumber                     AS SALES_REP_NUMBER,
    SR.PartyId                            AS PARTY_ID,
    SR.PartyName                          AS PARTY_NAME,
    SR.PersonFirstName                    AS PERSON_FIRST_NAME,
    SR.PersonLastName                     AS PERSON_LAST_NAME,
    SR.EmailAddress                       AS EMAIL_ADDRESS,
    SR.ResourceId                         AS RESOURCE_ID,
    CAST(SR.ResourceSalesrepPEOLastUpdateDate AS DATE) AS RESOURCE_LAST_UPDATE_DATE,
    SR.ResourceSalesrepPEOLastUpdatedBy   AS RESOURCE_LAST_UPDATED_BY,
    SR.ResourceSalesrepPEOLastUpdateLogin AS RESOURCE_LAST_UPDATE_LOGIN,
    SR.ResourceSalesrepPEOStatus          AS RESOURCE_STATUS,
    CAST(SR.StartDateActive AS DATE)      AS START_DATE_ACTIVE,
    CAST(SR.AddDateTime AS DATE)          AS BZ_LOAD_DATE,
    CAST(GETDATE() AS DATE)               AS SV_LOAD_DATE
FROM bzo.OM_SalesRep SR;
GO

/* ============================================================
   Indexes
   ============================================================ */
CREATE UNIQUE NONCLUSTERED INDEX UX_D_SALES_REP_ID
    ON svo.D_SALES_REP (SALES_REP_ID)
    ON [FG_SilverDim];
GO

/* ============================================================
   Plug Row (Unknown Sales Rep)
   ============================================================ */
IF NOT EXISTS (SELECT 1 FROM svo.D_SALES_REP WHERE SALES_REP_SK = 0)
BEGIN
    SET IDENTITY_INSERT svo.D_SALES_REP ON;

    INSERT INTO svo.D_SALES_REP
    (
        SALES_REP_SK,
        SALES_REP_ID,
        SALES_REP_NUMBER,
        PARTY_ID,
        PARTY_NAME,
        PERSON_FIRST_NAME,
        PERSON_LAST_NAME,
        EMAIL_ADDRESS,
        RESOURCE_ID,
        RESOURCE_LAST_UPDATE_DATE,
        RESOURCE_LAST_UPDATED_BY,
        RESOURCE_LAST_UPDATE_LOGIN,
        RESOURCE_STATUS,
        START_DATE_ACTIVE,
        BZ_LOAD_DATE,
        SV_LOAD_DATE
    )
    VALUES
    (
        0,
        -1,
        'UNKNOWN',
        -1,
        'Unknown',
        'Unknown',
        'Unknown',
        'unknown@unknown.com',
        -1,
        '0001-01-01',
        'Unknown',
        'Unknown',
        'Unknown',
        '0001-01-01',
        CAST('0001-01-01' AS DATE),
        CAST(GETDATE() AS DATE)
    );

    SET IDENTITY_INSERT svo.D_SALES_REP OFF;
END;
GO
