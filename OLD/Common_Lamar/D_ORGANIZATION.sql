USE [Oracle_Reporting_P2];
GO

/* ============================================================
   D_ORGANIZATION (Type 1, no SCD, no stored procs)
   Source: bzo.PIM_InvOrgParametersExtractPVO
   Grain: one row per OrganizationId
   ============================================================ */

IF OBJECT_ID('svo.D_ORGANIZATION','U') IS NOT NULL
    DROP TABLE svo.D_ORGANIZATION;
GO

CREATE TABLE svo.D_ORGANIZATION
(
    INVENTORY_ORG_SK          BIGINT IDENTITY(1,1) NOT NULL,
    ORGANIZATION_ID           BIGINT        NOT NULL,   -- Natural key

    ORGANIZATION_CODE         VARCHAR(18)    NULL,
    INVENTORY_FLAG            VARCHAR(1)     NOT NULL,

    BUSINESS_UNIT_ID          BIGINT         NULL,
    LEGAL_ENTITY_ID           BIGINT         NULL,
    MASTER_ORGANIZATION_ID    BIGINT         NULL,
    SOURCE_ORGANIZATION_ID    BIGINT         NULL,

    BZ_LOAD_DATE              DATE           NULL,
    SV_LOAD_DATE              DATE           NOT NULL DEFAULT CAST(GETDATE() AS DATE),

    CONSTRAINT PK_D_ORGANIZATION
        PRIMARY KEY CLUSTERED (INVENTORY_ORG_SK)
        ON [FG_SilverDim],

    CONSTRAINT UK_D_ORGANIZATION_NK
        UNIQUE (ORGANIZATION_ID)
)
ON [FG_SilverDim];
GO

CREATE NONCLUSTERED INDEX IX_D_ORGANIZATION_ORGID
ON svo.D_ORGANIZATION (ORGANIZATION_ID)
ON [FG_SilverDim];
GO


/* ============================================================
   LOAD (repeatable, no MERGE):
   - Clears table
   - Inserts plug row SK=0
   - Inserts current source rows
   ============================================================ */

TRUNCATE TABLE svo.D_ORGANIZATION;
GO

SET IDENTITY_INSERT svo.D_ORGANIZATION ON;

INSERT INTO svo.D_ORGANIZATION
(
    INVENTORY_ORG_SK,
    ORGANIZATION_ID,
    ORGANIZATION_CODE,
    INVENTORY_FLAG,
    BUSINESS_UNIT_ID,
    LEGAL_ENTITY_ID,
    MASTER_ORGANIZATION_ID,
    SOURCE_ORGANIZATION_ID,
    BZ_LOAD_DATE,
    SV_LOAD_DATE
)
VALUES
(
    0,
    0,
    'UNKNOWN',
    'N',
    NULL,
    NULL,
    NULL,
    NULL,
    NULL,
    CAST(GETDATE() AS DATE)
);

SET IDENTITY_INSERT svo.D_ORGANIZATION OFF;
GO

INSERT INTO svo.D_ORGANIZATION
(
    ORGANIZATION_ID,
    ORGANIZATION_CODE,
    INVENTORY_FLAG,
    BUSINESS_UNIT_ID,
    LEGAL_ENTITY_ID,
    MASTER_ORGANIZATION_ID,
    SOURCE_ORGANIZATION_ID,
    BZ_LOAD_DATE,
    SV_LOAD_DATE
)
SELECT
    p.OrganizationId                            AS ORGANIZATION_ID,
    p.OrganizationCode                          AS ORGANIZATION_CODE,
    p.InventoryFlag                             AS INVENTORY_FLAG,
    p.BusinessUnitId                            AS BUSINESS_UNIT_ID,
    p.LegalEntityId                             AS LEGAL_ENTITY_ID,
    p.MasterOrganizationId                      AS MASTER_ORGANIZATION_ID,
    p.SourceOrganizationId                      AS SOURCE_ORGANIZATION_ID,
    CAST(p.AddDateTime AS DATE)                 AS BZ_LOAD_DATE,
    CAST(GETDATE() AS DATE)                     AS SV_LOAD_DATE
FROM bzo.PIM_InvOrgParametersExtractPVO p
WHERE p.OrganizationId IS NOT NULL;
GO
