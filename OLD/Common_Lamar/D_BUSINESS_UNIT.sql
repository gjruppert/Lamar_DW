USE [Oracle_Reporting_P2];
GO
/* =============
   BUSINESS UNIT
   ============= */
IF OBJECT_ID(N'svo.D_BUSINESS_UNIT', 'U') IS NOT NULL
    DROP TABLE svo.D_BUSINESS_UNIT;
GO


CREATE TABLE svo.D_BUSINESS_UNIT
(
  BUSINESS_UNIT_SK                         BIGINT IDENTITY(1,1) NOT NULL PRIMARY KEY,

  BUSINESS_UNIT_ID                         BIGINT        NOT NULL,   -- BusinessUnitId
  BUSINESS_UNIT_NAME                       VARCHAR(240)  NULL,       -- BusinessUnitName
  BUSINESS_UNIT_ENTERPRISE_ID              BIGINT        NULL,       -- BusinessUnitEnterpriseId
  BUSINESS_UNIT_LEGAL_ENTITY_ID            VARCHAR(150)  NULL,       -- BusinessUnitLegalEntityId (source is varchar)
  BUSINESS_UNIT_LOCATION_ID                BIGINT        NULL,       -- BusinessUnitLocationId
  BUSINESS_UNIT_PRIMARY_LEDGER_ID          VARCHAR(150)  NULL,       -- BusinessUnitPrimaryLedgerId
  BUSINESS_UNIT_DEFAULT_CURRENCY_CODE      VARCHAR(150)  NULL,       -- BusinessUnitDefaultCurrencyCode
  BUSINESS_UNIT_DEFAULT_SET_ID             VARCHAR(150)  NULL,       -- BusinessUnitDefaultSetId
  BUSINESS_UNIT_ENABLED_FOR_HR_FLAG        VARCHAR(150)  NULL,       -- BusinessUnitEnabledForHrFlag
  BUSINESS_UNIT_STATUS                     VARCHAR(30)   NULL,       -- BusinessUnitStatus
  BUSINESS_UNIT_CREATED_BY                 VARCHAR(64)   NULL,       -- BusinessUnitCreatedBy
  BUSINESS_UNIT_CREATION_DATE              DATETIME      NULL,       -- BusinessUnitCreationDate
  BUSINESS_UNIT_DATE_FROM                  DATE          NULL,       -- BusinessUnitDateFrom
  BUSINESS_UNIT_DATE_TO                    DATE          NULL,       -- BusinessUnitDateTo
  BUSINESS_UNIT_LAST_UPDATE_DATE                  DATETIME     NULL,  -- BusinessUnitLastUpdateDate
  BUSINESS_UNIT_LAST_UPDATE_LOGIN                 VARCHAR(32)  NULL,  -- BusinessUnitLastUpdateLogin
  BUSINESS_UNIT_LAST_UPDATED_BY                   VARCHAR(64)  NULL,  -- BusinessUnitLastUpdatedBy
  FIN_BU_BUSINESS_UNIT_ID                         BIGINT       NULL,  -- FinBuBusinessUnitId

  BZ_LOAD_DATE                                    DATE         NOT NULL,
  SV_LOAD_DATE                                    DATE         NOT NULL
) ON FG_SilverDim;
GO

-- Uniqueness on the business key
CREATE UNIQUE NONCLUSTERED INDEX UX_D_BUSINESS_UNIT_ID
  ON svo.D_BUSINESS_UNIT (BUSINESS_UNIT_ID)
  ON FG_SilverDim;
GO

-- Plug row (PK = 0)
-- Remove an existing plug row if present (optional)
DELETE FROM svo.D_BUSINESS_UNIT WHERE BUSINESS_UNIT_SK = 0;

SET IDENTITY_INSERT svo.D_BUSINESS_UNIT ON;

INSERT INTO svo.D_BUSINESS_UNIT
(
  BUSINESS_UNIT_SK,
  BUSINESS_UNIT_ID,
  BUSINESS_UNIT_NAME,
  BUSINESS_UNIT_ENTERPRISE_ID,
  BUSINESS_UNIT_LEGAL_ENTITY_ID,
  BUSINESS_UNIT_LOCATION_ID,
  BUSINESS_UNIT_PRIMARY_LEDGER_ID,
  BUSINESS_UNIT_DEFAULT_CURRENCY_CODE,
  BUSINESS_UNIT_DEFAULT_SET_ID,
  BUSINESS_UNIT_ENABLED_FOR_HR_FLAG,
  BUSINESS_UNIT_STATUS,
  BUSINESS_UNIT_CREATED_BY,
  BUSINESS_UNIT_CREATION_DATE,
  BUSINESS_UNIT_DATE_FROM,
  BUSINESS_UNIT_DATE_TO,
  FIN_BU_BUSINESS_UNIT_ID,
  BZ_LOAD_DATE,
  SV_LOAD_DATE
)
VALUES
(
  0,                 -- BUSINESS_UNIT_SK
  -1,                -- BUSINESS_UNIT_ID
  'Unknown',         -- BUSINESS_UNIT_NAME
  -1,                -- BUSINESS_UNIT_ENTERPRISE_ID
  'UNK',             -- BUSINESS_UNIT_LEGAL_ENTITY_ID
  -1,                -- BUSINESS_UNIT_LOCATION_ID
  'UNK',             -- BUSINESS_UNIT_PRIMARY_LEDGER_ID
  'UNK',             -- BUSINESS_UNIT_DEFAULT_CURRENCY_CODE
  'UNK',             -- BUSINESS_UNIT_DEFAULT_SET_ID
  'UNK',             -- BUSINESS_UNIT_ENABLED_FOR_HR_FLAG
  'Unknown',         -- BUSINESS_UNIT_STATUS
  'Unknown',         -- BUSINESS_UNIT_CREATED_BY
  '1753-01-01',      -- BUSINESS_UNIT_CREATION_DATE (datetime)
  '0001-01-01',      -- BUSINESS_UNIT_DATE_FROM (date)
  '0001-01-01',      -- BUSINESS_UNIT_DATE_TO   (date)
  -1,                -- FIN_BU_BUSINESS_UNIT_ID
  '0001-01-01',      -- BZ_LOAD_DATE
  CAST(GETDATE() AS DATE)  -- SV_LOAD_DATE
);

SET IDENTITY_INSERT svo.D_BUSINESS_UNIT OFF;


INSERT INTO svo.D_BUSINESS_UNIT (
     BUSINESS_UNIT_ID,
     BUSINESS_UNIT_NAME,
     BUSINESS_UNIT_ENTERPRISE_ID,
     BUSINESS_UNIT_LEGAL_ENTITY_ID,
     BUSINESS_UNIT_LOCATION_ID,
     BUSINESS_UNIT_PRIMARY_LEDGER_ID,
     BUSINESS_UNIT_DEFAULT_CURRENCY_CODE,
     BUSINESS_UNIT_DEFAULT_SET_ID,
     BUSINESS_UNIT_ENABLED_FOR_HR_FLAG,
     BUSINESS_UNIT_STATUS,
     BUSINESS_UNIT_CREATED_BY,
     BUSINESS_UNIT_CREATION_DATE,
     BUSINESS_UNIT_DATE_FROM,
     BUSINESS_UNIT_DATE_TO,
     FIN_BU_BUSINESS_UNIT_ID,
     BZ_LOAD_DATE,
     SV_LOAD_DATE
)
SELECT 
   BusinessUnitId,
   BusinessUnitName,
   BusinessUnitEnterpriseId,
   BusinessUnitLegalEntityId,
   BusinessUnitLocationId,
   BusinessUnitPrimaryLedgerId,
   BusinessUnitDefaultCurrencyCode,
   BusinessUnitDefaultSetId,
   BusinessUnitEnabledForHrFlag,
   BusinessUnitStatus,
   BusinessUnitCreatedBy,
   CAST(BusinessUnitCreationDate AS DATE),
   CAST(BusinessUnitDateFrom AS DATE),
   CAST(BusinessUnitDateTo AS DATE),
   FinBuBusinessUnitId,
   CAST(GETDATE()-9 AS DATE) AS BZ_LOAD_DATE,
   CAST(GETDATE() AS DATE) AS SV_LOAD_DATE
FROM Oracle_Reporting_P2.bzo.CMN_BusinessUnitPVO;
GO

/* =========================
   BUSINESS_UNIT
   ========================= */
-- Unique index (leave as-is if already created)
IF NOT EXISTS (
    SELECT 1
    FROM sys.indexes
    WHERE name = 'UX_D_BUSINESS_UNIT_ID'
      AND object_id = OBJECT_ID('svo.D_BUSINESS_UNIT')
)
BEGIN
    CREATE UNIQUE NONCLUSTERED INDEX UX_D_BUSINESS_UNIT_ID
        ON svo.D_BUSINESS_UNIT (BUSINESS_UNIT_ID)
        ON FG_SilverDim;  -- drop this clause if the FG doesn't exist
END;
GO

-- Insert the “unknown” row with only required columns
IF NOT EXISTS (SELECT 1 FROM svo.D_BUSINESS_UNIT WHERE BUSINESS_UNIT_SK = 0)
BEGIN
    SET IDENTITY_INSERT svo.D_BUSINESS_UNIT ON;

    INSERT INTO svo.D_BUSINESS_UNIT (
          BUSINESS_UNIT_SK
        , BUSINESS_UNIT_ID
        , BZ_LOAD_DATE
        , SV_LOAD_DATE
    )
    VALUES (
          0
        , '-1'
        , CAST('0001-01-01' AS DATE)
        , CAST(GETDATE() AS DATE)
    );

    SET IDENTITY_INSERT svo.D_BUSINESS_UNIT OFF;
END;
