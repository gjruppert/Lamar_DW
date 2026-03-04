USE Oracle_Reporting_P2
--------------------------
-- D_PARTY_SITE
--------------------------
IF OBJECT_ID('svo.D_PARTY_SITE','U') IS NOT NULL DROP TABLE svo.D_PARTY_SITE;
GO

BEGIN
  CREATE TABLE svo.D_PARTY_SITE
  (
    PARTY_SITE_SK            BIGINT IDENTITY(1,1) NOT NULL PRIMARY KEY,
    PARTY_SITE_ID            BIGINT NOT NULL,
    PARTY_ID                 BIGINT NULL,
    PARTY_SITE_NAME          VARCHAR(240) NULL,
    PARTY_SITE_NUMBER        VARCHAR(30) NULL,
    LOCATION_ID              BIGINT NOT NULL,
    OVERALL_PRIMARY_FLAG     VARCHAR(1) NULL, 
    ACTUAL_CONTENT_SOURCE    VARCHAR(30) NULL,
    START_DATE_ACTIVE        DATE NULL,
    END_DATE_ACTIVE          DATE NULL,
    STATUS                   VARCHAR(1) NULL,
    CREATED_BY               VARCHAR(64) NULL,
    LAST_UPDATE_BY           VARCHAR(64) NULL,
    LAST_UPDATE_LOGIN        VARCHAR(64) NULL,
    BZ_LOAD_DATE             DATE         NOT NULL DEFAULT (CAST(GETDATE() AS date)),
    SV_LOAD_DATE             DATE         NOT NULL DEFAULT (CAST(GETDATE() AS date))
  ) ON FG_SilverDim;
  CREATE UNIQUE NONCLUSTERED INDEX UX_D_PARTY_SITE ON svo.D_PARTY_SITE(PARTY_SITE_ID) ON FG_SilverDim
END;

IF NOT EXISTS (SELECT 1 FROM svo.D_PARTY_SITE WHERE PARTY_SITE_SK=0)
BEGIN
    SET IDENTITY_INSERT svo.D_PARTY_SITE ON
    INSERT INTO svo.D_PARTY_SITE
        (PARTY_SITE_SK, PARTY_SITE_ID, PARTY_ID, PARTY_SITE_NAME, PARTY_SITE_NUMBER, LOCATION_ID,
        OVERALL_PRIMARY_FLAG, ACTUAL_CONTENT_SOURCE, START_DATE_ACTIVE, END_DATE_ACTIVE,
        STATUS, CREATED_BY, LAST_UPDATE_BY, LAST_UPDATE_LOGIN, BZ_LOAD_DATE, SV_LOAD_DATE)      
        VALUES
        ( 0, -1, -1, 'Unknown Party Site','UNK', -1, 'N','UNK', '0001-01-01','0001-01-01','U',              
        'SYSTEM', 'SYSTEM','UNK','0001-01-01',CAST(GETDATE() AS DATE) );
     SET IDENTITY_INSERT svo.D_PARTY_SITE OFF
END

GO

--------------------------
-- Load D_PARTY_SITE (T1)
--------------------------
MERGE svo.D_PARTY_SITE AS D
USING (
  SELECT
       PartySiteId                    AS PARTY_SITE_ID
      ,ISNULL(PartySiteName,'UNK')    AS PARTY_SITE_NAME
      ,PartySiteNumber
      ,LocationId
      ,OverallPrimaryFlag
      ,PartyId
      ,ActualContentSource 
      ,StartDateActive
      ,EndDateActive
      ,Status
      ,CreatedBy
      ,LastUpdatedBy
      ,LastUpdateLogin
      ,CAST(AddDateTime AS DATE) BZ_LOAD_DATE
  FROM Oracle_Reporting_P2.bzo.AR_PartySiteExtractPVO
) AS S
ON (D.PARTY_SITE_ID = S.PARTY_SITE_ID)
WHEN NOT MATCHED BY TARGET THEN
  INSERT (PARTY_SITE_ID, PARTY_ID, PARTY_SITE_NAME, PARTY_SITE_NUMBER, LOCATION_ID,
    OVERALL_PRIMARY_FLAG, ACTUAL_CONTENT_SOURCE, START_DATE_ACTIVE, END_DATE_ACTIVE,
    STATUS, CREATED_BY, LAST_UPDATE_BY, LAST_UPDATE_LOGIN, BZ_LOAD_DATE, SV_LOAD_DATE)
  VALUES (S.PARTY_SITE_ID,S.PartyId,S.PARTY_SITE_NAME,S.PartySiteNumber,S.LocationId, S.OverallPrimaryFlag,
          S.ActualContentSource, S.StartDateActive, S.EndDateActive, S.Status, S.CreatedBy, S.LastUpdatedBy,
          S.LastUpdateLogin, S.BZ_LOAD_DATE, CAST(GETDATE() AS DATE))
WHEN MATCHED THEN
  UPDATE SET
    PARTY_ID = S.PartyId, 
    PARTY_SITE_NAME =S.PARTY_SITE_NAME, 
    PARTY_SITE_NUMBER = S.PartySiteNumber, 
    LOCATION_ID = S.LocationId,
    OVERALL_PRIMARY_FLAG = S.OverallPrimaryFlag, 
    ACTUAL_CONTENT_SOURCE = S.ActualContentSource, 
    START_DATE_ACTIVE = S.StartDateActive, 
    END_DATE_ACTIVE = S.EndDateActive,
    STATUS = S.Status, 
    CREATED_BY = S.CreatedBy, 
    LAST_UPDATE_BY = S.LastUpdatedBy, 
    LAST_UPDATE_LOGIN = S.LastUpdateLogin, 
    BZ_LOAD_DATE = S.BZ_LOAD_DATE,
    SV_LOAD_DATE = CAST(GETDATE() AS DATE);

