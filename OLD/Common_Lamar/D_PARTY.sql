
--------------------------
-- D_PARTY
--------------------------
IF OBJECT_ID('svo.D_PARTY','U') IS NOT NULL DROP TABLE svo.D_PARTY;
GO

BEGIN
  CREATE TABLE svo.D_PARTY
  (
    PARTY_SK                 bigint IDENTITY(1,1) NOT NULL PRIMARY KEY,
    PARTY_ID                 bigint       NOT NULL,           -- PartyId
    PARTY_NUMBER             varchar(30)  NOT NULL,
    PARTY_NAME               varchar(360) NOT NULL,
    PARTY_TYPE               varchar(30)  NOT NULL,
    STATUS                   varchar(1)   NOT NULL,
    COUNTRY                  varchar(2)   NULL,
    STATE                    varchar(60)  NULL,
    CITY                     varchar(60)  NULL,
    POSTAL_CODE              varchar(60)  NULL,
    CREATED_BY               varchar(64)  NULL,
    CREATION_DATE            date     NULL,
    LAST_UPDATE_DATE         date     NULL,
    BZ_LOAD_DATE             date         NOT NULL DEFAULT (CAST(GETDATE() AS date)),
    SV_LOAD_DATE             date         NOT NULL DEFAULT (CAST(GETDATE() AS date))
  ) ON [FG_SilverDim];
  CREATE UNIQUE NONCLUSTERED INDEX UX_D_PARTY ON svo.D_PARTY(PARTY_ID) ON [FG_SilverDim];
END;

IF NOT EXISTS (SELECT 1 FROM svo.D_PARTY WHERE PARTY_SK=0)
BEGIN
  SET IDENTITY_INSERT svo.D_PARTY ON;
  INSERT svo.D_PARTY (PARTY_SK,PARTY_ID,PARTY_NUMBER,PARTY_NAME,PARTY_TYPE,STATUS,CREATED_BY,CREATION_DATE,LAST_UPDATE_DATE,BZ_LOAD_DATE,SV_LOAD_DATE)
  VALUES (0,-1,'UNK','Unknown','U','U','System',CAST(GETDATE() AS date),CAST(GETDATE() AS date),CAST(GETDATE() AS date),CAST(GETDATE() AS date));
  SET IDENTITY_INSERT svo.D_PARTY OFF;
END;

--------------------------
-- Load D_PARTY (T1)
--------------------------
MERGE svo.D_PARTY AS D
USING (
  SELECT
    PartyId         AS PARTY_ID,
    PartyNumber     AS PARTY_NUMBER,
    PartyName       AS PARTY_NAME,
    PartyType       AS PARTY_TYPE,
    Status          AS STATUS,
    ISNULL(Country,'UNK') AS COUNTRY, 
    ISNULL(State, 'UN') AS STATE,
    ISNULL(City,'UNK') AS CITY,
    ISNULL(PostalCode,'00000') POSTAL_CODE,
    CreatedBy, 
    CreationDate, 
    LastUpdateDate,
    AddDateTime
  FROM bzo.AR_PartyExtractPVO
) AS S
ON (D.PARTY_ID = S.PARTY_ID)
WHEN NOT MATCHED BY TARGET THEN
  INSERT (PARTY_ID, PARTY_NUMBER, PARTY_NAME, PARTY_TYPE, STATUS, COUNTRY, STATE, CITY, POSTAL_CODE, CREATED_BY, CREATION_DATE, LAST_UPDATE_DATE, BZ_LOAD_DATE, SV_LOAD_DATE)
  VALUES (S.PARTY_ID,S.PARTY_NUMBER,S.PARTY_NAME,S.PARTY_TYPE,S.STATUS,S.Country,S.State,S.City,S.Postal_Code,S.CreatedBy,S.CreationDate,S.LastUpdateDate, CAST(S.AddDateTime AS date), CAST(GETDATE() AS date))
WHEN MATCHED THEN
  UPDATE SET
    PARTY_NUMBER   = S.PARTY_NUMBER,
    PARTY_NAME     = S.PARTY_NAME,
    PARTY_TYPE     = S.PARTY_TYPE,
    STATUS         = S.STATUS,
    COUNTRY        = S.Country,
    STATE          = S.State,
    CITY           = S.City,
    POSTAL_CODE    = S.Postal_Code,
    CREATED_BY     = S.CreatedBy,
    CREATION_DATE   = CAST(S.CreationDate AS date),
    LAST_UPDATE_DATE = CAST(S.LastUpdateDate AS date),
    BZ_LOAD_DATE   =  CAST(S.AddDateTime AS date),
    SV_LOAD_DATE   = CAST(GETDATE() AS date);

