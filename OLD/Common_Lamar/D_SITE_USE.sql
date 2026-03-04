--------------------------
-- D_SITE_USE
--------------------------
IF OBJECT_ID('svo.D_SITE_USE','U') IS NOT NULL DROP TABLE svo.D_SITE_USE
GO

BEGIN
  CREATE TABLE svo.D_SITE_USE
  (
    SITE_USE_SK              bigint IDENTITY(1,1) NOT NULL PRIMARY KEY,
    SITE_USE                 bigint         NOT NULL,     -- SiteUseId
    CUSTOMER_SITE            bigint         NOT NULL,     -- CustAcctSiteId
    SITE_USE_CODE            varchar(30)    NOT NULL,
    LOCATION                 varchar(150)   NULL,
    PRIMARY_FLAG             varchar(1)     NULL,
    PAYMENT_TERM_ID          bigint         NULL,
    STATUS                   varchar(1)     NOT NULL,
    BZ_LOAD_DATE             date           NOT NULL DEFAULT (CAST(GETDATE() AS date)),
    SV_LOAD_DATE             date           NOT NULL DEFAULT (CAST(GETDATE() AS date))
  ) ON [FG_SilverDim];
  CREATE UNIQUE NONCLUSTERED INDEX UX_D_SITE_USE ON svo.D_SITE_USE(SITE_USE) ON [FG_SilverDim];
END;

IF NOT EXISTS (SELECT 1 FROM svo.D_SITE_USE WHERE SITE_USE_SK=0)
BEGIN
  SET IDENTITY_INSERT svo.D_SITE_USE ON;
  INSERT svo.D_SITE_USE (SITE_USE_SK,SITE_USE,CUSTOMER_SITE,SITE_USE_CODE,STATUS,BZ_LOAD_DATE,SV_LOAD_DATE)
  VALUES (0,-1,-1,'UNK','U',CAST(GETDATE() AS date),CAST(GETDATE() AS date));
  SET IDENTITY_INSERT svo.D_SITE_USE OFF;
END;

--------------------------
-- Load D_SITE_USE
--------------------------
MERGE svo.D_SITE_USE AS D
USING (
  SELECT
    SiteUseId        AS SITE_USE,
    CustAcctSiteId   AS CUSTOMER_SITE,
    SiteUseCode,
    Location,
    PrimaryFlag,
    PaymentTermId,
    Status,
    AddDateTime
  FROM bzo.AR_CustomerAcctSiteUseExtractPVO
) AS S
ON (D.SITE_USE = S.SITE_USE)
WHEN NOT MATCHED BY TARGET THEN
  INSERT (SITE_USE,CUSTOMER_SITE,SITE_USE_CODE,LOCATION,PRIMARY_FLAG,PAYMENT_TERM_ID,STATUS,BZ_LOAD_DATE,SV_LOAD_DATE)
  VALUES (S.SITE_USE,S.CUSTOMER_SITE,S.SiteUseCode,S.Location,S.PrimaryFlag,S.PaymentTermId,S.Status,CAST(S.AddDateTime AS date),CAST(GETDATE() AS date))
WHEN MATCHED THEN
  UPDATE SET
    CUSTOMER_SITE    = S.CUSTOMER_SITE,
    SITE_USE_CODE    = S.SiteUseCode,
    LOCATION         = S.Location,
    PRIMARY_FLAG     = S.PrimaryFlag,
    PAYMENT_TERM_ID  = S.PaymentTermId,
    STATUS           = S.Status,
    BZ_LOAD_DATE     = CAST(S.AddDateTime AS date),
    SV_LOAD_DATE     = CAST(GETDATE() AS date);

