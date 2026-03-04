
-- D_HOLD_CODE
IF OBJECT_ID('svo.D_HOLD_CODE','U') IS NOT NULL DROP TABLE svo.D_HOLD_CODE;
CREATE TABLE svo.D_HOLD_CODE
(
  HOLD_CODE_SK            BIGINT IDENTITY(1,1) NOT NULL PRIMARY KEY,
  HOLD_CODE_ID            BIGINT NOT NULL,    -- OM_HoldCodeExtractPVO.HoldHoldCodeId
  HOLD_TRANSLATION_ID     BIGINT NOT NULL,    -- OM_HoldCodeExtractPVO.HoldTranslationHoldCodeId
  HOLD_CODE               VARCHAR(30) NOT NULL,
  HOLD_NAME               VARCHAR(240) NULL,
  HOLD_DESCRIPTION        VARCHAR(1000) NULL,
  LAST_UPDATE_DATE        DATE NOT NULL,
  LAST_UPDATED_BY         VARCHAR(64) NOT NULL,
  BZ_LOAD_DATE            DATE NOT NULL,
  SV_LOAD_DATE            DATE NOT NULL
) ON FG_SilverDim;
GO

-- Plug row
IF NOT EXISTS (SELECT 1 FROM svo.D_HOLD_CODE WHERE HOLD_CODE_SK = 0)
BEGIN
  SET IDENTITY_INSERT svo.D_HOLD_CODE ON;
  INSERT INTO svo.D_HOLD_CODE
  (HOLD_CODE_SK, HOLD_CODE_ID, HOLD_TRANSLATION_ID, HOLD_CODE, HOLD_NAME, HOLD_DESCRIPTION, LAST_UPDATE_DATE, LAST_UPDATED_BY, BZ_LOAD_DATE, SV_LOAD_DATE)
  VALUES (0, -1, -1, 'UNK', 'Unknown', NULL, '0001-01-01', 'Unknown', '0001-01-01', CAST(GETDATE() AS DATE));
  SET IDENTITY_INSERT svo.D_HOLD_CODE OFF;
END
GO

---------

INSERT INTO svo.D_HOLD_CODE
(HOLD_CODE_ID, HOLD_TRANSLATION_ID, HOLD_CODE, HOLD_NAME, HOLD_DESCRIPTION, LAST_UPDATE_DATE, LAST_UPDATED_BY, BZ_LOAD_DATE, SV_LOAD_DATE)
SELECT
  H.HoldHoldCodeId,
  H.HoldTranslationHoldCodeId,
  H.HoldHoldCode,
  H.HoldTranslationHoldName,
  H.HoldTranslationHoldDescription,
  H.HoldLastUpdateDate,
  H.HoldLastUpdatedBy,
  CAST(H.AddDateTime AS DATE),
  CAST(GETDATE() AS DATE)
FROM bzo.OM_HoldCodeExtractPVO H
LEFT JOIN svo.D_HOLD_CODE D
  ON D.HOLD_CODE_ID = H.HoldHoldCodeId
 AND D.HOLD_TRANSLATION_ID = H.HoldTranslationHoldCodeId
WHERE D.HOLD_CODE_SK IS NULL;
-- Source columns: HoldHoldCodeId, HoldHoldCode, HoldTranslationHoldCodeId, HoldTranslationHoldName, HoldTranslationHoldDescription, HoldLastUpdateDate, HoldLastUpdatedBy, AddDateTime:contentReference[oaicite:2]{index=2}
