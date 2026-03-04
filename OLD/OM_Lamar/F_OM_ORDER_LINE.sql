-- F_OM_ORDER_LINE (grain: one row per order line)
IF OBJECT_ID('svo.F_OM_ORDER_LINE','U') IS NOT NULL DROP TABLE svo.F_OM_ORDER_LINE;
CREATE TABLE svo.F_OM_ORDER_LINE
(
  OM_ORDER_LINE_PK            BIGINT IDENTITY(1,1) NOT NULL PRIMARY KEY,

  -- Degenerate keys (useful in analysis)
  ORDER_HEADER_SK                   BIGINT NOT NULL,  -- OM_HeaderExtractPVO.HeaderId
  ORDER_LINE_SK                     BIGINT NOT NULL,  -- OM_LineExtractPVO.LineId

  -- Dates via D_CALENDAR (conformed)
  LINE_SHIP_DATE_KEY          INT NOT NULL,         -- yyyymmdd from LineActualShipDate or 0

  -- Measures
  LINE_ORDERED_QTY            BIGINT NOT NULL,
  LINE_EXTENDED_AMOUNT        DECIMAL(29,4) NULL,
  LINE_UNIT_LIST_PRICE        DECIMAL(29,4) NULL,
  LINE_UNIT_SELLING_PRICE     DECIMAL(29,4) NULL,

  -- Line attributes often queried
  LINE_STATUS_CODE            VARCHAR(30) NOT NULL,
  LINE_CATEGORY_CODE          VARCHAR(30) NOT NULL,

  BZ_LOAD_DATE                DATE NOT NULL,
  SV_LOAD_DATE                DATE NOT NULL
) ON FG_SilverFact;
GO

-- Plug-safe defaults (optional constraints omitted here for brevity)
INSERT INTO svo.F_OM_ORDER_LINE
( ORDER_HEADER_SK, 
 ORDER_LINE_SK,
 LINE_SHIP_DATE_KEY,
 LINE_ORDERED_QTY, 
 LINE_EXTENDED_AMOUNT, 
 LINE_UNIT_LIST_PRICE, 
 LINE_UNIT_SELLING_PRICE,
 LINE_STATUS_CODE, 
 LINE_CATEGORY_CODE,
 BZ_LOAD_DATE, 
 SV_LOAD_DATE)
SELECT
  ISNULL(OH.ORDER_HEADER_SK,0) AS ORDER_HEADER_SK,
  ISNULL(OL.ORDER_LINE_SK,0)   AS ORDER_LINE_SK,
  CONVERT(INT, FORMAT(ISNULL(L.LineActualShipDate,'0001-01-01'),'yyyyMMdd')) AS LineActualShipDate,
  ISNULL(L.LineOrderedQty,0),
  ISNULL(L.LineExtendedAmount,0),
  ISNULL(L.LineUnitListPrice,0),
  ISNULL(L.LineUnitSellingPrice,0),
  ISNULL(L.LineStatusCode,'UNK'),
  ISNULL(L.LineCategoryCode,'UNK'),
  CAST(L.AddDateTime AS DATE),
  CAST(GETDATE() AS DATE)
FROM bzo.OM_LineExtractPVO L
LEFT JOIN svo.D_OM_ORDER_HEADER OH   ON OH.ORDER_HEADER_ID = L.LineHeaderId 
LEFT JOIN svo.D_OM_ORDER_LINE   OL   ON OL.ORDER_LINE_ID = L.LineId
WHERE 1=1;
