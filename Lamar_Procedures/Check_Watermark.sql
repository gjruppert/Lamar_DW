/* Database-independent: uses current database context */
SELECT [TABLE_NAME]
      ,[LAST_WATERMARK]
      ,[CRE_DATE]
      ,[UDT_DATE]
  FROM etl.ETL_WATERMARK
  ORDER BY TABLE_NAME;
