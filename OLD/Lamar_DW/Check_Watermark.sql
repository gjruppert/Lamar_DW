SELECT TOP (1000) [TABLE_NAME]
      ,[LAST_WATERMARK]
      ,[CRE_DATE]
      ,[UDT_DATE]
  FROM [Oracle_Reporting_P2].[etl].[ETL_WATERMARK]

  order by 2 desc