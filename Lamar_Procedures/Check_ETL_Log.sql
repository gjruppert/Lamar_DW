/* Database-independent: uses current database context */
SELECT TOP (100) [RUN_ID]
      ,[PROC_NAME]
      ,[TARGET_OBJECT]
      ,[ASOF_DATE]
      ,[START_DTTM]
      ,[END_DTTM]
      ,[STATUS]
      ,[ROW_INSERTED]
      ,[ROW_EXPIRED]
      ,[ROW_UPDATED]
      ,[ROW_UPDATED_T1]
      ,[ERROR_MESSAGE]
  FROM etl.ETL_RUN
  ORDER BY RUN_ID DESC;
