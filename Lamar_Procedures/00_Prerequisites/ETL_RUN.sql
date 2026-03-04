/* =========================================================
   ETL Run Log (create once)
   Tracks each procedure execution for audit and retry.
   ========================================================= */
IF SCHEMA_ID('etl') IS NULL
    EXEC('CREATE SCHEMA etl AUTHORIZATION dbo;');
GO

IF OBJECT_ID('etl.ETL_RUN', 'U') IS NULL
BEGIN
    CREATE TABLE etl.ETL_RUN
    (
          RUN_ID        bigint IDENTITY(1,1) NOT NULL CONSTRAINT PK_ETL_RUN PRIMARY KEY
        , PROC_NAME     sysname              NOT NULL
        , TARGET_OBJECT sysname              NOT NULL
        , ASOF_DATE     date                 NULL
        , START_DTTM    datetime2(0)         NOT NULL CONSTRAINT DF_ETL_RUN_START DEFAULT (SYSDATETIME())
        , END_DTTM      datetime2(0)         NULL
        , STATUS        varchar(20)          NOT NULL CONSTRAINT DF_ETL_RUN_STATUS DEFAULT ('STARTED')
        , ROW_INSERTED  int                  NULL
        , ROW_EXPIRED   int                  NULL
        , ROW_UPDATED   int                  NULL
        , ROW_UPDATED_T1 int                 NULL   -- Type 1 updates (compat with Lamar_DW)
        , ERROR_MESSAGE nvarchar(4000)       NULL
        , BATCH_ID      int                 NULL
        , TABLE_BRIDGE_ID int               NULL
    );
END
GO

-- Add BATCH_ID and TABLE_BRIDGE_ID if table already existed (backward compatibility)
IF OBJECT_ID('etl.ETL_RUN', 'U') IS NOT NULL
BEGIN
    IF COL_LENGTH('etl.ETL_RUN', 'BATCH_ID') IS NULL
        ALTER TABLE etl.ETL_RUN ADD BATCH_ID int NULL;
    IF COL_LENGTH('etl.ETL_RUN', 'TABLE_BRIDGE_ID') IS NULL
        ALTER TABLE etl.ETL_RUN ADD TABLE_BRIDGE_ID int NULL;
END
GO
