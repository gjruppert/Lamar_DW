/* =========================================================
   Logging framework (create once)
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
        , ERROR_MESSAGE nvarchar(4000)       NULL
    );
END
GO
