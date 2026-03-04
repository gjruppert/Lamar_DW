/* =========================================================
   Reset watermarks for 0-to-100 full reload
   Run AFTER Create_Tables_DDL.sql and BEFORE the load runners.
   Sets LAST_WATERMARK = '1900-01-01' for all svo.* tables so
   that the next run of each SP does a full load (e.g. F_GL_LINES
   truncates and reloads when watermark is 1900-01-01).
   ========================================================= */
SET NOCOUNT ON;

IF OBJECT_ID('etl.ETL_WATERMARK', 'U') IS NULL
BEGIN
    RAISERROR('etl.ETL_WATERMARK does not exist. Run 00_Prerequisites/ETL_WATERMARK.sql first.', 16, 1);
    RETURN;
END

UPDATE etl.ETL_WATERMARK
SET LAST_WATERMARK = '1900-01-01',
    UDT_DATE      = SYSDATETIME()
WHERE TABLE_NAME LIKE 'svo.%';

PRINT 'Reset_Watermarks_For_Full_Reload: Updated ' + CAST(@@ROWCOUNT AS VARCHAR(20)) + ' watermark(s) to 1900-01-01.';
GO
