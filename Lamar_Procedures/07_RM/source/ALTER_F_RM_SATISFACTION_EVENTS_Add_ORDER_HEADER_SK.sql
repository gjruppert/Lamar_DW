/* =========================================================
   Add ORDER_HEADER_SK to svo.F_RM_SATISFACTION_EVENTS
   Run once on databases where the table was created before
   this column was added. Safe to run: adds column only if missing.
   ========================================================= */
SET NOCOUNT ON;

IF NOT EXISTS (
    SELECT 1 FROM sys.columns c
    INNER JOIN sys.tables t ON c.object_id = t.object_id
    INNER JOIN sys.schemas s ON t.schema_id = s.schema_id
    WHERE s.name = N'svo' AND t.name = N'F_RM_SATISFACTION_EVENTS' AND c.name = N'ORDER_HEADER_SK'
)
BEGIN
    ALTER TABLE svo.F_RM_SATISFACTION_EVENTS
    ADD ORDER_HEADER_SK BIGINT NOT NULL DEFAULT 0;
    PRINT 'Added column svo.F_RM_SATISFACTION_EVENTS.ORDER_HEADER_SK.';
END
ELSE
    PRINT 'Column ORDER_HEADER_SK already exists on svo.F_RM_SATISFACTION_EVENTS.';
GO
