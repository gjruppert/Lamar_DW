/* =========================================================
   usp_Load_D_RM_RULE
   Plug row only. No bzo source in folder; ensures SK=0 exists for FK references.
   ETL_RUN logged; no watermark update. Idempotent.
   ========================================================= */
CREATE OR ALTER PROCEDURE svo.usp_Load_D_RM_RULE
    @BatchId INT = NULL
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    DECLARE
        @ProcName       SYSNAME        = OBJECT_SCHEMA_NAME(@@PROCID) + '.' + OBJECT_NAME(@@PROCID),
        @TargetObject   SYSNAME        = 'svo.D_RM_RULE',
        @StartDttm      DATETIME2(0)   = SYSDATETIME(),
        @EndDttm        DATETIME2(0),
        @RunId          BIGINT         = NULL,
        @ErrMsg         NVARCHAR(4000) = NULL,
        @AsOfDate       DATE           = CAST(GETDATE() AS DATE),
        @TableBridgeID  INT            = NULL;

    SELECT @TableBridgeID = TableBridgeID FROM meta.MedallionTableBridge WHERE targettable = N'D_RM_RULE';

    BEGIN TRY
        INSERT INTO etl.ETL_RUN (PROC_NAME, TARGET_OBJECT, ASOF_DATE, START_DTTM, STATUS, BATCH_ID, TABLE_BRIDGE_ID)
        VALUES (@ProcName, @TargetObject, @AsOfDate, @StartDttm, 'STARTED', ISNULL(@BatchId, -1), ISNULL(@TableBridgeID, -1));

        SET @RunId = SCOPE_IDENTITY();
        IF @RunId IS NULL
            THROW 50001, 'ETL_RUN insert failed: RUN_ID is NULL.', 1;

        IF NOT EXISTS (SELECT 1 FROM svo.D_RM_RULE WHERE RM_RULE_SK = 0)
        BEGIN
            SET IDENTITY_INSERT svo.D_RM_RULE ON;
            INSERT INTO svo.D_RM_RULE
            (RM_RULE_SK, RULE_ID, RULE_NAME, RULE_TYPE_CODE, SSP_RULE_CODE, ALLOCATION_METHOD_CODE, SATISFACTION_METHOD_CODE, REVENUE_METHOD_CODE, ACTIVE_FLAG, START_DATE, END_DATE, CREATED_BY, CREATION_DATE, LAST_UPDATED_BY, LAST_UPDATE_DATE, BZ_LOAD_DATE, SV_LOAD_DATE)
            VALUES (0, -1, 'Unknown', 'Unk', 'Unk', 'Unk', 'Unk', 'Unk', 'U', '1900-01-01', '9999-12-31', 'Unknown', '1900-01-01', 'Unknown', '1900-01-01', CAST(GETDATE() AS DATE), CAST(GETDATE() AS DATE));
            SET IDENTITY_INSERT svo.D_RM_RULE OFF;
        END;

        SET @EndDttm = SYSDATETIME();
        UPDATE etl.ETL_RUN SET END_DTTM = @EndDttm, STATUS = 'SUCCESS', ROW_INSERTED = 0, ROW_UPDATED = 0, ROW_EXPIRED = 0, ERROR_MESSAGE = NULL WHERE RUN_ID = @RunId;
    END TRY
    BEGIN CATCH
        SET @EndDttm = SYSDATETIME();
        SET @ErrMsg = CONCAT('Error ', ERROR_NUMBER(), ' (Line ', ERROR_LINE(), '): ', ERROR_MESSAGE());
        IF @RunId IS NOT NULL
            UPDATE etl.ETL_RUN SET END_DTTM = @EndDttm, STATUS = 'FAILURE', ROW_INSERTED = 0, ROW_UPDATED = 0, ROW_EXPIRED = 0, ERROR_MESSAGE = @ErrMsg WHERE RUN_ID = @RunId;
        ;THROW;
    END CATCH
END;
GO
