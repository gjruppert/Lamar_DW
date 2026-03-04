/* =========================================================
   usp_Load_D_HOLD_CODE
   Type 1 incremental load. Source: bzo.OM_HoldCodeExtractPVO
   Watermark: AddDateTime. Grain: (HOLD_CODE_ID, HOLD_TRANSLATION_ID). Plug row SK=0 if missing.
   Idempotent: safe to retry.
   ========================================================= */
CREATE OR ALTER PROCEDURE svo.usp_Load_D_HOLD_CODE
    @BatchId INT = NULL
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    DECLARE
        @ProcName       SYSNAME        = OBJECT_SCHEMA_NAME(@@PROCID) + '.' + OBJECT_NAME(@@PROCID),
        @TargetObject   SYSNAME        = 'svo.D_HOLD_CODE',
        @StartDttm      DATETIME2(0)   = SYSDATETIME(),
        @EndDttm        DATETIME2(0),
        @RunId          BIGINT         = NULL,
        @ErrMsg         NVARCHAR(4000) = NULL,
        @AsOfDate       DATE           = CAST(GETDATE() AS DATE),
        @LastWatermark  DATETIME2(7),
        @MaxWatermark   DATETIME2(7)   = NULL,
        @RowInserted    INT            = 0,
        @RowUpdated     INT            = 0,
        @TableBridgeID  INT            = NULL;

    DECLARE @MergeActions TABLE (ActionTaken VARCHAR(10) NOT NULL);

    SELECT @TableBridgeID = TableBridgeID FROM meta.MedallionTableBridge WHERE targettable = N'OM_HoldCodeExtractPVO';

    BEGIN TRY
        SELECT @LastWatermark = w.LAST_WATERMARK
        FROM etl.ETL_WATERMARK w
        WHERE w.TABLE_NAME = @TargetObject;

        IF @LastWatermark IS NULL
            SET @LastWatermark = '1900-01-01';

        INSERT INTO etl.ETL_RUN (PROC_NAME, TARGET_OBJECT, ASOF_DATE, START_DTTM, STATUS, BATCH_ID, TABLE_BRIDGE_ID)
        VALUES (@ProcName, @TargetObject, @AsOfDate, @StartDttm, 'STARTED', ISNULL(@BatchId, -1), ISNULL(@TableBridgeID, -1));

        SET @RunId = SCOPE_IDENTITY();
        IF @RunId IS NULL
            THROW 50001, 'ETL_RUN insert failed: RUN_ID is NULL.', 1;

        IF NOT EXISTS (SELECT 1 FROM svo.D_HOLD_CODE WHERE HOLD_CODE_SK = 0)
        BEGIN
            SET IDENTITY_INSERT svo.D_HOLD_CODE ON;
            INSERT INTO svo.D_HOLD_CODE (HOLD_CODE_SK, HOLD_CODE_ID, HOLD_TRANSLATION_ID, HOLD_CODE, HOLD_NAME, HOLD_DESCRIPTION, LAST_UPDATE_DATE, LAST_UPDATED_BY, BZ_LOAD_DATE, SV_LOAD_DATE)
            VALUES (0, -1, -1, 'UNK', 'Unknown', NULL, '0001-01-01', 'Unknown', '0001-01-01', CAST(GETDATE() AS DATE));
            SET IDENTITY_INSERT svo.D_HOLD_CODE OFF;
        END;

        IF OBJECT_ID('tempdb..#src') IS NOT NULL DROP TABLE #src;

        SELECT
            H.HoldHoldCodeId AS HOLD_CODE_ID,
            H.HoldTranslationHoldCodeId AS HOLD_TRANSLATION_ID,
            H.HoldHoldCode AS HOLD_CODE,
            H.HoldTranslationHoldName AS HOLD_NAME,
            H.HoldTranslationHoldDescription AS HOLD_DESCRIPTION,
            H.HoldLastUpdateDate AS LAST_UPDATE_DATE,
            H.HoldLastUpdatedBy AS LAST_UPDATED_BY,
            CAST(H.AddDateTime AS DATE) AS BZ_LOAD_DATE,
            CAST(GETDATE() AS DATE) AS SV_LOAD_DATE,
            H.AddDateTime AS SourceAddDateTime
        INTO #src
        FROM bzo.OM_HoldCodeExtractPVO H
        WHERE H.AddDateTime > @LastWatermark;

        SELECT @MaxWatermark = MAX(SourceAddDateTime) FROM #src;

        MERGE svo.D_HOLD_CODE AS tgt
        USING #src AS src ON tgt.HOLD_CODE_ID = src.HOLD_CODE_ID AND tgt.HOLD_TRANSLATION_ID = src.HOLD_TRANSLATION_ID
        WHEN MATCHED THEN UPDATE SET
            tgt.HOLD_CODE = src.HOLD_CODE,
            tgt.HOLD_NAME = src.HOLD_NAME,
            tgt.HOLD_DESCRIPTION = src.HOLD_DESCRIPTION,
            tgt.LAST_UPDATE_DATE = src.LAST_UPDATE_DATE,
            tgt.LAST_UPDATED_BY = src.LAST_UPDATED_BY,
            tgt.BZ_LOAD_DATE = src.BZ_LOAD_DATE,
            tgt.SV_LOAD_DATE = src.SV_LOAD_DATE
        WHEN NOT MATCHED BY TARGET THEN INSERT (
            HOLD_CODE_ID, HOLD_TRANSLATION_ID, HOLD_CODE, HOLD_NAME, HOLD_DESCRIPTION, LAST_UPDATE_DATE, LAST_UPDATED_BY, BZ_LOAD_DATE, SV_LOAD_DATE
        ) VALUES (
            src.HOLD_CODE_ID, src.HOLD_TRANSLATION_ID, src.HOLD_CODE, src.HOLD_NAME, src.HOLD_DESCRIPTION, src.LAST_UPDATE_DATE, src.LAST_UPDATED_BY, src.BZ_LOAD_DATE, src.SV_LOAD_DATE
        )
        OUTPUT $action INTO @MergeActions(ActionTaken);

        SELECT @RowInserted = SUM(CASE WHEN ActionTaken = 'INSERT' THEN 1 ELSE 0 END),
               @RowUpdated  = SUM(CASE WHEN ActionTaken = 'UPDATE' THEN 1 ELSE 0 END)
        FROM @MergeActions;

        IF @MaxWatermark IS NOT NULL
            UPDATE etl.ETL_WATERMARK SET LAST_WATERMARK = @MaxWatermark, UDT_DATE = SYSDATETIME() WHERE TABLE_NAME = @TargetObject;

        SET @EndDttm = SYSDATETIME();
        UPDATE etl.ETL_RUN SET END_DTTM = @EndDttm, STATUS = 'SUCCESS', ROW_INSERTED = @RowInserted, ROW_UPDATED = @RowUpdated, ROW_EXPIRED = 0, ERROR_MESSAGE = NULL WHERE RUN_ID = @RunId;
        IF COL_LENGTH('etl.ETL_RUN', 'ROW_UPDATED_T1') IS NOT NULL
            UPDATE etl.ETL_RUN SET ROW_UPDATED_T1 = @RowUpdated WHERE RUN_ID = @RunId;
    END TRY
    BEGIN CATCH
        SET @EndDttm = SYSDATETIME();
        SET @ErrMsg = CONCAT('Error ', ERROR_NUMBER(), ' (Line ', ERROR_LINE(), '): ', ERROR_MESSAGE());
        IF @RunId IS NOT NULL
            UPDATE etl.ETL_RUN SET END_DTTM = @EndDttm, STATUS = 'FAILURE', ROW_INSERTED = @RowInserted, ROW_UPDATED = @RowUpdated, ROW_EXPIRED = 0, ERROR_MESSAGE = @ErrMsg WHERE RUN_ID = @RunId;
        ;THROW;
    END CATCH
END;
GO
