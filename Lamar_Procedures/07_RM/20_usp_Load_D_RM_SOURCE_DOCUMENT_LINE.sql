/* =========================================================
   usp_Load_D_RM_SOURCE_DOCUMENT_LINE
   Type 1 incremental load. Source: bzo.VRM_SourceDocumentLinesPVO
   Watermark: AddDateTime. Grain: SOURCE_DOCUMENT_LINE_ID. Plug row SK=0 if missing.
   Idempotent: safe to retry.
   ========================================================= */
CREATE OR ALTER PROCEDURE svo.usp_Load_D_RM_SOURCE_DOCUMENT_LINE
    @BatchId INT = NULL
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    DECLARE
        @ProcName       SYSNAME        = OBJECT_SCHEMA_NAME(@@PROCID) + '.' + OBJECT_NAME(@@PROCID),
        @TargetObject   SYSNAME        = 'svo.D_RM_SOURCE_DOCUMENT_LINE',
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

    SELECT @TableBridgeID = TableBridgeID FROM meta.MedallionTableBridge WHERE targettable = N'VRM_SourceDocumentLinesPVO';

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

        IF NOT EXISTS (SELECT 1 FROM svo.D_RM_SOURCE_DOCUMENT_LINE WHERE RM_SOURCE_DOCUMENT_LINE_SK = 0)
        BEGIN
            SET IDENTITY_INSERT svo.D_RM_SOURCE_DOCUMENT_LINE ON;
            INSERT INTO svo.D_RM_SOURCE_DOCUMENT_LINE
            (RM_SOURCE_DOCUMENT_LINE_SK, SOURCE_DOCUMENT_LINE_ID, LINE_CREATED_BY, LINE_LAST_UPDATED_BY, LINE_LAST_UPDATE_LOGIN, DOC_CREATED_BY, DOCUMENT_NUMBER, DOC_LAST_UPDATE_DATE, DOC_LAST_UPDATE_LOGIN, ORG_ID, ORDER_FULFILL_LINE_ID, BZ_LOAD_DATE, SV_LOAD_DATE)
            VALUES (0, -1, 'Unknown', 'Unknown', 'Unknown', 'Unknown', 'Unknown', '1900-01-01', 'Unknown', -1, -1, CAST(GETDATE() AS DATE), CAST(GETDATE() AS DATE));
            SET IDENTITY_INSERT svo.D_RM_SOURCE_DOCUMENT_LINE OFF;
        END;

        IF OBJECT_ID('tempdb..#src') IS NOT NULL DROP TABLE #src;

        SELECT
            S.SourceDocLinesDocumentLineId                   AS SOURCE_DOCUMENT_LINE_ID,
            ISNULL(S.SourceDocLinesCreatedBy, 'Unknown')     AS LINE_CREATED_BY,
            ISNULL(S.SourceDocLinesLastUpdatedBy, 'Unknown')  AS LINE_LAST_UPDATED_BY,
            S.SourceDocLinesLastUpdateLogin                  AS LINE_LAST_UPDATE_LOGIN,
            ISNULL(S.SourceDocumentsCreatedBy, 'Unknown')     AS DOC_CREATED_BY,
            ISNULL(S.SourceDocumentsDocumentNumber, 'Unknown') AS DOCUMENT_NUMBER,
            CAST(S.SourceDocumentsLastUpdateDate AS DATE)    AS DOC_LAST_UPDATE_DATE,
            S.SourceDocumentsLastUpdateLogin                 AS DOC_LAST_UPDATE_LOGIN,
            ISNULL(S.SourceDocumentsOrgId, -1)                AS ORG_ID,
            CAST(-1 AS BIGINT)                               AS ORDER_FULFILL_LINE_ID,   /* SourceDocLinesDocLineIdInt1 not in VRM_SourceDocumentLinesPVO; use -1 until column available */
            CAST(S.AddDateTime AS DATE)                      AS BZ_LOAD_DATE,
            CAST(GETDATE() AS DATE)                          AS SV_LOAD_DATE,
            ISNULL(S.AddDateTime, SYSDATETIME())             AS SourceAddDateTime
        INTO #src
        FROM bzo.VRM_SourceDocumentLinesPVO AS S
        WHERE S.AddDateTime > @LastWatermark;

        SELECT @MaxWatermark = MAX(SourceAddDateTime) FROM #src;

        MERGE svo.D_RM_SOURCE_DOCUMENT_LINE AS tgt
        USING #src AS src ON tgt.SOURCE_DOCUMENT_LINE_ID = src.SOURCE_DOCUMENT_LINE_ID
        WHEN MATCHED THEN UPDATE SET
            tgt.LINE_CREATED_BY = src.LINE_CREATED_BY,
            tgt.LINE_LAST_UPDATED_BY = src.LINE_LAST_UPDATED_BY,
            tgt.LINE_LAST_UPDATE_LOGIN = src.LINE_LAST_UPDATE_LOGIN,
            tgt.DOC_CREATED_BY = src.DOC_CREATED_BY,
            tgt.DOCUMENT_NUMBER = src.DOCUMENT_NUMBER,
            tgt.DOC_LAST_UPDATE_DATE = src.DOC_LAST_UPDATE_DATE,
            tgt.DOC_LAST_UPDATE_LOGIN = src.DOC_LAST_UPDATE_LOGIN,
            tgt.ORG_ID = src.ORG_ID,
            tgt.ORDER_FULFILL_LINE_ID = src.ORDER_FULFILL_LINE_ID,
            tgt.BZ_LOAD_DATE = src.BZ_LOAD_DATE,
            tgt.SV_LOAD_DATE = src.SV_LOAD_DATE
        WHEN NOT MATCHED BY TARGET THEN INSERT (
            SOURCE_DOCUMENT_LINE_ID, LINE_CREATED_BY, LINE_LAST_UPDATED_BY, LINE_LAST_UPDATE_LOGIN, DOC_CREATED_BY, DOCUMENT_NUMBER, DOC_LAST_UPDATE_DATE, DOC_LAST_UPDATE_LOGIN, ORG_ID, ORDER_FULFILL_LINE_ID, BZ_LOAD_DATE, SV_LOAD_DATE
        ) VALUES (
            src.SOURCE_DOCUMENT_LINE_ID, src.LINE_CREATED_BY, src.LINE_LAST_UPDATED_BY, src.LINE_LAST_UPDATE_LOGIN, src.DOC_CREATED_BY, src.DOCUMENT_NUMBER, src.DOC_LAST_UPDATE_DATE, src.DOC_LAST_UPDATE_LOGIN, src.ORG_ID, src.ORDER_FULFILL_LINE_ID, src.BZ_LOAD_DATE, src.SV_LOAD_DATE
        )
        OUTPUT $action INTO @MergeActions(ActionTaken);

        SELECT @RowInserted = SUM(CASE WHEN ActionTaken = 'INSERT' THEN 1 ELSE 0 END),
               @RowUpdated  = SUM(CASE WHEN ActionTaken = 'UPDATE' THEN 1 ELSE 0 END)
        FROM @MergeActions;

        IF @MaxWatermark IS NOT NULL
            UPDATE etl.ETL_WATERMARK SET LAST_WATERMARK = @MaxWatermark, UDT_DATE = SYSDATETIME() WHERE TABLE_NAME = @TargetObject;

        SET @EndDttm = SYSDATETIME();
        UPDATE etl.ETL_RUN SET END_DTTM = @EndDttm, STATUS = 'SUCCESS', ROW_INSERTED = @RowInserted, ROW_UPDATED = @RowUpdated, ROW_EXPIRED = 0, ERROR_MESSAGE = NULL WHERE RUN_ID = @RunId;
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
