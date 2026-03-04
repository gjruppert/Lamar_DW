/* =========================================================
   usp_Load_D_AR_COLLECTOR
   Type 1 incremental. Source: bzo.AR_CollectorExtractPVO
   Watermark: AddDateTime. Grain: AR_COLLECTOR_ID. Plug row SK=0.
   Idempotent: safe to retry.
   ========================================================= */
CREATE OR ALTER PROCEDURE svo.usp_Load_D_AR_COLLECTOR
    @BatchId INT = NULL
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    DECLARE
        @ProcName       SYSNAME        = OBJECT_SCHEMA_NAME(@@PROCID) + '.' + OBJECT_NAME(@@PROCID),
        @TargetObject   SYSNAME        = 'svo.D_AR_COLLECTOR',
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

    SELECT @TableBridgeID = TableBridgeID FROM meta.MedallionTableBridge WHERE targettable = N'AR_CollectorExtractPVO';

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

        IF NOT EXISTS (SELECT 1 FROM svo.D_AR_COLLECTOR WHERE AR_COLLECTOR_SK = 0)
        BEGIN
            SET IDENTITY_INSERT svo.D_AR_COLLECTOR ON;
            INSERT INTO svo.D_AR_COLLECTOR
            (AR_COLLECTOR_SK, AR_COLLECTOR_ID, COLLECTOR_NAME, COLLECTOR_DESCRIPTION, SOURCE_LAST_UPDATE_DATE, SOURCE_LAST_UPDATED_BY, SOURCE_LAST_UPDATE_LOGIN, BZ_LOAD_DATE, SV_LOAD_DATE)
            VALUES
            (0, 0, 'UNKNOWN', 'Unknown AR collector', CAST('1900-01-01' AS DATETIME), 'SYSTEM', NULL, CAST('1900-01-01' AS DATE), CAST('1900-01-01' AS DATE));
            SET IDENTITY_INSERT svo.D_AR_COLLECTOR OFF;
        END;

        IF OBJECT_ID('tempdb..#src') IS NOT NULL DROP TABLE #src;

        SELECT
            C.ArCollectorCollectorId         AS AR_COLLECTOR_ID,
            C.ArCollectorName                AS COLLECTOR_NAME,
            C.ArCollectorDescription        AS COLLECTOR_DESCRIPTION,
            C.ArCollectorLastUpdateDate     AS SOURCE_LAST_UPDATE_DATE,
            C.ArCollectorLastUpdatedBy      AS SOURCE_LAST_UPDATED_BY,
            C.ArCollectorLastUpdateLogin    AS SOURCE_LAST_UPDATE_LOGIN,
            CAST(ISNULL(C.AddDateTime, GETDATE()) AS DATE) AS BZ_LOAD_DATE,
            CAST(GETDATE() AS DATE)         AS SV_LOAD_DATE,
            C.AddDateTime                   AS SourceAddDateTime
        INTO #src
        FROM bzo.AR_CollectorExtractPVO AS C
        WHERE C.AddDateTime > @LastWatermark;

        SELECT @MaxWatermark = MAX(SourceAddDateTime) FROM #src;

        MERGE svo.D_AR_COLLECTOR AS D
        USING #src AS S ON D.AR_COLLECTOR_ID = S.AR_COLLECTOR_ID
        WHEN MATCHED AND
        (
            ISNULL(D.COLLECTOR_NAME, '')                <> ISNULL(S.COLLECTOR_NAME, '')
            OR ISNULL(D.COLLECTOR_DESCRIPTION, '')     <> ISNULL(S.COLLECTOR_DESCRIPTION, '')
            OR ISNULL(D.SOURCE_LAST_UPDATE_DATE, '19000101') <> ISNULL(S.SOURCE_LAST_UPDATE_DATE, '19000101')
            OR ISNULL(D.SOURCE_LAST_UPDATED_BY, '')    <> ISNULL(S.SOURCE_LAST_UPDATED_BY, '')
            OR ISNULL(D.SOURCE_LAST_UPDATE_LOGIN, '')   <> ISNULL(S.SOURCE_LAST_UPDATE_LOGIN, '')
        )
        THEN
            UPDATE SET
                D.COLLECTOR_NAME          = S.COLLECTOR_NAME,
                D.COLLECTOR_DESCRIPTION   = S.COLLECTOR_DESCRIPTION,
                D.SOURCE_LAST_UPDATE_DATE = S.SOURCE_LAST_UPDATE_DATE,
                D.SOURCE_LAST_UPDATED_BY  = S.SOURCE_LAST_UPDATED_BY,
                D.SOURCE_LAST_UPDATE_LOGIN = S.SOURCE_LAST_UPDATE_LOGIN,
                D.SV_LOAD_DATE            = S.SV_LOAD_DATE
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (AR_COLLECTOR_ID, COLLECTOR_NAME, COLLECTOR_DESCRIPTION, SOURCE_LAST_UPDATE_DATE, SOURCE_LAST_UPDATED_BY, SOURCE_LAST_UPDATE_LOGIN, BZ_LOAD_DATE, SV_LOAD_DATE)
            VALUES (S.AR_COLLECTOR_ID, S.COLLECTOR_NAME, S.COLLECTOR_DESCRIPTION, S.SOURCE_LAST_UPDATE_DATE, S.SOURCE_LAST_UPDATED_BY, S.SOURCE_LAST_UPDATE_LOGIN, S.BZ_LOAD_DATE, S.SV_LOAD_DATE)
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
