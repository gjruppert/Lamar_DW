/* =========================================================
   usp_Load_D_SALES_REP
   Type 1 incremental load. Source: bzo.OM_SalesRep
   Watermark: AddDateTime. Grain: SALES_REP_ID. Plug row SK=0 if missing.
   Idempotent: safe to retry.
   ========================================================= */
CREATE OR ALTER PROCEDURE svo.usp_Load_D_SALES_REP
    @BatchId INT = NULL
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    DECLARE
        @ProcName       SYSNAME        = OBJECT_SCHEMA_NAME(@@PROCID) + '.' + OBJECT_NAME(@@PROCID),
        @TargetObject   SYSNAME        = 'svo.D_SALES_REP',
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

    SELECT @TableBridgeID = TableBridgeID FROM meta.MedallionTableBridge WHERE targettable = N'OM_SalesRep';

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

        IF NOT EXISTS (SELECT 1 FROM svo.D_SALES_REP WHERE SALES_REP_SK = 0)
        BEGIN
            SET IDENTITY_INSERT svo.D_SALES_REP ON;
            INSERT INTO svo.D_SALES_REP (SALES_REP_SK, SALES_REP_ID, SALES_REP_NUMBER, PARTY_ID, PARTY_NAME, PERSON_FIRST_NAME, PERSON_LAST_NAME, EMAIL_ADDRESS, RESOURCE_ID, RESOURCE_LAST_UPDATE_DATE, RESOURCE_LAST_UPDATED_BY, RESOURCE_LAST_UPDATE_LOGIN, RESOURCE_STATUS, START_DATE_ACTIVE, BZ_LOAD_DATE, SV_LOAD_DATE)
            VALUES (0, -1, 'UNKNOWN', -1, 'Unknown', 'Unknown', 'Unknown', 'unknown@unknown.com', -1, '0001-01-01', 'Unknown', 'Unknown', 'Unknown', '0001-01-01', CAST('0001-01-01' AS DATE), CAST(GETDATE() AS DATE));
            SET IDENTITY_INSERT svo.D_SALES_REP OFF;
        END;

        IF OBJECT_ID('tempdb..#src') IS NOT NULL DROP TABLE #src;

        SELECT
            SR.ResourceSalesrepId AS SALES_REP_ID,
            SR.SalesrepNumber AS SALES_REP_NUMBER,
            SR.PartyId AS PARTY_ID,
            SR.PartyName AS PARTY_NAME,
            SR.PersonFirstName AS PERSON_FIRST_NAME,
            SR.PersonLastName AS PERSON_LAST_NAME,
            SR.EmailAddress AS EMAIL_ADDRESS,
            SR.ResourceId AS RESOURCE_ID,
            CAST(SR.ResourceSalesrepPEOLastUpdateDate AS DATE) AS RESOURCE_LAST_UPDATE_DATE,
            SR.ResourceSalesrepPEOLastUpdatedBy AS RESOURCE_LAST_UPDATED_BY,
            SR.ResourceSalesrepPEOLastUpdateLogin AS RESOURCE_LAST_UPDATE_LOGIN,
            SR.ResourceSalesrepPEOStatus AS RESOURCE_STATUS,
            CAST(SR.StartDateActive AS DATE) AS START_DATE_ACTIVE,
            CAST(SR.AddDateTime AS DATE) AS BZ_LOAD_DATE,
            CAST(GETDATE() AS DATE) AS SV_LOAD_DATE,
            SR.AddDateTime AS SourceAddDateTime
        INTO #src
        FROM bzo.OM_SalesRep SR
        WHERE SR.AddDateTime > @LastWatermark;

        SELECT @MaxWatermark = MAX(SourceAddDateTime) FROM #src;

        MERGE svo.D_SALES_REP AS tgt
        USING #src AS src ON tgt.SALES_REP_ID = src.SALES_REP_ID
        WHEN MATCHED THEN UPDATE SET
            tgt.SALES_REP_NUMBER = src.SALES_REP_NUMBER,
            tgt.PARTY_ID = src.PARTY_ID,
            tgt.PARTY_NAME = src.PARTY_NAME,
            tgt.PERSON_FIRST_NAME = src.PERSON_FIRST_NAME,
            tgt.PERSON_LAST_NAME = src.PERSON_LAST_NAME,
            tgt.EMAIL_ADDRESS = src.EMAIL_ADDRESS,
            tgt.RESOURCE_ID = src.RESOURCE_ID,
            tgt.RESOURCE_LAST_UPDATE_DATE = src.RESOURCE_LAST_UPDATE_DATE,
            tgt.RESOURCE_LAST_UPDATED_BY = src.RESOURCE_LAST_UPDATED_BY,
            tgt.RESOURCE_LAST_UPDATE_LOGIN = src.RESOURCE_LAST_UPDATE_LOGIN,
            tgt.RESOURCE_STATUS = src.RESOURCE_STATUS,
            tgt.START_DATE_ACTIVE = src.START_DATE_ACTIVE,
            tgt.BZ_LOAD_DATE = src.BZ_LOAD_DATE,
            tgt.SV_LOAD_DATE = src.SV_LOAD_DATE
        WHEN NOT MATCHED BY TARGET THEN INSERT (
            SALES_REP_ID, SALES_REP_NUMBER, PARTY_ID, PARTY_NAME, PERSON_FIRST_NAME, PERSON_LAST_NAME, EMAIL_ADDRESS, RESOURCE_ID, RESOURCE_LAST_UPDATE_DATE, RESOURCE_LAST_UPDATED_BY, RESOURCE_LAST_UPDATE_LOGIN, RESOURCE_STATUS, START_DATE_ACTIVE, BZ_LOAD_DATE, SV_LOAD_DATE
        ) VALUES (
            src.SALES_REP_ID, src.SALES_REP_NUMBER, src.PARTY_ID, src.PARTY_NAME, src.PERSON_FIRST_NAME, src.PERSON_LAST_NAME, src.EMAIL_ADDRESS, src.RESOURCE_ID, src.RESOURCE_LAST_UPDATE_DATE, src.RESOURCE_LAST_UPDATED_BY, src.RESOURCE_LAST_UPDATE_LOGIN, src.RESOURCE_STATUS, src.START_DATE_ACTIVE, src.BZ_LOAD_DATE, src.SV_LOAD_DATE
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
