/* =========================================================
   usp_Load_D_SM_SUBSCRIPTION_PRODUCT
   Type 1 incremental load. Source: bzo.OSS_SubscriptionProductExtractPVO
   Watermark: AddDateTime. Grain: SUBSCRIPTION_PRODUCT_ID. Plug row SK=0 if missing.
   Idempotent: safe to retry.
   ========================================================= */
CREATE OR ALTER PROCEDURE svo.usp_Load_D_SM_SUBSCRIPTION_PRODUCT
    @BatchId INT = NULL
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    DECLARE
        @ProcName       SYSNAME        = OBJECT_SCHEMA_NAME(@@PROCID) + '.' + OBJECT_NAME(@@PROCID),
        @TargetObject   SYSNAME        = 'svo.D_SM_SUBSCRIPTION_PRODUCT',
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

    SELECT @TableBridgeID = TableBridgeID FROM meta.MedallionTableBridge WHERE targettable = N'OSS_SubscriptionProductExtractPVO';

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

        IF NOT EXISTS (SELECT 1 FROM svo.D_SM_SUBSCRIPTION_PRODUCT WHERE SUBSCRIPTION_PRODUCT_SK = 0)
        BEGIN
            SET IDENTITY_INSERT svo.D_SM_SUBSCRIPTION_PRODUCT ON;
            INSERT INTO svo.D_SM_SUBSCRIPTION_PRODUCT (SUBSCRIPTION_PRODUCT_SK, SUBSCRIPTION_PRODUCT_ID, SUBSCRIPTION_ID, INVENTORY_ITEM_ID, DEFINITION_ORG_ID, BZ_LOAD_DATE, SV_LOAD_DATE)
            VALUES (0, -1, -1, -1, -1, '0001-01-01', '0001-01-01');
            SET IDENTITY_INSERT svo.D_SM_SUBSCRIPTION_PRODUCT OFF;
        END;

        IF OBJECT_ID('tempdb..#src') IS NOT NULL DROP TABLE #src;

        SELECT
            P.SubscriptionProductId AS SUBSCRIPTION_PRODUCT_ID,
            P.SubscriptionId       AS SUBSCRIPTION_ID,
            P.InventoryItemId      AS INVENTORY_ITEM_ID,
            P.DefinitionOrgId      AS DEFINITION_ORG_ID,
            CAST(P.AddDateTime AS DATE) AS BZ_LOAD_DATE,
            CAST(GETDATE() AS DATE)    AS SV_LOAD_DATE,
            P.AddDateTime          AS SourceAddDateTime
        INTO #src
        FROM bzo.OSS_SubscriptionProductExtractPVO P
        WHERE P.AddDateTime > @LastWatermark;

        SELECT @MaxWatermark = MAX(SourceAddDateTime) FROM #src;

        MERGE svo.D_SM_SUBSCRIPTION_PRODUCT AS tgt
        USING #src AS src ON tgt.SUBSCRIPTION_PRODUCT_ID = src.SUBSCRIPTION_PRODUCT_ID
        WHEN MATCHED THEN UPDATE SET
            tgt.SUBSCRIPTION_ID    = src.SUBSCRIPTION_ID,
            tgt.INVENTORY_ITEM_ID  = src.INVENTORY_ITEM_ID,
            tgt.DEFINITION_ORG_ID  = src.DEFINITION_ORG_ID,
            tgt.BZ_LOAD_DATE       = src.BZ_LOAD_DATE,
            tgt.SV_LOAD_DATE       = src.SV_LOAD_DATE
        WHEN NOT MATCHED BY TARGET THEN INSERT (
            SUBSCRIPTION_PRODUCT_ID, SUBSCRIPTION_ID, INVENTORY_ITEM_ID, DEFINITION_ORG_ID, BZ_LOAD_DATE, SV_LOAD_DATE
        ) VALUES (
            src.SUBSCRIPTION_PRODUCT_ID, src.SUBSCRIPTION_ID, src.INVENTORY_ITEM_ID, src.DEFINITION_ORG_ID, src.BZ_LOAD_DATE, src.SV_LOAD_DATE
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
