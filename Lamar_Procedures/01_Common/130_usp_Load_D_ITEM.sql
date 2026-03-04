/* =========================================================
   usp_Load_D_ITEM
   SCD2 incremental load. Source: bzo.PIM_ItemExtractPVO
   Watermark filter, expire changed rows, insert new current rows.
   Idempotent: safe to retry same watermark range.
   ========================================================= */
CREATE OR ALTER PROCEDURE svo.usp_Load_D_ITEM
    @BatchId INT = NULL
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    DECLARE
        @ProcName       SYSNAME        = OBJECT_SCHEMA_NAME(@@PROCID) + '.' + OBJECT_NAME(@@PROCID),
        @TargetObject   SYSNAME        = 'svo.D_ITEM',
        @StartDttm      DATETIME2(0)   = SYSDATETIME(),
        @EndDttm        DATETIME2(0),
        @RunId          BIGINT         = NULL,
        @ErrMsg         NVARCHAR(4000) = NULL,

        @AsOfDate       DATE           = CAST(GETDATE() AS DATE),
        @LoadDttm       DATETIME2(0)   = SYSDATETIME(),
        @HighDate       DATE           = '9999-12-31',

        @LastWatermark  DATETIME2(7),
        @MaxWatermark   DATETIME2(7)   = NULL,

        @RowInserted    INT            = 0,
        @RowExpired     INT            = 0,
        @RowUpdated     INT            = 0,
        @TableBridgeID  INT            = NULL;

    SELECT @TableBridgeID = TableBridgeID FROM meta.MedallionTableBridge WHERE targettable = N'PIM_ItemExtractPVO';

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

        IF EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'UX_D_ITEM_INVENTORY_ITEM_ID' AND object_id = OBJECT_ID('svo.D_ITEM'))
        BEGIN
            DROP INDEX UX_D_ITEM_INVENTORY_ITEM_ID ON svo.D_ITEM;
        END;

        IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'UX_D_ITEM_BK_CURR' AND object_id = OBJECT_ID('svo.D_ITEM'))
        BEGIN
            CREATE UNIQUE NONCLUSTERED INDEX UX_D_ITEM_BK_CURR
            ON svo.D_ITEM (INVENTORY_ITEM_ID)
            WHERE CURR_IND = 'Y'
            ON FG_SilverDim;
        END;

        IF NOT EXISTS (SELECT 1 FROM svo.D_ITEM WHERE ITEM_SK = 0)
        BEGIN
            SET IDENTITY_INSERT svo.D_ITEM ON;

            INSERT INTO svo.D_ITEM
            (ITEM_SK, INVENTORY_ITEM_ID, ITEM_NUMBER, ITEM_DESCRIPTION, ITEM_TYPE, ITEM_STATUS, BZ_LOAD_DATE, SV_LOAD_DATE, INFERRED_FLAG, EFF_DATE, END_DATE, CRE_DATE, UDT_DATE, CURR_IND)
            VALUES
            (0, -1, '-1', 'Unknown item', 'Unknown', 'Unknown', GETDATE(), GETDATE(), 'Y', @AsOfDate, @HighDate, @LoadDttm, @LoadDttm, 'Y');

            SET IDENTITY_INSERT svo.D_ITEM OFF;
        END;

        IF OBJECT_ID('tempdb..#src') IS NOT NULL DROP TABLE #src;

        SELECT
            s.INVENTORY_ITEM_ID, s.ITEM_NUMBER, s.ITEM_DESCRIPTION, s.ITEM_TYPE, s.ITEM_STATUS,
            s.BZ_LOAD_DATE, s.SV_LOAD_DATE, s.SourceAddDateTime
        INTO #src
        FROM
        (
            SELECT
                I.ItemBasePEOInventoryItemId AS INVENTORY_ITEM_ID,
                I.ItemBasePEOItemNumber AS ITEM_NUMBER,
                I.ItemTranslationPEODescription AS ITEM_DESCRIPTION,
                ISNULL(I.ItemBasePEOItemType,'UNK') AS ITEM_TYPE,
                I.ItemBasePEOInventoryItemStatusCode AS ITEM_STATUS,
                COALESCE(CAST(I.AddDateTime AS DATETIME), CAST(GETDATE()-1 AS DATETIME)) AS BZ_LOAD_DATE,
                GETDATE() AS SV_LOAD_DATE,
                I.AddDateTime AS SourceAddDateTime,
                ROW_NUMBER() OVER (PARTITION BY I.ItemBasePEOInventoryItemId ORDER BY I.AddDateTime DESC) AS rn
            FROM bzo.PIM_ItemExtractPVO I
            WHERE I.AddDateTime > @LastWatermark
        ) s
        WHERE s.rn = 1;

        SELECT @MaxWatermark = MAX(SourceAddDateTime) FROM #src;

        UPDATE tgt
        SET tgt.END_DATE = DATEADD(DAY, -1, @AsOfDate), tgt.CURR_IND = 'N', tgt.UDT_DATE = @LoadDttm
        FROM svo.D_ITEM tgt
        INNER JOIN #src src ON src.INVENTORY_ITEM_ID = tgt.INVENTORY_ITEM_ID
        WHERE tgt.CURR_IND = 'Y'
          AND (
                ISNULL(tgt.ITEM_NUMBER,'') <> ISNULL(src.ITEM_NUMBER,'')
             OR ISNULL(tgt.ITEM_DESCRIPTION,'') <> ISNULL(src.ITEM_DESCRIPTION,'')
             OR ISNULL(tgt.ITEM_TYPE,'') <> ISNULL(src.ITEM_TYPE,'')
             OR ISNULL(tgt.ITEM_STATUS,'') <> ISNULL(src.ITEM_STATUS,'')
          );

        SET @RowExpired = @@ROWCOUNT;

        INSERT INTO svo.D_ITEM
        (INVENTORY_ITEM_ID, ITEM_NUMBER, ITEM_DESCRIPTION, ITEM_TYPE, ITEM_STATUS, BZ_LOAD_DATE, SV_LOAD_DATE, INFERRED_FLAG, EFF_DATE, END_DATE, CRE_DATE, UDT_DATE, CURR_IND)
        SELECT
            src.INVENTORY_ITEM_ID, src.ITEM_NUMBER, src.ITEM_DESCRIPTION, src.ITEM_TYPE, src.ITEM_STATUS,
            src.BZ_LOAD_DATE, src.SV_LOAD_DATE, 'N', @AsOfDate, @HighDate, @LoadDttm, @LoadDttm, 'Y'
        FROM #src src
        LEFT JOIN svo.D_ITEM tgt ON tgt.INVENTORY_ITEM_ID = src.INVENTORY_ITEM_ID AND tgt.CURR_IND = 'Y'
        WHERE tgt.INVENTORY_ITEM_ID IS NULL;

        SET @RowInserted = @@ROWCOUNT;

        IF @MaxWatermark IS NOT NULL
        BEGIN
            UPDATE etl.ETL_WATERMARK SET LAST_WATERMARK = @MaxWatermark, UDT_DATE = SYSDATETIME() WHERE TABLE_NAME = @TargetObject;
        END

        SET @EndDttm = SYSDATETIME();
        UPDATE etl.ETL_RUN SET END_DTTM = @EndDttm, STATUS = 'SUCCESS', ROW_INSERTED = @RowInserted, ROW_EXPIRED = @RowExpired, ROW_UPDATED = @RowUpdated, ERROR_MESSAGE = NULL WHERE RUN_ID = @RunId;
    END TRY
    BEGIN CATCH
        SET @EndDttm = SYSDATETIME();
        SET @ErrMsg  = CONCAT('Error ', ERROR_NUMBER(), ' (Line ', ERROR_LINE(), '): ', ERROR_MESSAGE());
        IF @RunId IS NOT NULL
            UPDATE etl.ETL_RUN SET END_DTTM = @EndDttm, STATUS = 'FAILURE', ROW_INSERTED = @RowInserted, ROW_EXPIRED = @RowExpired, ROW_UPDATED = @RowUpdated, ERROR_MESSAGE = @ErrMsg WHERE RUN_ID = @RunId;
        ;THROW;
    END CATCH
END;
GO
