/* =========================================================
   usp_Load_F_OM_ORDER_LINE
   Incremental INSERT only. Source: bzo.OM_LineExtractPVO.
   Filter: L.AddDateTime > @LastWatermark. Dedupe by ORDER_LINE_ID (LineId).
   Resolve SKs via svo.D_OM_ORDER_HEADER, svo.D_OM_ORDER_LINE.
   ========================================================= */
CREATE OR ALTER PROCEDURE svo.usp_Load_F_OM_ORDER_LINE
    @BatchId INT = NULL
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    DECLARE
        @ProcName       SYSNAME        = OBJECT_SCHEMA_NAME(@@PROCID) + '.' + OBJECT_NAME(@@PROCID),
        @TargetObject   SYSNAME        = 'svo.F_OM_ORDER_LINE',
        @StartDttm      DATETIME2(0)   = SYSDATETIME(),
        @EndDttm        DATETIME2(0),
        @RunId          BIGINT         = NULL,
        @ErrMsg         NVARCHAR(4000) = NULL,
        @AsOfDate       DATE           = CAST(GETDATE() AS DATE),
        @LastWatermark  DATETIME2(7),
        @MaxWatermark   DATETIME2(7)   = NULL,
        @RowInserted    INT            = 0,
        @TableBridgeID  INT            = NULL;

    SELECT @TableBridgeID = TableBridgeID FROM meta.MedallionTableBridge WHERE targettable = N'OM_LineExtractPVO';

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

        IF OBJECT_ID('tempdb..#lines') IS NOT NULL DROP TABLE #lines;

        SELECT
            L.LineId,
            L.LineHeaderId,
            L.LineActualShipDate,
            L.LineOrderedQty,
            L.LineExtendedAmount,
            L.LineUnitListPrice,
            L.LineUnitSellingPrice,
            L.LineStatusCode,
            L.LineCategoryCode,
            L.AddDateTime AS LineAddDateTime
        INTO #lines
        FROM (
            SELECT *,
                ROW_NUMBER() OVER (PARTITION BY L.LineId ORDER BY L.AddDateTime DESC) AS rn
            FROM bzo.OM_LineExtractPVO L
            WHERE L.AddDateTime > @LastWatermark
        ) L
        WHERE L.rn = 1;

        SELECT @MaxWatermark = MAX(LineAddDateTime) FROM #lines;

        INSERT INTO svo.F_OM_ORDER_LINE WITH (TABLOCK) (
            ORDER_HEADER_SK,
            ORDER_LINE_SK,
            LINE_SHIP_DATE_KEY,
            LINE_ORDERED_QTY,
            LINE_EXTENDED_AMOUNT,
            LINE_UNIT_LIST_PRICE,
            LINE_UNIT_SELLING_PRICE,
            LINE_STATUS_CODE,
            LINE_CATEGORY_CODE,
            BZ_LOAD_DATE,
            SV_LOAD_DATE
        )
        SELECT
            ISNULL(OH.ORDER_HEADER_SK, 0),
            ISNULL(OL.ORDER_LINE_SK, 0),
            CONVERT(INT, FORMAT(ISNULL(L.LineActualShipDate, '0001-01-01'), 'yyyyMMdd')),
            ISNULL(L.LineOrderedQty, 0),
            ISNULL(L.LineExtendedAmount, 0),
            ISNULL(L.LineUnitListPrice, 0),
            ISNULL(L.LineUnitSellingPrice, 0),
            ISNULL(L.LineStatusCode, 'UNK'),
            ISNULL(L.LineCategoryCode, 'UNK'),
            CAST(L.LineAddDateTime AS DATETIME2(0)),
            SYSDATETIME()
        FROM #lines L
        LEFT JOIN svo.D_OM_ORDER_HEADER OH ON OH.ORDER_HEADER_ID = L.LineHeaderId
        LEFT JOIN svo.D_OM_ORDER_LINE OL ON OL.ORDER_LINE_ID = L.LineId
        WHERE OL.ORDER_LINE_SK IS NOT NULL
          AND NOT EXISTS (SELECT 1 FROM svo.F_OM_ORDER_LINE t WHERE t.ORDER_LINE_SK = OL.ORDER_LINE_SK);

        SET @RowInserted = @@ROWCOUNT;

        IF @MaxWatermark IS NOT NULL
            UPDATE etl.ETL_WATERMARK SET LAST_WATERMARK = @MaxWatermark, UDT_DATE = SYSDATETIME() WHERE TABLE_NAME = @TargetObject;

        SET @EndDttm = SYSDATETIME();
        UPDATE etl.ETL_RUN SET END_DTTM = @EndDttm, STATUS = 'SUCCESS', ROW_INSERTED = @RowInserted, ROW_UPDATED = 0, ROW_EXPIRED = 0, ERROR_MESSAGE = NULL WHERE RUN_ID = @RunId;
    END TRY
    BEGIN CATCH
        SET @EndDttm = SYSDATETIME();
        SET @ErrMsg = CONCAT('Error ', ERROR_NUMBER(), ' (Line ', ERROR_LINE(), '): ', ERROR_MESSAGE());
        IF @RunId IS NOT NULL
            UPDATE etl.ETL_RUN SET END_DTTM = @EndDttm, STATUS = 'FAILURE', ROW_INSERTED = @RowInserted, ROW_UPDATED = 0, ROW_EXPIRED = 0, ERROR_MESSAGE = @ErrMsg WHERE RUN_ID = @RunId;
        ;THROW;
    END CATCH
END;
GO
