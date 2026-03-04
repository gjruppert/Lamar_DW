/* =========================================================
   usp_Load_D_RM_SOURCE_DOC_PRICING_LINE
   Type 1 incremental load. Source: bzo.VRM_SourceDocLinePricingLinesPVO
   Watermark: AddDateTime. Grain: SOURCE_DOCUMENT_LINE_ID (one row per line; source has multiple
   rows per line per contract/reference pricing—dedupe by latest AddDateTime). Plug row SK=0 if missing.
   Idempotent: safe to retry.
   ========================================================= */
CREATE OR ALTER PROCEDURE svo.usp_Load_D_RM_SOURCE_DOC_PRICING_LINE
    @BatchId INT = NULL
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    DECLARE
        @ProcName       SYSNAME        = OBJECT_SCHEMA_NAME(@@PROCID) + '.' + OBJECT_NAME(@@PROCID),
        @TargetObject   SYSNAME        = 'svo.D_RM_SOURCE_DOC_PRICING_LINE',
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

    SELECT @TableBridgeID = TableBridgeID FROM meta.MedallionTableBridge WHERE targettable = N'VRM_SourceDocLinePricingLinesPVO';

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

        IF NOT EXISTS (SELECT 1 FROM svo.D_RM_SOURCE_DOC_PRICING_LINE WHERE RM_SOURCE_DOC_PRICING_LINE_SK = 0)
        BEGIN
            SET IDENTITY_INSERT svo.D_RM_SOURCE_DOC_PRICING_LINE ON;
            INSERT INTO svo.D_RM_SOURCE_DOC_PRICING_LINE
            (RM_SOURCE_DOC_PRICING_LINE_SK, SOURCE_DOCUMENT_LINE_ID, BILL_TO_CUSTOMER_ID, BILL_TO_CUSTOMER_SITE_ID, INVENTORY_ORG_ID, ITEM_ID, MEMO_LINE_ID, MEMO_LINE_NAME, MEMO_LINE_SEQ_ID, SALESREP_ID, SALESREP_NAME,
             SRC_ATTRIBUTE_CHAR_41, SRC_ATTRIBUTE_CHAR_42, SRC_ATTRIBUTE_CHAR_43, SRC_ATTRIBUTE_CHAR_44, SRC_ATTRIBUTE_CHAR_45, SRC_ATTRIBUTE_CHAR_46, SRC_ATTRIBUTE_CHAR_47, SRC_ATTRIBUTE_CHAR_48, SRC_ATTRIBUTE_CHAR_49, SRC_ATTRIBUTE_CHAR_50, SRC_ATTRIBUTE_CHAR_51, SRC_ATTRIBUTE_CHAR_52, SRC_ATTRIBUTE_CHAR_53, SRC_ATTRIBUTE_CHAR_54, SRC_ATTRIBUTE_CHAR_55, SRC_ATTRIBUTE_CHAR_56,
             SRC_ATTRIBUTE_DATE_1, SRC_ATTRIBUTE_DATE_2, SRC_ATTRIBUTE_NUMBER_12, BZ_LOAD_DATE, SV_LOAD_DATE)
            VALUES
            (0, -1, -1, -1, -1, -1, -1, 'Unknown', -1, -1, 'Unknown',
             'Unknown', 'Unknown', 'Unknown', 'Unknown', 'Unknown', 'Unknown', 'Unknown', 'Unknown', 'Unknown', 'Unknown', 'Unknown', 'Unknown', 'Unknown', 'Unknown', 'Unknown', 'Unknown',
             '1900-01-01', '9999-12-31', 0, CAST(GETDATE() AS DATE), CAST(GETDATE() AS DATE));
            SET IDENTITY_INSERT svo.D_RM_SOURCE_DOC_PRICING_LINE OFF;
        END;

        IF OBJECT_ID('tempdb..#src') IS NOT NULL DROP TABLE #src;

        SELECT
            ISNULL(P.SourceDocLinesDocumentLineId, -1)   AS SOURCE_DOCUMENT_LINE_ID,
            ISNULL(P.SourceDocLinesBillToCustomerId, -1) AS BILL_TO_CUSTOMER_ID,
            ISNULL(P.SourceDocLinesBillToCustomerSiteId, -1) AS BILL_TO_CUSTOMER_SITE_ID,
            ISNULL(P.SourceDocLinesInventoryOrgId, -1)   AS INVENTORY_ORG_ID,
            ISNULL(P.SourceDocLinesItemId, -1)           AS ITEM_ID,
            ISNULL(P.SourceDocLinesMemoLineId, -1)        AS MEMO_LINE_ID,
            ISNULL(P.SourceDocLinesMemoLineName, 'Unknown') AS MEMO_LINE_NAME,
            ISNULL(P.SourceDocLinesMemoLineSeqId, -1)     AS MEMO_LINE_SEQ_ID,
            ISNULL(P.SourceDocLinesSalesrepId, -1)        AS SALESREP_ID,
            ISNULL(P.SourceDocLinesSalesrepName, 'Unknown') AS SALESREP_NAME,
            ISNULL(P.SourceDocLinesSrcAttributeChar41, 'Unknown') AS SRC_ATTRIBUTE_CHAR_41,
            ISNULL(P.SourceDocLinesSrcAttributeChar42, 'Unknown') AS SRC_ATTRIBUTE_CHAR_42,
            ISNULL(P.SourceDocLinesSrcAttributeChar43, 'Unknown') AS SRC_ATTRIBUTE_CHAR_43,
            ISNULL(P.SourceDocLinesSrcAttributeChar44, 'Unknown') AS SRC_ATTRIBUTE_CHAR_44,
            ISNULL(P.SourceDocLinesSrcAttributeChar45, 'Unknown') AS SRC_ATTRIBUTE_CHAR_45,
            ISNULL(P.SourceDocLinesSrcAttributeChar46, 'Unknown') AS SRC_ATTRIBUTE_CHAR_46,
            ISNULL(P.SourceDocLinesSrcAttributeChar47, 'Unknown') AS SRC_ATTRIBUTE_CHAR_47,
            ISNULL(P.SourceDocLinesSrcAttributeChar48, 'Unknown') AS SRC_ATTRIBUTE_CHAR_48,
            ISNULL(P.SourceDocLinesSrcAttributeChar49, 'Unknown') AS SRC_ATTRIBUTE_CHAR_49,
            ISNULL(P.SourceDocLinesSrcAttributeChar50, 'Unknown') AS SRC_ATTRIBUTE_CHAR_50,
            ISNULL(P.SourceDocLinesSrcAttributeChar51, 'Unknown') AS SRC_ATTRIBUTE_CHAR_51,
            ISNULL(P.SourceDocLinesSrcAttributeChar52, 'Unknown') AS SRC_ATTRIBUTE_CHAR_52,
            ISNULL(P.SourceDocLinesSrcAttributeChar53, 'Unknown') AS SRC_ATTRIBUTE_CHAR_53,
            ISNULL(P.SourceDocLinesSrcAttributeChar54, 'Unknown') AS SRC_ATTRIBUTE_CHAR_54,
            ISNULL(P.SourceDocLinesSrcAttributeChar55, 'Unknown') AS SRC_ATTRIBUTE_CHAR_55,
            ISNULL(P.SourceDocLinesSrcAttributeChar56, 'Unknown') AS SRC_ATTRIBUTE_CHAR_56,
            ISNULL(P.SourceDocLinesSrcAttributeDate1, '1900-01-01') AS SRC_ATTRIBUTE_DATE_1,
            ISNULL(P.SourceDocLinesSrcAttributeDate2, '9999-12-31') AS SRC_ATTRIBUTE_DATE_2,
            ISNULL(P.SourceDocLinesSrcAttributeNumber12, 0) AS SRC_ATTRIBUTE_NUMBER_12,
            CAST(P.AddDateTime AS DATE)          AS BZ_LOAD_DATE,
            CAST(GETDATE() AS DATE)              AS SV_LOAD_DATE,
            ISNULL(P.AddDateTime, SYSDATETIME()) AS SourceAddDateTime
        INTO #src
        FROM bzo.VRM_SourceDocLinePricingLinesPVO AS P
        WHERE P.AddDateTime > @LastWatermark;

        SELECT @MaxWatermark = MAX(SourceAddDateTime) FROM #src;

        IF OBJECT_ID('tempdb..#src_dedup') IS NOT NULL DROP TABLE #src_dedup;
        ;WITH ranked AS (
            SELECT *, ROW_NUMBER() OVER (PARTITION BY SOURCE_DOCUMENT_LINE_ID ORDER BY SourceAddDateTime DESC) AS rn
            FROM #src
        )
        SELECT SOURCE_DOCUMENT_LINE_ID, BILL_TO_CUSTOMER_ID, BILL_TO_CUSTOMER_SITE_ID, INVENTORY_ORG_ID, ITEM_ID, MEMO_LINE_ID, MEMO_LINE_NAME, MEMO_LINE_SEQ_ID, SALESREP_ID, SALESREP_NAME,
            SRC_ATTRIBUTE_CHAR_41, SRC_ATTRIBUTE_CHAR_42, SRC_ATTRIBUTE_CHAR_43, SRC_ATTRIBUTE_CHAR_44, SRC_ATTRIBUTE_CHAR_45, SRC_ATTRIBUTE_CHAR_46, SRC_ATTRIBUTE_CHAR_47, SRC_ATTRIBUTE_CHAR_48, SRC_ATTRIBUTE_CHAR_49, SRC_ATTRIBUTE_CHAR_50, SRC_ATTRIBUTE_CHAR_51, SRC_ATTRIBUTE_CHAR_52, SRC_ATTRIBUTE_CHAR_53, SRC_ATTRIBUTE_CHAR_54, SRC_ATTRIBUTE_CHAR_55, SRC_ATTRIBUTE_CHAR_56,
            SRC_ATTRIBUTE_DATE_1, SRC_ATTRIBUTE_DATE_2, SRC_ATTRIBUTE_NUMBER_12, BZ_LOAD_DATE, SV_LOAD_DATE
        INTO #src_dedup
        FROM ranked
        WHERE rn = 1;

        MERGE svo.D_RM_SOURCE_DOC_PRICING_LINE AS tgt
        USING #src_dedup AS src ON tgt.SOURCE_DOCUMENT_LINE_ID = src.SOURCE_DOCUMENT_LINE_ID
        WHEN MATCHED THEN UPDATE SET
            tgt.BILL_TO_CUSTOMER_ID = src.BILL_TO_CUSTOMER_ID,
            tgt.BILL_TO_CUSTOMER_SITE_ID = src.BILL_TO_CUSTOMER_SITE_ID,
            tgt.INVENTORY_ORG_ID = src.INVENTORY_ORG_ID,
            tgt.ITEM_ID = src.ITEM_ID,
            tgt.MEMO_LINE_ID = src.MEMO_LINE_ID,
            tgt.MEMO_LINE_NAME = src.MEMO_LINE_NAME,
            tgt.MEMO_LINE_SEQ_ID = src.MEMO_LINE_SEQ_ID,
            tgt.SALESREP_ID = src.SALESREP_ID,
            tgt.SALESREP_NAME = src.SALESREP_NAME,
            tgt.SRC_ATTRIBUTE_CHAR_41 = src.SRC_ATTRIBUTE_CHAR_41,
            tgt.SRC_ATTRIBUTE_CHAR_42 = src.SRC_ATTRIBUTE_CHAR_42,
            tgt.SRC_ATTRIBUTE_CHAR_43 = src.SRC_ATTRIBUTE_CHAR_43,
            tgt.SRC_ATTRIBUTE_CHAR_44 = src.SRC_ATTRIBUTE_CHAR_44,
            tgt.SRC_ATTRIBUTE_CHAR_45 = src.SRC_ATTRIBUTE_CHAR_45,
            tgt.SRC_ATTRIBUTE_CHAR_46 = src.SRC_ATTRIBUTE_CHAR_46,
            tgt.SRC_ATTRIBUTE_CHAR_47 = src.SRC_ATTRIBUTE_CHAR_47,
            tgt.SRC_ATTRIBUTE_CHAR_48 = src.SRC_ATTRIBUTE_CHAR_48,
            tgt.SRC_ATTRIBUTE_CHAR_49 = src.SRC_ATTRIBUTE_CHAR_49,
            tgt.SRC_ATTRIBUTE_CHAR_50 = src.SRC_ATTRIBUTE_CHAR_50,
            tgt.SRC_ATTRIBUTE_CHAR_51 = src.SRC_ATTRIBUTE_CHAR_51,
            tgt.SRC_ATTRIBUTE_CHAR_52 = src.SRC_ATTRIBUTE_CHAR_52,
            tgt.SRC_ATTRIBUTE_CHAR_53 = src.SRC_ATTRIBUTE_CHAR_53,
            tgt.SRC_ATTRIBUTE_CHAR_54 = src.SRC_ATTRIBUTE_CHAR_54,
            tgt.SRC_ATTRIBUTE_CHAR_55 = src.SRC_ATTRIBUTE_CHAR_55,
            tgt.SRC_ATTRIBUTE_CHAR_56 = src.SRC_ATTRIBUTE_CHAR_56,
            tgt.SRC_ATTRIBUTE_DATE_1 = src.SRC_ATTRIBUTE_DATE_1,
            tgt.SRC_ATTRIBUTE_DATE_2 = src.SRC_ATTRIBUTE_DATE_2,
            tgt.SRC_ATTRIBUTE_NUMBER_12 = src.SRC_ATTRIBUTE_NUMBER_12,
            tgt.BZ_LOAD_DATE = src.BZ_LOAD_DATE,
            tgt.SV_LOAD_DATE = src.SV_LOAD_DATE
        WHEN NOT MATCHED BY TARGET THEN INSERT (
            SOURCE_DOCUMENT_LINE_ID, BILL_TO_CUSTOMER_ID, BILL_TO_CUSTOMER_SITE_ID, INVENTORY_ORG_ID, ITEM_ID, MEMO_LINE_ID, MEMO_LINE_NAME, MEMO_LINE_SEQ_ID, SALESREP_ID, SALESREP_NAME,
            SRC_ATTRIBUTE_CHAR_41, SRC_ATTRIBUTE_CHAR_42, SRC_ATTRIBUTE_CHAR_43, SRC_ATTRIBUTE_CHAR_44, SRC_ATTRIBUTE_CHAR_45, SRC_ATTRIBUTE_CHAR_46, SRC_ATTRIBUTE_CHAR_47, SRC_ATTRIBUTE_CHAR_48, SRC_ATTRIBUTE_CHAR_49, SRC_ATTRIBUTE_CHAR_50, SRC_ATTRIBUTE_CHAR_51, SRC_ATTRIBUTE_CHAR_52, SRC_ATTRIBUTE_CHAR_53, SRC_ATTRIBUTE_CHAR_54, SRC_ATTRIBUTE_CHAR_55, SRC_ATTRIBUTE_CHAR_56,
            SRC_ATTRIBUTE_DATE_1, SRC_ATTRIBUTE_DATE_2, SRC_ATTRIBUTE_NUMBER_12, BZ_LOAD_DATE, SV_LOAD_DATE
        ) VALUES (
            src.SOURCE_DOCUMENT_LINE_ID, src.BILL_TO_CUSTOMER_ID, src.BILL_TO_CUSTOMER_SITE_ID, src.INVENTORY_ORG_ID, src.ITEM_ID, src.MEMO_LINE_ID, src.MEMO_LINE_NAME, src.MEMO_LINE_SEQ_ID, src.SALESREP_ID, src.SALESREP_NAME,
            src.SRC_ATTRIBUTE_CHAR_41, src.SRC_ATTRIBUTE_CHAR_42, src.SRC_ATTRIBUTE_CHAR_43, src.SRC_ATTRIBUTE_CHAR_44, src.SRC_ATTRIBUTE_CHAR_45, src.SRC_ATTRIBUTE_CHAR_46, src.SRC_ATTRIBUTE_CHAR_47, src.SRC_ATTRIBUTE_CHAR_48, src.SRC_ATTRIBUTE_CHAR_49, src.SRC_ATTRIBUTE_CHAR_50, src.SRC_ATTRIBUTE_CHAR_51, src.SRC_ATTRIBUTE_CHAR_52, src.SRC_ATTRIBUTE_CHAR_53, src.SRC_ATTRIBUTE_CHAR_54, src.SRC_ATTRIBUTE_CHAR_55, src.SRC_ATTRIBUTE_CHAR_56,
            src.SRC_ATTRIBUTE_DATE_1, src.SRC_ATTRIBUTE_DATE_2, src.SRC_ATTRIBUTE_NUMBER_12, src.BZ_LOAD_DATE, src.SV_LOAD_DATE
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
