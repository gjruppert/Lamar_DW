/* =========================================================
   usp_Load_LINES_CODE_COMBO_LOOKUP
   Full reload when LastWatermark = 1900-01-01 (build from scratch): TRUNCATE + INSERT all rows,
   matching OneOff so F_GL_LINES gets correct COMPANY_SK/ACCOUNT_SK.
   Incremental when LastWatermark > 1900-01-01: batched MERGE on AddDateTime watermark.
   Watermark updated only at end of procedure (Option B1) so retry after failure re-processes full window.
   Required before D_ACCOUNT and others. Ensure etl.ETL_RUN exists.
   ========================================================= */
CREATE OR ALTER PROCEDURE svo.usp_Load_LINES_CODE_COMBO_LOOKUP
    @batch_size INT = 10000,
    @BatchId INT = NULL
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    DECLARE
        @ProcName       SYSNAME       = OBJECT_SCHEMA_NAME(@@PROCID) + '.' + OBJECT_NAME(@@PROCID),
        @TargetObject   SYSNAME       = 'svo.LINES_CODE_COMBO_LOOKUP',
        @RunId          BIGINT        = NULL,
        @TableBridgeID  INT           = NULL,
        @LastWatermark       DATETIME2(7),
        @MaxWatermark        DATETIME2(7),
        @OverallMaxWatermark DATETIME2(7) = NULL,
        @RowInserted         INT           = 0,
        @RowUpdated     INT           = 0,
        @BatchInserted  INT           = 0,
        @BatchUpdated   INT           = 0;

    DECLARE @MergeActions TABLE (ActionTaken VARCHAR(10) NOT NULL);

    SELECT @TableBridgeID = TableBridgeID FROM meta.MedallionTableBridge WHERE targettable = N'GL_CodeCombinationExtractPVO';

    BEGIN TRY
        SELECT @LastWatermark = w.LAST_WATERMARK
        FROM etl.ETL_WATERMARK w
        WHERE w.TABLE_NAME = @TargetObject;

        IF @LastWatermark IS NULL
            SET @LastWatermark = '1900-01-01';

        INSERT INTO etl.ETL_RUN (PROC_NAME, TARGET_OBJECT, ASOF_DATE, START_DTTM, STATUS, BATCH_ID, TABLE_BRIDGE_ID)
        VALUES (@ProcName, @TargetObject, CAST(GETDATE() AS DATE), SYSDATETIME(), 'STARTED', ISNULL(@BatchId, -1), ISNULL(@TableBridgeID, -1));
        SET @RunId = SCOPE_IDENTITY();

        IF @RunId IS NULL
            THROW 50001, 'ETL_RUN insert failed: RUN_ID is NULL. Ensure etl.ETL_RUN exists.', 1;

        /* Full reload when building from scratch: load ALL code combinations (no watermark filter). */
        IF @LastWatermark = '1900-01-01'
        BEGIN
            IF EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'UX_LINES_CODE_COMBO_LOOKUP_BK' AND object_id = OBJECT_ID('svo.LINES_CODE_COMBO_LOOKUP'))
                DROP INDEX UX_LINES_CODE_COMBO_LOOKUP_BK ON svo.LINES_CODE_COMBO_LOOKUP;

            TRUNCATE TABLE svo.LINES_CODE_COMBO_LOOKUP;

            INSERT INTO svo.LINES_CODE_COMBO_LOOKUP
            (CODE_COMBINATION_BK, COMPANY_ID, COMPANY_DESC, COSTCENTER_ID, COSTCENTER_DESC,
             BUSINESSOFFERING_ID, BUSINESSOFFERING_DESC, ACCOUNT_ID, ACCOUNT_DESC, INDUSTRY_ID, INDUSTRY_DESC,
             INTERCOMPANY_ID, INTERCOMPANY_DESC)
            SELECT
                CAST(C1.CODECOMBINATIONCODECOMBINATIONID AS BIGINT) AS CODE_COMBINATION_BK,
                C1.CODECOMBINATIONSEGMENT1   AS COMPANY_ID,
                VSEG1.DESCRIPTION            AS COMPANY_DESC,
                C1.CODECOMBINATIONSEGMENT2   AS COSTCENTER_ID,
                VSEG2.DESCRIPTION            AS COSTCENTER_DESC,
                C1.CODECOMBINATIONSEGMENT3   AS BUSINESSOFFERING_ID,
                VSEG3.DESCRIPTION            AS BUSINESSOFFERING_DESC,
                C1.CODECOMBINATIONSEGMENT4   AS ACCOUNT_ID,
                VSEG4.DESCRIPTION            AS ACCOUNT_DESC,
                C1.CODECOMBINATIONSEGMENT5   AS INDUSTRY_ID,
                VSEG5.DESCRIPTION            AS INDUSTRY_DESC,
                C1.CODECOMBINATIONSEGMENT6   AS INTERCOMPANY_ID,
                VSEG6.DESCRIPTION            AS INTERCOMPANY_DESC
            FROM bzo.GL_CodeCombinationExtractPVO AS C1
            INNER JOIN bzo.GL_ValueSetValuesPVO AS VSEG1 ON C1.CODECOMBINATIONSEGMENT1 = VSEG1.VALUE AND VSEG1.ATTRIBUTECATEGORY = 'COMPANY LAMAR'
            INNER JOIN bzo.GL_ValueSetValuesPVO AS VSEG2 ON C1.CODECOMBINATIONSEGMENT2 = VSEG2.VALUE AND VSEG2.ATTRIBUTECATEGORY = 'CENTER LAMAR'
            INNER JOIN bzo.GL_ValueSetValuesPVO AS VSEG3 ON C1.CODECOMBINATIONSEGMENT3 = VSEG3.VALUE AND VSEG3.ATTRIBUTECATEGORY = 'BUSINESS OFFERING LAMAR'
            INNER JOIN bzo.GL_ValueSetValuesPVO AS VSEG4 ON C1.CODECOMBINATIONSEGMENT4 = VSEG4.VALUE AND VSEG4.ATTRIBUTECATEGORY = 'ACCOUNT LAMAR'
            INNER JOIN bzo.GL_ValueSetValuesPVO AS VSEG5 ON C1.CODECOMBINATIONSEGMENT5 = VSEG5.VALUE AND VSEG5.ATTRIBUTECATEGORY = 'INDUSTRY LAMAR'
            INNER JOIN bzo.GL_ValueSetValuesPVO AS VSEG6 ON C1.CODECOMBINATIONSEGMENT6 = VSEG6.VALUE AND VSEG6.ATTRIBUTECATEGORY = 'INTERCOMPANY LAMAR';

            SET @RowInserted = @@ROWCOUNT;

            SELECT @MaxWatermark = COALESCE(MAX(C1.AddDateTime), '1900-01-02')
            FROM bzo.GL_CodeCombinationExtractPVO AS C1;

            IF @MaxWatermark IS NOT NULL
                MERGE etl.ETL_WATERMARK AS tgt
                USING (SELECT @TargetObject AS TABLE_NAME, @MaxWatermark AS LAST_WATERMARK) AS src
                    ON tgt.TABLE_NAME = src.TABLE_NAME
                WHEN MATCHED THEN
                    UPDATE SET tgt.LAST_WATERMARK = src.LAST_WATERMARK, tgt.UDT_DATE = SYSDATETIME()
                WHEN NOT MATCHED BY TARGET THEN
                    INSERT (TABLE_NAME, LAST_WATERMARK)
                    VALUES (src.TABLE_NAME, src.LAST_WATERMARK);

            IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'UX_LINES_CODE_COMBO_LOOKUP_BK' AND object_id = OBJECT_ID('svo.LINES_CODE_COMBO_LOOKUP'))
                CREATE UNIQUE NONCLUSTERED INDEX UX_LINES_CODE_COMBO_LOOKUP_BK ON svo.LINES_CODE_COMBO_LOOKUP(CODE_COMBINATION_BK) ON FG_SilverMisc;

            UPDATE etl.ETL_RUN
            SET END_DTTM = SYSDATETIME(), STATUS = 'SUCCESS',
                ROW_INSERTED = @RowInserted, ROW_UPDATED = 0, ROW_EXPIRED = 0, ERROR_MESSAGE = NULL
            WHERE RUN_ID = @RunId;

            IF COL_LENGTH('etl.ETL_RUN', 'ROW_UPDATED_T1') IS NOT NULL
                UPDATE etl.ETL_RUN SET ROW_UPDATED_T1 = 0 WHERE RUN_ID = @RunId;

            RETURN;
        END

        IF OBJECT_ID('tempdb..#src') IS NOT NULL
            DROP TABLE #src;

        CREATE TABLE #src (
            CODE_COMBINATION_BK  BIGINT NULL,
            COMPANY_ID           VARCHAR(100) NULL,
            COMPANY_DESC         VARCHAR(500) NULL,
            COSTCENTER_ID        VARCHAR(100) NULL,
            COSTCENTER_DESC      VARCHAR(500) NULL,
            BUSINESSOFFERING_ID  VARCHAR(100) NULL,
            BUSINESSOFFERING_DESC VARCHAR(500) NULL,
            ACCOUNT_ID           VARCHAR(100) NULL,
            ACCOUNT_DESC         VARCHAR(500) NULL,
            INDUSTRY_ID          VARCHAR(100) NULL,
            INDUSTRY_DESC        VARCHAR(500) NULL,
            INTERCOMPANY_ID      VARCHAR(100) NULL,
            INTERCOMPANY_DESC    VARCHAR(500) NULL,
            SourceAddDateTime    DATETIME2(7) NULL
        );

        WHILE 1 = 1
        BEGIN
            DELETE FROM #src;

            ;WITH cte AS (
                SELECT
                    CAST(C1.CODECOMBINATIONCODECOMBINATIONID AS BIGINT) AS CODE_COMBINATION_BK,
                    C1.CODECOMBINATIONSEGMENT1   AS COMPANY_ID,
                    VSEG1.DESCRIPTION            AS COMPANY_DESC,
                    C1.CODECOMBINATIONSEGMENT2   AS COSTCENTER_ID,
                    VSEG2.DESCRIPTION            AS COSTCENTER_DESC,
                    C1.CODECOMBINATIONSEGMENT3   AS BUSINESSOFFERING_ID,
                    VSEG3.DESCRIPTION            AS BUSINESSOFFERING_DESC,
                    C1.CODECOMBINATIONSEGMENT4   AS ACCOUNT_ID,
                    VSEG4.DESCRIPTION            AS ACCOUNT_DESC,
                    C1.CODECOMBINATIONSEGMENT5   AS INDUSTRY_ID,
                    VSEG5.DESCRIPTION            AS INDUSTRY_DESC,
                    C1.CODECOMBINATIONSEGMENT6   AS INTERCOMPANY_ID,
                    VSEG6.DESCRIPTION            AS INTERCOMPANY_DESC,
                    (SELECT MAX(v.dt) FROM (VALUES
                        (C1.AddDateTime),
                        (VSEG1.AddDateTime),
                        (VSEG2.AddDateTime),
                        (VSEG3.AddDateTime),
                        (VSEG4.AddDateTime),
                        (VSEG5.AddDateTime),
                        (VSEG6.AddDateTime)
                    ) v(dt)) AS SourceAddDateTime,
                    ROW_NUMBER() OVER (
                        PARTITION BY CAST(C1.CODECOMBINATIONCODECOMBINATIONID AS BIGINT)
                        ORDER BY (SELECT MAX(v.dt) FROM (VALUES
                            (C1.AddDateTime),(VSEG1.AddDateTime),(VSEG2.AddDateTime),
                            (VSEG3.AddDateTime),(VSEG4.AddDateTime),(VSEG5.AddDateTime),(VSEG6.AddDateTime)
                        ) v(dt)) DESC
                    ) AS rn
                FROM bzo.GL_CodeCombinationExtractPVO AS C1
                INNER JOIN bzo.GL_ValueSetValuesPVO AS VSEG1 ON C1.CODECOMBINATIONSEGMENT1 = VSEG1.VALUE AND VSEG1.ATTRIBUTECATEGORY = 'COMPANY LAMAR'
                INNER JOIN bzo.GL_ValueSetValuesPVO AS VSEG2 ON C1.CODECOMBINATIONSEGMENT2 = VSEG2.VALUE AND VSEG2.ATTRIBUTECATEGORY = 'CENTER LAMAR'
                INNER JOIN bzo.GL_ValueSetValuesPVO AS VSEG3 ON C1.CODECOMBINATIONSEGMENT3 = VSEG3.VALUE AND VSEG3.ATTRIBUTECATEGORY = 'BUSINESS OFFERING LAMAR'
                INNER JOIN bzo.GL_ValueSetValuesPVO AS VSEG4 ON C1.CODECOMBINATIONSEGMENT4 = VSEG4.VALUE AND VSEG4.ATTRIBUTECATEGORY = 'ACCOUNT LAMAR'
                INNER JOIN bzo.GL_ValueSetValuesPVO AS VSEG5 ON C1.CODECOMBINATIONSEGMENT5 = VSEG5.VALUE AND VSEG5.ATTRIBUTECATEGORY = 'INDUSTRY LAMAR'
                INNER JOIN bzo.GL_ValueSetValuesPVO AS VSEG6 ON C1.CODECOMBINATIONSEGMENT6 = VSEG6.VALUE AND VSEG6.ATTRIBUTECATEGORY = 'INTERCOMPANY LAMAR'
                WHERE (C1.AddDateTime > @LastWatermark OR VSEG1.AddDateTime > @LastWatermark OR VSEG2.AddDateTime > @LastWatermark
                    OR VSEG3.AddDateTime > @LastWatermark OR VSEG4.AddDateTime > @LastWatermark OR VSEG5.AddDateTime > @LastWatermark OR VSEG6.AddDateTime > @LastWatermark)
            )
            INSERT INTO #src (CODE_COMBINATION_BK, COMPANY_ID, COMPANY_DESC, COSTCENTER_ID, COSTCENTER_DESC,
                BUSINESSOFFERING_ID, BUSINESSOFFERING_DESC, ACCOUNT_ID, ACCOUNT_DESC, INDUSTRY_ID, INDUSTRY_DESC,
                INTERCOMPANY_ID, INTERCOMPANY_DESC, SourceAddDateTime)
            SELECT TOP (@batch_size)
                CODE_COMBINATION_BK, COMPANY_ID, COMPANY_DESC, COSTCENTER_ID, COSTCENTER_DESC,
                BUSINESSOFFERING_ID, BUSINESSOFFERING_DESC, ACCOUNT_ID, ACCOUNT_DESC, INDUSTRY_ID, INDUSTRY_DESC,
                INTERCOMPANY_ID, INTERCOMPANY_DESC, SourceAddDateTime
            FROM cte
            WHERE rn = 1
            ORDER BY SourceAddDateTime;

            IF @@ROWCOUNT = 0
                BREAK;

            SELECT @MaxWatermark = MAX(SourceAddDateTime) FROM #src;
            SET @OverallMaxWatermark = CASE
                WHEN @OverallMaxWatermark IS NULL THEN @MaxWatermark
                WHEN @MaxWatermark > @OverallMaxWatermark THEN @MaxWatermark
                ELSE @OverallMaxWatermark
            END;

            DELETE FROM @MergeActions;

            MERGE svo.LINES_CODE_COMBO_LOOKUP AS tgt
            USING #src AS src ON tgt.CODE_COMBINATION_BK = src.CODE_COMBINATION_BK
            WHEN MATCHED THEN
                UPDATE SET
                    tgt.COMPANY_ID = src.COMPANY_ID,
                    tgt.COMPANY_DESC = src.COMPANY_DESC,
                    tgt.COSTCENTER_ID = src.COSTCENTER_ID,
                    tgt.COSTCENTER_DESC = src.COSTCENTER_DESC,
                    tgt.BUSINESSOFFERING_ID = src.BUSINESSOFFERING_ID,
                    tgt.BUSINESSOFFERING_DESC = src.BUSINESSOFFERING_DESC,
                    tgt.ACCOUNT_ID = src.ACCOUNT_ID,
                    tgt.ACCOUNT_DESC = src.ACCOUNT_DESC,
                    tgt.INDUSTRY_ID = src.INDUSTRY_ID,
                    tgt.INDUSTRY_DESC = src.INDUSTRY_DESC,
                    tgt.INTERCOMPANY_ID = src.INTERCOMPANY_ID,
                    tgt.INTERCOMPANY_DESC = src.INTERCOMPANY_DESC
            WHEN NOT MATCHED BY TARGET THEN
                INSERT (CODE_COMBINATION_BK, COMPANY_ID, COMPANY_DESC, COSTCENTER_ID, COSTCENTER_DESC,
                    BUSINESSOFFERING_ID, BUSINESSOFFERING_DESC, ACCOUNT_ID, ACCOUNT_DESC, INDUSTRY_ID, INDUSTRY_DESC,
                    INTERCOMPANY_ID, INTERCOMPANY_DESC)
                VALUES (src.CODE_COMBINATION_BK, src.COMPANY_ID, src.COMPANY_DESC, src.COSTCENTER_ID, src.COSTCENTER_DESC,
                    src.BUSINESSOFFERING_ID, src.BUSINESSOFFERING_DESC, src.ACCOUNT_ID, src.ACCOUNT_DESC,
                    src.INDUSTRY_ID, src.INDUSTRY_DESC, src.INTERCOMPANY_ID, src.INTERCOMPANY_DESC)
            OUTPUT $action INTO @MergeActions(ActionTaken);

            SELECT @BatchInserted = SUM(CASE WHEN ActionTaken = 'INSERT' THEN 1 ELSE 0 END),
                   @BatchUpdated   = SUM(CASE WHEN ActionTaken = 'UPDATE' THEN 1 ELSE 0 END)
            FROM @MergeActions;

            SET @RowInserted += @BatchInserted;
            SET @RowUpdated  += @BatchUpdated;

            SET @LastWatermark = @MaxWatermark;

            IF (SELECT COUNT(*) FROM #src) < @batch_size
                BREAK;
        END

        /* Watermark updated only at end so retry after failure re-processes full window (Option B1). */
        IF @OverallMaxWatermark IS NOT NULL
            MERGE etl.ETL_WATERMARK AS tgt
            USING (SELECT @TargetObject AS TABLE_NAME, @OverallMaxWatermark AS LAST_WATERMARK) AS src
                ON tgt.TABLE_NAME = src.TABLE_NAME
            WHEN MATCHED THEN
                UPDATE SET tgt.LAST_WATERMARK = src.LAST_WATERMARK, tgt.UDT_DATE = SYSDATETIME()
            WHEN NOT MATCHED BY TARGET THEN
                INSERT (TABLE_NAME, LAST_WATERMARK)
                VALUES (src.TABLE_NAME, src.LAST_WATERMARK);

        IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'UX_LINES_CODE_COMBO_LOOKUP_BK' AND object_id = OBJECT_ID('svo.LINES_CODE_COMBO_LOOKUP'))
            CREATE UNIQUE NONCLUSTERED INDEX UX_LINES_CODE_COMBO_LOOKUP_BK ON svo.LINES_CODE_COMBO_LOOKUP(CODE_COMBINATION_BK) ON FG_SilverMisc;

        UPDATE etl.ETL_RUN
        SET END_DTTM = SYSDATETIME(), STATUS = 'SUCCESS',
            ROW_INSERTED = @RowInserted, ROW_UPDATED = @RowUpdated, ROW_EXPIRED = 0, ERROR_MESSAGE = NULL
        WHERE RUN_ID = @RunId;

        IF COL_LENGTH('etl.ETL_RUN', 'ROW_UPDATED_T1') IS NOT NULL
            UPDATE etl.ETL_RUN SET ROW_UPDATED_T1 = @RowUpdated WHERE RUN_ID = @RunId;
    END TRY
    BEGIN CATCH
        DECLARE @ErrMsg NVARCHAR(4000) = CONCAT('Error ', ERROR_NUMBER(), ' (Line ', ERROR_LINE(), '): ', ERROR_MESSAGE());
        IF @RunId IS NOT NULL
        BEGIN
            UPDATE etl.ETL_RUN
            SET END_DTTM = SYSDATETIME(), STATUS = 'FAILURE', ROW_INSERTED = @RowInserted, ROW_UPDATED = @RowUpdated,
                ROW_EXPIRED = 0, ERROR_MESSAGE = @ErrMsg
            WHERE RUN_ID = @RunId;
            IF COL_LENGTH('etl.ETL_RUN', 'ROW_UPDATED_T1') IS NOT NULL
                UPDATE etl.ETL_RUN SET ROW_UPDATED_T1 = @RowUpdated WHERE RUN_ID = @RunId;
        END
        IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'UX_LINES_CODE_COMBO_LOOKUP_BK' AND object_id = OBJECT_ID('svo.LINES_CODE_COMBO_LOOKUP'))
            CREATE UNIQUE NONCLUSTERED INDEX UX_LINES_CODE_COMBO_LOOKUP_BK ON svo.LINES_CODE_COMBO_LOOKUP(CODE_COMBINATION_BK) ON FG_SilverMisc;
        ;THROW;
    END CATCH
END;
GO
