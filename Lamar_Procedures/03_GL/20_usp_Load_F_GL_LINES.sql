/* =========================================================
   usp_Load_F_GL_LINES
   Source: bzo.GL_JournalLineExtractPVO (+ Header).
   Mode: If LastWatermark = 1900-01-01 then full reload (truncate target, one INSERT...SELECT).
   If LastWatermark > 1900-01-01 then incremental (one INSERT...SELECT with AddDateTime > watermark, NOT EXISTS on fact).
   Both paths are single set-based operations (no batching) for maximum throughput.
   Full reload: TRUNCATE then one INSERT...SELECT from source (all rows, no dedupe; matches original SQL).
   Incremental: one INSERT...SELECT from source WHERE AddDateTime > @LastWatermark, WHERE NOT EXISTS (fact).
   Grain: one row per (GLJEHEADERSJEBATCHID, JEHEADERID, JELINENUM, AddDateTime). No dedupe.
   Resolve GL_LINE_PK from CONCAT(batch id, header id, line num); GL_HEADER_SK from svo.D_GL_HEADER;
   other SKs from svo.LINES_CODE_COMBO_LOOKUP + svo.D_* (CURR_IND='Y' for SCD2 dims), D_LEDGER, D_CURRENCY.
   @batch_size: reserved/unused (kept for backward compatibility).
   @drop_non_lookup_indexes_during_load: when 1, drop non-lookup indexes before load, recreate after. Default 1.
   ========================================================= */
CREATE OR ALTER PROCEDURE svo.usp_Load_F_GL_LINES
    @batch_size INT = 50000,
    @drop_non_lookup_indexes_during_load BIT = 1,
    @BatchId INT = NULL
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    DECLARE
        @ProcName         SYSNAME        = OBJECT_SCHEMA_NAME(@@PROCID) + '.' + OBJECT_NAME(@@PROCID),
        @TargetObject     SYSNAME        = 'svo.F_GL_LINES',
        @StartDttm        DATETIME2(0)   = SYSDATETIME(),
        @EndDttm          DATETIME2(0),
        @RunId             BIGINT         = NULL,
        @ErrMsg            NVARCHAR(4000) = NULL,
        @AsOfDate          DATE           = CAST(GETDATE() AS DATE),
        @LastWatermark     DATETIME2(7),
        @RunningWatermark  DATETIME2(7),
        @MaxWatermark      DATETIME2(7)   = NULL,
        @LastJELINENUM     INT            = NULL,
        @RowInserted       INT            = 0,
        @BatchInserted     INT            = 0,
        @BatchNum          INT            = 0,
        @Cnt               INT            = 0,
        @kAdd              DATETIME2(7)   = NULL,
        @kJE               BIGINT         = NULL,
        @kJEL              INT            = NULL,
        @kJEVal            BIGINT         = NULL,
        @kJELVal           INT            = NULL,
        @Sql               NVARCHAR(MAX)  = NULL,
        @TableBridgeID     INT            = NULL;

    SELECT @TableBridgeID = TableBridgeID FROM meta.MedallionTableBridge WHERE targettable = N'GL_JournalLineExtractPVO';

    BEGIN TRY
        /* Clear temp tables from any prior run in same session */
        IF OBJECT_ID('tempdb..#lines') IS NOT NULL DROP TABLE #lines;
        IF OBJECT_ID('tempdb..#to_insert') IS NOT NULL DROP TABLE #to_insert;
        IF OBJECT_ID('tempdb..#existing') IS NOT NULL DROP TABLE #existing;
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

        PRINT 'usp_Load_F_GL_LINES: Started. LastWatermark = ' + CONVERT(VARCHAR(30), @LastWatermark, 121);

        /* Optional: drop non-lookup indexes to speed inserts on full reload; recreate at end (or in CATCH) */
        IF @drop_non_lookup_indexes_during_load = 1 AND @LastWatermark = '1900-01-01'
        BEGIN
            IF EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'IX_F_GL_LINES_COMPANY_SK' AND object_id = OBJECT_ID('svo.F_GL_LINES'))
                DROP INDEX IX_F_GL_LINES_COMPANY_SK ON svo.F_GL_LINES;
            IF EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'IX_F_GL_LINES_ACCOUNT_SK' AND object_id = OBJECT_ID('svo.F_GL_LINES'))
                DROP INDEX IX_F_GL_LINES_ACCOUNT_SK ON svo.F_GL_LINES;
            PRINT 'usp_Load_F_GL_LINES: Dropped non-lookup indexes for load.';
        END

        IF @LastWatermark = '1900-01-01'
        BEGIN
            /* Full reload: one INSERT...SELECT from source (all rows, no dedupe; lookups inline). No temp tables, one transaction. */
            PRINT 'usp_Load_F_GL_LINES: Full reload (watermark = 1900-01-01). Truncating target and loading all rows (single set-based insert).';
            TRUNCATE TABLE svo.F_GL_LINES;

            INSERT INTO svo.F_GL_LINES WITH (TABLOCK)
            (GL_LINE_PK, GL_HEADER_SK, ACCOUNT_SK, BUSINESS_OFFERING_SK, COMPANY_SK, COST_CENTER_SK, CURRENCY_SK, INDUSTRY_SK, INTERCOMPANY_SK, EFFECTIVE_DATE_SK, LEDGER_SK, LINE_NUM, DESCRIPTION, ACCOUNTED_CR, ACCOUNTED_DR, AMOUNT_USD, AMOUNT_LOCAL, CREATED_BY, LAST_UPDATED_BY, LAST_UPDATED_DATE, CREATION_DATE, BZ_LOAD_DATE, SV_LOAD_DATE, CODE_COMBINATION_ID)
            SELECT
                CAST(CONCAT(H.GLJEHEADERSJEBATCHID, H.JEHEADERID, L.JELINENUM) AS BIGINT),
                ISNULL(DH.GL_HEADER_SK, 0),
                ISNULL(DA.ACCOUNT_SK, 0),
                ISNULL(DBO.BUSINESS_OFFERING_SK, 0),
                ISNULL(DCO.COMPANY_SK, 0),
                ISNULL(DCC.COST_CENTER_SK, 0),
                ISNULL(CUR.CURRENCY_SK, 0),
                ISNULL(DI.INDUSTRY_SK, 0),
                ISNULL(DIC.INTERCOMPANY_SK, 0),
                ISNULL(CAST(CONVERT(CHAR(8), TRY_CONVERT(DATE, L.GLJELINESEFFECTIVEDATE), 112) AS INT), 0),
                ISNULL(LDG.LEDGER_SK, 0),
                CAST(L.JELINENUM AS BIGINT),
                NULLIF(LTRIM(RTRIM(L.GLJELINESDESCRIPTION)), ''),
                CAST(ISNULL(L.GLJELINESACCOUNTEDCR, 0) AS decimal(29,4)),
                CAST(ISNULL(L.GLJELINESACCOUNTEDDR, 0) AS decimal(29,4)),
                CASE L.GlJeLinesLedgerId WHEN '300000004574005' THEN 0 ELSE (CAST(ISNULL(L.GLJELINESACCOUNTEDDR, 0) AS decimal(29,4)) - CAST(ISNULL(L.GLJELINESACCOUNTEDCR, 0) AS decimal(29,4))) * CAST(ISNULL(L.GLJELINESCURRENCYCONVERSIONRATE, 1) AS NUMERIC(7,4)) END,
                (CAST(ISNULL(L.GLJELINESACCOUNTEDDR, 0) AS decimal(29,4)) - CAST(ISNULL(L.GLJELINESACCOUNTEDCR, 0) AS decimal(29,4))),
                NULLIF(TRIM(L.GLJELINESCREATEDBY), ''),
                NULLIF(TRIM(L.GLJELINESLASTUPDATEDBY), ''),
                CAST(L.GLJELINESLASTUPDATEDATE AS DATE),
                CAST(L.GLJELINESCREATIONDATE AS DATE),
                L.AddDateTime,
                SYSDATETIME(),
                L.GlJeLinesCodeCombinationId
            FROM (SELECT * FROM bzo.GL_JournalLineExtractPVO WITH (NOLOCK)) L
            INNER JOIN (SELECT * FROM bzo.GL_JournalHeaderExtractPVO WITH (NOLOCK)) H ON H.JEHEADERID = L.JEHEADERID
            LEFT JOIN svo.LINES_CODE_COMBO_LOOKUP C ON CAST(L.GLJELINESCODECOMBINATIONID AS BIGINT) = C.CODE_COMBINATION_BK
            LEFT JOIN svo.D_GL_HEADER AS DH ON DH.JE_HEADER_ID = L.JEHEADERID AND DH.CURR_IND = 'Y'
            LEFT JOIN svo.D_ACCOUNT AS DA ON DA.ACCOUNT_ID = C.ACCOUNT_ID AND DA.CURR_IND = 'Y'
            LEFT JOIN svo.D_BUSINESS_OFFERING AS DBO ON DBO.BUSINESS_OFFERING_ID = C.BUSINESSOFFERING_ID AND DBO.CURR_IND = 'Y'
            LEFT JOIN svo.D_COMPANY AS DCO ON DCO.COMPANY_ID = C.COMPANY_ID AND DCO.CURR_IND = 'Y'
            LEFT JOIN svo.D_COST_CENTER AS DCC ON DCC.COST_CENTER_ID = C.COSTCENTER_ID AND DCC.CURR_IND = 'Y'
            LEFT JOIN svo.D_CURRENCY AS CUR ON CUR.CURRENCY_ID = CONCAT(ISNULL(L.GLJELINESCURRENCYCODE, 'UNK'), CONVERT(CHAR(8), CONVERT(CHAR(8), ISNULL(L.GLJELINESCURRENCYCONVERSIONDATE, '0001-01-01'), 112)), ISNULL(TRIM(L.GLJELINESCURRENCYCONVERSIONTYPE), 'UNK'))
            LEFT JOIN svo.D_INDUSTRY AS DI ON DI.INDUSTRY_ID = C.INDUSTRY_ID AND DI.CURR_IND = 'Y'
            LEFT JOIN svo.D_INTERCOMPANY AS DIC ON DIC.INTERCOMPANY_ID = C.INTERCOMPANY_ID AND DIC.CURR_IND = 'Y'
            LEFT JOIN svo.D_LEDGER AS LDG ON LDG.LEDGER_ID = L.GlJeLinesLedgerId AND LDG.CURR_IND = 'Y'
            ;

            SET @RowInserted = @@ROWCOUNT;
            SELECT @MaxWatermark = MAX(AddDateTime) FROM (SELECT AddDateTime FROM bzo.GL_JournalLineExtractPVO WITH (NOLOCK)) x;
            PRINT 'usp_Load_F_GL_LINES: Full reload complete. Inserted ' + CAST(@RowInserted AS VARCHAR(20)) + ' rows.';
        END
        ELSE
        BEGIN
            /* Incremental: single set-based INSERT (AddDateTime > @LastWatermark, NOT EXISTS on fact). No batching. */
            PRINT 'usp_Load_F_GL_LINES: Incremental load (single set-based insert).';

            INSERT INTO svo.F_GL_LINES WITH (TABLOCK)
            (GL_LINE_PK, GL_HEADER_SK, ACCOUNT_SK, BUSINESS_OFFERING_SK, COMPANY_SK, COST_CENTER_SK, CURRENCY_SK, INDUSTRY_SK, INTERCOMPANY_SK, EFFECTIVE_DATE_SK, LEDGER_SK, LINE_NUM, DESCRIPTION, ACCOUNTED_CR, ACCOUNTED_DR, AMOUNT_USD, AMOUNT_LOCAL, CREATED_BY, LAST_UPDATED_BY, LAST_UPDATED_DATE, CREATION_DATE, BZ_LOAD_DATE, SV_LOAD_DATE, CODE_COMBINATION_ID)
            SELECT
                CAST(CONCAT(H.GLJEHEADERSJEBATCHID, H.JEHEADERID, L.JELINENUM) AS BIGINT),
                ISNULL(DH.GL_HEADER_SK, 0),
                ISNULL(DA.ACCOUNT_SK, 0),
                ISNULL(DBO.BUSINESS_OFFERING_SK, 0),
                ISNULL(DCO.COMPANY_SK, 0),
                ISNULL(DCC.COST_CENTER_SK, 0),
                ISNULL(CUR.CURRENCY_SK, 0),
                ISNULL(DI.INDUSTRY_SK, 0),
                ISNULL(DIC.INTERCOMPANY_SK, 0),
                ISNULL(CAST(CONVERT(CHAR(8), TRY_CONVERT(DATE, L.GLJELINESEFFECTIVEDATE), 112) AS INT), 0),
                ISNULL(LDG.LEDGER_SK, 0),
                CAST(L.JELINENUM AS BIGINT),
                NULLIF(LTRIM(RTRIM(L.GLJELINESDESCRIPTION)), ''),
                CAST(ISNULL(L.GLJELINESACCOUNTEDCR, 0) AS decimal(29,4)),
                CAST(ISNULL(L.GLJELINESACCOUNTEDDR, 0) AS decimal(29,4)),
                CASE L.GlJeLinesLedgerId WHEN '300000004574005' THEN 0 ELSE (CAST(ISNULL(L.GLJELINESACCOUNTEDDR, 0) AS decimal(29,4)) - CAST(ISNULL(L.GLJELINESACCOUNTEDCR, 0) AS decimal(29,4))) * CAST(ISNULL(L.GLJELINESCURRENCYCONVERSIONRATE, 1) AS NUMERIC(7,4)) END,
                (CAST(ISNULL(L.GLJELINESACCOUNTEDDR, 0) AS decimal(29,4)) - CAST(ISNULL(L.GLJELINESACCOUNTEDCR, 0) AS decimal(29,4))),
                NULLIF(TRIM(L.GLJELINESCREATEDBY), ''),
                NULLIF(TRIM(L.GLJELINESLASTUPDATEDBY), ''),
                CAST(L.GLJELINESLASTUPDATEDATE AS DATE),
                CAST(L.GLJELINESCREATIONDATE AS DATE),
                L.AddDateTime,
                SYSDATETIME(),
                L.GlJeLinesCodeCombinationId
            FROM (SELECT * FROM bzo.GL_JournalLineExtractPVO WITH (NOLOCK)) L
            INNER JOIN (SELECT * FROM bzo.GL_JournalHeaderExtractPVO WITH (NOLOCK)) H ON H.JEHEADERID = L.JEHEADERID
            LEFT JOIN svo.LINES_CODE_COMBO_LOOKUP C ON CAST(L.GLJELINESCODECOMBINATIONID AS BIGINT) = C.CODE_COMBINATION_BK
            LEFT JOIN svo.D_GL_HEADER AS DH ON DH.JE_HEADER_ID = L.JEHEADERID AND DH.CURR_IND = 'Y'
            LEFT JOIN svo.D_ACCOUNT AS DA ON DA.ACCOUNT_ID = C.ACCOUNT_ID AND DA.CURR_IND = 'Y'
            LEFT JOIN svo.D_BUSINESS_OFFERING AS DBO ON DBO.BUSINESS_OFFERING_ID = C.BUSINESSOFFERING_ID AND DBO.CURR_IND = 'Y'
            LEFT JOIN svo.D_COMPANY AS DCO ON DCO.COMPANY_ID = C.COMPANY_ID AND DCO.CURR_IND = 'Y'
            LEFT JOIN svo.D_COST_CENTER AS DCC ON DCC.COST_CENTER_ID = C.COSTCENTER_ID AND DCC.CURR_IND = 'Y'
            LEFT JOIN svo.D_CURRENCY AS CUR ON CUR.CURRENCY_ID = CONCAT(ISNULL(L.GLJELINESCURRENCYCODE, 'UNK'), CONVERT(CHAR(8), CONVERT(CHAR(8), ISNULL(L.GLJELINESCURRENCYCONVERSIONDATE, '0001-01-01'), 112)), ISNULL(TRIM(L.GLJELINESCURRENCYCONVERSIONTYPE), 'UNK'))
            LEFT JOIN svo.D_INDUSTRY AS DI ON DI.INDUSTRY_ID = C.INDUSTRY_ID AND DI.CURR_IND = 'Y'
            LEFT JOIN svo.D_INTERCOMPANY AS DIC ON DIC.INTERCOMPANY_ID = C.INTERCOMPANY_ID AND DIC.CURR_IND = 'Y'
            LEFT JOIN svo.D_LEDGER AS LDG ON LDG.LEDGER_ID = L.GlJeLinesLedgerId AND LDG.CURR_IND = 'Y'
            WHERE L.AddDateTime > @LastWatermark
              AND NOT EXISTS (SELECT 1 FROM svo.F_GL_LINES f WHERE f.GL_HEADER_SK = ISNULL(DH.GL_HEADER_SK, 0) AND f.LINE_NUM = L.JELINENUM);

            SET @RowInserted = @@ROWCOUNT;
            SELECT @MaxWatermark = MAX(AddDateTime) FROM (SELECT AddDateTime FROM bzo.GL_JournalLineExtractPVO WITH (NOLOCK) WHERE AddDateTime > @LastWatermark) x;
            PRINT 'usp_Load_F_GL_LINES: Incremental complete. Inserted ' + CAST(@RowInserted AS VARCHAR(20)) + ' rows.';
        END

        PRINT 'usp_Load_F_GL_LINES: Complete. Total rows inserted = ' + CAST(@RowInserted AS VARCHAR(20));
        IF @MaxWatermark IS NOT NULL
            PRINT 'usp_Load_F_GL_LINES: Watermark updated to ' + CONVERT(VARCHAR(30), @MaxWatermark, 121);

        /* Recreate indexes if we dropped them for the load */
        IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'IX_F_GL_LINES_COMPANY_SK' AND object_id = OBJECT_ID('svo.F_GL_LINES'))
            CREATE NONCLUSTERED INDEX IX_F_GL_LINES_COMPANY_SK ON svo.F_GL_LINES(COMPANY_SK)
            INCLUDE (GL_HEADER_SK, ACCOUNT_SK, BUSINESS_OFFERING_SK, COST_CENTER_SK, CURRENCY_SK, INDUSTRY_SK, INTERCOMPANY_SK, EFFECTIVE_DATE_SK, LEDGER_SK, LINE_NUM, DESCRIPTION, AMOUNT_USD, AMOUNT_LOCAL) ON FG_SilverFact;
        IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'IX_F_GL_LINES_ACCOUNT_SK' AND object_id = OBJECT_ID('svo.F_GL_LINES'))
            CREATE NONCLUSTERED INDEX IX_F_GL_LINES_ACCOUNT_SK ON svo.F_GL_LINES(ACCOUNT_SK)
            INCLUDE (GL_HEADER_SK, BUSINESS_OFFERING_SK, COMPANY_SK, COST_CENTER_SK, CURRENCY_SK, INDUSTRY_SK, INTERCOMPANY_SK, EFFECTIVE_DATE_SK, LEDGER_SK, LINE_NUM, DESCRIPTION, AMOUNT_USD, AMOUNT_LOCAL) ON FG_SilverFact;

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
        /* Restore indexes if we dropped them, so table is left in consistent state */
        IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'IX_F_GL_LINES_COMPANY_SK' AND object_id = OBJECT_ID('svo.F_GL_LINES'))
            CREATE NONCLUSTERED INDEX IX_F_GL_LINES_COMPANY_SK ON svo.F_GL_LINES(COMPANY_SK)
            INCLUDE (GL_HEADER_SK, ACCOUNT_SK, BUSINESS_OFFERING_SK, COST_CENTER_SK, CURRENCY_SK, INDUSTRY_SK, INTERCOMPANY_SK, EFFECTIVE_DATE_SK, LEDGER_SK, LINE_NUM, DESCRIPTION, AMOUNT_USD, AMOUNT_LOCAL) ON FG_SilverFact;
        IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'IX_F_GL_LINES_ACCOUNT_SK' AND object_id = OBJECT_ID('svo.F_GL_LINES'))
            CREATE NONCLUSTERED INDEX IX_F_GL_LINES_ACCOUNT_SK ON svo.F_GL_LINES(ACCOUNT_SK)
            INCLUDE (GL_HEADER_SK, BUSINESS_OFFERING_SK, COMPANY_SK, COST_CENTER_SK, CURRENCY_SK, INDUSTRY_SK, INTERCOMPANY_SK, EFFECTIVE_DATE_SK, LEDGER_SK, LINE_NUM, DESCRIPTION, AMOUNT_USD, AMOUNT_LOCAL) ON FG_SilverFact;
        ;THROW;
    END CATCH
END;
GO
