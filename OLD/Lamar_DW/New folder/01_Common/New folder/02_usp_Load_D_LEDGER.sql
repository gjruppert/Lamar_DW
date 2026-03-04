USE [Oracle_Reporting_P2]
GO
/****** Object:  StoredProcedure [svo].[usp_Load_D_LEDGER]    Script Date: 1/19/2026 11:58:37 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

ALTER PROCEDURE [svo].[usp_Load_D_LEDGER]
(
      @FullReload BIT = 0
    , @Debug      BIT = 0
)
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    DECLARE
          @ProcName   SYSNAME = OBJECT_SCHEMA_NAME(@@PROCID) + '.' + OBJECT_NAME(@@PROCID)
        , @Target     SYSNAME = 'svo.D_LEDGER'
        , @LoadLogId  BIGINT
        , @RowsSource BIGINT = 0
        , @RowsIns    BIGINT = 0
        , @RowsUpd    BIGINT = 0
        , @RowsDel    BIGINT = 0;

    BEGIN TRY
        INSERT INTO svo.DW_LOAD_LOG (PROC_NAME, TARGET_OBJECT, FULL_RELOAD_FLAG, DEBUG_INFO)
        VALUES (@ProcName, @Target, @FullReload, CASE WHEN @Debug = 1 THEN N'Debug enabled' ELSE NULL END);

        SET @LoadLogId = SCOPE_IDENTITY();

        IF @Debug = 1
            PRINT CONCAT(@ProcName, ' starting. FullReload=', @FullReload);

        /* ---------------------------
           Source set (with hash)
           --------------------------- */
        IF OBJECT_ID('tempdb..#src') IS NOT NULL DROP TABLE #src;

        SELECT
              LEDGER_ID                    = CAST(LedgerLedgerId AS BIGINT)
            , LEDGER_NAME                  = LedgerName
            , LEDGER_ACCOUNTED_PERIOD_TYPE = LedgerAccountedPeriodType
            , LEDGER_CHART_OF_ACCOUNTS_ID  = CAST(LedgerChartOfAccountsId AS BIGINT)
            , LEDGER_CURRENCY_CODE         = LedgerCurrencyCode
            , LEDGER_DESCRIPTION           = LedgerDescription
            , LEDGER_CATEGORY_CODE         = LedgerLedgerCategoryCode
            , LEDGER_PERIOD_SET_NAME       = LedgerPeriodSetName
            , LEDGER_SHORT_NAME            = LedgerShortName
            , BZ_LOAD_DATE                 = COALESCE(CAST(AddDateTime AS DATE), CAST('0001-01-01' AS DATE))
            , SV_LOAD_DATE                 = CAST(GETDATE() AS DATE)

            , ROW_HASH = HASHBYTES(
                'SHA2_256',
                CONCAT(
                      ISNULL(CONVERT(VARCHAR(30), CAST(LedgerLedgerId AS BIGINT)),''), '|'
                    , ISNULL(LedgerName,''), '|'
                    , ISNULL(LedgerAccountedPeriodType,''), '|'
                    , ISNULL(CONVERT(VARCHAR(30), CAST(LedgerChartOfAccountsId AS BIGINT)),''), '|'
                    , ISNULL(LedgerCurrencyCode,''), '|'
                    , ISNULL(LedgerDescription,''), '|'
                    , ISNULL(LedgerLedgerCategoryCode,''), '|'
                    , ISNULL(LedgerPeriodSetName,''), '|'
                    , ISNULL(LedgerShortName,'')
                )
            )
        INTO #src
        FROM src.bzo_GL_LedgerExtractPVO;

        /* De-dupe safety on natural key */
        ;WITH d AS
        (
            SELECT LEDGER_ID, rn = ROW_NUMBER() OVER (PARTITION BY LEDGER_ID ORDER BY LEDGER_ID)
            FROM #src
        )
        DELETE s
        FROM #src s
        INNER JOIN d
            ON d.LEDGER_ID = s.LEDGER_ID
           AND d.rn > 1;

        SELECT @RowsSource = COUNT(*) FROM #src;

        IF @Debug = 1
            PRINT CONCAT('Source rows: ', @RowsSource);

        BEGIN TRAN;

        /* Ensure SK=0 plug row exists */
        IF NOT EXISTS (SELECT 1 FROM svo.D_LEDGER WHERE LEDGER_SK = 0)
        BEGIN
            SET IDENTITY_INSERT svo.D_LEDGER ON;

            INSERT INTO svo.D_LEDGER
            (
                  LEDGER_SK
                , LEDGER_ID
                , LEDGER_NAME
                , LEDGER_ACCOUNTED_PERIOD_TYPE
                , LEDGER_CHART_OF_ACCOUNTS_ID
                , LEDGER_CURRENCY_CODE
                , LEDGER_DESCRIPTION
                , LEDGER_CATEGORY_CODE
                , LEDGER_PERIOD_SET_NAME
                , LEDGER_SHORT_NAME
                , BZ_LOAD_DATE
                , SV_LOAD_DATE
            )
            VALUES
            (
                  0
                , -1
                , 'Unknown'
                , NULL
                , 0
                , NULL
                , NULL
                , NULL
                , NULL
                , NULL
                , CAST('0001-01-01' AS DATE)
                , CAST(GETDATE() AS DATE)
            );

            SET IDENTITY_INSERT svo.D_LEDGER OFF;
        END

        IF @FullReload = 1
        BEGIN
            DELETE FROM svo.D_LEDGER
            WHERE LEDGER_SK <> 0;

            INSERT INTO svo.D_LEDGER
            (
                  LEDGER_ID
                , LEDGER_NAME
                , LEDGER_ACCOUNTED_PERIOD_TYPE
                , LEDGER_CHART_OF_ACCOUNTS_ID
                , LEDGER_CURRENCY_CODE
                , LEDGER_DESCRIPTION
                , LEDGER_CATEGORY_CODE
                , LEDGER_PERIOD_SET_NAME
                , LEDGER_SHORT_NAME
                , BZ_LOAD_DATE
                , SV_LOAD_DATE
            )
            SELECT
                  s.LEDGER_ID
                , s.LEDGER_NAME
                , s.LEDGER_ACCOUNTED_PERIOD_TYPE
                , s.LEDGER_CHART_OF_ACCOUNTS_ID
                , s.LEDGER_CURRENCY_CODE
                , s.LEDGER_DESCRIPTION
                , s.LEDGER_CATEGORY_CODE
                , s.LEDGER_PERIOD_SET_NAME
                , s.LEDGER_SHORT_NAME
                , s.BZ_LOAD_DATE
                , CAST(GETDATE() AS DATE)
            FROM #src s;

            SET @RowsIns = @@ROWCOUNT;
        END
        ELSE
        BEGIN
            /* Hash-based incremental update/insert */
            IF OBJECT_ID('tempdb..#tgt') IS NOT NULL DROP TABLE #tgt;

            SELECT
                  LEDGER_ID
                , ROW_HASH = HASHBYTES(
                    'SHA2_256',
                    CONCAT(
                          ISNULL(CONVERT(VARCHAR(30), LEDGER_ID),''), '|'
                        , ISNULL(LEDGER_NAME,''), '|'
                        , ISNULL(LEDGER_ACCOUNTED_PERIOD_TYPE,''), '|'
                        , ISNULL(CONVERT(VARCHAR(30), LEDGER_CHART_OF_ACCOUNTS_ID),''), '|'
                        , ISNULL(LEDGER_CURRENCY_CODE,''), '|'
                        , ISNULL(LEDGER_DESCRIPTION,''), '|'
                        , ISNULL(LEDGER_CATEGORY_CODE,''), '|'
                        , ISNULL(LEDGER_PERIOD_SET_NAME,''), '|'
                        , ISNULL(LEDGER_SHORT_NAME,'')
                    )
                  )
            INTO #tgt
            FROM svo.D_LEDGER
            WHERE LEDGER_SK <> 0;

            UPDATE t
                SET
                      t.LEDGER_NAME                  = s.LEDGER_NAME
                    , t.LEDGER_ACCOUNTED_PERIOD_TYPE = s.LEDGER_ACCOUNTED_PERIOD_TYPE
                    , t.LEDGER_CHART_OF_ACCOUNTS_ID  = s.LEDGER_CHART_OF_ACCOUNTS_ID
                    , t.LEDGER_CURRENCY_CODE         = s.LEDGER_CURRENCY_CODE
                    , t.LEDGER_DESCRIPTION           = s.LEDGER_DESCRIPTION
                    , t.LEDGER_CATEGORY_CODE         = s.LEDGER_CATEGORY_CODE
                    , t.LEDGER_PERIOD_SET_NAME       = s.LEDGER_PERIOD_SET_NAME
                    , t.LEDGER_SHORT_NAME            = s.LEDGER_SHORT_NAME
                    , t.BZ_LOAD_DATE                 = s.BZ_LOAD_DATE
                    , t.SV_LOAD_DATE                 = CAST(GETDATE() AS DATE)
            FROM svo.D_LEDGER t
            INNER JOIN #src s
                ON s.LEDGER_ID = t.LEDGER_ID
            INNER JOIN #tgt h
                ON h.LEDGER_ID = t.LEDGER_ID
            WHERE h.ROW_HASH <> s.ROW_HASH;

            SET @RowsUpd = @@ROWCOUNT;

            INSERT INTO svo.D_LEDGER
            (
                  LEDGER_ID
                , LEDGER_NAME
                , LEDGER_ACCOUNTED_PERIOD_TYPE
                , LEDGER_CHART_OF_ACCOUNTS_ID
                , LEDGER_CURRENCY_CODE
                , LEDGER_DESCRIPTION
                , LEDGER_CATEGORY_CODE
                , LEDGER_PERIOD_SET_NAME
                , LEDGER_SHORT_NAME
                , BZ_LOAD_DATE
                , SV_LOAD_DATE
            )
            SELECT
                  s.LEDGER_ID
                , s.LEDGER_NAME
                , s.LEDGER_ACCOUNTED_PERIOD_TYPE
                , s.LEDGER_CHART_OF_ACCOUNTS_ID
                , s.LEDGER_CURRENCY_CODE
                , s.LEDGER_DESCRIPTION
                , s.LEDGER_CATEGORY_CODE
                , s.LEDGER_PERIOD_SET_NAME
                , s.LEDGER_SHORT_NAME
                , s.BZ_LOAD_DATE
                , CAST(GETDATE() AS DATE)
            FROM #src s
            LEFT JOIN svo.D_LEDGER t
                ON t.LEDGER_ID = s.LEDGER_ID
            WHERE t.LEDGER_ID IS NULL;

            SET @RowsIns = @@ROWCOUNT;
        END

        COMMIT;

        UPDATE svo.DW_LOAD_LOG
            SET
                  LOAD_END_DT   = SYSUTCDATETIME()
                , STATUS        = 'SUCCESS'
                , ROWS_SOURCE   = @RowsSource
                , ROWS_INSERTED = @RowsIns
                , ROWS_UPDATED  = @RowsUpd
                , ROWS_DELETED  = @RowsDel
        WHERE LOAD_LOG_ID = @LoadLogId;

        IF @Debug = 1
            PRINT CONCAT('Done. Inserted=', @RowsIns, ' Updated=', @RowsUpd);

    END TRY
    BEGIN CATCH
        IF XACT_STATE() <> 0 ROLLBACK;

        DECLARE
              @ErrMsg NVARCHAR(4000) = ERROR_MESSAGE()
            , @ErrNum INT            = ERROR_NUMBER()
            , @ErrSev INT            = ERROR_SEVERITY()
            , @ErrSta INT            = ERROR_STATE()
            , @ErrLin INT            = ERROR_LINE();

        UPDATE svo.DW_LOAD_LOG
            SET
                  LOAD_END_DT     = SYSUTCDATETIME()
                , STATUS          = 'FAILED'
                , ERROR_NUMBER    = @ErrNum
                , ERROR_SEVERITY  = @ErrSev
                , ERROR_STATE     = @ErrSta
                , ERROR_LINE      = @ErrLin
                , ERROR_MESSAGE   = LEFT(@ErrMsg, 4000)
        WHERE LOAD_LOG_ID = @LoadLogId;

        RAISERROR('%s', @ErrSev, @ErrSta, @ErrMsg);
        RETURN;
    END CATCH
END


