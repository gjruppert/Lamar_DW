USE Oracle_Reporting_P2;
GO

IF OBJECT_ID('src.stage_usp_Load_LINES_CODE_COMBO_LOOKUP','P') IS NOT NULL
    DROP PROCEDURE src.stage_usp_Load_LINES_CODE_COMBO_LOOKUP;
GO

CREATE PROCEDURE src.stage_usp_Load_LINES_CODE_COMBO_LOOKUP
(
      @FullReload BIT = 1
    , @Debug      BIT = 0
)
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    DECLARE
          @ProcName   SYSNAME = OBJECT_SCHEMA_NAME(@@PROCID) + '.' + OBJECT_NAME(@@PROCID)
        , @Target     SYSNAME = 'src.stage_LINES_CODE_COMBO_LOOKUP'
        , @LoadLogId  BIGINT
        , @RowsSource BIGINT = 0
        , @RowsIns    BIGINT = 0
        , @RowsUpd    BIGINT = 0
        , @RowsDel    BIGINT = 0;

    BEGIN TRY
        /* Log start */
        IF OBJECT_ID('svo.DW_LOAD_LOG','U') IS NOT NULL
        BEGIN
            INSERT INTO svo.DW_LOAD_LOG (PROC_NAME, TARGET_OBJECT, FULL_RELOAD_FLAG, DEBUG_INFO)
            VALUES (@ProcName, @Target, @FullReload, CASE WHEN @Debug = 1 THEN N'Debug enabled' ELSE NULL END);

            SET @LoadLogId = SCOPE_IDENTITY();
        END

        IF @Debug = 1
            PRINT CONCAT(@ProcName, ' starting. FullReload=', @FullReload);

        /* ---------------------------
           Source set
           --------------------------- */
        IF OBJECT_ID('tempdb..#src') IS NOT NULL DROP TABLE #src;

        SELECT
              CODE_COMBINATION_BK   = CAST(C1.CODECOMBINATIONCODECOMBINATIONID AS BIGINT)
            , COMPANY_ID            = C1.CODECOMBINATIONSEGMENT1
            , COMPANY_DESC          = VSEG1.DESCRIPTION
            , COSTCENTER_ID         = C1.CODECOMBINATIONSEGMENT2
            , COSTCENTER_DESC       = VSEG2.DESCRIPTION
            , BUSINESSOFFERING_ID   = C1.CODECOMBINATIONSEGMENT3
            , BUSINESSOFFERING_DESC = VSEG3.DESCRIPTION
            , ACCOUNT_ID            = C1.CODECOMBINATIONSEGMENT4
            , ACCOUNT_DESC          = VSEG4.DESCRIPTION
            , INDUSTRY_ID           = C1.CODECOMBINATIONSEGMENT5
            , INDUSTRY_DESC         = VSEG5.DESCRIPTION
            , INTERCOMPANY_ID       = C1.CODECOMBINATIONSEGMENT6
            , INTERCOMPANY_DESC     = VSEG6.DESCRIPTION
        INTO #src
        FROM src.bzo_GL_CodeCombinationExtractPVO AS C1
        INNER JOIN src.bzo_GL_ValueSetValuesPVO AS VSEG1
            ON C1.CODECOMBINATIONSEGMENT1 = VSEG1.VALUE
           AND VSEG1.ATTRIBUTECATEGORY = 'COMPANY LAMAR'
        INNER JOIN src.bzo_GL_ValueSetValuesPVO AS VSEG2
            ON C1.CODECOMBINATIONSEGMENT2 = VSEG2.VALUE
           AND VSEG2.ATTRIBUTECATEGORY = 'CENTER LAMAR'
        INNER JOIN src.bzo_GL_ValueSetValuesPVO AS VSEG3
            ON C1.CODECOMBINATIONSEGMENT3 = VSEG3.VALUE
           AND VSEG3.ATTRIBUTECATEGORY = 'BUSINESS OFFERING LAMAR'
        INNER JOIN src.bzo_GL_ValueSetValuesPVO AS VSEG4
            ON C1.CODECOMBINATIONSEGMENT4 = VSEG4.VALUE
           AND VSEG4.ATTRIBUTECATEGORY = 'ACCOUNT LAMAR'
        INNER JOIN src.bzo_GL_ValueSetValuesPVO AS VSEG5
            ON C1.CODECOMBINATIONSEGMENT5 = VSEG5.VALUE
           AND VSEG5.ATTRIBUTECATEGORY = 'INDUSTRY LAMAR'
        INNER JOIN src.bzo_GL_ValueSetValuesPVO AS VSEG6
            ON C1.CODECOMBINATIONSEGMENT6 = VSEG6.VALUE
           AND VSEG6.ATTRIBUTECATEGORY = 'INTERCOMPANY LAMAR'
        WHERE C1.CODECOMBINATIONCODECOMBINATIONID IS NOT NULL;

        /* De-dupe safety on BK */
        ;WITH d AS
        (
            SELECT CODE_COMBINATION_BK, rn = ROW_NUMBER() OVER (PARTITION BY CODE_COMBINATION_BK ORDER BY CODE_COMBINATION_BK)
            FROM #src
        )
        DELETE s
        FROM #src s
        INNER JOIN d
            ON d.CODE_COMBINATION_BK = s.CODE_COMBINATION_BK
           AND d.rn > 1;

        SELECT @RowsSource = COUNT(*) FROM #src;

        IF @Debug = 1
            PRINT CONCAT('Source rows: ', @RowsSource);

        BEGIN TRAN;

        IF @FullReload = 1
        BEGIN
            TRUNCATE TABLE src.stage_LINES_CODE_COMBO_LOOKUP;

            INSERT INTO src.stage_LINES_CODE_COMBO_LOOKUP
            (
                  CODE_COMBINATION_BK
                , COMPANY_ID, COMPANY_DESC
                , COSTCENTER_ID, COSTCENTER_DESC
                , BUSINESSOFFERING_ID, BUSINESSOFFERING_DESC
                , ACCOUNT_ID, ACCOUNT_DESC
                , INDUSTRY_ID, INDUSTRY_DESC
                , INTERCOMPANY_ID, INTERCOMPANY_DESC
            )
            SELECT
                  CODE_COMBINATION_BK
                , COMPANY_ID, COMPANY_DESC
                , COSTCENTER_ID, COSTCENTER_DESC
                , BUSINESSOFFERING_ID, BUSINESSOFFERING_DESC
                , ACCOUNT_ID, ACCOUNT_DESC
                , INDUSTRY_ID, INDUSTRY_DESC
                , INTERCOMPANY_ID, INTERCOMPANY_DESC
            FROM #src;

            SET @RowsIns = @@ROWCOUNT;
        END
        ELSE
        BEGIN
            UPDATE t
                SET
                      t.COMPANY_ID            = s.COMPANY_ID
                    , t.COMPANY_DESC          = s.COMPANY_DESC
                    , t.COSTCENTER_ID         = s.COSTCENTER_ID
                    , t.COSTCENTER_DESC       = s.COSTCENTER_DESC
                    , t.BUSINESSOFFERING_ID   = s.BUSINESSOFFERING_ID
                    , t.BUSINESSOFFERING_DESC = s.BUSINESSOFFERING_DESC
                    , t.ACCOUNT_ID            = s.ACCOUNT_ID
                    , t.ACCOUNT_DESC          = s.ACCOUNT_DESC
                    , t.INDUSTRY_ID           = s.INDUSTRY_ID
                    , t.INDUSTRY_DESC         = s.INDUSTRY_DESC
                    , t.INTERCOMPANY_ID       = s.INTERCOMPANY_ID
                    , t.INTERCOMPANY_DESC     = s.INTERCOMPANY_DESC
            FROM src.stage_LINES_CODE_COMBO_LOOKUP t
            INNER JOIN #src s
                ON s.CODE_COMBINATION_BK = t.CODE_COMBINATION_BK
            WHERE
                ISNULL(t.COMPANY_ID,'')            <> ISNULL(s.COMPANY_ID,'')
             OR ISNULL(t.COMPANY_DESC,'')          <> ISNULL(s.COMPANY_DESC,'')
             OR ISNULL(t.COSTCENTER_ID,'')         <> ISNULL(s.COSTCENTER_ID,'')
             OR ISNULL(t.COSTCENTER_DESC,'')       <> ISNULL(s.COSTCENTER_DESC,'')
             OR ISNULL(t.BUSINESSOFFERING_ID,'')   <> ISNULL(s.BUSINESSOFFERING_ID,'')
             OR ISNULL(t.BUSINESSOFFERING_DESC,'') <> ISNULL(s.BUSINESSOFFERING_DESC,'')
             OR ISNULL(t.ACCOUNT_ID,'')            <> ISNULL(s.ACCOUNT_ID,'')
             OR ISNULL(t.ACCOUNT_DESC,'')          <> ISNULL(s.ACCOUNT_DESC,'')
             OR ISNULL(t.INDUSTRY_ID,'')           <> ISNULL(s.INDUSTRY_ID,'')
             OR ISNULL(t.INDUSTRY_DESC,'')         <> ISNULL(s.INDUSTRY_DESC,'')
             OR ISNULL(t.INTERCOMPANY_ID,'')       <> ISNULL(s.INTERCOMPANY_ID,'')
             OR ISNULL(t.INTERCOMPANY_DESC,'')     <> ISNULL(s.INTERCOMPANY_DESC,'');

            SET @RowsUpd = @@ROWCOUNT;

            INSERT INTO src.stage_LINES_CODE_COMBO_LOOKUP
            (
                  CODE_COMBINATION_BK
                , COMPANY_ID, COMPANY_DESC
                , COSTCENTER_ID, COSTCENTER_DESC
                , BUSINESSOFFERING_ID, BUSINESSOFFERING_DESC
                , ACCOUNT_ID, ACCOUNT_DESC
                , INDUSTRY_ID, INDUSTRY_DESC
                , INTERCOMPANY_ID, INTERCOMPANY_DESC
            )
            SELECT
                  s.CODE_COMBINATION_BK
                , s.COMPANY_ID, s.COMPANY_DESC
                , s.COSTCENTER_ID, s.COSTCENTER_DESC
                , s.BUSINESSOFFERing_ID, s.BUSINESSOFFERING_DESC
                , s.ACCOUNT_ID, s.ACCOUNT_DESC
                , s.INDUSTRY_ID, s.INDUSTRY_DESC
                , s.INTERCOMPANY_ID, s.INTERCOMPANY_DESC
            FROM #src s
            LEFT JOIN src.stage_LINES_CODE_COMBO_LOOKUP t
                ON t.CODE_COMBINATION_BK = s.CODE_COMBINATION_BK
            WHERE t.CODE_COMBINATION_BK IS NULL;

            SET @RowsIns = @@ROWCOUNT;
        END

        COMMIT;

        /* Log success */
        IF OBJECT_ID('svo.DW_LOAD_LOG','U') IS NOT NULL
        BEGIN
            UPDATE svo.DW_LOAD_LOG
                SET
                      LOAD_END_DT   = SYSUTCDATETIME()
                    , STATUS        = 'SUCCESS'
                    , ROWS_SOURCE   = @RowsSource
                    , ROWS_INSERTED = @RowsIns
                    , ROWS_UPDATED  = @RowsUpd
                    , ROWS_DELETED  = @RowsDel
            WHERE LOAD_LOG_ID = @LoadLogId;
        END

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

        IF OBJECT_ID('svo.DW_LOAD_LOG','U') IS NOT NULL
        BEGIN
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
        END

        /* Re-raise in a version-compatible way */
        RAISERROR('%s', @ErrSev, @ErrSta, @ErrMsg);
        RETURN;
    END CATCH
END
GO


