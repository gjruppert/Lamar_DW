CREATE OR ALTER PROCEDURE svo.usp_Load_LINES_CODE_COMBO_LOOKUP_T1
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    DECLARE
        @ProcName       SYSNAME       = OBJECT_SCHEMA_NAME(@@PROCID) + '.' + OBJECT_NAME(@@PROCID),
        @TargetObject   SYSNAME       = 'svo.LINES_CODE_COMBO_LOOKUP',
        @StartDttm      DATETIME2(0)  = SYSDATETIME(),
        @EndDttm        DATETIME2(0),
        @Status         VARCHAR(20)   = 'SUCCESS',
        @ErrMsg         NVARCHAR(4000) = NULL,
        @RunId          BIGINT        = NULL,

        @LastWatermark  DATETIME2(7),
        @MaxWatermark   DATETIME2(7) = NULL,

        @RowInserted    INT           = 0,
        @RowUpdatedT1   INT           = 0,
        @RowExpired     INT           = 0; -- not used for T1

    BEGIN TRY
        /* ===== Watermark ===== */
        SELECT
            @LastWatermark = w.LAST_WATERMARK
        FROM etl.ETL_WATERMARK w
        WHERE w.TABLE_NAME = @TargetObject;

        IF @LastWatermark IS NULL
            SET @LastWatermark = '1900-01-01';

        /* ===== ETL_RUN start ===== */
        INSERT INTO etl.ETL_RUN
        (
            PROC_NAME,
            TARGET_OBJECT,
            ASOF_DATE,
            START_DTTM,
            STATUS
        )
        VALUES
        (
            @ProcName,
            @TargetObject,
            CAST(GETDATE() AS DATE),
            @StartDttm,
            'STARTED'
        );

        SET @RunId = SCOPE_IDENTITY();

        /* ===== Incremental, deduped source ===== */
        IF OBJECT_ID('tempdb..#src') IS NOT NULL
            DROP TABLE #src;

        SELECT
            s.CODE_COMBINATION_BK,
            s.COMPANY_ID,
            s.COMPANY_DESC,
            s.COSTCENTER_ID,
            s.COSTCENTER_DESC,
            s.BUSINESSOFFERING_ID,
            s.BUSINESSOFFERING_DESC,
            s.ACCOUNT_ID,
            s.ACCOUNT_DESC,
            s.INDUSTRY_ID,
            s.INDUSTRY_DESC,
            s.INTERCOMPANY_ID,
            s.INTERCOMPANY_DESC,
            s.SourceAddDateTime
        INTO #src
        FROM
        (
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

                (SELECT MAX(v.dt)
                 FROM (VALUES
                        (C1.AddDateTime),
                        (VSEG1.AddDateTime),
                        (VSEG2.AddDateTime),
                        (VSEG3.AddDateTime),
                        (VSEG4.AddDateTime),
                        (VSEG5.AddDateTime),
                        (VSEG6.AddDateTime)
                      ) v(dt)
                ) AS SourceAddDateTime,

                ROW_NUMBER() OVER
                (
                    PARTITION BY CAST(C1.CODECOMBINATIONCODECOMBINATIONID AS BIGINT)
                    ORDER BY
                        (SELECT MAX(v.dt)
                         FROM (VALUES
                                (C1.AddDateTime),
                                (VSEG1.AddDateTime),
                                (VSEG2.AddDateTime),
                                (VSEG3.AddDateTime),
                                (VSEG4.AddDateTime),
                                (VSEG5.AddDateTime),
                                (VSEG6.AddDateTime)
                              ) v(dt)
                        ) DESC
                ) AS rn
            FROM bzo.GL_CodeCombinationExtractPVO AS C1
            INNER JOIN bzo.GL_ValueSetValuesPVO AS VSEG1
                ON C1.CODECOMBINATIONSEGMENT1 = VSEG1.VALUE
               AND VSEG1.ATTRIBUTECATEGORY = 'COMPANY LAMAR'
            INNER JOIN bzo.GL_ValueSetValuesPVO AS VSEG2
                ON C1.CODECOMBINATIONSEGMENT2 = VSEG2.VALUE
               AND VSEG2.ATTRIBUTECATEGORY = 'CENTER LAMAR'
            INNER JOIN bzo.GL_ValueSetValuesPVO AS VSEG3
                ON C1.CODECOMBINATIONSEGMENT3 = VSEG3.VALUE
               AND VSEG3.ATTRIBUTECATEGORY = 'BUSINESS OFFERING LAMAR'
            INNER JOIN bzo.GL_ValueSetValuesPVO AS VSEG4
                ON C1.CODECOMBINATIONSEGMENT4 = VSEG4.VALUE
               AND VSEG4.ATTRIBUTECATEGORY = 'ACCOUNT LAMAR'
            INNER JOIN bzo.GL_ValueSetValuesPVO AS VSEG5
                ON C1.CODECOMBINATIONSEGMENT5 = VSEG5.VALUE
               AND VSEG5.ATTRIBUTECATEGORY = 'INDUSTRY LAMAR'
            INNER JOIN bzo.GL_ValueSetValuesPVO AS VSEG6
                ON C1.CODECOMBINATIONSEGMENT6 = VSEG6.VALUE
               AND VSEG6.ATTRIBUTECATEGORY = 'INTERCOMPANY LAMAR'
            WHERE
                (C1.AddDateTime    > @LastWatermark OR
                 VSEG1.AddDateTime > @LastWatermark OR
                 VSEG2.AddDateTime > @LastWatermark OR
                 VSEG3.AddDateTime > @LastWatermark OR
                 VSEG4.AddDateTime > @LastWatermark OR
                 VSEG5.AddDateTime > @LastWatermark OR
                 VSEG6.AddDateTime > @LastWatermark)
        ) s
        WHERE s.rn = 1;

        SELECT @MaxWatermark = MAX(SourceAddDateTime) FROM #src;

        /* ===== MERGE (Type 1 Upsert) ===== */
        DECLARE @MergeActions TABLE (ActionTaken VARCHAR(10) NOT NULL);

        MERGE svo.LINES_CODE_COMBO_LOOKUP AS tgt
        USING #src AS src
            ON tgt.CODE_COMBINATION_BK = src.CODE_COMBINATION_BK
        WHEN MATCHED THEN
            UPDATE SET
                tgt.COMPANY_ID             = src.COMPANY_ID,
                tgt.COMPANY_DESC           = src.COMPANY_DESC,
                tgt.COSTCENTER_ID          = src.COSTCENTER_ID,
                tgt.COSTCENTER_DESC        = src.COSTCENTER_DESC,
                tgt.BUSINESSOFFERING_ID    = src.BUSINESSOFFERING_ID,
                tgt.BUSINESSOFFERING_DESC  = src.BUSINESSOFFERING_DESC,
                tgt.ACCOUNT_ID             = src.ACCOUNT_ID,
                tgt.ACCOUNT_DESC           = src.ACCOUNT_DESC,
                tgt.INDUSTRY_ID            = src.INDUSTRY_ID,
                tgt.INDUSTRY_DESC          = src.INDUSTRY_DESC,
                tgt.INTERCOMPANY_ID        = src.INTERCOMPANY_ID,
                tgt.INTERCOMPANY_DESC      = src.INTERCOMPANY_DESC
        WHEN NOT MATCHED BY TARGET THEN
            INSERT
            (
                CODE_COMBINATION_BK,
                COMPANY_ID,
                COMPANY_DESC,
                COSTCENTER_ID,
                COSTCENTER_DESC,
                BUSINESSOFFERING_ID,
                BUSINESSOFFERING_DESC,
                ACCOUNT_ID,
                ACCOUNT_DESC,
                INDUSTRY_ID,
                INDUSTRY_DESC,
                INTERCOMPANY_ID,
                INTERCOMPANY_DESC
            )
            VALUES
            (
                src.CODE_COMBINATION_BK,
                src.COMPANY_ID,
                src.COMPANY_DESC,
                src.COSTCENTER_ID,
                src.COSTCENTER_DESC,
                src.BUSINESSOFFERING_ID,
                src.BUSINESSOFFERING_DESC,
                src.ACCOUNT_ID,
                src.ACCOUNT_DESC,
                src.INDUSTRY_ID,
                src.INDUSTRY_DESC,
                src.INTERCOMPANY_ID,
                src.INTERCOMPANY_DESC
            )
        OUTPUT $action INTO @MergeActions(ActionTaken);

        SELECT
            @RowInserted  = SUM(CASE WHEN ActionTaken = 'INSERT' THEN 1 ELSE 0 END),
            @RowUpdatedT1 = SUM(CASE WHEN ActionTaken = 'UPDATE' THEN 1 ELSE 0 END)
        FROM @MergeActions;

        /* ===== Advance watermark only if rows processed ===== */
        IF @MaxWatermark IS NOT NULL
        BEGIN
            UPDATE etl.ETL_WATERMARK
            SET
                LAST_WATERMARK = @MaxWatermark,
                UDT_DATE       = SYSDATETIME()
            WHERE TABLE_NAME = @TargetObject;
        END

        /* ===== ETL_RUN end ===== */
        SET @EndDttm = SYSDATETIME();

        UPDATE etl.ETL_RUN
        SET
            END_DTTM        = @EndDttm,
            STATUS          = 'SUCCESS',
            ROW_INSERTED    = @RowInserted,
            ROW_UPDATED_T1  = @RowUpdatedT1,
            ROW_EXPIRED     = @RowExpired,
            ERROR_MESSAGE   = NULL
        WHERE RUN_ID = @RunId;
    END TRY
    BEGIN CATCH
        SET @EndDttm = SYSDATETIME();
        SET @ErrMsg  = CONCAT('Error ', ERROR_NUMBER(), ' (Line ', ERROR_LINE(), '): ', ERROR_MESSAGE());

        IF @RunId IS NOT NULL
        BEGIN
            UPDATE etl.ETL_RUN
            SET
                END_DTTM        = @EndDttm,
                STATUS          = 'FAILURE',
                ROW_INSERTED    = @RowInserted,
                ROW_UPDATED_T1  = @RowUpdatedT1,
                ROW_EXPIRED     = @RowExpired,
                ERROR_MESSAGE   = @ErrMsg
            WHERE RUN_ID = @RunId;
        END

        ;THROW;
    END CATCH
END;
GO