/* =========================================================
   usp_Load_D_PAYMENT_TERM
   SCD2 incremental load. Source: bzo.AP_PaymentTermHeaderTranslationExtractPVO
   Watermark filter, expire changed rows, insert new current rows.
   Idempotent: safe to retry same watermark range.
   ========================================================= */
CREATE OR ALTER PROCEDURE svo.usp_Load_D_PAYMENT_TERM
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    DECLARE
        @ProcName       SYSNAME        = OBJECT_SCHEMA_NAME(@@PROCID) + '.' + OBJECT_NAME(@@PROCID),
        @TargetObject   SYSNAME        = 'svo.D_PAYMENT_TERM',
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
        @RowUpdated     INT            = 0;

    BEGIN TRY
        SELECT @LastWatermark = w.LAST_WATERMARK
        FROM etl.ETL_WATERMARK w
        WHERE w.TABLE_NAME = @TargetObject;

        IF @LastWatermark IS NULL
            SET @LastWatermark = '1900-01-01';

        INSERT INTO etl.ETL_RUN (PROC_NAME, TARGET_OBJECT, ASOF_DATE, START_DTTM, STATUS)
        VALUES (@ProcName, @TargetObject, @AsOfDate, @StartDttm, 'STARTED');

        SET @RunId = SCOPE_IDENTITY();

        IF @RunId IS NULL
            THROW 50001, 'ETL_RUN insert failed: RUN_ID is NULL.', 1;

        IF EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'UX_D_PAYMENT_TERM_ID' AND object_id = OBJECT_ID('svo.D_PAYMENT_TERM'))
        BEGIN
            DROP INDEX UX_D_PAYMENT_TERM_ID ON svo.D_PAYMENT_TERM;
        END;

        IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'UX_D_PAYMENT_TERM_BK_CURR' AND object_id = OBJECT_ID('svo.D_PAYMENT_TERM'))
        BEGIN
            CREATE UNIQUE NONCLUSTERED INDEX UX_D_PAYMENT_TERM_BK_CURR
            ON svo.D_PAYMENT_TERM (PAYMENT_TERM_ID)
            WHERE CURR_IND = 'Y'
            ON FG_SilverDim;
        END;

        IF NOT EXISTS (SELECT 1 FROM svo.D_PAYMENT_TERM WHERE PAYMENT_TERM_SK = 0)
        BEGIN
            SET IDENTITY_INSERT svo.D_PAYMENT_TERM ON;

            INSERT INTO svo.D_PAYMENT_TERM
            (PAYMENT_TERM_SK, PAYMENT_TERM_ID, PAYMENT_TERM_NAME, BZ_LOAD_DATE, SV_LOAD_DATE, EFF_DATE, END_DATE, CRE_DATE, UDT_DATE, CURR_IND)
            VALUES
            (0, '-1', 'Unknown', NULL, CAST(GETDATE() AS DATE), @AsOfDate, @HighDate, @LoadDttm, @LoadDttm, 'Y');

            SET IDENTITY_INSERT svo.D_PAYMENT_TERM OFF;
        END;

        IF OBJECT_ID('tempdb..#src') IS NOT NULL DROP TABLE #src;

        SELECT
            s.PAYMENT_TERM_ID, s.PAYMENT_TERM_NAME, s.BZ_LOAD_DATE, s.SV_LOAD_DATE, s.SourceAddDateTime
        INTO #src
        FROM
        (
            SELECT
                src.PaymentTermHeaderTranslationTermId AS PAYMENT_TERM_ID,
                src.PaymentTermHeaderTranslationName AS PAYMENT_TERM_NAME,
                CAST(src.AddDateTime AS DATE) AS BZ_LOAD_DATE,
                CAST(GETDATE() AS DATE) AS SV_LOAD_DATE,
                src.AddDateTime AS SourceAddDateTime,
                ROW_NUMBER() OVER (PARTITION BY src.PaymentTermHeaderTranslationTermId ORDER BY src.AddDateTime DESC) AS rn
            FROM bzo.AP_PaymentTermHeaderTranslationExtractPVO src
            WHERE src.AddDateTime > @LastWatermark
        ) s
        WHERE s.rn = 1;

        SELECT @MaxWatermark = MAX(SourceAddDateTime) FROM #src;

        UPDATE tgt
        SET tgt.END_DATE = DATEADD(DAY, -1, @AsOfDate), tgt.CURR_IND = 'N', tgt.UDT_DATE = @LoadDttm
        FROM svo.D_PAYMENT_TERM tgt
        INNER JOIN #src src ON src.PAYMENT_TERM_ID = tgt.PAYMENT_TERM_ID
        WHERE tgt.CURR_IND = 'Y'
          AND ISNULL(tgt.PAYMENT_TERM_NAME,'') <> ISNULL(src.PAYMENT_TERM_NAME,'');

        SET @RowExpired = @@ROWCOUNT;

        INSERT INTO svo.D_PAYMENT_TERM
        (PAYMENT_TERM_ID, PAYMENT_TERM_NAME, BZ_LOAD_DATE, SV_LOAD_DATE, EFF_DATE, END_DATE, CRE_DATE, UDT_DATE, CURR_IND)
        SELECT
            src.PAYMENT_TERM_ID, src.PAYMENT_TERM_NAME, src.BZ_LOAD_DATE, src.SV_LOAD_DATE,
            @AsOfDate, @HighDate, @LoadDttm, @LoadDttm, 'Y'
        FROM #src src
        LEFT JOIN svo.D_PAYMENT_TERM tgt ON tgt.PAYMENT_TERM_ID = src.PAYMENT_TERM_ID AND tgt.CURR_IND = 'Y'
        WHERE tgt.PAYMENT_TERM_ID IS NULL;

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
