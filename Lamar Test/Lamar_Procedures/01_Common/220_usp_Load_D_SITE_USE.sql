/* =========================================================
   usp_Load_D_SITE_USE
   SCD2 incremental load. Source: bzo.AR_CustomerAcctSiteUseExtractPVO
   Watermark filter, expire changed rows, insert new current rows.
   Idempotent: safe to retry same watermark range.
   ========================================================= */
CREATE OR ALTER PROCEDURE svo.usp_Load_D_SITE_USE
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    DECLARE
        @ProcName       SYSNAME        = OBJECT_SCHEMA_NAME(@@PROCID) + '.' + OBJECT_NAME(@@PROCID),
        @TargetObject   SYSNAME        = 'svo.D_SITE_USE',
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

        IF EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'UX_D_SITE_USE_ID' AND object_id = OBJECT_ID('svo.D_SITE_USE'))
        BEGIN
            DROP INDEX UX_D_SITE_USE_ID ON svo.D_SITE_USE;
        END;

        IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'UX_D_SITE_USE_BK_CURR' AND object_id = OBJECT_ID('svo.D_SITE_USE'))
        BEGIN
            CREATE UNIQUE NONCLUSTERED INDEX UX_D_SITE_USE_BK_CURR
            ON svo.D_SITE_USE (SITE_USE)
            WHERE CURR_IND = 'Y'
            ON FG_SilverDim;
        END;

        IF NOT EXISTS (SELECT 1 FROM svo.D_SITE_USE WHERE SITE_USE_SK = 0)
        BEGIN
            SET IDENTITY_INSERT svo.D_SITE_USE ON;

            INSERT INTO svo.D_SITE_USE
            (SITE_USE_SK, SITE_USE, CUSTOMER_SITE, SITE_USE_CODE, LOCATION, PRIMARY_FLAG, PAYMENT_TERM_ID, STATUS, BZ_LOAD_DATE, SV_LOAD_DATE, EFF_DATE, END_DATE, CRE_DATE, UDT_DATE, CURR_IND)
            VALUES
            (0, -1, -1, 'UNK', NULL, NULL, NULL, 'U', CAST(GETDATE() AS DATE), CAST(GETDATE() AS DATE), @AsOfDate, @HighDate, @LoadDttm, @LoadDttm, 'Y');

            SET IDENTITY_INSERT svo.D_SITE_USE OFF;
        END;

        IF OBJECT_ID('tempdb..#src') IS NOT NULL DROP TABLE #src;

        SELECT
            s.SITE_USE, s.CUSTOMER_SITE, s.SITE_USE_CODE, s.LOCATION, s.PRIMARY_FLAG, s.PAYMENT_TERM_ID, s.STATUS,
            s.BZ_LOAD_DATE, s.SV_LOAD_DATE, s.SourceAddDateTime
        INTO #src
        FROM
        (
            SELECT
                S.SiteUseId AS SITE_USE,
                S.CustAcctSiteId AS CUSTOMER_SITE,
                S.SiteUseCode AS SITE_USE_CODE,
                S.Location AS LOCATION,
                S.PrimaryFlag AS PRIMARY_FLAG,
                S.PaymentTermId AS PAYMENT_TERM_ID,
                S.Status AS STATUS,
                CAST(S.AddDateTime AS DATE) AS BZ_LOAD_DATE,
                CAST(GETDATE() AS DATE) AS SV_LOAD_DATE,
                S.AddDateTime AS SourceAddDateTime,
                ROW_NUMBER() OVER (PARTITION BY S.SiteUseId ORDER BY S.AddDateTime DESC) AS rn
            FROM bzo.AR_CustomerAcctSiteUseExtractPVO S
            WHERE S.AddDateTime > @LastWatermark
        ) s
        WHERE s.rn = 1;

        SELECT @MaxWatermark = MAX(SourceAddDateTime) FROM #src;

        UPDATE tgt
        SET tgt.END_DATE = DATEADD(DAY, -1, @AsOfDate), tgt.CURR_IND = 'N', tgt.UDT_DATE = @LoadDttm
        FROM svo.D_SITE_USE tgt
        INNER JOIN #src src ON src.SITE_USE = tgt.SITE_USE
        WHERE tgt.CURR_IND = 'Y'
          AND (
                ISNULL(tgt.CUSTOMER_SITE, -999) <> ISNULL(src.CUSTOMER_SITE, -999)
             OR ISNULL(tgt.SITE_USE_CODE,'') <> ISNULL(src.SITE_USE_CODE,'')
             OR ISNULL(tgt.LOCATION,'') <> ISNULL(src.LOCATION,'')
             OR ISNULL(tgt.PRIMARY_FLAG,'') <> ISNULL(src.PRIMARY_FLAG,'')
             OR ISNULL(tgt.PAYMENT_TERM_ID, -999) <> ISNULL(src.PAYMENT_TERM_ID, -999)
             OR ISNULL(tgt.STATUS,'') <> ISNULL(src.STATUS,'')
          );

        SET @RowExpired = @@ROWCOUNT;

        INSERT INTO svo.D_SITE_USE
        (SITE_USE, CUSTOMER_SITE, SITE_USE_CODE, LOCATION, PRIMARY_FLAG, PAYMENT_TERM_ID, STATUS, BZ_LOAD_DATE, SV_LOAD_DATE, EFF_DATE, END_DATE, CRE_DATE, UDT_DATE, CURR_IND)
        SELECT
            src.SITE_USE, src.CUSTOMER_SITE, src.SITE_USE_CODE, src.LOCATION, src.PRIMARY_FLAG, src.PAYMENT_TERM_ID, src.STATUS,
            src.BZ_LOAD_DATE, src.SV_LOAD_DATE, @AsOfDate, @HighDate, @LoadDttm, @LoadDttm, 'Y'
        FROM #src src
        LEFT JOIN svo.D_SITE_USE tgt ON tgt.SITE_USE = src.SITE_USE AND tgt.CURR_IND = 'Y'
        WHERE tgt.SITE_USE IS NULL;

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
