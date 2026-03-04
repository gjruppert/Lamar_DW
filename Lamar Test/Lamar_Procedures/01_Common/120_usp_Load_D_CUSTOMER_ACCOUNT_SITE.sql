/* =========================================================
   usp_Load_D_CUSTOMER_ACCOUNT_SITE
   SCD2 incremental load. Source: bzo.AR_CustomerAccountSiteExtractPVO
   Watermark filter, expire changed rows, insert new current rows.
   Idempotent: safe to retry same watermark range.
   ========================================================= */
CREATE OR ALTER PROCEDURE svo.usp_Load_D_CUSTOMER_ACCOUNT_SITE
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    DECLARE
        @ProcName       SYSNAME        = OBJECT_SCHEMA_NAME(@@PROCID) + '.' + OBJECT_NAME(@@PROCID),
        @TargetObject   SYSNAME        = 'svo.D_CUSTOMER_ACCOUNT_SITE',
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

        IF EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'UX_D_CUSTOMER_ACCOUNT_SITE_ID' AND object_id = OBJECT_ID('svo.D_CUSTOMER_ACCOUNT_SITE'))
        BEGIN
            DROP INDEX UX_D_CUSTOMER_ACCOUNT_SITE_ID ON svo.D_CUSTOMER_ACCOUNT_SITE;
        END;

        IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'UX_D_CUSTOMER_ACCOUNT_SITE_BK_CURR' AND object_id = OBJECT_ID('svo.D_CUSTOMER_ACCOUNT_SITE'))
        BEGIN
            CREATE UNIQUE NONCLUSTERED INDEX UX_D_CUSTOMER_ACCOUNT_SITE_BK_CURR
            ON svo.D_CUSTOMER_ACCOUNT_SITE (CUSTOMER_SITE)
            WHERE CURR_IND = 'Y'
            ON FG_SilverDim;
        END;

        IF NOT EXISTS (SELECT 1 FROM svo.D_CUSTOMER_ACCOUNT_SITE WHERE CUSTOMER_SITE_SK = 0)
        BEGIN
            SET IDENTITY_INSERT svo.D_CUSTOMER_ACCOUNT_SITE ON;

            INSERT INTO svo.D_CUSTOMER_ACCOUNT_SITE
            (CUSTOMER_SITE_SK, CUSTOMER_SITE, CUSTOMER_ACCOUNT, PARTY_SITE, STATUS, BZ_LOAD_DATE, SV_LOAD_DATE, EFF_DATE, END_DATE, CRE_DATE, UDT_DATE, CURR_IND)
            VALUES
            (0, -1, -1, -1, 'U', CAST(GETDATE() AS DATE), CAST(GETDATE() AS DATE), @AsOfDate, @HighDate, @LoadDttm, @LoadDttm, 'Y');

            SET IDENTITY_INSERT svo.D_CUSTOMER_ACCOUNT_SITE OFF;
        END;

        IF OBJECT_ID('tempdb..#src') IS NOT NULL DROP TABLE #src;

        SELECT
            s.CUSTOMER_SITE, s.CUSTOMER_ACCOUNT, s.PARTY_SITE, s.STATUS, s.LANGUAGE, s.START_DATE_ACTIVE, s.END_DATE_ACTIVE,
            s.BZ_LOAD_DATE, s.SV_LOAD_DATE, s.SourceAddDateTime
        INTO #src
        FROM
        (
            SELECT
                S.CustAcctSiteId AS CUSTOMER_SITE,
                S.CustAccountId AS CUSTOMER_ACCOUNT,
                S.PartySiteId AS PARTY_SITE,
                S.Status AS STATUS,
                S.Language AS LANGUAGE,
                CAST(S.StartDate AS DATE) AS START_DATE_ACTIVE,
                CAST(S.EndDate AS DATE) AS END_DATE_ACTIVE,
                CAST(S.AddDateTime AS DATE) AS BZ_LOAD_DATE,
                CAST(GETDATE() AS DATE) AS SV_LOAD_DATE,
                S.AddDateTime AS SourceAddDateTime,
                ROW_NUMBER() OVER (PARTITION BY S.CustAcctSiteId ORDER BY S.AddDateTime DESC) AS rn
            FROM bzo.AR_CustomerAccountSiteExtractPVO S
            WHERE S.AddDateTime > @LastWatermark
        ) s
        WHERE s.rn = 1;

        SELECT @MaxWatermark = MAX(SourceAddDateTime) FROM #src;

        UPDATE tgt
        SET tgt.END_DATE = DATEADD(DAY, -1, @AsOfDate), tgt.CURR_IND = 'N', tgt.UDT_DATE = @LoadDttm
        FROM svo.D_CUSTOMER_ACCOUNT_SITE tgt
        INNER JOIN #src src ON src.CUSTOMER_SITE = tgt.CUSTOMER_SITE
        WHERE tgt.CURR_IND = 'Y'
          AND (
                ISNULL(tgt.CUSTOMER_ACCOUNT, -999) <> ISNULL(src.CUSTOMER_ACCOUNT, -999)
             OR ISNULL(tgt.PARTY_SITE, -999) <> ISNULL(src.PARTY_SITE, -999)
             OR ISNULL(tgt.STATUS,'') <> ISNULL(src.STATUS,'')
             OR ISNULL(tgt.LANGUAGE,'') <> ISNULL(src.LANGUAGE,'')
             OR ISNULL(tgt.START_DATE_ACTIVE,'1900-01-01') <> ISNULL(src.START_DATE_ACTIVE,'1900-01-01')
             OR ISNULL(tgt.END_DATE_ACTIVE,'1900-01-01') <> ISNULL(src.END_DATE_ACTIVE,'1900-01-01')
          );

        SET @RowExpired = @@ROWCOUNT;

        INSERT INTO svo.D_CUSTOMER_ACCOUNT_SITE
        (CUSTOMER_SITE, CUSTOMER_ACCOUNT, PARTY_SITE, STATUS, LANGUAGE, START_DATE_ACTIVE, END_DATE_ACTIVE, BZ_LOAD_DATE, SV_LOAD_DATE, EFF_DATE, END_DATE, CRE_DATE, UDT_DATE, CURR_IND)
        SELECT
            src.CUSTOMER_SITE, src.CUSTOMER_ACCOUNT, src.PARTY_SITE, src.STATUS, src.LANGUAGE, src.START_DATE_ACTIVE, src.END_DATE_ACTIVE,
            src.BZ_LOAD_DATE, src.SV_LOAD_DATE, @AsOfDate, @HighDate, @LoadDttm, @LoadDttm, 'Y'
        FROM #src src
        LEFT JOIN svo.D_CUSTOMER_ACCOUNT_SITE tgt ON tgt.CUSTOMER_SITE = src.CUSTOMER_SITE AND tgt.CURR_IND = 'Y'
        WHERE tgt.CUSTOMER_SITE IS NULL;

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
