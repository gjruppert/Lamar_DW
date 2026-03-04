CREATE OR ALTER PROCEDURE svo.usp_Load_D_CUSTOMER_ACCOUNT_SITE_T1
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    DECLARE
        @ProcName      SYSNAME       = OBJECT_SCHEMA_NAME(@@PROCID) + '.' + OBJECT_NAME(@@PROCID),
        @TargetObject  SYSNAME       = 'svo.D_CUSTOMER_ACCOUNT_SITE',
        @AsOfDate      DATE          = CAST(GETDATE() AS DATE),
        @StartDttm     DATETIME2(0)  = SYSDATETIME(),
        @EndDttm       DATETIME2(0),
        @RunId         BIGINT        = NULL,
        @ErrMsg        NVARCHAR(4000) = NULL,

        @LastWatermark DATETIME2(7)  = NULL,
        @MaxWatermark  DATETIME2(7)  = NULL,

        @RowInserted   INT           = 0,
        @RowUpdated    INT           = 0,
        @RowExpired    INT           = 0;

    BEGIN TRY
        /* ===== Watermark ===== */
        SELECT @LastWatermark = w.LAST_WATERMARK
        FROM etl.ETL_WATERMARK w
        WHERE w.TABLE_NAME = @TargetObject;

        IF @LastWatermark IS NULL
            SET @LastWatermark = '1900-01-01';

        /* ===== ETL_RUN start ===== */
        INSERT INTO etl.ETL_RUN (PROC_NAME, TARGET_OBJECT, ASOF_DATE, START_DTTM, STATUS)
        VALUES (@ProcName, @TargetObject, @AsOfDate, @StartDttm, 'STARTED');

        SET @RunId = SCOPE_IDENTITY();

        /* ===== Ensure unique index on BK ===== */
        IF NOT EXISTS
        (
            SELECT 1
            FROM sys.indexes
            WHERE name = 'UX_D_CUSTOMER_ACCOUNT_SITE'
              AND object_id = OBJECT_ID('svo.D_CUSTOMER_ACCOUNT_SITE')
        )
        BEGIN
            CREATE UNIQUE NONCLUSTERED INDEX UX_D_CUSTOMER_ACCOUNT_SITE
                ON svo.D_CUSTOMER_ACCOUNT_SITE (CUSTOMER_SITE)
                ON FG_SilverDim;
        END;

        /* ===== Ensure plug row (SK=0) exists ===== */
        IF NOT EXISTS (SELECT 1 FROM svo.D_CUSTOMER_ACCOUNT_SITE WHERE CUSTOMER_SITE_SK = 0)
        BEGIN
            SET IDENTITY_INSERT svo.D_CUSTOMER_ACCOUNT_SITE ON;

            INSERT INTO svo.D_CUSTOMER_ACCOUNT_SITE
            (
                CUSTOMER_SITE_SK,
                CUSTOMER_SITE,
                CUSTOMER_ACCOUNT,
                PARTY_SITE,
                STATUS,
                LANGUAGE,
                START_DATE_ACTIVE,
                END_DATE,
                BZ_LOAD_DATE,
                SV_LOAD_DATE
            )
            VALUES
            (
                0,
                -1,
                -1,
                -1,
                'U',
                NULL,
                CAST('0001-01-01' AS DATE),
                CAST('9999-12-31' AS DATE),
                CAST('0001-01-01' AS DATE),
                CAST(GETDATE() AS DATE)
            );

            SET IDENTITY_INSERT svo.D_CUSTOMER_ACCOUNT_SITE OFF;
        END;

        /* ===== Source (incremental + dedup by CUSTOMER_SITE) ===== */
        IF OBJECT_ID('tempdb..#src') IS NOT NULL DROP TABLE #src;

        SELECT
            s.CUSTOMER_SITE,
            s.CUSTOMER_ACCOUNT,
            s.PARTY_SITE,
            s.STATUS,
            s.LANGUAGE,
            s.START_DATE_ACTIVE,
            s.END_DATE,
            s.BZ_LOAD_DATE,
            s.SV_LOAD_DATE,
            s.AddDateTime
        INTO #src
        FROM
        (
            SELECT
                CAST(CustAcctSiteId AS BIGINT) AS CUSTOMER_SITE,
                CAST(CustAccountId  AS BIGINT) AS CUSTOMER_ACCOUNT,
                CAST(PartySiteId    AS BIGINT) AS PARTY_SITE,
                CAST(Status         AS VARCHAR(1)) AS STATUS,
                CAST(Language       AS VARCHAR(4)) AS LANGUAGE,
                CAST(StartDate      AS DATE) AS START_DATE_ACTIVE,
                CAST(EndDate        AS DATE) AS END_DATE,

                /* BZ_LOAD_DATE rule (never NULL) */
                COALESCE(CAST(AddDateTime AS DATE), CAST(GETDATE() AS DATE)) AS BZ_LOAD_DATE,
                CAST(GETDATE() AS DATE) AS SV_LOAD_DATE,

                AddDateTime,

                ROW_NUMBER() OVER
                (
                    PARTITION BY CAST(CustAcctSiteId AS BIGINT)
                    ORDER BY AddDateTime DESC
                ) AS rn
            FROM bzo.AR_CustomerAccountSiteExtractPVO
            WHERE AddDateTime > @LastWatermark
        ) s
        WHERE s.rn = 1;

        SELECT @MaxWatermark = MAX(AddDateTime) FROM #src;

        /* ===== MERGE (Type 1 overwrite) ===== */
        DECLARE @merge_audit TABLE (action NVARCHAR(10) NOT NULL);

        MERGE svo.D_CUSTOMER_ACCOUNT_SITE AS tgt
        USING
        (
            SELECT
                CUSTOMER_SITE,
                CUSTOMER_ACCOUNT,
                PARTY_SITE,
                STATUS,
                LANGUAGE,
                START_DATE_ACTIVE,
                END_DATE,
                BZ_LOAD_DATE,
                SV_LOAD_DATE
            FROM #src
        ) AS src
            ON tgt.CUSTOMER_SITE = src.CUSTOMER_SITE
        WHEN NOT MATCHED BY TARGET THEN
            INSERT
            (
                CUSTOMER_SITE,
                CUSTOMER_ACCOUNT,
                PARTY_SITE,
                STATUS,
                LANGUAGE,
                START_DATE_ACTIVE,
                END_DATE,
                BZ_LOAD_DATE,
                SV_LOAD_DATE
            )
            VALUES
            (
                src.CUSTOMER_SITE,
                src.CUSTOMER_ACCOUNT,
                src.PARTY_SITE,
                src.STATUS,
                src.LANGUAGE,
                src.START_DATE_ACTIVE,
                src.END_DATE,
                src.BZ_LOAD_DATE,
                src.SV_LOAD_DATE
            )
        WHEN MATCHED THEN
            UPDATE SET
                tgt.CUSTOMER_ACCOUNT = src.CUSTOMER_ACCOUNT,
                tgt.PARTY_SITE       = src.PARTY_SITE,
                tgt.STATUS           = src.STATUS,
                tgt.LANGUAGE         = src.LANGUAGE,
                tgt.START_DATE_ACTIVE       = src.START_DATE_ACTIVE,
                tgt.END_DATE         = src.END_DATE,
                tgt.BZ_LOAD_DATE     = src.BZ_LOAD_DATE,
                tgt.SV_LOAD_DATE     = src.SV_LOAD_DATE
        OUTPUT $action INTO @merge_audit(action);

        SELECT
            @RowInserted = SUM(CASE WHEN action = 'INSERT' THEN 1 ELSE 0 END),
            @RowUpdated  = SUM(CASE WHEN action = 'UPDATE' THEN 1 ELSE 0 END)
        FROM @merge_audit;

        IF @RowInserted IS NULL SET @RowInserted=0;
        IF @RowUpdated IS NULL SET @RowUpdated=0;
        /* ===== Advance watermark ===== */
        IF @MaxWatermark IS NOT NULL
        BEGIN
            UPDATE etl.ETL_WATERMARK
            SET
                LAST_WATERMARK = @MaxWatermark,
                UDT_DATE       = SYSDATETIME()
            WHERE TABLE_NAME = @TargetObject;
        END;

        /* ===== ETL_RUN end ===== */
        SET @EndDttm = SYSDATETIME();

        UPDATE etl.ETL_RUN
        SET
            END_DTTM      = @EndDttm,
            STATUS        = 'SUCCESS',
            ROW_INSERTED  = @RowInserted,
            ROW_EXPIRED   = @RowExpired,
            ROW_UPDATED   = @RowUpdated,
            ERROR_MESSAGE = NULL
        WHERE RUN_ID = @RunId;
    END TRY
    BEGIN CATCH
        SET @EndDttm = SYSDATETIME();
        SET @ErrMsg  = CONCAT('Error ', ERROR_NUMBER(), ' (Line ', ERROR_LINE(), '): ', ERROR_MESSAGE());

        IF @RunId IS NOT NULL
        BEGIN
            UPDATE etl.ETL_RUN
            SET
                END_DTTM      = @EndDttm,
                STATUS        = 'FAILURE',
                ROW_INSERTED  = @RowInserted,
                ROW_EXPIRED   = @RowExpired,
                ROW_UPDATED   = @RowUpdated,
                ERROR_MESSAGE = @ErrMsg
            WHERE RUN_ID = @RunId;
        END;

        ;THROW;
    END CATCH
END;
GO