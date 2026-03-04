CREATE OR ALTER PROCEDURE svo.usp_Load_D_CUSTOMER_ACCOUNT_SCD2
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    DECLARE
        @ProcName       SYSNAME        = OBJECT_SCHEMA_NAME(@@PROCID) + '.' + OBJECT_NAME(@@PROCID),
        @TargetObject   SYSNAME        = 'svo.D_CUSTOMER_ACCOUNT',
        @StartDttm      DATETIME2(0)   = SYSDATETIME(),
        @EndDttm        DATETIME2(0),
        @RunId          BIGINT         = NULL,
        @ErrMsg         NVARCHAR(4000) = NULL,

        @AsOfDate       DATE           = CAST(GETDATE() AS DATE),
        @LoadDttm       DATETIME2(0)   = SYSDATETIME(),
        @HighDate       DATE           = '9999-12-31',

        @LastWatermark  DATETIME2(7)   = NULL,
        @MaxWatermark   DATETIME2(7)   = NULL,

        @RowInserted    INT            = 0,
        @RowExpired     INT            = 0,
        @RowUpdated     INT            = 0;  -- not used for SCD2

    BEGIN TRY
        /* ===== Watermark ===== */
        SELECT
            @LastWatermark = w.LAST_WATERMARK
        FROM etl.ETL_WATERMARK w
        WHERE w.TABLE_NAME = @TargetObject;

        IF @LastWatermark IS NULL
            SET @LastWatermark = '1900-01-01';

        /* ===== ETL_RUN start ===== */
        INSERT INTO etl.ETL_RUN (PROC_NAME, TARGET_OBJECT, ASOF_DATE, START_DTTM, STATUS)
        VALUES (@ProcName, @TargetObject, @AsOfDate, @StartDttm, 'STARTED');

        SET @RunId = SCOPE_IDENTITY();

        /* ===== Unique index on BK ===== */
        IF NOT EXISTS
        (
            SELECT 1
            FROM sys.indexes
            WHERE name = 'UX_D_CUSTOMER_ACCOUNT_ID'
              AND object_id = OBJECT_ID('svo.D_CUSTOMER_ACCOUNT')
        )
        BEGIN
            CREATE UNIQUE NONCLUSTERED INDEX UX_D_CUSTOMER_ACCOUNT_ID
            ON svo.D_CUSTOMER_ACCOUNT (CUSTOMER_ACCOUNT_ID)
            ON FG_SilverDim;
        END;

        /* ===== Ensure plug row (SK=0) exists ===== */
        IF NOT EXISTS (SELECT 1 FROM svo.D_CUSTOMER_ACCOUNT WHERE CUSTOMER_SK = 0)
        BEGIN
            SET IDENTITY_INSERT svo.D_CUSTOMER_ACCOUNT ON;

            INSERT INTO svo.D_CUSTOMER_ACCOUNT
            (
                CUSTOMER_SK,
                CUSTOMER_ACCOUNT_ID,
                ACCOUNT_NUMBER,
                ACCOUNT_NAME,
                STATUS_CODE,
                CUSTOMER_TYPE,
                PARTY_ID,
                BZ_LOAD_DATE,
                SV_LOAD_DATE,
                EFF_DATE,
                END_DATE,
                CRE_DATE,
                UDT_DATE,
                CURR_IND
            )
            VALUES
            (
                0,
                -1,
                'UNKNOWN',
                'UNKNOWN CUSTOMER',
                'UNKNOWN',
                'UNKNOWN',
                NULL,
                CAST('0001-01-01' AS DATE),
                CAST(GETDATE() AS DATE),
                @AsOfDate,
                @HighDate,
                @LoadDttm,
                @LoadDttm,
                'Y'
            );

            SET IDENTITY_INSERT svo.D_CUSTOMER_ACCOUNT OFF;
        END;

        /* ===== Source (incremental + dedup by CUSTOMER_ACCOUNT_ID) ===== */
        IF OBJECT_ID('tempdb..#src') IS NOT NULL
            DROP TABLE #src;

        SELECT
            s.CUSTOMER_ACCOUNT_ID,
            s.ACCOUNT_NUMBER,
            s.ACCOUNT_NAME,
            s.STATUS_CODE,
            s.CUSTOMER_TYPE,
            s.PARTY_ID,
            s.BZ_LOAD_DATE,
            s.SV_LOAD_DATE,
            s.SourceAddDateTime
        INTO #src
        FROM
        (
            SELECT
                A.CustAccountId                               AS CUSTOMER_ACCOUNT_ID,
                A.AccountNumber                               AS ACCOUNT_NUMBER,
                A.AccountName                                 AS ACCOUNT_NAME,
                A.Status                                      AS STATUS_CODE,
                A.CustomerType                                AS CUSTOMER_TYPE,
                CAST(A.PartyId AS VARCHAR(60))                AS PARTY_ID,

                /* BZ_LOAD_DATE rule (never NULL) */
                COALESCE(CAST(A.AddDateTime AS DATE), CAST(GETDATE() AS DATE)) AS BZ_LOAD_DATE,
                CAST(GETDATE() AS DATE)                       AS SV_LOAD_DATE,

                A.AddDateTime                                 AS SourceAddDateTime,

                ROW_NUMBER() OVER
                (
                    PARTITION BY A.CustAccountId
                    ORDER BY A.AddDateTime DESC
                ) AS rn
            FROM bzo.AR_CustomerAccountExtractPVO A
            WHERE
                A.CustAccountId IS NOT NULL
                AND A.AddDateTime > @LastWatermark
        ) s
        WHERE s.rn = 1;

        /* Never treat the plug BK as a normal data row */
        DELETE FROM #src WHERE CUSTOMER_ACCOUNT_ID = -1;

        SELECT @MaxWatermark = MAX(SourceAddDateTime) FROM #src;

        /* ===== SCD2 expire changed current rows ===== */
        UPDATE tgt
        SET
            tgt.END_DATE = DATEADD(DAY, -1, @AsOfDate),
            tgt.CURR_IND = 'N',
            tgt.UDT_DATE = @LoadDttm
        FROM svo.D_CUSTOMER_ACCOUNT tgt
        INNER JOIN #src src
            ON src.CUSTOMER_ACCOUNT_ID = tgt.CUSTOMER_ACCOUNT_ID
        WHERE tgt.CURR_IND = 'Y'
          AND
          (
                ISNULL(tgt.ACCOUNT_NUMBER,'') <> ISNULL(src.ACCOUNT_NUMBER,'')
             OR ISNULL(tgt.ACCOUNT_NAME,'')   <> ISNULL(src.ACCOUNT_NAME,'')
             OR ISNULL(tgt.STATUS_CODE,'')    <> ISNULL(src.STATUS_CODE,'')
             OR ISNULL(tgt.CUSTOMER_TYPE,'')  <> ISNULL(src.CUSTOMER_TYPE,'')
             OR ISNULL(tgt.PARTY_ID,'')       <> ISNULL(src.PARTY_ID,'')
          );

        SET @RowExpired = @@ROWCOUNT;

        /* ===== Insert new current rows ===== */
        INSERT INTO svo.D_CUSTOMER_ACCOUNT
        (
            CUSTOMER_ACCOUNT_ID,
            ACCOUNT_NUMBER,
            ACCOUNT_NAME,
            STATUS_CODE,
            CUSTOMER_TYPE,
            PARTY_ID,
            BZ_LOAD_DATE,
            SV_LOAD_DATE,
            EFF_DATE,
            END_DATE,
            CRE_DATE,
            UDT_DATE,
            CURR_IND
        )
        SELECT
            src.CUSTOMER_ACCOUNT_ID,
            src.ACCOUNT_NUMBER,
            src.ACCOUNT_NAME,
            src.STATUS_CODE,
            src.CUSTOMER_TYPE,
            src.PARTY_ID,
            src.BZ_LOAD_DATE,
            src.SV_LOAD_DATE,
            @AsOfDate,
            @HighDate,
            @LoadDttm,
            @LoadDttm,
            'Y'
        FROM #src src
        LEFT JOIN svo.D_CUSTOMER_ACCOUNT tgt
            ON tgt.CUSTOMER_ACCOUNT_ID = src.CUSTOMER_ACCOUNT_ID
           AND tgt.CURR_IND = 'Y'
        WHERE tgt.CUSTOMER_ACCOUNT_ID IS NULL;

        SET @RowInserted = @@ROWCOUNT;

        /* ===== Advance watermark ===== */
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