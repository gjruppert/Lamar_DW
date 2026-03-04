/* =========================================================
   usp_Load_D_CUSTOMER_ACCOUNT
   SCD2 incremental load. Source: bzo.AR_CustomerAccountExtractPVO
   Watermark filter, expire changed rows, insert new current rows.
   Idempotent: safe to retry same watermark range.
   ========================================================= */
CREATE OR ALTER PROCEDURE svo.usp_Load_D_CUSTOMER_ACCOUNT
    @BatchId INT = NULL
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
        @ErrMsg         NVARCHAR(4000)  = NULL,

        @AsOfDate       DATE           = CAST(GETDATE() AS DATE),
        @LoadDttm       DATETIME2(0)   = SYSDATETIME(),
        @HighDate       DATE           = '9999-12-31',

        @LastWatermark  DATETIME2(7),
        @MaxWatermark   DATETIME2(7)   = NULL,

        @RowInserted    INT            = 0,
        @RowExpired     INT            = 0,
        @RowUpdated     INT            = 0,
        @TableBridgeID  INT            = NULL;

    SELECT @TableBridgeID = TableBridgeID FROM meta.MedallionTableBridge WHERE targettable = N'AR_CustomerAccountExtractPVO';

    BEGIN TRY
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

        IF EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'UX_D_CUSTOMER_ACCOUNT_ID' AND object_id = OBJECT_ID('svo.D_CUSTOMER_ACCOUNT'))
        BEGIN
            DROP INDEX UX_D_CUSTOMER_ACCOUNT_ID ON svo.D_CUSTOMER_ACCOUNT;
        END;

        IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'UX_D_CUSTOMER_ACCOUNT_BK_CURR' AND object_id = OBJECT_ID('svo.D_CUSTOMER_ACCOUNT'))
        BEGIN
            CREATE UNIQUE NONCLUSTERED INDEX UX_D_CUSTOMER_ACCOUNT_BK_CURR
            ON svo.D_CUSTOMER_ACCOUNT (CUSTOMER_ACCOUNT_ID)
            WHERE CURR_IND = 'Y'
            ON FG_SilverDim;
        END;

        IF NOT EXISTS (SELECT 1 FROM svo.D_CUSTOMER_ACCOUNT WHERE CUSTOMER_SK = 0)
        BEGIN
            SET IDENTITY_INSERT svo.D_CUSTOMER_ACCOUNT ON;

            INSERT INTO svo.D_CUSTOMER_ACCOUNT
            (CUSTOMER_SK, CUSTOMER_ACCOUNT_ID, ACCOUNT_NUMBER, ACCOUNT_NAME, STATUS_CODE, CUSTOMER_TYPE, PARTY_ID, BZ_LOAD_DATE, SV_LOAD_DATE, EFF_DATE, END_DATE, CRE_DATE, UDT_DATE, CURR_IND)
            VALUES
            (0, -1, 'UNKNOWN', 'UNKNOWN CUSTOMER', 'UNKNOWN', 'UNKNOWN', NULL, '0001-01-01', '0001-01-01', @AsOfDate, @HighDate, @LoadDttm, @LoadDttm, 'Y');

            SET IDENTITY_INSERT svo.D_CUSTOMER_ACCOUNT OFF;
        END;

        IF OBJECT_ID('tempdb..#src') IS NOT NULL DROP TABLE #src;

        SELECT
            s.CUSTOMER_ACCOUNT_ID, s.ACCOUNT_NUMBER, s.ACCOUNT_NAME, s.STATUS_CODE, s.CUSTOMER_TYPE, s.PARTY_ID,
            s.BZ_LOAD_DATE, s.SV_LOAD_DATE, s.SourceAddDateTime
        INTO #src
        FROM
        (
            SELECT
                A.CustAccountId AS CUSTOMER_ACCOUNT_ID,
                A.AccountNumber AS ACCOUNT_NUMBER,
                A.AccountName AS ACCOUNT_NAME,
                A.Status AS STATUS_CODE,
                A.CustomerType AS CUSTOMER_TYPE,
                A.PartyId AS PARTY_ID,
                CAST(A.AddDateTime AS DATETIME) AS BZ_LOAD_DATE,
                GETDATE() AS SV_LOAD_DATE,
                A.AddDateTime AS SourceAddDateTime,
                ROW_NUMBER() OVER (PARTITION BY A.CustAccountId ORDER BY A.AddDateTime DESC) AS rn
            FROM bzo.AR_CustomerAccountExtractPVO A
            WHERE A.CustAccountId IS NOT NULL
              AND A.AddDateTime > @LastWatermark
        ) s
        WHERE s.rn = 1;

        SELECT @MaxWatermark = MAX(SourceAddDateTime) FROM #src;

        UPDATE tgt
        SET tgt.END_DATE = DATEADD(DAY, -1, @AsOfDate), tgt.CURR_IND = 'N', tgt.UDT_DATE = @LoadDttm
        FROM svo.D_CUSTOMER_ACCOUNT tgt
        INNER JOIN #src src ON src.CUSTOMER_ACCOUNT_ID = tgt.CUSTOMER_ACCOUNT_ID
        WHERE tgt.CURR_IND = 'Y'
          AND (
                ISNULL(tgt.ACCOUNT_NUMBER,'') <> ISNULL(src.ACCOUNT_NUMBER,'')
             OR ISNULL(tgt.ACCOUNT_NAME,'') <> ISNULL(src.ACCOUNT_NAME,'')
             OR ISNULL(tgt.STATUS_CODE,'') <> ISNULL(src.STATUS_CODE,'')
             OR ISNULL(tgt.CUSTOMER_TYPE,'') <> ISNULL(src.CUSTOMER_TYPE,'')
             OR ISNULL(CAST(tgt.PARTY_ID AS VARCHAR(60)),'') <> ISNULL(CAST(src.PARTY_ID AS VARCHAR(60)),'')
          );

        SET @RowExpired = @@ROWCOUNT;

        INSERT INTO svo.D_CUSTOMER_ACCOUNT
        (CUSTOMER_ACCOUNT_ID, ACCOUNT_NUMBER, ACCOUNT_NAME, STATUS_CODE, CUSTOMER_TYPE, PARTY_ID, BZ_LOAD_DATE, SV_LOAD_DATE, EFF_DATE, END_DATE, CRE_DATE, UDT_DATE, CURR_IND)
        SELECT
            src.CUSTOMER_ACCOUNT_ID, src.ACCOUNT_NUMBER, src.ACCOUNT_NAME, src.STATUS_CODE, src.CUSTOMER_TYPE, src.PARTY_ID,
            src.BZ_LOAD_DATE, src.SV_LOAD_DATE, @AsOfDate, @HighDate, @LoadDttm, @LoadDttm, 'Y'
        FROM #src src
        LEFT JOIN svo.D_CUSTOMER_ACCOUNT tgt ON tgt.CUSTOMER_ACCOUNT_ID = src.CUSTOMER_ACCOUNT_ID AND tgt.CURR_IND = 'Y'
        WHERE tgt.CUSTOMER_ACCOUNT_ID IS NULL;

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
