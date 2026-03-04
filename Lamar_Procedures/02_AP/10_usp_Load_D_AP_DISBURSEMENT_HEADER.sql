/* =========================================================
   usp_Load_D_AP_DISBURSEMENT_HEADER
   Type 1 incremental load. Source: bzo.AP_DisbursementHeaderExtractPVO
   Watermark: AddDateTime. Grain: CHECK_ID. Plug row SK=0 if missing.
   Idempotent: safe to retry.
   ========================================================= */
CREATE OR ALTER PROCEDURE svo.usp_Load_D_AP_DISBURSEMENT_HEADER
    @BatchId INT = NULL
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    DECLARE
        @ProcName       SYSNAME        = OBJECT_SCHEMA_NAME(@@PROCID) + '.' + OBJECT_NAME(@@PROCID),
        @TargetObject   SYSNAME        = 'svo.D_AP_DISBURSEMENT_HEADER',
        @StartDttm      DATETIME2(0)   = SYSDATETIME(),
        @EndDttm        DATETIME2(0),
        @RunId          BIGINT         = NULL,
        @ErrMsg         NVARCHAR(4000) = NULL,
        @AsOfDate       DATE           = CAST(GETDATE() AS DATE),
        @LastWatermark  DATETIME2(7),
        @MaxWatermark   DATETIME2(7)   = NULL,
        @RowInserted    INT            = 0,
        @RowUpdated     INT            = 0,
        @TableBridgeID  INT            = NULL;

    DECLARE @MergeActions TABLE (ActionTaken VARCHAR(10) NOT NULL);

    SELECT @TableBridgeID = TableBridgeID FROM meta.MedallionTableBridge WHERE targettable = N'AP_DisbursementHeaderExtractPVO';

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

        IF EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'UX_D_AP_DISBURSEMENT_HEADER_CHECK_ID' AND object_id = OBJECT_ID('svo.D_AP_DISBURSEMENT_HEADER'))
            DROP INDEX UX_D_AP_DISBURSEMENT_HEADER_CHECK_ID ON svo.D_AP_DISBURSEMENT_HEADER;

        IF NOT EXISTS (SELECT 1 FROM svo.D_AP_DISBURSEMENT_HEADER WHERE AP_DISBURSEMENT_HEADER_SK = 0)
        BEGIN
            SET IDENTITY_INSERT svo.D_AP_DISBURSEMENT_HEADER ON;
            INSERT INTO svo.D_AP_DISBURSEMENT_HEADER (AP_DISBURSEMENT_HEADER_SK, CHECK_ID, PAYMENT_METHOD, CHECK_DATE_SK, CHECK_NUMBER, AMOUNT, BZ_LOAD_DATE, SV_LOAD_DATE)
            VALUES (0, -1, 'UNK', 0, 0, -1, GETDATE(), GETDATE());
            SET IDENTITY_INSERT svo.D_AP_DISBURSEMENT_HEADER OFF;
        END;

        IF OBJECT_ID('tempdb..#src') IS NOT NULL DROP TABLE #src;

        SELECT
            D.ApChecksAllCheckId AS CHECK_ID,
            D.ApChecksAllCheckNumber AS CHECK_NUMBER,
            CONVERT(INT, FORMAT(ISNULL(D.ApChecksAllCheckDate, '0001-01-01'), 'yyyyMMdd')) AS CHECK_DATE_SK,
            CONVERT(INT, FORMAT(ISNULL(D.ApChecksAllClearedDate, '0001-01-01'), 'yyyyMMdd')) AS CLEARED_DATE_SK,
            CONVERT(INT, FORMAT(ISNULL(D.ApChecksAllVoidDate, '0001-01-01'), 'yyyyMMdd')) AS VOID_DATE_SK,
            ISNULL(D.ApChecksAllCheckVoucherNum, -1) AS VOUCHER_NUM,
            ISNULL(D.ApChecksAllBankAccountNum, -1) AS BANK_ACCOUNT_NUM,
            ISNULL(PM.PAYMENT_METHOD_NAME, 'UNK') AS PAYMENT_METHOD,
            ISNULL(D.ApChecksAllAmount, 0) AS AMOUNT,
            ISNULL(D.ApChecksAllBaseAmount, 0) AS BASE_AMOUNT,
            ISNULL(D.ApChecksAllClearedAmount, 0) AS CLEARED_AMOUNT,
            ISNULL(D.ApChecksAllClearedChargesAmount, 0) AS CLEARED_CHARGES_AMOUNT,
            D.AddDateTime AS BZ_LOAD_DATE,
            SYSDATETIME() AS SV_LOAD_DATE,
            D.AddDateTime AS SourceAddDateTime
        INTO #src
        FROM bzo.AP_DisbursementHeaderExtractPVO D
        LEFT JOIN svo.D_PAYMENT_METHOD AS PM ON PM.PAYMENT_METHOD_ID = D.ApChecksAllPaymentMethodCode AND PM.CURR_IND = 'Y'
        WHERE D.AddDateTime > @LastWatermark;

        SELECT @MaxWatermark = MAX(SourceAddDateTime) FROM #src;

        MERGE svo.D_AP_DISBURSEMENT_HEADER AS tgt
        USING #src AS src ON tgt.CHECK_ID = src.CHECK_ID
        WHEN MATCHED THEN UPDATE SET
            tgt.CHECK_NUMBER = src.CHECK_NUMBER,
            tgt.CHECK_DATE_SK = src.CHECK_DATE_SK,
            tgt.CLEARED_DATE_SK = src.CLEARED_DATE_SK,
            tgt.VOID_DATE_SK = src.VOID_DATE_SK,
            tgt.VOUCHER_NUM = src.VOUCHER_NUM,
            tgt.BANK_ACCOUNT_NUM = src.BANK_ACCOUNT_NUM,
            tgt.PAYMENT_METHOD = src.PAYMENT_METHOD,
            tgt.AMOUNT = src.AMOUNT,
            tgt.BASE_AMOUNT = src.BASE_AMOUNT,
            tgt.CLEARED_AMOUNT = src.CLEARED_AMOUNT,
            tgt.CLEARED_CHARGES_AMOUNT = src.CLEARED_CHARGES_AMOUNT,
            tgt.BZ_LOAD_DATE = src.BZ_LOAD_DATE,
            tgt.SV_LOAD_DATE = src.SV_LOAD_DATE
        WHEN NOT MATCHED BY TARGET THEN INSERT (
            CHECK_ID, CHECK_NUMBER, CHECK_DATE_SK, CLEARED_DATE_SK, VOID_DATE_SK, VOUCHER_NUM, BANK_ACCOUNT_NUM,
            PAYMENT_METHOD, AMOUNT, BASE_AMOUNT, CLEARED_AMOUNT, CLEARED_CHARGES_AMOUNT, BZ_LOAD_DATE, SV_LOAD_DATE
        ) VALUES (
            src.CHECK_ID, src.CHECK_NUMBER, src.CHECK_DATE_SK, src.CLEARED_DATE_SK, src.VOID_DATE_SK, src.VOUCHER_NUM, src.BANK_ACCOUNT_NUM,
            src.PAYMENT_METHOD, src.AMOUNT, src.BASE_AMOUNT, src.CLEARED_AMOUNT, src.CLEARED_CHARGES_AMOUNT, src.BZ_LOAD_DATE, src.SV_LOAD_DATE
        )
        OUTPUT $action INTO @MergeActions(ActionTaken);

        SELECT @RowInserted = SUM(CASE WHEN ActionTaken = 'INSERT' THEN 1 ELSE 0 END),
               @RowUpdated  = SUM(CASE WHEN ActionTaken = 'UPDATE' THEN 1 ELSE 0 END)
        FROM @MergeActions;

        IF @MaxWatermark IS NOT NULL
            UPDATE etl.ETL_WATERMARK SET LAST_WATERMARK = @MaxWatermark, UDT_DATE = SYSDATETIME() WHERE TABLE_NAME = @TargetObject;

        IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'UX_D_AP_DISBURSEMENT_HEADER_CHECK_ID' AND object_id = OBJECT_ID('svo.D_AP_DISBURSEMENT_HEADER'))
            CREATE UNIQUE NONCLUSTERED INDEX UX_D_AP_DISBURSEMENT_HEADER_CHECK_ID ON svo.D_AP_DISBURSEMENT_HEADER(CHECK_ID) ON FG_SilverFact;

        SET @EndDttm = SYSDATETIME();
        UPDATE etl.ETL_RUN SET END_DTTM = @EndDttm, STATUS = 'SUCCESS', ROW_INSERTED = @RowInserted, ROW_UPDATED = @RowUpdated, ROW_EXPIRED = 0, ERROR_MESSAGE = NULL WHERE RUN_ID = @RunId;
        IF COL_LENGTH('etl.ETL_RUN', 'ROW_UPDATED_T1') IS NOT NULL
            UPDATE etl.ETL_RUN SET ROW_UPDATED_T1 = @RowUpdated WHERE RUN_ID = @RunId;
    END TRY
    BEGIN CATCH
        SET @EndDttm = SYSDATETIME();
        SET @ErrMsg = CONCAT('Error ', ERROR_NUMBER(), ' (Line ', ERROR_LINE(), '): ', ERROR_MESSAGE());
        IF @RunId IS NOT NULL
            UPDATE etl.ETL_RUN SET END_DTTM = @EndDttm, STATUS = 'FAILURE', ROW_INSERTED = @RowInserted, ROW_UPDATED = @RowUpdated, ROW_EXPIRED = 0, ERROR_MESSAGE = @ErrMsg WHERE RUN_ID = @RunId;
        IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'UX_D_AP_DISBURSEMENT_HEADER_CHECK_ID' AND object_id = OBJECT_ID('svo.D_AP_DISBURSEMENT_HEADER'))
            CREATE UNIQUE NONCLUSTERED INDEX UX_D_AP_DISBURSEMENT_HEADER_CHECK_ID ON svo.D_AP_DISBURSEMENT_HEADER(CHECK_ID) ON FG_SilverFact;
        ;THROW;
    END CATCH
END;
GO
