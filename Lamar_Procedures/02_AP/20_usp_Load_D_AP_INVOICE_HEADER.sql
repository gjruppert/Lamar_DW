/* =========================================================
   usp_Load_D_AP_INVOICE_HEADER
   Type 1 incremental load. Source: bzo.AP_InvoiceHeaderExtractPVO
   Watermark: AddDateTime. Grain: INVOICE_ID. Plug row SK=0 if missing.
   Joins: D_VENDOR_SITE (CURR_IND='Y'), D_PAYMENT_TERM (CURR_IND='Y').
   Idempotent: safe to retry.
   ========================================================= */
CREATE OR ALTER PROCEDURE svo.usp_Load_D_AP_INVOICE_HEADER
    @BatchId INT = NULL
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    DECLARE
        @ProcName       SYSNAME        = OBJECT_SCHEMA_NAME(@@PROCID) + '.' + OBJECT_NAME(@@PROCID),
        @TargetObject   SYSNAME        = 'svo.D_AP_INVOICE_HEADER',
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

    SELECT @TableBridgeID = TableBridgeID FROM meta.MedallionTableBridge WHERE targettable = N'AP_InvoiceHeaderExtractPVO';

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

        IF EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'UX_D_AP_INVOICE_HEADER_INVOICE_ID' AND object_id = OBJECT_ID('svo.D_AP_INVOICE_HEADER'))
            DROP INDEX UX_D_AP_INVOICE_HEADER_INVOICE_ID ON svo.D_AP_INVOICE_HEADER;

        IF NOT EXISTS (SELECT 1 FROM svo.D_AP_INVOICE_HEADER WHERE AP_INVOICE_HEADER_SK = 0)
        BEGIN
            SET IDENTITY_INSERT svo.D_AP_INVOICE_HEADER ON;
            INSERT INTO svo.D_AP_INVOICE_HEADER
            (AP_INVOICE_HEADER_SK, INVOICE_ID, INVOICE_NUMBER, VENDOR_SITE_SK, INVOICE_DATE_SK, GL_DATE_SK,
             INVOICE_TYPE_CODE, INVOICE_CURRENCY_CODE, PAYMENT_CURRENCY_CODE, APPROVAL_STATUS, PAYMENT_STATUS_FLAG, TERMS_DATE_SK,
             INVOICE_AMOUNT, AMOUNT_PAID, DISCOUNT_AMOUNT_TAKEN, CANCELLED_AMOUNT, CANCELLED_BY, CANCELLED_DATE_SK, EXCLUDE_FREIGHT_FROM_DISC, DESCRIPTION, PAYMENT_TERMS,
             CODE_COMBINATION_ID, BZ_LOAD_DATE, SV_LOAD_DATE)
            VALUES
            (0, -1, 'Unknown', 0, 10101, 10101, 'UNK', 'UNK', 'UNK', 'U', 'U', 10101, 0, 0, 0, 0, 'UNK', 10101, 'U', 'UNK', 'UNK', NULL, GETDATE(), GETDATE());
            SET IDENTITY_INSERT svo.D_AP_INVOICE_HEADER OFF;
        END;

        IF OBJECT_ID('tempdb..#src') IS NOT NULL DROP TABLE #src;

        SELECT
            H.ApInvoicesInvoiceId AS INVOICE_ID,
            H.ApInvoicesInvoiceNum AS INVOICE_NUMBER,
            ISNULL(VS.VENDOR_SITE_SK, 0) AS VENDOR_SITE_SK,
            CONVERT(INT, CONVERT(CHAR(8), ISNULL(H.ApInvoicesInvoiceDate, '0001-01-01'), 112)) AS INVOICE_DATE_SK,
            CONVERT(INT, CONVERT(CHAR(8), ISNULL(H.ApInvoicesGlDate, '0001-01-01'), 112)) AS GL_DATE_SK,
            ISNULL(H.ApInvoicesInvoiceTypeLookupCode, 'UNK') AS INVOICE_TYPE_CODE,
            ISNULL(H.ApInvoicesInvoiceCurrencyCode, 'UNK') AS INVOICE_CURRENCY_CODE,
            ISNULL(H.ApInvoicesPaymentCurrencyCode, 'UNK') AS PAYMENT_CURRENCY_CODE,
            ISNULL(H.ApInvoicesApprovalStatus, 'U') AS APPROVAL_STATUS,
            ISNULL(H.ApInvoicesPaymentStatusFlag, 'U') AS PAYMENT_STATUS_FLAG,
            CONVERT(INT, CONVERT(CHAR(8), ISNULL(H.ApInvoicesTermsDate, '0001-01-01'), 112)) AS TERMS_DATE_SK,
            ISNULL(H.ApInvoicesInvoiceAmount, 0) AS INVOICE_AMOUNT,
            ISNULL(H.ApInvoicesAmountPaid, 0) AS AMOUNT_PAID,
            ISNULL(H.ApInvoicesDiscountAmountTaken, 0) AS DISCOUNT_AMOUNT_TAKEN,
            ISNULL(H.ApInvoicesCancelledAmount, 0) AS CANCELLED_AMOUNT,
            ISNULL(H.ApInvoicesCancelledBy, 'UNK') AS CANCELLED_BY,
            CONVERT(INT, CONVERT(CHAR(8), ISNULL(H.ApInvoicesCancelledDate, '0001-01-01'), 112)) AS CANCELLED_DATE_SK,
            ISNULL(H.ApInvoicesExcludeFreightFromDiscount, 'U') AS EXCLUDE_FREIGHT_FROM_DISC,
            ISNULL(H.ApInvoicesDescription, 'UNK') AS DESCRIPTION,
            ISNULL(PT.PAYMENT_TERM_NAME, 'UNK') AS PAYMENT_TERMS,
            H.ApInvoicesAcctsPayCodeCombinationId AS CODE_COMBINATION_ID,
            ISNULL(H.AddDateTime, SYSDATETIME()) AS BZ_LOAD_DATE,
            SYSDATETIME() AS SV_LOAD_DATE,
            H.AddDateTime AS SourceAddDateTime
        INTO #src
        FROM bzo.AP_InvoiceHeaderExtractPVO H
        LEFT JOIN svo.D_PAYMENT_TERM AS PT ON PT.PAYMENT_TERM_ID = H.ApInvoicesTermsId AND PT.CURR_IND = 'Y'
        LEFT JOIN svo.D_VENDOR_SITE  AS VS ON VS.VENDOR_ID = H.ApInvoicesVendorId AND VS.VENDOR_SITE_ID = H.ApInvoicesVendorSiteId AND VS.CURR_IND = 'Y'
        WHERE H.AddDateTime > @LastWatermark;

        SELECT @MaxWatermark = MAX(SourceAddDateTime) FROM #src;

        MERGE svo.D_AP_INVOICE_HEADER AS tgt
        USING #src AS src ON tgt.INVOICE_ID = src.INVOICE_ID
        WHEN MATCHED THEN UPDATE SET
            tgt.INVOICE_NUMBER = src.INVOICE_NUMBER,
            tgt.VENDOR_SITE_SK = src.VENDOR_SITE_SK,
            tgt.INVOICE_DATE_SK = src.INVOICE_DATE_SK,
            tgt.GL_DATE_SK = src.GL_DATE_SK,
            tgt.INVOICE_TYPE_CODE = src.INVOICE_TYPE_CODE,
            tgt.INVOICE_CURRENCY_CODE = src.INVOICE_CURRENCY_CODE,
            tgt.PAYMENT_CURRENCY_CODE = src.PAYMENT_CURRENCY_CODE,
            tgt.APPROVAL_STATUS = src.APPROVAL_STATUS,
            tgt.PAYMENT_STATUS_FLAG = src.PAYMENT_STATUS_FLAG,
            tgt.TERMS_DATE_SK = src.TERMS_DATE_SK,
            tgt.INVOICE_AMOUNT = src.INVOICE_AMOUNT,
            tgt.AMOUNT_PAID = src.AMOUNT_PAID,
            tgt.DISCOUNT_AMOUNT_TAKEN = src.DISCOUNT_AMOUNT_TAKEN,
            tgt.CANCELLED_AMOUNT = src.CANCELLED_AMOUNT,
            tgt.CANCELLED_BY = src.CANCELLED_BY,
            tgt.CANCELLED_DATE_SK = src.CANCELLED_DATE_SK,
            tgt.EXCLUDE_FREIGHT_FROM_DISC = src.EXCLUDE_FREIGHT_FROM_DISC,
            tgt.DESCRIPTION = src.DESCRIPTION,
            tgt.PAYMENT_TERMS = src.PAYMENT_TERMS,
            tgt.CODE_COMBINATION_ID = src.CODE_COMBINATION_ID,
            tgt.BZ_LOAD_DATE = src.BZ_LOAD_DATE,
            tgt.SV_LOAD_DATE = src.SV_LOAD_DATE
        WHEN NOT MATCHED BY TARGET THEN INSERT (
            INVOICE_ID, INVOICE_NUMBER, VENDOR_SITE_SK, INVOICE_DATE_SK, GL_DATE_SK, INVOICE_TYPE_CODE, INVOICE_CURRENCY_CODE, PAYMENT_CURRENCY_CODE,
            APPROVAL_STATUS, PAYMENT_STATUS_FLAG, TERMS_DATE_SK, INVOICE_AMOUNT, AMOUNT_PAID, DISCOUNT_AMOUNT_TAKEN, CANCELLED_AMOUNT, CANCELLED_BY, CANCELLED_DATE_SK,
            EXCLUDE_FREIGHT_FROM_DISC, DESCRIPTION, PAYMENT_TERMS, CODE_COMBINATION_ID, BZ_LOAD_DATE, SV_LOAD_DATE
        ) VALUES (
            src.INVOICE_ID, src.INVOICE_NUMBER, src.VENDOR_SITE_SK, src.INVOICE_DATE_SK, src.GL_DATE_SK, src.INVOICE_TYPE_CODE, src.INVOICE_CURRENCY_CODE, src.PAYMENT_CURRENCY_CODE,
            src.APPROVAL_STATUS, src.PAYMENT_STATUS_FLAG, src.TERMS_DATE_SK, src.INVOICE_AMOUNT, src.AMOUNT_PAID, src.DISCOUNT_AMOUNT_TAKEN, src.CANCELLED_AMOUNT, src.CANCELLED_BY, src.CANCELLED_DATE_SK,
            src.EXCLUDE_FREIGHT_FROM_DISC, src.DESCRIPTION, src.PAYMENT_TERMS, src.CODE_COMBINATION_ID, src.BZ_LOAD_DATE, src.SV_LOAD_DATE
        )
        OUTPUT $action INTO @MergeActions(ActionTaken);

        SELECT @RowInserted = SUM(CASE WHEN ActionTaken = 'INSERT' THEN 1 ELSE 0 END),
               @RowUpdated  = SUM(CASE WHEN ActionTaken = 'UPDATE' THEN 1 ELSE 0 END)
        FROM @MergeActions;

        IF @MaxWatermark IS NOT NULL
            UPDATE etl.ETL_WATERMARK SET LAST_WATERMARK = @MaxWatermark, UDT_DATE = SYSDATETIME() WHERE TABLE_NAME = @TargetObject;

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
        IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'UX_D_AP_INVOICE_HEADER_INVOICE_ID' AND object_id = OBJECT_ID('svo.D_AP_INVOICE_HEADER'))
            CREATE UNIQUE NONCLUSTERED INDEX UX_D_AP_INVOICE_HEADER_INVOICE_ID ON svo.D_AP_INVOICE_HEADER(INVOICE_ID) ON FG_SilverDim;
        ;THROW;
    END CATCH
END;
GO
