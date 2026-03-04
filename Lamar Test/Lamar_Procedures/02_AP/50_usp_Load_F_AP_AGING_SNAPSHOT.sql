/* =========================================================
   usp_Load_F_AP_AGING_SNAPSHOT
   Derived fact: reload from svo.F_AP_PAYMENTS + D_CALENDAR.
   Option A: Delete for current snapshot date then INSERT (or full reload).
   No bzo source; watermark can advance by run/snapshot date or no-op.
   ========================================================= */
CREATE OR ALTER PROCEDURE svo.usp_Load_F_AP_AGING_SNAPSHOT
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    DECLARE
        @ProcName       SYSNAME        = OBJECT_SCHEMA_NAME(@@PROCID) + '.' + OBJECT_NAME(@@PROCID),
        @TargetObject   SYSNAME        = 'svo.F_AP_AGING_SNAPSHOT',
        @StartDttm      DATETIME2(0)   = SYSDATETIME(),
        @EndDttm        DATETIME2(0),
        @RunId          BIGINT         = NULL,
        @ErrMsg         NVARCHAR(4000) = NULL,
        @AsOfDate       DATE           = CAST(GETDATE() AS DATE),
        @SnapshotDateSk INT            = CONVERT(INT, FORMAT(CAST(GETDATE() AS DATE), 'yyyyMMdd')),
        @RowInserted    INT            = 0;

    BEGIN TRY
        INSERT INTO etl.ETL_RUN (PROC_NAME, TARGET_OBJECT, ASOF_DATE, START_DTTM, STATUS)
        VALUES (@ProcName, @TargetObject, @AsOfDate, @StartDttm, 'STARTED');

        SET @RunId = SCOPE_IDENTITY();
        IF @RunId IS NULL
            THROW 50001, 'ETL_RUN insert failed: RUN_ID is NULL.', 1;

        -- Option A: delete rows for current snapshot date then insert (idempotent for same day)
        DELETE FROM svo.F_AP_AGING_SNAPSHOT WHERE SNAPSHOT_DATE_SK = @SnapshotDateSk;

        INSERT INTO svo.F_AP_AGING_SNAPSHOT
        (
            SNAPSHOT_DATE_SK,
            AP_INVOICE_HEADER_SK,
            AP_DISBURSEMENT_HEADER_SK,
            ACCOUNT_SK,
            BUSINESS_OFFERING_SK,
            COMPANY_SK,
            COST_CENTER_SK,
            INDUSTRY_SK,
            INTERCOMPANY_SK,
            LEGAL_ENTITY_SK,
            BUSINESS_UNIT_SK,
            VENDOR_SITE_SK,
            LEDGER_SK,
            INV_CURRENCY_SK,
            PAY_CURRENCY_SK,
            DUE_DATE_SK,
            ACCOUNTING_DATE_SK,
            PAYMENT_DOCUMENT_ID,
            PAYMENT_NUM,
            ORIGINAL_AMOUNT,
            AMOUNT_PAID,
            AMOUNT_REMAINING,
            DAYS_PAST_DUE,
            AGING_BUCKET_CODE,
            BZ_LOAD_DATE,
            SV_LOAD_DATE
        )
        SELECT
            @SnapshotDateSk,
            P.AP_INVOICE_HEADER_SK,
            P.AP_DISBURSEMENT_HEADER_SK,
            P.ACCOUNT_SK,
            P.BUSINESS_OFFERING_SK,
            P.COMPANY_SK,
            P.COST_CENTER_SK,
            P.INDUSTRY_SK,
            P.INTERCOMPANY_SK,
            P.LEGAL_ENTITY_SK,
            P.BUSINESS_UNIT_SK,
            P.VENDOR_SITE_SK,
            P.LEDGER_SK,
            P.INV_CURRENCY_SK,
            P.PAY_CURRENCY_SK,
            P.DUE_DATE_SK,
            P.ACCOUNTING_DATE_SK,
            P.PAYMENT_DOCUMENT_ID,
            P.AP_PAYMENT_SCHEDULES_ALL_PAYMENT_NUM AS PAYMENT_NUM,
            SUM(ISNULL(P.AP_PAYMENT_SCHEDULES_ALL_GROSS_AMOUNT, 0)) AS ORIGINAL_AMOUNT,
            SUM(ISNULL(P.AP_PAYMENT_SCHEDULES_ALL_GROSS_AMOUNT, 0) - ISNULL(P.AP_PAYMENT_SCHEDULES_ALL_AMOUNT_REMAINING, 0)) AS AMOUNT_PAID,
            SUM(ISNULL(P.AP_PAYMENT_SCHEDULES_ALL_AMOUNT_REMAINING, 0)) AS AMOUNT_REMAINING,
            MAX(CASE WHEN D_DUE.[DATE] IS NULL THEN NULL ELSE DATEDIFF(DAY, D_DUE.[DATE], CAST(GETDATE() AS DATE)) END) AS DAYS_PAST_DUE,
            CASE
                WHEN MAX(CASE WHEN D_DUE.[DATE] IS NULL THEN NULL ELSE DATEDIFF(DAY, D_DUE.[DATE], CAST(GETDATE() AS DATE)) END) IS NULL THEN 'NO DUE DATE'
                WHEN MAX(CASE WHEN D_DUE.[DATE] IS NULL THEN NULL ELSE DATEDIFF(DAY, D_DUE.[DATE], CAST(GETDATE() AS DATE)) END) <= 0 THEN 'CURRENT'
                WHEN MAX(CASE WHEN D_DUE.[DATE] IS NULL THEN NULL ELSE DATEDIFF(DAY, D_DUE.[DATE], CAST(GETDATE() AS DATE)) END) BETWEEN 1 AND 30 THEN '1-30'
                WHEN MAX(CASE WHEN D_DUE.[DATE] IS NULL THEN NULL ELSE DATEDIFF(DAY, D_DUE.[DATE], CAST(GETDATE() AS DATE)) END) BETWEEN 31 AND 60 THEN '31-60'
                WHEN MAX(CASE WHEN D_DUE.[DATE] IS NULL THEN NULL ELSE DATEDIFF(DAY, D_DUE.[DATE], CAST(GETDATE() AS DATE)) END) BETWEEN 61 AND 90 THEN '61-90'
                ELSE '90+'
            END AS AGING_BUCKET_CODE,
            P.BZ_LOAD_DATE_PAID,
            CAST(GETDATE() AS DATE)
        FROM svo.F_AP_PAYMENTS AS P
        LEFT JOIN svo.D_CALENDAR AS D_DUE ON D_DUE.DATE_SK = P.DUE_DATE_SK
        WHERE ISNULL(P.AP_PAYMENT_SCHEDULES_ALL_AMOUNT_REMAINING, 0) <> 0
        GROUP BY
            P.AP_INVOICE_HEADER_SK,
            P.AP_DISBURSEMENT_HEADER_SK,
            P.ACCOUNT_SK,
            P.BUSINESS_OFFERING_SK,
            P.COMPANY_SK,
            P.COST_CENTER_SK,
            P.INDUSTRY_SK,
            P.INTERCOMPANY_SK,
            P.LEGAL_ENTITY_SK,
            P.BUSINESS_UNIT_SK,
            P.VENDOR_SITE_SK,
            P.LEDGER_SK,
            P.INV_CURRENCY_SK,
            P.PAY_CURRENCY_SK,
            P.DUE_DATE_SK,
            P.ACCOUNTING_DATE_SK,
            P.PAYMENT_DOCUMENT_ID,
            P.AP_PAYMENT_SCHEDULES_ALL_PAYMENT_NUM,
            P.BZ_LOAD_DATE_PAID,
            CASE
                WHEN CASE WHEN D_DUE.[DATE] IS NULL THEN NULL ELSE DATEDIFF(DAY, D_DUE.[DATE], CAST(GETDATE() AS DATE)) END IS NULL THEN 'NO DUE DATE'
                WHEN CASE WHEN D_DUE.[DATE] IS NULL THEN NULL ELSE DATEDIFF(DAY, D_DUE.[DATE], CAST(GETDATE() AS DATE)) END <= 0 THEN 'CURRENT'
                WHEN CASE WHEN D_DUE.[DATE] IS NULL THEN NULL ELSE DATEDIFF(DAY, D_DUE.[DATE], CAST(GETDATE() AS DATE)) END BETWEEN 1 AND 30 THEN '1-30'
                WHEN CASE WHEN D_DUE.[DATE] IS NULL THEN NULL ELSE DATEDIFF(DAY, D_DUE.[DATE], CAST(GETDATE() AS DATE)) END BETWEEN 31 AND 60 THEN '31-60'
                WHEN CASE WHEN D_DUE.[DATE] IS NULL THEN NULL ELSE DATEDIFF(DAY, D_DUE.[DATE], CAST(GETDATE() AS DATE)) END BETWEEN 61 AND 90 THEN '61-90'
                ELSE '90+'
            END
        HAVING MAX(CASE WHEN D_DUE.[DATE] IS NULL THEN NULL ELSE DATEDIFF(DAY, D_DUE.[DATE], CAST(GETDATE() AS DATE)) END) > 0;

        SET @RowInserted = @@ROWCOUNT;

        -- Advance watermark so run is logged (use snapshot date as logical watermark)
        UPDATE etl.ETL_WATERMARK SET LAST_WATERMARK = CAST(GETDATE() AS DATETIME2(7)), UDT_DATE = SYSDATETIME() WHERE TABLE_NAME = @TargetObject;

        SET @EndDttm = SYSDATETIME();
        UPDATE etl.ETL_RUN SET END_DTTM = @EndDttm, STATUS = 'SUCCESS', ROW_INSERTED = @RowInserted, ROW_UPDATED = 0, ROW_EXPIRED = 0, ERROR_MESSAGE = NULL WHERE RUN_ID = @RunId;
    END TRY
    BEGIN CATCH
        SET @EndDttm = SYSDATETIME();
        SET @ErrMsg = CONCAT('Error ', ERROR_NUMBER(), ' (Line ', ERROR_LINE(), '): ', ERROR_MESSAGE());
        IF @RunId IS NOT NULL
            UPDATE etl.ETL_RUN SET END_DTTM = @EndDttm, STATUS = 'FAILURE', ROW_INSERTED = @RowInserted, ROW_UPDATED = 0, ROW_EXPIRED = 0, ERROR_MESSAGE = @ErrMsg WHERE RUN_ID = @RunId;
        ;THROW;
    END CATCH
END;
GO
