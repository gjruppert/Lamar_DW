USE Oracle_Reporting_P2
SET NOCOUNT ON;
GO

/* ============================================================
   1) DIM: svo.D_AR_CASH_RECEIPT
   ============================================================ */

IF OBJECT_ID('[svo].[D_AR_CASH_RECEIPT]', 'U') IS NOT NULL
    DROP TABLE [svo].[D_AR_CASH_RECEIPT];
GO

CREATE TABLE [svo].[D_AR_CASH_RECEIPT]
(
    AR_CASH_RECEIPT_SK              bigint          IDENTITY(1,1) NOT NULL,

    AR_CASH_RECEIPT_ID              bigint          NOT NULL,    -- BK: ArCashReceiptCashReceiptId

    RECEIPT_NUMBER                  varchar(30)     NULL,
    RECEIPT_STATUS                  varchar(30)     NULL,
    CURRENCY_CODE                   varchar(15)     NOT NULL,
    EXCHANGE_RATE_TYPE              varchar(30)     NULL,

    AddDateTime                     datetime        NULL,
    BZ_LOAD_DATE                    AS CONVERT(date, AddDateTime) PERSISTED,
    SV_LOAD_DATE                    date            NOT NULL
        CONSTRAINT DF_D_AR_CASH_RECEIPT_SV_LOAD_DATE DEFAULT (CONVERT(date, GETDATE())),

    CONSTRAINT PK_D_AR_CASH_RECEIPT PRIMARY KEY CLUSTERED (AR_CASH_RECEIPT_SK)
        ON [FG_SilverDim]
)
ON [FG_SilverDim];
GO

CREATE UNIQUE NONCLUSTERED INDEX UX_D_AR_CASH_RECEIPT_BK
ON [svo].[D_AR_CASH_RECEIPT] (AR_CASH_RECEIPT_ID)
ON [FG_SilverDim];
GO

IF NOT EXISTS (SELECT 1 FROM [svo].[D_AR_CASH_RECEIPT] WHERE AR_CASH_RECEIPT_ID = 0)
BEGIN
    SET IDENTITY_INSERT [svo].[D_AR_CASH_RECEIPT] ON;

    INSERT INTO [svo].[D_AR_CASH_RECEIPT]
    (
        AR_CASH_RECEIPT_SK,
        AR_CASH_RECEIPT_ID,
        RECEIPT_NUMBER,
        RECEIPT_STATUS,
        CURRENCY_CODE,
        EXCHANGE_RATE_TYPE,
        AddDateTime,
        SV_LOAD_DATE
    )
    VALUES
    (
        0,
        0,
        'UNKNOWN',
        'UNKNOWN',
        'UNK',
        NULL,
        GETDATE(),
        CONVERT(date, GETDATE())
    );

    SET IDENTITY_INSERT [svo].[D_AR_CASH_RECEIPT] OFF;
END
GO

;WITH S AS
(
    SELECT
        R.ArCashReceiptCashReceiptId      AS AR_CASH_RECEIPT_ID,
        R.ArCashReceiptReceiptNumber      AS RECEIPT_NUMBER,
        R.ArCashReceiptStatus             AS RECEIPT_STATUS,
        R.ArCashReceiptCurrencyCode       AS CURRENCY_CODE,
        R.ArCashReceiptExchangeRateType   AS EXCHANGE_RATE_TYPE,
        R.AddDateTime
    FROM [bzo].[AR_ReceiptHeaderExtractPVO] R
)
MERGE [svo].[D_AR_CASH_RECEIPT] AS D
USING S
ON D.AR_CASH_RECEIPT_ID = S.AR_CASH_RECEIPT_ID
WHEN MATCHED THEN
    UPDATE SET
        D.RECEIPT_NUMBER     = S.RECEIPT_NUMBER,
        D.RECEIPT_STATUS     = S.RECEIPT_STATUS,
        D.CURRENCY_CODE      = S.CURRENCY_CODE,
        D.EXCHANGE_RATE_TYPE = S.EXCHANGE_RATE_TYPE,
        D.AddDateTime        = S.AddDateTime
WHEN NOT MATCHED BY TARGET THEN
    INSERT
    (
        AR_CASH_RECEIPT_ID,
        RECEIPT_NUMBER,
        RECEIPT_STATUS,
        CURRENCY_CODE,
        EXCHANGE_RATE_TYPE,
        AddDateTime
    )
    VALUES
    (
        S.AR_CASH_RECEIPT_ID,
        S.RECEIPT_NUMBER,
        S.RECEIPT_STATUS,
        S.CURRENCY_CODE,
        S.EXCHANGE_RATE_TYPE,
        S.AddDateTime
    )
WHEN NOT MATCHED BY SOURCE
    AND D.AR_CASH_RECEIPT_SK <> 0
THEN DELETE
;
GO
