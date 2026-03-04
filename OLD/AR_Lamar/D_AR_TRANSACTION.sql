USE Oracle_Reporting_P2
GO

SET NOCOUNT ON;
GO

/* ============================================================
   2) D_AR_TRANSACTION
   ============================================================ */

IF OBJECT_ID('[svo].[D_AR_TRANSACTION]', 'U') IS NOT NULL
    DROP TABLE [svo].[D_AR_TRANSACTION];
GO

CREATE TABLE [svo].[D_AR_TRANSACTION]
(
    AR_TRANSACTION_SK               bigint          IDENTITY(1,1) NOT NULL,
    AR_TRANSACTION_ID               bigint          NOT NULL,

    AR_TRANSACTION_NUMBER           varchar(50)     NULL,
    AR_REFERENCE                    varchar(240)    NULL,
    AR_PO_NUMBER                    varchar(50)     NULL,
    AR_TRANSACTION_CLASS_CODE       varchar(30)     NULL,
    AR_TRANSACTION_STATUS_CODE      varchar(30)     NULL,
    AR_REASON_CODE                  varchar(30)     NULL,
    AR_COMPLETE_FLAG                char(1)         NULL,

    AddDateTime                     datetime        NULL,
    BZ_LOAD_DATE                    AS CONVERT(date, AddDateTime) PERSISTED,
    SV_LOAD_DATE                    date            NOT NULL
        CONSTRAINT DF_D_AR_TRANSACTION_SV_LOAD_DATE DEFAULT (CONVERT(date, GETDATE())),

    CONSTRAINT PK_D_AR_TRANSACTION PRIMARY KEY CLUSTERED (AR_TRANSACTION_SK)
        ON [FG_SilverDim]
)
ON [FG_SilverDim];
GO

CREATE UNIQUE NONCLUSTERED INDEX UX_D_AR_TRANSACTION_BK
ON [svo].[D_AR_TRANSACTION] (AR_TRANSACTION_ID)
ON [FG_SilverDim];
GO

IF NOT EXISTS (SELECT 1 FROM [svo].[D_AR_TRANSACTION] WHERE AR_TRANSACTION_ID = 0)
BEGIN
    SET IDENTITY_INSERT [svo].[D_AR_TRANSACTION] ON;

    INSERT INTO [svo].[D_AR_TRANSACTION]
    (
        AR_TRANSACTION_SK,
        AR_TRANSACTION_ID,
        AR_TRANSACTION_NUMBER,
        AR_REFERENCE,
        AR_PO_NUMBER,
        AR_TRANSACTION_CLASS_CODE,
        AR_TRANSACTION_STATUS_CODE,
        AR_REASON_CODE,
        AR_COMPLETE_FLAG,
        AddDateTime,
        SV_LOAD_DATE
    )
    VALUES
    (
        0, 0, 'UNKNOWN', NULL, NULL, NULL, NULL, NULL, NULL, GETDATE(), CONVERT(date, GETDATE())
    );

    SET IDENTITY_INSERT [svo].[D_AR_TRANSACTION] OFF;
END
GO

;WITH S AS
(
    SELECT
        H.RaCustomerTrxCustomerTrxId      AS AR_TRANSACTION_ID,
        H.RaCustomerTrxTrxNumber          AS AR_TRANSACTION_NUMBER,
        H.RaCustomerTrxCustomerReference  AS AR_REFERENCE,
        H.RaCustomerTrxPurchaseOrder      AS AR_PO_NUMBER,
        H.RaCustomerTrxTrxClass           AS AR_TRANSACTION_CLASS_CODE,
        H.RaCustomerTrxStatusTrx          AS AR_TRANSACTION_STATUS_CODE,
        H.RaCustomerTrxReasonCode         AS AR_REASON_CODE,
        H.RaCustomerTrxCompleteFlag       AS AR_COMPLETE_FLAG,
        H.AddDateTime
    FROM [bzo].[AR_TransactionHeaderExtractPVO] H
)
MERGE [svo].[D_AR_TRANSACTION] AS D
USING S
ON D.AR_TRANSACTION_ID = S.AR_TRANSACTION_ID
WHEN MATCHED THEN
    UPDATE SET
        D.AR_TRANSACTION_NUMBER      = S.AR_TRANSACTION_NUMBER,
        D.AR_REFERENCE               = S.AR_REFERENCE,
        D.AR_PO_NUMBER               = S.AR_PO_NUMBER,
        D.AR_TRANSACTION_CLASS_CODE  = S.AR_TRANSACTION_CLASS_CODE,
        D.AR_TRANSACTION_STATUS_CODE = S.AR_TRANSACTION_STATUS_CODE,
        D.AR_REASON_CODE             = S.AR_REASON_CODE,
        D.AR_COMPLETE_FLAG           = S.AR_COMPLETE_FLAG,
        D.AddDateTime                = S.AddDateTime
WHEN NOT MATCHED BY TARGET THEN
    INSERT
    (
        AR_TRANSACTION_ID,
        AR_TRANSACTION_NUMBER,
        AR_REFERENCE,
        AR_PO_NUMBER,
        AR_TRANSACTION_CLASS_CODE,
        AR_TRANSACTION_STATUS_CODE,
        AR_REASON_CODE,
        AR_COMPLETE_FLAG,
        AddDateTime
    )
    VALUES
    (
        S.AR_TRANSACTION_ID,
        S.AR_TRANSACTION_NUMBER,
        S.AR_REFERENCE,
        S.AR_PO_NUMBER,
        S.AR_TRANSACTION_CLASS_CODE,
        S.AR_TRANSACTION_STATUS_CODE,
        S.AR_REASON_CODE,
        S.AR_COMPLETE_FLAG,
        S.AddDateTime
    )
WHEN NOT MATCHED BY SOURCE
    AND D.AR_TRANSACTION_SK <> 0
THEN DELETE
;
GO
