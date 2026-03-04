

/* D_PAYMENT_METHOD (Dim, Type 1, no SCD) */
IF OBJECT_ID('Oracle_Reporting_P2.svo.D_PAYMENT_METHOD','U') IS NOT NULL
    DROP TABLE Oracle_Reporting_P2.svo.D_PAYMENT_METHOD;
GO

CREATE TABLE Oracle_Reporting_P2.svo.D_PAYMENT_METHOD
(
    PAYMENT_METHOD_SK      BIGINT IDENTITY(1,1) NOT NULL,
    PAYMENT_METHOD_ID      VARCHAR(30) NOT NULL,          -- ArReceiptMethodReceiptMethodId
    PAYMENT_METHOD_NAME    VARCHAR(30) NOT NULL,     -- ArReceiptMethodName
    BZ_LOAD_DATE           DATE NULL,                -- CAST(AddDateTime AS DATE)
    SV_LOAD_DATE           DATE NOT NULL DEFAULT CAST(GETDATE() AS DATE),

    CONSTRAINT PK_D_PAYMENT_METHOD
        PRIMARY KEY CLUSTERED (PAYMENT_METHOD_SK)
) ON FG_SilverDim;
GO

CREATE UNIQUE NONCLUSTERED INDEX UX_D_PAYMENT_METHOD_ID
ON Oracle_Reporting_P2.svo.D_PAYMENT_METHOD (PAYMENT_METHOD_ID)
ON FG_SilverDim;
GO

TRUNCATE TABLE Oracle_Reporting_P2.svo.D_PAYMENT_METHOD;
GO

/* Plug row */
SET IDENTITY_INSERT Oracle_Reporting_P2.svo.D_PAYMENT_METHOD ON;

INSERT INTO Oracle_Reporting_P2.svo.D_PAYMENT_METHOD
(
    PAYMENT_METHOD_SK,
    PAYMENT_METHOD_ID,
    PAYMENT_METHOD_NAME,
    BZ_LOAD_DATE,
    SV_LOAD_DATE
)
VALUES
(
    0,
    -1,
    'Unknown',
    NULL,
    CAST(GETDATE() AS DATE)
);

SET IDENTITY_INSERT Oracle_Reporting_P2.svo.D_PAYMENT_METHOD OFF;
GO


/* Initial load (Bronze -> Silver) */
INSERT INTO Oracle_Reporting_P2.svo.D_PAYMENT_METHOD
(
    PAYMENT_METHOD_ID,
    PAYMENT_METHOD_NAME,
    BZ_LOAD_DATE,
    SV_LOAD_DATE
)
SELECT
    src.ArReceiptMethodReceiptMethodId,
    src.ArReceiptMethodName,
    CAST(src.AddDateTime AS DATE),
    CAST(GETDATE() AS DATE)
FROM DW_BronzeSilver_PROD.bzo.AR_ReceiptMethodExtractPVO src
WHERE NOT EXISTS
(
    SELECT 1
    FROM Oracle_Reporting_P2.svo.D_PAYMENT_METHOD tgt
    WHERE tgt.PAYMENT_METHOD_ID = src.ArReceiptMethodReceiptMethodId
);
GO
