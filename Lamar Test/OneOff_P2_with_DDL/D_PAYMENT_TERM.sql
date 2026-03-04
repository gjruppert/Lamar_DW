

IF OBJECT_ID('Oracle_Reporting_P2.svo.D_PAYMENT_TERM','U') IS NOT NULL
    DROP TABLE Oracle_Reporting_P2.svo.D_PAYMENT_TERM;
GO

CREATE TABLE Oracle_Reporting_P2.svo.D_PAYMENT_TERM
(
    PAYMENT_TERM_SK        BIGINT IDENTITY(1,1) NOT NULL,
    PAYMENT_TERM_ID        VARCHAR(30) NOT NULL,           -- PaymentTermHeaderTranslationTermId
    PAYMENT_TERM_NAME      VARCHAR(50) NOT NULL,      -- PaymentTermHeaderTranslationName
    BZ_LOAD_DATE           DATE NULL,
    SV_LOAD_DATE           DATE NOT NULL DEFAULT CAST(GETDATE() AS DATE),

    CONSTRAINT PK_D_PAYMENT_TERM
        PRIMARY KEY CLUSTERED (PAYMENT_TERM_SK)
) ON FG_SilverDim;
GO

CREATE UNIQUE NONCLUSTERED INDEX UX_D_PAYMENT_TERM_BK
ON Oracle_Reporting_P2.svo.D_PAYMENT_TERM (PAYMENT_TERM_ID)
ON FG_SilverDim;
GO

TRUNCATE TABLE Oracle_Reporting_P2.svo.D_PAYMENT_TERM;
GO

SET IDENTITY_INSERT Oracle_Reporting_P2.svo.D_PAYMENT_TERM ON;

INSERT INTO Oracle_Reporting_P2.svo.D_PAYMENT_TERM
(
    PAYMENT_TERM_SK,
    PAYMENT_TERM_ID,
    PAYMENT_TERM_NAME,
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

SET IDENTITY_INSERT Oracle_Reporting_P2.svo.D_PAYMENT_TERM OFF;
GO

INSERT INTO Oracle_Reporting_P2.svo.D_PAYMENT_TERM
(
    PAYMENT_TERM_ID,
    PAYMENT_TERM_NAME,
    BZ_LOAD_DATE,
    SV_LOAD_DATE
)
SELECT
    src.PaymentTermHeaderTranslationTermId,
    src.PaymentTermHeaderTranslationName,
    CAST(src.AddDateTime AS DATE),
    CAST(GETDATE() AS DATE)
FROM DW_BronzeSilver_PROD.bzo.AP_PaymentTermHeaderTranslationExtractPVO src
WHERE NOT EXISTS
(
    SELECT 1
    FROM Oracle_Reporting_P2.svo.D_PAYMENT_TERM tgt
    WHERE tgt.PAYMENT_TERM_ID = src.PaymentTermHeaderTranslationTermId
);
GO
