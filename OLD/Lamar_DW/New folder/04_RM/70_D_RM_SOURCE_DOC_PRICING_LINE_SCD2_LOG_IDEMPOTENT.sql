USE [Oracle_Reporting_P2];
GO

/* =========================================================
   01_D_RM_SOURCE_DOC_PRICING_LINE_SCD2_LOG_IDEMPOTENT
   Target: svo.D_RM_SOURCE_DOC_PRICING_LINE
   Source: src.bzo_VRM_SourceDocLinePricingLinesPVO (synonym)
   ========================================================= */

IF OBJECT_ID('svo.D_RM_SOURCE_DOC_PRICING_LINE', 'U') IS NOT NULL
    DROP TABLE svo.D_RM_SOURCE_DOC_PRICING_LINE;
GO

SET ANSI_NULLS ON;
GO
SET QUOTED_IDENTIFIER ON;
GO

CREATE TABLE [svo].[D_RM_SOURCE_DOC_PRICING_LINE]
(
    RM_SOURCE_DOC_PRICING_LINE_SK   BIGINT IDENTITY(1,1) NOT NULL,

    SOURCE_DOCUMENT_LINE_ID         BIGINT       NOT NULL,

    BILL_TO_CUSTOMER_ID             BIGINT       NOT NULL,
    BILL_TO_CUSTOMER_SITE_ID        BIGINT       NOT NULL,
    INVENTORY_ORG_ID                BIGINT       NOT NULL,
    ITEM_ID                         BIGINT       NOT NULL,
    MEMO_LINE_ID                    BIGINT       NOT NULL,
    MEMO_LINE_NAME                  VARCHAR(50)  NOT NULL,
    MEMO_LINE_SEQ_ID                BIGINT       NOT NULL,
    SALESREP_ID                     BIGINT       NOT NULL,
    SALESREP_NAME                   VARCHAR(360) NOT NULL,

    SRC_ATTRIBUTE_CHAR_41           VARCHAR(150) NOT NULL,
    SRC_ATTRIBUTE_CHAR_42           VARCHAR(150) NOT NULL,
    SRC_ATTRIBUTE_CHAR_43           VARCHAR(150) NOT NULL,
    SRC_ATTRIBUTE_CHAR_44           VARCHAR(150) NOT NULL,
    SRC_ATTRIBUTE_CHAR_45           VARCHAR(150) NOT NULL,
    SRC_ATTRIBUTE_CHAR_46           VARCHAR(150) NOT NULL,
    SRC_ATTRIBUTE_CHAR_47           VARCHAR(150) NOT NULL,
    SRC_ATTRIBUTE_CHAR_48           VARCHAR(150) NOT NULL,
    SRC_ATTRIBUTE_CHAR_49           VARCHAR(150) NOT NULL,
    SRC_ATTRIBUTE_CHAR_50           VARCHAR(150) NOT NULL,
    SRC_ATTRIBUTE_CHAR_51           VARCHAR(150) NOT NULL,
    SRC_ATTRIBUTE_CHAR_52           VARCHAR(150) NOT NULL,
    SRC_ATTRIBUTE_CHAR_53           VARCHAR(150) NOT NULL,
    SRC_ATTRIBUTE_CHAR_54           VARCHAR(150) NOT NULL,
    SRC_ATTRIBUTE_CHAR_55           VARCHAR(150) NOT NULL,
    SRC_ATTRIBUTE_CHAR_56           VARCHAR(150) NOT NULL,

    SRC_ATTRIBUTE_DATE_1            DATE         NOT NULL,
    SRC_ATTRIBUTE_DATE_2            DATE         NOT NULL,

    SRC_ATTRIBUTE_NUMBER_12         DECIMAL(38,5) NOT NULL,

    ROW_HASH                        VARBINARY(32) NOT NULL,

    EFF_DATE                        DATE         NOT NULL,
    END_DATE                        DATE         NOT NULL,
    CRE_DATE                        DATETIME2(0) NOT NULL,
    UDT_DATE                        DATETIME2(0) NOT NULL,
    CURR_IND                        BIT          NOT NULL,

    BZ_LOAD_DATE                    DATE         NOT NULL,
    SV_LOAD_DATE                    DATE         NOT NULL,

    CONSTRAINT PK_D_RM_SOURCE_DOC_PRICING_LINE
        PRIMARY KEY CLUSTERED (RM_SOURCE_DOC_PRICING_LINE_SK ASC)
) ON [FG_SilverDim];
GO

/* One current row per business key */
CREATE UNIQUE NONCLUSTERED INDEX UX_D_RM_SOURCE_DOC_PRICING_LINE_CURR
ON svo.D_RM_SOURCE_DOC_PRICING_LINE (SOURCE_DOCUMENT_LINE_ID)
WHERE CURR_IND = 1
ON [FG_SilverDim];
GO

CREATE NONCLUSTERED INDEX IX_D_RM_SOURCE_DOC_PRICING_LINE_BK_EFF
ON svo.D_RM_SOURCE_DOC_PRICING_LINE (SOURCE_DOCUMENT_LINE_ID, EFF_DATE, END_DATE, CURR_IND)
ON [FG_SilverDim];
GO

/* Plug row */
SET IDENTITY_INSERT svo.D_RM_SOURCE_DOC_PRICING_LINE ON;
INSERT INTO svo.D_RM_SOURCE_DOC_PRICING_LINE
(
    RM_SOURCE_DOC_PRICING_LINE_SK,
    SOURCE_DOCUMENT_LINE_ID,
    BILL_TO_CUSTOMER_ID,
    BILL_TO_CUSTOMER_SITE_ID,
    INVENTORY_ORG_ID,
    ITEM_ID,
    MEMO_LINE_ID,
    MEMO_LINE_NAME,
    MEMO_LINE_SEQ_ID,
    SALESREP_ID,
    SALESREP_NAME,
    SRC_ATTRIBUTE_CHAR_41,
    SRC_ATTRIBUTE_CHAR_42,
    SRC_ATTRIBUTE_CHAR_43,
    SRC_ATTRIBUTE_CHAR_44,
    SRC_ATTRIBUTE_CHAR_45,
    SRC_ATTRIBUTE_CHAR_46,
    SRC_ATTRIBUTE_CHAR_47,
    SRC_ATTRIBUTE_CHAR_48,
    SRC_ATTRIBUTE_CHAR_49,
    SRC_ATTRIBUTE_CHAR_50,
    SRC_ATTRIBUTE_CHAR_51,
    SRC_ATTRIBUTE_CHAR_52,
    SRC_ATTRIBUTE_CHAR_53,
    SRC_ATTRIBUTE_CHAR_54,
    SRC_ATTRIBUTE_CHAR_55,
    SRC_ATTRIBUTE_CHAR_56,
    SRC_ATTRIBUTE_DATE_1,
    SRC_ATTRIBUTE_DATE_2,
    SRC_ATTRIBUTE_NUMBER_12,
    ROW_HASH,
    EFF_DATE,
    END_DATE,
    CRE_DATE,
    UDT_DATE,
    CURR_IND,
    BZ_LOAD_DATE,
    SV_LOAD_DATE
)
VALUES
(
    0,
    0,
    0,0,0,0,0,
    'Unknown',
    0,
    0,
    'Unknown',
    'Unknown','Unknown','Unknown','Unknown','Unknown','Unknown','Unknown','Unknown',
    'Unknown','Unknown','Unknown','Unknown','Unknown','Unknown','Unknown','Unknown',
    CAST('0001-01-01' AS DATE),
    CAST('0001-01-01' AS DATE),
    CAST(0 AS DECIMAL(38,5)),
    HASHBYTES('SHA2_256', CONVERT(VARBINARY(MAX),
        CONCAT(
            '0','|','0','|','0','|','0','|','0','|','0','|','Unknown','|','0','|','0','|','Unknown','|',
            'Unknown','|','Unknown','|','Unknown','|','Unknown','|','Unknown','|','Unknown','|','Unknown','|','Unknown','|',
            'Unknown','|','Unknown','|','Unknown','|','Unknown','|','Unknown','|','Unknown','|','Unknown','|','Unknown','|',
            CONVERT(CHAR(10), CAST('0001-01-01' AS DATE), 120),'|',
            CONVERT(CHAR(10), CAST('0001-01-01' AS DATE), 120),'|',
            '0'
        )
    )),
    CAST('0001-01-01' AS DATE),
    CAST('9999-12-31' AS DATE),
    SYSUTCDATETIME(),
    SYSUTCDATETIME(),
    1,
    CAST('0001-01-01' AS DATE),
    CAST('0001-01-01' AS DATE)
);
SET IDENTITY_INSERT svo.D_RM_SOURCE_DOC_PRICING_LINE OFF;
GO

/* =========================================================
   Procedure: svo.usp_Load_D_RM_SOURCE_DOC_PRICING_LINE_SCD2
   ========================================================= */
IF OBJECT_ID('svo.usp_Load_D_RM_SOURCE_DOC_PRICING_LINE_SCD2', 'P') IS NOT NULL
    DROP PROCEDURE svo.usp_Load_D_RM_SOURCE_DOC_PRICING_LINE_SCD2;
GO

CREATE PROCEDURE svo.usp_Load_D_RM_SOURCE_DOC_PRICING_LINE_SCD2
    @FullReload BIT = 0,
    @AsOfDate   DATE = NULL
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    DECLARE
        @ProcName SYSNAME = QUOTENAME(OBJECT_SCHEMA_NAME(@@PROCID)) + '.' + QUOTENAME(OBJECT_NAME(@@PROCID)),
        @Target   SYSNAME = 'svo.D_RM_SOURCE_DOC_PRICING_LINE',
        @RunId    BIGINT,
        @NowUtc   DATETIME2(0) = SYSUTCDATETIME(),
        @AsOf     DATE = COALESCE(@AsOfDate, CAST(GETDATE() AS DATE)),
        @RowsInserted INT = 0,
        @RowsExpired  INT = 0;

    INSERT INTO etl.ETL_RUN (PROC_NAME, TARGET_OBJECT, ASOF_DATE, START_DTTM, STATUS)
    VALUES (@ProcName, @Target, @AsOf, SYSDATETIME(), 'STARTED');

    SET @RunId = SCOPE_IDENTITY();

    BEGIN TRY
        BEGIN TRAN;

        IF @FullReload = 1
        BEGIN
            DELETE FROM svo.D_RM_SOURCE_DOC_PRICING_LINE WHERE RM_SOURCE_DOC_PRICING_LINE_SK <> 0;
        END

        /* Stage source with null-handling and ROW_HASH computed inline */
        IF OBJECT_ID('tempdb..#Src', 'U') IS NOT NULL DROP TABLE #Src;

        SELECT
            SOURCE_DOCUMENT_LINE_ID   = CAST(COALESCE(P.SourceDocLinesDocumentLineId, 0) AS BIGINT),

            BILL_TO_CUSTOMER_ID       = CAST(COALESCE(P.SourceDocLinesBillToCustomerId, 0) AS BIGINT),
            BILL_TO_CUSTOMER_SITE_ID  = CAST(COALESCE(P.SourceDocLinesBillToCustomerSiteId, 0) AS BIGINT),
            INVENTORY_ORG_ID          = CAST(COALESCE(P.SourceDocLinesInventoryOrgId, 0) AS BIGINT),
            ITEM_ID                   = CAST(COALESCE(P.SourceDocLinesItemId, 0) AS BIGINT),
            MEMO_LINE_ID              = CAST(COALESCE(P.SourceDocLinesMemoLineId, 0) AS BIGINT),
            MEMO_LINE_NAME            = LEFT(LTRIM(RTRIM(COALESCE(P.SourceDocLinesMemoLineName, 'Unknown'))), 50),
            MEMO_LINE_SEQ_ID          = CAST(COALESCE(P.SourceDocLinesMemoLineSeqId, 0) AS BIGINT),
            SALESREP_ID               = CAST(COALESCE(P.SourceDocLinesSalesrepId, 0) AS BIGINT),
            SALESREP_NAME             = LEFT(LTRIM(RTRIM(COALESCE(P.SourceDocLinesSalesrepName, 'Unknown'))), 360),

            SRC_ATTRIBUTE_CHAR_41     = LEFT(LTRIM(RTRIM(COALESCE(P.SourceDocLinesSrcAttributeChar41, 'Unknown'))), 150),
            SRC_ATTRIBUTE_CHAR_42     = LEFT(LTRIM(RTRIM(COALESCE(P.SourceDocLinesSrcAttributeChar42, 'Unknown'))), 150),
            SRC_ATTRIBUTE_CHAR_43     = LEFT(LTRIM(RTRIM(COALESCE(P.SourceDocLinesSrcAttributeChar43, 'Unknown'))), 150),
            SRC_ATTRIBUTE_CHAR_44     = LEFT(LTRIM(RTRIM(COALESCE(P.SourceDocLinesSrcAttributeChar44, 'Unknown'))), 150),
            SRC_ATTRIBUTE_CHAR_45     = LEFT(LTRIM(RTRIM(COALESCE(P.SourceDocLinesSrcAttributeChar45, 'Unknown'))), 150),
            SRC_ATTRIBUTE_CHAR_46     = LEFT(LTRIM(RTRIM(COALESCE(P.SourceDocLinesSrcAttributeChar46, 'Unknown'))), 150),
            SRC_ATTRIBUTE_CHAR_47     = LEFT(LTRIM(RTRIM(COALESCE(P.SourceDocLinesSrcAttributeChar47, 'Unknown'))), 150),
            SRC_ATTRIBUTE_CHAR_48     = LEFT(LTRIM(RTRIM(COALESCE(P.SourceDocLinesSrcAttributeChar48, 'Unknown'))), 150),
            SRC_ATTRIBUTE_CHAR_49     = LEFT(LTRIM(RTRIM(COALESCE(P.SourceDocLinesSrcAttributeChar49, 'Unknown'))), 150),
            SRC_ATTRIBUTE_CHAR_50     = LEFT(LTRIM(RTRIM(COALESCE(P.SourceDocLinesSrcAttributeChar50, 'Unknown'))), 150),
            SRC_ATTRIBUTE_CHAR_51     = LEFT(LTRIM(RTRIM(COALESCE(P.SourceDocLinesSrcAttributeChar51, 'Unknown'))), 150),
            SRC_ATTRIBUTE_CHAR_52     = LEFT(LTRIM(RTRIM(COALESCE(P.SourceDocLinesSrcAttributeChar52, 'Unknown'))), 150),
            SRC_ATTRIBUTE_CHAR_53     = LEFT(LTRIM(RTRIM(COALESCE(P.SourceDocLinesSrcAttributeChar53, 'Unknown'))), 150),
            SRC_ATTRIBUTE_CHAR_54     = LEFT(LTRIM(RTRIM(COALESCE(P.SourceDocLinesSrcAttributeChar54, 'Unknown'))), 150),
            SRC_ATTRIBUTE_CHAR_55     = LEFT(LTRIM(RTRIM(COALESCE(P.SourceDocLinesSrcAttributeChar55, 'Unknown'))), 150),
            SRC_ATTRIBUTE_CHAR_56     = LEFT(LTRIM(RTRIM(COALESCE(P.SourceDocLinesSrcAttributeChar56, 'Unknown'))), 150),

            SRC_ATTRIBUTE_DATE_1      = CAST(COALESCE(P.SourceDocLinesSrcAttributeDate1, CAST('0001-01-01' AS DATE)) AS DATE),
            SRC_ATTRIBUTE_DATE_2      = CAST(COALESCE(P.SourceDocLinesSrcAttributeDate2, CAST('0001-01-01' AS DATE)) AS DATE),

            SRC_ATTRIBUTE_NUMBER_12   = CAST(COALESCE(P.SourceDocLinesSrcAttributeNumber12, CAST(0 AS DECIMAL(38,5))) AS DECIMAL(38,5)),

            BZ_LOAD_DATE              = COALESCE(CAST(P.AddDateTime AS DATE), CAST(GETDATE() AS DATE)),
            SV_LOAD_DATE              = CAST(GETDATE() AS DATE),

            ROW_HASH = HASHBYTES('SHA2_256', CONVERT(VARBINARY(MAX),
                CONCAT(
                    CAST(COALESCE(P.SourceDocLinesDocumentLineId, 0) AS BIGINT),'|',
                    CAST(COALESCE(P.SourceDocLinesBillToCustomerId, 0) AS BIGINT),'|',
                    CAST(COALESCE(P.SourceDocLinesBillToCustomerSiteId, 0) AS BIGINT),'|',
                    CAST(COALESCE(P.SourceDocLinesInventoryOrgId, 0) AS BIGINT),'|',
                    CAST(COALESCE(P.SourceDocLinesItemId, 0) AS BIGINT),'|',
                    CAST(COALESCE(P.SourceDocLinesMemoLineId, 0) AS BIGINT),'|',
                    LEFT(LTRIM(RTRIM(COALESCE(P.SourceDocLinesMemoLineName, 'Unknown'))), 50),'|',
                    CAST(COALESCE(P.SourceDocLinesMemoLineSeqId, 0) AS BIGINT),'|',
                    CAST(COALESCE(P.SourceDocLinesSalesrepId, 0) AS BIGINT),'|',
                    LEFT(LTRIM(RTRIM(COALESCE(P.SourceDocLinesSalesrepName, 'Unknown'))), 360),'|',
                    LEFT(LTRIM(RTRIM(COALESCE(P.SourceDocLinesSrcAttributeChar41, 'Unknown'))), 150),'|',
                    LEFT(LTRIM(RTRIM(COALESCE(P.SourceDocLinesSrcAttributeChar42, 'Unknown'))), 150),'|',
                    LEFT(LTRIM(RTRIM(COALESCE(P.SourceDocLinesSrcAttributeChar43, 'Unknown'))), 150),'|',
                    LEFT(LTRIM(RTRIM(COALESCE(P.SourceDocLinesSrcAttributeChar44, 'Unknown'))), 150),'|',
                    LEFT(LTRIM(RTRIM(COALESCE(P.SourceDocLinesSrcAttributeChar45, 'Unknown'))), 150),'|',
                    LEFT(LTRIM(RTRIM(COALESCE(P.SourceDocLinesSrcAttributeChar46, 'Unknown'))), 150),'|',
                    LEFT(LTRIM(RTRIM(COALESCE(P.SourceDocLinesSrcAttributeChar47, 'Unknown'))), 150),'|',
                    LEFT(LTRIM(RTRIM(COALESCE(P.SourceDocLinesSrcAttributeChar48, 'Unknown'))), 150),'|',
                    LEFT(LTRIM(RTRIM(COALESCE(P.SourceDocLinesSrcAttributeChar49, 'Unknown'))), 150),'|',
                    LEFT(LTRIM(RTRIM(COALESCE(P.SourceDocLinesSrcAttributeChar50, 'Unknown'))), 150),'|',
                    LEFT(LTRIM(RTRIM(COALESCE(P.SourceDocLinesSrcAttributeChar51, 'Unknown'))), 150),'|',
                    LEFT(LTRIM(RTRIM(COALESCE(P.SourceDocLinesSrcAttributeChar52, 'Unknown'))), 150),'|',
                    LEFT(LTRIM(RTRIM(COALESCE(P.SourceDocLinesSrcAttributeChar53, 'Unknown'))), 150),'|',
                    LEFT(LTRIM(RTRIM(COALESCE(P.SourceDocLinesSrcAttributeChar54, 'Unknown'))), 150),'|',
                    LEFT(LTRIM(RTRIM(COALESCE(P.SourceDocLinesSrcAttributeChar55, 'Unknown'))), 150),'|',
                    LEFT(LTRIM(RTRIM(COALESCE(P.SourceDocLinesSrcAttributeChar56, 'Unknown'))), 150),'|',
                    CONVERT(CHAR(10), CAST(COALESCE(P.SourceDocLinesSrcAttributeDate1, CAST('0001-01-01' AS DATE)) AS DATE), 120),'|',
                    CONVERT(CHAR(10), CAST(COALESCE(P.SourceDocLinesSrcAttributeDate2, CAST('0001-01-01' AS DATE)) AS DATE), 120),'|',
                    CONVERT(VARCHAR(50), CAST(COALESCE(P.SourceDocLinesSrcAttributeNumber12, CAST(0 AS DECIMAL(38,5))) AS DECIMAL(38,5)))
                )
            ))
        INTO #Src
        FROM src.bzo_VRM_SourceDocLinePricingLinesPVO AS P
        WHERE P.SourceDocLinesDocumentLineId IS NOT NULL;

        CREATE UNIQUE CLUSTERED INDEX CX_Src ON #Src (SOURCE_DOCUMENT_LINE_ID);

        /* Expire changed rows */
        UPDATE T
            SET END_DATE  = DATEADD(DAY, -1, @AsOf),
                CURR_IND  = 0,
                UDT_DATE  = @NowUtc,
                SV_LOAD_DATE = CAST(GETDATE() AS DATE)
        FROM svo.D_RM_SOURCE_DOC_PRICING_LINE AS T
        JOIN #Src AS S
            ON S.SOURCE_DOCUMENT_LINE_ID = T.SOURCE_DOCUMENT_LINE_ID
        WHERE T.CURR_IND = 1
          AND T.ROW_HASH <> S.ROW_HASH;

        SET @RowsExpired += @@ROWCOUNT;

        /* Expire missing rows (optional, but keeps dimension accurate) */
        UPDATE T
            SET END_DATE  = DATEADD(DAY, -1, @AsOf),
                CURR_IND  = 0,
                UDT_DATE  = @NowUtc,
                SV_LOAD_DATE = CAST(GETDATE() AS DATE)
        FROM svo.D_RM_SOURCE_DOC_PRICING_LINE AS T
        LEFT JOIN #Src AS S
            ON S.SOURCE_DOCUMENT_LINE_ID = T.SOURCE_DOCUMENT_LINE_ID
        WHERE T.CURR_IND = 1
          AND T.SOURCE_DOCUMENT_LINE_ID <> 0
          AND S.SOURCE_DOCUMENT_LINE_ID IS NULL;

        SET @RowsExpired += @@ROWCOUNT;

        /* Insert new and changed rows */
        INSERT INTO svo.D_RM_SOURCE_DOC_PRICING_LINE
        (
            SOURCE_DOCUMENT_LINE_ID,
            BILL_TO_CUSTOMER_ID,
            BILL_TO_CUSTOMER_SITE_ID,
            INVENTORY_ORG_ID,
            ITEM_ID,
            MEMO_LINE_ID,
            MEMO_LINE_NAME,
            MEMO_LINE_SEQ_ID,
            SALESREP_ID,
            SALESREP_NAME,
            SRC_ATTRIBUTE_CHAR_41,
            SRC_ATTRIBUTE_CHAR_42,
            SRC_ATTRIBUTE_CHAR_43,
            SRC_ATTRIBUTE_CHAR_44,
            SRC_ATTRIBUTE_CHAR_45,
            SRC_ATTRIBUTE_CHAR_46,
            SRC_ATTRIBUTE_CHAR_47,
            SRC_ATTRIBUTE_CHAR_48,
            SRC_ATTRIBUTE_CHAR_49,
            SRC_ATTRIBUTE_CHAR_50,
            SRC_ATTRIBUTE_CHAR_51,
            SRC_ATTRIBUTE_CHAR_52,
            SRC_ATTRIBUTE_CHAR_53,
            SRC_ATTRIBUTE_CHAR_54,
            SRC_ATTRIBUTE_CHAR_55,
            SRC_ATTRIBUTE_CHAR_56,
            SRC_ATTRIBUTE_DATE_1,
            SRC_ATTRIBUTE_DATE_2,
            SRC_ATTRIBUTE_NUMBER_12,
            ROW_HASH,
            EFF_DATE,
            END_DATE,
            CRE_DATE,
            UDT_DATE,
            CURR_IND,
            BZ_LOAD_DATE,
            SV_LOAD_DATE
        )
        SELECT
            S.SOURCE_DOCUMENT_LINE_ID,
            S.BILL_TO_CUSTOMER_ID,
            S.BILL_TO_CUSTOMER_SITE_ID,
            S.INVENTORY_ORG_ID,
            S.ITEM_ID,
            S.MEMO_LINE_ID,
            S.MEMO_LINE_NAME,
            S.MEMO_LINE_SEQ_ID,
            S.SALESREP_ID,
            S.SALESREP_NAME,
            S.SRC_ATTRIBUTE_CHAR_41,
            S.SRC_ATTRIBUTE_CHAR_42,
            S.SRC_ATTRIBUTE_CHAR_43,
            S.SRC_ATTRIBUTE_CHAR_44,
            S.SRC_ATTRIBUTE_CHAR_45,
            S.SRC_ATTRIBUTE_CHAR_46,
            S.SRC_ATTRIBUTE_CHAR_47,
            S.SRC_ATTRIBUTE_CHAR_48,
            S.SRC_ATTRIBUTE_CHAR_49,
            S.SRC_ATTRIBUTE_CHAR_50,
            S.SRC_ATTRIBUTE_CHAR_51,
            S.SRC_ATTRIBUTE_CHAR_52,
            S.SRC_ATTRIBUTE_CHAR_53,
            S.SRC_ATTRIBUTE_CHAR_54,
            S.SRC_ATTRIBUTE_CHAR_55,
            S.SRC_ATTRIBUTE_CHAR_56,
            S.SRC_ATTRIBUTE_DATE_1,
            S.SRC_ATTRIBUTE_DATE_2,
            S.SRC_ATTRIBUTE_NUMBER_12,
            S.ROW_HASH,
            @AsOf,
            CAST('9999-12-31' AS DATE),
            @NowUtc,
            @NowUtc,
            1,
            S.BZ_LOAD_DATE,
            S.SV_LOAD_DATE
        FROM #Src AS S
        LEFT JOIN svo.D_RM_SOURCE_DOC_PRICING_LINE AS T
            ON T.SOURCE_DOCUMENT_LINE_ID = S.SOURCE_DOCUMENT_LINE_ID
           AND T.CURR_IND = 1
        WHERE T.SOURCE_DOCUMENT_LINE_ID IS NULL
           OR T.ROW_HASH <> S.ROW_HASH;

        SET @RowsInserted = @@ROWCOUNT;

        UPDATE etl.ETL_RUN
            SET END_DTTM      = SYSDATETIME(),
                STATUS        = 'SUCCEEDED',
                ROW_INSERTED  = @RowsInserted,
                ROW_EXPIRED   = @RowsExpired
        WHERE RUN_ID = @RunId;

        COMMIT;
    END TRY
    BEGIN CATCH
        IF @@TRANCOUNT > 0 ROLLBACK;

        UPDATE etl.ETL_RUN
            SET END_DTTM = SYSDATETIME(),
                STATUS   = 'FAILED',
                ERROR_MESSAGE = LEFT(ERROR_MESSAGE(), 4000)
        WHERE RUN_ID = @RunId;

        THROW;
    END CATCH
END
GO