/* =====================================================================================
   D_RM_PERF_OBLIGATION_LINE (SCD2) + ETL_RUN + Idempotent
   Source: src.bzo_VRM_PerfObligationLinesPVO
===================================================================================== */

IF OBJECT_ID('svo.D_RM_PERF_OBLIGATION_LINE','U') IS NOT NULL
    DROP TABLE svo.D_RM_PERF_OBLIGATION_LINE;
GO

CREATE TABLE svo.D_RM_PERF_OBLIGATION_LINE
(
    RM_PERF_OBLIGATION_LINE_SK     BIGINT IDENTITY(1,1) NOT NULL
        CONSTRAINT PK_D_RM_PERF_OBLIGATION_LINE PRIMARY KEY CLUSTERED,

    PERF_OBLIGATION_LINE_ID        BIGINT       NOT NULL,  -- NK

    COMMENTS                       VARCHAR(1000)  NOT NULL,
    CONTR_CUR_NET_CONSIDER_AMT     DECIMAL(29,4)  NOT NULL,
    CREATED_BY                     VARCHAR(64)    NOT NULL,
    DOCUMENT_LINE_ID               BIGINT         NOT NULL,
    ENTERED_CUR_NET_CONSIDER_AMT   DECIMAL(29,4)  NOT NULL,
    ENTERED_CUR_RECOG_REV_AMT      DECIMAL(29,4)  NOT NULL,
    LAST_UPDATE_DATE               DATETIME       NOT NULL,
    LAST_UPDATED_BY                VARCHAR(64)    NOT NULL,
    LAST_UPDATE_LOGIN              VARCHAR(32)    NOT NULL,
    NET_LINE_AMT                   DECIMAL(29,4)  NOT NULL,
    PAYMENT_AMOUNT                 DECIMAL(29,4)  NOT NULL,
    PERF_OBLIGATION_ID             BIGINT         NOT NULL,
    PERF_OBLIGATION_LINE_NUMBER    BIGINT         NOT NULL,
    REVENUE_END_DATE               DATE           NOT NULL,
    REVENUE_START_DATE             DATE           NOT NULL,
    SATISFACTION_BASE_PROPORTION   BIGINT         NOT NULL,
    SOURCE_DOCUMENT_LINE_ID        BIGINT         NOT NULL,

    BZ_LOAD_DATE                   DATE           NOT NULL,
    SV_LOAD_DATE                   DATE           NOT NULL,

    -- SCD2
    EFF_DATE                       DATE           NOT NULL,
    END_DATE                       DATE           NOT NULL,
    CRE_DATE                       DATETIME2(0)   NOT NULL,
    UDT_DATE                       DATETIME2(0)   NOT NULL,
    CURR_IND                       BIT            NOT NULL,

    ROW_HASH                       VARBINARY(32)  NOT NULL
) ON [FG_SilverDim];
GO

-- One current row per NK
CREATE UNIQUE NONCLUSTERED INDEX UX_D_RM_PERF_OBLIGATION_LINE_NK_CURR
ON svo.D_RM_PERF_OBLIGATION_LINE (PERF_OBLIGATION_LINE_ID)
WHERE CURR_IND = 1
ON [FG_SilverDim];
GO

-- Plug row
SET IDENTITY_INSERT svo.D_RM_PERF_OBLIGATION_LINE ON;

INSERT INTO svo.D_RM_PERF_OBLIGATION_LINE
(
    RM_PERF_OBLIGATION_LINE_SK,
    PERF_OBLIGATION_LINE_ID,
    COMMENTS, CONTR_CUR_NET_CONSIDER_AMT, CREATED_BY, DOCUMENT_LINE_ID,
    ENTERED_CUR_NET_CONSIDER_AMT, ENTERED_CUR_RECOG_REV_AMT,
    LAST_UPDATE_DATE, LAST_UPDATED_BY, LAST_UPDATE_LOGIN,
    NET_LINE_AMT, PAYMENT_AMOUNT, PERF_OBLIGATION_ID, PERF_OBLIGATION_LINE_NUMBER,
    REVENUE_END_DATE, REVENUE_START_DATE, SATISFACTION_BASE_PROPORTION, SOURCE_DOCUMENT_LINE_ID,
    BZ_LOAD_DATE, SV_LOAD_DATE,
    EFF_DATE, END_DATE, CRE_DATE, UDT_DATE, CURR_IND,
    ROW_HASH
)
VALUES
(
    0,
    -1,
    'Unknown', 0, 'Unknown', -1,
    0, 0,
    CAST('1900-01-01' AS datetime), 'Unknown', 'Unknown',
    0, 0, -1, -1,
    CAST('0001-01-01' AS date), CAST('0001-01-01' AS date), 0, -1,
    CAST('0001-01-01' AS date), CAST(GETDATE() AS date),
    CAST('0001-01-01' AS date), CAST('9999-12-31' AS date), SYSUTCDATETIME(), SYSUTCDATETIME(), 1,
    HASHBYTES('SHA2_256', CONVERT(varbinary(max), ''))
);

SET IDENTITY_INSERT svo.D_RM_PERF_OBLIGATION_LINE OFF;
GO


CREATE OR ALTER PROCEDURE svo.usp_Load_D_RM_PERF_OBLIGATION_LINE_SCD2
      @FullReload bit = 0
    , @Debug      bit = 0
    , @AsOfDate   date = NULL
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    DECLARE @proc sysname = QUOTENAME(OBJECT_SCHEMA_NAME(@@PROCID)) + '.' + QUOTENAME(OBJECT_NAME(@@PROCID));
    DECLARE @run_id bigint;
    DECLARE @start_dttm datetime2(0) = SYSDATETIME();
    DECLARE @asof date = COALESCE(@AsOfDate, CAST(GETDATE() AS date));

    INSERT INTO etl.ETL_RUN (PROC_NAME, TARGET_OBJECT, ASOF_DATE, START_DTTM, STATUS)
    VALUES (@proc, 'svo.D_RM_PERF_OBLIGATION_LINE', @asof, @start_dttm, 'STARTED');

    SET @run_id = SCOPE_IDENTITY();

    BEGIN TRY
        IF @Debug = 1
            PRINT 'Starting ' + @proc + ' | RUN_ID=' + CONVERT(varchar(30), @run_id);

        BEGIN TRAN;

        IF @FullReload = 1
        BEGIN
            IF @Debug = 1
                PRINT 'FullReload requested. Rebuilding svo.D_RM_PERF_OBLIGATION_LINE';

            TRUNCATE TABLE svo.D_RM_PERF_OBLIGATION_LINE;

            -- reinsert plug
            SET IDENTITY_INSERT svo.D_RM_PERF_OBLIGATION_LINE ON;

            INSERT INTO svo.D_RM_PERF_OBLIGATION_LINE
            (
                RM_PERF_OBLIGATION_LINE_SK,
                PERF_OBLIGATION_LINE_ID,
                COMMENTS, CONTR_CUR_NET_CONSIDER_AMT, CREATED_BY, DOCUMENT_LINE_ID,
                ENTERED_CUR_NET_CONSIDER_AMT, ENTERED_CUR_RECOG_REV_AMT,
                LAST_UPDATE_DATE, LAST_UPDATED_BY, LAST_UPDATE_LOGIN,
                NET_LINE_AMT, PAYMENT_AMOUNT, PERF_OBLIGATION_ID, PERF_OBLIGATION_LINE_NUMBER,
                REVENUE_END_DATE, REVENUE_START_DATE, SATISFACTION_BASE_PROPORTION, SOURCE_DOCUMENT_LINE_ID,
                BZ_LOAD_DATE, SV_LOAD_DATE,
                EFF_DATE, END_DATE, CRE_DATE, UDT_DATE, CURR_IND,
                ROW_HASH
            )
            VALUES
            (
                0,
                -1,
                'Unknown', 0, 'Unknown', -1,
                0, 0,
                CAST('1900-01-01' AS datetime), 'Unknown', 'Unknown',
                0, 0, -1, -1,
                CAST('0001-01-01' AS date), CAST('0001-01-01' AS date), 0, -1,
                CAST('0001-01-01' AS date), CAST(GETDATE() AS date),
                CAST('0001-01-01' AS date), CAST('9999-12-31' AS date), SYSUTCDATETIME(), SYSUTCDATETIME(), 1,
                HASHBYTES('SHA2_256', CONVERT(varbinary(max), ''))
            );

            SET IDENTITY_INSERT svo.D_RM_PERF_OBLIGATION_LINE OFF;
        END

        DROP TABLE IF EXISTS #Src;
        DROP TABLE IF EXISTS #ToExpire;
        DROP TABLE IF EXISTS #ToInsert;

        SELECT
              PERF_OBLIGATION_LINE_ID      = TRY_CONVERT(bigint, L.PerfObligationLineId)

            , COMMENTS                     = LEFT(COALESCE(NULLIF(L.PerfObligationLinesComments,''), 'Unknown'), 1000)
            , CONTR_CUR_NET_CONSIDER_AMT   = COALESCE(TRY_CONVERT(decimal(29,4), L.PerfObligationLinesContrCurNetConsiderAmt), 0)
            , CREATED_BY                   = LEFT(COALESCE(NULLIF(L.PerfObligationLinesCreatedBy,''), 'Unknown'), 64)
            , DOCUMENT_LINE_ID             = COALESCE(TRY_CONVERT(bigint, L.PerfObligationLinesDocumentLineId), -1)
            , ENTERED_CUR_NET_CONSIDER_AMT = COALESCE(TRY_CONVERT(decimal(29,4), L.PerfObligationLinesEnteredCurNetConsiderAmt), 0)
            , ENTERED_CUR_RECOG_REV_AMT    = COALESCE(TRY_CONVERT(decimal(29,4), L.PerfObligationLinesEnteredCurRecogRevAmt), 0)

            , LAST_UPDATE_DATE             = COALESCE(TRY_CONVERT(datetime, L.PerfObligationLinesLastUpdateDate), CAST('1900-01-01' AS datetime))
            , LAST_UPDATED_BY              = LEFT(COALESCE(NULLIF(L.PerfObligationLinesLastUpdatedBy,''), 'Unknown'), 64)
            , LAST_UPDATE_LOGIN            = LEFT(COALESCE(NULLIF(L.PerfObligationLinesLastUpdateLogin,''), 'Unknown'), 32)

            , NET_LINE_AMT                 = COALESCE(TRY_CONVERT(decimal(29,4), L.PerfObligationLinesNetLineAmt), 0)
            , PAYMENT_AMOUNT               = COALESCE(TRY_CONVERT(decimal(29,4), L.PerfObligationLinesPaymentAmount), 0)
            , PERF_OBLIGATION_ID           = COALESCE(TRY_CONVERT(bigint, L.PerfObligationLinesPerfObligationId), 0)
            , PERF_OBLIGATION_LINE_NUMBER  = COALESCE(TRY_CONVERT(bigint, L.PerfObligationLinesPerfObligationLineNumber), 0)

            , REVENUE_START_DATE           = COALESCE(TRY_CONVERT(date, L.PerfObligationLinesRevenueStartDate), CAST('0001-01-01' AS date))
            , REVENUE_END_DATE             = COALESCE(TRY_CONVERT(date, L.PerfObligationLinesRevenueEndDate), CAST('0001-01-01' AS date))

            , SATISFACTION_BASE_PROPORTION =
                COALESCE(
                    TRY_CONVERT(bigint, L.PerfObligationLinesSatisfactionBaseProportion),
                    CASE
                        WHEN COALESCE(TRY_CONVERT(date, L.PerfObligationLinesRevenueStartDate), CAST('0001-01-01' AS date))
                           <= COALESCE(TRY_CONVERT(date, L.PerfObligationLinesRevenueEndDate),   CAST('0001-01-01' AS date))
                        THEN DATEDIFF(day,
                              COALESCE(TRY_CONVERT(date, L.PerfObligationLinesRevenueStartDate), CAST('0001-01-01' AS date)),
                              COALESCE(TRY_CONVERT(date, L.PerfObligationLinesRevenueEndDate),   CAST('0001-01-01' AS date))
                             ) + 1
                        ELSE 0
                    END
                )

            , SOURCE_DOCUMENT_LINE_ID      = COALESCE(TRY_CONVERT(bigint, L.SourceDocLinesDocumentLineId), -1)

            , BZ_LOAD_DATE                 = COALESCE(CAST(L.AddDateTime AS date), CAST(GETDATE() AS date))
            , SV_LOAD_DATE                 = CAST(GETDATE() AS date)

            , ROW_HASH = HASHBYTES
              (
                'SHA2_256',
                CONVERT(varbinary(max),
                    CONCAT(
                        COALESCE(CONVERT(varchar(30), TRY_CONVERT(bigint, L.PerfObligationLineId)), ''),
                        '|', LEFT(COALESCE(NULLIF(L.PerfObligationLinesComments,''), 'Unknown'), 1000),
                        '|', COALESCE(CONVERT(varchar(50), TRY_CONVERT(decimal(29,4), L.PerfObligationLinesContrCurNetConsiderAmt)), '0'),
                        '|', LEFT(COALESCE(NULLIF(L.PerfObligationLinesCreatedBy,''), 'Unknown'), 64),
                        '|', COALESCE(CONVERT(varchar(30), TRY_CONVERT(bigint, L.PerfObligationLinesDocumentLineId)), '-1'),
                        '|', COALESCE(CONVERT(varchar(50), TRY_CONVERT(decimal(29,4), L.PerfObligationLinesEnteredCurNetConsiderAmt)), '0'),
                        '|', COALESCE(CONVERT(varchar(50), TRY_CONVERT(decimal(29,4), L.PerfObligationLinesEnteredCurRecogRevAmt)), '0'),
                        '|', COALESCE(CONVERT(varchar(30), TRY_CONVERT(datetime, L.PerfObligationLinesLastUpdateDate), 126), '1900-01-01T00:00:00'),
                        '|', LEFT(COALESCE(NULLIF(L.PerfObligationLinesLastUpdatedBy,''), 'Unknown'), 64),
                        '|', LEFT(COALESCE(NULLIF(L.PerfObligationLinesLastUpdateLogin,''), 'Unknown'), 32),
                        '|', COALESCE(CONVERT(varchar(50), TRY_CONVERT(decimal(29,4), L.PerfObligationLinesNetLineAmt)), '0'),
                        '|', COALESCE(CONVERT(varchar(50), TRY_CONVERT(decimal(29,4), L.PerfObligationLinesPaymentAmount)), '0'),
                        '|', COALESCE(CONVERT(varchar(30), TRY_CONVERT(bigint, L.PerfObligationLinesPerfObligationId)), '0'),
                        '|', COALESCE(CONVERT(varchar(30), TRY_CONVERT(bigint, L.PerfObligationLinesPerfObligationLineNumber)), '0'),
                        '|', COALESCE(CONVERT(char(10), TRY_CONVERT(date, L.PerfObligationLinesRevenueStartDate), 120), '0001-01-01'),
                        '|', COALESCE(CONVERT(char(10), TRY_CONVERT(date, L.PerfObligationLinesRevenueEndDate), 120), '0001-01-01'),
                        '|', COALESCE(CONVERT(varchar(30), TRY_CONVERT(bigint, L.PerfObligationLinesSatisfactionBaseProportion)), ''),
                        '|', COALESCE(CONVERT(varchar(30), TRY_CONVERT(bigint, L.SourceDocLinesDocumentLineId)), '-1')
                    )
                )
              )
        INTO #Src
        FROM src.bzo_VRM_PerfObligationLinesPVO L
        WHERE TRY_CONVERT(bigint, L.PerfObligationLineId) IS NOT NULL;

        -- de-dupe to 1 row per NK
        ;WITH d AS
        (
            SELECT *,
                   rn = ROW_NUMBER() OVER
                        (PARTITION BY PERF_OBLIGATION_LINE_ID
                         ORDER BY BZ_LOAD_DATE DESC, LAST_UPDATE_DATE DESC)
            FROM #Src
        )
        SELECT *
        INTO #ToInsert
        FROM d
        WHERE rn = 1;

        -- Identify changed NKs vs current
        SELECT
            T.RM_PERF_OBLIGATION_LINE_SK
        INTO #ToExpire
        FROM svo.D_RM_PERF_OBLIGATION_LINE T
        JOIN #ToInsert S
          ON S.PERF_OBLIGATION_LINE_ID = T.PERF_OBLIGATION_LINE_ID
        WHERE T.CURR_IND = 1
          AND T.ROW_HASH <> S.ROW_HASH;

        -- Expire changed current rows
        UPDATE T
            SET END_DATE = DATEADD(day, -1, @asof),
                CURR_IND = 0,
                UDT_DATE = SYSUTCDATETIME()
        FROM svo.D_RM_PERF_OBLIGATION_LINE T
        JOIN #ToExpire E
          ON E.RM_PERF_OBLIGATION_LINE_SK = T.RM_PERF_OBLIGATION_LINE_SK;

        DECLARE @row_expired int = @@ROWCOUNT;

        -- Insert new rows for: new NKs OR changed NKs
        INSERT INTO svo.D_RM_PERF_OBLIGATION_LINE
        (
            PERF_OBLIGATION_LINE_ID,
            COMMENTS, CONTR_CUR_NET_CONSIDER_AMT, CREATED_BY, DOCUMENT_LINE_ID,
            ENTERED_CUR_NET_CONSIDER_AMT, ENTERED_CUR_RECOG_REV_AMT,
            LAST_UPDATE_DATE, LAST_UPDATED_BY, LAST_UPDATE_LOGIN,
            NET_LINE_AMT, PAYMENT_AMOUNT, PERF_OBLIGATION_ID, PERF_OBLIGATION_LINE_NUMBER,
            REVENUE_END_DATE, REVENUE_START_DATE, SATISFACTION_BASE_PROPORTION, SOURCE_DOCUMENT_LINE_ID,
            BZ_LOAD_DATE, SV_LOAD_DATE,
            EFF_DATE, END_DATE, CRE_DATE, UDT_DATE, CURR_IND,
            ROW_HASH
        )
        SELECT
            S.PERF_OBLIGATION_LINE_ID,
            S.COMMENTS, S.CONTR_CUR_NET_CONSIDER_AMT, S.CREATED_BY, S.DOCUMENT_LINE_ID,
            S.ENTERED_CUR_NET_CONSIDER_AMT, S.ENTERED_CUR_RECOG_REV_AMT,
            S.LAST_UPDATE_DATE, S.LAST_UPDATED_BY, S.LAST_UPDATE_LOGIN,
            S.NET_LINE_AMT, S.PAYMENT_AMOUNT, S.PERF_OBLIGATION_ID, S.PERF_OBLIGATION_LINE_NUMBER,
            S.REVENUE_END_DATE, S.REVENUE_START_DATE, S.SATISFACTION_BASE_PROPORTION, S.SOURCE_DOCUMENT_LINE_ID,
            S.BZ_LOAD_DATE, S.SV_LOAD_DATE,
            @asof, CAST('9999-12-31' AS date), SYSUTCDATETIME(), SYSUTCDATETIME(), 1,
            S.ROW_HASH
        FROM #ToInsert S
        LEFT JOIN svo.D_RM_PERF_OBLIGATION_LINE T
          ON T.PERF_OBLIGATION_LINE_ID = S.PERF_OBLIGATION_LINE_ID
         AND T.CURR_IND = 1
        WHERE T.PERF_OBLIGATION_LINE_ID IS NULL      -- brand new NK
           OR EXISTS (SELECT 1 FROM #ToExpire E WHERE E.RM_PERF_OBLIGATION_LINE_SK = T.RM_PERF_OBLIGATION_LINE_SK);

        DECLARE @row_inserted int = @@ROWCOUNT;

        UPDATE etl.ETL_RUN
            SET END_DTTM = SYSDATETIME(),
                STATUS = 'SUCCEEDED',
                ROW_INSERTED = @row_inserted,
                ROW_EXPIRED = @row_expired
        WHERE RUN_ID = @run_id;

        COMMIT;

        IF @Debug = 1
            PRINT 'Completed ' + @proc + ' | inserted=' + CONVERT(varchar(12), @row_inserted)
                + ' expired=' + CONVERT(varchar(12), @row_expired);

    END TRY
    BEGIN CATCH
        IF @@TRANCOUNT > 0 ROLLBACK;

        DECLARE @ErrMsg nvarchar(4000) = ERROR_MESSAGE();

        UPDATE etl.ETL_RUN
            SET END_DTTM = SYSDATETIME(),
                STATUS = 'FAILED',
                ERROR_MESSAGE = @ErrMsg
        WHERE RUN_ID = @run_id;

        THROW;
    END CATCH
END
GO