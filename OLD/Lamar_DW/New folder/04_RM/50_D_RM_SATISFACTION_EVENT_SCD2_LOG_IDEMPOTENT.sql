/* =====================================================================
   D_RM_SATISFACTION_EVENT (SCD2) + ETL_RUN + Idempotent
   ===================================================================== */
USE Oracle_Reporting_P2;
GO

IF OBJECT_ID('svo.D_RM_SATISFACTION_EVENT','U') IS NOT NULL
    DROP TABLE svo.D_RM_SATISFACTION_EVENT;
GO

CREATE TABLE svo.D_RM_SATISFACTION_EVENT
(
    RM_SATISFACTION_EVENT_SK      BIGINT IDENTITY(1,1) NOT NULL,
    POL_SATISFACTION_EVENT_ID     BIGINT NOT NULL,

    ATTRIBUTE_CATEGORY            VARCHAR(30)   NOT NULL,
    COMMENTS                      VARCHAR(2000) NOT NULL,

    CREATED_BY                    VARCHAR(64)   NOT NULL,
    CREATED_FROM                  VARCHAR(30)   NOT NULL,
    CREATION_DATE                 DATE          NOT NULL,

    DISCARDED_DATE                DATE          NOT NULL,
    DISCARDED_FLAG                VARCHAR(1)    NOT NULL,

    DOCUMENT_LINE_ID              BIGINT        NOT NULL,
    DOCUMENT_SUB_LINE_ID          BIGINT        NOT NULL,

    HOLD_FLAG                     VARCHAR(1)    NOT NULL,

    LAST_UPDATE_DATE              DATE          NOT NULL,
    LAST_UPDATED_BY               VARCHAR(64)   NOT NULL,
    LAST_UPDATE_LOGIN             VARCHAR(32)   NOT NULL,

    OBJECT_VERSION_NUMBER         BIGINT        NOT NULL,
    PERF_OBLIGATION_LINE_ID       BIGINT        NOT NULL,

    PROCESSED_AMOUNT              DECIMAL(29,4) NOT NULL,
    PROCESSED_FLAG                VARCHAR(1)    NOT NULL,
    PROCESSED_PERIOD_PROPORTION   DECIMAL(29,4) NOT NULL,

    SATISFACTION_MEASUREMENT_DATE DATE          NOT NULL,
    SATISFACTION_MEASUREMENT_NUM  BIGINT        NOT NULL,
    SATISFACTION_PERCENT          DECIMAL(29,4) NOT NULL,

    SATISFACTION_PERIOD_END_DATE  DATE          NOT NULL,
    SATISFACTION_PERIOD_PROPORTION DECIMAL(29,4) NOT NULL,
    SATISFACTION_PERIOD_START_DATE DATE          NOT NULL,

    SATISFACTION_QUANTITY         DECIMAL(29,4) NOT NULL,
    SPLIT_FLAG                    VARCHAR(1)    NOT NULL,

    -- SCD2
    EFF_DATE                      DATE          NOT NULL,
    END_DATE                      DATE          NOT NULL,
    CRE_DATE                      DATETIME2(0)  NOT NULL,
    UDT_DATE                      DATETIME2(0)  NOT NULL,
    CURR_IND                      BIT           NOT NULL,

    ROW_HASH                      VARBINARY(32) NOT NULL,

    -- Load dates
    BZ_LOAD_DATE                  DATE          NOT NULL,
    SV_LOAD_DATE                  DATE          NOT NULL,

    CONSTRAINT PK_D_RM_SATISFACTION_EVENT
        PRIMARY KEY CLUSTERED (RM_SATISFACTION_EVENT_SK)
) ON FG_SilverDim;
GO

-- Only one current row per business key
CREATE UNIQUE NONCLUSTERED INDEX UX_D_RM_SATISFACTION_EVENT_BK_CURR
ON svo.D_RM_SATISFACTION_EVENT (POL_SATISFACTION_EVENT_ID)
WHERE CURR_IND = 1
ON FG_SilverDim;
GO

-- Helpful lookup for history
CREATE NONCLUSTERED INDEX IX_D_RM_SATISFACTION_EVENT_BK_EFF
ON svo.D_RM_SATISFACTION_EVENT (POL_SATISFACTION_EVENT_ID, EFF_DATE, END_DATE, CURR_IND)
ON FG_SilverDim;
GO

-- Plug row (SK=0, BK=-1)
SET IDENTITY_INSERT svo.D_RM_SATISFACTION_EVENT ON;

INSERT INTO svo.D_RM_SATISFACTION_EVENT
(
    RM_SATISFACTION_EVENT_SK,
    POL_SATISFACTION_EVENT_ID,
    ATTRIBUTE_CATEGORY,
    COMMENTS,
    CREATED_BY,
    CREATED_FROM,
    CREATION_DATE,
    DISCARDED_DATE,
    DISCARDED_FLAG,
    DOCUMENT_LINE_ID,
    DOCUMENT_SUB_LINE_ID,
    HOLD_FLAG,
    LAST_UPDATE_DATE,
    LAST_UPDATED_BY,
    LAST_UPDATE_LOGIN,
    OBJECT_VERSION_NUMBER,
    PERF_OBLIGATION_LINE_ID,
    PROCESSED_AMOUNT,
    PROCESSED_FLAG,
    PROCESSED_PERIOD_PROPORTION,
    SATISFACTION_MEASUREMENT_DATE,
    SATISFACTION_MEASUREMENT_NUM,
    SATISFACTION_PERCENT,
    SATISFACTION_PERIOD_END_DATE,
    SATISFACTION_PERIOD_PROPORTION,
    SATISFACTION_PERIOD_START_DATE,
    SATISFACTION_QUANTITY,
    SPLIT_FLAG,
    EFF_DATE,
    END_DATE,
    CRE_DATE,
    UDT_DATE,
    CURR_IND,
    ROW_HASH,
    BZ_LOAD_DATE,
    SV_LOAD_DATE
)
VALUES
(
    0,
    -1,
    'Unknown',
    'Unknown',
    'Unknown',
    'Unknown',
    CAST('0001-01-01' AS date),
    CAST('0001-01-01' AS date),
    'U',
    0,
    0,
    'U',
    CAST('0001-01-01' AS date),
    'Unknown',
    'Unknown',
    0,
    0,
    0,
    'U',
    0,
    CAST('0001-01-01' AS date),
    0,
    0,
    CAST('0001-01-01' AS date),
    0,
    CAST('0001-01-01' AS date),
    0,
    'U',
    CAST('0001-01-01' AS date),
    CAST('9999-12-31' AS date),
    SYSDATETIME(),
    SYSDATETIME(),
    1,
    HASHBYTES('SHA2_256', CONVERT(varbinary(max), 'PLUG')),
    CAST('0001-01-01' AS date),
    CAST(GETDATE() AS date)
);

SET IDENTITY_INSERT svo.D_RM_SATISFACTION_EVENT OFF;
GO

/* =====================================================================
   Loader proc
   ===================================================================== */
CREATE OR ALTER PROCEDURE svo.usp_Load_D_RM_SATISFACTION_EVENT_SCD2
      @FullReload bit = 0
    , @Debug      bit = 0
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    DECLARE
        @ProcName      sysname       = QUOTENAME(OBJECT_SCHEMA_NAME(@@PROCID)) + '.' + QUOTENAME(OBJECT_NAME(@@PROCID)),
        @TargetObject  sysname       = 'svo.D_RM_SATISFACTION_EVENT',
        @AsOfDate      date          = CAST(GETDATE() AS date),
        @RunId         bigint        = NULL,
        @Inserted      int           = 0,
        @Expired       int           = 0;

    INSERT INTO etl.ETL_RUN (PROC_NAME, TARGET_OBJECT, ASOF_DATE, STATUS)
    VALUES (@ProcName, @TargetObject, @AsOfDate, 'STARTED');

    SET @RunId = SCOPE_IDENTITY();

    BEGIN TRY
        IF @Debug = 1
            PRINT 'Starting ' + @ProcName + ' | RUN_ID=' + CONVERT(varchar(30), @RunId);

        BEGIN TRAN;

        IF @FullReload = 1
        BEGIN
            IF @Debug = 1
                PRINT 'FullReload requested. Rebuilding ' + @TargetObject;

            TRUNCATE TABLE svo.D_RM_SATISFACTION_EVENT;

            -- Reinsert plug row
            SET IDENTITY_INSERT svo.D_RM_SATISFACTION_EVENT ON;

            INSERT INTO svo.D_RM_SATISFACTION_EVENT
            (
                RM_SATISFACTION_EVENT_SK,
                POL_SATISFACTION_EVENT_ID,
                ATTRIBUTE_CATEGORY,
                COMMENTS,
                CREATED_BY,
                CREATED_FROM,
                CREATION_DATE,
                DISCARDED_DATE,
                DISCARDED_FLAG,
                DOCUMENT_LINE_ID,
                DOCUMENT_SUB_LINE_ID,
                HOLD_FLAG,
                LAST_UPDATE_DATE,
                LAST_UPDATED_BY,
                LAST_UPDATE_LOGIN,
                OBJECT_VERSION_NUMBER,
                PERF_OBLIGATION_LINE_ID,
                PROCESSED_AMOUNT,
                PROCESSED_FLAG,
                PROCESSED_PERIOD_PROPORTION,
                SATISFACTION_MEASUREMENT_DATE,
                SATISFACTION_MEASUREMENT_NUM,
                SATISFACTION_PERCENT,
                SATISFACTION_PERIOD_END_DATE,
                SATISFACTION_PERIOD_PROPORTION,
                SATISFACTION_PERIOD_START_DATE,
                SATISFACTION_QUANTITY,
                SPLIT_FLAG,
                EFF_DATE,
                END_DATE,
                CRE_DATE,
                UDT_DATE,
                CURR_IND,
                ROW_HASH,
                BZ_LOAD_DATE,
                SV_LOAD_DATE
            )
            VALUES
            (
                0,
                -1,
                'Unknown',
                'Unknown',
                'Unknown',
                'Unknown',
                CAST('0001-01-01' AS date),
                CAST('0001-01-01' AS date),
                'U',
                0,
                0,
                'U',
                CAST('0001-01-01' AS date),
                'Unknown',
                'Unknown',
                0,
                0,
                0,
                'U',
                0,
                CAST('0001-01-01' AS date),
                0,
                0,
                CAST('0001-01-01' AS date),
                0,
                CAST('0001-01-01' AS date),
                0,
                'U',
                CAST('0001-01-01' AS date),
                CAST('9999-12-31' AS date),
                SYSDATETIME(),
                SYSDATETIME(),
                1,
                HASHBYTES('SHA2_256', CONVERT(varbinary(max), 'PLUG')),
                CAST('0001-01-01' AS date),
                CAST(GETDATE() AS date)
            );

            SET IDENTITY_INSERT svo.D_RM_SATISFACTION_EVENT OFF;

            ;WITH SrcRaw AS
            (
                SELECT
                    POL_SATISFACTION_EVENT_ID     = TRY_CONVERT(bigint, E.PolSatisfactionEventId),

                    ATTRIBUTE_CATEGORY            = COALESCE(NULLIF(E.PolSatisfactionEventsAttributeCategory,''), 'Unknown'),
                    COMMENTS                      = COALESCE(NULLIF(E.PolSatisfactionEventsComments,''), 'Unknown'),

                    CREATED_BY                    = COALESCE(NULLIF(E.PolSatisfactionEventsCreatedBy,''), 'Unknown'),
                    CREATED_FROM                  = COALESCE(NULLIF(E.PolSatisfactionEventsCreatedFrom,''), 'Unknown'),
                    CREATION_DATE                 = COALESCE(TRY_CONVERT(date, E.PolSatisfactionEventsCreationDate), CAST('0001-01-01' AS date)),

                    DISCARDED_DATE                = COALESCE(TRY_CONVERT(date, E.PolSatisfactionEventsDiscardedDate), CAST('0001-01-01' AS date)),
                    DISCARDED_FLAG                = COALESCE(NULLIF(E.PolSatisfactionEventsDiscardedFlag,''), 'U'),

                    DOCUMENT_LINE_ID              = COALESCE(TRY_CONVERT(bigint, E.PolSatisfactionEventsDocumentLineId), 0),
                    DOCUMENT_SUB_LINE_ID          = COALESCE(TRY_CONVERT(bigint, E.PolSatisfactionEventsDocumentSubLineId), 0),

                    HOLD_FLAG                     = COALESCE(NULLIF(E.PolSatisfactionEventsHoldFlag,''), 'U'),

                    LAST_UPDATE_DATE              = COALESCE(TRY_CONVERT(date, E.PolSatisfactionEventsLastUpdateDate), CAST('0001-01-01' AS date)),
                    LAST_UPDATED_BY               = COALESCE(NULLIF(E.PolSatisfactionEventsLastUpdatedBy,''), 'Unknown'),
                    LAST_UPDATE_LOGIN             = COALESCE(NULLIF(E.PolSatisfactionEventsLastUpdateLogin,''), 'Unknown'),

                    OBJECT_VERSION_NUMBER         = COALESCE(TRY_CONVERT(bigint, E.PolSatisfactionEventsObjectVersionNumber), 0),

                    PERF_OBLIGATION_LINE_ID       = COALESCE(TRY_CONVERT(bigint, E.PolSatisfactionEventsPerfObligationLineId), 0),

                    PROCESSED_AMOUNT              = COALESCE(TRY_CONVERT(decimal(29,4), E.PolSatisfactionEventsProcessedAmount), 0),
                    PROCESSED_FLAG                = COALESCE(NULLIF(E.PolSatisfactionEventsProcessedFlag,''), 'U'),
                    PROCESSED_PERIOD_PROPORTION   = COALESCE(TRY_CONVERT(decimal(29,4), E.PolSatisfactionEventsProcessedPeriodProportion), 0),

                    SATISFACTION_MEASUREMENT_DATE = COALESCE(TRY_CONVERT(date, E.PolSatisfactionEventsSatisfactionMeasurementDate), CAST('0001-01-01' AS date)),
                    SATISFACTION_MEASUREMENT_NUM  = COALESCE(TRY_CONVERT(bigint, E.PolSatisfactionEventsSatisfactionMeasurementNum), 0),
                    SATISFACTION_PERCENT          = COALESCE(TRY_CONVERT(decimal(29,4), E.PolSatisfactionEventsSatisfactionPercent), 0),

                    SATISFACTION_PERIOD_END_DATE  = COALESCE(TRY_CONVERT(date, E.PolSatisfactionEventsSatisfactionPeriodEndDate), CAST('0001-01-01' AS date)),
                    SATISFACTION_PERIOD_PROPORTION = COALESCE(TRY_CONVERT(decimal(29,4), E.PolSatisfactionEventsSatisfactionPeriodProportion), 0),
                    SATISFACTION_PERIOD_START_DATE = COALESCE(TRY_CONVERT(date, E.PolSatisfactionEventsSatisfactionPeriodStartDate), CAST('0001-01-01' AS date)),

                    SATISFACTION_QUANTITY         = COALESCE(TRY_CONVERT(decimal(29,4), E.PolSatisfactionEventsSatisfactionQuantity), 0),

                    SPLIT_FLAG                    = COALESCE(NULLIF(E.PolSatisfactionEventsSplitFlag,''), 'U'),

                    BZ_LOAD_DATE                  = COALESCE(CAST(E.AddDateTime AS date), CAST(GETDATE() AS date)),
                    SV_LOAD_DATE                  = CAST(GETDATE() AS date),

                    rn = ROW_NUMBER() OVER
                    (
                        PARTITION BY TRY_CONVERT(bigint, E.PolSatisfactionEventId)
                        ORDER BY TRY_CONVERT(datetime2(7), E.AddDateTime) DESC,
                                 TRY_CONVERT(datetime2(7), E.PolSatisfactionEventsLastUpdateDate) DESC,
                                 TRY_CONVERT(bigint, E.PolSatisfactionEventsObjectVersionNumber) DESC
                    )
                FROM src.bzo_VRM_PolSatisfactionEventsPVO E
                WHERE TRY_CONVERT(bigint, E.PolSatisfactionEventId) IS NOT NULL
            ),
            Src AS
            (
                SELECT
                    *,
                    ROW_HASH = HASHBYTES
                    (
                        'SHA2_256',
                        CONVERT(varbinary(max),
                            CONCAT
                            (
                                POL_SATISFACTION_EVENT_ID, '|',
                                ATTRIBUTE_CATEGORY, '|', COMMENTS, '|',
                                CREATED_BY, '|', CREATED_FROM, '|', CONVERT(char(10), CREATION_DATE, 120), '|',
                                CONVERT(char(10), DISCARDED_DATE, 120), '|', DISCARDED_FLAG, '|',
                                DOCUMENT_LINE_ID, '|', DOCUMENT_SUB_LINE_ID, '|',
                                HOLD_FLAG, '|',
                                CONVERT(char(10), LAST_UPDATE_DATE, 120), '|', LAST_UPDATED_BY, '|', LAST_UPDATE_LOGIN, '|',
                                OBJECT_VERSION_NUMBER, '|', PERF_OBLIGATION_LINE_ID, '|',
                                CONVERT(varchar(50), PROCESSED_AMOUNT), '|', PROCESSED_FLAG, '|', CONVERT(varchar(50), PROCESSED_PERIOD_PROPORTION), '|',
                                CONVERT(char(10), SATISFACTION_MEASUREMENT_DATE, 120), '|', SATISFACTION_MEASUREMENT_NUM, '|', CONVERT(varchar(50), SATISFACTION_PERCENT), '|',
                                CONVERT(char(10), SATISFACTION_PERIOD_START_DATE, 120), '|', CONVERT(char(10), SATISFACTION_PERIOD_END_DATE, 120), '|', CONVERT(varchar(50), SATISFACTION_PERIOD_PROPORTION), '|',
                                CONVERT(varchar(50), SATISFACTION_QUANTITY), '|', SPLIT_FLAG
                            )
                        )
                    )
                FROM SrcRaw
                WHERE rn = 1
            )
            INSERT INTO svo.D_RM_SATISFACTION_EVENT
            (
                POL_SATISFACTION_EVENT_ID,
                ATTRIBUTE_CATEGORY,
                COMMENTS,
                CREATED_BY,
                CREATED_FROM,
                CREATION_DATE,
                DISCARDED_DATE,
                DISCARDED_FLAG,
                DOCUMENT_LINE_ID,
                DOCUMENT_SUB_LINE_ID,
                HOLD_FLAG,
                LAST_UPDATE_DATE,
                LAST_UPDATED_BY,
                LAST_UPDATE_LOGIN,
                OBJECT_VERSION_NUMBER,
                PERF_OBLIGATION_LINE_ID,
                PROCESSED_AMOUNT,
                PROCESSED_FLAG,
                PROCESSED_PERIOD_PROPORTION,
                SATISFACTION_MEASUREMENT_DATE,
                SATISFACTION_MEASUREMENT_NUM,
                SATISFACTION_PERCENT,
                SATISFACTION_PERIOD_END_DATE,
                SATISFACTION_PERIOD_PROPORTION,
                SATISFACTION_PERIOD_START_DATE,
                SATISFACTION_QUANTITY,
                SPLIT_FLAG,
                EFF_DATE,
                END_DATE,
                CRE_DATE,
                UDT_DATE,
                CURR_IND,
                ROW_HASH,
                BZ_LOAD_DATE,
                SV_LOAD_DATE
            )
            SELECT
                S.POL_SATISFACTION_EVENT_ID,
                S.ATTRIBUTE_CATEGORY,
                S.COMMENTS,
                S.CREATED_BY,
                S.CREATED_FROM,
                S.CREATION_DATE,
                S.DISCARDED_DATE,
                S.DISCARDED_FLAG,
                S.DOCUMENT_LINE_ID,
                S.DOCUMENT_SUB_LINE_ID,
                S.HOLD_FLAG,
                S.LAST_UPDATE_DATE,
                S.LAST_UPDATED_BY,
                S.LAST_UPDATE_LOGIN,
                S.OBJECT_VERSION_NUMBER,
                S.PERF_OBLIGATION_LINE_ID,
                S.PROCESSED_AMOUNT,
                S.PROCESSED_FLAG,
                S.PROCESSED_PERIOD_PROPORTION,
                S.SATISFACTION_MEASUREMENT_DATE,
                S.SATISFACTION_MEASUREMENT_NUM,
                S.SATISFACTION_PERCENT,
                S.SATISFACTION_PERIOD_END_DATE,
                S.SATISFACTION_PERIOD_PROPORTION,
                S.SATISFACTION_PERIOD_START_DATE,
                S.SATISFACTION_QUANTITY,
                S.SPLIT_FLAG,
                @AsOfDate,
                CAST('9999-12-31' AS date),
                SYSDATETIME(),
                SYSDATETIME(),
                1,
                S.ROW_HASH,
                S.BZ_LOAD_DATE,
                S.SV_LOAD_DATE
            FROM Src S;

            SET @Inserted = @@ROWCOUNT;

            COMMIT;

            UPDATE etl.ETL_RUN
               SET END_DTTM = SYSDATETIME(),
                   STATUS = 'SUCCESS',
                   ROW_INSERTED = @Inserted,
                   ROW_EXPIRED = 0
             WHERE RUN_ID = @RunId;

            RETURN;
        END

        /* Incremental SCD2 */
        IF OBJECT_ID('tempdb..#Src') IS NOT NULL DROP TABLE #Src;

        ;WITH SrcRaw AS
        (
            SELECT
                POL_SATISFACTION_EVENT_ID     = TRY_CONVERT(bigint, E.PolSatisfactionEventId),

                ATTRIBUTE_CATEGORY            = COALESCE(NULLIF(E.PolSatisfactionEventsAttributeCategory,''), 'Unknown'),
                COMMENTS                      = COALESCE(NULLIF(E.PolSatisfactionEventsComments,''), 'Unknown'),

                CREATED_BY                    = COALESCE(NULLIF(E.PolSatisfactionEventsCreatedBy,''), 'Unknown'),
                CREATED_FROM                  = COALESCE(NULLIF(E.PolSatisfactionEventsCreatedFrom,''), 'Unknown'),
                CREATION_DATE                 = COALESCE(TRY_CONVERT(date, E.PolSatisfactionEventsCreationDate), CAST('0001-01-01' AS date)),

                DISCARDED_DATE                = COALESCE(TRY_CONVERT(date, E.PolSatisfactionEventsDiscardedDate), CAST('0001-01-01' AS date)),
                DISCARDED_FLAG                = COALESCE(NULLIF(E.PolSatisfactionEventsDiscardedFlag,''), 'U'),

                DOCUMENT_LINE_ID              = COALESCE(TRY_CONVERT(bigint, E.PolSatisfactionEventsDocumentLineId), 0),
                DOCUMENT_SUB_LINE_ID          = COALESCE(TRY_CONVERT(bigint, E.PolSatisfactionEventsDocumentSubLineId), 0),

                HOLD_FLAG                     = COALESCE(NULLIF(E.PolSatisfactionEventsHoldFlag,''), 'U'),

                LAST_UPDATE_DATE              = COALESCE(TRY_CONVERT(date, E.PolSatisfactionEventsLastUpdateDate), CAST('0001-01-01' AS date)),
                LAST_UPDATED_BY               = COALESCE(NULLIF(E.PolSatisfactionEventsLastUpdatedBy,''), 'Unknown'),
                LAST_UPDATE_LOGIN             = COALESCE(NULLIF(E.PolSatisfactionEventsLastUpdateLogin,''), 'Unknown'),

                OBJECT_VERSION_NUMBER         = COALESCE(TRY_CONVERT(bigint, E.PolSatisfactionEventsObjectVersionNumber), 0),

                PERF_OBLIGATION_LINE_ID       = COALESCE(TRY_CONVERT(bigint, E.PolSatisfactionEventsPerfObligationLineId), 0),

                PROCESSED_AMOUNT              = COALESCE(TRY_CONVERT(decimal(29,4), E.PolSatisfactionEventsProcessedAmount), 0),
                PROCESSED_FLAG                = COALESCE(NULLIF(E.PolSatisfactionEventsProcessedFlag,''), 'U'),
                PROCESSED_PERIOD_PROPORTION   = COALESCE(TRY_CONVERT(decimal(29,4), E.PolSatisfactionEventsProcessedPeriodProportion), 0),

                SATISFACTION_MEASUREMENT_DATE = COALESCE(TRY_CONVERT(date, E.PolSatisfactionEventsSatisfactionMeasurementDate), CAST('0001-01-01' AS date)),
                SATISFACTION_MEASUREMENT_NUM  = COALESCE(TRY_CONVERT(bigint, E.PolSatisfactionEventsSatisfactionMeasurementNum), 0),
                SATISFACTION_PERCENT          = COALESCE(TRY_CONVERT(decimal(29,4), E.PolSatisfactionEventsSatisfactionPercent), 0),

                SATISFACTION_PERIOD_END_DATE  = COALESCE(TRY_CONVERT(date, E.PolSatisfactionEventsSatisfactionPeriodEndDate), CAST('0001-01-01' AS date)),
                SATISFACTION_PERIOD_PROPORTION = COALESCE(TRY_CONVERT(decimal(29,4), E.PolSatisfactionEventsSatisfactionPeriodProportion), 0),
                SATISFACTION_PERIOD_START_DATE = COALESCE(TRY_CONVERT(date, E.PolSatisfactionEventsSatisfactionPeriodStartDate), CAST('0001-01-01' AS date)),

                SATISFACTION_QUANTITY         = COALESCE(TRY_CONVERT(decimal(29,4), E.PolSatisfactionEventsSatisfactionQuantity), 0),

                SPLIT_FLAG                    = COALESCE(NULLIF(E.PolSatisfactionEventsSplitFlag,''), 'U'),

                BZ_LOAD_DATE                  = COALESCE(CAST(E.AddDateTime AS date), CAST(GETDATE() AS date)),
                SV_LOAD_DATE                  = CAST(GETDATE() AS date),

                rn = ROW_NUMBER() OVER
                (
                    PARTITION BY TRY_CONVERT(bigint, E.PolSatisfactionEventId)
                    ORDER BY TRY_CONVERT(datetime2(7), E.AddDateTime) DESC,
                             TRY_CONVERT(datetime2(7), E.PolSatisfactionEventsLastUpdateDate) DESC,
                             TRY_CONVERT(bigint, E.PolSatisfactionEventsObjectVersionNumber) DESC
                )
            FROM src.bzo_VRM_PolSatisfactionEventsPVO E
            WHERE TRY_CONVERT(bigint, E.PolSatisfactionEventId) IS NOT NULL
        )
        SELECT
            S.*,
            ROW_HASH = HASHBYTES
            (
                'SHA2_256',
                CONVERT(varbinary(max),
                    CONCAT
                    (
                        POL_SATISFACTION_EVENT_ID, '|',
                        ATTRIBUTE_CATEGORY, '|', COMMENTS, '|',
                        CREATED_BY, '|', CREATED_FROM, '|', CONVERT(char(10), CREATION_DATE, 120), '|',
                        CONVERT(char(10), DISCARDED_DATE, 120), '|', DISCARDED_FLAG, '|',
                        DOCUMENT_LINE_ID, '|', DOCUMENT_SUB_LINE_ID, '|',
                        HOLD_FLAG, '|',
                        CONVERT(char(10), LAST_UPDATE_DATE, 120), '|', LAST_UPDATED_BY, '|', LAST_UPDATE_LOGIN, '|',
                        OBJECT_VERSION_NUMBER, '|', PERF_OBLIGATION_LINE_ID, '|',
                        CONVERT(varchar(50), PROCESSED_AMOUNT), '|', PROCESSED_FLAG, '|', CONVERT(varchar(50), PROCESSED_PERIOD_PROPORTION), '|',
                        CONVERT(char(10), SATISFACTION_MEASUREMENT_DATE, 120), '|', SATISFACTION_MEASUREMENT_NUM, '|', CONVERT(varchar(50), SATISFACTION_PERCENT), '|',
                        CONVERT(char(10), SATISFACTION_PERIOD_START_DATE, 120), '|', CONVERT(char(10), SATISFACTION_PERIOD_END_DATE, 120), '|', CONVERT(varchar(50), SATISFACTION_PERIOD_PROPORTION), '|',
                        CONVERT(varchar(50), SATISFACTION_QUANTITY), '|', SPLIT_FLAG
                    )
                )
            )
        INTO #Src
        FROM SrcRaw S
        WHERE rn = 1;

        -- Expire changed current rows
        UPDATE T
           SET T.END_DATE = DATEADD(day, -1, @AsOfDate),
               T.CURR_IND = 0,
               T.UDT_DATE = SYSDATETIME(),
               T.SV_LOAD_DATE = CAST(GETDATE() AS date)
        FROM svo.D_RM_SATISFACTION_EVENT T
        JOIN #Src S
          ON S.POL_SATISFACTION_EVENT_ID = T.POL_SATISFACTION_EVENT_ID
        WHERE T.CURR_IND = 1
          AND T.POL_SATISFACTION_EVENT_ID <> -1
          AND T.ROW_HASH <> S.ROW_HASH;

        SET @Expired = @@ROWCOUNT;

        -- Insert new keys and changed keys
        INSERT INTO svo.D_RM_SATISFACTION_EVENT
        (
            POL_SATISFACTION_EVENT_ID,
            ATTRIBUTE_CATEGORY,
            COMMENTS,
            CREATED_BY,
            CREATED_FROM,
            CREATION_DATE,
            DISCARDED_DATE,
            DISCARDED_FLAG,
            DOCUMENT_LINE_ID,
            DOCUMENT_SUB_LINE_ID,
            HOLD_FLAG,
            LAST_UPDATE_DATE,
            LAST_UPDATED_BY,
            LAST_UPDATE_LOGIN,
            OBJECT_VERSION_NUMBER,
            PERF_OBLIGATION_LINE_ID,
            PROCESSED_AMOUNT,
            PROCESSED_FLAG,
            PROCESSED_PERIOD_PROPORTION,
            SATISFACTION_MEASUREMENT_DATE,
            SATISFACTION_MEASUREMENT_NUM,
            SATISFACTION_PERCENT,
            SATISFACTION_PERIOD_END_DATE,
            SATISFACTION_PERIOD_PROPORTION,
            SATISFACTION_PERIOD_START_DATE,
            SATISFACTION_QUANTITY,
            SPLIT_FLAG,
            EFF_DATE,
            END_DATE,
            CRE_DATE,
            UDT_DATE,
            CURR_IND,
            ROW_HASH,
            BZ_LOAD_DATE,
            SV_LOAD_DATE
        )
        SELECT
            S.POL_SATISFACTION_EVENT_ID,
            S.ATTRIBUTE_CATEGORY,
            S.COMMENTS,
            S.CREATED_BY,
            S.CREATED_FROM,
            S.CREATION_DATE,
            S.DISCARDED_DATE,
            S.DISCARDED_FLAG,
            S.DOCUMENT_LINE_ID,
            S.DOCUMENT_SUB_LINE_ID,
            S.HOLD_FLAG,
            S.LAST_UPDATE_DATE,
            S.LAST_UPDATED_BY,
            S.LAST_UPDATE_LOGIN,
            S.OBJECT_VERSION_NUMBER,
            S.PERF_OBLIGATION_LINE_ID,
            S.PROCESSED_AMOUNT,
            S.PROCESSED_FLAG,
            S.PROCESSED_PERIOD_PROPORTION,
            S.SATISFACTION_MEASUREMENT_DATE,
            S.SATISFACTION_MEASUREMENT_NUM,
            S.SATISFACTION_PERCENT,
            S.SATISFACTION_PERIOD_END_DATE,
            S.SATISFACTION_PERIOD_PROPORTION,
            S.SATISFACTION_PERIOD_START_DATE,
            S.SATISFACTION_QUANTITY,
            S.SPLIT_FLAG,
            @AsOfDate,
            CAST('9999-12-31' AS date),
            SYSDATETIME(),
            SYSDATETIME(),
            1,
            S.ROW_HASH,
            S.BZ_LOAD_DATE,
            S.SV_LOAD_DATE
        FROM #Src S
        LEFT JOIN svo.D_RM_SATISFACTION_EVENT T
          ON T.POL_SATISFACTION_EVENT_ID = S.POL_SATISFACTION_EVENT_ID
         AND T.CURR_IND = 1
        WHERE S.POL_SATISFACTION_EVENT_ID <> -1
          AND (T.POL_SATISFACTION_EVENT_ID IS NULL OR T.ROW_HASH <> S.ROW_HASH);

        SET @Inserted = @@ROWCOUNT;

        COMMIT;

        UPDATE etl.ETL_RUN
           SET END_DTTM = SYSDATETIME(),
               STATUS = 'SUCCESS',
               ROW_INSERTED = @Inserted,
               ROW_EXPIRED = @Expired
         WHERE RUN_ID = @RunId;

        IF @Debug = 1
            PRINT 'Completed ' + @ProcName
                + ' | inserted=' + CONVERT(varchar(30), @Inserted)
                + ' | expired=' + CONVERT(varchar(30), @Expired);
    END TRY
    BEGIN CATCH
        IF @@TRANCOUNT > 0 ROLLBACK;

        UPDATE etl.ETL_RUN
           SET END_DTTM = SYSDATETIME(),
               STATUS = 'FAILED',
               ERROR_MESSAGE = LEFT(ERROR_MESSAGE(), 4000)
         WHERE RUN_ID = @RunId;

        DECLARE @ErrMsg nvarchar(4000) = ERROR_MESSAGE();
        RAISERROR('%s failed: %s', 16, 1, @ProcName, @ErrMsg);
    END CATCH
END
GO