USE [Oracle_Reporting_P2];
GO

/*==========================================================
  1) DDL: svo.D_RM_SATISFACTION_METHOD (SCD2)
==========================================================*/
IF OBJECT_ID('svo.D_RM_SATISFACTION_METHOD','U') IS NOT NULL
    DROP TABLE svo.D_RM_SATISFACTION_METHOD;
GO

CREATE TABLE svo.D_RM_SATISFACTION_METHOD
(
    RM_SATISFACTION_METHOD_SK   BIGINT IDENTITY(1,1) NOT NULL,
    SATISFACTION_METHOD_CODE    VARCHAR(60)  NOT NULL,
    SATISFACTION_METHOD_NAME    VARCHAR(240) NOT NULL,

    EFF_DATE                    DATE         NOT NULL,
    END_DATE                    DATE         NOT NULL,
    CRE_DATE                    DATETIME2(0) NOT NULL,
    UDT_DATE                    DATETIME2(0) NOT NULL,
    CURR_IND                    CHAR(1)      NOT NULL,

    ROW_HASH                    VARBINARY(32) NOT NULL,

    BZ_LOAD_DATE                DATE         NOT NULL,
    SV_LOAD_DATE                DATE         NOT NULL,

    CONSTRAINT PK_D_RM_SATISFACTION_METHOD
        PRIMARY KEY CLUSTERED (RM_SATISFACTION_METHOD_SK ASC)
) ON [FG_SilverDim];
GO

/* One current row per business key */
CREATE UNIQUE NONCLUSTERED INDEX UX_D_RM_SATISFACTION_METHOD_CODE_CURR
ON svo.D_RM_SATISFACTION_METHOD (SATISFACTION_METHOD_CODE)
WHERE CURR_IND = 'Y'
ON [FG_SilverDim];
GO

/* Optional lookup index */
CREATE NONCLUSTERED INDEX IX_D_RM_SATISFACTION_METHOD_CODE
ON svo.D_RM_SATISFACTION_METHOD (SATISFACTION_METHOD_CODE, CURR_IND, END_DATE)
ON [FG_SilverDim];
GO

/* Plug row */
SET IDENTITY_INSERT svo.D_RM_SATISFACTION_METHOD ON;

INSERT INTO svo.D_RM_SATISFACTION_METHOD
(
    RM_SATISFACTION_METHOD_SK,
    SATISFACTION_METHOD_CODE,
    SATISFACTION_METHOD_NAME,
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
    'UNK',
    'Unknown',
    CAST('0001-01-01' AS DATE),
    CAST('9999-12-31' AS DATE),
    CAST('0001-01-01' AS DATETIME2(0)),
    CAST('0001-01-01' AS DATETIME2(0)),
    'Y',
    HASHBYTES('SHA2_256', CONVERT(VARCHAR(4000), 'UNK|Unknown')),
    CAST(GETDATE() AS DATE),
    CAST(GETDATE() AS DATE)
);

SET IDENTITY_INSERT svo.D_RM_SATISFACTION_METHOD OFF;
GO


/*==========================================================
  2) PROC: svo.usp_Load_D_RM_SATISFACTION_METHOD_SCD2
      - SCD2 (EFF_DATE/END_DATE/CRE_DATE/UDT_DATE/CURR_IND)
      - Logs to etl.ETL_RUN
      - Idempotent (hash compare)
==========================================================*/
IF OBJECT_ID('svo.usp_Load_D_RM_SATISFACTION_METHOD_SCD2','P') IS NOT NULL
    DROP PROCEDURE svo.usp_Load_D_RM_SATISFACTION_METHOD_SCD2;
GO

CREATE PROCEDURE svo.usp_Load_D_RM_SATISFACTION_METHOD_SCD2
    @AsOfDate   DATE = NULL,
    @FullReload BIT  = 0
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    DECLARE
        @Now        DATETIME2(0) = SYSDATETIME(),
        @Today      DATE         = CAST(GETDATE() AS DATE),
        @RunId      BIGINT       = NULL,
        @ProcName   SYSNAME      = QUOTENAME(OBJECT_SCHEMA_NAME(@@PROCID)) + '.' + QUOTENAME(OBJECT_NAME(@@PROCID)),
        @TargetObj  SYSNAME      = N'svo.D_RM_SATISFACTION_METHOD',
        @RowsInserted INT = 0,
        @RowsExpired  INT = 0;

    SET @AsOfDate = COALESCE(@AsOfDate, @Today);

    INSERT INTO etl.ETL_RUN (PROC_NAME, TARGET_OBJECT, ASOF_DATE, START_DTTM, STATUS)
    VALUES (@ProcName, @TargetObj, @AsOfDate, @Now, 'STARTED');

    SET @RunId = SCOPE_IDENTITY();

    BEGIN TRY
        BEGIN TRAN;

        /* Build source set (distinct codes) from synonym */
        IF OBJECT_ID('tempdb..#src') IS NOT NULL DROP TABLE #src;
        CREATE TABLE #src
        (
            SATISFACTION_METHOD_CODE VARCHAR(60)  NOT NULL,
            SATISFACTION_METHOD_NAME VARCHAR(240) NOT NULL,
            BZ_LOAD_DATE             DATE         NOT NULL,
            ROW_HASH                 VARBINARY(32) NOT NULL,
            CONSTRAINT PK__src PRIMARY KEY (SATISFACTION_METHOD_CODE)
        );

        INSERT INTO #src
        (
            SATISFACTION_METHOD_CODE,
            SATISFACTION_METHOD_NAME,
            BZ_LOAD_DATE,
            ROW_HASH
        )
        SELECT
            Code = LEFT(LTRIM(RTRIM(M.PerfObligationsSatisfactionMethod)), 60),
            Name = LEFT(LTRIM(RTRIM(M.PerfObligationsSatisfactionMethod)), 240),
            BZ_LOAD_DATE =
                COALESCE(
                    MIN(CAST(M.AddDateTime AS DATE)),
                    @Today
                ),
            ROW_HASH =
                HASHBYTES(
                    'SHA2_256',
                    CONVERT(VARCHAR(4000),
                        LEFT(LTRIM(RTRIM(M.PerfObligationsSatisfactionMethod)), 60)
                        + '|' +
                        LEFT(LTRIM(RTRIM(M.PerfObligationsSatisfactionMethod)), 240)
                    )
                )
        FROM src.bzo_VRM_PerfObligationsPVO AS M
        WHERE M.PerfObligationsSatisfactionMethod IS NOT NULL
          AND LTRIM(RTRIM(M.PerfObligationsSatisfactionMethod)) <> ''
        GROUP BY
            LEFT(LTRIM(RTRIM(M.PerfObligationsSatisfactionMethod)), 60),
            LEFT(LTRIM(RTRIM(M.PerfObligationsSatisfactionMethod)), 240);

        /* Full reload option (truncate and reload) */
        IF @FullReload = 1
        BEGIN
            TRUNCATE TABLE svo.D_RM_SATISFACTION_METHOD;

            DBCC CHECKIDENT ('svo.D_RM_SATISFACTION_METHOD', RESEED, 0);

            SET IDENTITY_INSERT svo.D_RM_SATISFACTION_METHOD ON;

            INSERT INTO svo.D_RM_SATISFACTION_METHOD
            (
                RM_SATISFACTION_METHOD_SK,
                SATISFACTION_METHOD_CODE,
                SATISFACTION_METHOD_NAME,
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
                'UNK',
                'Unknown',
                CAST('0001-01-01' AS DATE),
                CAST('9999-12-31' AS DATE),
                CAST('0001-01-01' AS DATETIME2(0)),
                CAST('0001-01-01' AS DATETIME2(0)),
                'Y',
                HASHBYTES('SHA2_256', CONVERT(VARCHAR(4000), 'UNK|Unknown')),
                @Today,
                @Today
            );

            SET IDENTITY_INSERT svo.D_RM_SATISFACTION_METHOD OFF;

            INSERT INTO svo.D_RM_SATISFACTION_METHOD
            (
                SATISFACTION_METHOD_CODE,
                SATISFACTION_METHOD_NAME,
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
                s.SATISFACTION_METHOD_CODE,
                s.SATISFACTION_METHOD_NAME,
                @AsOfDate,
                CAST('9999-12-31' AS DATE),
                @Now,
                @Now,
                'Y',
                s.ROW_HASH,
                COALESCE(s.BZ_LOAD_DATE, @Today),
                @Today
            FROM #src s;

            SET @RowsInserted = @@ROWCOUNT;
            SET @RowsExpired  = 0;

            COMMIT TRAN;

            UPDATE etl.ETL_RUN
            SET END_DTTM = SYSDATETIME(),
                STATUS = 'SUCCEEDED',
                ROW_INSERTED = @RowsInserted,
                ROW_EXPIRED = @RowsExpired
            WHERE RUN_ID = @RunId;

            RETURN;
        END

        /* Incremental SCD2 */
        IF OBJECT_ID('tempdb..#chg') IS NOT NULL DROP TABLE #chg;
        CREATE TABLE #chg
        (
            SATISFACTION_METHOD_CODE VARCHAR(60)  NOT NULL,
            SATISFACTION_METHOD_NAME VARCHAR(240) NOT NULL,
            BZ_LOAD_DATE             DATE         NOT NULL,
            ROW_HASH                 VARBINARY(32) NOT NULL,
            ChangeType               CHAR(1)      NOT NULL, /* N=new, C=changed */
            CONSTRAINT PK__chg PRIMARY KEY (SATISFACTION_METHOD_CODE)
        );

        /* New codes */
        INSERT INTO #chg
        (
            SATISFACTION_METHOD_CODE,
            SATISFACTION_METHOD_NAME,
            BZ_LOAD_DATE,
            ROW_HASH,
            ChangeType
        )
        SELECT
            s.SATISFACTION_METHOD_CODE,
            s.SATISFACTION_METHOD_NAME,
            s.BZ_LOAD_DATE,
            s.ROW_HASH,
            'N'
        FROM #src s
        LEFT JOIN svo.D_RM_SATISFACTION_METHOD t
            ON t.SATISFACTION_METHOD_CODE = s.SATISFACTION_METHOD_CODE
           AND t.CURR_IND = 'Y'
        WHERE t.RM_SATISFACTION_METHOD_SK IS NULL;

        /* Changed codes */
        INSERT INTO #chg
        (
            SATISFACTION_METHOD_CODE,
            SATISFACTION_METHOD_NAME,
            BZ_LOAD_DATE,
            ROW_HASH,
            ChangeType
        )
        SELECT
            s.SATISFACTION_METHOD_CODE,
            s.SATISFACTION_METHOD_NAME,
            s.BZ_LOAD_DATE,
            s.ROW_HASH,
            'C'
        FROM #src s
        JOIN svo.D_RM_SATISFACTION_METHOD t
            ON t.SATISFACTION_METHOD_CODE = s.SATISFACTION_METHOD_CODE
           AND t.CURR_IND = 'Y'
        WHERE t.ROW_HASH <> s.ROW_HASH;

        /* Expire changed current rows */
        UPDATE t
        SET
            t.END_DATE     = DATEADD(DAY, -1, @AsOfDate),
            t.CURR_IND     = 'N',
            t.UDT_DATE     = @Now,
            t.SV_LOAD_DATE = @Today
        FROM svo.D_RM_SATISFACTION_METHOD t
        JOIN #chg c
            ON c.SATISFACTION_METHOD_CODE = t.SATISFACTION_METHOD_CODE
        WHERE t.CURR_IND = 'Y'
          AND c.ChangeType = 'C';

        SET @RowsExpired = @@ROWCOUNT;

        /* Insert new current rows for new and changed */
        INSERT INTO svo.D_RM_SATISFACTION_METHOD
        (
            SATISFACTION_METHOD_CODE,
            SATISFACTION_METHOD_NAME,
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
            c.SATISFACTION_METHOD_CODE,
            c.SATISFACTION_METHOD_NAME,
            @AsOfDate,
            CAST('9999-12-31' AS DATE),
            @Now,
            @Now,
            'Y',
            c.ROW_HASH,
            COALESCE(c.BZ_LOAD_DATE, @Today),
            @Today
        FROM #chg c;

        SET @RowsInserted = @@ROWCOUNT;

        COMMIT TRAN;

        UPDATE etl.ETL_RUN
        SET END_DTTM = SYSDATETIME(),
            STATUS = 'SUCCEEDED',
            ROW_INSERTED = @RowsInserted,
            ROW_EXPIRED = @RowsExpired
        WHERE RUN_ID = @RunId;
    END TRY
    BEGIN CATCH
        IF @@TRANCOUNT > 0 ROLLBACK TRAN;

        UPDATE etl.ETL_RUN
        SET END_DTTM = SYSDATETIME(),
            STATUS = 'FAILED',
            ERROR_MESSAGE = LEFT(ERROR_MESSAGE(), 4000)
        WHERE RUN_ID = @RunId;

        THROW;
    END CATCH
END
GO