/* =========================================================
   usp_Load_F_GL_BALANCES
   Incremental INSERT only. Sources: bzo.GL_BalanceExtractPVO, GL_FiscalPeriodExtractPVO.
   Filter: B.AddDateTime > @LastWatermark. Dedupe by (LEDGER_SK, BALANCE_DATE_SK, CODE_COMBINATION_ID, CURRENCY_CODE, PERIOD_NUMBER).
   Resolve SKs via svo.LINES_CODE_COMBO_LOOKUP and svo.D_* (CURR_IND='Y'), D_LEDGER.
   Target: svo.F_GL_BALANCES (BALANCE_DATE_SK, BEGIN/PERIOD/END_BALANCE_AMT, LAST_UPDATE_DATE, LAST_UPDATED_BY, CODE_COMBINATION_ID).
   Exclude BalanceTranslatedFlag = 'R'.
   ========================================================= */
CREATE OR ALTER PROCEDURE svo.usp_Load_F_GL_BALANCES
    @BatchId INT = NULL
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    DECLARE
        @ProcName       SYSNAME        = OBJECT_SCHEMA_NAME(@@PROCID) + '.' + OBJECT_NAME(@@PROCID),
        @TargetObject   SYSNAME        = 'svo.F_GL_BALANCES',
        @StartDttm      DATETIME2(0)   = SYSDATETIME(),
        @EndDttm        DATETIME2(0),
        @RunId          BIGINT         = NULL,
        @ErrMsg         NVARCHAR(4000) = NULL,
        @AsOfDate       DATE           = CAST(GETDATE() AS DATE),
        @LastWatermark  DATETIME2(7),
        @MaxWatermark   DATETIME2(7)   = NULL,
        @RowInserted    INT            = 0,
        @TableBridgeID  INT            = NULL;

    SELECT @TableBridgeID = TableBridgeID FROM meta.MedallionTableBridge WHERE targettable = N'GL_BalanceExtractPVO';

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

        IF OBJECT_ID('tempdb..#bal') IS NOT NULL DROP TABLE #bal;

        SELECT
            B.BalanceLedgerId,
            B.BalanceCodeCombinationId,
            B.PeriodStartDate,
            B.BalanceCurrencyCode,
            B.BalancePeriodNetDr,
            B.BalancePeriodNetCr,
            B.BalanceBeginBalanceDr,
            B.BalanceBeginBalanceCr,
            B.BalancePeriodNum,
            B.BalanceLastUpdateDate,
            B.BalanceLastUpdatedBy,
            B.BalAddDateTime
        INTO #bal
        FROM (
            SELECT
                x.BalanceLedgerId,
                x.BalanceCodeCombinationId,
                x.BalanceCurrencyCode,
                x.BalancePeriodNetDr,
                x.BalancePeriodNetCr,
                x.BalanceBeginBalanceDr,
                x.BalanceBeginBalanceCr,
                x.BalancePeriodNum,
                x.BalanceLastUpdateDate,
                x.BalanceLastUpdatedBy,
                x.AddDateTime AS BalAddDateTime,
                P.PeriodStartDate,
                ROW_NUMBER() OVER (
                    PARTITION BY x.BalanceLedgerId, x.BalanceCodeCombinationId, P.PeriodStartDate, x.BalanceCurrencyCode, x.BalancePeriodNum
                    ORDER BY x.AddDateTime DESC
                ) AS rn
            FROM bzo.GL_BalanceExtractPVO x
            INNER JOIN bzo.GL_FiscalPeriodExtractPVO P ON x.BalancePeriodName = P.PeriodPeriodName AND P.PeriodStartDate IS NOT NULL
            WHERE x.AddDateTime > @LastWatermark
              AND x.BalanceTranslatedFlag <> 'R'
        ) B
        WHERE B.rn = 1;

        SELECT @MaxWatermark = MAX(BalAddDateTime) FROM #bal;

        INSERT INTO svo.F_GL_BALANCES
        (ACCOUNT_SK, BUSINESS_OFFERING_SK, COMPANY_SK, COST_CENTER_SK, INDUSTRY_SK, INTERCOMPANY_SK, LEDGER_SK, BALANCE_DATE_SK,
         BEGIN_BALANCE_AMT, PERIOD_ACTIVITY_AMT, END_BALANCE_AMT, CURRENCY_CODE, PERIOD_NUMBER, CODE_COMBINATION_ID,
         LAST_UPDATE_DATE, LAST_UPDATED_BY, BZ_LOAD_DATE, SV_LOAD_DATE)
        SELECT
            ISNULL(DA.ACCOUNT_SK, 0),
            ISNULL(DBO.BUSINESS_OFFERING_SK, 0),
            ISNULL(DCO.COMPANY_SK, 0),
            ISNULL(DCC.COST_CENTER_SK, 0),
            ISNULL(DI.INDUSTRY_SK, 0),
            ISNULL(DIC.INTERCOMPANY_SK, 0),
            ISNULL(LDG.LEDGER_SK, 0),
            ISNULL(CAST(CONVERT(CHAR(8), TRY_CONVERT(DATE, B.PeriodStartDate), 112) AS INT), 0),
            CAST(ISNULL(B.BalanceBeginBalanceDr, 0) - ISNULL(B.BalanceBeginBalanceCr, 0) AS DECIMAL(18,4)),
            CAST(ISNULL(B.BalancePeriodNetDr, 0) - ISNULL(B.BalancePeriodNetCr, 0) AS DECIMAL(18,4)),
            CAST((ISNULL(B.BalanceBeginBalanceDr, 0) - ISNULL(B.BalanceBeginBalanceCr, 0)) + (ISNULL(B.BalancePeriodNetDr, 0) - ISNULL(B.BalancePeriodNetCr, 0)) AS DECIMAL(18,4)),
            ISNULL(B.BalanceCurrencyCode, 'UNK'),
            TRY_CONVERT(INT, B.BalancePeriodNum),
            TRY_CONVERT(BIGINT, B.BalanceCodeCombinationId),
            ISNULL(B.BalanceLastUpdateDate, CAST(GETDATE() AS DATETIME)),
            ISNULL(B.BalanceLastUpdatedBy, 'SYSTEM'),
            ISNULL(B.BalAddDateTime, SYSDATETIME()),
            SYSDATETIME()
        FROM #bal B
        LEFT JOIN svo.LINES_CODE_COMBO_LOOKUP C ON CAST(B.BalanceCodeCombinationId AS BIGINT) = C.CODE_COMBINATION_BK
        LEFT JOIN svo.D_ACCOUNT AS DA ON DA.ACCOUNT_ID = C.ACCOUNT_ID AND DA.CURR_IND = 'Y'
        LEFT JOIN svo.D_BUSINESS_OFFERING AS DBO ON DBO.BUSINESS_OFFERING_ID = C.BUSINESSOFFERING_ID AND DBO.CURR_IND = 'Y'
        LEFT JOIN svo.D_COMPANY AS DCO ON DCO.COMPANY_ID = C.COMPANY_ID AND DCO.CURR_IND = 'Y'
        LEFT JOIN svo.D_COST_CENTER AS DCC ON DCC.COST_CENTER_ID = C.COSTCENTER_ID AND DCC.CURR_IND = 'Y'
        LEFT JOIN svo.D_INDUSTRY AS DI ON DI.INDUSTRY_ID = C.INDUSTRY_ID AND DI.CURR_IND = 'Y'
        LEFT JOIN svo.D_INTERCOMPANY AS DIC ON DIC.INTERCOMPANY_ID = C.INTERCOMPANY_ID AND DIC.CURR_IND = 'Y'
        LEFT JOIN svo.D_LEDGER AS LDG ON LDG.LEDGER_ID = B.BalanceLedgerId AND LDG.CURR_IND = 'Y'
        WHERE NOT EXISTS (
            SELECT 1 FROM svo.F_GL_BALANCES t
            WHERE t.LEDGER_SK = ISNULL(LDG.LEDGER_SK, 0)
              AND t.BALANCE_DATE_SK = ISNULL(CAST(CONVERT(CHAR(8), TRY_CONVERT(DATE, B.PeriodStartDate), 112) AS INT), 0)
              AND (t.CODE_COMBINATION_ID = TRY_CONVERT(BIGINT, B.BalanceCodeCombinationId) OR (t.CODE_COMBINATION_ID IS NULL AND B.BalanceCodeCombinationId IS NULL))
              AND t.CURRENCY_CODE = ISNULL(B.BalanceCurrencyCode, 'UNK')
              AND (t.PERIOD_NUMBER = TRY_CONVERT(INT, B.BalancePeriodNum) OR (t.PERIOD_NUMBER IS NULL AND B.BalancePeriodNum IS NULL))
        );

        SET @RowInserted = @@ROWCOUNT;

        IF @MaxWatermark IS NOT NULL
            UPDATE etl.ETL_WATERMARK SET LAST_WATERMARK = @MaxWatermark, UDT_DATE = SYSDATETIME() WHERE TABLE_NAME = @TargetObject;

        IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'IX_F_GL_BALANCES_COMPANY_SK' AND object_id = OBJECT_ID('svo.F_GL_BALANCES'))
            CREATE NONCLUSTERED INDEX IX_F_GL_BALANCES_COMPANY_SK ON svo.F_GL_BALANCES(COMPANY_SK)
            INCLUDE (ACCOUNT_SK, BUSINESS_OFFERING_SK, COST_CENTER_SK, INDUSTRY_SK, INTERCOMPANY_SK, LEDGER_SK, BALANCE_DATE_SK, BEGIN_BALANCE_AMT, PERIOD_ACTIVITY_AMT, END_BALANCE_AMT, CURRENCY_CODE) ON FG_SilverFact;
        IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'IX_F_GL_BALANCES_ACCOUNT_SK' AND object_id = OBJECT_ID('svo.F_GL_BALANCES'))
            CREATE NONCLUSTERED INDEX IX_F_GL_BALANCES_ACCOUNT_SK ON svo.F_GL_BALANCES(ACCOUNT_SK)
            INCLUDE (BUSINESS_OFFERING_SK, COMPANY_SK, COST_CENTER_SK, INDUSTRY_SK, INTERCOMPANY_SK, LEDGER_SK, BALANCE_DATE_SK, BEGIN_BALANCE_AMT, PERIOD_ACTIVITY_AMT, END_BALANCE_AMT, CURRENCY_CODE) ON FG_SilverFact;

        SET @EndDttm = SYSDATETIME();
        UPDATE etl.ETL_RUN SET END_DTTM = @EndDttm, STATUS = 'SUCCESS', ROW_INSERTED = @RowInserted, ROW_UPDATED = 0, ROW_EXPIRED = 0, ERROR_MESSAGE = NULL WHERE RUN_ID = @RunId;
    END TRY
    BEGIN CATCH
        SET @EndDttm = SYSDATETIME();
        SET @ErrMsg = CONCAT('Error ', ERROR_NUMBER(), ' (Line ', ERROR_LINE(), '): ', ERROR_MESSAGE());
        IF @RunId IS NOT NULL
            UPDATE etl.ETL_RUN SET END_DTTM = @EndDttm, STATUS = 'FAILURE', ROW_INSERTED = @RowInserted, ROW_UPDATED = 0, ROW_EXPIRED = 0, ERROR_MESSAGE = @ErrMsg WHERE RUN_ID = @RunId;
        IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'IX_F_GL_BALANCES_COMPANY_SK' AND object_id = OBJECT_ID('svo.F_GL_BALANCES'))
            CREATE NONCLUSTERED INDEX IX_F_GL_BALANCES_COMPANY_SK ON svo.F_GL_BALANCES(COMPANY_SK)
            INCLUDE (ACCOUNT_SK, BUSINESS_OFFERING_SK, COST_CENTER_SK, INDUSTRY_SK, INTERCOMPANY_SK, LEDGER_SK, BALANCE_DATE_SK, BEGIN_BALANCE_AMT, PERIOD_ACTIVITY_AMT, END_BALANCE_AMT, CURRENCY_CODE) ON FG_SilverFact;
        IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'IX_F_GL_BALANCES_ACCOUNT_SK' AND object_id = OBJECT_ID('svo.F_GL_BALANCES'))
            CREATE NONCLUSTERED INDEX IX_F_GL_BALANCES_ACCOUNT_SK ON svo.F_GL_BALANCES(ACCOUNT_SK)
            INCLUDE (BUSINESS_OFFERING_SK, COMPANY_SK, COST_CENTER_SK, INDUSTRY_SK, INTERCOMPANY_SK, LEDGER_SK, BALANCE_DATE_SK, BEGIN_BALANCE_AMT, PERIOD_ACTIVITY_AMT, END_BALANCE_AMT, CURRENCY_CODE) ON FG_SilverFact;
        ;THROW;
    END CATCH
END;
GO
