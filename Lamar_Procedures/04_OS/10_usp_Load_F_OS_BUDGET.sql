GO
/****** Object:  StoredProcedure [svo].[usp_Load_F_OS_BUDGET]    Script Date: 2/13/2026 11:09:55 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
/* =========================================================
   usp_Load_F_OS_BUDGET
   Source: bzo.OneStreamCSVDetails_MTD.
   Full refresh only (TRUNCATE then INSERT). Source has no AddDateTime; use SYSDATETIME() for load dates.
   Mappings: Account->D_ACCOUNT.ACCOUNT_ID, Entity->D_COMPANY.COMPANY_ID, UD4->D_BUSINESS_OFFERING.BUSINESS_OFFERING_ID,
   UD3->D_COST_CENTER.COST_CENTER_LVL2_CODE (Sales Category), UD2->D_COST_CENTER.COST_CENTER_LVL3_CODE (Sales Region).
   Time format YYYY'm'M (e.g. 2025M1) -> BUDGET_DATE_SK (first of month, e.g. 20250101).
   ========================================================= */
CREATE OR ALTER PROCEDURE [svo].[usp_Load_F_OS_BUDGET]
    @BatchId INT = NULL
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    DECLARE
        @ProcName       SYSNAME        = OBJECT_SCHEMA_NAME(@@PROCID) + '.' + OBJECT_NAME(@@PROCID),
        @TargetObject   SYSNAME        = 'svo.F_OS_BUDGET',
        @StartDttm      DATETIME2(0)   = SYSDATETIME(),
        @EndDttm        DATETIME2(0),
        @RunId          BIGINT         = NULL,
        @ErrMsg         NVARCHAR(4000) = NULL,
        @AsOfDate       DATE           = CAST(GETDATE() AS DATE),
        @RowInserted    INT            = 0,
        @TableBridgeID  INT            = NULL;

    SELECT @TableBridgeID = TableBridgeID FROM meta.MedallionTableBridge WHERE targettable = N'OneStreamCSVDetails_MTD';

    BEGIN TRY
        INSERT INTO etl.ETL_RUN (PROC_NAME, TARGET_OBJECT, ASOF_DATE, START_DTTM, STATUS, BATCH_ID, TABLE_BRIDGE_ID)
        VALUES (@ProcName, @TargetObject, @AsOfDate, @StartDttm, 'STARTED', ISNULL(@BatchId, -1), ISNULL(@TableBridgeID, -1));

        SET @RunId = SCOPE_IDENTITY();
        IF @RunId IS NULL
            THROW 50001, 'ETL_RUN insert failed: RUN_ID is NULL.', 1;

        PRINT 'usp_Load_F_OS_BUDGET: Full refresh. Truncating target and loading from bzo.OneStreamCSVDetails_MTD.';

        IF EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'IX_F_OS_BUDGET_COMPANY_SK' AND object_id = OBJECT_ID('svo.F_OS_BUDGET'))
            DROP INDEX IX_F_OS_BUDGET_COMPANY_SK ON svo.F_OS_BUDGET;
        IF EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'IX_F_OS_BUDGET_ACCOUNT_SK' AND object_id = OBJECT_ID('svo.F_OS_BUDGET'))
            DROP INDEX IX_F_OS_BUDGET_ACCOUNT_SK ON svo.F_OS_BUDGET;

        TRUNCATE TABLE svo.F_OS_BUDGET;

        INSERT INTO svo.F_OS_BUDGET
        (ACCOUNT_SK, COMPANY_SK, BUSINESS_OFFERING_SK, COST_CENTER_SK, BUDGET_DATE_SK, AMOUNT, BZ_LOAD_DATE, SV_LOAD_DATE)
        SELECT
            ISNULL(DA.ACCOUNT_SK, 0),
            ISNULL(DCO.COMPANY_SK,0),
            ISNULL(DBO.BUSINESS_OFFERING_SK, 0),
            ISNULL(DCC.COST_CENTER_SK, 0),
            CONVERT(INT, SUBSTRING(src.[Time], 1, 4) + RIGHT('0' + LTRIM(RTRIM(SUBSTRING(src.[Time], CHARINDEX('M', src.[Time]) + 1, 2))), 2) + '01'),
            TRY_CAST(src.Amount AS DECIMAL(29,4)),
            SYSDATETIME(),
            SYSDATETIME()
        FROM bzo.OneStreamCSVDetails_MTD src
        LEFT JOIN svo.D_ACCOUNT DA ON DA.ACCOUNT_ID = src.Account AND DA.CURR_IND = 'Y'
        LEFT JOIN svo.D_COMPANY DCO ON DCO.COMPANY_ID = src.Entity AND DCO.CURR_IND = 'Y'
        LEFT JOIN svo.D_BUSINESS_OFFERING DBO ON REPLACE( REPLACE(DBO. BUSINESS_OFFERING_LVL3_CODE, 'MIxS', 'NO_'), 'OSP', 'OTH') = LEFT(src.UD4, 3) AND DBO.CURR_IND = 'Y'
        LEFT JOIN svo.D_COST_CENTER DCC ON REPLACE(UPPER(DCC.COST_CENTER_LVL4_DESC),' ','_') = src.UD3  AND DCC.CURR_IND = 'Y' 
        WHERE TRY_CAST(src.Amount AS DECIMAL(29,4)) IS NOT NULL;
--        and DCC.COST_CENTER_LVL2_CODE = 'PFT';


        SET @RowInserted = @@ROWCOUNT;

        IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'IX_F_OS_BUDGET_COMPANY_SK' AND object_id = OBJECT_ID('svo.F_OS_BUDGET'))
            CREATE NONCLUSTERED INDEX IX_F_OS_BUDGET_COMPANY_SK ON svo.F_OS_BUDGET(COMPANY_SK)
            INCLUDE (ACCOUNT_SK, BUSINESS_OFFERING_SK, COST_CENTER_SK, BUDGET_DATE_SK, AMOUNT) ON FG_SilverFact;
        IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'IX_F_OS_BUDGET_ACCOUNT_SK' AND object_id = OBJECT_ID('svo.F_OS_BUDGET'))
            CREATE NONCLUSTERED INDEX IX_F_OS_BUDGET_ACCOUNT_SK ON svo.F_OS_BUDGET(ACCOUNT_SK)
            INCLUDE (COMPANY_SK, BUSINESS_OFFERING_SK, COST_CENTER_SK, BUDGET_DATE_SK, AMOUNT) ON FG_SilverFact;

        SET @EndDttm = SYSDATETIME();
        UPDATE etl.ETL_RUN SET END_DTTM = @EndDttm, STATUS = 'SUCCESS', ROW_INSERTED = @RowInserted, ROW_UPDATED = 0, ROW_EXPIRED = 0, ERROR_MESSAGE = NULL WHERE RUN_ID = @RunId;
        PRINT 'usp_Load_F_OS_BUDGET: Complete. Inserted ' + CAST(@RowInserted AS VARCHAR(20)) + ' rows.';
    END TRY
    BEGIN CATCH
        SET @EndDttm = SYSDATETIME();
        SET @ErrMsg = CONCAT('Error ', ERROR_NUMBER(), ' (Line ', ERROR_LINE(), '): ', ERROR_MESSAGE());
        IF @RunId IS NOT NULL
            UPDATE etl.ETL_RUN SET END_DTTM = @EndDttm, STATUS = 'FAILURE', ROW_INSERTED = @RowInserted, ROW_UPDATED = 0, ROW_EXPIRED = 0, ERROR_MESSAGE = @ErrMsg WHERE RUN_ID = @RunId;
        IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'IX_F_OS_BUDGET_COMPANY_SK' AND object_id = OBJECT_ID('svo.F_OS_BUDGET'))
            CREATE NONCLUSTERED INDEX IX_F_OS_BUDGET_COMPANY_SK ON svo.F_OS_BUDGET(COMPANY_SK)
            INCLUDE (ACCOUNT_SK, BUSINESS_OFFERING_SK, COST_CENTER_SK, BUDGET_DATE_SK, AMOUNT) ON FG_SilverFact;
        IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'IX_F_OS_BUDGET_ACCOUNT_SK' AND object_id = OBJECT_ID('svo.F_OS_BUDGET'))
            CREATE NONCLUSTERED INDEX IX_F_OS_BUDGET_ACCOUNT_SK ON svo.F_OS_BUDGET(ACCOUNT_SK)
            INCLUDE (COMPANY_SK, BUSINESS_OFFERING_SK, COST_CENTER_SK, BUDGET_DATE_SK, AMOUNT) ON FG_SilverFact;
        ;THROW;
    END CATCH
END;

