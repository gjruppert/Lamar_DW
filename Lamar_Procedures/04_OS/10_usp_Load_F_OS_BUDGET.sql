GO
/****** Object:  StoredProcedure [svo].[usp_Load_F_OS_BUDGET]    Script Date: 2/13/2026 11:09:55 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
/* =========================================================
   usp_Load_F_OS_BUDGET
   Source: bzo.OneStreamCSVDetails_MTD and bzo.OneStreamCSVDetails_MTD_2025 only. Full refresh (TRUNCATE then INSERT).
   Source has no AddDateTime; use SYSDATETIME() for load dates.
   Mappings: Account->D_ACCOUNT.ACCOUNT_ID (605860->605850 manual), Entity->D_COMPANY.COMPANY_ID, UD4->D_BUSINESS_OFFERING,
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

        PRINT 'usp_Load_F_OS_BUDGET: Full refresh. Truncating target and loading from OneStreamCSVDetails_MTD and OneStreamCSVDetails_MTD_2025.';

        IF EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'IX_F_OS_BUDGET_COMPANY_SK' AND object_id = OBJECT_ID('svo.F_OS_BUDGET'))
            DROP INDEX IX_F_OS_BUDGET_COMPANY_SK ON svo.F_OS_BUDGET;
        IF EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'IX_F_OS_BUDGET_ACCOUNT_SK' AND object_id = OBJECT_ID('svo.F_OS_BUDGET'))
            DROP INDEX IX_F_OS_BUDGET_ACCOUNT_SK ON svo.F_OS_BUDGET;

        TRUNCATE TABLE svo.F_OS_BUDGET;

        with DBO as ( SELECT *
         FROM
         DW_BronzeSilver_DEV1.svo. D_BUSINESS_OFFERING BO
         WHERE 1=1
         AND BO. BUSINESS_OFFERING_LVL5_CODE IN (SELECT MIN(BO. BUSINESS_OFFERING_LVL5_CODE) FROM DW_BronzeSilver_DEV1.svo.D_BUSINESS_OFFERING BO GROUP BY BO.BUSINESS_OFFERING_LVL3_CODE)
      )

        INSERT INTO svo.F_OS_BUDGET WITH (TABLOCK)
        (ACCOUNT_SK, COMPANY_SK, BUSINESS_OFFERING_SK, COST_CENTER_SK, BUDGET_DATE_SK, AMOUNT, BZ_LOAD_DATE, SV_LOAD_DATE)
          SELECT 
                ISNULL(DA.ACCOUNT_SK, 0) account_sk,
                ISNULL(DCO.COMPANY_SK, 0) company_sk,
                ISNULL(DBO.BUSINESS_OFFERING_SK, 0) bo_sk,
                ISNULL(DCC.COST_CENTER_SK, 0) cost_center_sk,
                CONVERT(INT, SUBSTRING(src.[Time], 1, 4) + RIGHT('0' + LTRIM(RTRIM(SUBSTRING(src.[Time], CHARINDEX('M', src.[Time]) + 1, 2))), 2) + '01') dt_sk,
                TRY_CAST(src.Amount AS DECIMAL(29,4)) amt,
                SYSDATETIME(),
                SYSDATETIME()
            FROM (
                SELECT Account, Entity, UD4, UD3, [Time], Amount FROM bzo.OneStreamCSVDetails_MTD WITH (NOLOCK)
                UNION
                SELECT Account, Entity, UD4, UD3, [Time], Amount FROM bzo.OneStreamCSVDetails_MTD_2025 WITH (NOLOCK)
            ) src
            JOIN svo.D_ACCOUNT DA ON DA.ACCOUNT_ID = CASE WHEN TRIM(src.Account) = '605860' THEN '605850' ELSE TRIM(src.Account) END AND DA.CURR_IND = 'Y'
            LEFT JOIN svo.D_COMPANY DCO ON DCO.COMPANY_ID = src.Entity AND DCO.CURR_IND = 'Y'
            LEFT JOIN  DBO ON REPLACE(REPLACE(DBO.BUSINESS_OFFERING_LVL3_CODE, 'MIxS', 'NO_'), 'OSP', 'OTH') = LEFT(src.UD4, 3) AND DBO.CURR_IND = 'Y'
            LEFT JOIN svo.D_COST_CENTER DCC ON REPLACE(UPPER(DCC.COST_CENTER_LVL4_DESC), ' ', '_') = src.UD3 AND DCC.CURR_IND = 'Y'
            WHERE TRY_CAST(src.Amount AS DECIMAL(29,4)) IS NOT NULL
              AND TRIM(src.Account) IS NOT NULL

        SET @RowInserted = @@ROWCOUNT;

        IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'IX_F_OS_BUDGET_COMPANY_SK' AND object_id = OBJECT_ID('svo.F_OS_BUDGET'))
            CREATE NONCLUSTERED INDEX IX_F_OS_BUDGET_COMPANY_SK ON svo.F_OS_BUDGET(COMPANY_SK)
            INCLUDE (ACCOUNT_SK, BUSINESS_OFFERING_SK, COST_CENTER_SK, BUDGET_DATE_SK, AMOUNT) ON FG_SilverFact;
        IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'IX_F_OS_BUDGET_ACCOUNT_SK' AND object_id = OBJECT_ID('svo.F_OS_BUDGET'))
            CREATE NONCLUSTERED INDEX IX_F_OS_BUDGET_ACCOUNT_SK ON svo.F_OS_BUDGET(ACCOUNT_SK)
            INCLUDE (COMPANY_SK, BUSINESS_OFFERING_SK, COST_CENTER_SK, BUDGET_DATE_SK, AMOUNT) ON FG_SilverFact;

        UPDATE etl.ETL_WATERMARK SET LAST_WATERMARK = CAST(GETDATE() AS DATETIME2(7)), UDT_DATE = SYSDATETIME() WHERE TABLE_NAME = @TargetObject;

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

