/*=================================================================================================
   svo.usp_Load_F_SF_OPPORTUNITY_LINE_ITEM
   Incremental load: full reload when LastWatermark = 1900-01-01; else MERGE (insert new, update
   changed) using SystemModstamp > @LastWatermark. Grain: 1 row per OpportunityLineItem.Id.
   Source: bzo.SF_OpportunityLineItem; joins to svo.D_SF_OPPORTUNITY for OPPORTUNITY_SK.
   Full reload: drops UK and IX before TRUNCATE+INSERT, recreates after (faster bulk load).
   Logging: etl.ETL_RUN, etl.ETL_WATERMARK
=================================================================================================*/



IF OBJECT_ID(N'svo.usp_Load_F_SF_OPPORTUNITY_LINE_ITEM', N'P') IS NOT NULL
    DROP PROCEDURE svo.usp_Load_F_SF_OPPORTUNITY_LINE_ITEM;
GO

CREATE OR ALTER PROCEDURE svo.usp_Load_F_SF_OPPORTUNITY_LINE_ITEM
(
    @AsOfDate DATE = NULL,
    @BatchId INT = NULL
)
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    DECLARE
        @ProcName      SYSNAME        = OBJECT_SCHEMA_NAME(@@PROCID) + '.' + OBJECT_NAME(@@PROCID),
        @TargetObject  SYSNAME        = N'svo.F_SF_OPPORTUNITY_LINE_ITEM',
        @RunId         BIGINT         = NULL,
        @RowInserted   INT             = 0,
        @RowUpdated    INT             = 0,
        @StartDttm     DATETIME2(0)    = SYSDATETIME(),
        @EndDttm       DATETIME2(0)    = NULL,
        @ErrMsg        NVARCHAR(4000)  = NULL,
        @LastWatermark DATETIME2(7)    = NULL,
        @MaxWatermark  DATETIME2(7)    = NULL,
        @TableBridgeID INT             = NULL;

    DECLARE @MergeOutput TABLE (action NVARCHAR(10));

    SET @AsOfDate = COALESCE(@AsOfDate, CAST(GETDATE() AS DATE));

    SELECT @TableBridgeID = TableBridgeID FROM meta.MedallionTableBridge WHERE targettable = N'SF_OpportunityLineItem';

    BEGIN TRY
        SELECT @LastWatermark = w.LAST_WATERMARK
        FROM etl.ETL_WATERMARK w
        WHERE w.TABLE_NAME = @TargetObject;

        IF @LastWatermark IS NULL
            SET @LastWatermark = '1900-01-01';

        /* ----- Ensure target table exists ----- */
        IF OBJECT_ID(@TargetObject, N'U') IS NULL
        BEGIN
            CREATE TABLE svo.F_SF_OPPORTUNITY_LINE_ITEM
            (
                OPPORTUNITY_LINE_ITEM_PK          BIGINT IDENTITY(1,1) NOT NULL,
                OPPORTUNITY_LINE_ITEM_ID          NVARCHAR(18) NOT NULL,
                OPPORTUNITY_ID                    NVARCHAR(18) NULL,
                OPPORTUNITY_SK                    BIGINT NOT NULL,
                PRODUCT2_ID                       NVARCHAR(18) NULL,
                PRICEBOOK_ENTRY_ID                NVARCHAR(18) NULL,
                PRODUCT_CODE                      NVARCHAR(255) NULL,
                PRODUCT_NAME                      NVARCHAR(376) NULL,
                DESCRIPTION                       NVARCHAR(255) NULL,
                CURRENCY_ISO_CODE                 NVARCHAR(255) NULL,
                START_DATE                        DATE NULL,
                END_DATE                          DATE NULL,
                SERVICE_DATE                      DATE NULL,
                QUANTITY                          FLOAT NULL,
                UNIT_PRICE                        NUMERIC(18,2) NULL,
                LIST_PRICE                        NUMERIC(18,2) NULL,
                SUBTOTAL                          NUMERIC(18,2) NULL,
                TOTAL_PRICE                       NUMERIC(18,2) NULL,
                DISCOUNT                          FLOAT NULL,
                RATE_PER_PERIOD                   NUMERIC(18,2) NULL,
                LM_TOTAL_INVESTMENT_PER_PERIOD    NUMERIC(18,2) NULL,
                LM_MARKET_BUDGET                  NUMERIC(18,2) NULL,
                FEE_TYPE                          NVARCHAR(255) NULL,
                LM_BOOKING_TYPE                   NVARCHAR(20) NULL,
                LM_PRICE_TYPE                     NVARCHAR(10) NULL,
                LM_PRODUCT_TYPE                   NVARCHAR(255) NULL,
                LM_MARKET                         NVARCHAR(50) NULL,
                LM_PANEL_NUMBER                   NVARCHAR(1300) NULL,
                LM_ORACLE_PART_NUMBER             NVARCHAR(100) NULL,
                IS_DELETED                        BIT NULL,
                BZ_LOAD_DATE                      DATETIME2(0) NULL,
                SV_LOAD_DATE                      DATETIME2(0) NOT NULL DEFAULT SYSDATETIME(),
                CONSTRAINT PK_F_SF_OPPORTUNITY_LINE_ITEM PRIMARY KEY CLUSTERED (OPPORTUNITY_LINE_ITEM_PK) ON [PRIMARY],
                CONSTRAINT UK_F_SF_OPPORTUNITY_LINE_ITEM_NK UNIQUE (OPPORTUNITY_LINE_ITEM_ID)
            ) ON [PRIMARY];

            CREATE NONCLUSTERED INDEX IX_F_SF_OPPORTUNITY_LINE_ITEM_OPPORTUNITY_SK
            ON svo.F_SF_OPPORTUNITY_LINE_ITEM (OPPORTUNITY_SK) ON [PRIMARY];
        END

        INSERT INTO etl.ETL_RUN (PROC_NAME, TARGET_OBJECT, ASOF_DATE, START_DTTM, STATUS, BATCH_ID, TABLE_BRIDGE_ID)
        VALUES (@ProcName, @TargetObject, @AsOfDate, @StartDttm, 'STARTED', ISNULL(@BatchId, -1), ISNULL(@TableBridgeID, -1));
        SET @RunId = SCOPE_IDENTITY();

        IF @LastWatermark = '1900-01-01'
        BEGIN
            /* Full reload: drop nonclustered indexes for faster bulk insert, recreate after */
            IF EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'IX_F_SF_OPPORTUNITY_LINE_ITEM_OPPORTUNITY_SK' AND object_id = OBJECT_ID('svo.F_SF_OPPORTUNITY_LINE_ITEM'))
                DROP INDEX IX_F_SF_OPPORTUNITY_LINE_ITEM_OPPORTUNITY_SK ON svo.F_SF_OPPORTUNITY_LINE_ITEM;
            IF EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'UK_F_SF_OPPORTUNITY_LINE_ITEM_NK' AND object_id = OBJECT_ID('svo.F_SF_OPPORTUNITY_LINE_ITEM'))
                ALTER TABLE svo.F_SF_OPPORTUNITY_LINE_ITEM DROP CONSTRAINT UK_F_SF_OPPORTUNITY_LINE_ITEM_NK;

            TRUNCATE TABLE svo.F_SF_OPPORTUNITY_LINE_ITEM;

            INSERT INTO svo.F_SF_OPPORTUNITY_LINE_ITEM
            (
                OPPORTUNITY_LINE_ITEM_ID, OPPORTUNITY_ID, OPPORTUNITY_SK,
                PRODUCT2_ID, PRICEBOOK_ENTRY_ID, PRODUCT_CODE, PRODUCT_NAME, DESCRIPTION,
                CURRENCY_ISO_CODE, START_DATE, END_DATE, SERVICE_DATE,
                QUANTITY, UNIT_PRICE, LIST_PRICE, SUBTOTAL, TOTAL_PRICE, DISCOUNT,
                RATE_PER_PERIOD, LM_TOTAL_INVESTMENT_PER_PERIOD, LM_MARKET_BUDGET,
                FEE_TYPE, LM_BOOKING_TYPE, LM_PRICE_TYPE, LM_PRODUCT_TYPE, LM_MARKET,
                LM_PANEL_NUMBER, LM_ORACLE_PART_NUMBER, IS_DELETED, BZ_LOAD_DATE, SV_LOAD_DATE
            )
            SELECT
                li.Id,
                li.OpportunityId,
                ISNULL(d.OPPORTUNITY_SK, 0),
                li.Product2Id,
                li.PricebookEntryId,
                li.ProductCode,
                li.Name,
                li.Description,
                li.CurrencyIsoCode,
                CAST(li.Start_Date__c AS DATE),
                CAST(li.End_Date__c AS DATE),
                CAST(li.ServiceDate AS DATE),
                li.Quantity,
                li.UnitPrice,
                li.ListPrice,
                li.Subtotal,
                li.TotalPrice,
                li.Discount,
                li.Rate_Per_Period__c,
                li.LM_totalInvestmentPerPeriod__c,
                li.LM_Market_Budget__c,
                li.Fee_Type__c,
                li.LM_Booking_Type__c,
                li.LM_Price_Type__c,
                li.LM_Product_Type__c,
                li.LM_Market__c,
                li.LM_Panel_Number__c,
                li.LM_Oracle_Part_Number__c,
                li.IsDeleted,
                CAST(COALESCE(li.CreatedDate, li.LastModifiedDate) AS DATETIME2(0)),
                SYSDATETIME()
            FROM bzo.SF_OpportunityLineItem li
            LEFT JOIN svo.D_SF_OPPORTUNITY d ON d.OPPORTUNITY_ID = li.OpportunityId
            WHERE li.Id IS NOT NULL;

            SET @RowInserted = @@ROWCOUNT;

            /* Recreate indexes after bulk insert */
            IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'UK_F_SF_OPPORTUNITY_LINE_ITEM_NK' AND object_id = OBJECT_ID('svo.F_SF_OPPORTUNITY_LINE_ITEM'))
                ALTER TABLE svo.F_SF_OPPORTUNITY_LINE_ITEM ADD CONSTRAINT UK_F_SF_OPPORTUNITY_LINE_ITEM_NK UNIQUE (OPPORTUNITY_LINE_ITEM_ID);
            IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'IX_F_SF_OPPORTUNITY_LINE_ITEM_OPPORTUNITY_SK' AND object_id = OBJECT_ID('svo.F_SF_OPPORTUNITY_LINE_ITEM'))
                CREATE NONCLUSTERED INDEX IX_F_SF_OPPORTUNITY_LINE_ITEM_OPPORTUNITY_SK ON svo.F_SF_OPPORTUNITY_LINE_ITEM (OPPORTUNITY_SK) ON FG_SilverFact;

            SELECT @MaxWatermark = MAX(COALESCE(li.SystemModstamp, li.LastModifiedDate, li.CreatedDate))
            FROM bzo.SF_OpportunityLineItem li
            WHERE li.Id IS NOT NULL;

            /* Ensure watermark is set even when source is empty */
            IF @MaxWatermark IS NULL
                SET @MaxWatermark = CAST(GETDATE() AS DATETIME2(7));
        END
        ELSE
        BEGIN
            /* Incremental: source rows changed since watermark (dedupe by Id, latest SystemModstamp) */
            IF OBJECT_ID('tempdb..#src') IS NOT NULL DROP TABLE #src;

            ;WITH src AS (
                SELECT
                    li.Id,
                    li.OpportunityId,
                    li.Product2Id,
                    li.PricebookEntryId,
                    li.ProductCode,
                    li.Name,
                    li.Description,
                    li.CurrencyIsoCode,
                    li.Start_Date__c,
                    li.End_Date__c,
                    li.ServiceDate,
                    li.Quantity,
                    li.UnitPrice,
                    li.ListPrice,
                    li.Subtotal,
                    li.TotalPrice,
                    li.Discount,
                    li.Rate_Per_Period__c,
                    li.LM_totalInvestmentPerPeriod__c,
                    li.LM_Market_Budget__c,
                    li.Fee_Type__c,
                    li.LM_Booking_Type__c,
                    li.LM_Price_Type__c,
                    li.LM_Product_Type__c,
                    li.LM_Market__c,
                    li.LM_Panel_Number__c,
                    li.LM_Oracle_Part_Number__c,
                    li.IsDeleted,
                    li.CreatedDate,
                    li.LastModifiedDate,
                    COALESCE(li.SystemModstamp, li.LastModifiedDate, li.CreatedDate) AS WatermarkVal,
                    ROW_NUMBER() OVER (PARTITION BY li.Id ORDER BY COALESCE(li.SystemModstamp, li.LastModifiedDate, li.CreatedDate) DESC) AS rn
                FROM bzo.SF_OpportunityLineItem li
                WHERE li.Id IS NOT NULL
                  AND COALESCE(li.SystemModstamp, li.LastModifiedDate, li.CreatedDate) > @LastWatermark
            )
            SELECT
                Id, OpportunityId, Product2Id, PricebookEntryId, ProductCode, Name, Description,
                CurrencyIsoCode, Start_Date__c, End_Date__c, ServiceDate, Quantity, UnitPrice,
                ListPrice, Subtotal, TotalPrice, Discount, Rate_Per_Period__c,
                LM_totalInvestmentPerPeriod__c, LM_Market_Budget__c, Fee_Type__c,
                LM_Booking_Type__c, LM_Price_Type__c, LM_Product_Type__c, LM_Market__c,
                LM_Panel_Number__c, LM_Oracle_Part_Number__c, IsDeleted, CreatedDate, LastModifiedDate, WatermarkVal
            INTO #src
            FROM src
            WHERE rn = 1;

            SELECT @MaxWatermark = MAX(WatermarkVal) FROM #src;

            MERGE svo.F_SF_OPPORTUNITY_LINE_ITEM AS tgt
            USING (
                SELECT
                    s.Id,
                    s.OpportunityId,
                    ISNULL(d.OPPORTUNITY_SK, 0) AS OPPORTUNITY_SK,
                    s.Product2Id,
                    s.PricebookEntryId,
                    s.ProductCode,
                    s.Name,
                    s.Description,
                    s.CurrencyIsoCode,
                    CAST(s.Start_Date__c AS DATE) AS START_DATE,
                    CAST(s.End_Date__c AS DATE) AS END_DATE,
                    CAST(s.ServiceDate AS DATE) AS SERVICE_DATE,
                    s.Quantity,
                    s.UnitPrice,
                    s.ListPrice,
                    s.Subtotal,
                    s.TotalPrice,
                    s.Discount,
                    s.Rate_Per_Period__c,
                    s.LM_totalInvestmentPerPeriod__c,
                    s.LM_Market_Budget__c,
                    s.Fee_Type__c,
                    s.LM_Booking_Type__c,
                    s.LM_Price_Type__c,
                    s.LM_Product_Type__c,
                    s.LM_Market__c,
                    s.LM_Panel_Number__c,
                    s.LM_Oracle_Part_Number__c,
                    s.IsDeleted,
                    CAST(COALESCE(s.CreatedDate, s.LastModifiedDate) AS DATETIME) AS BZ_LOAD_DATE,
                    SYSDATETIME() AS SV_LOAD_DATE
                FROM #src s
                LEFT JOIN svo.D_SF_OPPORTUNITY d ON d.OPPORTUNITY_ID = s.OpportunityId
            ) AS src
            ON tgt.OPPORTUNITY_LINE_ITEM_ID = src.Id
            WHEN MATCHED THEN
                UPDATE SET
                    tgt.OPPORTUNITY_ID = src.OpportunityId,
                    tgt.OPPORTUNITY_SK = src.OPPORTUNITY_SK,
                    tgt.PRODUCT2_ID = src.Product2Id,
                    tgt.PRICEBOOK_ENTRY_ID = src.PricebookEntryId,
                    tgt.PRODUCT_CODE = src.ProductCode,
                    tgt.PRODUCT_NAME = src.Name,
                    tgt.DESCRIPTION = src.Description,
                    tgt.CURRENCY_ISO_CODE = src.CurrencyIsoCode,
                    tgt.START_DATE = src.START_DATE,
                    tgt.END_DATE = src.END_DATE,
                    tgt.SERVICE_DATE = src.SERVICE_DATE,
                    tgt.QUANTITY = src.Quantity,
                    tgt.UNIT_PRICE = src.UnitPrice,
                    tgt.LIST_PRICE = src.ListPrice,
                    tgt.SUBTOTAL = src.Subtotal,
                    tgt.TOTAL_PRICE = src.TotalPrice,
                    tgt.DISCOUNT = src.Discount,
                    tgt.RATE_PER_PERIOD = src.Rate_Per_Period__c,
                    tgt.LM_TOTAL_INVESTMENT_PER_PERIOD = src.LM_totalInvestmentPerPeriod__c,
                    tgt.LM_MARKET_BUDGET = src.LM_Market_Budget__c,
                    tgt.FEE_TYPE = src.Fee_Type__c,
                    tgt.LM_BOOKING_TYPE = src.LM_Booking_Type__c,
                    tgt.LM_PRICE_TYPE = src.LM_Price_Type__c,
                    tgt.LM_PRODUCT_TYPE = src.LM_Product_Type__c,
                    tgt.LM_MARKET = src.LM_Market__c,
                    tgt.LM_PANEL_NUMBER = src.LM_Panel_Number__c,
                    tgt.LM_ORACLE_PART_NUMBER = src.LM_Oracle_Part_Number__c,
                    tgt.IS_DELETED = src.IsDeleted,
                    tgt.BZ_LOAD_DATE = src.BZ_LOAD_DATE,
                    tgt.SV_LOAD_DATE = src.SV_LOAD_DATE
            WHEN NOT MATCHED BY TARGET THEN
                INSERT (
                    OPPORTUNITY_LINE_ITEM_ID, OPPORTUNITY_ID, OPPORTUNITY_SK,
                    PRODUCT2_ID, PRICEBOOK_ENTRY_ID, PRODUCT_CODE, PRODUCT_NAME, DESCRIPTION,
                    CURRENCY_ISO_CODE, START_DATE, END_DATE, SERVICE_DATE,
                    QUANTITY, UNIT_PRICE, LIST_PRICE, SUBTOTAL, TOTAL_PRICE, DISCOUNT,
                    RATE_PER_PERIOD, LM_TOTAL_INVESTMENT_PER_PERIOD, LM_MARKET_BUDGET,
                    FEE_TYPE, LM_BOOKING_TYPE, LM_PRICE_TYPE, LM_PRODUCT_TYPE, LM_MARKET,
                    LM_PANEL_NUMBER, LM_ORACLE_PART_NUMBER, IS_DELETED, BZ_LOAD_DATE, SV_LOAD_DATE
                )
                VALUES (
                    src.Id, src.OpportunityId, src.OPPORTUNITY_SK,
                    src.Product2Id, src.PricebookEntryId, src.ProductCode, src.Name, src.Description,
                    src.CurrencyIsoCode, src.START_DATE, src.END_DATE, src.SERVICE_DATE,
                    src.Quantity, src.UnitPrice, src.ListPrice, src.Subtotal, src.TotalPrice, src.Discount,
                    src.Rate_Per_Period__c, src.LM_totalInvestmentPerPeriod__c, src.LM_Market_Budget__c,
                    src.Fee_Type__c, src.LM_Booking_Type__c, src.LM_Price_Type__c, src.LM_Product_Type__c, src.LM_Market__c,
                    src.LM_Panel_Number__c, src.LM_Oracle_Part_Number__c, src.IsDeleted, src.BZ_LOAD_DATE, src.SV_LOAD_DATE
                )
            OUTPUT $action INTO @MergeOutput;

            SET @RowInserted = (SELECT COUNT(*) FROM @MergeOutput WHERE action = 'INSERT');
            SET @RowUpdated  = (SELECT COUNT(*) FROM @MergeOutput WHERE action = 'UPDATE');
        END

        IF @MaxWatermark IS NOT NULL
        BEGIN
            MERGE etl.ETL_WATERMARK AS tgt
            USING (SELECT @TargetObject AS TABLE_NAME, @MaxWatermark AS LAST_WATERMARK) AS src
            ON tgt.TABLE_NAME = src.TABLE_NAME
            WHEN MATCHED THEN
                UPDATE SET tgt.LAST_WATERMARK = src.LAST_WATERMARK, tgt.UDT_DATE = SYSDATETIME()
            WHEN NOT MATCHED BY TARGET THEN
                INSERT (TABLE_NAME, LAST_WATERMARK) VALUES (src.TABLE_NAME, src.LAST_WATERMARK);
        END

        SET @EndDttm = SYSDATETIME();
        UPDATE etl.ETL_RUN
        SET END_DTTM = @EndDttm, STATUS = 'SUCCESS', ROW_INSERTED = @RowInserted, ROW_UPDATED = @RowUpdated, ERROR_MESSAGE = NULL
        WHERE RUN_ID = @RunId;
    END TRY
    BEGIN CATCH
        SET @EndDttm = SYSDATETIME();
        SET @ErrMsg = CONCAT(N'Error ', ERROR_NUMBER(), N' (Line ', ERROR_LINE(), N'): ', ERROR_MESSAGE());
        IF @RunId IS NOT NULL
            UPDATE etl.ETL_RUN
            SET END_DTTM = @EndDttm, STATUS = 'FAILURE', ROW_INSERTED = @RowInserted, ROW_UPDATED = @RowUpdated, ERROR_MESSAGE = @ErrMsg
            WHERE RUN_ID = @RunId;
        /* Restore indexes if full reload dropped them, so table is left in consistent state */
        IF @LastWatermark = '1900-01-01'
        BEGIN
            IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'UK_F_SF_OPPORTUNITY_LINE_ITEM_NK' AND object_id = OBJECT_ID('svo.F_SF_OPPORTUNITY_LINE_ITEM'))
                ALTER TABLE svo.F_SF_OPPORTUNITY_LINE_ITEM ADD CONSTRAINT UK_F_SF_OPPORTUNITY_LINE_ITEM_NK UNIQUE (OPPORTUNITY_LINE_ITEM_ID);
            IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'IX_F_SF_OPPORTUNITY_LINE_ITEM_OPPORTUNITY_SK' AND object_id = OBJECT_ID('svo.F_SF_OPPORTUNITY_LINE_ITEM'))
                CREATE NONCLUSTERED INDEX IX_F_SF_OPPORTUNITY_LINE_ITEM_OPPORTUNITY_SK ON svo.F_SF_OPPORTUNITY_LINE_ITEM (OPPORTUNITY_SK) ON FG_SilverFact;
        END
        ;THROW;
    END CATCH
END;
GO
