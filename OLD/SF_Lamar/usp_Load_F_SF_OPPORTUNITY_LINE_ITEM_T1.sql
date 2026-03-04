/*=================================================================================================
   svo.usp_Load_F_SF_OPPORTUNITY_LINE_ITEM_T1
   Full refresh of Salesforce Opportunity Line Item fact.
   Source: bzo.OpportunityLineItem; joins to svo.D_SF_OPPORTUNITY for OPPORTUNITY_SK.
   Grain: 1 row per OpportunityLineItem.Id
   Logging: etl.ETL_RUN
=================================================================================================*/

USE [Oracle_Reporting_P2];
GO

IF OBJECT_ID(N'svo.usp_Load_F_SF_OPPORTUNITY_LINE_ITEM_T1', N'P') IS NOT NULL
    DROP PROCEDURE svo.usp_Load_F_SF_OPPORTUNITY_LINE_ITEM_T1;
GO

CREATE PROCEDURE svo.usp_Load_F_SF_OPPORTUNITY_LINE_ITEM_T1
(
    @AsOfDate DATE = NULL
)
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    DECLARE
        @ProcName     SYSNAME       = OBJECT_SCHEMA_NAME(@@PROCID) + '.' + OBJECT_NAME(@@PROCID),
        @TargetObject SYSNAME       = N'svo.F_SF_OPPORTUNITY_LINE_ITEM',
        @RunId        BIGINT        = NULL,
        @RowInserted INT           = 0,
        @StartDttm    DATETIME2(0)  = SYSDATETIME(),
        @EndDttm      DATETIME2(0)  = NULL,
        @ErrMsg       NVARCHAR(4000)= NULL;

    SET @AsOfDate = COALESCE(@AsOfDate, CAST(GETDATE() AS DATE));

    BEGIN TRY
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
                LM_ORACLE_PART_NUMBER             NVARCHAR(50) NULL,
                IS_DELETED                        BIT NULL,
                BZ_LOAD_DATE                      DATE NULL,
                SV_LOAD_DATE                      DATE NOT NULL DEFAULT CAST(GETDATE() AS DATE),
                CONSTRAINT PK_F_SF_OPPORTUNITY_LINE_ITEM PRIMARY KEY CLUSTERED (OPPORTUNITY_LINE_ITEM_PK) ON [FG_SilverFact],
                CONSTRAINT UK_F_SF_OPPORTUNITY_LINE_ITEM_NK UNIQUE (OPPORTUNITY_LINE_ITEM_ID)
            ) ON [FG_SilverFact];

            CREATE NONCLUSTERED INDEX IX_F_SF_OPPORTUNITY_LINE_ITEM_OPPORTUNITY_SK
            ON svo.F_SF_OPPORTUNITY_LINE_ITEM (OPPORTUNITY_SK) ON [FG_SilverFact];
        END

        INSERT INTO etl.ETL_RUN (PROC_NAME, TARGET_OBJECT, ASOF_DATE, START_DTTM, STATUS)
        VALUES (@ProcName, @TargetObject, @AsOfDate, @StartDttm, 'STARTED');
        SET @RunId = SCOPE_IDENTITY();

        TRUNCATE TABLE svo.F_SF_OPPORTUNITY_LINE_ITEM;

        INSERT INTO svo.F_SF_OPPORTUNITY_LINE_ITEM
        (
            OPPORTUNITY_LINE_ITEM_ID,
            OPPORTUNITY_ID,
            OPPORTUNITY_SK,
            PRODUCT2_ID,
            PRICEBOOK_ENTRY_ID,
            PRODUCT_CODE,
            PRODUCT_NAME,
            DESCRIPTION,
            CURRENCY_ISO_CODE,
            START_DATE,
            END_DATE,
            SERVICE_DATE,
            QUANTITY,
            UNIT_PRICE,
            LIST_PRICE,
            SUBTOTAL,
            TOTAL_PRICE,
            DISCOUNT,
            RATE_PER_PERIOD,
            LM_TOTAL_INVESTMENT_PER_PERIOD,
            LM_MARKET_BUDGET,
            FEE_TYPE,
            LM_BOOKING_TYPE,
            LM_PRICE_TYPE,
            LM_PRODUCT_TYPE,
            LM_MARKET,
            LM_PANEL_NUMBER,
            LM_ORACLE_PART_NUMBER,
            IS_DELETED,
            BZ_LOAD_DATE,
            SV_LOAD_DATE
        )
        SELECT
            li.Id,
            li.OpportunityId,
            ISNULL(d.OPPORTUNITY_SK, 0) AS OPPORTUNITY_SK,
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
            CAST(COALESCE(li.CreatedDate, li.LastModifiedDate) AS DATE) AS BZ_LOAD_DATE,
            @AsOfDate AS SV_LOAD_DATE
        FROM bzo.OpportunityLineItem li
        LEFT JOIN svo.D_SF_OPPORTUNITY d ON d.OPPORTUNITY_ID = li.OpportunityId
        WHERE li.Id IS NOT NULL;

        SET @RowInserted = @@ROWCOUNT;

        SET @EndDttm = SYSDATETIME();
        UPDATE etl.ETL_RUN
        SET END_DTTM = @EndDttm, STATUS = 'SUCCESS', ROW_INSERTED = @RowInserted, ERROR_MESSAGE = NULL
        WHERE RUN_ID = @RunId;
    END TRY
    BEGIN CATCH
        SET @EndDttm = SYSDATETIME();
        SET @ErrMsg = CONCAT(N'Error ', ERROR_NUMBER(), N' (Line ', ERROR_LINE(), N'): ', ERROR_MESSAGE());
        IF @RunId IS NOT NULL
            UPDATE etl.ETL_RUN
            SET END_DTTM = @EndDttm, STATUS = 'FAILURE', ROW_INSERTED = @RowInserted, ERROR_MESSAGE = @ErrMsg
            WHERE RUN_ID = @RunId;
        ;THROW;
    END CATCH
END;
GO
