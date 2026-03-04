/*=================================================================================================
   svo.usp_Load_D_SF_OPPORTUNITY
   Type-1 load for Salesforce Opportunity dimension.
   Source:  bzo.SF_Opportunity, bzo.SF_OpportunityLineItem (rollup for TOTAL_PRICE).
   Grain:   1 row per Opportunity.Id
   Prereq:  svo.D_SF_OPPORTUNITY must exist (run DDL or Create_Tables_DDL.sql first).
   Logging: etl.ETL_RUN, etl.ETL_WATERMARK (advances to MAX(SystemModstamp) from bzo.SF_Opportunity after success).
=================================================================================================*/


IF OBJECT_ID(N'svo.usp_Load_D_SF_OPPORTUNITY', N'P') IS NOT NULL
    DROP PROCEDURE svo.usp_Load_D_SF_OPPORTUNITY;
GO

CREATE OR ALTER PROCEDURE svo.usp_Load_D_SF_OPPORTUNITY
(
    @AsOfDate DATE = NULL,
    @BatchId INT = NULL
)
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    DECLARE
        @ProcName     SYSNAME       = OBJECT_SCHEMA_NAME(@@PROCID) + '.' + OBJECT_NAME(@@PROCID),
        @TargetObject SYSNAME       = N'svo.D_SF_OPPORTUNITY',
        @RunId        BIGINT        = NULL,
        @RowInserted INT           = 0,
        @RowUpdated   INT           = 0,
        @StartDttm    DATETIME2(0)   = SYSDATETIME(),
        @EndDttm      DATETIME2(0)   = NULL,
        @ErrMsg       NVARCHAR(4000)= NULL,
        @MaxWatermark DATETIME2(7)   = NULL,
        @TableBridgeID INT          = NULL;

    SET @AsOfDate = COALESCE(@AsOfDate, CAST(GETDATE() AS DATE));

    SELECT @TableBridgeID = TableBridgeID FROM meta.MedallionTableBridge WHERE targettable = N'SF_Opportunity';

    BEGIN TRY
        IF OBJECT_ID(@TargetObject, N'U') IS NULL
        BEGIN
            RAISERROR(N'svo.D_SF_OPPORTUNITY does not exist. Run D_SF_OPPORTUNITY.sql DDL first.', 16, 1);
            RETURN;
        END

        INSERT INTO etl.ETL_RUN (PROC_NAME, TARGET_OBJECT, ASOF_DATE, START_DTTM, STATUS, BATCH_ID, TABLE_BRIDGE_ID)
        VALUES (@ProcName, @TargetObject, @AsOfDate, @StartDttm, 'STARTED', ISNULL(@BatchId, -1), ISNULL(@TableBridgeID, -1));
        SET @RunId = SCOPE_IDENTITY();

        /* ----- Plug row (SK=0) ----- */
        IF NOT EXISTS (SELECT 1 FROM svo.D_SF_OPPORTUNITY WHERE OPPORTUNITY_SK = 0)
        BEGIN
            SET IDENTITY_INSERT svo.D_SF_OPPORTUNITY ON;
            INSERT INTO svo.D_SF_OPPORTUNITY
            (OPPORTUNITY_SK, OPPORTUNITY_ID, OPPORTUNITY_NAME, STAGE_NAME, IS_DELETED, BZ_LOAD_DATE, SV_LOAD_DATE)
            VALUES (0, N'-1', N'Unknown Opportunity', N'Unknown', 0, CAST(@AsOfDate AS DATETIME), CAST(@AsOfDate AS DATETIME));
            SET IDENTITY_INSERT svo.D_SF_OPPORTUNITY OFF;
        END

        /* ----- Staging: line rollup ----- */
        IF OBJECT_ID('tempdb..#LineAgg','U') IS NOT NULL DROP TABLE #LineAgg;
        SELECT
            OpportunityId = oli.OpportunityId,
            LINE_TOTALPRICE = SUM(COALESCE(oli.TotalPrice, 0))
        INTO #LineAgg
        FROM bzo.SF_OpportunityLineItem oli
        WHERE oli.OpportunityId IS NOT NULL
          AND (oli.IsDeleted = 0 OR oli.IsDeleted IS NULL)
        GROUP BY oli.OpportunityId;
        CREATE UNIQUE CLUSTERED INDEX CX_LineAgg ON #LineAgg (OpportunityId);

        /* ----- Staging: source with HASHDIFF ----- */
        IF OBJECT_ID('tempdb..#SrcOpportunity','U') IS NOT NULL DROP TABLE #SrcOpportunity;
        SELECT
            OPPORTUNITY_ID = o.Id,
            ACCOUNT_ID = o.AccountId,
            AMOUNT = o.Amount,
            BUDGET_CONFIRMED = o.Budget_Confirmed__c,
            CAFSL_DATA_SET_EXCLUDE_LIST = o.cafsl__Data_Set_Exclude_List__c,
            CAMPAIGN_END_DATE = o.Campaign_End_Date__c,
            CAMPAIGN_START_DATE = o.Campaign_Start_Date__c,
            CAMPAIGN_ID = o.CampaignId,
            CLOSE_DATE = o.CloseDate,
            CONTACT_ID = o.ContactId,
            CONTRACT_ID = o.ContractId,
            CREATED_BY_ID = o.CreatedById,
            CREATED_DATE = o.CreatedDate,
            CURRENCY_ISO_CODE = o.CurrencyIsoCode,
            CUSTOMER_ID = o.Customer__c,
            DAYS_SINCE_LAST_ACTIVITY = o.Days_Since_Last_Activity__c,
            DESCRIPTION = o.Description,
            DISCOVERY_COMPLETED = o.Discovery_Completed__c,
            DOZISF_ZOOMINFO_OPSOS_APP_FIELD = o.DOZISF__ZoomInfo_Opsos_App_Field__c,
            DOZISF_ZOOMINFO_OPSOS_CURRENT_ENDPOINT = o.DOZISF__ZoomInfo_Opsos_Current_Endpoint__c,
            DOZISF_ZOOMINFO_OPSOS_LAST_PROCESSED_DATE = o.DOZISF__ZoomInfo_Opsos_Last_Processed_Date__c,
            EXPECTED_REVENUE = o.ExpectedRevenue,
            FIELD_HISTORY_DATE = o.Field_History_Date__c,
            FISCAL = o.Fiscal,
            FISCAL_QUARTER = o.FiscalQuarter,
            FISCAL_YEAR = o.FiscalYear,
            FORECAST_MANAGER_ID = o.Forecast_Manager__c,
            FORECAST_CATEGORY = o.ForecastCategory,
            FORECAST_CATEGORY_NAME = o.ForecastCategoryName,
            HAS_OPEN_ACTIVITY = o.HasOpenActivity,
            HAS_OPPORTUNITY_LINE_ITEM = o.HasOpportunityLineItem,
            HAS_OVERDUE_TASK = o.HasOverdueTask,
            INDUSTRY = o.Industry__c,
            IS_CLOSED = o.IsClosed,
            IS_DELETED = o.IsDeleted,
            IS_EXCLUDED_FROM_TERRITORY2_FILTER = o.IsExcludedFromTerritory2Filter,
            IS_PRIVATE = o.IsPrivate,
            IS_SPLIT = o.IsSplit,
            IS_WON = o.IsWon,
            LAST_TASK_DATE = o.Last_Task_Date__c,
            LAST_ACTIVITY_DATE = o.LastActivityDate,
            LAST_AMOUNT_CHANGED_HISTORY_ID = o.LastAmountChangedHistoryId,
            LAST_CLOSE_DATE_CHANGED_HISTORY_ID = o.LastCloseDateChangedHistoryId,
            LAST_MODIFIED_BY_ID = o.LastModifiedById,
            LAST_MODIFIED_DATE = o.LastModifiedDate,
            LAST_REFERENCED_DATE = o.LastReferencedDate,
            LAST_STAGE_CHANGE_DATE = o.LastStageChangeDate,
            LAST_VIEWED_DATE = o.LastViewedDate,
            LEAD_SOURCE = o.LeadSource,
            LID_LINKEDIN_COMPANY_ID = o.LID__LinkedIn_Company_Id__c,
            LM_ACCOUNTTYPE = o.LM_AccountType__c,
            LM_ADDITIONAL_RFP_DETAILS = o.LM_Additional_RFP_Details__c,
            LM_AE_CODE = o.LM_AE_Code__c,
            LM_BRAND_INDUSTRY = o.LM_Brand_Industry__c,
            LM_BRAND_NAME = o.LM_Brand_Name__c,
            LM_BUSINESS_UNIT = o.LM_Business_Unit__c,
            LM_COLLABORATIVE = o.lm_collaborative__c,
            LM_CPQ_TRANSACTION_NUMBER = o.LM_CPQ_Transaction_Number__c,
            LM_CUSTOMER_PO_NUMBER = o.LM_Customer_PO_Number__c,
            LM_ESTIMATED_AMOUNT = o.LM_Estimated_Amount__c,
            LM_INDUSTRY_CATEGORY = o.LM_Industry_Category__c,
            LM_INVENTORY_ON_HOLD = o.LM_Inventory_on_Hold__c,
            LM_IS_TEAM_MATCH = o.LM_Is_Team_Match__c,
            LM_LAST_ACTIVITY_DATE = o.LM_Last_Activity_Date__c,
            LM_NAICS_CODE = o.LM_NAICS_Code__c,
            LM_OFFICE = o.LM_Office__c,
            LM_PRIMARY_ORACLE_QUOTE_ID = o.LM_Primary_Oracle_Quote__c,
            LM_QUOTE_STATUS = o.LM_Quote_Status__c,
            LM_RELATED_OPPORTUNITY_ID = o.LM_Related_Opportunity__c,
            LM_RESPONSE_DUE_DATETIME = o.LM_Response_Due_DateTime__c,
            LM_RESPONSE_DUE_DATETIME_CPQ = o.LM_Response_Due_DateTime_CPQ__c,
            LM_SALES_CATEGORY = o.LM_Sales_Category__c,
            LM_TCV_AIRPORT = o.LM_TCV_Airport__c,
            LM_TCV_LOGOS = o.LM_TCV_Logos__c,
            LM_TCV_OUTDOOR = o.LM_TCV_Outdoor__c,
            LM_TCV_TRANSIT = o.LM_TCV_Transit__c,
            LM_TOTAL_CONTRACT_VALUE = o.LM_Total_Contract_Value__c,
            LM_TRANSACTION_TYPE = o.LM_Transaction_Type__c,
            LOSS_REASON = o.Loss_Reason__c,
            NAICS_DESCRIPTION = o.NAICS_Description__c,
            OPPORTUNITY_NAME = o.Name,
            NEXT_STEP = o.NextStep,
            NO_ACTIVITY_60_DAYS = o.No_Activity_60_Days__c,
            NUMBER_OF_WEEKS = o.Number_of_Weeks__c,
            OWNER_ID = o.OwnerId,
            PRICEBOOK2_ID = o.Pricebook2Id,
            PROBABILITY = o.Probability,
            PUSH_COUNT = o.PushCount,
            ROI_ANALYSIS_COMPLETED = o.ROI_Analysis_Completed__c,
            STAGE_NAME = o.StageName,
            SYSTEM_MODSTAMP = o.SystemModstamp,
            TERRITORY2_ID = o.Territory2Id,
            TOTAL_BY_LOB = o.Total_By_LOB__c,
            TOTAL_IN_MARKET = o.Total_In_Market__c,
            TOTAL_OUT_OF_MARKET = o.Total_Out_of_Market__c,
            TOTAL_PRICE = COALESCE(o.Total_Price__c, la.LINE_TOTALPRICE),
            TOTAL_OPPORTUNITY_QUANTITY = o.TotalOpportunityQuantity,
            TRANSACTION_ID = o.Transaction_ID__c,
            OPPORTUNITY_TYPE = o.Type,
            UNIQUEENTRY_CURRENT_ENDPOINT = o.UniqueEntry__Current_Endpoint__c,
            UNIQUEENTRY_RINGLEAD_APP_FIELD = o.UniqueEntry__RingLead_App_Field__c,
            UNIQUEENTRY_RINGLEAD_LAST_PROCESSED_DATE = o.UniqueEntry__RingLead_Last_Processed_Date__c,
            HASHDIFF = HASHBYTES('SHA2_256',
                CONCAT(
                    COALESCE(o.Id, ''), '|',
                    COALESCE(o.AccountId, ''), '|',
                    COALESCE(CONVERT(varchar(50), o.Amount), ''), '|',
                    COALESCE(CONVERT(varchar(1), o.Budget_Confirmed__c), ''), '|',
                    COALESCE(CONVERT(varchar(max), o.cafsl__Data_Set_Exclude_List__c), ''), '|',
                    COALESCE(CONVERT(varchar(30), o.Campaign_End_Date__c, 126), ''), '|',
                    COALESCE(CONVERT(varchar(30), o.Campaign_Start_Date__c, 126), ''), '|',
                    COALESCE(o.CampaignId, ''), '|',
                    COALESCE(CONVERT(varchar(30), o.CloseDate, 126), ''), '|',
                    COALESCE(o.ContactId, ''), '|',
                    COALESCE(o.ContractId, ''), '|',
                    COALESCE(o.CreatedById, ''), '|',
                    COALESCE(CONVERT(varchar(30), o.CreatedDate, 126), ''), '|',
                    COALESCE(o.CurrencyIsoCode, ''), '|',
                    COALESCE(o.Customer__c, ''), '|',
                    COALESCE(CONVERT(varchar(50), o.Days_Since_Last_Activity__c), ''), '|',
                    COALESCE(CONVERT(varchar(max), o.Description), ''), '|',
                    COALESCE(CONVERT(varchar(1), o.Discovery_Completed__c), ''), '|',
                    COALESCE(o.DOZISF__ZoomInfo_Opsos_App_Field__c, ''), '|',
                    COALESCE(o.DOZISF__ZoomInfo_Opsos_Current_Endpoint__c, ''), '|',
                    COALESCE(CONVERT(varchar(30), o.DOZISF__ZoomInfo_Opsos_Last_Processed_Date__c, 126), ''), '|',
                    COALESCE(CONVERT(varchar(50), o.ExpectedRevenue), ''), '|',
                    COALESCE(CONVERT(varchar(30), o.Field_History_Date__c, 126), ''), '|',
                    COALESCE(o.Fiscal, ''), '|',
                    COALESCE(CONVERT(varchar(10), o.FiscalQuarter), ''), '|',
                    COALESCE(CONVERT(varchar(10), o.FiscalYear), ''), '|',
                    COALESCE(o.Forecast_Manager__c, ''), '|',
                    COALESCE(o.ForecastCategory, ''), '|',
                    COALESCE(o.ForecastCategoryName, ''), '|',
                    COALESCE(CONVERT(varchar(1), o.HasOpenActivity), ''), '|',
                    COALESCE(CONVERT(varchar(1), o.HasOpportunityLineItem), ''), '|',
                    COALESCE(CONVERT(varchar(1), o.HasOverdueTask), ''), '|',
                    COALESCE(o.Industry__c, ''), '|',
                    COALESCE(CONVERT(varchar(1), o.IsClosed), ''), '|',
                    COALESCE(CONVERT(varchar(1), o.IsDeleted), ''), '|',
                    COALESCE(CONVERT(varchar(1), o.IsExcludedFromTerritory2Filter), ''), '|',
                    COALESCE(CONVERT(varchar(1), o.IsPrivate), ''), '|',
                    COALESCE(CONVERT(varchar(1), o.IsSplit), ''), '|',
                    COALESCE(CONVERT(varchar(1), o.IsWon), ''), '|',
                    COALESCE(CONVERT(varchar(30), o.Last_Task_Date__c, 126), ''), '|',
                    COALESCE(CONVERT(varchar(30), o.LastActivityDate, 126), ''), '|',
                    COALESCE(o.LastAmountChangedHistoryId, ''), '|',
                    COALESCE(o.LastCloseDateChangedHistoryId, ''), '|',
                    COALESCE(o.LastModifiedById, ''), '|',
                    COALESCE(CONVERT(varchar(30), o.LastModifiedDate, 126), ''), '|',
                    COALESCE(CONVERT(varchar(30), o.LastReferencedDate, 126), ''), '|',
                    COALESCE(CONVERT(varchar(30), o.LastStageChangeDate, 126), ''), '|',
                    COALESCE(CONVERT(varchar(30), o.LastViewedDate, 126), ''), '|',
                    COALESCE(o.LeadSource, ''), '|',
                    COALESCE(o.LID__LinkedIn_Company_Id__c, ''), '|',
                    COALESCE(o.LM_AccountType__c, ''), '|',
                    COALESCE(CONVERT(varchar(max), o.LM_Additional_RFP_Details__c), ''), '|',
                    COALESCE(o.LM_AE_Code__c, ''), '|',
                    COALESCE(o.LM_Brand_Industry__c, ''), '|',
                    COALESCE(o.LM_Brand_Name__c, ''), '|',
                    COALESCE(o.LM_Business_Unit__c, ''), '|',
                    COALESCE(CONVERT(varchar(1), o.lm_collaborative__c), ''), '|',
                    COALESCE(o.LM_CPQ_Transaction_Number__c, ''), '|',
                    COALESCE(o.LM_Customer_PO_Number__c, ''), '|',
                    COALESCE(CONVERT(varchar(50), o.LM_Estimated_Amount__c), ''), '|',
                    COALESCE(o.LM_Industry_Category__c, ''), '|',
                    COALESCE(CONVERT(varchar(50), o.LM_Inventory_on_Hold__c), ''), '|',
                    COALESCE(CONVERT(varchar(1), o.LM_Is_Team_Match__c), ''), '|',
                    COALESCE(CONVERT(varchar(30), o.LM_Last_Activity_Date__c, 126), ''), '|',
                    COALESCE(o.LM_NAICS_Code__c, ''), '|',
                    COALESCE(o.LM_Office__c, ''), '|',
                    COALESCE(o.LM_Primary_Oracle_Quote__c, ''), '|',
                    COALESCE(o.LM_Quote_Status__c, ''), '|',
                    COALESCE(o.LM_Related_Opportunity__c, ''), '|',
                    COALESCE(CONVERT(varchar(30), o.LM_Response_Due_DateTime__c, 126), ''), '|',
                    COALESCE(o.LM_Response_Due_DateTime_CPQ__c, ''), '|',
                    COALESCE(o.LM_Sales_Category__c, ''), '|',
                    COALESCE(CONVERT(varchar(50), o.LM_TCV_Airport__c), ''), '|',
                    COALESCE(CONVERT(varchar(50), o.LM_TCV_Logos__c), ''), '|',
                    COALESCE(CONVERT(varchar(50), o.LM_TCV_Outdoor__c), ''), '|',
                    COALESCE(CONVERT(varchar(50), o.LM_TCV_Transit__c), ''), '|',
                    COALESCE(CONVERT(varchar(50), o.LM_Total_Contract_Value__c), ''), '|',
                    COALESCE(o.LM_Transaction_Type__c, ''), '|',
                    COALESCE(o.Loss_Reason__c, ''), '|',
                    COALESCE(o.NAICS_Description__c, ''), '|',
                    COALESCE(o.Name, ''), '|',
                    COALESCE(o.NextStep, ''), '|',
                    COALESCE(CONVERT(varchar(1), o.No_Activity_60_Days__c), ''), '|',
                    COALESCE(CONVERT(varchar(50), o.Number_of_Weeks__c), ''), '|',
                    COALESCE(o.OwnerId, ''), '|',
                    COALESCE(o.Pricebook2Id, ''), '|',
                    COALESCE(CONVERT(varchar(50), o.Probability), ''), '|',
                    COALESCE(CONVERT(varchar(20), o.PushCount), ''), '|',
                    COALESCE(CONVERT(varchar(1), o.ROI_Analysis_Completed__c), ''), '|',
                    COALESCE(o.StageName, ''), '|',
                    COALESCE(CONVERT(varchar(30), o.SystemModstamp, 126), ''), '|',
                    COALESCE(o.Territory2Id, ''), '|',
                    COALESCE(CONVERT(varchar(50), o.Total_By_LOB__c), ''), '|',
                    COALESCE(CONVERT(varchar(50), o.Total_In_Market__c), ''), '|',
                    COALESCE(CONVERT(varchar(50), o.Total_Out_of_Market__c), ''), '|',
                    COALESCE(CONVERT(varchar(50), COALESCE(o.Total_Price__c, la.LINE_TOTALPRICE)), ''), '|',
                    COALESCE(CONVERT(varchar(50), o.TotalOpportunityQuantity), ''), '|',
                    COALESCE(o.Transaction_ID__c, ''), '|',
                    COALESCE(o.Type, ''), '|',
                    COALESCE(o.UniqueEntry__Current_Endpoint__c, ''), '|',
                    COALESCE(o.UniqueEntry__RingLead_App_Field__c, ''), '|',
                    COALESCE(CONVERT(varchar(30), o.UniqueEntry__RingLead_Last_Processed_Date__c, 126), '')
                )
            ),
            BZ_LOAD_DATE = CAST(o.SystemModstamp AS DATETIME),
            SV_LOAD_DATE = CAST(@AsOfDate AS DATETIME)
        INTO #SrcOpportunity
        FROM bzo.SF_Opportunity o
        LEFT JOIN #LineAgg la ON la.OpportunityId = o.Id
        WHERE o.Id IS NOT NULL;
        CREATE UNIQUE CLUSTERED INDEX CX_SrcOpportunity ON #SrcOpportunity (OPPORTUNITY_ID);

        BEGIN TRAN;

        /* ----- Insert new BKs ----- */
        INSERT INTO svo.D_SF_OPPORTUNITY
        (
            OPPORTUNITY_ID,
            ACCOUNT_ID, AMOUNT, BUDGET_CONFIRMED, CAFSL_DATA_SET_EXCLUDE_LIST,
            CAMPAIGN_END_DATE, CAMPAIGN_START_DATE, CAMPAIGN_ID, CLOSE_DATE,
            CONTACT_ID, CONTRACT_ID, CREATED_BY_ID, CREATED_DATE, CURRENCY_ISO_CODE,
            CUSTOMER_ID, DAYS_SINCE_LAST_ACTIVITY, DESCRIPTION, DISCOVERY_COMPLETED,
            DOZISF_ZOOMINFO_OPSOS_APP_FIELD, DOZISF_ZOOMINFO_OPSOS_CURRENT_ENDPOINT, DOZISF_ZOOMINFO_OPSOS_LAST_PROCESSED_DATE,
            EXPECTED_REVENUE, FIELD_HISTORY_DATE, FISCAL, FISCAL_QUARTER, FISCAL_YEAR,
            FORECAST_MANAGER_ID, FORECAST_CATEGORY, FORECAST_CATEGORY_NAME,
            HAS_OPEN_ACTIVITY, HAS_OPPORTUNITY_LINE_ITEM, HAS_OVERDUE_TASK,
            INDUSTRY, IS_CLOSED, IS_DELETED, IS_EXCLUDED_FROM_TERRITORY2_FILTER, IS_PRIVATE, IS_SPLIT, IS_WON,
            LAST_TASK_DATE, LAST_ACTIVITY_DATE, LAST_AMOUNT_CHANGED_HISTORY_ID, LAST_CLOSE_DATE_CHANGED_HISTORY_ID,
            LAST_MODIFIED_BY_ID, LAST_MODIFIED_DATE, LAST_REFERENCED_DATE, LAST_STAGE_CHANGE_DATE, LAST_VIEWED_DATE,
            LEAD_SOURCE, LID_LINKEDIN_COMPANY_ID,
            LM_ACCOUNTTYPE, LM_ADDITIONAL_RFP_DETAILS, LM_AE_CODE, LM_BRAND_INDUSTRY, LM_BRAND_NAME, LM_BUSINESS_UNIT,
            LM_COLLABORATIVE, LM_CPQ_TRANSACTION_NUMBER, LM_CUSTOMER_PO_NUMBER, LM_ESTIMATED_AMOUNT, LM_INDUSTRY_CATEGORY,
            LM_INVENTORY_ON_HOLD, LM_IS_TEAM_MATCH, LM_LAST_ACTIVITY_DATE, LM_NAICS_CODE,
            LM_OFFICE, LM_PRIMARY_ORACLE_QUOTE_ID, LM_QUOTE_STATUS, LM_RELATED_OPPORTUNITY_ID, LM_RESPONSE_DUE_DATETIME,
            LM_RESPONSE_DUE_DATETIME_CPQ, LM_SALES_CATEGORY, LM_TCV_AIRPORT, LM_TCV_LOGOS, LM_TCV_OUTDOOR, LM_TCV_TRANSIT,
            LM_TOTAL_CONTRACT_VALUE, LM_TRANSACTION_TYPE, LOSS_REASON, NAICS_DESCRIPTION, OPPORTUNITY_NAME, NEXT_STEP,
            NO_ACTIVITY_60_DAYS, NUMBER_OF_WEEKS, OWNER_ID, PRICEBOOK2_ID, PROBABILITY, PUSH_COUNT, ROI_ANALYSIS_COMPLETED,
            STAGE_NAME, SYSTEM_MODSTAMP, TERRITORY2_ID, TOTAL_BY_LOB, TOTAL_IN_MARKET, TOTAL_OUT_OF_MARKET,
            TOTAL_PRICE, TOTAL_OPPORTUNITY_QUANTITY, TRANSACTION_ID, OPPORTUNITY_TYPE,
            UNIQUEENTRY_CURRENT_ENDPOINT, UNIQUEENTRY_RINGLEAD_APP_FIELD, UNIQUEENTRY_RINGLEAD_LAST_PROCESSED_DATE,
            HASHDIFF, BZ_LOAD_DATE, SV_LOAD_DATE
        )
        SELECT
            s.OPPORTUNITY_ID,
            s.ACCOUNT_ID, s.AMOUNT, s.BUDGET_CONFIRMED, s.CAFSL_DATA_SET_EXCLUDE_LIST,
            s.CAMPAIGN_END_DATE, s.CAMPAIGN_START_DATE, s.CAMPAIGN_ID, s.CLOSE_DATE,
            s.CONTACT_ID, s.CONTRACT_ID, s.CREATED_BY_ID, s.CREATED_DATE, s.CURRENCY_ISO_CODE,
            s.CUSTOMER_ID, s.DAYS_SINCE_LAST_ACTIVITY, s.DESCRIPTION, s.DISCOVERY_COMPLETED,
            s.DOZISF_ZOOMINFO_OPSOS_APP_FIELD, s.DOZISF_ZOOMINFO_OPSOS_CURRENT_ENDPOINT, s.DOZISF_ZOOMINFO_OPSOS_LAST_PROCESSED_DATE,
            s.EXPECTED_REVENUE, s.FIELD_HISTORY_DATE, s.FISCAL, s.FISCAL_QUARTER, s.FISCAL_YEAR,
            s.FORECAST_MANAGER_ID, s.FORECAST_CATEGORY, s.FORECAST_CATEGORY_NAME,
            s.HAS_OPEN_ACTIVITY, s.HAS_OPPORTUNITY_LINE_ITEM, s.HAS_OVERDUE_TASK,
            s.INDUSTRY, s.IS_CLOSED, s.IS_DELETED, s.IS_EXCLUDED_FROM_TERRITORY2_FILTER, s.IS_PRIVATE, s.IS_SPLIT, s.IS_WON,
            s.LAST_TASK_DATE, s.LAST_ACTIVITY_DATE, s.LAST_AMOUNT_CHANGED_HISTORY_ID, s.LAST_CLOSE_DATE_CHANGED_HISTORY_ID,
            s.LAST_MODIFIED_BY_ID, s.LAST_MODIFIED_DATE, s.LAST_REFERENCED_DATE, s.LAST_STAGE_CHANGE_DATE, s.LAST_VIEWED_DATE,
            s.LEAD_SOURCE, s.LID_LINKEDIN_COMPANY_ID,
            s.LM_ACCOUNTTYPE, s.LM_ADDITIONAL_RFP_DETAILS, s.LM_AE_CODE, s.LM_BRAND_INDUSTRY, s.LM_BRAND_NAME, s.LM_BUSINESS_UNIT,
            s.LM_COLLABORATIVE, s.LM_CPQ_TRANSACTION_NUMBER, s.LM_CUSTOMER_PO_NUMBER, s.LM_ESTIMATED_AMOUNT, s.LM_INDUSTRY_CATEGORY,
            s.LM_INVENTORY_ON_HOLD, s.LM_IS_TEAM_MATCH, s.LM_LAST_ACTIVITY_DATE, s.LM_NAICS_CODE,
            s.LM_OFFICE, s.LM_PRIMARY_ORACLE_QUOTE_ID, s.LM_QUOTE_STATUS, s.LM_RELATED_OPPORTUNITY_ID, s.LM_RESPONSE_DUE_DATETIME,
            s.LM_RESPONSE_DUE_DATETIME_CPQ, s.LM_SALES_CATEGORY, s.LM_TCV_AIRPORT, s.LM_TCV_LOGOS, s.LM_TCV_OUTDOOR, s.LM_TCV_TRANSIT,
            s.LM_TOTAL_CONTRACT_VALUE, s.LM_TRANSACTION_TYPE, s.LOSS_REASON, s.NAICS_DESCRIPTION, s.OPPORTUNITY_NAME, s.NEXT_STEP,
            s.NO_ACTIVITY_60_DAYS, s.NUMBER_OF_WEEKS, s.OWNER_ID, s.PRICEBOOK2_ID, s.PROBABILITY, s.PUSH_COUNT, s.ROI_ANALYSIS_COMPLETED,
            s.STAGE_NAME, s.SYSTEM_MODSTAMP, s.TERRITORY2_ID, s.TOTAL_BY_LOB, s.TOTAL_IN_MARKET, s.TOTAL_OUT_OF_MARKET,
            s.TOTAL_PRICE, s.TOTAL_OPPORTUNITY_QUANTITY, s.TRANSACTION_ID, s.OPPORTUNITY_TYPE,
            s.UNIQUEENTRY_CURRENT_ENDPOINT, s.UNIQUEENTRY_RINGLEAD_APP_FIELD, s.UNIQUEENTRY_RINGLEAD_LAST_PROCESSED_DATE,
            s.HASHDIFF, s.BZ_LOAD_DATE, s.SV_LOAD_DATE
        FROM #SrcOpportunity s
        LEFT JOIN svo.D_SF_OPPORTUNITY d ON d.OPPORTUNITY_ID = s.OPPORTUNITY_ID
        WHERE d.OPPORTUNITY_ID IS NULL;

        SET @RowInserted = @@ROWCOUNT;

        /* ----- Update existing when HASHDIFF changed (skip plug row) ----- */
        UPDATE d
        SET
            d.ACCOUNT_ID = s.ACCOUNT_ID,
            d.AMOUNT = s.AMOUNT,
            d.BUDGET_CONFIRMED = s.BUDGET_CONFIRMED,
            d.CAFSL_DATA_SET_EXCLUDE_LIST = s.CAFSL_DATA_SET_EXCLUDE_LIST,
            d.CAMPAIGN_END_DATE = s.CAMPAIGN_END_DATE,
            d.CAMPAIGN_START_DATE = s.CAMPAIGN_START_DATE,
            d.CAMPAIGN_ID = s.CAMPAIGN_ID,
            d.CLOSE_DATE = s.CLOSE_DATE,
            d.CONTACT_ID = s.CONTACT_ID,
            d.CONTRACT_ID = s.CONTRACT_ID,
            d.CREATED_BY_ID = s.CREATED_BY_ID,
            d.CREATED_DATE = s.CREATED_DATE,
            d.CURRENCY_ISO_CODE = s.CURRENCY_ISO_CODE,
            d.CUSTOMER_ID = s.CUSTOMER_ID,
            d.DAYS_SINCE_LAST_ACTIVITY = s.DAYS_SINCE_LAST_ACTIVITY,
            d.DESCRIPTION = s.DESCRIPTION,
            d.DISCOVERY_COMPLETED = s.DISCOVERY_COMPLETED,
            d.DOZISF_ZOOMINFO_OPSOS_APP_FIELD = s.DOZISF_ZOOMINFO_OPSOS_APP_FIELD,
            d.DOZISF_ZOOMINFO_OPSOS_CURRENT_ENDPOINT = s.DOZISF_ZOOMINFO_OPSOS_CURRENT_ENDPOINT,
            d.DOZISF_ZOOMINFO_OPSOS_LAST_PROCESSED_DATE = s.DOZISF_ZOOMINFO_OPSOS_LAST_PROCESSED_DATE,
            d.EXPECTED_REVENUE = s.EXPECTED_REVENUE,
            d.FIELD_HISTORY_DATE = s.FIELD_HISTORY_DATE,
            d.FISCAL = s.FISCAL,
            d.FISCAL_QUARTER = s.FISCAL_QUARTER,
            d.FISCAL_YEAR = s.FISCAL_YEAR,
            d.FORECAST_MANAGER_ID = s.FORECAST_MANAGER_ID,
            d.FORECAST_CATEGORY = s.FORECAST_CATEGORY,
            d.FORECAST_CATEGORY_NAME = s.FORECAST_CATEGORY_NAME,
            d.HAS_OPEN_ACTIVITY = s.HAS_OPEN_ACTIVITY,
            d.HAS_OPPORTUNITY_LINE_ITEM = s.HAS_OPPORTUNITY_LINE_ITEM,
            d.HAS_OVERDUE_TASK = s.HAS_OVERDUE_TASK,
            d.INDUSTRY = s.INDUSTRY,
            d.IS_CLOSED = s.IS_CLOSED,
            d.IS_DELETED = s.IS_DELETED,
            d.IS_EXCLUDED_FROM_TERRITORY2_FILTER = s.IS_EXCLUDED_FROM_TERRITORY2_FILTER,
            d.IS_PRIVATE = s.IS_PRIVATE,
            d.IS_SPLIT = s.IS_SPLIT,
            d.IS_WON = s.IS_WON,
            d.LAST_TASK_DATE = s.LAST_TASK_DATE,
            d.LAST_ACTIVITY_DATE = s.LAST_ACTIVITY_DATE,
            d.LAST_AMOUNT_CHANGED_HISTORY_ID = s.LAST_AMOUNT_CHANGED_HISTORY_ID,
            d.LAST_CLOSE_DATE_CHANGED_HISTORY_ID = s.LAST_CLOSE_DATE_CHANGED_HISTORY_ID,
            d.LAST_MODIFIED_BY_ID = s.LAST_MODIFIED_BY_ID,
            d.LAST_MODIFIED_DATE = s.LAST_MODIFIED_DATE,
            d.LAST_REFERENCED_DATE = s.LAST_REFERENCED_DATE,
            d.LAST_STAGE_CHANGE_DATE = s.LAST_STAGE_CHANGE_DATE,
            d.LAST_VIEWED_DATE = s.LAST_VIEWED_DATE,
            d.LEAD_SOURCE = s.LEAD_SOURCE,
            d.LID_LINKEDIN_COMPANY_ID = s.LID_LINKEDIN_COMPANY_ID,
            d.LM_ACCOUNTTYPE = s.LM_ACCOUNTTYPE,
            d.LM_ADDITIONAL_RFP_DETAILS = s.LM_ADDITIONAL_RFP_DETAILS,
            d.LM_AE_CODE = s.LM_AE_CODE,
            d.LM_BRAND_INDUSTRY = s.LM_BRAND_INDUSTRY,
            d.LM_BRAND_NAME = s.LM_BRAND_NAME,
            d.LM_BUSINESS_UNIT = s.LM_BUSINESS_UNIT,
            d.LM_COLLABORATIVE = s.LM_COLLABORATIVE,
            d.LM_CPQ_TRANSACTION_NUMBER = s.LM_CPQ_TRANSACTION_NUMBER,
            d.LM_CUSTOMER_PO_NUMBER = s.LM_CUSTOMER_PO_NUMBER,
            d.LM_ESTIMATED_AMOUNT = s.LM_ESTIMATED_AMOUNT,
            d.LM_INDUSTRY_CATEGORY = s.LM_INDUSTRY_CATEGORY,
            d.LM_INVENTORY_ON_HOLD = s.LM_INVENTORY_ON_HOLD,
            d.LM_IS_TEAM_MATCH = s.LM_IS_TEAM_MATCH,
            d.LM_LAST_ACTIVITY_DATE = s.LM_LAST_ACTIVITY_DATE,
            d.LM_NAICS_CODE = s.LM_NAICS_CODE,
            d.LM_OFFICE = s.LM_OFFICE,
            d.LM_PRIMARY_ORACLE_QUOTE_ID = s.LM_PRIMARY_ORACLE_QUOTE_ID,
            d.LM_QUOTE_STATUS = s.LM_QUOTE_STATUS,
            d.LM_RELATED_OPPORTUNITY_ID = s.LM_RELATED_OPPORTUNITY_ID,
            d.LM_RESPONSE_DUE_DATETIME = s.LM_RESPONSE_DUE_DATETIME,
            d.LM_RESPONSE_DUE_DATETIME_CPQ = s.LM_RESPONSE_DUE_DATETIME_CPQ,
            d.LM_SALES_CATEGORY = s.LM_SALES_CATEGORY,
            d.LM_TCV_AIRPORT = s.LM_TCV_AIRPORT,
            d.LM_TCV_LOGOS = s.LM_TCV_LOGOS,
            d.LM_TCV_OUTDOOR = s.LM_TCV_OUTDOOR,
            d.LM_TCV_TRANSIT = s.LM_TCV_TRANSIT,
            d.LM_TOTAL_CONTRACT_VALUE = s.LM_TOTAL_CONTRACT_VALUE,
            d.LM_TRANSACTION_TYPE = s.LM_TRANSACTION_TYPE,
            d.LOSS_REASON = s.LOSS_REASON,
            d.NAICS_DESCRIPTION = s.NAICS_DESCRIPTION,
            d.OPPORTUNITY_NAME = s.OPPORTUNITY_NAME,
            d.NEXT_STEP = s.NEXT_STEP,
            d.NO_ACTIVITY_60_DAYS = s.NO_ACTIVITY_60_DAYS,
            d.NUMBER_OF_WEEKS = s.NUMBER_OF_WEEKS,
            d.OWNER_ID = s.OWNER_ID,
            d.PRICEBOOK2_ID = s.PRICEBOOK2_ID,
            d.PROBABILITY = s.PROBABILITY,
            d.PUSH_COUNT = s.PUSH_COUNT,
            d.ROI_ANALYSIS_COMPLETED = s.ROI_ANALYSIS_COMPLETED,
            d.STAGE_NAME = s.STAGE_NAME,
            d.SYSTEM_MODSTAMP = s.SYSTEM_MODSTAMP,
            d.TERRITORY2_ID = s.TERRITORY2_ID,
            d.TOTAL_BY_LOB = s.TOTAL_BY_LOB,
            d.TOTAL_IN_MARKET = s.TOTAL_IN_MARKET,
            d.TOTAL_OUT_OF_MARKET = s.TOTAL_OUT_OF_MARKET,
            d.TOTAL_PRICE = s.TOTAL_PRICE,
            d.TOTAL_OPPORTUNITY_QUANTITY = s.TOTAL_OPPORTUNITY_QUANTITY,
            d.TRANSACTION_ID = s.TRANSACTION_ID,
            d.OPPORTUNITY_TYPE = s.OPPORTUNITY_TYPE,
            d.UNIQUEENTRY_CURRENT_ENDPOINT = s.UNIQUEENTRY_CURRENT_ENDPOINT,
            d.UNIQUEENTRY_RINGLEAD_APP_FIELD = s.UNIQUEENTRY_RINGLEAD_APP_FIELD,
            d.UNIQUEENTRY_RINGLEAD_LAST_PROCESSED_DATE = s.UNIQUEENTRY_RINGLEAD_LAST_PROCESSED_DATE,
            d.HASHDIFF = s.HASHDIFF,
            d.BZ_LOAD_DATE = s.BZ_LOAD_DATE,
            d.SV_LOAD_DATE = CAST(@AsOfDate AS DATETIME)
        FROM svo.D_SF_OPPORTUNITY d
        INNER JOIN #SrcOpportunity s ON s.OPPORTUNITY_ID = d.OPPORTUNITY_ID
        WHERE d.OPPORTUNITY_SK <> 0
          AND d.HASHDIFF <> s.HASHDIFF;

        SET @RowUpdated = @@ROWCOUNT;

        COMMIT;

        SELECT @MaxWatermark = MAX(o.SystemModstamp)
        FROM bzo.SF_Opportunity o
        WHERE o.Id IS NOT NULL;

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
        IF @@TRANCOUNT > 0 ROLLBACK;
        IF @RunId IS NOT NULL
            UPDATE etl.ETL_RUN
            SET END_DTTM = @EndDttm, STATUS = 'FAILURE', ROW_INSERTED = @RowInserted, ROW_UPDATED = @RowUpdated, ERROR_MESSAGE = @ErrMsg
            WHERE RUN_ID = @RunId;
        ;THROW;
    END CATCH
END;
GO
