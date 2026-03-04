USE [Oracle_Reporting_P2]
GO

/****** Object:  Table [bzo].[OpportunityLineItem]    Script Date: 2/16/2026 12:14:01 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [bzo].[OpportunityLineItem](
	[Advertiser__c] [nvarchar](1300) NULL,
	[cafsl__External_Id__c] [float] NULL,
	[CanUseRevenueSchedule] [bit] NULL,
	[Company__c] [nvarchar](255) NULL,
	[Company_Name__c] [nvarchar](255) NULL,
	[CreatedById] [nvarchar](18) NULL,
	[CreatedDate] [datetime2](7) NULL,
	[CurrencyIsoCode] [nvarchar](255) NULL,
	[Customer__c] [nvarchar](1300) NULL,
	[Description] [nvarchar](255) NULL,
	[Discount] [float] NULL,
	[End_Date__c] [datetime2](7) NULL,
	[Fee_Type__c] [nvarchar](255) NULL,
	[HasQuantitySchedule] [bit] NULL,
	[HasRevenueSchedule] [bit] NULL,
	[HasSchedule] [bit] NULL,
	[Id] [nvarchar](18) NOT NULL,
	[IsDeleted] [bit] NULL,
	[LastModifiedById] [nvarchar](18) NULL,
	[LastModifiedDate] [datetime2](7) NULL,
	[LastReferencedDate] [datetime2](7) NULL,
	[LastViewedDate] [datetime2](7) NULL,
	[ListPrice] [numeric](18, 2) NULL,
	[LM_Account__c] [nvarchar](18) NULL,
	[LM_Active__c] [bit] NULL,
	[LM_Booking_Type__c] [nvarchar](20) NULL,
	[LM_Business_Unit__c] [nvarchar](10) NULL,
	[LM_Company_Number__c] [nvarchar](4) NULL,
	[LM_Completed_Installation__c] [datetime2](7) NULL,
	[LM_Creative_Status__c] [nvarchar](255) NULL,
	[LM_Demographic__c] [nvarchar](50) NULL,
	[LM_Flight_Weeks__c] [nvarchar](50) NULL,
	[LM_FRR_Expiration__c] [datetime2](7) NULL,
	[LM_Hold_Expiration_Date__c] [datetime2](7) NULL,
	[LM_Hold_ID__c] [nvarchar](50) NULL,
	[LM_Hold_Start_Date__c] [datetime2](7) NULL,
	[LM_Hold_Status__c] [nvarchar](20) NULL,
	[LM_Line_of_Business__c] [nvarchar](255) NULL,
	[LM_Location_Description__c] [nvarchar](255) NULL,
	[LM_Market__c] [nvarchar](50) NULL,
	[LM_Market_Budget__c] [numeric](18, 2) NULL,
	[LM_Name__c] [nvarchar](255) NULL,
	[LM_Number_of_Days_on_Hold__c] [float] NULL,
	[LM_Number_of_Periods__c] [float] NULL,
	[LM_Opportunity_Stage__c] [nvarchar](255) NULL,
	[LM_Oracle_Part_Number__c] [nvarchar](50) NULL,
	[LM_Panel_Number__c] [nvarchar](1300) NULL,
	[LM_Preemptive_Status__c] [nvarchar](50) NULL,
	[LM_Price_Type__c] [nvarchar](10) NULL,
	[LM_Product_Type__c] [nvarchar](255) NULL,
	[LM_Production_Status__c] [nvarchar](255) NULL,
	[LM_Response_Status__c] [nvarchar](50) NULL,
	[LM_Scheduled_Installation__c] [datetime2](7) NULL,
	[LM_totalInvestmentPerPeriod__c] [numeric](18, 2) NULL,
	[LM_TRP__c] [nvarchar](50) NULL,
	[Name] [nvarchar](376) NULL,
	[of_Weeks_Days_in_Campaign__c] [nvarchar](255) NULL,
	[Opportunity_Owner__c] [nvarchar](1300) NULL,
	[OpportunityId] [nvarchar](18) NULL,
	[Photosheet__c] [nvarchar](1300) NULL,
	[PricebookEntryId] [nvarchar](18) NULL,
	[Product__c] [nvarchar](18) NULL,
	[Product2Id] [nvarchar](18) NULL,
	[ProductCode] [nvarchar](255) NULL,
	[Quantity] [float] NULL,
	[Rate_Per_Period__c] [numeric](18, 2) NULL,
	[ServiceDate] [datetime2](7) NULL,
	[SortOrder] [int] NULL,
	[Start_Date__c] [datetime2](7) NULL,
	[Subtotal] [numeric](18, 2) NULL,
	[SystemModstamp] [datetime2](7) NULL,
	[TotalPrice] [numeric](18, 2) NULL,
	[UnitPrice] [numeric](18, 2) NULL,
 CONSTRAINT [PK_OpportunityLineItem_Id] PRIMARY KEY NONCLUSTERED 
(
	[Id] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY]
GO

