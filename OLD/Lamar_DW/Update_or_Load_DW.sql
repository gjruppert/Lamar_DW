--
--  Common Dimensions
--


EXEC svo.usp_Load_LINES_CODE_COMBO_LOOKUP_T1;
--EXEC svo.usp_Load_D_ACCOUNT_SCD2;
--EXEC svo.usp_Load_D_BUSINESS_OFFERING_SCD2;
--EXEC svo.usp_Load_D_COMPANY_SCD2;
--EXEC svo.usp_Load_D_COST_CENTER_SCD2;
--EXEC svo.usp_Load_D_INDUSTRY_SCD2;
--EXEC svo.usp_Load_D_INTERCOMPANY_SCD2;
--EXEC svo.usp_Load_D_CUSTOMER_ACCOUNT_SCD2;
--EXEC svo.usp_Load_D_CUSTOMER_ACCOUNT_SITE_SCD2;
--EXEC svo.usp_Load_D_LEGAL_ENTITY_SCD2;
--EXEC svo.usp_Load_D_LEDGER_SCD2;
--EXEC svo.usp_Load_D_ORGANIZATION_SCD2;
--EXEC svo.usp_Load_D_BUSINESS_UNIT_SCD2;
--EXEC svo.usp_Load_D_PARTY_SCD2;
--EXEC svo.usp_Load_D_PARTY_SITE_SCD2;
--EXEC svo.usp_Load_D_PARTY_CONTACT_POINT_SCD2;
--EXEC svo.usp_Load_D_SITE_USE_SCD2;
--EXEC svo.usp_Load_D_PAYMENT_METHOD_SCD2;
--EXEC svo.usp_Load_D_PAYMENT_METHOD_SCD2;
--EXEC svo.usp_Load_D_PAYMENT_TERM_SCD2;
--EXEC svo.usp_Load_D_VENDOR_SITE_SCD2;
--EXEC svo.usp_Load_D_CALENDAR;
--EXEC svo.usp_Load_D_ITEM_SCD2;
--EXEC svo.usp_Load_D_VENDOR_SCD2;
--EXEC svo.usp_Load_D_CURRENCY;


--
--  GL
--

--EXEC svo.usp_Load_D_GL_HEADER_SCD2;
--EXEC svo.usp_Load_F_GL_BALANCES_T1
--EXEC svo.usp_Load_F_GL_LINES_T1;

--
--  RM
--

--EXEC svo.usp_Load_D_RM_BILLING_LINE_T1;
--EXEC svo.usp_Load_D_RM_CONTRACT @FullReload = 1, @Debug = 1;
--EXEC svo.usp_Load_D_RM_PERF_OBLIGATION_LINE_SCD2 @FullReload = 1, @Debug = 1;
--EXEC svo.usp_Load_D_RM_PERF_OBLIGATION_SCD2;
--EXEC svo.usp_Load_D_RM_SATISFACTION_EVENT_SCD2;
--EXEC svo.usp_Load_D_RM_SATISFACTION_METHOD_SCD2
--EXEC svo.usp_Load_D_RM_SOURCE_DOC_PRICING_LINE_SCD2;
--EXEC svo.usp_Load_F_RM_SATISFACTION_EVENTS_T1  @FullReload = 1;

--
--  Salesforce
--

EXEC svo.usp_Load_D_SF_OPPORTUNITY;
EXEC svo.usp_Load_F_SF_OPPORTUNITY_LINE_ITEM;