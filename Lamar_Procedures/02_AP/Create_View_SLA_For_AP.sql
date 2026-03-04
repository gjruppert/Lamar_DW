/* =========================================================
   Wrapper view for SLA_SubledgerJournalDistributionPVO.
   Use this if the procedure fails with "Invalid column name"
   when selecting directly from bzo.SLA_SubledgerJournalDistributionPVO.
   The procedure would then select FROM bzo.V_SLA_For_AP instead.
   Deploy to Oracle_Reporting_P2.
   ========================================================= */
CREATE OR ALTER VIEW bzo.V_SLA_For_AP
AS
SELECT
    AeHeaderId,
    RefAeHeaderId,
    TempLineNum,
    TransactionEntityEntityCode,
    TransactionEntitySourceIdInt1,
    TransactionEntityTransactionNumber,
    XladistlinkEventClassCode,
    XladistlinkEventId,
    XladistlinkEventTypeCode,
    XladistlinkSourceDistributionType,
    XladistlinkSourceDistributionIdNum1,
    XladistlinkSourceDistributionIdNum2,
    XlalinesAccountingClassCode,
    XlalinesCodeCombinationId,
    XlalinesAccountingDate,
    XlalinesAeLineNum,
    XlalinesApplicationId,
    XlalinesDescription,
    XladistlinkUnroundedAccountedDr,
    XladistlinkUnroundedAccountedCr,
    XladistlinkUnroundedEnteredDr,
    XladistlinkUnroundedEnteredCr,
    XladistlinkLastUpdateDate,
    XladistlinkLastUpdatedBy,
    XladistlinkLastUpdateLogin,
    XlalinesLedgerId,
    XlalinesPartySiteId,
    AddDateTime
FROM bzo.SLA_SubledgerJournalDistributionPVO;
