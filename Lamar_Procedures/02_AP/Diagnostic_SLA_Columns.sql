/* Run this on Oracle_Reporting_P2 to list actual columns in bzo.SLA_SubledgerJournalDistributionPVO */
/* Share the results so we can map to the correct column names */

-- All columns
SELECT 
    TABLE_SCHEMA,
    TABLE_NAME,
    COLUMN_NAME,
    ORDINAL_POSITION
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_NAME = 'SLA_SubledgerJournalDistributionPVO'
ORDER BY ORDINAL_POSITION;

-- Columns that might be TransactionEntitySourceIdInt1 or XladistlinkSourceDistributionIdNum1
SELECT COLUMN_NAME
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_NAME = 'SLA_SubledgerJournalDistributionPVO'
  AND (COLUMN_NAME LIKE '%TransactionEntity%SourceId%'
    OR COLUMN_NAME LIKE '%SourceDistributionId%');
