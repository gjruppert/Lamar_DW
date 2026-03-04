/* =========================================================
   usp_Load_D_BUSINESS_UNIT
   SCD2 incremental load. Source: bzo.CMN_BusinessUnitPVO
   Watermark filter, expire changed rows, insert new current rows.
   Idempotent: safe to retry same watermark range.
   ========================================================= */
CREATE OR ALTER PROCEDURE svo.usp_Load_D_BUSINESS_UNIT
    @BatchId INT = NULL
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    DECLARE
        @ProcName       SYSNAME        = OBJECT_SCHEMA_NAME(@@PROCID) + '.' + OBJECT_NAME(@@PROCID),
        @TargetObject   SYSNAME        = 'svo.D_BUSINESS_UNIT',
        @StartDttm      DATETIME2(0)   = SYSDATETIME(),
        @EndDttm        DATETIME2(0),
        @RunId          BIGINT         = NULL,
        @ErrMsg         NVARCHAR(4000)  = NULL,

        @AsOfDate       DATE           = CAST(GETDATE() AS DATE),
        @LoadDttm       DATETIME2(0)   = SYSDATETIME(),
        @HighDate       DATE           = '9999-12-31',

        @LastWatermark  DATETIME2(7),
        @MaxWatermark   DATETIME2(7)   = NULL,

        @RowInserted    INT            = 0,
        @RowExpired     INT            = 0,
        @RowUpdated     INT            = 0,
        @TableBridgeID  INT            = NULL;

    SELECT @TableBridgeID = TableBridgeID FROM meta.MedallionTableBridge WHERE targettable = N'CMN_BusinessUnitPVO';

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

        IF EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'UX_D_BUSINESS_UNIT_ID' AND object_id = OBJECT_ID('svo.D_BUSINESS_UNIT'))
        BEGIN
            DROP INDEX UX_D_BUSINESS_UNIT_ID ON svo.D_BUSINESS_UNIT;
        END;

        IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'UX_D_BUSINESS_UNIT_BK_CURR' AND object_id = OBJECT_ID('svo.D_BUSINESS_UNIT'))
        BEGIN
            CREATE UNIQUE NONCLUSTERED INDEX UX_D_BUSINESS_UNIT_BK_CURR
            ON svo.D_BUSINESS_UNIT (BUSINESS_UNIT_ID)
            WHERE CURR_IND = 'Y'
            ON FG_SilverDim;
        END;

        IF NOT EXISTS (SELECT 1 FROM svo.D_BUSINESS_UNIT WHERE BUSINESS_UNIT_SK = 0)
        BEGIN
            SET IDENTITY_INSERT svo.D_BUSINESS_UNIT ON;

            INSERT INTO svo.D_BUSINESS_UNIT
            (BUSINESS_UNIT_SK, BUSINESS_UNIT_ID, BUSINESS_UNIT_NAME, BUSINESS_UNIT_ENTERPRISE_ID, BUSINESS_UNIT_LEGAL_ENTITY_ID,
             BUSINESS_UNIT_LOCATION_ID, BUSINESS_UNIT_PRIMARY_LEDGER_ID, BUSINESS_UNIT_DEFAULT_CURRENCY_CODE, BUSINESS_UNIT_DEFAULT_SET_ID,
             BUSINESS_UNIT_ENABLED_FOR_HR_FLAG, BUSINESS_UNIT_STATUS, BUSINESS_UNIT_CREATED_BY, BUSINESS_UNIT_CREATION_DATE,
             BUSINESS_UNIT_DATE_FROM, BUSINESS_UNIT_DATE_TO, FIN_BU_BUSINESS_UNIT_ID, BZ_LOAD_DATE, SV_LOAD_DATE,
             EFF_DATE, END_DATE, CRE_DATE, UDT_DATE, CURR_IND)
            VALUES
            (0, -1, 'Unknown', -1, 'UNK', -1, 'UNK', 'UNK', 'UNK', 'UNK', 'Unknown', 'Unknown', '1753-01-01', '0001-01-01', '0001-01-01', -1, '0001-01-01', GETDATE(),
             @AsOfDate, @HighDate, @LoadDttm, @LoadDttm, 'Y');

            SET IDENTITY_INSERT svo.D_BUSINESS_UNIT OFF;
        END;

        IF OBJECT_ID('tempdb..#src') IS NOT NULL DROP TABLE #src;

        SELECT
            s.BUSINESS_UNIT_ID, s.BUSINESS_UNIT_NAME, s.BUSINESS_UNIT_ENTERPRISE_ID, s.BUSINESS_UNIT_LEGAL_ENTITY_ID,
            s.BUSINESS_UNIT_LOCATION_ID, s.BUSINESS_UNIT_PRIMARY_LEDGER_ID, s.BUSINESS_UNIT_DEFAULT_CURRENCY_CODE,
            s.BUSINESS_UNIT_DEFAULT_SET_ID, s.BUSINESS_UNIT_ENABLED_FOR_HR_FLAG, s.BUSINESS_UNIT_STATUS,
            s.BUSINESS_UNIT_CREATED_BY, s.BUSINESS_UNIT_CREATION_DATE, s.BUSINESS_UNIT_DATE_FROM, s.BUSINESS_UNIT_DATE_TO,
            s.FIN_BU_BUSINESS_UNIT_ID, s.BZ_LOAD_DATE, s.SV_LOAD_DATE, s.SourceAddDateTime
        INTO #src
        FROM
        (
            SELECT
                B.BusinessUnitId AS BUSINESS_UNIT_ID,
                B.BusinessUnitName AS BUSINESS_UNIT_NAME,
                B.BusinessUnitEnterpriseId AS BUSINESS_UNIT_ENTERPRISE_ID,
                B.BusinessUnitLegalEntityId AS BUSINESS_UNIT_LEGAL_ENTITY_ID,
                B.BusinessUnitLocationId AS BUSINESS_UNIT_LOCATION_ID,
                B.BusinessUnitPrimaryLedgerId AS BUSINESS_UNIT_PRIMARY_LEDGER_ID,
                B.BusinessUnitDefaultCurrencyCode AS BUSINESS_UNIT_DEFAULT_CURRENCY_CODE,
                B.BusinessUnitDefaultSetId AS BUSINESS_UNIT_DEFAULT_SET_ID,
                B.BusinessUnitEnabledForHrFlag AS BUSINESS_UNIT_ENABLED_FOR_HR_FLAG,
                B.BusinessUnitStatus AS BUSINESS_UNIT_STATUS,
                B.BusinessUnitCreatedBy AS BUSINESS_UNIT_CREATED_BY,
                CAST(B.BusinessUnitCreationDate AS DATE) AS BUSINESS_UNIT_CREATION_DATE,
                CAST(B.BusinessUnitDateFrom AS DATE) AS BUSINESS_UNIT_DATE_FROM,
                CAST(B.BusinessUnitDateTo AS DATE) AS BUSINESS_UNIT_DATE_TO,
                B.FinBuBusinessUnitId AS FIN_BU_BUSINESS_UNIT_ID,
                COALESCE(CAST(B.AddDateTime AS DATETIME), CAST('1900-01-01' AS DATETIME)) AS BZ_LOAD_DATE,
                GETDATE() AS SV_LOAD_DATE,
                COALESCE(B.AddDateTime, B.BusinessUnitLastUpdateDate, B.BusinessUnitCreationDate) AS SourceAddDateTime,
                ROW_NUMBER() OVER (PARTITION BY B.BusinessUnitId ORDER BY COALESCE(B.AddDateTime, B.BusinessUnitLastUpdateDate, B.BusinessUnitCreationDate) DESC) AS rn
            FROM bzo.CMN_BusinessUnitPVO B
            WHERE B.BusinessUnitId IS NOT NULL
              AND COALESCE(B.AddDateTime, B.BusinessUnitLastUpdateDate, B.BusinessUnitCreationDate) > @LastWatermark
        ) s
        WHERE s.rn = 1;

        SELECT @MaxWatermark = MAX(SourceAddDateTime) FROM #src;

        UPDATE tgt
        SET tgt.END_DATE = DATEADD(DAY, -1, @AsOfDate), tgt.CURR_IND = 'N', tgt.UDT_DATE = @LoadDttm
        FROM svo.D_BUSINESS_UNIT tgt
        INNER JOIN #src src ON src.BUSINESS_UNIT_ID = tgt.BUSINESS_UNIT_ID
        WHERE tgt.CURR_IND = 'Y'
          AND (
                ISNULL(tgt.BUSINESS_UNIT_NAME,'') <> ISNULL(src.BUSINESS_UNIT_NAME,'')
             OR ISNULL(tgt.BUSINESS_UNIT_ENTERPRISE_ID, -999) <> ISNULL(src.BUSINESS_UNIT_ENTERPRISE_ID, -999)
             OR ISNULL(tgt.BUSINESS_UNIT_LEGAL_ENTITY_ID,'') <> ISNULL(src.BUSINESS_UNIT_LEGAL_ENTITY_ID,'')
             OR ISNULL(tgt.BUSINESS_UNIT_LOCATION_ID, -999) <> ISNULL(src.BUSINESS_UNIT_LOCATION_ID, -999)
             OR ISNULL(tgt.BUSINESS_UNIT_PRIMARY_LEDGER_ID,'') <> ISNULL(src.BUSINESS_UNIT_PRIMARY_LEDGER_ID,'')
             OR ISNULL(tgt.BUSINESS_UNIT_DEFAULT_CURRENCY_CODE,'') <> ISNULL(src.BUSINESS_UNIT_DEFAULT_CURRENCY_CODE,'')
             OR ISNULL(tgt.BUSINESS_UNIT_DEFAULT_SET_ID,'') <> ISNULL(src.BUSINESS_UNIT_DEFAULT_SET_ID,'')
             OR ISNULL(tgt.BUSINESS_UNIT_ENABLED_FOR_HR_FLAG,'') <> ISNULL(src.BUSINESS_UNIT_ENABLED_FOR_HR_FLAG,'')
             OR ISNULL(tgt.BUSINESS_UNIT_STATUS,'') <> ISNULL(src.BUSINESS_UNIT_STATUS,'')
             OR ISNULL(tgt.BUSINESS_UNIT_CREATED_BY,'') <> ISNULL(src.BUSINESS_UNIT_CREATED_BY,'')
             OR ISNULL(tgt.BUSINESS_UNIT_CREATION_DATE,'1900-01-01') <> ISNULL(src.BUSINESS_UNIT_CREATION_DATE,'1900-01-01')
             OR ISNULL(tgt.BUSINESS_UNIT_DATE_FROM,'1900-01-01') <> ISNULL(src.BUSINESS_UNIT_DATE_FROM,'1900-01-01')
             OR ISNULL(tgt.BUSINESS_UNIT_DATE_TO,'1900-01-01') <> ISNULL(src.BUSINESS_UNIT_DATE_TO,'1900-01-01')
             OR ISNULL(tgt.FIN_BU_BUSINESS_UNIT_ID, -999) <> ISNULL(src.FIN_BU_BUSINESS_UNIT_ID, -999)
          );

        SET @RowExpired = @@ROWCOUNT;

        INSERT INTO svo.D_BUSINESS_UNIT
        (BUSINESS_UNIT_ID, BUSINESS_UNIT_NAME, BUSINESS_UNIT_ENTERPRISE_ID, BUSINESS_UNIT_LEGAL_ENTITY_ID,
         BUSINESS_UNIT_LOCATION_ID, BUSINESS_UNIT_PRIMARY_LEDGER_ID, BUSINESS_UNIT_DEFAULT_CURRENCY_CODE,
         BUSINESS_UNIT_DEFAULT_SET_ID, BUSINESS_UNIT_ENABLED_FOR_HR_FLAG, BUSINESS_UNIT_STATUS,
         BUSINESS_UNIT_CREATED_BY, BUSINESS_UNIT_CREATION_DATE, BUSINESS_UNIT_DATE_FROM, BUSINESS_UNIT_DATE_TO,
         FIN_BU_BUSINESS_UNIT_ID, BZ_LOAD_DATE, SV_LOAD_DATE, EFF_DATE, END_DATE, CRE_DATE, UDT_DATE, CURR_IND)
        SELECT
            src.BUSINESS_UNIT_ID, src.BUSINESS_UNIT_NAME, src.BUSINESS_UNIT_ENTERPRISE_ID, src.BUSINESS_UNIT_LEGAL_ENTITY_ID,
            src.BUSINESS_UNIT_LOCATION_ID, src.BUSINESS_UNIT_PRIMARY_LEDGER_ID, src.BUSINESS_UNIT_DEFAULT_CURRENCY_CODE,
            src.BUSINESS_UNIT_DEFAULT_SET_ID, src.BUSINESS_UNIT_ENABLED_FOR_HR_FLAG, src.BUSINESS_UNIT_STATUS,
            src.BUSINESS_UNIT_CREATED_BY, src.BUSINESS_UNIT_CREATION_DATE, src.BUSINESS_UNIT_DATE_FROM, src.BUSINESS_UNIT_DATE_TO,
            src.FIN_BU_BUSINESS_UNIT_ID, src.BZ_LOAD_DATE, src.SV_LOAD_DATE,
            @AsOfDate, @HighDate, @LoadDttm, @LoadDttm, 'Y'
        FROM #src src
        LEFT JOIN svo.D_BUSINESS_UNIT tgt ON tgt.BUSINESS_UNIT_ID = src.BUSINESS_UNIT_ID AND tgt.CURR_IND = 'Y'
        WHERE tgt.BUSINESS_UNIT_ID IS NULL;

        SET @RowInserted = @@ROWCOUNT;

        IF @MaxWatermark IS NOT NULL
        BEGIN
            UPDATE etl.ETL_WATERMARK SET LAST_WATERMARK = @MaxWatermark, UDT_DATE = SYSDATETIME() WHERE TABLE_NAME = @TargetObject;
        END

        SET @EndDttm = SYSDATETIME();
        UPDATE etl.ETL_RUN SET END_DTTM = @EndDttm, STATUS = 'SUCCESS', ROW_INSERTED = @RowInserted, ROW_EXPIRED = @RowExpired, ROW_UPDATED = @RowUpdated, ERROR_MESSAGE = NULL WHERE RUN_ID = @RunId;
    END TRY
    BEGIN CATCH
        SET @EndDttm = SYSDATETIME();
        SET @ErrMsg  = CONCAT('Error ', ERROR_NUMBER(), ' (Line ', ERROR_LINE(), '): ', ERROR_MESSAGE());
        IF @RunId IS NOT NULL
            UPDATE etl.ETL_RUN SET END_DTTM = @EndDttm, STATUS = 'FAILURE', ROW_INSERTED = @RowInserted, ROW_EXPIRED = @RowExpired, ROW_UPDATED = @RowUpdated, ERROR_MESSAGE = @ErrMsg WHERE RUN_ID = @RunId;
        IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'UX_D_BUSINESS_UNIT_BK_CURR' AND object_id = OBJECT_ID('svo.D_BUSINESS_UNIT'))
            CREATE UNIQUE NONCLUSTERED INDEX UX_D_BUSINESS_UNIT_BK_CURR ON svo.D_BUSINESS_UNIT(BUSINESS_UNIT_ID) WHERE CURR_IND = 'Y' ON FG_SilverDim;
        ;THROW;
    END CATCH
END;
GO
