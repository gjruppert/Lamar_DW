/* =========================================================
   usp_Load_D_ORGANIZATION
   SCD2 incremental load. Source: bzo.PIM_InvOrgParametersExtractPVO
   Watermark filter, expire changed rows, insert new current rows.
   Idempotent: safe to retry same watermark range.
   ========================================================= */
CREATE OR ALTER PROCEDURE svo.usp_Load_D_ORGANIZATION
    @BatchId INT = NULL
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    DECLARE
        @ProcName       SYSNAME        = OBJECT_SCHEMA_NAME(@@PROCID) + '.' + OBJECT_NAME(@@PROCID),
        @TargetObject   SYSNAME        = 'svo.D_ORGANIZATION',
        @StartDttm      DATETIME2(0)   = SYSDATETIME(),
        @EndDttm        DATETIME2(0),
        @RunId          BIGINT         = NULL,
        @ErrMsg         NVARCHAR(4000) = NULL,

        @AsOfDate       DATE           = CAST(GETDATE() AS DATE),
        @LoadDttm       DATETIME2(0)   = SYSDATETIME(),
        @HighDate       DATE           = '9999-12-31',

        @LastWatermark  DATETIME2(7),
        @MaxWatermark   DATETIME2(7)   = NULL,

        @RowInserted    INT            = 0,
        @RowExpired     INT            = 0,
        @RowUpdated     INT            = 0,
        @TableBridgeID  INT            = NULL;

    SELECT @TableBridgeID = TableBridgeID FROM meta.MedallionTableBridge WHERE targettable = N'PIM_InvOrgParametersExtractPVO';

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

        IF EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'UX_D_ORGANIZATION_ORGANIZATION_ID' AND object_id = OBJECT_ID('svo.D_ORGANIZATION'))
        BEGIN
            DROP INDEX UX_D_ORGANIZATION_ORGANIZATION_ID ON svo.D_ORGANIZATION;
        END;

        IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'UX_D_ORGANIZATION_BK_CURR' AND object_id = OBJECT_ID('svo.D_ORGANIZATION'))
        BEGIN
            CREATE UNIQUE NONCLUSTERED INDEX UX_D_ORGANIZATION_BK_CURR
            ON svo.D_ORGANIZATION (ORGANIZATION_ID)
            WHERE CURR_IND = 'Y'
            ON FG_SilverDim;
        END;

        IF NOT EXISTS (SELECT 1 FROM svo.D_ORGANIZATION WHERE INVENTORY_ORG_SK = 0)
        BEGIN
            SET IDENTITY_INSERT svo.D_ORGANIZATION ON;

            INSERT INTO svo.D_ORGANIZATION
            (INVENTORY_ORG_SK, ORGANIZATION_ID, ORGANIZATION_CODE, INVENTORY_FLAG, BUSINESS_UNIT_ID, LEGAL_ENTITY_ID, MASTER_ORGANIZATION_ID, SOURCE_ORGANIZATION_ID, BZ_LOAD_DATE, SV_LOAD_DATE, EFF_DATE, END_DATE, CRE_DATE, UDT_DATE, CURR_IND)
            VALUES
            (0, -1, 'UNKNOWN', 'N', NULL, NULL, NULL, NULL, GETDATE(), GETDATE(), @AsOfDate, @HighDate, @LoadDttm, @LoadDttm, 'Y');

            SET IDENTITY_INSERT svo.D_ORGANIZATION OFF;
        END;

        IF OBJECT_ID('tempdb..#src') IS NOT NULL DROP TABLE #src;

        SELECT
            s.ORGANIZATION_ID, s.ORGANIZATION_CODE, s.INVENTORY_FLAG, s.BUSINESS_UNIT_ID, s.LEGAL_ENTITY_ID,
            s.MASTER_ORGANIZATION_ID, s.SOURCE_ORGANIZATION_ID, s.BZ_LOAD_DATE, s.SV_LOAD_DATE, s.SourceAddDateTime
        INTO #src
        FROM
        (
            SELECT
                p.OrganizationId AS ORGANIZATION_ID,
                p.OrganizationCode AS ORGANIZATION_CODE,
                p.InventoryFlag AS INVENTORY_FLAG,
                p.BusinessUnitId AS BUSINESS_UNIT_ID,
                p.LegalEntityId AS LEGAL_ENTITY_ID,
                p.MasterOrganizationId AS MASTER_ORGANIZATION_ID,
                p.SourceOrganizationId AS SOURCE_ORGANIZATION_ID,
                CAST(p.AddDateTime AS DATETIME) AS BZ_LOAD_DATE,
                GETDATE() AS SV_LOAD_DATE,
                p.AddDateTime AS SourceAddDateTime,
                ROW_NUMBER() OVER (PARTITION BY p.OrganizationId ORDER BY p.AddDateTime DESC) AS rn
            FROM bzo.PIM_InvOrgParametersExtractPVO p
            WHERE p.OrganizationId IS NOT NULL
              AND p.AddDateTime > @LastWatermark
        ) s
        WHERE s.rn = 1;

        SELECT @MaxWatermark = MAX(SourceAddDateTime) FROM #src;

        UPDATE tgt
        SET tgt.END_DATE = DATEADD(DAY, -1, @AsOfDate), tgt.CURR_IND = 'N', tgt.UDT_DATE = @LoadDttm
        FROM svo.D_ORGANIZATION tgt
        INNER JOIN #src src ON src.ORGANIZATION_ID = tgt.ORGANIZATION_ID
        WHERE tgt.CURR_IND = 'Y'
          AND (
                ISNULL(tgt.ORGANIZATION_CODE,'') <> ISNULL(src.ORGANIZATION_CODE,'')
             OR ISNULL(tgt.INVENTORY_FLAG,'') <> ISNULL(src.INVENTORY_FLAG,'')
             OR ISNULL(tgt.BUSINESS_UNIT_ID, -999) <> ISNULL(src.BUSINESS_UNIT_ID, -999)
             OR ISNULL(tgt.LEGAL_ENTITY_ID, -999) <> ISNULL(src.LEGAL_ENTITY_ID, -999)
             OR ISNULL(tgt.MASTER_ORGANIZATION_ID, -999) <> ISNULL(src.MASTER_ORGANIZATION_ID, -999)
             OR ISNULL(tgt.SOURCE_ORGANIZATION_ID, -999) <> ISNULL(src.SOURCE_ORGANIZATION_ID, -999)
          );

        SET @RowExpired = @@ROWCOUNT;

        INSERT INTO svo.D_ORGANIZATION
        (ORGANIZATION_ID, ORGANIZATION_CODE, INVENTORY_FLAG, BUSINESS_UNIT_ID, LEGAL_ENTITY_ID, MASTER_ORGANIZATION_ID, SOURCE_ORGANIZATION_ID, BZ_LOAD_DATE, SV_LOAD_DATE, EFF_DATE, END_DATE, CRE_DATE, UDT_DATE, CURR_IND)
        SELECT
            src.ORGANIZATION_ID, src.ORGANIZATION_CODE, src.INVENTORY_FLAG, src.BUSINESS_UNIT_ID, src.LEGAL_ENTITY_ID,
            src.MASTER_ORGANIZATION_ID, src.SOURCE_ORGANIZATION_ID, src.BZ_LOAD_DATE, src.SV_LOAD_DATE,
            @AsOfDate, @HighDate, @LoadDttm, @LoadDttm, 'Y'
        FROM #src src
        LEFT JOIN svo.D_ORGANIZATION tgt ON tgt.ORGANIZATION_ID = src.ORGANIZATION_ID AND tgt.CURR_IND = 'Y'
        WHERE tgt.ORGANIZATION_ID IS NULL;

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
        ;THROW;
    END CATCH
END;
GO
