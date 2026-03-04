/* =========================================================
   usp_Load_D_LEGAL_ENTITY
   SCD2 incremental load. Source: bzo.GL_LegalEntityExtractPVO
   Watermark filter, expire changed rows, insert new current rows.
   Idempotent: safe to retry same watermark range.
   ========================================================= */
CREATE OR ALTER PROCEDURE svo.usp_Load_D_LEGAL_ENTITY
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    DECLARE
        @ProcName       SYSNAME        = OBJECT_SCHEMA_NAME(@@PROCID) + '.' + OBJECT_NAME(@@PROCID),
        @TargetObject   SYSNAME        = 'svo.D_LEGAL_ENTITY',
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
        @RowUpdated     INT            = 0;

    BEGIN TRY
        SELECT @LastWatermark = w.LAST_WATERMARK
        FROM etl.ETL_WATERMARK w
        WHERE w.TABLE_NAME = @TargetObject;

        IF @LastWatermark IS NULL
            SET @LastWatermark = '1900-01-01';

        INSERT INTO etl.ETL_RUN (PROC_NAME, TARGET_OBJECT, ASOF_DATE, START_DTTM, STATUS)
        VALUES (@ProcName, @TargetObject, @AsOfDate, @StartDttm, 'STARTED');

        SET @RunId = SCOPE_IDENTITY();

        IF @RunId IS NULL
            THROW 50001, 'ETL_RUN insert failed: RUN_ID is NULL.', 1;

        IF EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'UX_D_LEGAL_ENTITY_ID' AND object_id = OBJECT_ID('svo.D_LEGAL_ENTITY'))
        BEGIN
            DROP INDEX UX_D_LEGAL_ENTITY_ID ON svo.D_LEGAL_ENTITY;
        END;

        IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'UX_D_LEGAL_ENTITY_BK_CURR' AND object_id = OBJECT_ID('svo.D_LEGAL_ENTITY'))
        BEGIN
            CREATE UNIQUE NONCLUSTERED INDEX UX_D_LEGAL_ENTITY_BK_CURR
            ON svo.D_LEGAL_ENTITY (LEGAL_ENTITY_ID)
            WHERE CURR_IND = 'Y'
            ON FG_SilverDim;
        END;

        IF NOT EXISTS (SELECT 1 FROM svo.D_LEGAL_ENTITY WHERE LEGAL_ENTITY_SK = 0)
        BEGIN
            SET IDENTITY_INSERT svo.D_LEGAL_ENTITY ON;

            INSERT INTO svo.D_LEGAL_ENTITY
            (LEGAL_ENTITY_SK, LEGAL_ENTITY_ID, LEGAL_ENTITY_ENTERPRISE_ID, LEGAL_ENTITY_GEOGRAPHY_ID, LEGAL_ENTITY_LEGAL_EMPLOYER_FLAG,
             LEGAL_ENTITY_IDENTIFIER, LEGAL_ENTITY_NAME, LEGAL_ENTITY_OBJECT_VERSION_NUMBER, LEGAL_ENTITY_PARTY_ID, LEGAL_ENTITY_PSU_FLAG,
             LEGAL_ENTITY_TRANSACTING_ENTITY_FLAG, LEGAL_ENTITY_LAST_UPDATE_DATE, LEGAL_ENTITY_LAST_UPDATE_LOGIN, LEGAL_ENTITY_LAST_UPDATED_BY,
             BZ_LOAD_DATE, SV_LOAD_DATE, EFF_DATE, END_DATE, CRE_DATE, UDT_DATE, CURR_IND)
            VALUES
            (0, -1, -1, -1, 'NULL', '00-0000000', 'Unknown', -1, -1, 'NULL', 'NULL', '0001-01-01', 'Unknown', 'Unknown',
             CAST('0001-01-01' AS DATE), CAST(GETDATE() AS DATE), @AsOfDate, @HighDate, @LoadDttm, @LoadDttm, 'Y');

            SET IDENTITY_INSERT svo.D_LEGAL_ENTITY OFF;
        END;

        IF OBJECT_ID('tempdb..#src') IS NOT NULL DROP TABLE #src;

        SELECT
            s.LEGAL_ENTITY_ID, s.LEGAL_ENTITY_ENTERPRISE_ID, s.LEGAL_ENTITY_GEOGRAPHY_ID, s.LEGAL_ENTITY_LEGAL_EMPLOYER_FLAG,
            s.LEGAL_ENTITY_IDENTIFIER, s.LEGAL_ENTITY_NAME, s.LEGAL_ENTITY_OBJECT_VERSION_NUMBER, s.LEGAL_ENTITY_PARTY_ID,
            s.LEGAL_ENTITY_PSU_FLAG, s.LEGAL_ENTITY_TRANSACTING_ENTITY_FLAG, s.LEGAL_ENTITY_LAST_UPDATE_DATE,
            s.LEGAL_ENTITY_LAST_UPDATE_LOGIN, s.LEGAL_ENTITY_LAST_UPDATED_BY, s.BZ_LOAD_DATE, s.SV_LOAD_DATE, s.SourceAddDateTime
        INTO #src
        FROM
        (
            SELECT
                L.LegalEntityLegalEntityId AS LEGAL_ENTITY_ID,
                L.LegalEntityEnterpriseId AS LEGAL_ENTITY_ENTERPRISE_ID,
                L.LegalEntityGeographyId AS LEGAL_ENTITY_GEOGRAPHY_ID,
                L.LegalEntityLegalEmployerFlag AS LEGAL_ENTITY_LEGAL_EMPLOYER_FLAG,
                L.LegalEntityLegalEntityIdentifier AS LEGAL_ENTITY_IDENTIFIER,
                L.LegalEntityName AS LEGAL_ENTITY_NAME,
                L.LegalEntityObjectVersionNumber AS LEGAL_ENTITY_OBJECT_VERSION_NUMBER,
                L.LegalEntityPartyId AS LEGAL_ENTITY_PARTY_ID,
                L.LegalEntityPsuFlag AS LEGAL_ENTITY_PSU_FLAG,
                L.LegalEntityTransactingEntityFlag AS LEGAL_ENTITY_TRANSACTING_ENTITY_FLAG,
                CAST(L.LegalEntityLastUpdateDate AS DATE) AS LEGAL_ENTITY_LAST_UPDATE_DATE,
                L.LegalEntityLastUpdateLogin AS LEGAL_ENTITY_LAST_UPDATE_LOGIN,
                L.LegalEntityLastUpdatedBy AS LEGAL_ENTITY_LAST_UPDATED_BY,
                COALESCE(CAST(L.AddDateTime AS DATE), '0001-01-01') AS BZ_LOAD_DATE,
                CAST(GETDATE() AS DATE) AS SV_LOAD_DATE,
                L.AddDateTime AS SourceAddDateTime,
                ROW_NUMBER() OVER (PARTITION BY L.LegalEntityLegalEntityId ORDER BY L.AddDateTime DESC) AS rn
            FROM bzo.GL_LegalEntityExtractPVO L
            WHERE L.AddDateTime > @LastWatermark
        ) s
        WHERE s.rn = 1;

        SELECT @MaxWatermark = MAX(SourceAddDateTime) FROM #src;

        UPDATE tgt
        SET tgt.END_DATE = DATEADD(DAY, -1, @AsOfDate), tgt.CURR_IND = 'N', tgt.UDT_DATE = @LoadDttm
        FROM svo.D_LEGAL_ENTITY tgt
        INNER JOIN #src src ON src.LEGAL_ENTITY_ID = tgt.LEGAL_ENTITY_ID
        WHERE tgt.CURR_IND = 'Y'
          AND (
                ISNULL(tgt.LEGAL_ENTITY_ENTERPRISE_ID, -999) <> ISNULL(src.LEGAL_ENTITY_ENTERPRISE_ID, -999)
             OR ISNULL(tgt.LEGAL_ENTITY_GEOGRAPHY_ID, -999) <> ISNULL(src.LEGAL_ENTITY_GEOGRAPHY_ID, -999)
             OR ISNULL(tgt.LEGAL_ENTITY_LEGAL_EMPLOYER_FLAG,'') <> ISNULL(src.LEGAL_ENTITY_LEGAL_EMPLOYER_FLAG,'')
             OR ISNULL(tgt.LEGAL_ENTITY_IDENTIFIER,'') <> ISNULL(src.LEGAL_ENTITY_IDENTIFIER,'')
             OR ISNULL(tgt.LEGAL_ENTITY_NAME,'') <> ISNULL(src.LEGAL_ENTITY_NAME,'')
             OR ISNULL(tgt.LEGAL_ENTITY_OBJECT_VERSION_NUMBER, -999) <> ISNULL(src.LEGAL_ENTITY_OBJECT_VERSION_NUMBER, -999)
             OR ISNULL(tgt.LEGAL_ENTITY_PARTY_ID, -999) <> ISNULL(src.LEGAL_ENTITY_PARTY_ID, -999)
             OR ISNULL(tgt.LEGAL_ENTITY_PSU_FLAG,'') <> ISNULL(src.LEGAL_ENTITY_PSU_FLAG,'')
             OR ISNULL(tgt.LEGAL_ENTITY_TRANSACTING_ENTITY_FLAG,'') <> ISNULL(src.LEGAL_ENTITY_TRANSACTING_ENTITY_FLAG,'')
             OR ISNULL(tgt.LEGAL_ENTITY_LAST_UPDATE_DATE,'1900-01-01') <> ISNULL(src.LEGAL_ENTITY_LAST_UPDATE_DATE,'1900-01-01')
             OR ISNULL(tgt.LEGAL_ENTITY_LAST_UPDATE_LOGIN,'') <> ISNULL(src.LEGAL_ENTITY_LAST_UPDATE_LOGIN,'')
             OR ISNULL(tgt.LEGAL_ENTITY_LAST_UPDATED_BY,'') <> ISNULL(src.LEGAL_ENTITY_LAST_UPDATED_BY,'')
          );

        SET @RowExpired = @@ROWCOUNT;

        INSERT INTO svo.D_LEGAL_ENTITY
        (LEGAL_ENTITY_ID, LEGAL_ENTITY_ENTERPRISE_ID, LEGAL_ENTITY_GEOGRAPHY_ID, LEGAL_ENTITY_LEGAL_EMPLOYER_FLAG,
         LEGAL_ENTITY_IDENTIFIER, LEGAL_ENTITY_NAME, LEGAL_ENTITY_OBJECT_VERSION_NUMBER, LEGAL_ENTITY_PARTY_ID,
         LEGAL_ENTITY_PSU_FLAG, LEGAL_ENTITY_TRANSACTING_ENTITY_FLAG, LEGAL_ENTITY_LAST_UPDATE_DATE,
         LEGAL_ENTITY_LAST_UPDATE_LOGIN, LEGAL_ENTITY_LAST_UPDATED_BY, BZ_LOAD_DATE, SV_LOAD_DATE,
         EFF_DATE, END_DATE, CRE_DATE, UDT_DATE, CURR_IND)
        SELECT
            src.LEGAL_ENTITY_ID, src.LEGAL_ENTITY_ENTERPRISE_ID, src.LEGAL_ENTITY_GEOGRAPHY_ID, src.LEGAL_ENTITY_LEGAL_EMPLOYER_FLAG,
            src.LEGAL_ENTITY_IDENTIFIER, src.LEGAL_ENTITY_NAME, src.LEGAL_ENTITY_OBJECT_VERSION_NUMBER, src.LEGAL_ENTITY_PARTY_ID,
            src.LEGAL_ENTITY_PSU_FLAG, src.LEGAL_ENTITY_TRANSACTING_ENTITY_FLAG, src.LEGAL_ENTITY_LAST_UPDATE_DATE,
            src.LEGAL_ENTITY_LAST_UPDATE_LOGIN, src.LEGAL_ENTITY_LAST_UPDATED_BY, src.BZ_LOAD_DATE, src.SV_LOAD_DATE,
            @AsOfDate, @HighDate, @LoadDttm, @LoadDttm, 'Y'
        FROM #src src
        LEFT JOIN svo.D_LEGAL_ENTITY tgt ON tgt.LEGAL_ENTITY_ID = src.LEGAL_ENTITY_ID AND tgt.CURR_IND = 'Y'
        WHERE tgt.LEGAL_ENTITY_ID IS NULL;

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
