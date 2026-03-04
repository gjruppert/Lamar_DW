/* =========================================================
   usp_Load_D_PARTY
   SCD2 incremental load. Source: bzo.AR_PartyExtractPVO
   Watermark filter, expire changed rows, insert new current rows.
   Idempotent: safe to retry same watermark range.
   ========================================================= */
CREATE OR ALTER PROCEDURE svo.usp_Load_D_PARTY
    @BatchId INT = NULL
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    DECLARE
        @ProcName       SYSNAME        = OBJECT_SCHEMA_NAME(@@PROCID) + '.' + OBJECT_NAME(@@PROCID),
        @TargetObject   SYSNAME        = 'svo.D_PARTY',
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

    SELECT @TableBridgeID = TableBridgeID FROM meta.MedallionTableBridge WHERE targettable = N'AR_PartyExtractPVO';

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

        IF EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'UX_D_PARTY' AND object_id = OBJECT_ID('svo.D_PARTY'))
        BEGIN
            DROP INDEX UX_D_PARTY ON svo.D_PARTY;
        END;

        IF EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'UX_D_PARTY_ID' AND object_id = OBJECT_ID('svo.D_PARTY'))
        BEGIN
            DROP INDEX UX_D_PARTY_ID ON svo.D_PARTY;
        END;

        IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'UX_D_PARTY_BK_CURR' AND object_id = OBJECT_ID('svo.D_PARTY'))
        BEGIN
            CREATE UNIQUE NONCLUSTERED INDEX UX_D_PARTY_BK_CURR
            ON svo.D_PARTY (PARTY_ID)
            WHERE CURR_IND = 'Y'
            ON FG_SilverDim;
        END;

        IF NOT EXISTS (SELECT 1 FROM svo.D_PARTY WHERE PARTY_SK = 0)
        BEGIN
            SET IDENTITY_INSERT svo.D_PARTY ON;

            INSERT INTO svo.D_PARTY
            (PARTY_SK, PARTY_ID, PARTY_NUMBER, PARTY_NAME, PARTY_TYPE, STATUS, COUNTRY, STATE, CITY, POSTAL_CODE,
             CREATED_BY, CREATION_DATE, LAST_UPDATE_DATE, BZ_LOAD_DATE, SV_LOAD_DATE, EFF_DATE, END_DATE, CRE_DATE, UDT_DATE, CURR_IND)
            VALUES
            (0, -1, 'UNK', 'Unknown', 'U', 'U', 'UNK', 'UN', 'UNK', '00000', 'System', GETDATE(), GETDATE(),
             GETDATE(), GETDATE(), @AsOfDate, @HighDate, @LoadDttm, @LoadDttm, 'Y');

            SET IDENTITY_INSERT svo.D_PARTY OFF;
        END;

        IF OBJECT_ID('tempdb..#src') IS NOT NULL DROP TABLE #src;

        SELECT
            s.PARTY_ID, s.PARTY_NUMBER, s.PARTY_NAME, s.PARTY_TYPE, s.STATUS, s.COUNTRY, s.STATE, s.CITY, s.POSTAL_CODE,
            s.CREATED_BY, s.CREATION_DATE, s.LAST_UPDATE_DATE, s.BZ_LOAD_DATE, s.SV_LOAD_DATE, s.SourceAddDateTime
        INTO #src
        FROM
        (
            SELECT
                S.PartyId AS PARTY_ID,
                S.PartyNumber AS PARTY_NUMBER,
                S.PartyName AS PARTY_NAME,
                S.PartyType AS PARTY_TYPE,
                S.Status AS STATUS,
                ISNULL(S.Country,'UNK') AS COUNTRY,
                ISNULL(S.State,'UN') AS STATE,
                ISNULL(S.City,'UNK') AS CITY,
                ISNULL(S.PostalCode,'00000') AS POSTAL_CODE,
                S.CreatedBy AS CREATED_BY,
                CAST(S.CreationDate AS DATE) AS CREATION_DATE,
                CAST(S.LastUpdateDate AS DATE) AS LAST_UPDATE_DATE,
                CAST(S.AddDateTime AS DATETIME) AS BZ_LOAD_DATE,
                GETDATE() AS SV_LOAD_DATE,
                S.AddDateTime AS SourceAddDateTime,
                ROW_NUMBER() OVER (PARTITION BY S.PartyId ORDER BY S.AddDateTime DESC) AS rn
            FROM bzo.AR_PartyExtractPVO S
            WHERE S.PartyId IS NOT NULL
              AND S.AddDateTime > @LastWatermark
        ) s
        WHERE s.rn = 1;

        DELETE FROM #src WHERE PARTY_ID = -1;

        SELECT @MaxWatermark = MAX(SourceAddDateTime) FROM #src;

        UPDATE tgt
        SET tgt.END_DATE = DATEADD(DAY, -1, @AsOfDate), tgt.CURR_IND = 'N', tgt.UDT_DATE = @LoadDttm
        FROM svo.D_PARTY tgt
        INNER JOIN #src src ON src.PARTY_ID = tgt.PARTY_ID
        WHERE tgt.CURR_IND = 'Y'
          AND (
                ISNULL(tgt.PARTY_NUMBER,'') <> ISNULL(src.PARTY_NUMBER,'')
             OR ISNULL(tgt.PARTY_NAME,'') <> ISNULL(src.PARTY_NAME,'')
             OR ISNULL(tgt.PARTY_TYPE,'') <> ISNULL(src.PARTY_TYPE,'')
             OR ISNULL(tgt.STATUS,'') <> ISNULL(src.STATUS,'')
             OR ISNULL(tgt.COUNTRY,'') <> ISNULL(src.COUNTRY,'')
             OR ISNULL(tgt.STATE,'') <> ISNULL(src.STATE,'')
             OR ISNULL(tgt.CITY,'') <> ISNULL(src.CITY,'')
             OR ISNULL(tgt.POSTAL_CODE,'') <> ISNULL(src.POSTAL_CODE,'')
             OR ISNULL(tgt.CREATED_BY,'') <> ISNULL(src.CREATED_BY,'')
             OR ISNULL(tgt.CREATION_DATE, CAST('1900-01-01' AS DATE)) <> ISNULL(src.CREATION_DATE, CAST('1900-01-01' AS DATE))
             OR ISNULL(tgt.LAST_UPDATE_DATE, CAST('1900-01-01' AS DATE)) <> ISNULL(src.LAST_UPDATE_DATE, CAST('1900-01-01' AS DATE))
          );

        SET @RowExpired = @@ROWCOUNT;

        INSERT INTO svo.D_PARTY
        (PARTY_ID, PARTY_NUMBER, PARTY_NAME, PARTY_TYPE, STATUS, COUNTRY, STATE, CITY, POSTAL_CODE,
         CREATED_BY, CREATION_DATE, LAST_UPDATE_DATE, BZ_LOAD_DATE, SV_LOAD_DATE, EFF_DATE, END_DATE, CRE_DATE, UDT_DATE, CURR_IND)
        SELECT
            src.PARTY_ID, src.PARTY_NUMBER, src.PARTY_NAME, src.PARTY_TYPE, src.STATUS, src.COUNTRY, src.STATE, src.CITY, src.POSTAL_CODE,
            src.CREATED_BY, src.CREATION_DATE, src.LAST_UPDATE_DATE, src.BZ_LOAD_DATE, src.SV_LOAD_DATE,
            @AsOfDate, @HighDate, @LoadDttm, @LoadDttm, 'Y'
        FROM #src src
        LEFT JOIN svo.D_PARTY tgt ON tgt.PARTY_ID = src.PARTY_ID AND tgt.CURR_IND = 'Y'
        WHERE tgt.PARTY_ID IS NULL;

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
