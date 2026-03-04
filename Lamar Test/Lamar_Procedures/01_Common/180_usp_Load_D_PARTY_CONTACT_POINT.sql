/* =========================================================
   usp_Load_D_PARTY_CONTACT_POINT
   SCD2 incremental load. Source: bzo.AR_PartyContactPointExtractPVO
   Watermark filter, expire changed rows, insert new current rows.
   Idempotent: safe to retry same watermark range.
   ========================================================= */
CREATE OR ALTER PROCEDURE svo.usp_Load_D_PARTY_CONTACT_POINT
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    DECLARE
        @ProcName       SYSNAME        = OBJECT_SCHEMA_NAME(@@PROCID) + '.' + OBJECT_NAME(@@PROCID),
        @TargetObject   SYSNAME        = 'svo.D_PARTY_CONTACT_POINT',
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

        IF EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'UX_D_PARTY_CONTACT_POINT_ID' AND object_id = OBJECT_ID('svo.D_PARTY_CONTACT_POINT'))
        BEGIN
            DROP INDEX UX_D_PARTY_CONTACT_POINT_ID ON svo.D_PARTY_CONTACT_POINT;
        END;

        IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'UX_D_PARTY_CONTACT_POINT_BK_CURR' AND object_id = OBJECT_ID('svo.D_PARTY_CONTACT_POINT'))
        BEGIN
            CREATE UNIQUE NONCLUSTERED INDEX UX_D_PARTY_CONTACT_POINT_BK_CURR
            ON svo.D_PARTY_CONTACT_POINT (CONTACT_POINT_ID)
            WHERE CURR_IND = 'Y'
            ON FG_SilverDim;
        END;

        IF NOT EXISTS (SELECT 1 FROM svo.D_PARTY_CONTACT_POINT WHERE PARTY_CONTACT_POINT_SK = 0)
        BEGIN
            SET IDENTITY_INSERT svo.D_PARTY_CONTACT_POINT ON;

            INSERT INTO svo.D_PARTY_CONTACT_POINT
            (PARTY_CONTACT_POINT_SK, CONTACT_POINT_ID, OWNER_TABLE_ID, CONTACT_POINT_TYPE, CONTACT_POINT_PURPOSE, PHONE_NUMBER,
             PRIMARY_FLAG, LAST_UPDATE_DATE, LAST_UPDATED_BY, LAST_UPDATE_LOGIN, BZ_LOAD_DATE, SV_LOAD_DATE,
             EFF_DATE, END_DATE, CRE_DATE, UDT_DATE, CURR_IND)
            VALUES
            (0, 0, 0, 'UNKNOWN', 'UNKNOWN', 'UNKNOWN', 'N', '0001-01-01', 'SYSTEM', 'UNKNOWN', '0001-01-01', CAST(GETDATE() AS DATE),
             @AsOfDate, @HighDate, @LoadDttm, @LoadDttm, 'Y');

            SET IDENTITY_INSERT svo.D_PARTY_CONTACT_POINT OFF;
        END;

        IF OBJECT_ID('tempdb..#src') IS NOT NULL DROP TABLE #src;

        SELECT
            s.CONTACT_POINT_ID, s.OWNER_TABLE_ID, s.CONTACT_POINT_TYPE, s.CONTACT_POINT_PURPOSE, s.PHONE_NUMBER,
            s.PRIMARY_FLAG, s.LAST_UPDATE_DATE, s.LAST_UPDATED_BY, s.LAST_UPDATE_LOGIN, s.BZ_LOAD_DATE, s.SV_LOAD_DATE, s.SourceAddDateTime
        INTO #src
        FROM
        (
            SELECT
                C.ContactPointId AS CONTACT_POINT_ID,
                C.OwnerTableId AS OWNER_TABLE_ID,
                C.ContactPointType AS CONTACT_POINT_TYPE,
                C.ContactPointPurpose AS CONTACT_POINT_PURPOSE,
                C.PhoneNumber AS PHONE_NUMBER,
                C.PrimaryFlag AS PRIMARY_FLAG,
                CAST(C.LastUpdateDate AS DATE) AS LAST_UPDATE_DATE,
                C.LastUpdatedBy AS LAST_UPDATED_BY,
                C.LastUpdateLogin AS LAST_UPDATE_LOGIN,
                COALESCE(CAST(C.AddDateTime AS DATE), CAST(GETDATE() AS DATE)) AS BZ_LOAD_DATE,
                CAST(GETDATE() AS DATE) AS SV_LOAD_DATE,
                C.AddDateTime AS SourceAddDateTime,
                ROW_NUMBER() OVER (PARTITION BY C.ContactPointId ORDER BY C.AddDateTime DESC) AS rn
            FROM bzo.AR_PartyContactPointExtractPVO C
            WHERE C.AddDateTime > @LastWatermark
        ) s
        WHERE s.rn = 1;

        SELECT @MaxWatermark = MAX(SourceAddDateTime) FROM #src;

        UPDATE tgt
        SET tgt.END_DATE = DATEADD(DAY, -1, @AsOfDate), tgt.CURR_IND = 'N', tgt.UDT_DATE = @LoadDttm
        FROM svo.D_PARTY_CONTACT_POINT tgt
        INNER JOIN #src src ON src.CONTACT_POINT_ID = tgt.CONTACT_POINT_ID
        WHERE tgt.CURR_IND = 'Y'
          AND (
                ISNULL(tgt.OWNER_TABLE_ID, -999) <> ISNULL(src.OWNER_TABLE_ID, -999)
             OR ISNULL(tgt.CONTACT_POINT_TYPE,'') <> ISNULL(src.CONTACT_POINT_TYPE,'')
             OR ISNULL(tgt.CONTACT_POINT_PURPOSE,'') <> ISNULL(src.CONTACT_POINT_PURPOSE,'')
             OR ISNULL(tgt.PHONE_NUMBER,'') <> ISNULL(src.PHONE_NUMBER,'')
             OR ISNULL(tgt.PRIMARY_FLAG,'') <> ISNULL(src.PRIMARY_FLAG,'')
             OR ISNULL(tgt.LAST_UPDATE_DATE,'1900-01-01') <> ISNULL(src.LAST_UPDATE_DATE,'1900-01-01')
             OR ISNULL(tgt.LAST_UPDATED_BY,'') <> ISNULL(src.LAST_UPDATED_BY,'')
             OR ISNULL(tgt.LAST_UPDATE_LOGIN,'') <> ISNULL(src.LAST_UPDATE_LOGIN,'')
          );

        SET @RowExpired = @@ROWCOUNT;

        INSERT INTO svo.D_PARTY_CONTACT_POINT
        (CONTACT_POINT_ID, OWNER_TABLE_ID, CONTACT_POINT_TYPE, CONTACT_POINT_PURPOSE, PHONE_NUMBER,
         PRIMARY_FLAG, LAST_UPDATE_DATE, LAST_UPDATED_BY, LAST_UPDATE_LOGIN, BZ_LOAD_DATE, SV_LOAD_DATE,
         EFF_DATE, END_DATE, CRE_DATE, UDT_DATE, CURR_IND)
        SELECT
            src.CONTACT_POINT_ID, src.OWNER_TABLE_ID, src.CONTACT_POINT_TYPE, src.CONTACT_POINT_PURPOSE, src.PHONE_NUMBER,
            src.PRIMARY_FLAG, src.LAST_UPDATE_DATE, src.LAST_UPDATED_BY, src.LAST_UPDATE_LOGIN, src.BZ_LOAD_DATE, src.SV_LOAD_DATE,
            @AsOfDate, @HighDate, @LoadDttm, @LoadDttm, 'Y'
        FROM #src src
        LEFT JOIN svo.D_PARTY_CONTACT_POINT tgt ON tgt.CONTACT_POINT_ID = src.CONTACT_POINT_ID AND tgt.CURR_IND = 'Y'
        WHERE tgt.CONTACT_POINT_ID IS NULL;

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
