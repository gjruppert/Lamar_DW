CREATE OR ALTER PROCEDURE svo.usp_Load_D_PARTY_SITE_SCD2
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    DECLARE
        @ProcName       SYSNAME        = OBJECT_SCHEMA_NAME(@@PROCID) + '.' + OBJECT_NAME(@@PROCID),
        @TargetObject   SYSNAME        = 'svo.D_PARTY_SITE',
        @StartDttm      DATETIME2(0)   = SYSDATETIME(),
        @EndDttm        DATETIME2(0),
        @RunId          BIGINT         = NULL,
        @ErrMsg         VARCHAR(4000)  = NULL,

        @AsOfDate       DATE           = CAST(GETDATE() AS DATE),
        @LoadDttm       DATETIME2(0)   = SYSDATETIME(),
        @HighDate       DATE           = '9999-12-31',

        @LastWatermark  DATETIME2(7),
        @MaxWatermark   DATETIME2(7)   = NULL,

        @RowInserted    INT            = 0,
        @RowExpired     INT            = 0,
        @RowUpdated     INT            = 0;

    BEGIN TRY
        /* ===== Watermark ===== */
        SELECT @LastWatermark = w.LAST_WATERMARK
        FROM etl.ETL_WATERMARK w
        WHERE w.TABLE_NAME = @TargetObject;

        IF @LastWatermark IS NULL
            SET @LastWatermark = '1900-01-01';

        /* ===== ETL_RUN start ===== */
        INSERT INTO etl.ETL_RUN (PROC_NAME, TARGET_OBJECT, ASOF_DATE, START_DTTM, STATUS)
        VALUES (@ProcName, @TargetObject, @AsOfDate, @StartDttm, 'STARTED');

        SET @RunId = SCOPE_IDENTITY();

        /* ===== Unique index for CURRENT rows only ===== */
        IF NOT EXISTS
        (
            SELECT 1
            FROM sys.indexes
            WHERE name = 'UX_D_PARTY_SITE_ID_CURR'
              AND object_id = OBJECT_ID('svo.D_PARTY_SITE')
        )
        BEGIN
            CREATE UNIQUE NONCLUSTERED INDEX UX_D_PARTY_SITE_ID_CURR
            ON svo.D_PARTY_SITE (PARTY_SITE_ID)
            WHERE CURR_IND = 'Y'
            ON FG_SilverDim;
        END;

        /* ===== Ensure plug row (SK=0) exists ===== */
        IF NOT EXISTS (SELECT 1 FROM svo.D_PARTY_SITE WHERE PARTY_SITE_SK = 0)
        BEGIN
            SET IDENTITY_INSERT svo.D_PARTY_SITE ON;

            INSERT INTO svo.D_PARTY_SITE
            (
                PARTY_SITE_SK,
                PARTY_SITE_ID,
                PARTY_ID,
                PARTY_SITE_NAME,
                PARTY_SITE_NUMBER,
                LOCATION_ID,
                OVERALL_PRIMARY_FLAG,
                ACTUAL_CONTENT_SOURCE,
                START_DATE_ACTIVE,
                END_DATE_ACTIVE,
                STATUS,
                CREATED_BY,
                LAST_UPDATE_BY,
                LAST_UPDATE_LOGIN,
                BZ_LOAD_DATE,
                SV_LOAD_DATE,
                EFF_DATE,
                END_DATE,
                CRE_DATE,
                UDT_DATE,
                CURR_IND
            )
            VALUES
            (
                0,
                -1,
                -1,
                'Unknown Party Site',
                'UNK',
                -1,
                'N',
                'UNK',
                '0001-01-01',
                '9999-12-31',
                'U',
                'SYSTEM',
                'SYSTEM',
                'UNK',
                CAST('0001-01-01' AS DATE),
                CAST(GETDATE() AS DATE),
                @AsOfDate,
                @HighDate,
                @LoadDttm,
                @LoadDttm,
                'Y'
            );

            SET IDENTITY_INSERT svo.D_PARTY_SITE OFF;
        END;

        /* ===== Source (incremental + dedup by PARTY_SITE_ID) ===== */
        IF OBJECT_ID('tempdb..#src') IS NOT NULL
            DROP TABLE #src;

        SELECT
            s.PARTY_SITE_ID,
            s.PARTY_ID,
            s.PARTY_SITE_NAME,
            s.PARTY_SITE_NUMBER,
            s.LOCATION_ID,
            s.OVERALL_PRIMARY_FLAG,
            s.ACTUAL_CONTENT_SOURCE,
            s.START_DATE_ACTIVE,
            s.END_DATE_ACTIVE,
            s.STATUS,
            s.CREATED_BY,
            s.LAST_UPDATE_BY,
            s.LAST_UPDATE_LOGIN,
            s.BZ_LOAD_DATE,
            s.SV_LOAD_DATE,
            s.SourceAddDateTime
        INTO #src
        FROM
        (
            SELECT
                PartySiteId                                  AS PARTY_SITE_ID,
                PartyId                                      AS PARTY_ID,
                ISNULL(PartySiteName,'UNK')                  AS PARTY_SITE_NAME,
                ISNULL(PartySiteNumber,'UNK')                AS PARTY_SITE_NUMBER,
                LocationId                                   AS LOCATION_ID,
                ISNULL(OverallPrimaryFlag,'N')               AS OVERALL_PRIMARY_FLAG,
                ISNULL(ActualContentSource,'UNK')            AS ACTUAL_CONTENT_SOURCE,
                CAST(StartDateActive AS DATE)                AS START_DATE_ACTIVE,
                CAST(EndDateActive   AS DATE)                AS END_DATE_ACTIVE,
                ISNULL(Status,'U')                           AS STATUS,
                ISNULL(CreatedBy,'UNK')                      AS CREATED_BY,
                ISNULL(LastUpdatedBy,'UNK')                  AS LAST_UPDATE_BY,
                ISNULL(LastUpdateLogin,'UNK')                AS LAST_UPDATE_LOGIN,
                CAST(AddDateTime AS DATE)                    AS BZ_LOAD_DATE,
                CAST(GETDATE() AS DATE)                      AS SV_LOAD_DATE,
                AddDateTime                                  AS SourceAddDateTime,
                ROW_NUMBER() OVER
                (
                    PARTITION BY PartySiteId
                    ORDER BY AddDateTime DESC
                ) AS rn
            FROM bzo.AR_PartySiteExtractPVO
            WHERE AddDateTime > @LastWatermark
              AND PartySiteId IS NOT NULL
        ) s
        WHERE s.rn = 1;

        DELETE FROM #src WHERE PARTY_SITE_ID = -1;

        SELECT @MaxWatermark = MAX(SourceAddDateTime) FROM #src;

        /* ===== SCD2 expire changed current rows ===== */
        UPDATE tgt
        SET
            tgt.END_DATE = DATEADD(DAY, -1, @AsOfDate),
            tgt.CURR_IND = 'N',
            tgt.UDT_DATE = @LoadDttm
        FROM svo.D_PARTY_SITE tgt
        INNER JOIN #src src
            ON src.PARTY_SITE_ID = tgt.PARTY_SITE_ID
        WHERE tgt.CURR_IND = 'Y'
          AND
          (
                ISNULL(tgt.PARTY_ID,-1) <> ISNULL(src.PARTY_ID,-1)
             OR ISNULL(tgt.PARTY_SITE_NAME,'') <> ISNULL(src.PARTY_SITE_NAME,'')
             OR ISNULL(tgt.PARTY_SITE_NUMBER,'') <> ISNULL(src.PARTY_SITE_NUMBER,'')
             OR ISNULL(tgt.LOCATION_ID,-1) <> ISNULL(src.LOCATION_ID,-1)
             OR ISNULL(tgt.OVERALL_PRIMARY_FLAG,'') <> ISNULL(src.OVERALL_PRIMARY_FLAG,'')
             OR ISNULL(tgt.ACTUAL_CONTENT_SOURCE,'') <> ISNULL(src.ACTUAL_CONTENT_SOURCE,'')
             OR ISNULL(tgt.START_DATE_ACTIVE,'1900-01-01') <> ISNULL(src.START_DATE_ACTIVE,'1900-01-01')
             OR ISNULL(tgt.END_DATE_ACTIVE,'1900-01-01') <> ISNULL(src.END_DATE_ACTIVE,'1900-01-01')
             OR ISNULL(tgt.STATUS,'') <> ISNULL(src.STATUS,'')
             OR ISNULL(tgt.CREATED_BY,'') <> ISNULL(src.CREATED_BY,'')
             OR ISNULL(tgt.LAST_UPDATE_BY,'') <> ISNULL(src.LAST_UPDATE_BY,'')
             OR ISNULL(tgt.LAST_UPDATE_LOGIN,'') <> ISNULL(src.LAST_UPDATE_LOGIN,'')
          );

        SET @RowExpired = @@ROWCOUNT;

        /* ===== Insert new current rows (new BKs + changed BKs just expired) ===== */
        INSERT INTO svo.D_PARTY_SITE
        (
            PARTY_SITE_ID,
            PARTY_ID,
            PARTY_SITE_NAME,
            PARTY_SITE_NUMBER,
            LOCATION_ID,
            OVERALL_PRIMARY_FLAG,
            ACTUAL_CONTENT_SOURCE,
            START_DATE_ACTIVE,
            END_DATE_ACTIVE,
            STATUS,
            CREATED_BY,
            LAST_UPDATE_BY,
            LAST_UPDATE_LOGIN,
            BZ_LOAD_DATE,
            SV_LOAD_DATE,
            EFF_DATE,
            END_DATE,
            CRE_DATE,
            UDT_DATE,
            CURR_IND
        )
        SELECT
            src.PARTY_SITE_ID,
            src.PARTY_ID,
            src.PARTY_SITE_NAME,
            src.PARTY_SITE_NUMBER,
            src.LOCATION_ID,
            src.OVERALL_PRIMARY_FLAG,
            src.ACTUAL_CONTENT_SOURCE,
            src.START_DATE_ACTIVE,
            src.END_DATE_ACTIVE,
            src.STATUS,
            src.CREATED_BY,
            src.LAST_UPDATE_BY,
            src.LAST_UPDATE_LOGIN,
            src.BZ_LOAD_DATE,
            src.SV_LOAD_DATE,
            @AsOfDate,
            @HighDate,
            @LoadDttm,
            @LoadDttm,
            'Y'
        FROM #src src
        LEFT JOIN svo.D_PARTY_SITE tgt
            ON tgt.PARTY_SITE_ID = src.PARTY_SITE_ID
           AND tgt.CURR_IND = 'Y'
        WHERE tgt.PARTY_SITE_ID IS NULL;

        SET @RowInserted = @@ROWCOUNT;

        /* ===== Advance watermark ===== */
        IF @MaxWatermark IS NOT NULL
        BEGIN
            UPDATE etl.ETL_WATERMARK
            SET
                LAST_WATERMARK = @MaxWatermark,
                UDT_DATE       = SYSDATETIME()
            WHERE TABLE_NAME = @TargetObject;
        END;

        /* ===== ETL_RUN end ===== */
        SET @EndDttm = SYSDATETIME();

        UPDATE etl.ETL_RUN
        SET
            END_DTTM      = @EndDttm,
            STATUS        = 'SUCCESS',
            ROW_INSERTED  = @RowInserted,
            ROW_EXPIRED   = @RowExpired,
            ROW_UPDATED   = @RowUpdated,
            ERROR_MESSAGE = NULL
        WHERE RUN_ID = @RunId;
    END TRY
    BEGIN CATCH
        SET @EndDttm = SYSDATETIME();
        SET @ErrMsg  = CONCAT('Error ', ERROR_NUMBER(), ' (Line ', ERROR_LINE(), '): ', ERROR_MESSAGE());

        IF @RunId IS NOT NULL
        BEGIN
            UPDATE etl.ETL_RUN
            SET
                END_DTTM      = @EndDttm,
                STATUS        = 'FAILURE',
                ROW_INSERTED  = @RowInserted,
                ROW_EXPIRED   = @RowExpired,
                ROW_UPDATED   = @RowUpdated,
                ERROR_MESSAGE = @ErrMsg
            WHERE RUN_ID = @RunId;
        END;

        ;THROW;
    END CATCH
END;
GO