CREATE OR ALTER PROCEDURE svo.usp_Load_D_LEGAL_ENTITY_SCD2
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    DECLARE
        @ProcName      SYSNAME        = OBJECT_SCHEMA_NAME(@@PROCID) + '.' + OBJECT_NAME(@@PROCID),
        @TargetObject  SYSNAME        = 'svo.D_LEGAL_ENTITY',
        @StartDttm     DATETIME2(0)   = SYSDATETIME(),
        @EndDttm       DATETIME2(0),
        @RunId         BIGINT         = NULL,
        @ErrMsg        NVARCHAR(4000) = NULL,

        @AsOfDate      DATE           = CAST(GETDATE() AS DATE),
        @LoadDttm      DATETIME2(0)   = SYSDATETIME(),
        @HighDate      DATE           = '9999-12-31',

        @LastWatermark DATETIME2(7),
        @MaxWatermark  DATETIME2(7)   = NULL,

        @RowInserted   INT            = 0,
        @RowExpired    INT            = 0,
        @RowUpdated    INT            = 0; -- not used for SCD2

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

        /* ===== Filtered unique index for SCD2 current row ===== */
        IF NOT EXISTS
        (
            SELECT 1
            FROM sys.indexes
            WHERE name = 'UX_D_LEGAL_ENTITY_ID_CURR'
              AND object_id = OBJECT_ID('svo.D_LEGAL_ENTITY')
        )
        BEGIN
            CREATE UNIQUE NONCLUSTERED INDEX UX_D_LEGAL_ENTITY_ID_CURR
            ON svo.D_LEGAL_ENTITY (LEGAL_ENTITY_ID)
            WHERE CURR_IND = 'Y'
            ON FG_SilverDim;
        END;

        /* ===== Ensure plug row (SK=0) exists ===== */
        IF NOT EXISTS (SELECT 1 FROM svo.D_LEGAL_ENTITY WHERE LEGAL_ENTITY_SK = 0)
        BEGIN
            SET IDENTITY_INSERT svo.D_LEGAL_ENTITY ON;

            INSERT INTO svo.D_LEGAL_ENTITY
            (
                LEGAL_ENTITY_SK,
                LEGAL_ENTITY_ID,
                LEGAL_ENTITY_ENTERPRISE_ID,
                LEGAL_ENTITY_GEOGRAPHY_ID,
                LEGAL_ENTITY_LEGAL_EMPLOYER_FLAG,
                LEGAL_ENTITY_IDENTIFIER,
                LEGAL_ENTITY_NAME,
                LEGAL_ENTITY_OBJECT_VERSION_NUMBER,
                LEGAL_ENTITY_PARTY_ID,
                LEGAL_ENTITY_PSU_FLAG,
                LEGAL_ENTITY_TRANSACTING_ENTITY_FLAG,
                LEGAL_ENTITY_LAST_UPDATE_DATE,
                LEGAL_ENTITY_LAST_UPDATE_LOGIN,
                LEGAL_ENTITY_LAST_UPDATED_BY,
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
                -1,
                'UNK',
                'UNK',
                'Unknown',
                -1,
                -1,
                'UNK',
                'UNK',
                CAST('0001-01-01' AS DATE),
                'UNK',
                'UNK',
                CAST(GETDATE() AS DATE),
                CAST(GETDATE() AS DATE),
                @AsOfDate,
                @HighDate,
                @LoadDttm,
                @LoadDttm,
                'Y'
            );

            SET IDENTITY_INSERT svo.D_LEGAL_ENTITY OFF;
        END;

        /* ===== Source (incremental + dedup by LEGAL_ENTITY_ID) ===== */
        IF OBJECT_ID('tempdb..#src') IS NOT NULL
            DROP TABLE #src;

        SELECT
            s.LEGAL_ENTITY_ID,
            s.LEGAL_ENTITY_ENTERPRISE_ID,
            s.LEGAL_ENTITY_GEOGRAPHY_ID,
            s.LEGAL_ENTITY_LEGAL_EMPLOYER_FLAG,
            s.LEGAL_ENTITY_IDENTIFIER,
            s.LEGAL_ENTITY_NAME,
            s.LEGAL_ENTITY_OBJECT_VERSION_NUMBER,
            s.LEGAL_ENTITY_PARTY_ID,
            s.LEGAL_ENTITY_PSU_FLAG,
            s.LEGAL_ENTITY_TRANSACTING_ENTITY_FLAG,
            s.LEGAL_ENTITY_LAST_UPDATE_DATE,
            s.LEGAL_ENTITY_LAST_UPDATE_LOGIN,
            s.LEGAL_ENTITY_LAST_UPDATED_BY,
            s.BZ_LOAD_DATE,
            s.SV_LOAD_DATE,
            s.SourceAddDateTime
        INTO #src
        FROM
        (
            SELECT
                CAST(p.LEGALENTITYLEGALENTITYID AS BIGINT)                 AS LEGAL_ENTITY_ID,
                TRY_CAST(p.LEGALENTITYENTERPRISEID AS INT)                AS LEGAL_ENTITY_ENTERPRISE_ID,
                TRY_CAST(p.LEGALENTITYGEOGRAPHYID AS BIGINT)              AS LEGAL_ENTITY_GEOGRAPHY_ID,
                CAST(p.LEGALENTITYLEGALEMPLOYERFLAG AS VARCHAR(4))        AS LEGAL_ENTITY_LEGAL_EMPLOYER_FLAG,
                CAST(p.LEGALENTITYLEGALENTITYIDENTIFIER AS VARCHAR(100))  AS LEGAL_ENTITY_IDENTIFIER,
                CAST(p.LEGALENTITYNAME AS VARCHAR(100))                   AS LEGAL_ENTITY_NAME,
                TRY_CAST(p.LEGALENTITYOBJECTVERSIONNUMBER AS FLOAT)       AS LEGAL_ENTITY_OBJECT_VERSION_NUMBER,
                TRY_CAST(p.LEGALENTITYPARTYID AS BIGINT)                  AS LEGAL_ENTITY_PARTY_ID,
                CAST(p.LEGALENTITYPSUFLAG AS VARCHAR(4))                  AS LEGAL_ENTITY_PSU_FLAG,
                CAST(p.LEGALENTITYTRANSACTINGENTITYFLAG AS VARCHAR(4))    AS LEGAL_ENTITY_TRANSACTING_ENTITY_FLAG,
                CAST(p.LEGALENTITYLASTUPDATEDATE AS DATE)                 AS LEGAL_ENTITY_LAST_UPDATE_DATE,
                CAST(p.LEGALENTITYLASTUPDATELOGIN AS VARCHAR(100))        AS LEGAL_ENTITY_LAST_UPDATE_LOGIN,
                CAST(p.LEGALENTITYLASTUPDATEDBY AS VARCHAR(64))           AS LEGAL_ENTITY_LAST_UPDATED_BY,

                /* BZ_LOAD_DATE rule (never NULL) */
                COALESCE(CAST(p.AddDateTime AS DATE), CAST(GETDATE() AS DATE)) AS BZ_LOAD_DATE,
                CAST(GETDATE() AS DATE)                                       AS SV_LOAD_DATE,

                p.AddDateTime AS SourceAddDateTime,

                ROW_NUMBER() OVER
                (
                    PARTITION BY CAST(p.LEGALENTITYLEGALENTITYID AS BIGINT)
                    ORDER BY p.AddDateTime DESC
                ) AS rn
            FROM bzo.GL_LegalEntityExtractPVO p
            WHERE
                p.LEGALENTITYLEGALENTITYID IS NOT NULL
                AND p.AddDateTime > @LastWatermark
        ) s
        WHERE s.rn = 1;

        /* Never treat the plug BK as a normal data row */
        DELETE FROM #src WHERE LEGAL_ENTITY_ID = -1;

        SELECT @MaxWatermark = MAX(SourceAddDateTime) FROM #src;

        /* ===== SCD2 expire changed current rows ===== */
        UPDATE tgt
        SET
            tgt.END_DATE = DATEADD(DAY, -1, @AsOfDate),
            tgt.CURR_IND = 'N',
            tgt.UDT_DATE = @LoadDttm
        FROM svo.D_LEGAL_ENTITY tgt
        INNER JOIN #src src
            ON src.LEGAL_ENTITY_ID = tgt.LEGAL_ENTITY_ID
        WHERE tgt.CURR_IND = 'Y'
          AND
          (
                ISNULL(tgt.LEGAL_ENTITY_ENTERPRISE_ID,-999) <> ISNULL(src.LEGAL_ENTITY_ENTERPRISE_ID,-999)
             OR ISNULL(tgt.LEGAL_ENTITY_GEOGRAPHY_ID,-999) <> ISNULL(src.LEGAL_ENTITY_GEOGRAPHY_ID,-999)
             OR ISNULL(tgt.LEGAL_ENTITY_LEGAL_EMPLOYER_FLAG,'') <> ISNULL(src.LEGAL_ENTITY_LEGAL_EMPLOYER_FLAG,'')
             OR ISNULL(tgt.LEGAL_ENTITY_IDENTIFIER,'') <> ISNULL(src.LEGAL_ENTITY_IDENTIFIER,'')
             OR ISNULL(tgt.LEGAL_ENTITY_NAME,'') <> ISNULL(src.LEGAL_ENTITY_NAME,'')
             OR ISNULL(tgt.LEGAL_ENTITY_OBJECT_VERSION_NUMBER,-999.0) <> ISNULL(src.LEGAL_ENTITY_OBJECT_VERSION_NUMBER,-999.0)
             OR ISNULL(tgt.LEGAL_ENTITY_PARTY_ID,-999) <> ISNULL(src.LEGAL_ENTITY_PARTY_ID,-999)
             OR ISNULL(tgt.LEGAL_ENTITY_PSU_FLAG,'') <> ISNULL(src.LEGAL_ENTITY_PSU_FLAG,'')
             OR ISNULL(tgt.LEGAL_ENTITY_TRANSACTING_ENTITY_FLAG,'') <> ISNULL(src.LEGAL_ENTITY_TRANSACTING_ENTITY_FLAG,'')
             OR ISNULL(tgt.LEGAL_ENTITY_LAST_UPDATE_DATE,'1900-01-01') <> ISNULL(src.LEGAL_ENTITY_LAST_UPDATE_DATE,'1900-01-01')
             OR ISNULL(tgt.LEGAL_ENTITY_LAST_UPDATE_LOGIN,'') <> ISNULL(src.LEGAL_ENTITY_LAST_UPDATE_LOGIN,'')
             OR ISNULL(tgt.LEGAL_ENTITY_LAST_UPDATED_BY,'') <> ISNULL(src.LEGAL_ENTITY_LAST_UPDATED_BY,'')
          );

        SET @RowExpired = @@ROWCOUNT;

        /* ===== Insert new current rows (new BKs + changed BKs just expired) ===== */
        INSERT INTO svo.D_LEGAL_ENTITY
        (
            LEGAL_ENTITY_ID,
            LEGAL_ENTITY_ENTERPRISE_ID,
            LEGAL_ENTITY_GEOGRAPHY_ID,
            LEGAL_ENTITY_LEGAL_EMPLOYER_FLAG,
            LEGAL_ENTITY_IDENTIFIER,
            LEGAL_ENTITY_NAME,
            LEGAL_ENTITY_OBJECT_VERSION_NUMBER,
            LEGAL_ENTITY_PARTY_ID,
            LEGAL_ENTITY_PSU_FLAG,
            LEGAL_ENTITY_TRANSACTING_ENTITY_FLAG,
            LEGAL_ENTITY_LAST_UPDATE_DATE,
            LEGAL_ENTITY_LAST_UPDATE_LOGIN,
            LEGAL_ENTITY_LAST_UPDATED_BY,
            BZ_LOAD_DATE,
            SV_LOAD_DATE,
            EFF_DATE,
            END_DATE,
            CRE_DATE,
            UDT_DATE,
            CURR_IND
        )
        SELECT
            src.LEGAL_ENTITY_ID,
            src.LEGAL_ENTITY_ENTERPRISE_ID,
            src.LEGAL_ENTITY_GEOGRAPHY_ID,
            src.LEGAL_ENTITY_LEGAL_EMPLOYER_FLAG,
            src.LEGAL_ENTITY_IDENTIFIER,
            src.LEGAL_ENTITY_NAME,
            src.LEGAL_ENTITY_OBJECT_VERSION_NUMBER,
            src.LEGAL_ENTITY_PARTY_ID,
            src.LEGAL_ENTITY_PSU_FLAG,
            src.LEGAL_ENTITY_TRANSACTING_ENTITY_FLAG,
            src.LEGAL_ENTITY_LAST_UPDATE_DATE,
            src.LEGAL_ENTITY_LAST_UPDATE_LOGIN,
            src.LEGAL_ENTITY_LAST_UPDATED_BY,
            src.BZ_LOAD_DATE,
            src.SV_LOAD_DATE,
            @AsOfDate,
            @HighDate,
            @LoadDttm,
            @LoadDttm,
            'Y'
        FROM #src src
        LEFT JOIN svo.D_LEGAL_ENTITY tgt
            ON tgt.LEGAL_ENTITY_ID = src.LEGAL_ENTITY_ID
           AND tgt.CURR_IND = 'Y'
        WHERE tgt.LEGAL_ENTITY_ID IS NULL;

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