CREATE OR ALTER PROCEDURE svo.usp_Load_D_INDUSTRY_SCD2
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    DECLARE
        @ProcName       SYSNAME        = OBJECT_SCHEMA_NAME(@@PROCID) + '.' + OBJECT_NAME(@@PROCID),
        @TargetObject   SYSNAME        = 'svo.D_INDUSTRY',
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
        @RowUpdated     INT            = 0; -- not used for SCD2

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

        /* ===== Unique index on BK ===== */
        IF NOT EXISTS
        (
            SELECT 1
            FROM sys.indexes
            WHERE name = 'UX_D_INDUSTRY_ID'
              AND object_id = OBJECT_ID('svo.D_INDUSTRY')
        )
        BEGIN
            CREATE UNIQUE NONCLUSTERED INDEX UX_D_INDUSTRY_ID
            ON svo.D_INDUSTRY (INDUSTRY_ID)
            ON FG_SilverDim;
        END;

        /* ===== Ensure plug row (SK=0) exists ===== */
        IF NOT EXISTS (SELECT 1 FROM svo.D_INDUSTRY WHERE INDUSTRY_SK = 0)
        BEGIN
            SET IDENTITY_INSERT svo.D_INDUSTRY ON;

            INSERT INTO svo.D_INDUSTRY
            (
                INDUSTRY_SK,
                INDUSTRY_ID,
                INDUSTRY_LVL1_CODE, INDUSTRY_LVL1_DESC,
                INDUSTRY_LVL2_CODE, INDUSTRY_LVL2_DESC,
                INDUSTRY_LVL3_CODE, INDUSTRY_LVL3_DESC,
                INDUSTRY_DISTANCE,
                INDUSTRY_CATEGORY,
                INDUSTRY_ENABLED_FLAG,
                START_DATE_ACTIVE,
                END_DATE_ACTIVE,
                CREATED_BY,
                CREATION_DATE,
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
                '-1',
                '-1','Unknown',
                '-1','Unknown',
                '-1','Unknown',
                0,
                'Missing',
                NULL,
                '0001-01-01',
                '9999-12-31',
                'UNK',
                '2025-10-18',
                CAST(GETDATE() AS DATE),
                CAST(GETDATE() AS DATE),
                @AsOfDate,
                @HighDate,
                @LoadDttm,
                @LoadDttm,
                'Y'
            );

            SET IDENTITY_INSERT svo.D_INDUSTRY OFF;
        END;

        /* ===== Source (incremental + dedup by INDUSTRY_ID) ===== */
        IF OBJECT_ID('tempdb..#src') IS NOT NULL
            DROP TABLE #src;

        SELECT
            s.INDUSTRY_ID,
            s.INDUSTRY_LVL1_CODE, s.INDUSTRY_LVL1_DESC,
            s.INDUSTRY_LVL2_CODE, s.INDUSTRY_LVL2_DESC,
            s.INDUSTRY_LVL3_CODE, s.INDUSTRY_LVL3_DESC,
            s.INDUSTRY_DISTANCE,
            s.INDUSTRY_CATEGORY,
            s.INDUSTRY_ENABLED_FLAG,
            s.START_DATE_ACTIVE,
            s.END_DATE_ACTIVE,
            s.CREATED_BY,
            s.CREATION_DATE,
            s.BZ_LOAD_DATE,
            s.SV_LOAD_DATE,
            s.SourceAddDateTime
        INTO #src
        FROM
        (
            SELECT
                ISNULL(LTRIM(RTRIM(lvl3.VALUE)),'-1') AS INDUSTRY_ID,

                COALESCE(h1.DEP31PK1VALUE,h1.DEP30PK1VALUE,lvl3.VALUE) AS INDUSTRY_LVL1_CODE,
                COALESCE(lvl1.DESCRIPTION,lvl2.DESCRIPTION,lvl3.DESCRIPTION) AS INDUSTRY_LVL1_DESC,

                COALESCE(h1.DEP30PK1VALUE,lvl3.VALUE) AS INDUSTRY_LVL2_CODE,
                COALESCE(lvl2.DESCRIPTION,lvl3.DESCRIPTION) AS INDUSTRY_LVL2_DESC,

                lvl3.VALUE       AS INDUSTRY_LVL3_CODE,
                lvl3.DESCRIPTION AS INDUSTRY_LVL3_DESC,

                0 AS INDUSTRY_DISTANCE,
                lvl3.ATTRIBUTECATEGORY AS INDUSTRY_CATEGORY,
                lvl3.ENABLEDFLAG AS INDUSTRY_ENABLED_FLAG,
                ISNULL(lvl3.STARTDATEACTIVE, '0001-01-01') AS START_DATE_ACTIVE,
                ISNULL(lvl3.ENDDATEACTIVE,  '9999-12-31') AS END_DATE_ACTIVE,
                lvl3.CREATEDBY AS CREATED_BY,
                CAST(lvl3.CREATIONDATE AS DATE) AS CREATION_DATE,

                /* BZ_LOAD_DATE rule (never NULL) */
                COALESCE(CAST(h1.AddDateTime AS DATE), CAST(GETDATE() AS DATE)) AS BZ_LOAD_DATE,
                CAST(GETDATE() AS DATE) AS SV_LOAD_DATE,

                (SELECT MAX(v.dt)
                 FROM (VALUES
                        (h1.AddDateTime),
                        (ver1.AddDateTime),
                        (lvl1.AddDateTime),
                        (lvl2.AddDateTime),
                        (lvl3.AddDateTime)
                      ) v(dt)
                ) AS SourceAddDateTime,

                ROW_NUMBER() OVER
                (
                    PARTITION BY ISNULL(LTRIM(RTRIM(lvl3.VALUE)),'-1')
                    ORDER BY
                        (SELECT MAX(v.dt)
                         FROM (VALUES
                                (h1.AddDateTime),
                                (ver1.AddDateTime),
                                (lvl1.AddDateTime),
                                (lvl2.AddDateTime),
                                (lvl3.AddDateTime)
                              ) v(dt)
                        ) DESC
                ) AS rn
            FROM bzo.GL_SegmentValueHierarchyExtractPVO h1
            INNER JOIN bzo.GL_FndTreeAndVersionVO ver1
                ON ver1.TREEVERSIONID = h1.TREEVERSIONID
               AND ver1.TREENAME LIKE 'INDUSTRY LAMAR'
            INNER JOIN bzo.GL_ValueSetValuesPVO lvl1
                ON lvl1.VALUE = h1.DEP31PK1VALUE
               AND lvl1.ATTRIBUTECATEGORY = 'INDUSTRY LAMAR'
            INNER JOIN bzo.GL_ValueSetValuesPVO lvl2
                ON lvl2.VALUE = h1.DEP30PK1VALUE
               AND lvl2.ATTRIBUTECATEGORY = 'INDUSTRY LAMAR'
            RIGHT JOIN bzo.GL_ValueSetValuesPVO lvl3
                ON lvl3.VALUE = h1.DEP0PK1VALUE
               AND lvl3.ATTRIBUTECATEGORY = 'INDUSTRY LAMAR'
            WHERE
                lvl3.ATTRIBUTECATEGORY = 'INDUSTRY LAMAR'
                AND lvl3.SUMMARYFLAG = 'N'
                AND
                (
                    h1.AddDateTime   > @LastWatermark OR
                    ver1.AddDateTime > @LastWatermark OR
                    lvl1.AddDateTime > @LastWatermark OR
                    lvl2.AddDateTime > @LastWatermark OR
                    lvl3.AddDateTime > @LastWatermark
                )
        ) s
        WHERE s.rn = 1;

        /* Never treat the plug BK as a normal data row */
        DELETE FROM #src WHERE INDUSTRY_ID = '-1';

        SELECT @MaxWatermark = MAX(SourceAddDateTime) FROM #src;

        /* ===== SCD2 expire changed current rows ===== */
        UPDATE tgt
        SET
            tgt.END_DATE = DATEADD(DAY, -1, @AsOfDate),
            tgt.CURR_IND = 'N',
            tgt.UDT_DATE = @LoadDttm
        FROM svo.D_INDUSTRY tgt
        INNER JOIN #src src
            ON src.INDUSTRY_ID = tgt.INDUSTRY_ID
        WHERE tgt.CURR_IND = 'Y'
          AND
          (
                ISNULL(tgt.INDUSTRY_LVL1_CODE,'') <> ISNULL(src.INDUSTRY_LVL1_CODE,'')
             OR ISNULL(tgt.INDUSTRY_LVL1_DESC,'') <> ISNULL(src.INDUSTRY_LVL1_DESC,'')
             OR ISNULL(tgt.INDUSTRY_LVL2_CODE,'') <> ISNULL(src.INDUSTRY_LVL2_CODE,'')
             OR ISNULL(tgt.INDUSTRY_LVL2_DESC,'') <> ISNULL(src.INDUSTRY_LVL2_DESC,'')
             OR ISNULL(tgt.INDUSTRY_LVL3_CODE,'') <> ISNULL(src.INDUSTRY_LVL3_CODE,'')
             OR ISNULL(tgt.INDUSTRY_LVL3_DESC,'') <> ISNULL(src.INDUSTRY_LVL3_DESC,'')
             OR ISNULL(tgt.INDUSTRY_DISTANCE,-999) <> ISNULL(src.INDUSTRY_DISTANCE,-999)
             OR ISNULL(tgt.INDUSTRY_CATEGORY,'') <> ISNULL(src.INDUSTRY_CATEGORY,'')
             OR ISNULL(tgt.INDUSTRY_ENABLED_FLAG,'') <> ISNULL(src.INDUSTRY_ENABLED_FLAG,'')
             OR tgt.START_DATE_ACTIVE <> src.START_DATE_ACTIVE
             OR tgt.END_DATE_ACTIVE <> src.END_DATE_ACTIVE
             OR ISNULL(tgt.CREATED_BY,'') <> ISNULL(src.CREATED_BY,'')
             OR ISNULL(tgt.CREATION_DATE,'1900-01-01') <> ISNULL(src.CREATION_DATE,'1900-01-01')
          );

        SET @RowExpired = @@ROWCOUNT;

        /* ===== Insert new current rows ===== */
        INSERT INTO svo.D_INDUSTRY
        (
            INDUSTRY_ID,
            INDUSTRY_LVL1_CODE, INDUSTRY_LVL1_DESC,
            INDUSTRY_LVL2_CODE, INDUSTRY_LVL2_DESC,
            INDUSTRY_LVL3_CODE, INDUSTRY_LVL3_DESC,
            INDUSTRY_DISTANCE,
            INDUSTRY_CATEGORY,
            INDUSTRY_ENABLED_FLAG,
            START_DATE_ACTIVE,
            END_DATE_ACTIVE,
            CREATED_BY,
            CREATION_DATE,
            BZ_LOAD_DATE,
            SV_LOAD_DATE,
            EFF_DATE,
            END_DATE,
            CRE_DATE,
            UDT_DATE,
            CURR_IND
        )
        SELECT
            src.INDUSTRY_ID,
            src.INDUSTRY_LVL1_CODE, src.INDUSTRY_LVL1_DESC,
            src.INDUSTRY_LVL2_CODE, src.INDUSTRY_LVL2_DESC,
            src.INDUSTRY_LVL3_CODE, src.INDUSTRY_LVL3_DESC,
            src.INDUSTRY_DISTANCE,
            src.INDUSTRY_CATEGORY,
            src.INDUSTRY_ENABLED_FLAG,
            src.START_DATE_ACTIVE,
            src.END_DATE_ACTIVE,
            src.CREATED_BY,
            src.CREATION_DATE,
            src.BZ_LOAD_DATE,
            src.SV_LOAD_DATE,
            @AsOfDate,
            @HighDate,
            @LoadDttm,
            @LoadDttm,
            'Y'
        FROM #src src
        LEFT JOIN svo.D_INDUSTRY tgt
            ON tgt.INDUSTRY_ID = src.INDUSTRY_ID
           AND tgt.CURR_IND = 'Y'
        WHERE tgt.INDUSTRY_ID IS NULL;

        SET @RowInserted = @@ROWCOUNT;

        /* ===== Advance watermark ===== */
        IF @MaxWatermark IS NOT NULL
        BEGIN
            UPDATE etl.ETL_WATERMARK
            SET
                LAST_WATERMARK = @MaxWatermark,
                UDT_DATE       = SYSDATETIME()
            WHERE TABLE_NAME = @TargetObject;
        END

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