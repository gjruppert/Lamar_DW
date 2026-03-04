CREATE OR ALTER PROCEDURE svo.usp_Load_D_ACCOUNT_SCD2
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    DECLARE
        @ProcName       SYSNAME        = OBJECT_SCHEMA_NAME(@@PROCID) + '.' + OBJECT_NAME(@@PROCID),
        @TargetObject   SYSNAME        = 'svo.D_ACCOUNT',
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
        SELECT
            @LastWatermark = w.LAST_WATERMARK
        FROM etl.ETL_WATERMARK w
        WHERE w.TABLE_NAME = @TargetObject;

        IF @LastWatermark IS NULL
            SET @LastWatermark = '1900-01-01';

        /* ===== ETL_RUN start ===== */
        INSERT INTO etl.ETL_RUN (PROC_NAME, TARGET_OBJECT, ASOF_DATE, START_DTTM, STATUS)
        VALUES (@ProcName, @TargetObject, @AsOfDate, @StartDttm, 'STARTED');

        SET @RunId = SCOPE_IDENTITY();

        /* ===== Ensure plug row (SK=0) exists ===== */
        IF NOT EXISTS (SELECT 1 FROM svo.D_ACCOUNT WHERE ACCOUNT_SK = 0)
        BEGIN
            SET IDENTITY_INSERT svo.D_ACCOUNT ON;

            INSERT INTO svo.D_ACCOUNT
            (
                ACCOUNT_SK,
                ACCOUNT_ID,
                ACCOUNT_LVL1_CODE, ACCOUNT_LVL1_DESC,
                ACCOUNT_LVL2_CODE, ACCOUNT_LVL2_DESC,
                ACCOUNT_LVL3_CODE, ACCOUNT_LVL3_DESC,
                ACCOUNT_LVL4_CODE, ACCOUNT_LVL4_DESC,
                ACCOUNT_LVL5_CODE, ACCOUNT_LVL5_DESC,
                ACCOUNT_LVL6_CODE, ACCOUNT_LVL6_DESC,
                ACCOUNT_LVL7_CODE, ACCOUNT_LVL7_DESC,
                ACCOUNT_LVL8_CODE, ACCOUNT_LVL8_DESC,
                ACCOUNT_LVL9_CODE, ACCOUNT_LVL9_DESC,
                ACCOUNT_DISTANCE,
                ACCOUNT_CATEGORY,
                ACCOUNT_ENABLED_FLAG,
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
                '-1','Unknown',
                '-1','Unknown',
                '-1','Unknown',
                '-1','Unknown',
                '-1','Unknown',
                '-1','Unknown',
                0,
                'Missing',
                NULL,
                '0001-01-01',
                '9999-12-31',
                'SYSTEM',
                '2025-10-18',
                CAST(GETDATE() AS DATE),
                CAST(GETDATE() AS DATE),
                @AsOfDate,
                @HighDate,
                @LoadDttm,
                @LoadDttm,
                'Y'
            );

            SET IDENTITY_INSERT svo.D_ACCOUNT OFF;
        END;

        /* ===== Unique index on BK ===== */
        IF NOT EXISTS
        (
            SELECT 1
            FROM sys.indexes
            WHERE name = 'UX_D_ACCOUNT_ID'
              AND object_id = OBJECT_ID('svo.D_ACCOUNT')
        )
        BEGIN
            CREATE UNIQUE NONCLUSTERED INDEX UX_D_ACCOUNT_ID
            ON svo.D_ACCOUNT (ACCOUNT_ID)
            ON FG_SilverDim;
        END;

        /* ===== Source (incremental + dedup by ACCOUNT_ID) ===== */
        IF OBJECT_ID('tempdb..#src') IS NOT NULL
            DROP TABLE #src;

        SELECT
            s.ACCOUNT_ID,
            s.ACCOUNT_LVL1_CODE, s.ACCOUNT_LVL1_DESC,
            s.ACCOUNT_LVL2_CODE, s.ACCOUNT_LVL2_DESC,
            s.ACCOUNT_LVL3_CODE, s.ACCOUNT_LVL3_DESC,
            s.ACCOUNT_LVL4_CODE, s.ACCOUNT_LVL4_DESC,
            s.ACCOUNT_LVL5_CODE, s.ACCOUNT_LVL5_DESC,
            s.ACCOUNT_LVL6_CODE, s.ACCOUNT_LVL6_DESC,
            s.ACCOUNT_LVL7_CODE, s.ACCOUNT_LVL7_DESC,
            s.ACCOUNT_LVL8_CODE, s.ACCOUNT_LVL8_DESC,
            s.ACCOUNT_LVL9_CODE, s.ACCOUNT_LVL9_DESC,
            s.ACCOUNT_DISTANCE,
            s.ACCOUNT_CATEGORY,
            s.ACCOUNT_ENABLED_FLAG,
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
                ISNULL(LTRIM(RTRIM(lvl9.VALUE)),'-1') AS ACCOUNT_ID,

                COALESCE(h1.DEP31PK1VALUE,h1.DEP30PK1VALUE,h1.DEP29PK1VALUE,h1.DEP28PK1VALUE,h1.DEP27PK1VALUE,h1.DEP26PK1VALUE,h1.DEP25PK1VALUE,h1.DEP24PK1VALUE,lvl9.VALUE) AS ACCOUNT_LVL1_CODE,
                COALESCE(lvl1.DESCRIPTION,lvl2.DESCRIPTION,lvl3.DESCRIPTION,lvl4.DESCRIPTION,lvl5.DESCRIPTION,lvl6.DESCRIPTION,lvl8.DESCRIPTION,lvl9.DESCRIPTION)  AS ACCOUNT_LVL1_DESC,

                COALESCE(h1.DEP30PK1VALUE,h1.DEP29PK1VALUE,h1.DEP28PK1VALUE,h1.DEP27PK1VALUE,h1.DEP26PK1VALUE,h1.DEP25PK1VALUE,h1.DEP24PK1VALUE,lvl9.VALUE) AS ACCOUNT_LVL2_CODE,
                COALESCE(lvl2.DESCRIPTION,lvl3.DESCRIPTION,lvl4.DESCRIPTION,lvl5.DESCRIPTION,lvl6.DESCRIPTION,lvl8.DESCRIPTION,lvl9.DESCRIPTION)  AS ACCOUNT_LVL2_DESC,

                COALESCE(h1.DEP29PK1VALUE,h1.DEP28PK1VALUE,h1.DEP27PK1VALUE,h1.DEP26PK1VALUE,h1.DEP25PK1VALUE,h1.DEP24PK1VALUE,lvl9.VALUE) AS ACCOUNT_LVL3_CODE,
                COALESCE(lvl3.DESCRIPTION,lvl4.DESCRIPTION,lvl5.DESCRIPTION,lvl6.DESCRIPTION,lvl8.DESCRIPTION,lvl9.DESCRIPTION)  AS ACCOUNT_LVL3_DESC,

                COALESCE(h1.DEP28PK1VALUE,h1.DEP27PK1VALUE,h1.DEP26PK1VALUE,h1.DEP25PK1VALUE,h1.DEP24PK1VALUE,lvl9.VALUE) AS ACCOUNT_LVL4_CODE,
                COALESCE(lvl4.DESCRIPTION,lvl5.DESCRIPTION,lvl6.DESCRIPTION,lvl8.DESCRIPTION,lvl9.DESCRIPTION)  AS ACCOUNT_LVL4_DESC,

                COALESCE(h1.DEP27PK1VALUE,h1.DEP26PK1VALUE,h1.DEP25PK1VALUE,h1.DEP24PK1VALUE,lvl9.VALUE) AS ACCOUNT_LVL5_CODE,
                COALESCE(lvl5.DESCRIPTION,lvl6.DESCRIPTION,lvl8.DESCRIPTION,lvl9.DESCRIPTION)  AS ACCOUNT_LVL5_DESC,

                COALESCE(h1.DEP26PK1VALUE,h1.DEP25PK1VALUE,h1.DEP24PK1VALUE,lvl9.VALUE) AS ACCOUNT_LVL6_CODE,
                COALESCE(lvl6.DESCRIPTION,lvl8.DESCRIPTION,lvl9.DESCRIPTION) AS ACCOUNT_LVL6_DESC,

                COALESCE(h1.DEP25PK1VALUE,h1.DEP24PK1VALUE,lvl9.VALUE) AS ACCOUNT_LVL7_CODE,
                COALESCE(lvl7.DESCRIPTION,lvl8.DESCRIPTION,lvl9.DESCRIPTION) AS ACCOUNT_LVL7_DESC,

                COALESCE(h1.DEP24PK1VALUE,lvl9.VALUE)  AS ACCOUNT_LVL8_CODE,
                COALESCE(lvl8.DESCRIPTION,lvl9.DESCRIPTION) AS ACCOUNT_LVL8_DESC,

                lvl9.VALUE       AS ACCOUNT_LVL9_CODE,
                lvl9.DESCRIPTION AS ACCOUNT_LVL9_DESC,

                0 AS ACCOUNT_DISTANCE,
                lvl9.ATTRIBUTECATEGORY AS ACCOUNT_CATEGORY,
                lvl9.ENABLEDFLAG AS ACCOUNT_ENABLED_FLAG,
                ISNULL(lvl9.STARTDATEACTIVE, '0001-01-01') AS START_DATE_ACTIVE,
                ISNULL(lvl9.ENDDATEACTIVE,  '9999-12-31') AS END_DATE_ACTIVE,
                lvl9.CREATEDBY AS CREATED_BY,
                CAST(lvl9.CREATIONDATE AS DATE) AS CREATION_DATE,

                COALESCE(CAST(h1.AddDateTime AS DATE), CAST(GETDATE() AS DATE)) AS BZ_LOAD_DATE,
                CAST(GETDATE() AS DATE) AS SV_LOAD_DATE,

                (SELECT MAX(v.dt)
                 FROM (VALUES
                        (h1.AddDateTime),
                        (ver1.AddDateTime),
                        (lvl1.AddDateTime),
                        (lvl2.AddDateTime),
                        (lvl3.AddDateTime),
                        (lvl4.AddDateTime),
                        (lvl5.AddDateTime),
                        (lvl6.AddDateTime),
                        (lvl7.AddDateTime),
                        (lvl8.AddDateTime),
                        (lvl9.AddDateTime)
                      ) v(dt)
                ) AS SourceAddDateTime,

                ROW_NUMBER() OVER
                (
                    PARTITION BY ISNULL(LTRIM(RTRIM(lvl9.VALUE)),'-1')
                    ORDER BY
                        (SELECT MAX(v.dt)
                         FROM (VALUES
                                (h1.AddDateTime),
                                (ver1.AddDateTime),
                                (lvl1.AddDateTime),
                                (lvl2.AddDateTime),
                                (lvl3.AddDateTime),
                                (lvl4.AddDateTime),
                                (lvl5.AddDateTime),
                                (lvl6.AddDateTime),
                                (lvl7.AddDateTime),
                                (lvl8.AddDateTime),
                                (lvl9.AddDateTime)
                              ) v(dt)
                        ) DESC
                ) AS rn
            FROM bzo.GL_SegmentValueHierarchyExtractPVO h1
            INNER JOIN bzo.GL_FndTreeAndVersionVO ver1
                ON ver1.TREEVERSIONID = h1.TREEVERSIONID
               AND ver1.TREENAME LIKE 'ACCOUNT LAMAR'
            INNER JOIN bzo.GL_ValueSetValuesPVO lvl1
                ON lvl1.VALUE = h1.DEP31PK1VALUE
               AND lvl1.ATTRIBUTECATEGORY = 'ACCOUNT LAMAR'
            INNER JOIN bzo.GL_ValueSetValuesPVO lvl2
                ON lvl2.VALUE = h1.DEP30PK1VALUE
               AND lvl2.ATTRIBUTECATEGORY = 'ACCOUNT LAMAR'
            INNER JOIN bzo.GL_ValueSetValuesPVO lvl3
                ON lvl3.VALUE = h1.DEP29PK1VALUE
               AND lvl3.ATTRIBUTECATEGORY = 'ACCOUNT LAMAR'
            INNER JOIN bzo.GL_ValueSetValuesPVO lvl4
                ON lvl4.VALUE = h1.DEP28PK1VALUE
               AND lvl4.ATTRIBUTECATEGORY = 'ACCOUNT LAMAR'
            INNER JOIN bzo.GL_ValueSetValuesPVO lvl5
                ON lvl5.VALUE = h1.DEP27PK1VALUE
               AND lvl5.ATTRIBUTECATEGORY = 'ACCOUNT LAMAR'
            INNER JOIN bzo.GL_ValueSetValuesPVO lvl6
                ON lvl6.VALUE = h1.DEP26PK1VALUE
               AND lvl6.ATTRIBUTECATEGORY = 'ACCOUNT LAMAR'
            INNER JOIN bzo.GL_ValueSetValuesPVO lvl7
                ON lvl7.VALUE = h1.DEP25PK1VALUE
               AND lvl7.ATTRIBUTECATEGORY = 'ACCOUNT LAMAR'
            INNER JOIN bzo.GL_ValueSetValuesPVO lvl8
                ON lvl8.VALUE = h1.DEP24PK1VALUE
               AND lvl8.ATTRIBUTECATEGORY = 'ACCOUNT LAMAR'
            RIGHT JOIN bzo.GL_ValueSetValuesPVO lvl9
                ON lvl9.VALUE = h1.DEP0PK1VALUE
               AND lvl9.ATTRIBUTECATEGORY = 'ACCOUNT LAMAR'
            WHERE
                lvl9.ATTRIBUTECATEGORY = 'ACCOUNT LAMAR'
                AND lvl9.SUMMARYFLAG = 'N'
                AND
                (
                    h1.AddDateTime   > @LastWatermark OR
                    ver1.AddDateTime > @LastWatermark OR
                    lvl1.AddDateTime > @LastWatermark OR
                    lvl2.AddDateTime > @LastWatermark OR
                    lvl3.AddDateTime > @LastWatermark OR
                    lvl4.AddDateTime > @LastWatermark OR
                    lvl5.AddDateTime > @LastWatermark OR
                    lvl6.AddDateTime > @LastWatermark OR
                    lvl7.AddDateTime > @LastWatermark OR
                    lvl8.AddDateTime > @LastWatermark OR
                    lvl9.AddDateTime > @LastWatermark
                )
        ) s
        WHERE s.rn = 1;

        SELECT @MaxWatermark = MAX(SourceAddDateTime) FROM #src;

        /* ===== SCD2 expire changed current rows ===== */
        UPDATE tgt
        SET
            tgt.END_DATE = DATEADD(DAY, -1, @AsOfDate),
            tgt.CURR_IND = 'N',
            tgt.UDT_DATE = @LoadDttm
        FROM svo.D_ACCOUNT tgt
        INNER JOIN #src src
            ON src.ACCOUNT_ID = tgt.ACCOUNT_ID
        WHERE tgt.CURR_IND = 'Y'
          AND
          (
                ISNULL(tgt.ACCOUNT_LVL1_CODE,'') <> ISNULL(src.ACCOUNT_LVL1_CODE,'')
             OR ISNULL(tgt.ACCOUNT_LVL1_DESC,'') <> ISNULL(src.ACCOUNT_LVL1_DESC,'')
             OR ISNULL(tgt.ACCOUNT_LVL2_CODE,'') <> ISNULL(src.ACCOUNT_LVL2_CODE,'')
             OR ISNULL(tgt.ACCOUNT_LVL2_DESC,'') <> ISNULL(src.ACCOUNT_LVL2_DESC,'')
             OR ISNULL(tgt.ACCOUNT_LVL3_CODE,'') <> ISNULL(src.ACCOUNT_LVL3_CODE,'')
             OR ISNULL(tgt.ACCOUNT_LVL3_DESC,'') <> ISNULL(src.ACCOUNT_LVL3_DESC,'')
             OR ISNULL(tgt.ACCOUNT_LVL4_CODE,'') <> ISNULL(src.ACCOUNT_LVL4_CODE,'')
             OR ISNULL(tgt.ACCOUNT_LVL4_DESC,'') <> ISNULL(src.ACCOUNT_LVL4_DESC,'')
             OR ISNULL(tgt.ACCOUNT_LVL5_CODE,'') <> ISNULL(src.ACCOUNT_LVL5_CODE,'')
             OR ISNULL(tgt.ACCOUNT_LVL5_DESC,'') <> ISNULL(src.ACCOUNT_LVL5_DESC,'')
             OR ISNULL(tgt.ACCOUNT_LVL6_CODE,'') <> ISNULL(src.ACCOUNT_LVL6_CODE,'')
             OR ISNULL(tgt.ACCOUNT_LVL6_DESC,'') <> ISNULL(src.ACCOUNT_LVL6_DESC,'')
             OR ISNULL(tgt.ACCOUNT_LVL7_CODE,'') <> ISNULL(src.ACCOUNT_LVL7_CODE,'')
             OR ISNULL(tgt.ACCOUNT_LVL7_DESC,'') <> ISNULL(src.ACCOUNT_LVL7_DESC,'')
             OR ISNULL(tgt.ACCOUNT_LVL8_CODE,'') <> ISNULL(src.ACCOUNT_LVL8_CODE,'')
             OR ISNULL(tgt.ACCOUNT_LVL8_DESC,'') <> ISNULL(src.ACCOUNT_LVL8_DESC,'')
             OR ISNULL(tgt.ACCOUNT_LVL9_CODE,'') <> ISNULL(src.ACCOUNT_LVL9_CODE,'')
             OR ISNULL(tgt.ACCOUNT_LVL9_DESC,'') <> ISNULL(src.ACCOUNT_LVL9_DESC,'')
             OR ISNULL(tgt.ACCOUNT_DISTANCE, -999) <> ISNULL(src.ACCOUNT_DISTANCE, -999)
             OR ISNULL(tgt.ACCOUNT_CATEGORY,'') <> ISNULL(src.ACCOUNT_CATEGORY,'')
             OR ISNULL(tgt.ACCOUNT_ENABLED_FLAG,'') <> ISNULL(src.ACCOUNT_ENABLED_FLAG,'')
             OR tgt.START_DATE_ACTIVE <> src.START_DATE_ACTIVE
             OR tgt.END_DATE_ACTIVE <> src.END_DATE_ACTIVE
             OR ISNULL(tgt.CREATED_BY,'') <> ISNULL(src.CREATED_BY,'')
             OR ISNULL(tgt.CREATION_DATE,'1900-01-01') <> ISNULL(src.CREATION_DATE,'1900-01-01')
          );

        SET @RowExpired = @@ROWCOUNT;

        /* ===== Insert new current rows ===== */
        INSERT INTO svo.D_ACCOUNT
        (
            ACCOUNT_ID,
            ACCOUNT_LVL1_CODE, ACCOUNT_LVL1_DESC,
            ACCOUNT_LVL2_CODE, ACCOUNT_LVL2_DESC,
            ACCOUNT_LVL3_CODE, ACCOUNT_LVL3_DESC,
            ACCOUNT_LVL4_CODE, ACCOUNT_LVL4_DESC,
            ACCOUNT_LVL5_CODE, ACCOUNT_LVL5_DESC,
            ACCOUNT_LVL6_CODE, ACCOUNT_LVL6_DESC,
            ACCOUNT_LVL7_CODE, ACCOUNT_LVL7_DESC,
            ACCOUNT_LVL8_CODE, ACCOUNT_LVL8_DESC,
            ACCOUNT_LVL9_CODE, ACCOUNT_LVL9_DESC,
            ACCOUNT_DISTANCE,
            ACCOUNT_CATEGORY,
            ACCOUNT_ENABLED_FLAG,
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
            src.ACCOUNT_ID,
            src.ACCOUNT_LVL1_CODE, src.ACCOUNT_LVL1_DESC,
            src.ACCOUNT_LVL2_CODE, src.ACCOUNT_LVL2_DESC,
            src.ACCOUNT_LVL3_CODE, src.ACCOUNT_LVL3_DESC,
            src.ACCOUNT_LVL4_CODE, src.ACCOUNT_LVL4_DESC,
            src.ACCOUNT_LVL5_CODE, src.ACCOUNT_LVL5_DESC,
            src.ACCOUNT_LVL6_CODE, src.ACCOUNT_LVL6_DESC,
            src.ACCOUNT_LVL7_CODE, src.ACCOUNT_LVL7_DESC,
            src.ACCOUNT_LVL8_CODE, src.ACCOUNT_LVL8_DESC,
            src.ACCOUNT_LVL9_CODE, src.ACCOUNT_LVL9_DESC,
            src.ACCOUNT_DISTANCE,
            src.ACCOUNT_CATEGORY,
            src.ACCOUNT_ENABLED_FLAG,
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
        LEFT JOIN svo.D_ACCOUNT tgt
            ON tgt.ACCOUNT_ID = src.ACCOUNT_ID
           AND tgt.CURR_IND = 'Y'
        WHERE tgt.ACCOUNT_ID IS NULL;

        SET @RowInserted = @@ROWCOUNT;

        /* ===== Advance watermark ===== */
        IF @MaxWatermark IS NOT NULL
        BEGIN
            UPDATE etl.ETL_WATERMARK
            SET LAST_WATERMARK = @MaxWatermark,
                UDT_DATE       = SYSDATETIME()
            WHERE TABLE_NAME = @TargetObject;
        END

        /* ===== ETL_RUN end ===== */
        SET @EndDttm = SYSDATETIME();

        UPDATE etl.ETL_RUN
        SET END_DTTM        = @EndDttm,
            STATUS          = 'SUCCESS',
            ROW_INSERTED    = @RowInserted,
            ROW_EXPIRED     = @RowExpired,
            ROW_UPDATED     = @RowUpdated,
            ERROR_MESSAGE   = NULL
        WHERE RUN_ID = @RunId;
    END TRY
    BEGIN CATCH
        SET @EndDttm = SYSDATETIME();
        SET @ErrMsg  = CONCAT('Error ', ERROR_NUMBER(), ' (Line ', ERROR_LINE(), '): ', ERROR_MESSAGE());

        IF @RunId IS NOT NULL
        BEGIN
            UPDATE etl.ETL_RUN
            SET END_DTTM        = @EndDttm,
                STATUS          = 'FAILURE',
                ROW_INSERTED    = @RowInserted,
                ROW_EXPIRED     = @RowExpired,
                ROW_UPDATED     = @RowUpdated,
                ERROR_MESSAGE   = @ErrMsg
            WHERE RUN_ID = @RunId;
        END;

        ;THROW;
    END CATCH
END;
GO