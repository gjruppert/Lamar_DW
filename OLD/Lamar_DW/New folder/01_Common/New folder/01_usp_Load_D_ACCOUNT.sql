/* =====================================================================================
   D_ACCOUNT (Dim, Type 1, hash-based incremental; optional full reload)
   - Uses src synonyms:
       src.bzo_GL_SegmentValueHierarchyExtractPVO
       src.bzo_GL_FndTreeAndVersionVO
       src.bzo_GL_ValueSetValuesPVO
   - Logging: svo.DW_LOAD_LOG
   - Error rethrow: RAISERROR (compat-friendly)
   - Load date rules:
       BZ_LOAD_DATE = CAST(AddDateTime AS DATE)
       SV_LOAD_DATE = CAST(GETDATE() AS DATE)
   ===================================================================================== */

USE Oracle_Reporting_P2;
GO

/* =======
   TABLE
   ======= */
IF OBJECT_ID(N'svo.D_ACCOUNT', 'U') IS NULL
BEGIN
    CREATE TABLE svo.D_ACCOUNT
    (
        ACCOUNT_SK            BIGINT IDENTITY(1,1) NOT NULL PRIMARY KEY,
        ACCOUNT_ID            VARCHAR(100) NOT NULL,
        ACCOUNT_LVL1_CODE     VARCHAR(100) NULL,
        ACCOUNT_LVL1_DESC     VARCHAR(500) NULL,
        ACCOUNT_LVL2_CODE     VARCHAR(25)  NULL,
        ACCOUNT_LVL2_DESC     VARCHAR(500) NULL,
        ACCOUNT_LVL3_CODE     VARCHAR(25)  NULL,
        ACCOUNT_LVL3_DESC     VARCHAR(500) NULL,
        ACCOUNT_LVL4_CODE     VARCHAR(150) NULL,
        ACCOUNT_LVL4_DESC     VARCHAR(500) NULL,
        ACCOUNT_LVL5_CODE     VARCHAR(100) NULL,
        ACCOUNT_LVL5_DESC     VARCHAR(500) NULL,
        ACCOUNT_LVL6_CODE     VARCHAR(25)  NULL,
        ACCOUNT_LVL6_DESC     VARCHAR(500) NULL,
        ACCOUNT_LVL7_CODE     VARCHAR(25)  NULL,
        ACCOUNT_LVL7_DESC     VARCHAR(500) NULL,
        ACCOUNT_LVL8_CODE     VARCHAR(150) NULL,
        ACCOUNT_LVL8_DESC     VARCHAR(500) NULL,
        ACCOUNT_LVL9_CODE     VARCHAR(150) NULL,
        ACCOUNT_LVL9_DESC     VARCHAR(500) NULL,
        ACCOUNT_DISTANCE      SMALLINT     NULL,
        ACCOUNT_CATEGORY      VARCHAR(60)  NULL,
        ACCOUNT_ENABLED_FLAG  VARCHAR(4)   NULL,
        START_DATE_ACTIVE     DATE         NOT NULL,
        END_DATE_ACTIVE       DATE         NOT NULL,
        CREATED_BY            VARCHAR(64)  NULL,
        CREATION_DATE         DATE         NULL,
        BZ_LOAD_DATE          DATE         NULL,
        SV_LOAD_DATE          DATE         NULL
    ) ON FG_SilverDim;
END
GO

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'UX_D_ACCOUNT_ID' AND object_id = OBJECT_ID('svo.D_ACCOUNT'))
BEGIN
    CREATE UNIQUE NONCLUSTERED INDEX UX_D_ACCOUNT_ID
    ON svo.D_ACCOUNT(ACCOUNT_ID)
    ON FG_SilverDim;
END
GO

/* =========
   PROCEDURE
   ========= */
IF OBJECT_ID('svo.usp_Load_D_ACCOUNT','P') IS NOT NULL
    DROP PROCEDURE svo.usp_Load_D_ACCOUNT;
GO

CREATE PROCEDURE svo.usp_Load_D_ACCOUNT
(
      @FullReload BIT = 0
    , @Debug      BIT = 0
)
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    DECLARE
          @ProcName   SYSNAME = OBJECT_SCHEMA_NAME(@@PROCID) + '.' + OBJECT_NAME(@@PROCID)
        , @Target     SYSNAME = 'svo.D_ACCOUNT'
        , @LoadLogId  BIGINT
        , @RowsSource BIGINT = 0
        , @RowsIns    BIGINT = 0
        , @RowsUpd    BIGINT = 0
        , @RowsDel    BIGINT = 0;

    BEGIN TRY
        INSERT INTO svo.DW_LOAD_LOG (PROC_NAME, TARGET_OBJECT, FULL_RELOAD_FLAG, DEBUG_INFO)
        VALUES (@ProcName, @Target, @FullReload, CASE WHEN @Debug = 1 THEN N'Debug enabled' ELSE NULL END);

        SET @LoadLogId = SCOPE_IDENTITY();

        IF @Debug = 1
            PRINT CONCAT(@ProcName, ' starting. FullReload=', @FullReload);

        /* ---------------------------
           Source set (with hash)
           --------------------------- */
        IF OBJECT_ID('tempdb..#src') IS NOT NULL DROP TABLE #src;

        SELECT
              ACCOUNT_ID                = ISNULL(LTRIM(RTRIM(lvl9.VALUE)),'-1')
            , ACCOUNT_LVL1_CODE         = COALESCE(h1.DEP31PK1VALUE,h1.DEP30PK1VALUE,h1.DEP29PK1VALUE,h1.DEP28PK1VALUE,h1.DEP27PK1VALUE,h1.DEP26PK1VALUE,h1.DEP25PK1VALUE,h1.DEP24PK1VALUE,lvl9.VALUE)
            , ACCOUNT_LVL1_DESC         = COALESCE(lvl1.DESCRIPTION,lvl2.DESCRIPTION,lvl3.DESCRIPTION,lvl4.DESCRIPTION,lvl5.DESCRIPTION,lvl6.DESCRIPTION,lvl8.DESCRIPTION,lvl9.DESCRIPTION)
            , ACCOUNT_LVL2_CODE         = COALESCE(h1.DEP30PK1VALUE,h1.DEP29PK1VALUE,h1.DEP28PK1VALUE,h1.DEP27PK1VALUE,h1.DEP26PK1VALUE,h1.DEP25PK1VALUE,h1.DEP24PK1VALUE,lvl9.VALUE)
            , ACCOUNT_LVL2_DESC         = COALESCE(lvl2.DESCRIPTION,lvl3.DESCRIPTION,lvl4.DESCRIPTION,lvl5.DESCRIPTION,lvl6.DESCRIPTION,lvl8.DESCRIPTION,lvl9.DESCRIPTION)
            , ACCOUNT_LVL3_CODE         = COALESCE(h1.DEP29PK1VALUE,h1.DEP28PK1VALUE,h1.DEP27PK1VALUE,h1.DEP26PK1VALUE,h1.DEP25PK1VALUE,h1.DEP24PK1VALUE,lvl9.VALUE)
            , ACCOUNT_LVL3_DESC         = COALESCE(lvl3.DESCRIPTION,lvl4.DESCRIPTION,lvl5.DESCRIPTION,lvl6.DESCRIPTION,lvl8.DESCRIPTION,lvl9.DESCRIPTION)
            , ACCOUNT_LVL4_CODE         = COALESCE(h1.DEP28PK1VALUE,h1.DEP27PK1VALUE,h1.DEP26PK1VALUE,h1.DEP25PK1VALUE,h1.DEP24PK1VALUE,lvl9.VALUE)
            , ACCOUNT_LVL4_DESC         = COALESCE(lvl4.DESCRIPTION,lvl5.DESCRIPTION,lvl6.DESCRIPTION,lvl8.DESCRIPTION,lvl9.DESCRIPTION)
            , ACCOUNT_LVL5_CODE         = COALESCE(h1.DEP27PK1VALUE,h1.DEP26PK1VALUE,h1.DEP25PK1VALUE,h1.DEP24PK1VALUE,lvl9.VALUE)
            , ACCOUNT_LVL5_DESC         = COALESCE(lvl5.DESCRIPTION,lvl6.DESCRIPTION,lvl8.DESCRIPTION,lvl9.DESCRIPTION)
            , ACCOUNT_LVL6_CODE         = COALESCE(h1.DEP26PK1VALUE,h1.DEP25PK1VALUE,h1.DEP24PK1VALUE,lvl9.VALUE)
            , ACCOUNT_LVL6_DESC         = COALESCE(lvl6.DESCRIPTION,lvl8.DESCRIPTION,lvl9.DESCRIPTION)
            , ACCOUNT_LVL7_CODE         = COALESCE(h1.DEP25PK1VALUE,h1.DEP24PK1VALUE,lvl9.VALUE)
            , ACCOUNT_LVL7_DESC         = COALESCE(lvl7.DESCRIPTION,lvl8.DESCRIPTION,lvl9.DESCRIPTION)
            , ACCOUNT_LVL8_CODE         = COALESCE(h1.DEP24PK1VALUE,lvl9.VALUE)
            , ACCOUNT_LVL8_DESC         = COALESCE(lvl8.DESCRIPTION,lvl9.DESCRIPTION)
            , ACCOUNT_LVL9_CODE         = lvl9.VALUE
            , ACCOUNT_LVL9_DESC         = lvl9.DESCRIPTION
            , ACCOUNT_DISTANCE          = CAST(0 AS SMALLINT)
            , ACCOUNT_CATEGORY          = lvl9.ATTRIBUTECATEGORY
            , ACCOUNT_ENABLED_FLAG      = lvl9.ENABLEDFLAG
            , START_DATE_ACTIVE         = ISNULL(lvl9.STARTDATEACTIVE, CAST('0001-01-01' AS DATE))
            , END_DATE_ACTIVE           = ISNULL(lvl9.ENDDATEACTIVE,  CAST('9999-12-31' AS DATE))
            , CREATED_BY                = lvl9.CREATEDBY
            , CREATION_DATE             = CAST(lvl9.CREATIONDATE AS DATE)
            , BZ_LOAD_DATE              = COALESCE(CAST(h1.AddDateTime AS DATE), CAST('0001-01-01' AS DATE))
            , SV_LOAD_DATE              = CAST(GETDATE() AS DATE)

            , ROW_HASH = HASHBYTES(
                'SHA2_256',
                CONCAT(
                      ISNULL(LTRIM(RTRIM(lvl9.VALUE)),'-1'), '|'
                    , ISNULL(COALESCE(h1.DEP31PK1VALUE,h1.DEP30PK1VALUE,h1.DEP29PK1VALUE,h1.DEP28PK1VALUE,h1.DEP27PK1VALUE,h1.DEP26PK1VALUE,h1.DEP25PK1VALUE,h1.DEP24PK1VALUE,lvl9.VALUE),''), '|'
                    , ISNULL(COALESCE(lvl1.DESCRIPTION,lvl2.DESCRIPTION,lvl3.DESCRIPTION,lvl4.DESCRIPTION,lvl5.DESCRIPTION,lvl6.DESCRIPTION,lvl8.DESCRIPTION,lvl9.DESCRIPTION),''), '|'
                    , ISNULL(COALESCE(h1.DEP30PK1VALUE,h1.DEP29PK1VALUE,h1.DEP28PK1VALUE,h1.DEP27PK1VALUE,h1.DEP26PK1VALUE,h1.DEP25PK1VALUE,h1.DEP24PK1VALUE,lvl9.VALUE),''), '|'
                    , ISNULL(COALESCE(lvl2.DESCRIPTION,lvl3.DESCRIPTION,lvl4.DESCRIPTION,lvl5.DESCRIPTION,lvl6.DESCRIPTION,lvl8.DESCRIPTION,lvl9.DESCRIPTION),''), '|'
                    , ISNULL(COALESCE(h1.DEP29PK1VALUE,h1.DEP28PK1VALUE,h1.DEP27PK1VALUE,h1.DEP26PK1VALUE,h1.DEP25PK1VALUE,h1.DEP24PK1VALUE,lvl9.VALUE),''), '|'
                    , ISNULL(COALESCE(lvl3.DESCRIPTION,lvl4.DESCRIPTION,lvl5.DESCRIPTION,lvl6.DESCRIPTION,lvl8.DESCRIPTION,lvl9.DESCRIPTION),''), '|'
                    , ISNULL(COALESCE(h1.DEP28PK1VALUE,h1.DEP27PK1VALUE,h1.DEP26PK1VALUE,h1.DEP25PK1VALUE,h1.DEP24PK1VALUE,lvl9.VALUE),''), '|'
                    , ISNULL(COALESCE(lvl4.DESCRIPTION,lvl5.DESCRIPTION,lvl6.DESCRIPTION,lvl8.DESCRIPTION,lvl9.DESCRIPTION),''), '|'
                    , ISNULL(COALESCE(h1.DEP27PK1VALUE,h1.DEP26PK1VALUE,h1.DEP25PK1VALUE,h1.DEP24PK1VALUE,lvl9.VALUE),''), '|'
                    , ISNULL(COALESCE(lvl5.DESCRIPTION,lvl6.DESCRIPTION,lvl8.DESCRIPTION,lvl9.DESCRIPTION),''), '|'
                    , ISNULL(COALESCE(h1.DEP26PK1VALUE,h1.DEP25PK1VALUE,h1.DEP24PK1VALUE,lvl9.VALUE),''), '|'
                    , ISNULL(COALESCE(lvl6.DESCRIPTION,lvl8.DESCRIPTION,lvl9.DESCRIPTION),''), '|'
                    , ISNULL(COALESCE(h1.DEP25PK1VALUE,h1.DEP24PK1VALUE,lvl9.VALUE),''), '|'
                    , ISNULL(COALESCE(lvl7.DESCRIPTION,lvl8.DESCRIPTION,lvl9.DESCRIPTION),''), '|'
                    , ISNULL(COALESCE(h1.DEP24PK1VALUE,lvl9.VALUE),''), '|'
                    , ISNULL(COALESCE(lvl8.DESCRIPTION,lvl9.DESCRIPTION),''), '|'
                    , ISNULL(lvl9.VALUE,''), '|'
                    , ISNULL(lvl9.DESCRIPTION,''), '|'
                    , ISNULL(lvl9.ATTRIBUTECATEGORY,''), '|'
                    , ISNULL(lvl9.ENABLEDFLAG,''), '|'
                    , CONVERT(VARCHAR(10), ISNULL(lvl9.STARTDATEACTIVE, CAST('0001-01-01' AS DATE)), 120), '|'
                    , CONVERT(VARCHAR(10), ISNULL(lvl9.ENDDATEACTIVE,  CAST('9999-12-31' AS DATE)), 120), '|'
                    , ISNULL(lvl9.CREATEDBY,''), '|'
                    , CONVERT(VARCHAR(10), CAST(lvl9.CREATIONDATE AS DATE), 120)
                )
            )
        INTO #src
        FROM src.bzo_GL_SegmentValueHierarchyExtractPVO h1
        INNER JOIN src.bzo_GL_FndTreeAndVersionVO ver1
            ON ver1.TREEVERSIONID = h1.TREEVERSIONID
           AND ver1.TREENAME LIKE 'ACCOUNT LAMAR'
        INNER JOIN src.bzo_GL_ValueSetValuesPVO lvl1
            ON lvl1.VALUE = h1.DEP31PK1VALUE
           AND lvl1.ATTRIBUTECATEGORY = 'ACCOUNT LAMAR'
        INNER JOIN src.bzo_GL_ValueSetValuesPVO lvl2
            ON lvl2.VALUE = h1.DEP30PK1VALUE
           AND lvl2.ATTRIBUTECATEGORY = 'ACCOUNT LAMAR'
        INNER JOIN src.bzo_GL_ValueSetValuesPVO lvl3
            ON lvl3.VALUE = h1.DEP29PK1VALUE
           AND lvl3.ATTRIBUTECATEGORY = 'ACCOUNT LAMAR'
        INNER JOIN src.bzo_GL_ValueSetValuesPVO lvl4
            ON lvl4.VALUE = h1.DEP28PK1VALUE
           AND lvl4.ATTRIBUTECATEGORY = 'ACCOUNT LAMAR'
        INNER JOIN src.bzo_GL_ValueSetValuesPVO lvl5
            ON lvl5.VALUE = h1.DEP27PK1VALUE
           AND lvl5.ATTRIBUTECATEGORY = 'ACCOUNT LAMAR'
        INNER JOIN src.bzo_GL_ValueSetValuesPVO lvl6
            ON lvl6.VALUE = h1.DEP26PK1VALUE
           AND lvl6.ATTRIBUTECATEGORY = 'ACCOUNT LAMAR'
        INNER JOIN src.bzo_GL_ValueSetValuesPVO lvl7
            ON lvl7.VALUE = h1.DEP25PK1VALUE
           AND lvl7.ATTRIBUTECATEGORY = 'ACCOUNT LAMAR'
        INNER JOIN src.bzo_GL_ValueSetValuesPVO lvl8
            ON lvl8.VALUE = h1.DEP24PK1VALUE
           AND lvl8.ATTRIBUTECATEGORY = 'ACCOUNT LAMAR'
        RIGHT JOIN src.bzo_GL_ValueSetValuesPVO lvl9
            ON lvl9.VALUE = h1.DEP0PK1VALUE
           AND lvl9.ATTRIBUTECATEGORY = 'ACCOUNT LAMAR'
        WHERE lvl9.ATTRIBUTECATEGORY = 'ACCOUNT LAMAR'
          AND lvl9.SUMMARYFLAG = 'N';

        /* De-dupe safety on natural key */
        ;WITH d AS
        (
            SELECT ACCOUNT_ID, rn = ROW_NUMBER() OVER (PARTITION BY ACCOUNT_ID ORDER BY ACCOUNT_ID)
            FROM #src
        )
        DELETE s
        FROM #src s
        INNER JOIN d
            ON d.ACCOUNT_ID = s.ACCOUNT_ID
           AND d.rn > 1;

        SELECT @RowsSource = COUNT(*) FROM #src;

        IF @Debug = 1
            PRINT CONCAT('Source rows: ', @RowsSource);

        BEGIN TRAN;

        /* Ensure SK=0 plug row exists */
        IF NOT EXISTS (SELECT 1 FROM svo.D_ACCOUNT WHERE ACCOUNT_SK = 0)
        BEGIN
            SET IDENTITY_INSERT svo.D_ACCOUNT ON;

            INSERT INTO svo.D_ACCOUNT
            (
                  ACCOUNT_SK
                , ACCOUNT_ID
                , ACCOUNT_LVL1_CODE, ACCOUNT_LVL1_DESC
                , ACCOUNT_LVL2_CODE, ACCOUNT_LVL2_DESC
                , ACCOUNT_LVL3_CODE, ACCOUNT_LVL3_DESC
                , ACCOUNT_LVL4_CODE, ACCOUNT_LVL4_DESC
                , ACCOUNT_LVL5_CODE, ACCOUNT_LVL5_DESC
                , ACCOUNT_LVL6_CODE, ACCOUNT_LVL6_DESC
                , ACCOUNT_LVL7_CODE, ACCOUNT_LVL7_DESC
                , ACCOUNT_LVL8_CODE, ACCOUNT_LVL8_DESC
                , ACCOUNT_LVL9_CODE, ACCOUNT_LVL9_DESC
                , ACCOUNT_DISTANCE
                , ACCOUNT_CATEGORY
                , ACCOUNT_ENABLED_FLAG
                , START_DATE_ACTIVE
                , END_DATE_ACTIVE
                , CREATED_BY
                , CREATION_DATE
                , BZ_LOAD_DATE
                , SV_LOAD_DATE
            )
            VALUES
            (
                  0
                , '-1'
                , '-1', 'Unknown'
                , '-1', 'Unknown'
                , '-1', 'Unknown'
                , '-1', 'Unknown'
                , '-1', 'Unknown'
                , '-1', 'Unknown'
                , '-1', 'Unknown'
                , '-1', 'Unknown'
                , '-1', 'Unknown'
                , 0
                , 'Missing'
                , NULL            -- fixed: was literal 'NULL'
                , CAST('0001-01-01' AS DATE)
                , CAST('9999-12-31' AS DATE)
                , 'GJR'
                , CAST('2025-10-18' AS DATE)
                , CAST('0001-01-01' AS DATE)
                , CAST(GETDATE() AS DATE)
            );

            SET IDENTITY_INSERT svo.D_ACCOUNT OFF;
        END

        IF @FullReload = 1
        BEGIN
            DELETE FROM svo.D_ACCOUNT
            WHERE ACCOUNT_SK <> 0;

            INSERT INTO svo.D_ACCOUNT
            (
                  ACCOUNT_ID
                , ACCOUNT_LVL1_CODE, ACCOUNT_LVL1_DESC
                , ACCOUNT_LVL2_CODE, ACCOUNT_LVL2_DESC
                , ACCOUNT_LVL3_CODE, ACCOUNT_LVL3_DESC
                , ACCOUNT_LVL4_CODE, ACCOUNT_LVL4_DESC
                , ACCOUNT_LVL5_CODE, ACCOUNT_LVL5_DESC
                , ACCOUNT_LVL6_CODE, ACCOUNT_LVL6_DESC
                , ACCOUNT_LVL7_CODE, ACCOUNT_LVL7_DESC
                , ACCOUNT_LVL8_CODE, ACCOUNT_LVL8_DESC
                , ACCOUNT_LVL9_CODE, ACCOUNT_LVL9_DESC
                , ACCOUNT_DISTANCE
                , ACCOUNT_CATEGORY
                , ACCOUNT_ENABLED_FLAG
                , START_DATE_ACTIVE
                , END_DATE_ACTIVE
                , CREATED_BY
                , CREATION_DATE
                , BZ_LOAD_DATE
                , SV_LOAD_DATE
            )
            SELECT
                  s.ACCOUNT_ID
                , s.ACCOUNT_LVL1_CODE, s.ACCOUNT_LVL1_DESC
                , s.ACCOUNT_LVL2_CODE, s.ACCOUNT_LVL2_DESC
                , s.ACCOUNT_LVL3_CODE, s.ACCOUNT_LVL3_DESC
                , s.ACCOUNT_LVL4_CODE, s.ACCOUNT_LVL4_DESC
                , s.ACCOUNT_LVL5_CODE, s.ACCOUNT_LVL5_DESC
                , s.ACCOUNT_LVL6_CODE, s.ACCOUNT_LVL6_DESC
                , s.ACCOUNT_LVL7_CODE, s.ACCOUNT_LVL7_DESC
                , s.ACCOUNT_LVL8_CODE, s.ACCOUNT_LVL8_DESC
                , s.ACCOUNT_LVL9_CODE, s.ACCOUNT_LVL9_DESC
                , s.ACCOUNT_DISTANCE
                , s.ACCOUNT_CATEGORY
                , s.ACCOUNT_ENABLED_FLAG
                , s.START_DATE_ACTIVE
                , s.END_DATE_ACTIVE
                , s.CREATED_BY
                , s.CREATION_DATE
                , s.BZ_LOAD_DATE
                , CAST(GETDATE() AS DATE)
            FROM #src s;

            SET @RowsIns = @@ROWCOUNT;
        END
        ELSE
        BEGIN
            /* Hash-based incremental update/insert */
            IF OBJECT_ID('tempdb..#tgt') IS NOT NULL DROP TABLE #tgt;

            SELECT
                  ACCOUNT_ID
                , ROW_HASH = HASHBYTES(
                    'SHA2_256',
                    CONCAT(
                          ISNULL(ACCOUNT_ID,'-1'), '|'
                        , ISNULL(ACCOUNT_LVL1_CODE,''), '|', ISNULL(ACCOUNT_LVL1_DESC,''), '|'
                        , ISNULL(ACCOUNT_LVL2_CODE,''), '|', ISNULL(ACCOUNT_LVL2_DESC,''), '|'
                        , ISNULL(ACCOUNT_LVL3_CODE,''), '|', ISNULL(ACCOUNT_LVL3_DESC,''), '|'
                        , ISNULL(ACCOUNT_LVL4_CODE,''), '|', ISNULL(ACCOUNT_LVL4_DESC,''), '|'
                        , ISNULL(ACCOUNT_LVL5_CODE,''), '|', ISNULL(ACCOUNT_LVL5_DESC,''), '|'
                        , ISNULL(ACCOUNT_LVL6_CODE,''), '|', ISNULL(ACCOUNT_LVL6_DESC,''), '|'
                        , ISNULL(ACCOUNT_LVL7_CODE,''), '|', ISNULL(ACCOUNT_LVL7_DESC,''), '|'
                        , ISNULL(ACCOUNT_LVL8_CODE,''), '|', ISNULL(ACCOUNT_LVL8_DESC,''), '|'
                        , ISNULL(ACCOUNT_LVL9_CODE,''), '|', ISNULL(ACCOUNT_LVL9_DESC,''), '|'
                        , ISNULL(ACCOUNT_CATEGORY,''), '|'
                        , ISNULL(ACCOUNT_ENABLED_FLAG,''), '|'
                        , CONVERT(VARCHAR(10), ISNULL(START_DATE_ACTIVE, CAST('0001-01-01' AS DATE)), 120), '|'
                        , CONVERT(VARCHAR(10), ISNULL(END_DATE_ACTIVE,   CAST('9999-12-31' AS DATE)), 120), '|'
                        , ISNULL(CREATED_BY,''), '|'
                        , CONVERT(VARCHAR(10), ISNULL(CREATION_DATE, CAST('0001-01-01' AS DATE)), 120)
                    )
                  )
            INTO #tgt
            FROM svo.D_ACCOUNT
            WHERE ACCOUNT_SK <> 0;

            UPDATE t
                SET
                      t.ACCOUNT_LVL1_CODE        = s.ACCOUNT_LVL1_CODE
                    , t.ACCOUNT_LVL1_DESC        = s.ACCOUNT_LVL1_DESC
                    , t.ACCOUNT_LVL2_CODE        = s.ACCOUNT_LVL2_CODE
                    , t.ACCOUNT_LVL2_DESC        = s.ACCOUNT_LVL2_DESC
                    , t.ACCOUNT_LVL3_CODE        = s.ACCOUNT_LVL3_CODE
                    , t.ACCOUNT_LVL3_DESC        = s.ACCOUNT_LVL3_DESC
                    , t.ACCOUNT_LVL4_CODE        = s.ACCOUNT_LVL4_CODE
                    , t.ACCOUNT_LVL4_DESC        = s.ACCOUNT_LVL4_DESC
                    , t.ACCOUNT_LVL5_CODE        = s.ACCOUNT_LVL5_CODE
                    , t.ACCOUNT_LVL5_DESC        = s.ACCOUNT_LVL5_DESC
                    , t.ACCOUNT_LVL6_CODE        = s.ACCOUNT_LVL6_CODE
                    , t.ACCOUNT_LVL6_DESC        = s.ACCOUNT_LVL6_DESC
                    , t.ACCOUNT_LVL7_CODE        = s.ACCOUNT_LVL7_CODE
                    , t.ACCOUNT_LVL7_DESC        = s.ACCOUNT_LVL7_DESC
                    , t.ACCOUNT_LVL8_CODE        = s.ACCOUNT_LVL8_CODE
                    , t.ACCOUNT_LVL8_DESC        = s.ACCOUNT_LVL8_DESC
                    , t.ACCOUNT_LVL9_CODE        = s.ACCOUNT_LVL9_CODE
                    , t.ACCOUNT_LVL9_DESC        = s.ACCOUNT_LVL9_DESC
                    , t.ACCOUNT_DISTANCE         = s.ACCOUNT_DISTANCE
                    , t.ACCOUNT_CATEGORY         = s.ACCOUNT_CATEGORY
                    , t.ACCOUNT_ENABLED_FLAG     = s.ACCOUNT_ENABLED_FLAG
                    , t.START_DATE_ACTIVE        = s.START_DATE_ACTIVE
                    , t.END_DATE_ACTIVE          = s.END_DATE_ACTIVE
                    , t.CREATED_BY               = s.CREATED_BY
                    , t.CREATION_DATE            = s.CREATION_DATE
                    , t.BZ_LOAD_DATE             = s.BZ_LOAD_DATE
                    , t.SV_LOAD_DATE             = CAST(GETDATE() AS DATE)
            FROM svo.D_ACCOUNT t
            INNER JOIN #src s
                ON s.ACCOUNT_ID = t.ACCOUNT_ID
            INNER JOIN #tgt h
                ON h.ACCOUNT_ID = t.ACCOUNT_ID
            WHERE h.ROW_HASH <> s.ROW_HASH;

            SET @RowsUpd = @@ROWCOUNT;

            INSERT INTO svo.D_ACCOUNT
            (
                  ACCOUNT_ID
                , ACCOUNT_LVL1_CODE, ACCOUNT_LVL1_DESC
                , ACCOUNT_LVL2_CODE, ACCOUNT_LVL2_DESC
                , ACCOUNT_LVL3_CODE, ACCOUNT_LVL3_DESC
                , ACCOUNT_LVL4_CODE, ACCOUNT_LVL4_DESC
                , ACCOUNT_LVL5_CODE, ACCOUNT_LVL5_DESC
                , ACCOUNT_LVL6_CODE, ACCOUNT_LVL6_DESC
                , ACCOUNT_LVL7_CODE, ACCOUNT_LVL7_DESC
                , ACCOUNT_LVL8_CODE, ACCOUNT_LVL8_DESC
                , ACCOUNT_LVL9_CODE, ACCOUNT_LVL9_DESC
                , ACCOUNT_DISTANCE
                , ACCOUNT_CATEGORY
                , ACCOUNT_ENABLED_FLAG
                , START_DATE_ACTIVE
                , END_DATE_ACTIVE
                , CREATED_BY
                , CREATION_DATE
                , BZ_LOAD_DATE
                , SV_LOAD_DATE
            )
            SELECT
                  s.ACCOUNT_ID
                , s.ACCOUNT_LVL1_CODE, s.ACCOUNT_LVL1_DESC
                , s.ACCOUNT_LVL2_CODE, s.ACCOUNT_LVL2_DESC
                , s.ACCOUNT_LVL3_CODE, s.ACCOUNT_LVL3_DESC
                , s.ACCOUNT_LVL4_CODE, s.ACCOUNT_LVL4_DESC
                , s.ACCOUNT_LVL5_CODE, s.ACCOUNT_LVL5_DESC
                , s.ACCOUNT_LVL6_CODE, s.ACCOUNT_LVL6_DESC
                , s.ACCOUNT_LVL7_CODE, s.ACCOUNT_LVL7_DESC
                , s.ACCOUNT_LVL8_CODE, s.ACCOUNT_LVL8_DESC
                , s.ACCOUNT_LVL9_CODE, s.ACCOUNT_LVL9_DESC
                , s.ACCOUNT_DISTANCE
                , s.ACCOUNT_CATEGORY
                , s.ACCOUNT_ENABLED_FLAG
                , s.START_DATE_ACTIVE
                , s.END_DATE_ACTIVE
                , s.CREATED_BY
                , s.CREATION_DATE
                , s.BZ_LOAD_DATE
                , CAST(GETDATE() AS DATE)
            FROM #src s
            LEFT JOIN svo.D_ACCOUNT t
                ON t.ACCOUNT_ID = s.ACCOUNT_ID
            WHERE t.ACCOUNT_ID IS NULL;

            SET @RowsIns = @@ROWCOUNT;
        END

        COMMIT;

        UPDATE svo.DW_LOAD_LOG
            SET
                  LOAD_END_DT   = SYSUTCDATETIME()
                , STATUS        = 'SUCCESS'
                , ROWS_SOURCE   = @RowsSource
                , ROWS_INSERTED = @RowsIns
                , ROWS_UPDATED  = @RowsUpd
                , ROWS_DELETED  = @RowsDel
        WHERE LOAD_LOG_ID = @LoadLogId;

        IF @Debug = 1
            PRINT CONCAT('Done. Inserted=', @RowsIns, ' Updated=', @RowsUpd);

    END TRY
    BEGIN CATCH
        IF XACT_STATE() <> 0 ROLLBACK;

        DECLARE
              @ErrMsg NVARCHAR(4000) = ERROR_MESSAGE()
            , @ErrNum INT            = ERROR_NUMBER()
            , @ErrSev INT            = ERROR_SEVERITY()
            , @ErrSta INT            = ERROR_STATE()
            , @ErrLin INT            = ERROR_LINE();

        UPDATE svo.DW_LOAD_LOG
            SET
                  LOAD_END_DT     = SYSUTCDATETIME()
                , STATUS          = 'FAILED'
                , ERROR_NUMBER    = @ErrNum
                , ERROR_SEVERITY  = @ErrSev
                , ERROR_STATE     = @ErrSta
                , ERROR_LINE      = @ErrLin
                , ERROR_MESSAGE   = LEFT(@ErrMsg, 4000)
        WHERE LOAD_LOG_ID = @LoadLogId;

        RAISERROR('%s', @ErrSev, @ErrSta, @ErrMsg);
        RETURN;
    END CATCH
END
GO

/* Run:
EXEC svo.usp_Load_D_ACCOUNT @FullReload = 1, @Debug = 1;  -- initial rebuild
EXEC svo.usp_Load_D_ACCOUNT @FullReload = 0, @Debug = 0;  -- daily incremental
*/


