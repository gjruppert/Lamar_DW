/* =========================================================
   usp_Load_D_COST_CENTER
   SCD2 incremental load. CodeCombo dim from GL value set hierarchy.
   Watermark filter, expire changed rows, insert new current rows.
   Idempotent: safe to retry same watermark range.
   ========================================================= */
CREATE OR ALTER PROCEDURE svo.usp_Load_D_COST_CENTER
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    DECLARE
        @ProcName       SYSNAME        = OBJECT_SCHEMA_NAME(@@PROCID) + '.' + OBJECT_NAME(@@PROCID),
        @TargetObject   SYSNAME        = 'svo.D_COST_CENTER',
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

        IF EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'UX_D_COST_CENTER_ID' AND object_id = OBJECT_ID('svo.D_COST_CENTER'))
        BEGIN
            DROP INDEX UX_D_COST_CENTER_ID ON svo.D_COST_CENTER;
        END;

        IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'UX_D_COST_CENTER_BK_CURR' AND object_id = OBJECT_ID('svo.D_COST_CENTER'))
        BEGIN
            CREATE UNIQUE NONCLUSTERED INDEX UX_D_COST_CENTER_BK_CURR
            ON svo.D_COST_CENTER (COST_CENTER_ID)
            WHERE CURR_IND = 'Y'
            ON FG_SilverDim;
        END;

        IF NOT EXISTS (SELECT 1 FROM svo.D_COST_CENTER WHERE COST_CENTER_SK = 0)
        BEGIN
            SET IDENTITY_INSERT svo.D_COST_CENTER ON;

            INSERT INTO svo.D_COST_CENTER
            (COST_CENTER_SK, COST_CENTER_ID, COST_CENTER_LVL1_CODE, COST_CENTER_LVL1_DESC, COST_CENTER_LVL2_CODE, COST_CENTER_LVL2_DESC,
             COST_CENTER_LVL3_CODE, COST_CENTER_LVL3_DESC, COST_CENTER_LVL4_CODE, COST_CENTER_LVL4_DESC,
             COST_CENTER_DISTANCE, COST_CENTER_CATEGORY, COST_CENTER_ENABLED_FLAG, START_DATE_ACTIVE, END_DATE_ACTIVE,
             CREATED_BY, CREATION_DATE, BZ_LOAD_DATE, SV_LOAD_DATE, EFF_DATE, END_DATE, CRE_DATE, UDT_DATE, CURR_IND)
            VALUES
            (0,'-1','-1','Unknown','-1','Unknown','-1','Unknown','-1','Unknown',
             0,'Missing',NULL,'0001-01-01','9999-12-31','SYSTEM','2025-10-18',CAST(GETDATE() AS DATE),CAST(GETDATE() AS DATE),
             @AsOfDate,@HighDate,@LoadDttm,@LoadDttm,'Y');

            SET IDENTITY_INSERT svo.D_COST_CENTER OFF;
        END;

        IF OBJECT_ID('tempdb..#src') IS NOT NULL DROP TABLE #src;

        SELECT
            s.COST_CENTER_ID,
            s.COST_CENTER_LVL1_CODE, s.COST_CENTER_LVL1_DESC, s.COST_CENTER_LVL2_CODE, s.COST_CENTER_LVL2_DESC,
            s.COST_CENTER_LVL3_CODE, s.COST_CENTER_LVL3_DESC, s.COST_CENTER_LVL4_CODE, s.COST_CENTER_LVL4_DESC,
            s.COST_CENTER_DISTANCE, s.COST_CENTER_CATEGORY, s.COST_CENTER_ENABLED_FLAG,
            s.START_DATE_ACTIVE, s.END_DATE_ACTIVE, s.CREATED_BY, s.CREATION_DATE, s.BZ_LOAD_DATE, s.SV_LOAD_DATE, s.SourceAddDateTime
        INTO #src
        FROM
        (
            SELECT
                ISNULL(LTRIM(RTRIM(lvl4.VALUE)),'-1') AS COST_CENTER_ID,
                COALESCE(h1.DEP31PK1VALUE,h1.DEP30PK1VALUE,h1.DEP29PK1VALUE,lvl4.VALUE) AS COST_CENTER_LVL1_CODE,
                COALESCE(lvl1.DESCRIPTION,lvl2.DESCRIPTION,lvl3.DESCRIPTION,lvl4.DESCRIPTION) AS COST_CENTER_LVL1_DESC,
                COALESCE(h1.DEP30PK1VALUE,h1.DEP29PK1VALUE,lvl4.VALUE) AS COST_CENTER_LVL2_CODE,
                COALESCE(lvl2.DESCRIPTION,lvl3.DESCRIPTION,lvl4.DESCRIPTION) AS COST_CENTER_LVL2_DESC,
                COALESCE(h1.DEP29PK1VALUE,lvl4.VALUE) AS COST_CENTER_LVL3_CODE,
                COALESCE(lvl3.DESCRIPTION,lvl4.DESCRIPTION) AS COST_CENTER_LVL3_DESC,
                lvl4.VALUE AS COST_CENTER_LVL4_CODE, lvl4.DESCRIPTION AS COST_CENTER_LVL4_DESC,
                0 AS COST_CENTER_DISTANCE, lvl4.ATTRIBUTECATEGORY AS COST_CENTER_CATEGORY, lvl4.ENABLEDFLAG AS COST_CENTER_ENABLED_FLAG,
                ISNULL(lvl4.STARTDATEACTIVE, '0001-01-01') AS START_DATE_ACTIVE, ISNULL(lvl4.ENDDATEACTIVE,  '9999-12-31') AS END_DATE_ACTIVE,
                lvl4.CREATEDBY AS CREATED_BY, CAST(lvl4.CREATIONDATE AS DATE) AS CREATION_DATE,
                COALESCE(CAST(h1.AddDateTime AS DATE), CAST(GETDATE() AS DATE)) AS BZ_LOAD_DATE, CAST(GETDATE() AS DATE) AS SV_LOAD_DATE,
                (SELECT MAX(v.dt) FROM (VALUES (h1.AddDateTime),(ver1.AddDateTime),(lvl1.AddDateTime),(lvl2.AddDateTime),(lvl3.AddDateTime),(lvl4.AddDateTime)) v(dt)) AS SourceAddDateTime,
                ROW_NUMBER() OVER (PARTITION BY ISNULL(LTRIM(RTRIM(lvl4.VALUE)),'-1') ORDER BY (SELECT MAX(v.dt) FROM (VALUES (h1.AddDateTime),(ver1.AddDateTime),(lvl1.AddDateTime),(lvl2.AddDateTime),(lvl3.AddDateTime),(lvl4.AddDateTime)) v(dt)) DESC) AS rn
            FROM bzo.GL_SegmentValueHierarchyExtractPVO h1
            INNER JOIN bzo.GL_FndTreeAndVersionVO ver1 ON ver1.TREEVERSIONID = h1.TREEVERSIONID AND ver1.TREENAME LIKE 'CENTER LAMAR'
            INNER JOIN bzo.GL_ValueSetValuesPVO lvl1 ON lvl1.VALUE = h1.DEP31PK1VALUE AND lvl1.ATTRIBUTECATEGORY = 'CENTER LAMAR'
            INNER JOIN bzo.GL_ValueSetValuesPVO lvl2 ON lvl2.VALUE = h1.DEP30PK1VALUE AND lvl2.ATTRIBUTECATEGORY = 'CENTER LAMAR'
            INNER JOIN bzo.GL_ValueSetValuesPVO lvl3 ON lvl3.VALUE = h1.DEP29PK1VALUE AND lvl3.ATTRIBUTECATEGORY = 'CENTER LAMAR'
            RIGHT JOIN bzo.GL_ValueSetValuesPVO lvl4 ON lvl4.VALUE = h1.DEP0PK1VALUE AND lvl4.ATTRIBUTECATEGORY = 'CENTER LAMAR'
            WHERE lvl4.ATTRIBUTECATEGORY = 'CENTER LAMAR' AND lvl4.SUMMARYFLAG = 'N'
              AND (h1.AddDateTime > @LastWatermark OR ver1.AddDateTime > @LastWatermark OR lvl1.AddDateTime > @LastWatermark OR lvl2.AddDateTime > @LastWatermark OR lvl3.AddDateTime > @LastWatermark OR lvl4.AddDateTime > @LastWatermark)
        ) s
        WHERE s.rn = 1;

        SELECT @MaxWatermark = MAX(SourceAddDateTime) FROM #src;

        UPDATE tgt
        SET tgt.END_DATE = DATEADD(DAY, -1, @AsOfDate), tgt.CURR_IND = 'N', tgt.UDT_DATE = @LoadDttm
        FROM svo.D_COST_CENTER tgt
        INNER JOIN #src src ON src.COST_CENTER_ID = tgt.COST_CENTER_ID
        WHERE tgt.CURR_IND = 'Y'
          AND (
                ISNULL(tgt.COST_CENTER_LVL1_CODE,'') <> ISNULL(src.COST_CENTER_LVL1_CODE,'')
             OR ISNULL(tgt.COST_CENTER_LVL1_DESC,'') <> ISNULL(src.COST_CENTER_LVL1_DESC,'')
             OR ISNULL(tgt.COST_CENTER_LVL2_CODE,'') <> ISNULL(src.COST_CENTER_LVL2_CODE,'')
             OR ISNULL(tgt.COST_CENTER_LVL2_DESC,'') <> ISNULL(src.COST_CENTER_LVL2_DESC,'')
             OR ISNULL(tgt.COST_CENTER_LVL3_CODE,'') <> ISNULL(src.COST_CENTER_LVL3_CODE,'')
             OR ISNULL(tgt.COST_CENTER_LVL3_DESC,'') <> ISNULL(src.COST_CENTER_LVL3_DESC,'')
             OR ISNULL(tgt.COST_CENTER_LVL4_CODE,'') <> ISNULL(src.COST_CENTER_LVL4_CODE,'')
             OR ISNULL(tgt.COST_CENTER_LVL4_DESC,'') <> ISNULL(src.COST_CENTER_LVL4_DESC,'')
             OR ISNULL(tgt.COST_CENTER_DISTANCE, -999) <> ISNULL(src.COST_CENTER_DISTANCE, -999)
             OR ISNULL(tgt.COST_CENTER_CATEGORY,'') <> ISNULL(src.COST_CENTER_CATEGORY,'')
             OR ISNULL(tgt.COST_CENTER_ENABLED_FLAG,'') <> ISNULL(src.COST_CENTER_ENABLED_FLAG,'')
             OR tgt.START_DATE_ACTIVE <> src.START_DATE_ACTIVE
             OR tgt.END_DATE_ACTIVE <> src.END_DATE_ACTIVE
             OR ISNULL(tgt.CREATED_BY,'') <> ISNULL(src.CREATED_BY,'')
             OR ISNULL(tgt.CREATION_DATE,'1900-01-01') <> ISNULL(src.CREATION_DATE,'1900-01-01')
          );

        SET @RowExpired = @@ROWCOUNT;

        INSERT INTO svo.D_COST_CENTER
        (COST_CENTER_ID, COST_CENTER_LVL1_CODE, COST_CENTER_LVL1_DESC, COST_CENTER_LVL2_CODE, COST_CENTER_LVL2_DESC,
         COST_CENTER_LVL3_CODE, COST_CENTER_LVL3_DESC, COST_CENTER_LVL4_CODE, COST_CENTER_LVL4_DESC,
         COST_CENTER_DISTANCE, COST_CENTER_CATEGORY, COST_CENTER_ENABLED_FLAG, START_DATE_ACTIVE, END_DATE_ACTIVE,
         CREATED_BY, CREATION_DATE, BZ_LOAD_DATE, SV_LOAD_DATE, EFF_DATE, END_DATE, CRE_DATE, UDT_DATE, CURR_IND)
        SELECT
            src.COST_CENTER_ID, src.COST_CENTER_LVL1_CODE, src.COST_CENTER_LVL1_DESC, src.COST_CENTER_LVL2_CODE, src.COST_CENTER_LVL2_DESC,
            src.COST_CENTER_LVL3_CODE, src.COST_CENTER_LVL3_DESC, src.COST_CENTER_LVL4_CODE, src.COST_CENTER_LVL4_DESC,
            src.COST_CENTER_DISTANCE, src.COST_CENTER_CATEGORY, src.COST_CENTER_ENABLED_FLAG, src.START_DATE_ACTIVE, src.END_DATE_ACTIVE,
            src.CREATED_BY, src.CREATION_DATE, src.BZ_LOAD_DATE, src.SV_LOAD_DATE,
            @AsOfDate, @HighDate, @LoadDttm, @LoadDttm, 'Y'
        FROM #src src
        LEFT JOIN svo.D_COST_CENTER tgt ON tgt.COST_CENTER_ID = src.COST_CENTER_ID AND tgt.CURR_IND = 'Y'
        WHERE tgt.COST_CENTER_ID IS NULL;

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
