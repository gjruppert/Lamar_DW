CREATE OR ALTER PROCEDURE svo.usp_Load_D_ORGANIZATION_T1
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    DECLARE
        @ProcName      SYSNAME        = OBJECT_SCHEMA_NAME(@@PROCID) + '.' + OBJECT_NAME(@@PROCID),
        @TargetObject  SYSNAME        = 'svo.D_ORGANIZATION',
        @StartDttm     DATETIME2(0)   = SYSDATETIME(),
        @EndDttm       DATETIME2(0),
        @RunId         BIGINT         = NULL,
        @ErrMsg        NVARCHAR(4000) = NULL,

        @AsOfDate      DATE           = CAST(GETDATE() AS DATE),

        @LastWatermark DATETIME2(7),
        @MaxWatermark  DATETIME2(7)   = NULL,

        @RowInserted   INT            = 0,
        @RowExpired    INT            = 0,  -- not used for T1
        @RowUpdated    INT            = 0;

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
            WHERE name = 'UX_D_ORGANIZATION_ORGANIZATION_ID'
              AND object_id = OBJECT_ID(@TargetObject)
        )
        BEGIN
            EXEC('CREATE UNIQUE NONCLUSTERED INDEX UX_D_ORGANIZATION_ORGANIZATION_ID
                  ON svo.D_ORGANIZATION(ORGANIZATION_ID)
                  ON FG_SilverDim;');
        END;

        /* ===== Plug row (SK=0, BK=-1) ===== */
        IF NOT EXISTS (SELECT 1 FROM svo.D_ORGANIZATION WHERE INVENTORY_ORG_SK = 0)
        BEGIN
            SET IDENTITY_INSERT svo.D_ORGANIZATION ON;

            INSERT INTO svo.D_ORGANIZATION
            (
                INVENTORY_ORG_SK,
                ORGANIZATION_ID,
                ORGANIZATION_CODE,
                INVENTORY_FLAG,
                BUSINESS_UNIT_ID,
                LEGAL_ENTITY_ID,
                MASTER_ORGANIZATION_ID,
                SOURCE_ORGANIZATION_ID,
                BZ_LOAD_DATE,
                SV_LOAD_DATE
            )
            VALUES
            (
                0,
                -1,
                'UNKNOWN',
                'N',
                NULL,
                NULL,
                NULL,
                NULL,
                CAST('0001-01-01' AS DATE),
                @AsOfDate
            );

            SET IDENTITY_INSERT svo.D_ORGANIZATION OFF;
        END;

        /* ===== Source (incremental + dedup by ORGANIZATION_ID) ===== */
        IF OBJECT_ID('tempdb..#src') IS NOT NULL DROP TABLE #src;

        SELECT
            s.ORGANIZATION_ID,
            s.ORGANIZATION_CODE,
            s.INVENTORY_FLAG,
            s.BUSINESS_UNIT_ID,
            s.LEGAL_ENTITY_ID,
            s.MASTER_ORGANIZATION_ID,
            s.SOURCE_ORGANIZATION_ID,
            s.BZ_LOAD_DATE,
            s.SV_LOAD_DATE,
            s.SourceAddDateTime
        INTO #src
        FROM
        (
            SELECT
                CAST(p.OrganizationId AS BIGINT)            AS ORGANIZATION_ID,
                p.OrganizationCode                          AS ORGANIZATION_CODE,
                p.InventoryFlag                             AS INVENTORY_FLAG,
                CAST(p.BusinessUnitId AS BIGINT)            AS BUSINESS_UNIT_ID,
                CAST(p.LegalEntityId AS BIGINT)             AS LEGAL_ENTITY_ID,
                CAST(p.MasterOrganizationId AS BIGINT)      AS MASTER_ORGANIZATION_ID,
                CAST(p.SourceOrganizationId AS BIGINT)      AS SOURCE_ORGANIZATION_ID,
                COALESCE(CAST(p.AddDateTime AS DATE), @AsOfDate) AS BZ_LOAD_DATE,
                @AsOfDate                                        AS SV_LOAD_DATE,
                p.AddDateTime                                     AS SourceAddDateTime,
                ROW_NUMBER() OVER
                (
                    PARTITION BY CAST(p.OrganizationId AS BIGINT)
                    ORDER BY p.AddDateTime DESC
                ) AS rn
            FROM bzo.PIM_InvOrgParametersExtractPVO p
            WHERE p.OrganizationId IS NOT NULL
              AND p.AddDateTime > @LastWatermark
        ) s
        WHERE s.rn = 1;

        DELETE FROM #src WHERE ORGANIZATION_ID = -1;

        SELECT @MaxWatermark = MAX(SourceAddDateTime) FROM #src;

        /* ===== T1 Update existing rows (only when changed) ===== */
        UPDATE tgt
        SET
            tgt.ORGANIZATION_CODE      = src.ORGANIZATION_CODE,
            tgt.INVENTORY_FLAG         = src.INVENTORY_FLAG,
            tgt.BUSINESS_UNIT_ID       = src.BUSINESS_UNIT_ID,
            tgt.LEGAL_ENTITY_ID        = src.LEGAL_ENTITY_ID,
            tgt.MASTER_ORGANIZATION_ID = src.MASTER_ORGANIZATION_ID,
            tgt.SOURCE_ORGANIZATION_ID = src.SOURCE_ORGANIZATION_ID,
            tgt.BZ_LOAD_DATE           = src.BZ_LOAD_DATE,
            tgt.SV_LOAD_DATE           = src.SV_LOAD_DATE
        FROM svo.D_ORGANIZATION tgt
        INNER JOIN #src src
            ON src.ORGANIZATION_ID = tgt.ORGANIZATION_ID
        WHERE
            (
                ISNULL(tgt.ORGANIZATION_CODE,'') <> ISNULL(src.ORGANIZATION_CODE,'')
             OR ISNULL(tgt.INVENTORY_FLAG,'') <> ISNULL(src.INVENTORY_FLAG,'')
             OR ISNULL(tgt.BUSINESS_UNIT_ID,-1) <> ISNULL(src.BUSINESS_UNIT_ID,-1)
             OR ISNULL(tgt.LEGAL_ENTITY_ID,-1) <> ISNULL(src.LEGAL_ENTITY_ID,-1)
             OR ISNULL(tgt.MASTER_ORGANIZATION_ID,-1) <> ISNULL(src.MASTER_ORGANIZATION_ID,-1)
             OR ISNULL(tgt.SOURCE_ORGANIZATION_ID,-1) <> ISNULL(src.SOURCE_ORGANIZATION_ID,-1)
            );

        SET @RowUpdated = @@ROWCOUNT;

        /* ===== T1 Insert new rows ===== */
        INSERT INTO svo.D_ORGANIZATION
        (
            ORGANIZATION_ID,
            ORGANIZATION_CODE,
            INVENTORY_FLAG,
            BUSINESS_UNIT_ID,
            LEGAL_ENTITY_ID,
            MASTER_ORGANIZATION_ID,
            SOURCE_ORGANIZATION_ID,
            BZ_LOAD_DATE,
            SV_LOAD_DATE
        )
        SELECT
            src.ORGANIZATION_ID,
            src.ORGANIZATION_CODE,
            src.INVENTORY_FLAG,
            src.BUSINESS_UNIT_ID,
            src.LEGAL_ENTITY_ID,
            src.MASTER_ORGANIZATION_ID,
            src.SOURCE_ORGANIZATION_ID,
            src.BZ_LOAD_DATE,
            src.SV_LOAD_DATE
        FROM #src src
        LEFT JOIN svo.D_ORGANIZATION tgt
            ON tgt.ORGANIZATION_ID = src.ORGANIZATION_ID
        WHERE tgt.ORGANIZATION_ID IS NULL;

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
        SET END_DTTM      = @EndDttm,
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
            SET END_DTTM      = @EndDttm,
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