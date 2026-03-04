CREATE OR ALTER PROCEDURE svo.usp_Load_D_ITEM_T1
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    DECLARE
        @ProcName      SYSNAME       = OBJECT_SCHEMA_NAME(@@PROCID) + '.' + OBJECT_NAME(@@PROCID),
        @TargetObject  SYSNAME       = 'svo.D_ITEM',
        @AsOfDate      DATE          = CAST(GETDATE() AS DATE),
        @StartDttm     DATETIME2(0)  = SYSDATETIME(),
        @EndDttm       DATETIME2(0),
        @RunId         BIGINT        = NULL,
        @ErrMsg        NVARCHAR(4000) = NULL,

        @LastWatermark DATETIME2(7)  = NULL,
        @MaxWatermark  DATETIME2(7)  = NULL,

        @RowInserted   INT           = 0,
        @RowUpdated    INT           = 0,
        @RowExpired    INT           = 0; -- not used for T1

    BEGIN TRY
        /* ===== Ensure watermark row exists ===== */
        IF NOT EXISTS (SELECT 1 FROM etl.ETL_WATERMARK WHERE TABLE_NAME = @TargetObject)
        BEGIN
            INSERT INTO etl.ETL_WATERMARK (TABLE_NAME, LAST_WATERMARK, CRE_DATE, UDT_DATE)
            VALUES (@TargetObject, '1900-01-01', SYSDATETIME(), SYSDATETIME());
        END;

        SELECT @LastWatermark = w.LAST_WATERMARK
        FROM etl.ETL_WATERMARK w
        WHERE w.TABLE_NAME = @TargetObject;

        IF @LastWatermark IS NULL
            SET @LastWatermark = '1900-01-01';

        /* ===== ETL_RUN start ===== */
        INSERT INTO etl.ETL_RUN (PROC_NAME, TARGET_OBJECT, ASOF_DATE, START_DTTM, STATUS)
        VALUES (@ProcName, @TargetObject, @AsOfDate, @StartDttm, 'STARTED');

        SET @RunId = SCOPE_IDENTITY();

        /* ===== Unique index on BK (INVENTORY_ITEM_ID, ORG_ID) ===== */
        IF NOT EXISTS
        (
            SELECT 1
            FROM sys.indexes
            WHERE name = 'UX_D_ITEM_INV_ORG'
              AND object_id = OBJECT_ID('svo.D_ITEM')
        )
        BEGIN
            CREATE UNIQUE NONCLUSTERED INDEX UX_D_ITEM_INV_ORG
                ON svo.D_ITEM (INVENTORY_ITEM_ID, ORG_ID)
                ON FG_SilverDim;
        END;

        /* ===== Plug row (SK=0) ===== */
        IF NOT EXISTS (SELECT 1 FROM svo.D_ITEM WHERE ITEM_SK = 0)
        BEGIN
            SET IDENTITY_INSERT svo.D_ITEM ON;

            INSERT INTO svo.D_ITEM
            (
                ITEM_SK,
                INVENTORY_ITEM_ID,
                ORG_ID,
                ITEM_NUMBER,
                ITEM_DESCRIPTION,
                ITEM_TYPE,
                ITEM_STATUS,
                BZ_LOAD_DATE,
                SV_LOAD_DATE,
                INFERRED_FLAG
            )
            VALUES
            (
                0,
                -1,
                -1,
                'UNKNOWN',
                'Unknown item',
                'Unknown',
                'Unknown',
                CAST(GETDATE() AS DATE),
                CAST(GETDATE() AS DATE),
                'Y'
            );

            SET IDENTITY_INSERT svo.D_ITEM OFF;
        END;

        /* ===== Source (incremental + dedup by BK) ===== */
        IF OBJECT_ID('tempdb..#src') IS NOT NULL DROP TABLE #src;

        SELECT
            s.INVENTORY_ITEM_ID,
            s.ORG_ID,
            s.ITEM_NUMBER,
            s.ITEM_DESCRIPTION,
            s.ITEM_TYPE,
            s.ITEM_STATUS,
            s.SourceAddDateTime
        INTO #src
        FROM
        (
            SELECT
                I.ITEMBASEPEOINVENTORYITEMID              AS INVENTORY_ITEM_ID,
                I.ItemBasePEOOrganizationId               AS ORG_ID,
                I.ITEMBASEPEOITEMNUMBER                   AS ITEM_NUMBER,
                I.ITEMTRANSLATIONPEODESCRIPTION           AS ITEM_DESCRIPTION,
                ISNULL(I.ITEMBASEPEOITEMTYPE,'UNK')       AS ITEM_TYPE,
                I.ItemBasePEOInventoryItemStatusCode      AS ITEM_STATUS,
                I.AddDateTime                             AS SourceAddDateTime,
                ROW_NUMBER() OVER
                (
                    PARTITION BY I.ITEMBASEPEOINVENTORYITEMID, I.ItemBasePEOOrganizationId
                    ORDER BY I.AddDateTime DESC
                ) AS rn
            FROM bzo.PIM_ItemExtractPVO AS I
            WHERE
                I.ITEMBASEPEOINVENTORYITEMID IS NOT NULL
                AND I.ItemBasePEOOrganizationId IS NOT NULL
                AND I.AddDateTime > @LastWatermark
        ) s
        WHERE s.rn = 1;

        /* never treat plug BK as data */
        DELETE FROM #src WHERE INVENTORY_ITEM_ID = -1 AND ORG_ID = -1;

        SELECT @MaxWatermark = MAX(SourceAddDateTime) FROM #src;

        /* ===== MERGE (Type 1) ===== */
        DECLARE @MergeAudit TABLE (ActionTaken NVARCHAR(10) NOT NULL);

        MERGE svo.D_ITEM AS tgt
        USING
        (
            SELECT
                INVENTORY_ITEM_ID,
                ORG_ID,
                ITEM_NUMBER,
                ITEM_DESCRIPTION,
                ITEM_TYPE,
                ITEM_STATUS,
                SourceAddDateTime
            FROM #src
        ) AS src
            ON tgt.INVENTORY_ITEM_ID = src.INVENTORY_ITEM_ID
           AND tgt.ORG_ID            = src.ORG_ID
        WHEN MATCHED THEN
            UPDATE SET
                tgt.ITEM_NUMBER      = src.ITEM_NUMBER,
                tgt.ITEM_DESCRIPTION = src.ITEM_DESCRIPTION,
                tgt.ITEM_TYPE        = src.ITEM_TYPE,
                tgt.ITEM_STATUS      = src.ITEM_STATUS,
                tgt.BZ_LOAD_DATE     = COALESCE(CAST(src.SourceAddDateTime AS DATE), CAST(GETDATE() AS DATE)),
                tgt.SV_LOAD_DATE     = CAST(GETDATE() AS DATE),
                tgt.INFERRED_FLAG    = 'N'
        WHEN NOT MATCHED BY TARGET THEN
            INSERT
            (
                INVENTORY_ITEM_ID,
                ORG_ID,
                ITEM_NUMBER,
                ITEM_DESCRIPTION,
                ITEM_TYPE,
                ITEM_STATUS,
                BZ_LOAD_DATE,
                SV_LOAD_DATE,
                INFERRED_FLAG
            )
            VALUES
            (
                src.INVENTORY_ITEM_ID,
                src.ORG_ID,
                src.ITEM_NUMBER,
                src.ITEM_DESCRIPTION,
                src.ITEM_TYPE,
                src.ITEM_STATUS,
                COALESCE(CAST(src.SourceAddDateTime AS DATE), CAST(GETDATE() AS DATE)),
                CAST(GETDATE() AS DATE),
                'N'
            )
        OUTPUT $action INTO @MergeAudit(ActionTaken);

        SELECT @RowInserted = SUM(CASE WHEN ActionTaken = 'INSERT' THEN 1 ELSE 0 END),
               @RowUpdated  = SUM(CASE WHEN ActionTaken = 'UPDATE' THEN 1 ELSE 0 END)
        FROM @MergeAudit;
        IF @RowInserted IS NULL SET @RowInserted=0;
        IF @RowUpdated IS NULL SET @RowUpdated=0;

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
            ROW_UPDATED   = @RowUpdated,
            ROW_EXPIRED   = @RowExpired,
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
                ROW_UPDATED   = @RowUpdated,
                ROW_EXPIRED   = @RowExpired,
                ERROR_MESSAGE = @ErrMsg
            WHERE RUN_ID = @RunId;
        END;

        ;THROW;
    END CATCH
END;
GO