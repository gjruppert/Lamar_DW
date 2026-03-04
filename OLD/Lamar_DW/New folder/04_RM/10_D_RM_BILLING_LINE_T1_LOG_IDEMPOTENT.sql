/* =====================================================================================
   D_RM_BILLING_LINE (Dim, Type 1, idempotent MERGE)
   - Source synonym:
       src.bzo_VRM_BillingLineDetailsPVO
   - Logging:
       etl.ETL_RUN
   - Dates:
       BZ_LOAD_DATE = COALESCE(CAST(AddDateTime AS DATE), CAST(GETDATE() AS DATE))  -- never NULL
       SV_LOAD_DATE = CAST(GETDATE() AS DATE)
   ===================================================================================== */

SET ANSI_NULLS ON;
GO
SET QUOTED_IDENTIFIER ON;
GO

/* =================
   TABLE (create if missing)
   ================= */
IF OBJECT_ID(N'svo.D_RM_BILLING_LINE', N'U') IS NULL
BEGIN
    CREATE TABLE [svo].[D_RM_BILLING_LINE]
    (
        RM_BILLING_LINE_SK         BIGINT IDENTITY(1,1) NOT NULL,
        BILLING_LINE_DETAIL_ID     BIGINT        NOT NULL,   -- BK

        BILL_DATE                  DATE          NOT NULL,
        BILL_ID                    BIGINT        NOT NULL,
        BILL_LINE_ID               BIGINT        NOT NULL,
        BILL_LINE_NUMBER           VARCHAR(30)   NOT NULL,
        BILL_NUMBER                VARCHAR(60)   NOT NULL,

        CREATED_BY                 VARCHAR(250)  NULL,
        CREATION_DATE              DATE          NOT NULL,
        LAST_UPDATE_DATE           DATE          NOT NULL,
        LAST_UPDATED_BY            VARCHAR(250)  NULL,

        BZ_LOAD_DATE               DATE          NOT NULL,
        SV_LOAD_DATE               DATE          NOT NULL,

        CONSTRAINT PK_D_RM_BILLING_LINE
            PRIMARY KEY CLUSTERED (RM_BILLING_LINE_SK ASC) ON [FG_SilverDim]
    ) ON [FG_SilverDim];

    CREATE UNIQUE NONCLUSTERED INDEX UX_D_RM_BILLING_LINE_DETAIL_ID
    ON [svo].[D_RM_BILLING_LINE] (BILLING_LINE_DETAIL_ID)
    ON [FG_SilverDim];
END
GO

/* =================
   PLUG ROW (SK=0)
   ================= */
IF NOT EXISTS (SELECT 1 FROM svo.D_RM_BILLING_LINE WHERE RM_BILLING_LINE_SK = 0)
BEGIN
    SET IDENTITY_INSERT svo.D_RM_BILLING_LINE ON;

    INSERT INTO svo.D_RM_BILLING_LINE
    (
        RM_BILLING_LINE_SK,
        BILLING_LINE_DETAIL_ID,
        BILL_DATE,
        BILL_ID,
        BILL_LINE_ID,
        BILL_LINE_NUMBER,
        BILL_NUMBER,
        CREATED_BY,
        CREATION_DATE,
        LAST_UPDATE_DATE,
        LAST_UPDATED_BY,
        BZ_LOAD_DATE,
        SV_LOAD_DATE
    )
    VALUES
    (
        0,
        -1,
        CAST('0001-01-01' AS DATE),
        -1,
        -1,
        'UNKNOWN',
        'UNKNOWN',
        'SYSTEM',
        CAST('0001-01-01' AS DATE),
        CAST('0001-01-01' AS DATE),
        'SYSTEM',
        CAST('0001-01-01' AS DATE),
        CAST(GETDATE() AS DATE)
    );

    SET IDENTITY_INSERT svo.D_RM_BILLING_LINE OFF;
END
GO

/* =================
   PROCEDURE
   ================= */
IF OBJECT_ID(N'svo.usp_Load_D_RM_BILLING_LINE_T1', N'P') IS NOT NULL
    DROP PROCEDURE svo.usp_Load_D_RM_BILLING_LINE_T1;
GO

CREATE PROCEDURE svo.usp_Load_D_RM_BILLING_LINE_T1
(
      @FullReload BIT = 0
    , @Debug      BIT = 0
)
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    DECLARE
          @ProcName SYSNAME = OBJECT_SCHEMA_NAME(@@PROCID) + '.' + OBJECT_NAME(@@PROCID)
        , @Target   SYSNAME = N'svo.D_RM_BILLING_LINE'
        , @RunId    BIGINT
        , @RowsIns  INT = 0
        , @RowsUpd  INT = 0;

    BEGIN TRY
        /* Log start */
        INSERT INTO etl.ETL_RUN (PROC_NAME, TARGET_OBJECT, ASOF_DATE, STATUS)
        VALUES (@ProcName, @Target, CAST(GETDATE() AS DATE), 'STARTED');

        SET @RunId = SCOPE_IDENTITY();

        IF @Debug = 1 PRINT CONCAT(@ProcName, ' starting. FullReload=', @FullReload);

        IF OBJECT_ID('tempdb..#src') IS NOT NULL DROP TABLE #src;

        SELECT
              BILLING_LINE_DETAIL_ID = TRY_CONVERT(BIGINT, B.BillingLineDetailId)
            , BILL_DATE              = TRY_CONVERT(DATE,   B.BillingLineDetailsBillDate)
            , BILL_ID                = TRY_CONVERT(BIGINT, B.BillingLineDetailsBillId)
            , BILL_LINE_ID           = TRY_CONVERT(BIGINT, B.BillingLineDetailsBillLineId)
            , BILL_LINE_NUMBER       = ISNULL(NULLIF(LTRIM(RTRIM(B.BillingLineDetailsBillLineNumber)), ''), 'UNKNOWN')
            , BILL_NUMBER            = ISNULL(NULLIF(LTRIM(RTRIM(B.BillingLineDetailsBillNumber)), ''), 'UNKNOWN')

            , CREATED_BY             = NULLIF(LTRIM(RTRIM(B.BillingLineDetailsCreatedBy)), '')
            , CREATION_DATE          = COALESCE(TRY_CONVERT(DATE, B.BillingLineDetailsCreationDate), CAST('0001-01-01' AS DATE))
            , LAST_UPDATE_DATE       = COALESCE(TRY_CONVERT(DATE, B.BillingLineDetailsLastUpdateDate), CAST('0001-01-01' AS DATE))
            , LAST_UPDATED_BY        = NULLIF(LTRIM(RTRIM(B.BillingLineDetailsLastUpdatedBy)), '')

            , BZ_LOAD_DATE           = COALESCE(CAST(B.AddDateTime AS DATE), CAST(GETDATE() AS DATE))
            , SV_LOAD_DATE           = CAST(GETDATE() AS DATE)
        INTO #src
        FROM src.bzo_VRM_BillingLineDetailsPVO AS B
        WHERE B.BillingLineDetailId IS NOT NULL;

        /* De-dupe safety on BK */
        ;WITH d AS
        (
            SELECT BILLING_LINE_DETAIL_ID,
                   rn = ROW_NUMBER() OVER (PARTITION BY BILLING_LINE_DETAIL_ID ORDER BY BILLING_LINE_DETAIL_ID)
            FROM #src
        )
        DELETE s
        FROM #src s
        JOIN d
          ON d.BILLING_LINE_DETAIL_ID = s.BILLING_LINE_DETAIL_ID
         AND d.rn > 1;

        BEGIN TRAN;

        IF @FullReload = 1
        BEGIN
            DELETE FROM svo.D_RM_BILLING_LINE
            WHERE RM_BILLING_LINE_SK <> 0;

            INSERT INTO svo.D_RM_BILLING_LINE
            (
                BILLING_LINE_DETAIL_ID, BILL_DATE, BILL_ID, BILL_LINE_ID, BILL_LINE_NUMBER, BILL_NUMBER,
                CREATED_BY, CREATION_DATE, LAST_UPDATE_DATE, LAST_UPDATED_BY,
                BZ_LOAD_DATE, SV_LOAD_DATE
            )
            SELECT
                BILLING_LINE_DETAIL_ID, BILL_DATE, BILL_ID, BILL_LINE_ID, BILL_LINE_NUMBER, BILL_NUMBER,
                CREATED_BY, CREATION_DATE, LAST_UPDATE_DATE, LAST_UPDATED_BY,
                BZ_LOAD_DATE, CAST(GETDATE() AS DATE)
            FROM #src;

            SET @RowsIns = @@ROWCOUNT;
        END
        ELSE
        BEGIN
            MERGE svo.D_RM_BILLING_LINE AS tgt
            USING #src AS src
              ON tgt.BILLING_LINE_DETAIL_ID = src.BILLING_LINE_DETAIL_ID
            WHEN MATCHED AND tgt.RM_BILLING_LINE_SK <> 0 AND
            (
                   tgt.BILL_DATE        <> src.BILL_DATE
                OR tgt.BILL_ID          <> src.BILL_ID
                OR tgt.BILL_LINE_ID     <> src.BILL_LINE_ID
                OR tgt.BILL_LINE_NUMBER <> src.BILL_LINE_NUMBER
                OR tgt.BILL_NUMBER      <> src.BILL_NUMBER
                OR ISNULL(tgt.CREATED_BY,'')      <> ISNULL(src.CREATED_BY,'')
                OR tgt.CREATION_DATE    <> src.CREATION_DATE
                OR tgt.LAST_UPDATE_DATE <> src.LAST_UPDATE_DATE
                OR ISNULL(tgt.LAST_UPDATED_BY,'') <> ISNULL(src.LAST_UPDATED_BY,'')
            )
            THEN UPDATE SET
                  tgt.BILL_DATE        = src.BILL_DATE
                , tgt.BILL_ID          = src.BILL_ID
                , tgt.BILL_LINE_ID     = src.BILL_LINE_ID
                , tgt.BILL_LINE_NUMBER = src.BILL_LINE_NUMBER
                , tgt.BILL_NUMBER      = src.BILL_NUMBER
                , tgt.CREATED_BY       = src.CREATED_BY
                , tgt.CREATION_DATE    = src.CREATION_DATE
                , tgt.LAST_UPDATE_DATE = src.LAST_UPDATE_DATE
                , tgt.LAST_UPDATED_BY  = src.LAST_UPDATED_BY
                , tgt.BZ_LOAD_DATE     = src.BZ_LOAD_DATE
                , tgt.SV_LOAD_DATE     = CAST(GETDATE() AS DATE)
            WHEN NOT MATCHED BY TARGET THEN
            INSERT
            (
                BILLING_LINE_DETAIL_ID, BILL_DATE, BILL_ID, BILL_LINE_ID, BILL_LINE_NUMBER, BILL_NUMBER,
                CREATED_BY, CREATION_DATE, LAST_UPDATE_DATE, LAST_UPDATED_BY,
                BZ_LOAD_DATE, SV_LOAD_DATE
            )
            VALUES
            (
                src.BILLING_LINE_DETAIL_ID, src.BILL_DATE, src.BILL_ID, src.BILL_LINE_ID, src.BILL_LINE_NUMBER, src.BILL_NUMBER,
                src.CREATED_BY, src.CREATION_DATE, src.LAST_UPDATE_DATE, src.LAST_UPDATED_BY,
                src.BZ_LOAD_DATE, CAST(GETDATE() AS DATE)
            );

            /* Split counts: inserted = new BKs; updated = rows touched today */
            SELECT @RowsIns = COUNT(*)
            FROM #src s
            WHERE NOT EXISTS (SELECT 1 FROM svo.D_RM_BILLING_LINE t WHERE t.BILLING_LINE_DETAIL_ID = s.BILLING_LINE_DETAIL_ID);

            SELECT @RowsUpd = COUNT(*)
            FROM svo.D_RM_BILLING_LINE t
            WHERE t.RM_BILLING_LINE_SK <> 0
              AND t.SV_LOAD_DATE = CAST(GETDATE() AS DATE);
        END

        COMMIT;

        UPDATE etl.ETL_RUN
           SET END_DTTM       = SYSDATETIME()
             , STATUS         = 'SUCCESS'
             , ROW_INSERTED   = @RowsIns
             , ROW_UPDATED_T1 = @RowsUpd
        WHERE RUN_ID = @RunId;

        IF @Debug = 1 PRINT CONCAT('Done. Inserted=', @RowsIns, ' Updated=', @RowsUpd);
    END TRY
    BEGIN CATCH
        IF XACT_STATE() <> 0 ROLLBACK;

        DECLARE @ErrMsg NVARCHAR(4000) = ERROR_MESSAGE();

        IF @RunId IS NOT NULL
        BEGIN
            UPDATE etl.ETL_RUN
               SET END_DTTM       = SYSDATETIME()
                 , STATUS         = 'FAILED'
                 , ERROR_MESSAGE  = LEFT(@ErrMsg, 4000)
            WHERE RUN_ID = @RunId;
        END

        RAISERROR('%s', 16, 1, @ErrMsg);
        RETURN;
    END CATCH
END
GO

/* Run:
EXEC svo.usp_Load_D_RM_BILLING_LINE_T1 @FullReload = 1, @Debug = 1;
EXEC svo.usp_Load_D_RM_BILLING_LINE_T1 @FullReload = 0, @Debug = 0;
*/
