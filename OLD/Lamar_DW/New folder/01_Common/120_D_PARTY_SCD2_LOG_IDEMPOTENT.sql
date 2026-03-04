USE [Oracle_Reporting_P2];
GO

/* =====================================================================
   DIM: svo.D_PARTY (Hybrid Type1/Type2 SCD)
   BK: PARTY_ID

   NOTE: The uploaded D_PARTY.sql appears truncated mid-SELECT, so the
         source mapping below follows the standard AR_PartyExtractPVO
         columns used in your partial script (PartyId, PartyNumber, etc.).

   Source (synonym-based / DB independent):
     - src.bzo_AR_PartyExtractPVO

   Hybrid approach (locked):
     - Type 1: PARTY_NUMBER, PARTY_NAME
     - Type 2: PARTY_TYPE, STATUS, COUNTRY/STATE/CITY/POSTAL_CODE, CREATED_BY,
               CREATION_DATE, LAST_UPDATE_DATE

   Locked rules applied:
     - Stored proc in svo schema
     - BZ_LOAD_DATE never NULL:
         COALESCE(CAST(AddDateTime AS date), CAST(GETDATE() AS date))
     - SCD2 fields: EFF_DATE, END_DATE, CRE_DATE, UDT_DATE, CURR_IND
     - ETL run logging (etl.ETL_RUN)
     - Idempotent + transactional
   ===================================================================== */

/* =========================
   0) Logging (create once)
   ========================= */
IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'etl')
    EXEC('CREATE SCHEMA etl AUTHORIZATION dbo;');
GO

IF OBJECT_ID(N'etl.ETL_RUN', 'U') IS NULL
BEGIN
    CREATE TABLE etl.ETL_RUN
    (
          RUN_ID          bigint IDENTITY(1,1) NOT NULL CONSTRAINT PK_ETL_RUN PRIMARY KEY
        , PROC_NAME       sysname              NOT NULL
        , TARGET_OBJECT   sysname              NOT NULL
        , ASOF_DATE       date                 NULL
        , START_DTTM      datetime2(0)         NOT NULL
        , END_DTTM        datetime2(0)         NULL
        , STATUS          varchar(20)          NOT NULL  -- STARTED/SUCCESS/FAILED
        , ROW_INSERTED    int                  NULL
        , ROW_EXPIRED     int                  NULL
        , ROW_UPDATED_T1  int                  NULL
        , ERROR_MESSAGE   nvarchar(4000)       NULL
    );
END
GO

IF COL_LENGTH('etl.ETL_RUN', 'ROW_UPDATED_T1') IS NULL
    ALTER TABLE etl.ETL_RUN ADD ROW_UPDATED_T1 int NULL;
GO

/* ==============================================
   1) Ensure standard SCD columns exist (add only if needed)
   ============================================== */
IF COL_LENGTH('svo.D_PARTY', 'EFF_DATE') IS NULL
    ALTER TABLE svo.D_PARTY ADD EFF_DATE date NOT NULL
        CONSTRAINT DF_D_PARTY_EFF_DATE DEFAULT (CAST('0001-01-01' AS date));
GO
IF COL_LENGTH('svo.D_PARTY', 'END_DATE') IS NULL
    ALTER TABLE svo.D_PARTY ADD END_DATE date NOT NULL
        CONSTRAINT DF_D_PARTY_END_DATE DEFAULT (CAST('9999-12-31' AS date));
GO
IF COL_LENGTH('svo.D_PARTY', 'CRE_DATE') IS NULL
    ALTER TABLE svo.D_PARTY ADD CRE_DATE datetime2(0) NOT NULL
        CONSTRAINT DF_D_PARTY_CRE_DATE DEFAULT (SYSDATETIME());
GO
IF COL_LENGTH('svo.D_PARTY', 'UDT_DATE') IS NULL
    ALTER TABLE svo.D_PARTY ADD UDT_DATE datetime2(0) NOT NULL
        CONSTRAINT DF_D_PARTY_UDT_DATE DEFAULT (SYSDATETIME());
GO
IF COL_LENGTH('svo.D_PARTY', 'CURR_IND') IS NULL
    ALTER TABLE svo.D_PARTY ADD CURR_IND char(1) NOT NULL
        CONSTRAINT DF_D_PARTY_CURR_IND DEFAULT ('Y');
GO

/* ==============================================
   2) Create filtered unique index for current rows (BK)
   ============================================== */
IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'UX_D_PARTY_ID_CURR' AND object_id = OBJECT_ID('svo.D_PARTY'))
BEGIN
    CREATE UNIQUE NONCLUSTERED INDEX UX_D_PARTY_ID_CURR
        ON svo.D_PARTY(PARTY_ID)
        WHERE CURR_IND = 'Y' AND END_DATE = '9999-12-31'
        ON FG_SilverDim;
END
GO

/* ================================
   3) Hybrid SCD loader stored procedure
   ================================ */
CREATE OR ALTER PROCEDURE svo.usp_Load_D_PARTY_SCD2
(
      @AsOfDate date = NULL
)
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    IF @AsOfDate IS NULL
        SET @AsOfDate = CAST(GETDATE() AS date);

    DECLARE @HighDate date = '9999-12-31';

    DECLARE
          @RunId        bigint
        , @ProcName     sysname = OBJECT_SCHEMA_NAME(@@PROCID) + '.' + OBJECT_NAME(@@PROCID)
        , @TargetObject sysname = 'svo.D_PARTY';

    DECLARE
          @Inserted   int = 0
        , @Expired    int = 0
        , @UpdatedT1  int = 0;

    INSERT INTO etl.ETL_RUN (PROC_NAME, TARGET_OBJECT, ASOF_DATE, START_DTTM, STATUS)
    VALUES (@ProcName, @TargetObject, @AsOfDate, SYSDATETIME(), 'STARTED');

    SET @RunId = SCOPE_IDENTITY();

    BEGIN TRY
        BEGIN TRAN;

        /* Plug row (SK=0, BK=-1) */
        IF NOT EXISTS (SELECT 1 FROM svo.D_PARTY WHERE PARTY_SK = 0)
        BEGIN
            SET IDENTITY_INSERT svo.D_PARTY ON;

            INSERT INTO svo.D_PARTY (PARTY_SK, PARTY_ID, PARTY_NUMBER, PARTY_NAME, PARTY_TYPE, STATUS, COUNTRY, STATE, CITY, POSTAL_CODE, CREATED_BY, CREATION_DATE, LAST_UPDATE_DATE, BZ_LOAD_DATE, SV_LOAD_DATE, EFF_DATE, END_DATE, CRE_DATE, UDT_DATE, CURR_IND)
            VALUES (0, -1, '-1', 'Unknown', 'Unknown', 'U', 'UNK', 'UN', 'UNK', 'UNK', 'Unknown', CAST('0001-01-01' AS date), CAST('0001-01-01' AS date), CAST(GETDATE() AS date), CAST(GETDATE() AS date), CAST(GETDATE() AS date), CAST('9999-12-31' AS date), SYSDATETIME(), SYSDATETIME(), 'Y');

            SET IDENTITY_INSERT svo.D_PARTY OFF;
        END

        /* Source snapshot */
        IF OBJECT_ID('tempdb..#src') IS NOT NULL DROP TABLE #src;
        SELECT
              CAST(PartyId AS bigint)                          AS PARTY_ID
            , ISNULL(CAST(PartyNumber AS varchar(30)),'-1')    AS PARTY_NUMBER
            , ISNULL(CAST(PartyName AS varchar(360)),'Unknown')AS PARTY_NAME
            , ISNULL(CAST(PartyType AS varchar(30)),'Unknown') AS PARTY_TYPE
            , ISNULL(CAST(Status AS varchar(1)),'U')           AS STATUS
            , ISNULL(CAST(Country AS varchar(2)),'UNK')        AS COUNTRY
            , ISNULL(CAST(State AS varchar(60)),'UN')          AS STATE
            , ISNULL(CAST(City AS varchar(60)),'UNK')          AS CITY
            , ISNULL(CAST(PostalCode AS varchar(60)),'UNK')    AS POSTAL_CODE
            , CAST(CreatedBy AS varchar(64))                   AS CREATED_BY
            , CAST(CreationDate AS date)                       AS CREATION_DATE
            , CAST(LastUpdateDate AS date)                     AS LAST_UPDATE_DATE
            , COALESCE(CAST(AddDateTime AS date), CAST(GETDATE() AS date)) AS BZ_LOAD_DATE
            , CAST(GETDATE() AS date)                          AS SV_LOAD_DATE
        INTO #src
        FROM src.bzo_AR_PartyExtractPVO;

        DELETE FROM #src WHERE PARTY_ID IS NULL OR PARTY_ID = -1;

        /* Type2 hash (exclude Type1 columns and load dates) */
        ALTER TABLE #src ADD HASH_T2 varbinary(32) NULL;

        UPDATE s
            SET s.HASH_T2 = HASHBYTES('SHA2_256', CONCAT(COALESCE(CONVERT(nvarchar(400), s.PARTY_ID), N''), N'|', COALESCE(CONVERT(nvarchar(400), s.PARTY_TYPE), N''), N'|', COALESCE(CONVERT(nvarchar(400), s.STATUS), N''), N'|', COALESCE(CONVERT(nvarchar(400), s.COUNTRY), N''), N'|', COALESCE(CONVERT(nvarchar(400), s.STATE), N''), N'|', COALESCE(CONVERT(nvarchar(400), s.CITY), N''), N'|', COALESCE(CONVERT(nvarchar(400), s.POSTAL_CODE), N''), N'|', COALESCE(CONVERT(nvarchar(400), s.CREATED_BY), N''), N'|', COALESCE(CONVERT(nvarchar(10), s.CREATION_DATE, 120), N''), N'|', COALESCE(CONVERT(nvarchar(10), s.LAST_UPDATE_DATE, 120), N'')))
        FROM #src s;

        /* Current target snapshot */
        IF OBJECT_ID('tempdb..#tgt') IS NOT NULL DROP TABLE #tgt;

        SELECT
              t.PARTY_ID
            , t.PARTY_NUMBER
            , t.PARTY_NAME
            , HASHBYTES('SHA2_256', CONCAT(COALESCE(CONVERT(nvarchar(400), t.PARTY_ID), N''), N'|', COALESCE(CONVERT(nvarchar(400), t.PARTY_TYPE), N''), N'|', COALESCE(CONVERT(nvarchar(400), t.STATUS), N''), N'|', COALESCE(CONVERT(nvarchar(400), t.COUNTRY), N''), N'|', COALESCE(CONVERT(nvarchar(400), t.STATE), N''), N'|', COALESCE(CONVERT(nvarchar(400), t.CITY), N''), N'|', COALESCE(CONVERT(nvarchar(400), t.POSTAL_CODE), N''), N'|', COALESCE(CONVERT(nvarchar(400), t.CREATED_BY), N''), N'|', COALESCE(CONVERT(nvarchar(10), t.CREATION_DATE, 120), N''), N'|', COALESCE(CONVERT(nvarchar(10), t.LAST_UPDATE_DATE, 120), N''))) AS HASH_T2
        INTO #tgt
        FROM svo.D_PARTY t
        WHERE t.CURR_IND = 'Y'
          AND t.END_DATE = @HighDate
          AND t.PARTY_ID <> -1;

        /* Type 1 updates (number/name) when Type2 unchanged */
        UPDATE tgt
            SET
                  tgt.PARTY_NUMBER = src.PARTY_NUMBER
                , tgt.PARTY_NAME   = src.PARTY_NAME
                , tgt.UDT_DATE     = SYSDATETIME()
                , tgt.SV_LOAD_DATE = CAST(GETDATE() AS date)
                , tgt.BZ_LOAD_DATE = COALESCE(tgt.BZ_LOAD_DATE, src.BZ_LOAD_DATE, CAST(GETDATE() AS date))
        FROM svo.D_PARTY tgt
        INNER JOIN #tgt cur ON cur.PARTY_ID = tgt.PARTY_ID
        INNER JOIN #src src ON src.PARTY_ID = cur.PARTY_ID
        WHERE tgt.CURR_IND = 'Y'
          AND tgt.END_DATE = @HighDate
          AND cur.HASH_T2 = src.HASH_T2
          AND (
                ISNULL(tgt.PARTY_NUMBER,'') <> ISNULL(src.PARTY_NUMBER,'')
             OR ISNULL(tgt.PARTY_NAME,'')   <> ISNULL(src.PARTY_NAME,'')
          );

        SET @UpdatedT1 = @@ROWCOUNT;

        /* Delta for Type2 (new or changed) */
        IF OBJECT_ID('tempdb..#delta_t2') IS NOT NULL DROP TABLE #delta_t2;

        SELECT s.*
        INTO #delta_t2
        FROM #src s
        LEFT JOIN #tgt t ON t.PARTY_ID = s.PARTY_ID
        WHERE t.PARTY_ID IS NULL
           OR t.HASH_T2 <> s.HASH_T2;

        /* Expire current rows for changed keys */
        UPDATE tgt
            SET
                  tgt.END_DATE     = DATEADD(day, -1, @AsOfDate)
                , tgt.CURR_IND     = 'N'
                , tgt.UDT_DATE     = SYSDATETIME()
                , tgt.SV_LOAD_DATE = CAST(GETDATE() AS date)
        FROM svo.D_PARTY tgt
        INNER JOIN #delta_t2 d ON d.PARTY_ID = tgt.PARTY_ID
        WHERE tgt.CURR_IND = 'Y'
          AND tgt.END_DATE = @HighDate
          AND tgt.PARTY_ID <> -1
          AND EXISTS (SELECT 1 FROM #tgt cur WHERE cur.PARTY_ID = d.PARTY_ID);

        SET @Expired = @@ROWCOUNT;

        /* Insert new current rows */
        INSERT INTO svo.D_PARTY
        (
              PARTY_ID, PARTY_NUMBER, PARTY_NAME, PARTY_TYPE, STATUS, COUNTRY, STATE, CITY, POSTAL_CODE, CREATED_BY, CREATION_DATE, LAST_UPDATE_DATE, BZ_LOAD_DATE, SV_LOAD_DATE
            , EFF_DATE
            , END_DATE
            , CRE_DATE
            , UDT_DATE
            , CURR_IND
        )
        SELECT
              d.PARTY_ID, d.PARTY_NUMBER, d.PARTY_NAME, d.PARTY_TYPE, d.STATUS, d.COUNTRY, d.STATE, d.CITY, d.POSTAL_CODE, d.CREATED_BY, d.CREATION_DATE, d.LAST_UPDATE_DATE, d.BZ_LOAD_DATE, d.SV_LOAD_DATE
            , @AsOfDate
            , @HighDate
            , SYSDATETIME()
            , SYSDATETIME()
            , 'Y'
        FROM #delta_t2 d;

        SET @Inserted = @@ROWCOUNT;

        COMMIT;

        UPDATE etl.ETL_RUN
            SET
                  END_DTTM       = SYSDATETIME()
                , STATUS         = 'SUCCESS'
                , ROW_INSERTED   = @Inserted
                , ROW_EXPIRED    = @Expired
                , ROW_UPDATED_T1 = @UpdatedT1
                , ERROR_MESSAGE  = NULL
        WHERE RUN_ID = @RunId;

    END TRY
    BEGIN CATCH
        IF @@TRANCOUNT > 0 ROLLBACK;

        DECLARE @Err nvarchar(4000) = ERROR_MESSAGE();

        UPDATE etl.ETL_RUN
            SET
                  END_DTTM       = SYSDATETIME()
                , STATUS         = 'FAILED'
                , ROW_INSERTED   = @Inserted
                , ROW_EXPIRED    = @Expired
                , ROW_UPDATED_T1 = @UpdatedT1
                , ERROR_MESSAGE  = @Err
        WHERE RUN_ID = @RunId;

        THROW;
    END CATCH
END
GO

