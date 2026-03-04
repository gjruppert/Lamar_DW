/* =====================================================================================
   F_GL_LINES (Fact, Type 1, idempotent MERGE)
   - Source synonyms:
       src.bzo_GL_JournalLineExtractPVO
       src.bzo_GL_JournalHeaderExtractPVO
       src.stage_LINES_CODE_COMBO_LOOKUP
   - Dimension lookups (targets):
       svo.D_GL_HEADER, svo.D_ACCOUNT, svo.D_BUSINESS_OFFERING, svo.D_COMPANY,
       svo.D_COST_CENTER, svo.D_INDUSTRY, svo.D_INTERCOMPANY, svo.D_LEDGER, svo.D_CURRENCY
   - Logging: etl.ETL_RUN
   - Dates:
       BZ_LOAD_DATE = COALESCE(CAST(AddDateTime AS DATE), CAST(GETDATE() AS DATE))  (never NULL)
       SV_LOAD_DATE = CAST(GETDATE() AS DATE)
   ===================================================================================== */

IF OBJECT_ID(N'svo.usp_Load_F_GL_LINES_T1', N'P') IS NOT NULL
    DROP PROCEDURE svo.usp_Load_F_GL_LINES_T1;
GO

CREATE PROCEDURE svo.usp_Load_F_GL_LINES_T1
(
      @FullReload BIT = 0
    , @AsOfDate   DATE = NULL
)
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    DECLARE
          @ProcName SYSNAME = OBJECT_SCHEMA_NAME(@@PROCID) + '.' + OBJECT_NAME(@@PROCID)
        , @Target   SYSNAME = N'svo.F_GL_LINES'
        , @RunId    BIGINT
        , @RowsIns  INT = 0
        , @RowsUpdT1 INT = 0;

    SET @AsOfDate = COALESCE(@AsOfDate, CAST(GETDATE() AS DATE));

    BEGIN TRY
        /* -------------------------
           Ensure target table exists
           ------------------------- */
        IF OBJECT_ID(N'svo.F_GL_LINES', N'U') IS NULL
        BEGIN
            CREATE TABLE svo.F_GL_LINES
            (
                GL_LINE_PK            BIGINT      NOT NULL,
                GL_HEADER_SK          BIGINT      NOT NULL,
                ACCOUNT_SK            BIGINT      NOT NULL,
                BUSINESS_OFFERING_SK  BIGINT      NOT NULL,
                COMPANY_SK            BIGINT      NOT NULL,
                COST_CENTER_SK        BIGINT      NOT NULL,
                CURRENCY_SK           BIGINT      NOT NULL,
                INDUSTRY_SK           BIGINT      NOT NULL,
                INTERCOMPANY_SK       BIGINT      NOT NULL,
                EFFECTIVE_DATE_SK     INT         NOT NULL,
                LEDGER_SK             BIGINT      NOT NULL,

                LINE_NUM              BIGINT      NULL,
                [DESCRIPTION]         NVARCHAR(1000) NULL,
                ACCOUNTED_CR          NUMERIC(18,4) NULL,
                ACCOUNTED_DR          NUMERIC(18,4) NULL,
                AMOUNT_USD            NUMERIC(18,4) NULL,
                AMOUNT_LOCAL          NUMERIC(18,4) NULL,
                CREATED_BY            NVARCHAR(32)  NULL,
                LAST_UPDATED_BY       NVARCHAR(64)  NULL,
                LAST_UPDATED_DATE     DATE          NULL,
                CREATION_DATE         DATE          NULL,
                BZ_LOAD_DATE          DATE          NOT NULL,
                SV_LOAD_DATE          DATE          NOT NULL,
                CODE_COMBINATION_ID   BIGINT        NULL,

                CONSTRAINT PK_F_GL_LINES PRIMARY KEY CLUSTERED (GL_LINE_PK) ON FG_SilverFact
            ) ON FG_SilverFact;
        END

        IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'F_GL_LINES_EFFDT_ACCTSK_COSK_ETC' AND object_id = OBJECT_ID('svo.F_GL_LINES'))
        BEGIN
            CREATE NONCLUSTERED INDEX [F_GL_LINES_EFFDT_ACCTSK_COSK_ETC] ON [svo].[F_GL_LINES]
            (
                [EFFECTIVE_DATE_SK] ASC,
                [ACCOUNT_SK] ASC,
                [COMPANY_SK] ASC,
                [BUSINESS_OFFERING_SK] ASC,
                [INDUSTRY_SK] ASC,
                [COST_CENTER_SK] ASC,
                [INTERCOMPANY_SK] ASC
            )
            INCLUDE([DESCRIPTION],[ACCOUNTED_CR],[ACCOUNTED_DR],[AMOUNT_USD],[AMOUNT_LOCAL])
            ON FG_SilverFact;
        END

        /* -------------------------
           Log start (etl.ETL_RUN)
           ------------------------- */
        INSERT INTO etl.ETL_RUN (PROC_NAME, TARGET_OBJECT, ASOF_DATE, STATUS)
        VALUES (@ProcName, @Target, @AsOfDate, 'STARTED');

        SET @RunId = SCOPE_IDENTITY();

        /* -------------------------
           Build source set
           ------------------------- */
        IF OBJECT_ID('tempdb..#src') IS NOT NULL DROP TABLE #src;

        SELECT
            GL_LINE_PK =
                TRY_CONVERT(BIGINT, CONCAT(
                    TRY_CONVERT(VARCHAR(30), H.GLJEHEADERSJEBATCHID),
                    TRY_CONVERT(VARCHAR(30), H.JEHEADERID),
                    TRY_CONVERT(VARCHAR(30), L.JELINENUM)
                )),

            GL_HEADER_SK         = ISNULL(DH.GL_HEADER_SK, 0),
            ACCOUNT_SK           = ISNULL(DA.ACCOUNT_SK, 0),
            BUSINESS_OFFERING_SK = ISNULL(DBO.BUSINESS_OFFERING_SK, 0),
            COMPANY_SK           = ISNULL(DCO.COMPANY_SK, 0),
            COST_CENTER_SK       = ISNULL(DCC.COST_CENTER_SK, 0),
            CURRENCY_SK          = ISNULL(CUR.CURRENCY_SK, 0),
            INDUSTRY_SK          = ISNULL(DI.INDUSTRY_SK, 0),
            INTERCOMPANY_SK      = ISNULL(DIC.INTERCOMPANY_SK, 0),

            EFFECTIVE_DATE_SK =
                ISNULL(
                    TRY_CONVERT(INT, CONVERT(CHAR(8), TRY_CONVERT(DATE, L.GLJELINESEFFECTIVEDATE), 112)),
                    0
                ),

            LEDGER_SK            = ISNULL(LDG.LEDGER_SK, 0),

            LINE_NUM             = TRY_CONVERT(BIGINT, L.JELINENUM),
            [DESCRIPTION]        = NULLIF(LTRIM(RTRIM(L.GLJELINESDESCRIPTION)), ''),

            ACCOUNTED_CR         = TRY_CONVERT(NUMERIC(18,4), ISNULL(L.GLJELINESACCOUNTEDCR, 0)),
            ACCOUNTED_DR         = TRY_CONVERT(NUMERIC(18,4), ISNULL(L.GLJELINESACCOUNTEDDR, 0)),

            AMOUNT_USD =
                CASE L.GlJeLinesLedgerId
                    WHEN '300000004574005' THEN TRY_CONVERT(NUMERIC(18,4), 0)
                    ELSE TRY_CONVERT(NUMERIC(18,4),
                         (TRY_CONVERT(NUMERIC(18,4), ISNULL(L.GLJELINESACCOUNTEDDR,0)) - TRY_CONVERT(NUMERIC(18,4), ISNULL(L.GLJELINESACCOUNTEDCR,0)))
                         * TRY_CONVERT(NUMERIC(18,4), ISNULL(L.GLJELINESCURRENCYCONVERSIONRATE, 1))
                    )
                END,

            AMOUNT_LOCAL =
                TRY_CONVERT(NUMERIC(18,4),
                    (TRY_CONVERT(NUMERIC(18,4), ISNULL(L.GLJELINESACCOUNTEDDR,0)) - TRY_CONVERT(NUMERIC(18,4), ISNULL(L.GLJELINESACCOUNTEDCR,0)))
                ),

            CREATED_BY           = NULLIF(LTRIM(RTRIM(L.GLJELINESCREATEDBY)), ''),
            LAST_UPDATED_BY      = NULLIF(LTRIM(RTRIM(L.GLJELINESLASTUPDATEDBY)), ''),
            LAST_UPDATED_DATE    = TRY_CONVERT(DATE, L.GLJELINESLASTUPDATEDATE),
            CREATION_DATE        = TRY_CONVERT(DATE, L.GLJELINESCREATIONDATE),

            BZ_LOAD_DATE         = COALESCE(CAST(L.AddDateTime AS DATE), @AsOfDate),
            SV_LOAD_DATE         = @AsOfDate,

            CODE_COMBINATION_ID  = TRY_CONVERT(BIGINT, L.GlJeLinesCodeCombinationId)

        INTO #src
        FROM src.bzo_GL_JournalLineExtractPVO   L
        JOIN src.bzo_GL_JournalHeaderExtractPVO H
            ON H.JEHEADERID = L.JEHEADERID
        LEFT JOIN src.stage_LINES_CODE_COMBO_LOOKUP C
            ON TRY_CONVERT(BIGINT, L.GLJELINESCODECOMBINATIONID) = C.CODE_COMBINATION_BK

        /* Surrogate lookups */
        LEFT JOIN svo.D_GL_HEADER          AS DH  ON DH.JE_HEADER_ID          = L.JEHEADERID AND DH.CURR_IND = 'Y'
        LEFT JOIN svo.D_ACCOUNT            AS DA  ON DA.ACCOUNT_ID            = C.ACCOUNT_ID AND DA.CURR_IND = 'Y'
        LEFT JOIN svo.D_BUSINESS_OFFERING  AS DBO ON DBO.BUSINESS_OFFERING_ID = C.BUSINESSOFFERING_ID AND DBO.CURR_IND = 'Y'
        LEFT JOIN svo.D_COMPANY            AS DCO ON DCO.COMPANY_ID           = C.COMPANY_ID AND DCO.CURR_IND = 'Y'
        LEFT JOIN svo.D_COST_CENTER        AS DCC ON DCC.COST_CENTER_ID       = C.COSTCENTER_ID AND DCC.CURR_IND = 'Y'
        LEFT JOIN svo.D_INDUSTRY           AS DI  ON DI.INDUSTRY_ID           = C.INDUSTRY_ID AND DI.CURR_IND = 'Y'
        LEFT JOIN svo.D_INTERCOMPANY       AS DIC ON DIC.INTERCOMPANY_ID      = C.INTERCOMPANY_ID AND DIC.CURR_IND = 'Y'
        LEFT JOIN svo.D_LEDGER             AS LDG ON LDG.LEDGER_ID            = L.GlJeLinesLedgerId AND LDG.CURR_IND = 'Y'
        LEFT JOIN svo.D_CURRENCY           AS CUR ON CUR.CURRENCY_ID          = CONCAT(
                   ISNULL(L.GLJELINESCURRENCYCODE, 'UNK'),
                   CONVERT(CHAR(8), COALESCE(TRY_CONVERT(DATE, L.GLJELINESCURRENCYCONVERSIONDATE), CAST('0001-01-01' AS DATE)), 112),
                   ISNULL(LTRIM(RTRIM(L.GLJELINESCURRENCYCONVERSIONTYPE)), 'UNK')
               );

        /* Hard stop if PK fails */
        IF EXISTS (SELECT 1 FROM #src WHERE GL_LINE_PK IS NULL)
            RAISERROR('GL_LINE_PK computed NULL for one or more rows. Fix PK expression/source data.', 16, 1);

        /* De-dupe safety on PK */
        ;WITH d AS
        (
            SELECT GL_LINE_PK, rn = ROW_NUMBER() OVER (PARTITION BY GL_LINE_PK ORDER BY GL_LINE_PK)
            FROM #src
        )
        DELETE s
        FROM #src s
        JOIN d ON d.GL_LINE_PK = s.GL_LINE_PK
             AND d.rn > 1;

        BEGIN TRAN;

        IF @FullReload = 1
        BEGIN
            TRUNCATE TABLE svo.F_GL_LINES;

            INSERT INTO svo.F_GL_LINES
            (
                GL_LINE_PK, GL_HEADER_SK, ACCOUNT_SK, BUSINESS_OFFERING_SK, COMPANY_SK, COST_CENTER_SK,
                CURRENCY_SK, INDUSTRY_SK, INTERCOMPANY_SK, EFFECTIVE_DATE_SK, LEDGER_SK,
                LINE_NUM, [DESCRIPTION], ACCOUNTED_CR, ACCOUNTED_DR, AMOUNT_USD, AMOUNT_LOCAL,
                CREATED_BY, LAST_UPDATED_BY, LAST_UPDATED_DATE, CREATION_DATE, BZ_LOAD_DATE, SV_LOAD_DATE,
                CODE_COMBINATION_ID
            )
            SELECT
                GL_LINE_PK, GL_HEADER_SK, ACCOUNT_SK, BUSINESS_OFFERING_SK, COMPANY_SK, COST_CENTER_SK,
                CURRENCY_SK, INDUSTRY_SK, INTERCOMPANY_SK, EFFECTIVE_DATE_SK, LEDGER_SK,
                LINE_NUM, [DESCRIPTION], ACCOUNTED_CR, ACCOUNTED_DR, AMOUNT_USD, AMOUNT_LOCAL,
                CREATED_BY, LAST_UPDATED_BY, LAST_UPDATED_DATE, CREATION_DATE, BZ_LOAD_DATE, SV_LOAD_DATE,
                CODE_COMBINATION_ID
            FROM #src;

            SET @RowsIns = @@ROWCOUNT;
            SET @RowsUpdT1 = 0;
        END
        ELSE
        BEGIN
            DECLARE @MergeOut TABLE (ActionDesc NVARCHAR(10));

            MERGE svo.F_GL_LINES AS tgt
            USING #src AS src
            ON tgt.GL_LINE_PK = src.GL_LINE_PK
            WHEN MATCHED AND
            (
                   ISNULL(tgt.GL_HEADER_SK,0)          <> ISNULL(src.GL_HEADER_SK,0)
                OR ISNULL(tgt.ACCOUNT_SK,0)            <> ISNULL(src.ACCOUNT_SK,0)
                OR ISNULL(tgt.BUSINESS_OFFERING_SK,0)  <> ISNULL(src.BUSINESS_OFFERING_SK,0)
                OR ISNULL(tgt.COMPANY_SK,0)            <> ISNULL(src.COMPANY_SK,0)
                OR ISNULL(tgt.COST_CENTER_SK,0)        <> ISNULL(src.COST_CENTER_SK,0)
                OR ISNULL(tgt.CURRENCY_SK,0)           <> ISNULL(src.CURRENCY_SK,0)
                OR ISNULL(tgt.INDUSTRY_SK,0)           <> ISNULL(src.INDUSTRY_SK,0)
                OR ISNULL(tgt.INTERCOMPANY_SK,0)       <> ISNULL(src.INTERCOMPANY_SK,0)
                OR ISNULL(tgt.EFFECTIVE_DATE_SK,0)     <> ISNULL(src.EFFECTIVE_DATE_SK,0)
                OR ISNULL(tgt.LEDGER_SK,0)             <> ISNULL(src.LEDGER_SK,0)
                OR ISNULL(tgt.LINE_NUM,-1)             <> ISNULL(src.LINE_NUM,-1)
                OR ISNULL(tgt.[DESCRIPTION],N'')       <> ISNULL(src.[DESCRIPTION],N'')
                OR ISNULL(tgt.ACCOUNTED_CR,0)          <> ISNULL(src.ACCOUNTED_CR,0)
                OR ISNULL(tgt.ACCOUNTED_DR,0)          <> ISNULL(src.ACCOUNTED_DR,0)
                OR ISNULL(tgt.AMOUNT_USD,0)            <> ISNULL(src.AMOUNT_USD,0)
                OR ISNULL(tgt.AMOUNT_LOCAL,0)          <> ISNULL(src.AMOUNT_LOCAL,0)
                OR ISNULL(tgt.CREATED_BY,N'')          <> ISNULL(src.CREATED_BY,N'')
                OR ISNULL(tgt.LAST_UPDATED_BY,N'')     <> ISNULL(src.LAST_UPDATED_BY,N'')
                OR ISNULL(tgt.LAST_UPDATED_DATE,'0001-01-01') <> ISNULL(src.LAST_UPDATED_DATE,'0001-01-01')
                OR ISNULL(tgt.CREATION_DATE,'0001-01-01')      <> ISNULL(src.CREATION_DATE,'0001-01-01')
                OR ISNULL(tgt.CODE_COMBINATION_ID,-1)  <> ISNULL(src.CODE_COMBINATION_ID,-1)
            )
            THEN UPDATE SET
                  tgt.GL_HEADER_SK          = src.GL_HEADER_SK
                , tgt.ACCOUNT_SK            = src.ACCOUNT_SK
                , tgt.BUSINESS_OFFERING_SK  = src.BUSINESS_OFFERING_SK
                , tgt.COMPANY_SK            = src.COMPANY_SK
                , tgt.COST_CENTER_SK        = src.COST_CENTER_SK
                , tgt.CURRENCY_SK           = src.CURRENCY_SK
                , tgt.INDUSTRY_SK           = src.INDUSTRY_SK
                , tgt.INTERCOMPANY_SK       = src.INTERCOMPANY_SK
                , tgt.EFFECTIVE_DATE_SK     = src.EFFECTIVE_DATE_SK
                , tgt.LEDGER_SK             = src.LEDGER_SK
                , tgt.LINE_NUM              = src.LINE_NUM
                , tgt.[DESCRIPTION]         = src.[DESCRIPTION]
                , tgt.ACCOUNTED_CR          = src.ACCOUNTED_CR
                , tgt.ACCOUNTED_DR          = src.ACCOUNTED_DR
                , tgt.AMOUNT_USD            = src.AMOUNT_USD
                , tgt.AMOUNT_LOCAL          = src.AMOUNT_LOCAL
                , tgt.CREATED_BY            = src.CREATED_BY
                , tgt.LAST_UPDATED_BY       = src.LAST_UPDATED_BY
                , tgt.LAST_UPDATED_DATE     = src.LAST_UPDATED_DATE
                , tgt.CREATION_DATE         = src.CREATION_DATE
                , tgt.BZ_LOAD_DATE          = src.BZ_LOAD_DATE
                , tgt.SV_LOAD_DATE          = @AsOfDate
                , tgt.CODE_COMBINATION_ID   = src.CODE_COMBINATION_ID
            WHEN NOT MATCHED BY TARGET THEN
                INSERT
                (
                    GL_LINE_PK, GL_HEADER_SK, ACCOUNT_SK, BUSINESS_OFFERING_SK, COMPANY_SK, COST_CENTER_SK,
                    CURRENCY_SK, INDUSTRY_SK, INTERCOMPANY_SK, EFFECTIVE_DATE_SK, LEDGER_SK,
                    LINE_NUM, [DESCRIPTION], ACCOUNTED_CR, ACCOUNTED_DR, AMOUNT_USD, AMOUNT_LOCAL,
                    CREATED_BY, LAST_UPDATED_BY, LAST_UPDATED_DATE, CREATION_DATE, BZ_LOAD_DATE, SV_LOAD_DATE,
                    CODE_COMBINATION_ID
                )
                VALUES
                (
                    src.GL_LINE_PK, src.GL_HEADER_SK, src.ACCOUNT_SK, src.BUSINESS_OFFERING_SK, src.COMPANY_SK, src.COST_CENTER_SK,
                    src.CURRENCY_SK, src.INDUSTRY_SK, src.INTERCOMPANY_SK, src.EFFECTIVE_DATE_SK, src.LEDGER_SK,
                    src.LINE_NUM, src.[DESCRIPTION], src.ACCOUNTED_CR, src.ACCOUNTED_DR, src.AMOUNT_USD, src.AMOUNT_LOCAL,
                    src.CREATED_BY, src.LAST_UPDATED_BY, src.LAST_UPDATED_DATE, src.CREATION_DATE, src.BZ_LOAD_DATE, @AsOfDate,
                    src.CODE_COMBINATION_ID
                )
            OUTPUT $action INTO @MergeOut(ActionDesc);

            SELECT
                @RowsIns = SUM(CASE WHEN ActionDesc = 'INSERT' THEN 1 ELSE 0 END),
                @RowsUpdT1 = SUM(CASE WHEN ActionDesc = 'UPDATE' THEN 1 ELSE 0 END)
            FROM @MergeOut;
        END

        COMMIT;

        UPDATE etl.ETL_RUN
            SET END_DTTM = SYSDATETIME(),
                STATUS = 'SUCCESS',
                ROW_INSERTED = @RowsIns,
                ROW_UPDATED_T1 = @RowsUpdT1,
                ROW_EXPIRED = 0
        WHERE RUN_ID = @RunId;

    END TRY
    BEGIN CATCH
        IF XACT_STATE() <> 0 ROLLBACK;

        DECLARE @ErrMsg NVARCHAR(4000) = ERROR_MESSAGE();

        UPDATE etl.ETL_RUN
            SET END_DTTM = SYSDATETIME(),
                STATUS = 'FAILED',
                ERROR_MESSAGE = LEFT(@ErrMsg, 4000)
        WHERE RUN_ID = @RunId;

        RAISERROR('%s', 16, 1, @ErrMsg);
        RETURN;
    END CATCH
END
GO

/* Run:
EXEC svo.usp_Load_F_GL_LINES_T1 @FullReload = 1;  -- initial load
EXEC svo.usp_Load_F_GL_LINES_T1 @FullReload = 0;  -- incremental merge
*/
