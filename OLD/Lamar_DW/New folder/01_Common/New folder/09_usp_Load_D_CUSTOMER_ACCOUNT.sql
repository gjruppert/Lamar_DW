CREATE OR ALTER PROCEDURE svo.usp_Load_D_CUSTOMER_ACCOUNT
      @FullReload bit = 0
    , @Debug      bit = 0
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    DECLARE @proc  sysname      = QUOTENAME(OBJECT_SCHEMA_NAME(@@PROCID)) + '.' + QUOTENAME(OBJECT_NAME(@@PROCID));
    DECLARE @start datetime2(3) = SYSDATETIME();

    BEGIN TRY
        IF @Debug = 1 PRINT 'Starting ' + @proc;

        IF @FullReload = 1
        BEGIN
            IF @Debug = 1 PRINT 'FullReload requested - truncating svo.D_CUSTOMER_ACCOUNT';
            TRUNCATE TABLE svo.D_CUSTOMER_ACCOUNT;
        END

        /* Plug row (SK=0) */
        IF NOT EXISTS (SELECT 1 FROM svo.D_CUSTOMER_ACCOUNT WHERE CUSTOMER_SK = 0)
        BEGIN
            SET IDENTITY_INSERT svo.D_CUSTOMER_ACCOUNT ON;

            INSERT INTO svo.D_CUSTOMER_ACCOUNT
            (
                  CUSTOMER_SK
                , CUSTOMER_ACCOUNT_ID
                , ACCOUNT_NUMBER
                , ACCOUNT_NAME
                , STATUS_CODE
                , CUSTOMER_TYPE
                , PARTY_ID
                , BZ_LOAD_DATE
                , SV_LOAD_DATE
            )
            VALUES
            (
                  0
                , -1
                , 'UNKNOWN'
                , 'UNKNOWN CUSTOMER'
                , 'UNKNOWN'
                , 'UNKNOWN'
                , NULL
                , CAST('0001-01-01' AS date)
                , CAST('0001-01-01' AS date)
            );

            SET IDENTITY_INSERT svo.D_CUSTOMER_ACCOUNT OFF;
        END

        /* Stage the latest row per CustomerAccountId (in case source has dupes) */
        ;WITH SrcBase AS
        (
            SELECT
                  A.CustAccountId  AS CUSTOMER_ACCOUNT_ID
                , A.AccountNumber  AS ACCOUNT_NUMBER
                , A.AccountName    AS ACCOUNT_NAME
                , A.Status         AS STATUS_CODE
                , A.CustomerType   AS CUSTOMER_TYPE
                , A.PartyId        AS PARTY_ID
                , CAST(A.AddDateTime AS date) AS BZ_LOAD_DATE
                , CAST(GETDATE() AS date)     AS SV_LOAD_DATE
                , rn = ROW_NUMBER() OVER
                    (
                      PARTITION BY A.CustAccountId
                      ORDER BY TRY_CONVERT(datetime2(7), A.AddDateTime) DESC
                    )
            FROM src.bzo_AR_CustomerAccountExtractPVO A
            WHERE A.CustAccountId IS NOT NULL
        ),
        Src AS
        (
            SELECT
                  CUSTOMER_ACCOUNT_ID
                , ACCOUNT_NUMBER
                , ACCOUNT_NAME
                , STATUS_CODE
                , CUSTOMER_TYPE
                , PARTY_ID
                , BZ_LOAD_DATE
                , SV_LOAD_DATE
            FROM SrcBase
            WHERE rn = 1
        )
        /* Type 1 update (only when not full reload) */
        UPDATE T
            SET
                  T.ACCOUNT_NUMBER      = S.ACCOUNT_NUMBER
                , T.ACCOUNT_NAME        = S.ACCOUNT_NAME
                , T.STATUS_CODE         = S.STATUS_CODE
                , T.CUSTOMER_TYPE       = S.CUSTOMER_TYPE
                , T.PARTY_ID            = S.PARTY_ID
                , T.BZ_LOAD_DATE        = S.BZ_LOAD_DATE
                , T.SV_LOAD_DATE        = S.SV_LOAD_DATE
        FROM svo.D_CUSTOMER_ACCOUNT T
        JOIN Src S
          ON S.CUSTOMER_ACCOUNT_ID = T.CUSTOMER_ACCOUNT_ID
        WHERE @FullReload = 0
          AND T.CUSTOMER_SK <> 0
          AND (
                 ISNULL(T.ACCOUNT_NUMBER,'') <> ISNULL(S.ACCOUNT_NUMBER,'')
              OR ISNULL(T.ACCOUNT_NAME,'')   <> ISNULL(S.ACCOUNT_NAME,'')
              OR ISNULL(T.STATUS_CODE,'')    <> ISNULL(S.STATUS_CODE,'')
              OR ISNULL(T.CUSTOMER_TYPE,'')  <> ISNULL(S.CUSTOMER_TYPE,'')
              OR ISNULL(T.PARTY_ID,'')       <> ISNULL(S.PARTY_ID,'')
              OR ISNULL(T.BZ_LOAD_DATE,'0001-01-01') <> ISNULL(S.BZ_LOAD_DATE,'0001-01-01')
          );

        /* Inserts */
        INSERT INTO svo.D_CUSTOMER_ACCOUNT
        (
              CUSTOMER_ACCOUNT_ID
            , ACCOUNT_NUMBER
            , ACCOUNT_NAME
            , STATUS_CODE
            , CUSTOMER_TYPE
            , PARTY_ID
            , BZ_LOAD_DATE
            , SV_LOAD_DATE
        )
        SELECT
              S.CUSTOMER_ACCOUNT_ID
            , S.ACCOUNT_NUMBER
            , S.ACCOUNT_NAME
            , S.STATUS_CODE
            , S.CUSTOMER_TYPE
            , S.PARTY_ID
            , S.BZ_LOAD_DATE
            , S.SV_LOAD_DATE
        FROM Src S
        WHERE NOT EXISTS
        (
            SELECT 1
            FROM svo.D_CUSTOMER_ACCOUNT T
            WHERE T.CUSTOMER_ACCOUNT_ID = S.CUSTOMER_ACCOUNT_ID
        );

        /* Unique index on BK */
        IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'UX_D_CUSTOMER_ACCOUNT_ID' AND object_id = OBJECT_ID('svo.D_CUSTOMER_ACCOUNT'))
        BEGIN
            CREATE UNIQUE NONCLUSTERED INDEX UX_D_CUSTOMER_ACCOUNT_ID
                ON svo.D_CUSTOMER_ACCOUNT (CUSTOMER_ACCOUNT_ID)
                ON FG_SilverDim;
        END

        IF @Debug = 1
        BEGIN
            DECLARE @rows bigint = (SELECT COUNT_BIG(*) FROM svo.D_CUSTOMER_ACCOUNT);
            PRINT 'Completed ' + @proc
                + ' | rows=' + CONVERT(varchar(30), @rows)
                + ' | ms=' + CONVERT(varchar(30), DATEDIFF(millisecond, @start, SYSDATETIME()));
        END
    END TRY
    BEGIN CATCH
        DECLARE @ErrNum int = ERROR_NUMBER();
        DECLARE @ErrSev int = ERROR_SEVERITY();
        DECLARE @ErrSta int = ERROR_STATE();
        DECLARE @ErrLin int = ERROR_LINE();
        DECLARE @ErrMsg nvarchar(4000) = ERROR_MESSAGE();

        IF @Debug = 1
            PRINT 'FAILED ' + @proc
                + ' | line=' + CONVERT(varchar(12), @ErrLin)
                + ' | err=' + CONVERT(varchar(12), @ErrNum)
                + ' | msg=' + @ErrMsg;

        RAISERROR('%s failed. Line %d. Error %d: %s', @ErrSev, @ErrSta, @proc, @ErrLin, @ErrNum, @ErrMsg);
        RETURN;
    END CATCH
END
GO


