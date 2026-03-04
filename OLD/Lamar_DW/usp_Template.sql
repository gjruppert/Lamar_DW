/*
    Standard SCD-2 loader template for your SVO dimensions using:
      EFF_DATE, END_DATE, CRE_DATE, UDT_DATE, CURR_IND
    and your warehouse load-date rule:
      BZ_LOAD_DATE = CAST(AddDateTime AS date)
      SV_LOAD_DATE = CAST(GETDATE() AS date)

    Pattern:
      1) Close out CURRENT rows that changed (set END_DATE, CURR_IND='N', UDT_DATE)
      2) Insert NEW BKs and NEW VERSIONS for changed BKs (EFF_DATE=@AsOfDate, END_DATE=9999-12-31, CURR_IND='Y')

    Notes:
      - Replace: <TARGET_TABLE>, <SOURCE_OBJECT>, <BK_COLUMN>, and the <ATTR_COLUMN_LIST>.
      - Source must provide AddDateTime plus BK + attributes.
      - This avoids doing “update + insert” in a single MERGE (which is where MERGE gets messy).
*/

CREATE OR ALTER PROCEDURE dbo.usp_SCD2_Load__<TARGET_TABLE>
(
    @AsOfDate date = NULL   -- if NULL, uses today (ET)
)
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    IF @AsOfDate IS NULL
        SET @AsOfDate = CAST(GETDATE() AS date);

    DECLARE @HighDate date = '9999-12-31';

    BEGIN TRY
        BEGIN TRAN;

        /* ==========================================
           0) Source shaping + change detection hash
           ========================================== */

        ;WITH src AS
        (
            SELECT
                s.<BK_COLUMN>                                            AS BK,
                CAST(s.AddDateTime AS date)                               AS BZ_LOAD_DATE,
                CAST(GETDATE() AS date)                                   AS SV_LOAD_DATE,

                /* ===== Attributes to store on the dimension =====
                   Include every SCD-tracked attribute column here.
                   (Do NOT include SK, EFF/END/CRE/UDT/CURR, load dates)
                */
                s.<ATTR1> ,
                s.<ATTR2> ,
                s.<ATTR3> ,
                s.<ATTRN> ,

                /* Hash for fast compare (normalize NULLs to stable tokens) */
                HASHBYTES('SHA2_256',
                    CONCAT(
                        COALESCE(CONVERT(nvarchar(4000), s.<ATTR1>), N'<NULL>'), N'|',
                        COALESCE(CONVERT(nvarchar(4000), s.<ATTR2>), N'<NULL>'), N'|',
                        COALESCE(CONVERT(nvarchar(4000), s.<ATTR3>), N'<NULL>'), N'|',
                        COALESCE(CONVERT(nvarchar(4000), s.<ATTRN>), N'<NULL>')
                    )
                ) AS SRC_HASH
            FROM <SOURCE_OBJECT> s
            WHERE s.<BK_COLUMN> IS NOT NULL
        ),
        tgt_curr AS
        (
            SELECT
                t.<BK_COLUMN> AS BK,

                HASHBYTES('SHA2_256',
                    CONCAT(
                        COALESCE(CONVERT(nvarchar(4000), t.<ATTR1>), N'<NULL>'), N'|',
                        COALESCE(CONVERT(nvarchar(4000), t.<ATTR2>), N'<NULL>'), N'|',
                        COALESCE(CONVERT(nvarchar(4000), t.<ATTR3>), N'<NULL>'), N'|',
                        COALESCE(CONVERT(nvarchar(4000), t.<ATTRN>), N'<NULL>')
                    )
                ) AS TGT_HASH
            FROM <TARGET_TABLE> t
            WHERE t.CURR_IND = 'Y'
              AND t.END_DATE = @HighDate
        ),
        delta AS
        (
            SELECT
                s.*
            FROM src s
            LEFT JOIN tgt_curr tc
                ON tc.BK = s.BK
            WHERE
                tc.BK IS NULL            -- brand new BK
                OR tc.TGT_HASH <> s.SRC_HASH  -- changed attributes
        )

        /* ==========================================
           1) Close out changed CURRENT rows
           ========================================== */

        UPDATE t
            SET
                t.END_DATE     = DATEADD(day, -1, @AsOfDate),
                t.CURR_IND     = 'N',
                t.UDT_DATE     = SYSDATETIME(),
                t.SV_LOAD_DATE = CAST(GETDATE() AS date)
        FROM <TARGET_TABLE> t
        INNER JOIN delta d
            ON d.BK = t.<BK_COLUMN>
        WHERE t.CURR_IND = 'Y'
          AND t.END_DATE = @HighDate
          -- close only when it actually existed and changed (not brand-new BK)
          AND EXISTS (SELECT 1 FROM tgt_curr tc WHERE tc.BK = d.BK);

        /* ==========================================
           2) Insert new rows (new BKs + new versions)
           ========================================== */

        INSERT INTO <TARGET_TABLE>
        (
            <BK_COLUMN>,

            /* Attributes */
            <ATTR1>,
            <ATTR2>,
            <ATTR3>,
            <ATTRN>,

            /* SCD fields */
            EFF_DATE,
            END_DATE,
            CRE_DATE,
            UDT_DATE,
            CURR_IND,

            /* Load dates */
            BZ_LOAD_DATE,
            SV_LOAD_DATE
        )
        SELECT
            d.BK,

            d.<ATTR1>,
            d.<ATTR2>,
            d.<ATTR3>,
            d.<ATTRN>,

            @AsOfDate             AS EFF_DATE,
            @HighDate             AS END_DATE,
            SYSDATETIME()         AS CRE_DATE,
            SYSDATETIME()         AS UDT_DATE,
            'Y'                   AS CURR_IND,

            d.BZ_LOAD_DATE,
            d.SV_LOAD_DATE
        FROM delta d;

        COMMIT;
    END TRY
    BEGIN CATCH
        IF @@TRANCOUNT > 0 ROLLBACK;

        DECLARE
            @ErrMsg  nvarchar(4000) = ERROR_MESSAGE(),
            @ErrNum  int            = ERROR_NUMBER(),
            @ErrSev  int            = ERROR_SEVERITY(),
            @ErrSta  int            = ERROR_STATE(),
            @ErrLin  int            = ERROR_LINE(),
            @ErrProc nvarchar(200)  = ERROR_PROCEDURE();

        RAISERROR(
            N'SCD2 load failed. Proc=%s Line=%d Err=%d State=%d Msg=%s',
            @ErrSev, 1,
            @ErrProc, @ErrLin, @ErrNum, @ErrSta, @ErrMsg
        );
        RETURN;
    END CATCH
END
GO
