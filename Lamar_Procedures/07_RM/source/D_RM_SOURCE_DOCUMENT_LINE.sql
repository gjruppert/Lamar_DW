IF OBJECT_ID('svo.D_RM_SOURCE_DOCUMENT_LINE', 'U') IS NOT NULL
    DROP TABLE svo.D_RM_SOURCE_DOCUMENT_LINE;
GO

SET ANSI_NULLS ON;
GO
SET QUOTED_IDENTIFIER ON;
GO

CREATE TABLE [svo].[D_RM_SOURCE_DOCUMENT_LINE]
(
    RM_SOURCE_DOCUMENT_LINE_SK      BIGINT IDENTITY(1,1) NOT NULL,

    SOURCE_DOCUMENT_LINE_ID         BIGINT        NOT NULL,   -- SourceDocLinesDocumentLineId

    LINE_CREATED_BY                 VARCHAR(64)   NOT NULL,   -- SourceDocLinesCreatedBy
    LINE_LAST_UPDATED_BY            VARCHAR(64)   NOT NULL,   -- SourceDocLinesLastUpdatedBy
    LINE_LAST_UPDATE_LOGIN          VARCHAR(32)   NULL,       -- SourceDocLinesLastUpdateLogin

    DOC_CREATED_BY                  VARCHAR(64)   NOT NULL,   -- SourceDocumentsCreatedBy
    DOCUMENT_NUMBER                 VARCHAR(300)  NOT NULL,   -- SourceDocumentsDocumentNumber
    DOC_LAST_UPDATE_DATE            DATE          NOT NULL,   -- from SourceDocumentsLastUpdateDate
    DOC_LAST_UPDATE_LOGIN           VARCHAR(32)   NULL,       -- SourceDocumentsLastUpdateLogin
    ORG_ID                          BIGINT        NULL,       -- SourceDocumentsOrgId
    ORDER_FULFILL_LINE_ID                BIGINT        NULL,       -- SourceDocLinesDocLineIdInt1

    BZ_LOAD_DATE                    DATE          NOT NULL,
    SV_LOAD_DATE                    DATE          NOT NULL,

    CONSTRAINT PK_D_RM_SOURCE_DOCUMENT_LINE
        PRIMARY KEY CLUSTERED (RM_SOURCE_DOCUMENT_LINE_SK ASC)
) ON [FG_SilverDim];
GO

CREATE UNIQUE NONCLUSTERED INDEX UX_D_RM_SOURCE_DOCUMENT_LINE_ID
ON [svo].[D_RM_SOURCE_DOCUMENT_LINE] (SOURCE_DOCUMENT_LINE_ID)
ON [FG_SilverDim];
GO

-- Plug row
SET IDENTITY_INSERT svo.D_RM_SOURCE_DOCUMENT_LINE ON;

INSERT INTO svo.D_RM_SOURCE_DOCUMENT_LINE
(
    RM_SOURCE_DOCUMENT_LINE_SK,
    SOURCE_DOCUMENT_LINE_ID,
    LINE_CREATED_BY,
    LINE_LAST_UPDATED_BY,
    LINE_LAST_UPDATE_LOGIN,
    DOC_CREATED_BY,
    DOCUMENT_NUMBER,
    DOC_LAST_UPDATE_DATE,
    DOC_LAST_UPDATE_LOGIN,
    ORG_ID,
    ORDER_FULFILL_LINE_ID,
    BZ_LOAD_DATE,
    SV_LOAD_DATE
)
VALUES
(
    0,
    0,
    'UNKNOWN',
    'UNKNOWN',
    'UNKNOWN',
    'UNKNOWN',
    'UNKNOWN',
    '1900-01-01',
    'UNKNOWN',
    0,
    -1,
    CAST(GETDATE() AS DATE),
    CAST(GETDATE() AS DATE)
);

SET IDENTITY_INSERT svo.D_RM_SOURCE_DOCUMENT_LINE OFF;
GO

INSERT INTO svo.D_RM_SOURCE_DOCUMENT_LINE
(
    SOURCE_DOCUMENT_LINE_ID,
    LINE_CREATED_BY,
    LINE_LAST_UPDATED_BY,
    LINE_LAST_UPDATE_LOGIN,
    DOC_CREATED_BY,
    DOCUMENT_NUMBER,
    DOC_LAST_UPDATE_DATE,
    DOC_LAST_UPDATE_LOGIN,
    ORG_ID,
    ORDER_FULFILL_LINE_ID,
    BZ_LOAD_DATE,
    SV_LOAD_DATE
)
SELECT
    S.SourceDocLinesDocumentLineId          AS SOURCE_DOCUMENT_LINE_ID,
    S.SourceDocLinesCreatedBy               AS LINE_CREATED_BY,
    S.SourceDocLinesLastUpdatedBy           AS LINE_LAST_UPDATED_BY,
    S.SourceDocLinesLastUpdateLogin         AS LINE_LAST_UPDATE_LOGIN,
    S.SourceDocumentsCreatedBy              AS DOC_CREATED_BY,
    S.SourceDocumentsDocumentNumber         AS DOCUMENT_NUMBER,
    CAST(S.SourceDocumentsLastUpdateDate AS DATE) AS DOC_LAST_UPDATE_DATE,
    S.SourceDocumentsLastUpdateLogin        AS DOC_LAST_UPDATE_LOGIN,
    S.SourceDocumentsOrgId                  AS ORG_ID,
    S.SourceDocLinesDocLineIdInt1          AS ORDER_FULFILL_LINE_ID,
    CAST(S.AddDateTime AS DATE)             AS BZ_LOAD_DATE,
    CAST(GETDATE() AS DATE)                 AS SV_LOAD_DATE
FROM bzo.VRM_SourceDocumentLinesPVO AS S;
GO