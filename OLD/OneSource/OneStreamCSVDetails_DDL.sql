USE [DW_BronzeSilver_PROD]
GO
/****** Object:  Table [dbo].[OneStreamCSVDetails]    Script Date: 2/4/2026 5:01:28 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[OneStreamCSVDetails](
	[Cube] [varchar](100) NOT NULL,
	[Entity] [varchar](100) NOT NULL,
	[Parent] [varchar](100) NULL,
	[Cons] [varchar](100) NOT NULL,
	[Scenario] [varchar](100) NOT NULL,
	[Time] [varchar](100) NOT NULL,
	[View] [varchar](100) NOT NULL,
	[Account] [varchar](100) NOT NULL,
	[Flow] [varchar](100) NOT NULL,
	[Origin] [varchar](100) NOT NULL,
	[IC] [varchar](100) NOT NULL,
	[UD1] [varchar](100) NOT NULL,
	[UD2] [varchar](100) NOT NULL,
	[UD3] [varchar](100) NOT NULL,
	[UD4] [varchar](100) NOT NULL,
	[UD5] [varchar](100) NOT NULL,
	[UD6] [varchar](100) NOT NULL,
	[UD7] [varchar](100) NOT NULL,
	[UD8] [varchar](100) NOT NULL,
	[Amount] [varchar](100) NOT NULL,
	[HasData] [varchar](100) NOT NULL,
	[Annotation] [varchar](100) NULL,
	[Assumptions] [varchar](100) NULL,
	[AuditComment] [varchar](100) NULL,
	[Footnote] [varchar](100) NULL,
	[VarianceExplanation] [varchar](100) NULL
) ON [PRIMARY]
GO
