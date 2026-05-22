USE [MB_DB_Reporting_JKT]
GO

/****** Object:  Table [dbo].[MB_Data_Penggunaan_Voucher]    Script Date: 4/14/2026 9:58:27 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [dbo].[MB_Data_Penggunaan_Voucher](
	[NamaCabang] [varchar](40) NULL,
	[kodeToko] [bigint] NULL,
	[namaToko] [varchar](100) NULL,
	[tglBayar] [smalldatetime] NULL,
	[nomorTransaksi] [bigint] NULL,
	[NoPolisi] [varchar](15) NULL,
	[namaMember] [varchar](30) NULL,
	[NoTelp] [varchar](15) NULL,
	[namaJenisMember] [varchar](30) NULL,
	[namaGroupVoucher] [varchar](50) NULL,
	[namaMarchant] [varchar](50) NULL,
	[NomorSeriVoucher] [varchar](20) NULL,
	[KodeCaraBayar] [varchar](3) NULL,
	[nominalVoucher] [decimal](18, 2) NULL,
	[totalQty] [decimal](18, 2) NULL,
	[totalRpPenjualan] [decimal](18, 2) NULL
) ON [PRIMARY]
GO


