USE SMI_DB_Reporting_JKT
GO

/****** Object:  Table [dbo].[SMI_Data_Penggunaan_Voucher]    Script Date: 10/2/2025 1:32:27 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [dbo].[SMI_Data_Penggunaan_Voucher](
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


------------------------------------------------------



-- public.smi_sales_voucher_cab definition

-- Drop table

-- DROP TABLE public.smi_sales_voucher_cab;

CREATE TABLE public.smi_sales_voucher_cab (
	insertdate timestamptz DEFAULT now() NOT NULL,
	namacabang varchar(40) NULL,
	kodetoko numeric NULL,
	namatoko varchar(50) NULL,
	tglbayar timestamp NULL,
	nomortransaksi numeric NULL,
	nopolisi varchar(14) NULL,
	namamember varchar(50) NULL,
	notelp varchar(15) NULL,
	namajenismember varchar(50) NULL,
	namagroupvoucher varchar(50) NULL,
	namamarchant varchar(50) NULL,
	nomorserivoucher varchar(20) NULL,
	kodecarabayar varchar(3) NULL,
	nominalvoucher numeric(20, 2) NULL,
	qty numeric(20, 2) NULL,
	totalrppenjualan numeric(20, 2) NULL
);

