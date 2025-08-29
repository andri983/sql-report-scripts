=======
--RMS--
=======

USE [SMI_DB_Reporting_SBY]
GO

/****** Object:  Table [dbo].[[SMI_Data_Penggunaan_Voucher_Detail]]    Script Date: 7/14/2025 9:28:09 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [dbo].[SMI_Data_Penggunaan_Voucher_Detail](
	[idcabang] [int] NULL,
	[kodeToko] [bigint] NULL,
	[namaToko] [varchar](100) NULL,
	[tglBayar] [smalldatetime] NULL,
	[nomorTransaksi] [bigint] NULL,
	[NoPolisi] [varchar](15) NULL,
	[namaMember] [varchar](30) NULL,
	[NoTelp] [varchar](15) NULL,
	[namaJenisMember] [varchar](30) NULL,
	[NomorSeriVoucher] [varchar](20) NULL,
	[nominalVoucher] [decimal](18, 2) NULL,
	[kodeProduk] [varchar](10) NULL,
	[namaPendek] [varchar](30) NULL,
	[Qty] [decimal](18, 2) NULL,
	[totalRpPenjualan] [decimal](18, 2) NULL
) ON [PRIMARY]
GO


*************
-----S P-----
*************

USE [SMI_DB_Reporting_SBY]
GO

/****** Object:  StoredProcedure [dbo].[spDataPenggunaanVoucherDetail]    Script Date: 7/3/2025 11:26:56 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE PROCEDURE [dbo].[spDataPenggunaanVoucherDetail] (@date1 date,@date2 date,@idcabang int)
as
begin

	---Start Query Header Voucher
	insert into SMI_Data_Penggunaan_Voucher_Detail
	select e.idcabang,a.kodetoko,namaToko,tglBayar,a.nomorTransaksi,g.NoPolisi,namaMember,NoTelp,namaJenisMember,c.NomorSeriVoucher,c.nominalVoucher,i.kodeProduk,i.namaPendek,i.qty,i.subTotal as totalRpPenjualan --,totalQty,totalRpPenjualan 
	from PB_DC.dbo.transaksitokoHeader a
			join PB_DC.dbo.TransaksiTokoPembayaranPerCaraBayarNonTunaiVoucher c on c.kodetoko=a.kodetoko and c.nomorTransaksi=a.nomorTransaksi
					join PB_DC.dbo.mstToko f on f.kodetoko=a.kodetoko and f.kodetoko=c.kodetoko
					join PB_DC.dbo.masterToolsToko e on e.kodetoko=a.kodetoko and e.kodetoko=c.kodetoko and e.kodetoko=f.kodetoko
						join PB_DC.dbo.SMITransaksiTokoPerjenisMember g on g.kodetoko=a.kodetoko and g.kodetoko=c.kodetoko and g.kodetoko=f.kodetoko and g.kodetoko=e.kodetoko  
							and g.nomorTransaksi=a.nomorTransaksi and g.nomorTransaksi=c.nomorTransaksi
							--join master Member	
								join PB_DC.dbo.mstMember b on b.noPolisi=g.noPolisi
								join PB_DC.dbo.mstJenisMember d on d.idjenisMember=g.idjenisMember
							--join untuk mencari marchant
								join PB_DC.dbo.SMIMstVoucher h on h.nomorSerivoucher=c.nomorSerivoucher
							--join transaksi detail
								join PB_DC.dbo.TransaksiTokoDetail i on i.kodetoko=a.kodetoko and i.nomorTransaksi=a.nomorTransaksi					
	where convert(date,tglBayar) between @date1 and @date2 and idcabang=@idcabang
	--where convert(date,tglBayar) between '2025-06-01' and '2025-06-30' and idcabang=5
	and i.idjenisproduk<>4
	and i.statusproduk<>'K'
	--and a.kodetoko=3051002 and a.nomorTransaksi=202506120008
	union all
	select e.idcabang,a.kodetoko,namaToko,tglBayar,a.nomorTransaksi,g.NoPolisi,namaMember,NoTelp,namaJenisMember,c.NomorSeriVoucher,c.nominalVoucher,i.kodeProduk,i.namaPendek,i.qty,i.subTotal as totalRpPenjualan --,totalQty,totalRpPenjualan 
	from PB_DC.dbo.transaksitokoHeader a
			join PB_DC.dbo.TransaksiTokoPembayaranPerCaraBayarNonTunaiVoucher c on c.kodetoko=a.kodetoko and c.nomorTransaksi=a.nomorTransaksi
					join PB_DC.dbo.mstToko f on f.kodetoko=a.kodetoko and f.kodetoko=c.kodetoko
					join PB_DC.dbo.masterToolsToko e on e.kodetoko=a.kodetoko and e.kodetoko=c.kodetoko and e.kodetoko=f.kodetoko
						join PB_DC.dbo.SMITransaksiTokoPerjenisMember g on g.kodetoko=a.kodetoko and g.kodetoko=c.kodetoko and g.kodetoko=f.kodetoko 
						and g.kodetoko=e.kodetoko  
							and g.nomorTransaksi=a.nomorTransaksi and g.nomorTransaksi=c.nomorTransaksi
							--join master Member	
								join PB_DC.dbo.mstMember b on b.noPolisi=g.noPolisi
								join PB_DC.dbo.mstJenisMember d on d.idjenisMember=g.idjenisMember
							--join untuk mencari marchant
								join PB_DC.dbo.SMIMstVoucherDiskon h on h.nomorSerivoucher=c.nomorSerivoucher
							--join transaksi detail
								join PB_DC.dbo.TransaksiTokoDetail i on i.kodetoko=a.kodetoko and i.nomorTransaksi=a.nomorTransaksi									
	where convert(date,tglBayar) between @date1 and @date2 and idcabang=@idcabang
	--where convert(date,tglBayar) between '2025-06-01' and '2025-06-30' and idcabang=5
	and i.idjenisproduk<>4
	and i.statusproduk<>'K'
	--and a.kodetoko=3051002 and a.nomorTransaksi=202506120008
	order by kodetoko,tglBayar
	---End Query
	
end;
GO

--select a.* from transaksitokodetail a where a.kodetoko=3051002 and a.nomorTransaksi=202506120008
--select * from [SMI_DB_Reporting_SBY].dbo.SMI_Data_Penggunaan_Voucher_Detail


*************
--JOB AGENT--
*************

SMI_Data_Penggunaan_Voucher_Detail

truncate table [SMI_Data_Penggunaan_Voucher_Detail]

truncate table [SMI_Data_Penggunaan_Voucher_Detail];

exec spDataPenggunaanVoucherDetail

USE SMI_DB_Reporting_SBY;
DECLARE @date1 date
DECLARE @date2 date
DECLARE @idcabang int=5

declare @tglHariIni int

set @tglHariIni=DAY(CURRENT_TIMESTAMP)
		if @tglHariIni>1 ---Data yg diambil dari tgl 1 - H-1
			begin
				set @date1 = convert(date,DATEADD(mm, DATEDIFF(mm, 0, GETDATE()), 0))
				set @date2 = convert(date,getdate()-1)
			end
		else             ---Data yang diambil 1 bulan full, Month-1 (Contoh : Jika hari ini tgl 1 februari, maka yg diambil full 1 bulan januari)
			begin
				set @date1 =  convert(date,DATEADD(MONTH, DATEDIFF(MONTH, 0, GETDATE())-1, 0))
				set @date2 =  convert(date,dateadd(d,-(day(getdate())),getdate()))
			end
exec spDataPenggunaanVoucherDetail @date1,@date2,@idcabang;




==============
--POSTGRESQL--
==============

CREATE TABLE public.smi_sales_voucher_cab_detail(
	insertdate timestamptz DEFAULT now() NOT NULL,
	idcabang int NULL,
	kodetoko bigint NULL,
	namatoko varchar(100) NULL,
	tglbayar timestamp NULL,
	nomortransaksi bigint NULL,
	nopolisi varchar(15) NULL,
	namamember varchar(30) NULL,
	notelp varchar(15) NULL,
	namajenismember varchar(30) NULL,
	nomorserivoucher varchar(20) NULL,
	nominalnoucher decimal(18, 2) NULL,
	kodeproduk varchar(10) NULL,
	namapendek varchar(30) NULL,
	qty decimal(18, 2) NULL,
	totalRpPenjualan decimal(18, 2) NULL)
	
	
--select * from public.smi_sales_voucher_cab;