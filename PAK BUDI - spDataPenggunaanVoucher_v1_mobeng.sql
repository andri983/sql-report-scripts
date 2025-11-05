USE MB_DB_Reporting_JKT
GO

/****** Object:  StoredProcedure [dbo].[spDataPenggunaanVoucher]    Script Date: 10/28/2025 10:33:39 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE PROCEDURE [dbo].[spDataPenggunaanVoucher] (@date1 date,@date2 date,@idcabang int)
as
begin

	--Start Query Header Voucher
	INSERT INTO MB_DB_Reporting_JKT.DBO.MB_Data_Penggunaan_Voucher
	SELECT j.NamaCabang,a.kodetoko,namaToko,tglBayar,a.nomorTransaksi,g.NoPolisi,namaMember,NoTelp,namaJenisMember,h.namaGroupVoucher,i.namaMarchant,c.NomorSeriVoucher as kodeVoucher,c.kodeCaraBayar,c.nominalVoucher,totalQty,totalRpPenjualan 
	FROM MB_DC.DBO.transaksitokoHeader a
			JOIN MB_DC.DBO.TransaksiTokoPembayaranPerCaraBayarNonTunaiVoucher c on c.kodetoko=a.kodetoko and c.nomorTransaksi=a.nomorTransaksi
					JOIN MB_DC.DBO.mstToko f on f.kodetoko=a.kodetoko and f.kodetoko=c.kodetoko
					JOIN MB_DC.DBO.masterToolsToko e on e.kodetoko=a.kodetoko and e.kodetoko=c.kodetoko and e.kodetoko=f.kodetoko
						JOIN MB_DC.DBO.SMITransaksiTokoPerjenisMember g on g.kodetoko=a.kodetoko and g.kodetoko=c.kodetoko and g.kodetoko=f.kodetoko and g.kodetoko=e.kodetoko  
							and g.nomorTransaksi=a.nomorTransaksi and g.nomorTransaksi=c.nomorTransaksi
							--JOIN master Member	
							JOIN MB_DC.DBO.mstMember b 
								ON b.noPolisi=g.noPolisi
							JOIN MB_DC.DBO.mstJenisMember d 
								ON d.idjenisMember=g.idjenisMember
							--JOIN untuk mencari group
							LEFT JOIN MB_DC.DBO.smimstgroupvoucherheader h 
								ON h.idGroupVoucher=c.idGroupVoucher
							--JOIN untuk mencari marchant
							LEFT JOIN MB_DC.DBO.MstMarchantVoucher i 
								ON c.idMarchant = i.idMarchant 
							LEFT JOIN MB_DC.DBO.MstCabang j
								ON e.idcabang = j.idCabang
	--WHERE CONVERT(date,tglBayar) between '2025-10-01' and '2025-10-01' and e.idcabang=2
	WHERE CONVERT(date,tglBayar) between @date1 and @date2 and e.idcabang=@idcabang
	and c.kodecarabayar='VOU' and substring(c.nomorserivoucher,1,1) in ('1','3')

	UNION ALL

	SELECT j.NamaCabang,a.kodetoko,namaToko,tglBayar,a.nomorTransaksi,g.NoPolisi,namaMember,NoTelp,namaJenisMember,h.namaGroupVoucher,i.namaMarchant,c.NomorSeriVoucher as kodeVoucher,c.kodeCaraBayar,c.nominalVoucher,totalQty,totalRpPenjualan 
	FROM MB_DC.DBO.transaksitokoHeader a
			JOIN MB_DC.DBO.TransaksiTokoPembayaranPerCaraBayarNonTunaiVoucher c on c.kodetoko=a.kodetoko and c.nomorTransaksi=a.nomorTransaksi
					JOIN MB_DC.DBO.mstToko f on f.kodetoko=a.kodetoko and f.kodetoko=c.kodetoko
					JOIN MB_DC.DBO.masterToolsToko e on e.kodetoko=a.kodetoko and e.kodetoko=c.kodetoko and e.kodetoko=f.kodetoko
						JOIN MB_DC.DBO.SMITransaksiTokoPerjenisMember g on g.kodetoko=a.kodetoko and g.kodetoko=c.kodetoko and g.kodetoko=f.kodetoko and g.kodetoko=e.kodetoko  
							and g.nomorTransaksi=a.nomorTransaksi and g.nomorTransaksi=c.nomorTransaksi
							--JOIN master Member	
							JOIN MB_DC.DBO.mstMember b 
								on b.noPolisi=g.noPolisi
							JOIN MB_DC.DBO.mstJenisMember d 
								on d.idjenisMember=g.idjenisMember
							--JOIN untuk mencari group
							LEFT JOIN MB_DC.DBO.smimstgroupvoucherheaderDiskon h 
								on h.idGroupVoucher=c.idGroupVoucher
							--JOIN untuk mencari marchant
							LEFT JOIN MB_DC.DBO.MstMarchantVoucher i 
								ON c.idMarchant = i.idMarchant  
							LEFT JOIN MB_DC.DBO.MstCabang j
								ON e.idcabang = j.idCabang
	--WHERE CONVERT(date,tglBayar) between '2025-10-01' and '2025-10-01' and e.idcabang=2
	WHERE CONVERT(date,tglBayar) between @date1 and @date2 and e.idcabang=@idcabang
	and c.kodecarabayar='VOU' and substring(c.nomorserivoucher,1,1) in ('2','4')

	UNION ALL

	SELECT j.NamaCabang,a.kodetoko,namaToko,tglBayar,a.nomorTransaksi,g.NoPolisi,namaMember,NoTelp,namaJenisMember,h.namaGroupVoucher,i.namaMarchant,c.NomorSeriVoucher as kodeVoucher,c.kodeCaraBayar,c.nominalVoucher,totalQty,totalRpPenjualan 
	FROM MB_DC.DBO.transaksitokoHeader a
			JOIN MB_DC.DBO.TransaksiTokoPembayaranPerCaraBayarNonTunaiVoucher c on c.kodetoko=a.kodetoko and c.nomorTransaksi=a.nomorTransaksi
					JOIN MB_DC.DBO.mstToko f on f.kodetoko=a.kodetoko and f.kodetoko=c.kodetoko
					JOIN MB_DC.DBO.masterToolsToko e on e.kodetoko=a.kodetoko and e.kodetoko=c.kodetoko and e.kodetoko=f.kodetoko
						JOIN MB_DC.DBO.SMITransaksiTokoPerjenisMember g on g.kodetoko=a.kodetoko and g.kodetoko=c.kodetoko and g.kodetoko=f.kodetoko and g.kodetoko=e.kodetoko  
							and g.nomorTransaksi=a.nomorTransaksi and g.nomorTransaksi=c.nomorTransaksi
							--JOIN master Member	
								JOIN MB_DC.DBO.mstMember b on b.noPolisi=g.noPolisi
								JOIN MB_DC.DBO.mstJenisMember d on d.idjenisMember=g.idjenisMember
							--JOIN untuk mencari group
							LEFT JOIN MB_DC.DBO.smimstgroupvoucherheaderPersen h 
								on h.idGroupVoucher=c.idGroupVoucher
							--JOIN untuk mencari marchant
							LEFT JOIN MB_DC.DBO.MstMarchantVoucherPersen I 
								ON c.idMarchant = I.idMarchant  
							LEFT JOIN MB_DC.DBO.MstCabang j
								ON e.idcabang = j.idCabang
	--WHERE CONVERT(date,tglBayar) between '2025-10-01' and '2025-10-01' and e.idcabang=2
	WHERE CONVERT(date,tglBayar) between @date1 and @date2 and e.idcabang=@idcabang
	and c.kodecarabayar='PVO' and substring(c.nomorserivoucher,1,1) in ('5','7')

	UNION ALL

	SELECT j.NamaCabang,a.kodetoko,namaToko,tglBayar,a.nomorTransaksi,g.NoPolisi,namaMember,NoTelp,namaJenisMember,h.namaGroupVoucher,i.namaMarchant,c.NomorSeriVoucher as kodeVoucher,c.kodeCaraBayar,c.nominalVoucher,totalQty,totalRpPenjualan 
	FROM MB_DC.DBO.transaksitokoHeader a
			JOIN MB_DC.DBO.TransaksiTokoPembayaranPerCaraBayarNonTunaiVoucher c on c.kodetoko=a.kodetoko and c.nomorTransaksi=a.nomorTransaksi
					JOIN MB_DC.DBO.mstToko f on f.kodetoko=a.kodetoko and f.kodetoko=c.kodetoko
					JOIN MB_DC.DBO.masterToolsToko e on e.kodetoko=a.kodetoko and e.kodetoko=c.kodetoko and e.kodetoko=f.kodetoko
						JOIN MB_DC.DBO.SMITransaksiTokoPerjenisMember g on g.kodetoko=a.kodetoko and g.kodetoko=c.kodetoko and g.kodetoko=f.kodetoko and g.kodetoko=e.kodetoko  
							and g.nomorTransaksi=a.nomorTransaksi and g.nomorTransaksi=c.nomorTransaksi
							--JOIN master Member	
								JOIN MB_DC.DBO.mstMember b on b.noPolisi=g.noPolisi
								JOIN MB_DC.DBO.mstJenisMember d on d.idjenisMember=g.idjenisMember
							--JOIN untuk mencari group
							LEFT JOIN MB_DC.DBO.smimstgroupvoucherheaderDiskonPersen h
								on h.idGroupVoucher=c.idGroupVoucher
							--JOIN untuk mencari marchant
							LEFT JOIN MB_DC.DBO.MstMarchantVoucherPersen I 
								ON c.idMarchant = I.idMarchant  
							LEFT JOIN MB_DC.DBO.MstCabang j
								ON e.idcabang = j.idCabang
	--WHERE CONVERT(date,tglBayar) between '2025-10-01' and '2025-10-01' and e.idcabang=2
	WHERE CONVERT(date,tglBayar) between @date1 and @date2 and e.idcabang=@idcabang
	and c.kodecarabayar='VOU' and substring(c.nomorserivoucher,1,1) in ('6','8')

	UNION ALL

	SELECT j.NamaCabang,a.kodetoko,namaToko,tglBayar,a.nomorTransaksi,g.NoPolisi,namaMember,NoTelp,namaJenisMember,h.namaGroupVoucher,i.namaMarchant,c.NomorSeriVoucher as kodeVoucher,c.kodeCaraBayar,c.nominalVoucher,totalQty,totalRpPenjualan 
	FROM MB_DC.DBO.transaksitokoHeader a
			JOIN MB_DC.DBO.TransaksiTokoPembayaranPerCaraBayarNonTunaiVoucher c on c.kodetoko=a.kodetoko and c.nomorTransaksi=a.nomorTransaksi
					JOIN MB_DC.DBO.mstToko f on f.kodetoko=a.kodetoko and f.kodetoko=c.kodetoko
					JOIN MB_DC.DBO.masterToolsToko e on e.kodetoko=a.kodetoko and e.kodetoko=c.kodetoko and e.kodetoko=f.kodetoko
						JOIN MB_DC.DBO.SMITransaksiTokoPerjenisMember g on g.kodetoko=a.kodetoko and g.kodetoko=c.kodetoko and g.kodetoko=f.kodetoko and g.kodetoko=e.kodetoko  
							and g.nomorTransaksi=a.nomorTransaksi and g.nomorTransaksi=c.nomorTransaksi
							--JOIN master Member	
								JOIN MB_DC.DBO.mstMember b on b.noPolisi=g.noPolisi
								JOIN MB_DC.DBO.mstJenisMember d on d.idjenisMember=g.idjenisMember
							--JOIN untuk mencari group
							LEFT JOIN MB_DC.DBO.smimstgroupvoucherheaderPersen h 
								on h.idGroupVoucher=c.idGroupVoucher
							--JOIN untuk mencari marchant
							LEFT JOIN MB_DC.DBO.MstMarchantVoucherPersen I 
								ON c.idMarchant = I.idMarchant 
							LEFT JOIN MB_DC.DBO.MstCabang j
								ON e.idcabang = j.idCabang
	--WHERE CONVERT(date,tglBayar) between '2025-10-01' and '2025-10-01' and e.idcabang=2
	WHERE CONVERT(date,tglBayar) between @date1 and @date2 and e.idcabang=@idcabang
	and c.kodecarabayar='VOU' and substring(c.nomorserivoucher,1,1) in ('9')

	UNION ALL

	SELECT j.NamaCabang,a.kodetoko,namaToko,tglBayar,a.nomorTransaksi,g.NoPolisi,namaMember,NoTelp,namaJenisMember,' ' as namaGroupVoucher,' ' as namaMarchant,c.NomorSeriVoucher as kodeVoucher,c.kodeCaraBayar,c.nominalVoucher,totalQty,totalRpPenjualan 
	FROM MB_DC.DBO.transaksitokoHeader a
			JOIN MB_DC.DBO.TransaksiTokoPembayaranPerCaraBayarNonTunaiVoucher c on c.kodetoko=a.kodetoko and c.nomorTransaksi=a.nomorTransaksi
					JOIN MB_DC.DBO.mstToko f on f.kodetoko=a.kodetoko and f.kodetoko=c.kodetoko
					JOIN MB_DC.DBO.masterToolsToko e on e.kodetoko=a.kodetoko and e.kodetoko=c.kodetoko and e.kodetoko=f.kodetoko
						JOIN MB_DC.DBO.SMITransaksiTokoPerjenisMember g on g.kodetoko=a.kodetoko and g.kodetoko=c.kodetoko and g.kodetoko=f.kodetoko and g.kodetoko=e.kodetoko  
							and g.nomorTransaksi=a.nomorTransaksi and g.nomorTransaksi=c.nomorTransaksi
							--JOIN master Member	
								LEFT JOIN MB_DC.DBO.mstMember b on b.noPolisi=g.noPolisi
								LEFT JOIN MB_DC.DBO.mstJenisMember d on d.idjenisMember=g.idjenisMember 
							LEFT JOIN MB_DC.DBO.MstCabang j
								ON e.idcabang = j.idCabang
	--WHERE CONVERT(date,tglBayar) between '2025-10-01' and '2025-10-01' and e.idcabang=2
	WHERE CONVERT(date,tglBayar) between @date1 and @date2 and e.idcabang=@idcabang
	and SUBSTRING(c.nomorseriVoucher,1,1) not like '[0-9]'
	ORDER BY kodetoko,tglBayar
	---End Query
	
end;
GO

-----


USE MB_DB_Reporting_JKT
GO

/****** Object:  Table [dbo].[SMI_Data_Penggunaan_Voucher]    Script Date: 10/28/2025 10:38:45 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [dbo].MB_Data_Penggunaan_Voucher(
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


-----


select * from [dbo].MB_Data_Penggunaan_Voucher;