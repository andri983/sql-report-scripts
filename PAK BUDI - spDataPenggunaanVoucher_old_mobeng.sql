USE [MB_DB_Reporting_SBY]
GO

/****** Object:  StoredProcedure [dbo].[spDataPenggunaanVoucher]    Script Date: 10/28/2025 10:34:14 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO


CREATE PROCEDURE [dbo].[spDataPenggunaanVoucher] (@date1 date,@date2 date,@idcabang int)
as
begin

	---Start Query Header Voucher
	insert into MB_DB_Reporting_SBY.dbo.MB_Data_Penggunaan_Voucher
	select a.kodetoko,namaToko,tglBayar,a.nomorTransaksi,g.NoPolisi,namaMember,NoTelp,namaJenisMember,c.NomorSeriVoucher as kodeVoucher,c.nominalVoucher,totalQty,totalRpPenjualan from MB_DC_SBY.dbo.transaksitokoHeader a
			join MB_DC_SBY.dbo.TransaksiTokoPembayaranPerCaraBayarNonTunaiVoucher c on c.kodetoko=a.kodetoko and c.nomorTransaksi=a.nomorTransaksi
					join MB_DC_SBY.dbo.mstToko f on f.kodetoko=a.kodetoko and f.kodetoko=c.kodetoko
					join MB_DC_SBY.dbo.masterToolsToko e on e.kodetoko=a.kodetoko and e.kodetoko=c.kodetoko and e.kodetoko=f.kodetoko
						join MB_DC_SBY.dbo.SMITransaksiTokoPerjenisMember g on g.kodetoko=a.kodetoko and g.kodetoko=c.kodetoko and g.kodetoko=f.kodetoko and g.kodetoko=e.kodetoko  
							and g.nomorTransaksi=a.nomorTransaksi and g.nomorTransaksi=c.nomorTransaksi
							--join master Member	
								join MB_DC_SBY.dbo.mstMember b on b.noPolisi=g.noPolisi
								join MB_DC_SBY.dbo.mstJenisMember d on d.idjenisMember=g.idjenisMember
							--join untuk mencari marchant
								join MB_DC_SBY.dbo.SMIMstVoucher h on h.nomorSerivoucher=c.nomorSerivoucher
	where convert(date,tglBayar) between @date1 and @date2 and idcabang=@idcabang
	union all
	select a.kodetoko,namaToko,tglBayar,a.nomorTransaksi,g.NoPolisi,namaMember,NoTelp,namaJenisMember,c.NomorSeriVoucher as kodeVoucher,c.nominalVoucher,totalQty,totalRpPenjualan from MB_DC_SBY.dbo.transaksitokoHeader a
			join MB_DC_SBY.dbo.TransaksiTokoPembayaranPerCaraBayarNonTunaiVoucher c on c.kodetoko=a.kodetoko and c.nomorTransaksi=a.nomorTransaksi
					join MB_DC_SBY.dbo.mstToko f on f.kodetoko=a.kodetoko and f.kodetoko=c.kodetoko
					join MB_DC_SBY.dbo.masterToolsToko e on e.kodetoko=a.kodetoko and e.kodetoko=c.kodetoko and e.kodetoko=f.kodetoko
						join MB_DC_SBY.dbo.SMITransaksiTokoPerjenisMember g on g.kodetoko=a.kodetoko and g.kodetoko=c.kodetoko and g.kodetoko=f.kodetoko and g.kodetoko=e.kodetoko  
							and g.nomorTransaksi=a.nomorTransaksi and g.nomorTransaksi=c.nomorTransaksi
							--join master Member	
								join MB_DC_SBY.dbo.mstMember b on b.noPolisi=g.noPolisi
								join MB_DC_SBY.dbo.mstJenisMember d on d.idjenisMember=g.idjenisMember
							--join untuk mencari marchant
								join MB_DC_SBY.dbo.SMIMstVoucherDiskon h on h.nomorSerivoucher=c.nomorSerivoucher
	where convert(date,tglBayar) between @date1 and @date2 and idcabang=@idcabang
	order by kodetoko,tglBayar
	---End Query
	
end;
GO


