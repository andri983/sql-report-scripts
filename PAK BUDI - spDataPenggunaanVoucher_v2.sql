USE [PB_DC]
GO

/****** Object:  StoredProcedure [dbo].[spDataPenggunaanVoucher]    Script Date: 8/19/2025 3:09:04 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

ALTER PROCEDURE [dbo].[spDataPenggunaanVoucher] (@date1 date,@date2 date,@idcabang int)
as
begin
	---Start Query Header Voucher
	---//SMIMstVoucher
	select a.kodetoko,namaToko,tglBayar,a.nomorTransaksi,g.NoPolisi,namaMember,NoTelp,namaJenisMember,c.NomorSeriVoucher as kodeVoucher,c.nominalVoucher,totalQty,totalRpPenjualan from transaksitokoHeader a
			join TransaksiTokoPembayaranPerCaraBayarNonTunaiVoucher c on c.kodetoko=a.kodetoko and c.nomorTransaksi=a.nomorTransaksi
					join mstToko f on f.kodetoko=a.kodetoko and f.kodetoko=c.kodetoko
					join masterToolsToko e on e.kodetoko=a.kodetoko and e.kodetoko=c.kodetoko and e.kodetoko=f.kodetoko
						join SMITransaksiTokoPerjenisMember g on g.kodetoko=a.kodetoko and g.kodetoko=c.kodetoko and g.kodetoko=f.kodetoko and g.kodetoko=e.kodetoko  
							and g.nomorTransaksi=a.nomorTransaksi and g.nomorTransaksi=c.nomorTransaksi
							--join master Member	
								join mstMember b on b.noPolisi=g.noPolisi
								join mstJenisMember d on d.idjenisMember=g.idjenisMember
							--join untuk mencari marchant
								join SMIMstVoucher h on h.nomorSerivoucher=c.nomorSerivoucher
	where convert(date,tglBayar) between @date1 and @date2 and idcabang=@idcabang
	and c.kodecarabayar='PVO' and substring(c.nomorserivoucher,1,1) in ('1','3')
	union all---//SMIMstVoucherDiskon
	select a.kodetoko,namaToko,tglBayar,a.nomorTransaksi,g.NoPolisi,namaMember,NoTelp,namaJenisMember,c.NomorSeriVoucher as kodeVoucher,c.nominalVoucher,totalQty,totalRpPenjualan from transaksitokoHeader a
			join TransaksiTokoPembayaranPerCaraBayarNonTunaiVoucher c on c.kodetoko=a.kodetoko and c.nomorTransaksi=a.nomorTransaksi
					join mstToko f on f.kodetoko=a.kodetoko and f.kodetoko=c.kodetoko
					join masterToolsToko e on e.kodetoko=a.kodetoko and e.kodetoko=c.kodetoko and e.kodetoko=f.kodetoko
						join SMITransaksiTokoPerjenisMember g on g.kodetoko=a.kodetoko and g.kodetoko=c.kodetoko and g.kodetoko=f.kodetoko and g.kodetoko=e.kodetoko  
							and g.nomorTransaksi=a.nomorTransaksi and g.nomorTransaksi=c.nomorTransaksi
							--join master Member	
								join mstMember b on b.noPolisi=g.noPolisi
								join mstJenisMember d on d.idjenisMember=g.idjenisMember
							--join untuk mencari marchant
								join SMIMstVoucherDiskon h on h.nomorSerivoucher=c.nomorSerivoucher
	where convert(date,tglBayar) between @date1 and @date2 and idcabang=@idcabang
	and c.kodecarabayar='VOU' and substring(c.nomorserivoucher,1,1) in ('2','4')
	union all---//SMIMstVoucherPersen
		select a.kodetoko,namaToko,tglBayar,a.nomorTransaksi,g.NoPolisi,namaMember,NoTelp,namaJenisMember,c.NomorSeriVoucher as kodeVoucher,c.nominalVoucher,totalQty,totalRpPenjualan from transaksitokoHeader a
			join TransaksiTokoPembayaranPerCaraBayarNonTunaiVoucher c on c.kodetoko=a.kodetoko and c.nomorTransaksi=a.nomorTransaksi
					join mstToko f on f.kodetoko=a.kodetoko and f.kodetoko=c.kodetoko
					join masterToolsToko e on e.kodetoko=a.kodetoko and e.kodetoko=c.kodetoko and e.kodetoko=f.kodetoko
						join SMITransaksiTokoPerjenisMember g on g.kodetoko=a.kodetoko and g.kodetoko=c.kodetoko and g.kodetoko=f.kodetoko and g.kodetoko=e.kodetoko  
							and g.nomorTransaksi=a.nomorTransaksi and g.nomorTransaksi=c.nomorTransaksi
							--join master Member	
								join mstMember b on b.noPolisi=g.noPolisi
								join mstJenisMember d on d.idjenisMember=g.idjenisMember
							--join untuk mencari marchant
								join SMIMstVoucherPersen h on h.nomorSerivoucher=c.nomorSerivoucher
	where convert(date,tglBayar) between @date1 and @date2 and idcabang=@idcabang
	and c.kodecarabayar='PVO' and substring(c.nomorserivoucher,1,1) in ('5','7')
	union all---//SMIMstVoucherDiskonPersen
	select a.kodetoko,namaToko,tglBayar,a.nomorTransaksi,g.NoPolisi,namaMember,NoTelp,namaJenisMember,c.NomorSeriVoucher as kodeVoucher,c.nominalVoucher,totalQty,totalRpPenjualan from transaksitokoHeader a
			join TransaksiTokoPembayaranPerCaraBayarNonTunaiVoucher c on c.kodetoko=a.kodetoko and c.nomorTransaksi=a.nomorTransaksi
					join mstToko f on f.kodetoko=a.kodetoko and f.kodetoko=c.kodetoko
					join masterToolsToko e on e.kodetoko=a.kodetoko and e.kodetoko=c.kodetoko and e.kodetoko=f.kodetoko
						join SMITransaksiTokoPerjenisMember g on g.kodetoko=a.kodetoko and g.kodetoko=c.kodetoko and g.kodetoko=f.kodetoko and g.kodetoko=e.kodetoko  
							and g.nomorTransaksi=a.nomorTransaksi and g.nomorTransaksi=c.nomorTransaksi
							--join master Member	
								join mstMember b on b.noPolisi=g.noPolisi
								join mstJenisMember d on d.idjenisMember=g.idjenisMember
							--join untuk mencari marchant
								join SMIMstVoucherDiskonPersen h on h.nomorSerivoucher=c.nomorSerivoucher
	where convert(date,tglBayar) between @date1 and @date2 and idcabang=@idcabang
	and c.kodecarabayar='VOU' and substring(c.nomorserivoucher,1,1) in ('6','8')
	order by kodetoko,tglBayar
	---End Query
	
end;
GO


