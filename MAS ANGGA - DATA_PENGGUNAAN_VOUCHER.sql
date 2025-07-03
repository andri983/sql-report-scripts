--USE [SMI_DB_Reporting_Bali]
--GO
--
--/****** Object:  StoredProcedure [dbo].[spDataPenggunaanVoucher]    Script Date: 7/3/2025 11:26:56 AM ******/
--SET ANSI_NULLS ON
--GO
--
--SET QUOTED_IDENTIFIER ON
--GO
--
--CREATE PROCEDURE [dbo].[spDataPenggunaanVoucher] (@date1 date,@date2 date,@idcabang int)
--as
--begin

	---Start Query Header Voucher
--	insert into [SMI_DB_Reporting_Bali].dbo.SMI_Data_Penggunaan_Voucher
	select a.kodetoko,namaToko,tglBayar,a.nomorTransaksi,g.NoPolisi,namaMember,NoTelp,namaJenisMember,c.NomorSeriVoucher as kodeVoucher,c.nominalVoucher,i.namaPendek,i.qty,i.subTotal,totalQty,totalRpPenjualan 
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
--	where convert(date,tglBayar) between @date1 and @date2 and idcabang=@idcabang
	where convert(date,tglBayar) between '2025-06-01' and '2025-06-30' and idcabang=5
	and i.idjenisproduk<>4
	and i.statusproduk<>'K'
	and a.kodetoko=3051002 and a.nomorTransaksi=202506120008
	union all
	select a.kodetoko,namaToko,tglBayar,a.nomorTransaksi,g.NoPolisi,namaMember,NoTelp,namaJenisMember,c.NomorSeriVoucher as kodeVoucher,c.nominalVoucher,i.namaPendek,i.qty,i.subTotal,totalQty,totalRpPenjualan 
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
--	where convert(date,tglBayar) between @date1 and @date2 and idcabang=@idcabang
	where convert(date,tglBayar) between '2025-06-01' and '2025-06-30' and idcabang=5
	and i.idjenisproduk<>4
	and i.statusproduk<>'K'
	and a.kodetoko=3051002 and a.nomorTransaksi=202506120008
	order by kodetoko,tglBayar
	---End Query
	
--end;
--GO


