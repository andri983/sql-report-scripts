use PB_DC
GO

---All data
declare @date1 date='2025-08-01'
declare @date2 date=CONVERT(date,dateadd(day,-1,@date1))

--select a.kodeToko,date_1,date_2,case when date_1<date_2 then convert(decimal(18,2),(date_1/date_2)*100) when date_1>=date_2 then 100 end as Persentase from

select 
a.kodeToko [Kode Toko]
,a.namaToko [Nama Toko]
,regional
,area
--,b.tglBisnis [Tgl. Sales]
,case when f.totalHeader=f.totalDetail and f.totaldetail=f.totalRekapHeader then 'Ok' else 'Not Ok' end as [Status Sales]
,tglEod [Tgl. EOD],tglUpload [Tgl. Upload EOD]
,case when date_1<date_2 then concat(convert(decimal(18),(date_1/date_2)*100),' %') when date_1>=date_2 then '100 %' end as [Persentase upload Stok]
,max_upload [Jam Upload Terakhir] 
				
from MstToko a 
	left join	
		(select kodetoko,tglbisnis,tglEod,tglUpload from tbltglbisnis where tglbisnis=@date1) b on b.kodeToko=a.kodeToko
	left join
		(select kodetoko,convert(decimal(18,2),count(*)) as date_2 from SaldoStokProdukToko where tglSaldo=@date2
		group by kodetoko) c on c.kodeToko=a.kodeToko 
	left join
		(select kodetoko,convert(decimal(18,2),count(*)) as date_1,max(tglUpload) max_upload from SaldoStokProdukToko where tglSaldo=@date1
		group by kodetoko) d on d.kodeToko=a.kodeToko
	left join MasterToolsToko e on e.kodetoko=a.kodetoko
	left join 
		(select 
			a.kodetoko,
			totalHeader,
			totalDetail,
			totalRekapHeader

				from (--Sales Header
					select a.kodetoko,tglbisnis,sum(totalRpPenjualan) totalHeader 
						from PB_DC.dbo.TransaksiTokoHeader a with (nolock)
						where tglBisnis=@date1
						group by a.kodetoko,tglbisnis
					) a
				left join 
					(--Sales Detail
					select b.kodetoko,tglbisnis,sum(subtotal) totalDetail 
						from PB_DC.dbo.TransaksiTokoHeader a with (nolock) 
						join PB_DC.dbo.TransaksiTokoDetail b with (nolock) on b.kodetoko=a.kodetoko and b.nomorTransaksi=a.nomorTransaksi 
						where tglBisnis=@date1 and idJenisProduk<>4 and statusProduk<>'K'
						group by b.kodetoko,tglbisnis 
					) b on b.kodeToko=a.kodeToko and b.tglBisnis=a.tglBisnis
				left join
					(--Rekap Header
					select kodetoko,TglPembukuanTransaksi,TotalRpTransaksi totalRekapHeader 
						from PB_DC.dbo.RekapTransaksiTokoHeader with (nolock) 
						where tglpembukuanTransaksi=@date1
					) c on c.KodeToko=a.kodeToko and c.TglPembukuanTransaksi=a.tglBisnis) f on f.kodetoko=a.kodetoko
		left join PB_DC.dbo.v_smi_pivot_area_toko g on g.kodetoko=a.kodetoko --area & RH
	where a.statusData=1 and tglBuka<=convert(date,getdate()) and idcabang=6 --and a.kodetoko=3021004
order by a.kodetoko,[Persentase upload Stok]