select 
t."TANGGAL",
t."NAMA CABANG",
--t."KODE TOKO LAMA",
t."KODE TOKO",
t."NAMA TOKO",
t."NOMOR TRANSAKSI",
t."BRAND",
t."JENIS",
t."CATEGORY",
t."SKU LAMA",
t."KODE PRODUK",
t."NAMA PANJANG",
Round(sum(t."QTY"),0) as  "QTY",
Round(t."Hpp",2) as "Hpp",
Round (t."HARGA JUAL NORMAL",0) as "HARGA JUAL NORMAL",
t.Pdisc1,
t.Pdisc2,
round(sum(t.Disc),0) as disc,
Round(sum(t."SUBTOTAL"),0) as "SUBTOTAL",
t."NO. POLISI",
t."CUSTOMER",
t."MODEL",
t."TAHUN KENDARAAN"
from (
	SELECT
	t1.Tanggal::date as "TANGGAL",
	t1.NamaCabang as "NAMA CABANG",
	t1.KodeTokoLama as "KODE TOKO LAMA",
	t1.KodeToko::text as "KODE TOKO",
	t1.NamaToko as "NAMA TOKO",
	t1.NomorTransaksi::text as "NOMOR TRANSAKSI",
	t1.Brand as "BRAND",
	t1.Jenis as "JENIS",
	t1.Category as "CATEGORY",
	t1.KodeprodukLama as "SKU LAMA",
	t1.KodeProduk::text as "KODE PRODUK",
	t1.NamaPanjang as "NAMA PANJANG",
	Round(t1.Qty,0) as  "QTY",
	Round(t1.Hpp,2) as "Hpp",
	Round (t1.HargaJualNormal,0) as "HARGA JUAL NORMAL",
	0 as Pdisc1,
	0 as Pdisc2,
	round(t1.Disc,0) as disc,
	Round(t1.Subtotal,0) as "SUBTOTAL",
	t1.nopolisi as "NO. POLISI",
	t2.namamember as "CUSTOMER",
	concat(t2.brandmotor, ' / ', t2.typemotor) AS "MODEL",
	t2.tahunmotor AS "TAHUN KENDARAAN"
--	From mb_rms20_rpt.mv_mb_rms20_transaksi_toko_perjenis_member_v3 as t1
--	left join mb_rms20_mbdc.mv_profile_customer t2 on upper(t2.nopolisi)=upper(t1.nopolisi)
	From public.mb_rms20_transaksi_toko_perjenis_member_v3 as t1
	left join mb_rms20_mbdc.mv_profile_customer t2 on upper(t2.nopolisi)=upper(t1.nopolisi) 
	Where
	t1.namacabang ='Surabaya'
	and t1.idjenisproduk<>4
	and t1.statusproduk<>'K'
--	and (t1.tanggal::date >= date_trunc('month',now()::date - interval '1' month) 
--	and t1.tanggal::date < date_trunc('month',now()::date ))
	and t1.tanggal::date between '2026-01-01' and '2026-04-27'
	--Order by t1.namacabang Asc, t1.Tanggal Asc, t1.KodeToko asc,t1.NomorTransaksi asc;
union All
	SELECT
	t1.Tanggal::date as "TANGGAL",
	t1.NamaCabang as "NAMA CABANG",
	t1.KodeTokoLama as "KODE TOKO LAMA",
	t1.KodeToko::text as "KODE TOKO",
	t1.NamaToko as "NAMA TOKO",
	t1.NomorTransaksi::text as "NOMOR TRANSAKSI",
	t1.Brand as "BRAND",
	t1.Jenis as "JENIS",
	t1.Category as "CATEGORY",
	t1.KodeprodukLama as "SKU LAMA",
	t1.KodeProduk::text as "KODE PRODUK",
	t1.NamaPanjang as "NAMA PANJANG",
	Round(t1.Qty,0) as  "QTY",
	Round(t1.Hpp,2) as "Hpp",
	Round (t1.HargaJualNormal,0) as "HARGA JUAL NORMAL",
	'0' as Pdisc1,
	'0' as Pdisc2,
	round(t1.Disc,0) as disc,
	Round(t1.Subtotal,0) as "SUBTOTAL",
	t1.nopolisi as "NO. POLISI",
	t2.namamember as "CUSTOMER",
	concat(t2.brandmotor, ' / ', t2.typemotor) AS "MODEL",
	t2.tahunmotor AS "TAHUN KENDARAAN"
--	From mb_rms10_rpt.mv_mb_rms10_transaksi_toko_perjenis_member_v3 as t1
--	left join mb_rms10_mbdc.mv_profile_customer t2 on upper(t2.nopolisi)=upper(t1.nopolisi)
	From public.mb_rms10_transaksi_toko_perjenis_member_v3 as t1
	left join mb_rms10_mbdc.mv_profile_customer t2 on upper(t2.nopolisi)=upper(t1.nopolisi) 
	Where
	t1.namacabang='Jakarta Baru'
	and t1.idjenisproduk<>4
	and t1.statusproduk<>'K'
--	and (t1.tanggal::date >= date_trunc('month',now()::date - interval '1' month) 
--	and t1.tanggal::date < date_trunc('month',now()::date ))
 and t1.tanggal::date between '2026-01-01' and '2026-04-27'
) as t
group by 
t."TANGGAL",
t."NAMA CABANG",
t."KODE TOKO LAMA",
t."KODE TOKO",
t."NAMA TOKO",
t."NOMOR TRANSAKSI",
t."BRAND",
t."JENIS",
t."CATEGORY",
t."SKU LAMA",
t."KODE PRODUK",
t."NAMA PANJANG",
t."Hpp",
t."HARGA JUAL NORMAL",
t.Pdisc1,
t.Pdisc2,
t."NO. POLISI",
t."CUSTOMER",
t."MODEL",
t."TAHUN KENDARAAN"
Order by t."NAMA CABANG" Asc, t."TANGGAL" Asc, t."KODE TOKO" asc,t."NOMOR TRANSAKSI" asc;