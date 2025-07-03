SELECT
t1.Tanggal::date as "TANGGAL",
t1.NamaCabang as "NAMA CABANG",
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
--Round(t1.Subtotal,0) as "SUBTOTAL",
CASE 
    WHEN t1.statusproduk = 'T' THEN 0 
    ELSE ROUND(t1.Subtotal, 0) 
END AS "SUBTOTAL",
t1.nopolisi as "NO. POLISI",
t2.namamember as "CUSTOMER"
From public.mb_rms10_transaksi_toko_perjenis_member_v3 as t1
left join mb_rms10_mbdc.mv_profile_customer t2 on upper(t2.nopolisi)=upper(t1.nopolisi) 
where t1.namacabang ='Jakarta Baru'
--and t1.idjenisproduk<>4 -- REMAKS UNTUK MENGELUARKAN DATA PAKET
--and t1.statusproduk<>'K' -- REMAKS UNTUK MENGELUARKAN DATA PAKET
--and t1.kodetoko='3021001'
--and t1.nomortransaksi='202507010002'
and (t1.idjenisproduk=4 or t1.statusproduk='T')
and 
case
	when to_char(now()::date,'dd')::character varying = '01' then (t1.tanggal between date_trunc('month',current_date - interval '1' month)::date and ((date_trunc('month', current_date) - interval '0 month' - interval '1 day')::date))
	else(t1.tanggal::date between date_trunc('month', current_date)::date and current_date::date) 
end
Order by t1.namacabang Asc, t1.Tanggal Asc, t1.KodeToko asc,t1.NomorTransaksi asc,t1.NomorTransaksi asc,t1.nomorurut  asc;