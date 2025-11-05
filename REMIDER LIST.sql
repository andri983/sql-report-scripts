-------------------------------------------------------------------------------------------------------------------
--Accu
select * from public.pra_his;
select * from public.pra_his where reminder1='2025-10-22' and wa_status1=0;
select * from public.pra_his where wa_status1=1;
-------------------------------------------------------------------------------------------------------------------
--Offering
select * from public.car_his;
select * from public.car_his where reminder='2025-10-22' and wa_status=0;
select * from public.car_his where wa_status=1;

--Summary
select 
x.tahun as "TAHUN", 
x.bulan as "BULAN", 
x.namacabang as "NAMA CABANG",
sum(x.wa_status) as "JUMLAH WA REMIND",
sum(total_read) as "JUMLAH STATUS READ",
sum(total_sent) as "JUMLAH STATUS SENT",
sum(total_failed) as "JUMLAH STATUS FAILED",
sum(total_delivered) as "JUMLAH STATUS DELIVERED",
sum(total_null) as "JUMLAH STATUS KOSONG"
from (
	select 
	TO_CHAR(tglreport, 'YYYY') AS tahun,
	TO_CHAR(tglreport, 'MM') AS bulan,
	namacabang,sum(wa_status) as wa_status,
	COUNT(*) FILTER (WHERE wa_status_data = 'read') AS total_read,
    COUNT(*) FILTER (WHERE wa_status_data = 'sent') AS total_sent,
    COUNT(*) FILTER (WHERE wa_status_data = 'failed') AS total_failed,
    COUNT(*) FILTER (WHERE wa_status_data = 'delivered') AS total_delivered,
    COUNT(*) FILTER (WHERE wa_status_data IS NULL) AS total_null
	from public.car_his 
	where wa_status=1
	group by namacabang, tglreport
)as x
group by x.tahun,x.bulan,x.namacabang
order by x.tahun asc, x.bulan asc;

--Detail
SELECT 
    x.tgltransaksi as "Tgl Transaksi", 
    x.tglreport as "Tgl Report", 
    x.tglakhirvoucher as "Tgl Akhir Voucher", 
    x.nomorserivoucher as "Nomor Seri VOucher", 
    x.namacabang as "Nama Cabang", 
    x.kodetoko as "Kode Toko", 
    x.nomortransaksi as "Nomor Transaksi", 
    x.brand as "Brand", 
    x.category as "Category", 
    x.idproduk as "Id Produk", 
    x.kodeproduk as "Kode Produk", 
    x.namapanjang as "Nama Panjang", 
    x.qty as "Qty", 
    x.subtotal as "Subtotal", 
    x.nopolisi as "No. POlisi", 
    x.namamember as "Nama Member", 
    x.notelp as "No. Hp/WA", 
    x.idjenismember as "Id Jenis Member", 
    x.namajenismember as "Nama Jenis Member", 
    x.reminder as "Reminder",
    x.wa_status as "WA Status", 
    x.wa_send_date as "WA Send Date", 
    x.wa_xid as "WA XID", 
    x.wa_status_data as "WA Status Data",
    a.tglbayar as "Tgl Transaksi", 
    a.kodetoko as "Kode Toko", 
    a.nomortransaksi as "Nomor Transaksi", 
    a.nomorserivoucher as "Nomor Serial Vuocher"
FROM 
    public.car_his x
LEFT JOIN (
    SELECT tglbayar,kodetoko,nomortransaksi,nomorserivoucher 
    FROM smi_sales_voucher_cab_his
) AS a ON x.nomorserivoucher = a.nomorserivoucher
ORDER BY x.tgltransaksi;
-------------------------------------------------------------------------------------------------------------------
--Goliaht SMI
--Summary
select x.tahun as "TAHUN", x.bulan as "BULAN", x.namacabang as "NAMA CABANG",sum(x.wa_status) as "JUMLAH WA REMIND"  
from (
	select 
	TO_CHAR(kolom_d::date, 'YYYY') AS tahun,
	TO_CHAR(kolom_d::date, 'MM') AS bulan,
	namacabang,sum(wa_status) as wa_status
	from public.smi_trx_oil_goliaht_his 
	where wa_status=1 --and kolom_d::date between '2024-10-01' and '2025-07-31' 
	group by namacabang, kolom_d::date
)as x
group by x.tahun, x.bulan, x.namacabang
order by x.tahun, x.bulan, x.namacabang;

--Detail
select * from public.smi_trx_oil_goliaht_his 
select * from public.smi_trx_oil_goliaht_his where kolom_d::date='2025-10-22' and wa_status=0; 
select * from public.smi_trx_oil_goliaht_his where kolom_d::date='2025-10-22' and kolom_ac='-30' and wa_status=0; 
select * from public.smi_trx_oil_goliaht_his where wa_status=1;
-------------------------------------------------------------------------------------------------------------------
--Goliaht MOB
--Summary
select x.tahun as "TAHUN", x.bulan as "BULAN", x.namacabang as "NAMA CABANG",sum(x.wa_status) as "JUMLAH WA REMIND"  
from (
	select 
	TO_CHAR(kolom_d::date, 'YYYY') AS tahun,
	TO_CHAR(kolom_d::date, 'MM') AS bulan,
	namacabang,sum(wa_status) as wa_status
	from public.mb_trx_oil_goliaht_his 
	where wa_status=1 --and kolom_d::date between '2024-10-01' and '2025-07-31' 
	group by namacabang, kolom_d::date
)as x
group by x.tahun, x.bulan, x.namacabang
order by x.tahun, x.bulan, x.namacabang;

--Detail
select * from public.mb_trx_oil_goliaht_his 
where wa_status=1 and kolom_d::date between '2025-10-14' and '2025-10-14';
select * from public.mb_trx_oil_goliaht_his where kolom_d::date='2025-10-22' and wa_status=0; 
-------------------------------------------------------------------------------------------------------------------
