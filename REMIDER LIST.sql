select count(*) from public.smi_goliaht_voucher where statuskirim=0;

select count(*) from public.mb_goliaht_voucher where statuskirim=0;

select count(*) from public.car_voucher where statuskirim=0;

select count(*) from public.pkb_voucher where statuskirim=0;

select * from public.pkb_his where statuskirim=0;

select count(*) from public.lc_voucher where statuskirim=0;


select * from public.car_his order by tglreport desc;

select * from public.car_his where reminder='2026-02-19' 

-------------------------------------------------------------------------------------------------------------------
--Accu
select * from public.pra_his;
select * from public.pra_his where reminder1='2025-11-11' and wa_status1=0;
select * from public.pra_his where wa_status1=1;

--Summary
select 
x.tahun as "TAHUN", 
x.bulan as "BULAN", 
x.namacabang as "NAMA CABANG",
sum(x.wa_status1) as "JUMLAH WA REMIND",
sum(total_read) as "JUMLAH STATUS READ",
sum(total_sent) as "JUMLAH STATUS SENT",
sum(total_failed) as "JUMLAH STATUS FAILED",
sum(total_delivered) as "JUMLAH STATUS DELIVERED",
sum(total_null) as "JUMLAH STATUS KOSONG"
from (
	select 
	TO_CHAR(reportdate, 'YYYY') AS tahun,
	TO_CHAR(reportdate, 'MM') AS bulan,
	namacabang,sum(wa_status1) as wa_status1,
	COUNT(*) FILTER (WHERE wa_status_data1 = 'read') AS total_read,
    COUNT(*) FILTER (WHERE wa_status_data1 = 'sent') AS total_sent,
    COUNT(*) FILTER (WHERE wa_status_data1 = 'failed') AS total_failed,
    COUNT(*) FILTER (WHERE wa_status_data1 = 'delivered') AS total_delivered,
    COUNT(*) FILTER (WHERE wa_status_data1 IS NULL) AS total_null
	from public.pra_his 
	where wa_status1=1
	group by namacabang, reportdate
)as x
group by x.tahun,x.bulan,x.namacabang
order by x.tahun asc, x.bulan asc;
-------------------------------------------------------------------------------------------------------------------
--Offering
select * from public.car_base_nopol where nopolisi ='B5867SBB';
select * from public.car_base_nopol where insertdate::date ='2026-02-12';

select * from public.car_his order by tglreport desc;
select * from public.car_his where reminder='2026-02-11' and wa_status=0;--12,10,8,7,6
select * from public.car_his where tglreport='2026-02-12'
select * from public.car_his where wa_status=1;
select * from public.car_his where wa_status=1 and wa_status_data is null;

        SELECT * FROM public.smi_rms10_transaksi_toko_perjenis_member_v3 where nopolisi='B5867SBB'

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
--select x.tahun as "TAHUN", x.bulan as "BULAN", x.namacabang as "NAMA CABANG",sum(x.wa_status) as "JUMLAH WA REMIND"  
--from (
--	select 
--	TO_CHAR(kolom_d::date, 'YYYY') AS tahun,
--	TO_CHAR(kolom_d::date, 'MM') AS bulan,
--	namacabang,sum(wa_status) as wa_status
--	from public.smi_trx_oil_goliaht_his 
--	where wa_status=1 --and kolom_d::date between '2024-10-01' and '2025-07-31' 
--	group by namacabang, kolom_d::date
--)as x
--group by x.tahun, x.bulan, x.namacabang
--order by x.tahun, x.bulan, x.namacabang;
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
	TO_CHAR(kolom_d, 'YYYY') AS tahun,
	TO_CHAR(kolom_d, 'MM') AS bulan,
	namacabang,sum(wa_status) as wa_status,
	COUNT(*) FILTER (WHERE wa_status_data = 'read') AS total_read,
    COUNT(*) FILTER (WHERE wa_status_data = 'sent') AS total_sent,
    COUNT(*) FILTER (WHERE wa_status_data = 'failed') AS total_failed,
    COUNT(*) FILTER (WHERE wa_status_data = 'delivered') AS total_delivered,
    COUNT(*) FILTER (WHERE wa_status_data IS NULL) AS total_null
	from public.smi_trx_oil_goliaht_his 
	where wa_status=1
	group by namacabang, kolom_d
)as x
group by x.tahun,x.bulan,x.namacabang
order by x.tahun asc, x.bulan asc;

--Detail
select * from public.smi_trx_oil_goliaht_his order by kolom_d desc limit 1;
select * from public.smi_trx_oil_goliaht_his where wa_status=1 and kolom_d::date between '2025-12-01' and '2025-12-31' and wa_status_data is NULL;
select * from public.smi_trx_oil_goliaht_his where kolom_d::date='2025-10-22' and wa_status=0; 
select * from public.smi_trx_oil_goliaht_his where kolom_d::date='2025-10-22' and kolom_ac='-30' and wa_status=0; 
select * from public.smi_trx_oil_goliaht_his where wa_status=1;
select * from public.smi_trx_oil_goliaht_his where kolom_d::date='2025-12-15' and kolom_ac='0' and namacabang='Jakarta Baru';
select * from public.smi_trx_oil_goliaht_his where kolom_d::date='2026-01-09' and kolom_ac='0' and wa_status=0;
select distinct namacabang from public.smi_trx_oil_goliaht_his where kolom_d::date='2026-01-12' and kolom_ac='0';
select distinct namacabang from public.smi_trx_oil_goliaht_his where kolom_d::date='2025-12-30';
select * from public.smi_trx_oil_goliaht_monitoring_all where kolom_d::date='2026-01-03' and namacabang='Jakarta Baru'

select distinct namacabang from smi_rpt_goliaht_last_trx_monthly_temp order by tanggal desc;
-------------------------------------------------------------------------------------------------------------------
--Goliaht MOB
--Summary
--select x.tahun as "TAHUN", x.bulan as "BULAN", x.namacabang as "NAMA CABANG",sum(x.wa_status) as "JUMLAH WA REMIND"  
--from (
--	select 
--	TO_CHAR(kolom_d::date, 'YYYY') AS tahun,
--	TO_CHAR(kolom_d::date, 'MM') AS bulan,
--	namacabang,sum(wa_status) as wa_status
--	from public.mb_trx_oil_goliaht_his 
--	where wa_status=1 --and kolom_d::date between '2024-10-01' and '2025-07-31' 
--	group by namacabang, kolom_d::date
--)as x
--group by x.tahun, x.bulan, x.namacabang
--order by x.tahun, x.bulan, x.namacabang;
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
	TO_CHAR(kolom_d, 'YYYY') AS tahun,
	TO_CHAR(kolom_d, 'MM') AS bulan,
	namacabang,sum(wa_status) as wa_status,
	COUNT(*) FILTER (WHERE wa_status_data = 'read') AS total_read,
    COUNT(*) FILTER (WHERE wa_status_data = 'sent') AS total_sent,
    COUNT(*) FILTER (WHERE wa_status_data = 'failed') AS total_failed,
    COUNT(*) FILTER (WHERE wa_status_data = 'delivered') AS total_delivered,
    COUNT(*) FILTER (WHERE wa_status_data IS NULL) AS total_null
	from public.mb_trx_oil_goliaht_his 
	where wa_status=1
	group by namacabang, kolom_d
)as x
group by x.tahun,x.bulan,x.namacabang
order by x.tahun asc, x.bulan asc;

--Detail
select * from public.mb_trx_oil_goliaht_his 
where wa_status=1 and kolom_d::date between '2025-11-16' and '2025-11-16';
select * from public.mb_trx_oil_goliaht_his where kolom_d::date='2026-01-10' and wa_status=0; 
-------------------------------------------------------------------------------------------------------------------
