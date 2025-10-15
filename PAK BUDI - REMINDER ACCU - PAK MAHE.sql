SELECT 
a.tglBisnis,c.idcabang,d.nopolisi,e.namaMember,e.notelp,a.kodeToko,a.nomorTransaksi,a.nomorImen,
CASE
    WHEN b.trans_polutan_awal='1' THEN 'BAGUS diatas 12.5 V OFF'
    WHEN b.trans_polutan_awal='2' THEN 'LEMAH antara 12.2 dan 12.5 V OFF'
    WHEN b.trans_polutan_awal='3' THEN 'RUSAK dibawah 12.2 V OFF'
    WHEN b.trans_polutan_awal='4' THEN '- Cust. tolak cek Voltase OFF'
    WHEN b.trans_polutan_awal IS NULL THEN 'Tidak Mengisi'
	ELSE 'Imen Versi Lama'
END AS statusaki
FROM PB_DC.dbo.TransaksiTokoHeader a
LEFT JOIN PB_DC.dbo.imen_trans_header b with(nolock) on b.kode_toko=a.kodeToko and b.trans_id=a.nomorImen
LEFT JOIN PB_DC.dbo.mastertoolstoko c with(nolock) on c.kodetoko=a.kodeToko
LEFT JOIN PB_DC.dbo.SMITransaksiTokoMember d with(nolock) on d.kodetoko=a.kodeToko and d.nomortransaksi=a.nomortransaksi
LEFT JOIN PB_DC.dbo.MstMember e with(nolock) on e.NoPolisi=d.nopolisi
WHERE a.tglBisnis between '2025-09-01' and '2025-09-07'--a.tglBisnis=convert(date,getdate()-1)
and a.kodetoko in ('3021003','3021004','3021005','3021021','3021028','3021033','3021065','3021068','3021069','3021074','3021080','3021095','3021111','3021140','3021177','3021240','3021288','3021382');

--select * FROM PB_DC.dbo.SMITransaksiTokoMember

--select * from PB_DC.dbo.imen_trans_header where kode_toko='3021004'	and trans_id ='1000083852'

--select * from PB_DC.dbo.TransaksiTokoHeader where kodetoko='3021074' and nomortransaksi='202509010065';
--select * from PB_DC.dbo.TransaksiTokoDetail where kodetoko='3021004' and nomortransaksi='202509010001';

--select * from PB_DC.dbo.TransaksiTokoHeader a WHERE a.tglBisnis=convert(date,getdate()-1)

SELECT * FROM public.pra_his_all where transactiondate=current_date-1;
SELECT * FROM public.pra_his where transactiondate=current_date-2;

SELECT * FROM public.car_his where tgltransaksi=current_date-2;

--Report Auto Reminder Accu All Status MTD H-1	
SELECT 
transactiondate as "Transaction Date", 
reportdate as "Report Date", 
namacabang as "Nama Cabang", 
kodetoko as "Kode Toko", 
nomortransaksi as "Nomor Transaksi", 
nopolisi as "No. Polisi", 
namamember as "Nama Member", 
notelp as "NO. WA",
idjenismember as "ID Jenis Member", 
namajenismember as "Nama Jenis Member", 
statusaccu as "Status Accu", 
lasttrxaccu as "last Trx Accu"
FROM public.pra_his_all
WHERE 
CASE
	when to_char(current_date::date,'dd')::character varying = '01'  then (transactiondate between date_trunc('month',current_date - interval '1' month)::date and ((date_trunc('month', current_date) - interval '0 month' - interval '1 day')::date))
	else(transactiondate::date between date_trunc('month', current_date)::date and current_date::date) 
END;

--Report Auto Reminder Accu All Status M-1	
SELECT 
transactiondate as "Transaction Date", 
reportdate as "Report Date", 
namacabang as "Nama Cabang", 
kodetoko as "Kode Toko", 
nomortransaksi as "Nomor Transaksi", 
nopolisi as "No. Polisi", 
namamember as "Nama Member", 
notelp as "NO. WA",
idjenismember as "ID Jenis Member", 
namajenismember as "Nama Jenis Member", 
statusaccu as "Status Accu", 
lasttrxaccu as "last Trx Accu"
FROM public.pra_his_all
WHERE (transactiondate::date >= date_trunc('month',now()::date - interval '1' month) 
AND transactiondate::date < date_trunc('month',now()::date ));



--Report Auto Reminder Detail MTD H-1	
SELECT 
transactiondate as "Transaction Date", 
reportdate as "Report Date", 
namacabang as "Nama Cabang", 
kodetoko as "Kode Toko", 
nomortransaksi as "Nomor Transaksi", 
nopolisi as "No. Polisi", 
namamember as "Nama Member", 
notelp as "No. Telp", 
idjenismember as "ID Jenis Member", 
namajenismember as "Nama Jenis Member", 
statusaccu as "Status Accu", 
lasttrxaccu1 as "Last Transaction Accu 1", 
wareminder1 as "WA Reminder 1", 
reminder1 as "Reminder 1", 
wa_status1 as "WA Status 1", 
wa_send_date1 as "WA Send Date 1", 
wa_xid1 as "WA XID", 
wa_status_data1 as "WA Status Data 1", 
lasttrxaccu2 as "Last Transaction Accu 2", 
wareminder2 as "WA Reminder 2", 
reminder2 as "Reminder 2", 
wa_status2 as "WA Status 2", 
wa_send_date2 as "WA SEND DATE 2", 
wa_xid2 as "WA XID", 
wa_status_data2 as "WA Status Data"
FROM public.pra_his
WHERE 
    transactiondate::DATE BETWEEN 
    CASE
        -- Jika hari ini tanggal 1, ambil awal bulan sebelumnya.
        WHEN EXTRACT(DAY FROM CURRENT_DATE) = 1 THEN DATE_TRUNC('month', CURRENT_DATE - INTERVAL '1 month')::DATE
        -- Selain itu (jika bukan tanggal 1), ambil awal bulan ini.
        ELSE DATE_TRUNC('month', CURRENT_DATE)::DATE
    END
    AND
    -- Tanggal akhir selalu tanggal hari ini.
    CURRENT_DATE::DATE
ORDER BY transactiondate;

--Report Auto Reminder Detail M-1		
SELECT 
transactiondate as "Transaction Date", 
reportdate as "Report Date", 
namacabang as "Nama Cabang", 
kodetoko as "Kode Toko", 
nomortransaksi as "Nomor Transaksi", 
nopolisi as "No. Polisi", 
namamember as "Nama Member", 
notelp as "No. Telp", 
idjenismember as "ID Jenis Member", 
namajenismember as "Nama Jenis Member", 
statusaccu as "Status Accu", 
lasttrxaccu1 as "Last Transaction Accu 1", 
wareminder1 as "WA Reminder 1", 
reminder1 as "Reminder 1", 
wa_status1 as "WA Status 1", 
wa_send_date1 as "WA Send Date 1", 
wa_xid1 as "WA XID", 
wa_status_data1 as "WA Status Data 1", 
lasttrxaccu2 as "Last Transaction Accu 2", 
wareminder2 as "WA Reminder 2", 
reminder2 as "Reminder 2", 
wa_status2 as "WA Status 2", 
wa_send_date2 as "WA SEND DATE 2", 
wa_xid2 as "WA XID", 
wa_status_data2 as "WA Status Data"
FROM public.pra_his
WHERE (transactiondate::date >= date_trunc('month',now()::date - interval '1' month) 
AND transactiondate::date < date_trunc('month',now()::date ))
ORDER BY transactiondate;

select distinct idcabang from public.smi_monitoring_eod_toko

select * FROM public.smi_rms10_Transaksi_Toko_Perjenis_Member_v3 a 

SELECT a.tanggal, a.namacabang, a.kodetoko, a.namatoko, a.nomortransaksi, a.idbrand, a.brand, a.iddivisi, a.jenis, a.idsubdepartement, a.namasubdepartement, a.iddepartement, a.category, a.kodeproduklama, a.idproduk, a.kodeproduk, a.namapendek, a.namapanjang, a.qty, a.hpp, a.subtotalhpp, a.hargajualnormal, a.disc, a.hargajualtransaksi, a.subtotal, 
--a.nopolisi, b.namaMember,b.NoTelp, 
--CASE 
--    WHEN b.notelp IS NOT NULL AND b.notelp ~ '^[0-9]+$'
--    THEN '62' || SUBSTRING(b.notelp FROM 2)
--    ELSE NULL
--END AS notelp,
a.idjenismember, a.namajenismember
FROM SMI_rms10_Transaksi_Toko_Perjenis_Member_v3 a
LEFT JOIN PB_DC.dbo.MstMember b with(nolock) on b.NoPolisi=a.nopolisi
WHERE a.tanggal between '2025-09-01' and '2025-09-11'
		AND a.idjenisproduk <>4
		AND a.statusproduk<>'K'
		AND a.idcabang='2'
		AND a.category='ACCU';


----------------
--UMUM/TANPA NOHP
----------------
----------------------------------------------------------------------
Robotic Jobs Query Template/ PRA ALL STATUS JKT V1
Name	PRA ALL STATUS JKT V1
Title	
Source	POOL/localhost <DATARMS>
Query	
SELECT 
transactiondate as "Transaction Date", 
reportdate as "Report Date", 
namacabang as "Nama Cabang", 
kodetoko as "Kode Toko", 
nomortransaksi as "Nomor Transaksi", 
nopolisi as "No. Polisi", 
namamember as "Nama Member", 
idjenismember as "ID Jenis Member", 
namajenismember as "Nama Jenis Member", 
statusaccu as "Status Accu", 
lasttrxaccu as "last Trx Accu"
FROM public.pra_his_all
WHERE 
CASE
	when to_char(current_date::date,'dd')::character varying = '01'  then (transactiondate between date_trunc('month',current_date - interval '1' month)::date and ((date_trunc('month', current_date) - interval '0 month' - interval '1 day')::date))
	else(transactiondate::date between date_trunc('month', current_date)::date and current_date::date) 
END;
Context	{}
{}
{}
Description	

----------------------------------------------------------------------

Robotic Jobs Template SheetPRA ALL STATUS JKT V1 / - / PRA ALL STATUS JKT V1
Sheet name	PRA ALL STATUS JKT V1
Title	
Name	PRA ALL STATUS JKT V1
Detail Template	
Display Name
/ PRA ALL STATUS JKT V1	 
 	 
 	 
 	 
Paper Format	European A4
Context	{}
{}
{}
Description	
Active	
 

----------------------------------------------------------------------





Dear All,

Ini adalah auto email (Report Auto Reminder Acu All Status secara otomatis)
Mohon kepada user untuk tidak melakukan reply atau kirim email kepada account email ini.
Jika ada tanggapan dari auto email ini.
Silahkan menghubungi kami melalui email it@surganyamotor.co.id
Atau telp di nomor 081510401208

Terima kasih

diganti menjadi :

Dear All,

Email ini dikirim secara otomatis.
Perlu diketahui bahwa link download dalam email ini hanya berlaku selama 7 hari.

Mohon untuk tidak membalas atau mengirim email ke alamat ini.
Jika Anda memiliki pertanyaan atau memerlukan bantuan, silakan hubungi kami melalui:

📧 Email: it@surganyamotor.co.id
📞 Telp/WA: 0815 1040 1208

Terima kasih atas perhatian dan kerja samanya.

Hormat kami,
Tim IT – Surganya Motor Indonesia
