---SQL SERVER
---GET DATA STATUS ACCU
--SELECT 
--a.tglBisnis,c.idcabang,d.nopolisi,e.namaMember,e.notelp,a.kodeToko,a.nomorTransaksi,a.nomorImen,
--CASE
--    WHEN b.trans_polutan_awal='1' THEN 'BAGUS diatas 12.5 V OFF'
--    WHEN b.trans_polutan_awal='2' THEN 'LEMAH antara 12.2 dan 12.5 V OFF'
--    WHEN b.trans_polutan_awal='3' THEN 'RUSAK dibawah 12.2 V OFF'
--    WHEN b.trans_polutan_awal='4' THEN '- Cust. tolak cek Voltase OFF'
--    WHEN b.trans_polutan_awal IS NULL THEN 'Tidak Mengisi'
--	ELSE 'Imen Versi Lama'
--END AS statusaki
--FROM PB_DC.dbo.TransaksiTokoHeader a
--LEFT JOIN PB_DC.dbo.imen_trans_header b with(nolock) on b.kode_toko=a.kodeToko and b.trans_id=a.nomorImen
--LEFT JOIN PB_DC.dbo.mastertoolstoko c with(nolock) on c.kodetoko=a.kodeToko
--LEFT JOIN PB_DC.dbo.SMITransaksiTokoMember d with(nolock) on d.kodetoko=a.kodeToko and d.nomortransaksi=a.nomortransaksi
--LEFT JOIN PB_DC.dbo.MstMember e with(nolock) on e.NoPolisi=d.nopolisi
--WHERE a.tglBisnis=convert(date,getdate()-1);
WITH cteMotor AS (
	 SELECT
		m.nopolisi,
		bm.brandMotor,
		tm.typeMotor,
		vm.varianMotor,
		tmotor.tahunMotor,
		jm.jenisMotor
	FROM PB_DC.dbo.mstmember AS m WITH (NOLOCK)
	LEFT JOIN PB_DC.dbo.SMIMstTahunMotor AS tmotor WITH (NOLOCK) ON tmotor.idTahunMotor = m.idTahunMotor
	LEFT JOIN PB_DC.dbo.SMIMstTypeMotor AS tm WITH (NOLOCK) ON tm.idTypeMotor = m.idTypeMotor
	LEFT JOIN PB_DC.dbo.SMIMstBrandMotor AS bm WITH (NOLOCK) ON bm.idBrandMotor = tm.idBrandMotor
	LEFT JOIN PB_DC.dbo.SMIMstVarianMotor AS vm WITH (NOLOCK) ON vm.idVarianMotor = tmotor.idVarianMotor 
			AND vm.idTypeMotor = m.idTypeMotor AND vm.idTypeMotor = tm.idTypeMotor
	LEFT JOIN PB_DC.dbo.SMIMstJenisMotor AS jm WITH (NOLOCK) ON jm.idJenisMotor = tm.idJenisMotor
),
cteTransaksi AS (
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
JOIN PB_DC.dbo.SMITransaksiTokoPerjenisMember d with(nolock) on d.kodetoko=a.kodeToko and d.nomortransaksi=a.nomortransaksi
LEFT JOIN PB_DC.dbo.MstMember e with(nolock) on e.NoPolisi=d.nopolisi
WHERE a.tglBisnis=convert(date,getdate()-1)
)
SELECT 
    t.tglBisnis,
    t.idcabang,
    t.nopolisi,
    t.namaMember,
    t.notelp,
    t.kodeToko,
    t.nomorTransaksi,
    t.nomorImen,
    t.statusaki
FROM cteTransaksi t
LEFT JOIN cteMotor m ON m.nopolisi = t.nopolisi
WHERE m.jenisMotor <> 'ELECTRIC';



---POSTGRESQL
---TRANSFER RMS - DATARMS
CREATE TABLE public.pra_status_accu (
	insertdate timestamptz DEFAULT now() NOT NULL,
	tglbisnis date NOT NULL,
	idcabang INT8 NOT NULL,
	nopolisi varchar(20) NOT NULL,
	namamember varchar(30) NULL,
	notelp varchar(15) NULL,
    kodetoko INT8 NOT NULL,
    nomortransaksi INT8 NOT NULL,
    nomorimen INT8 NOT NULL,
    statusaccu VARCHAR(50) NOT NULL
);
CREATE INDEX pra_status_accu_idcabang_idx ON public.pra_status_accu USING btree (idcabang, kodetoko, nomortransaksi);

SELECT * FROM public.pra_status_accu;


/*
Transfer Jobspra_status_accu_rms10pra_status_accu
Name	pra_status_accu
  
Schedule Actions	pra_status_accu
Execution Date	09/02/2025 04:30:00
Interval Unit	Days
 
Transfer Jobs
pra_status_accu_rms10	rms10.planetban.co.id <SMI_DB_Reporting_JKT>	POOL/localhost <DATARMS>	09/01/2025 14:36:21		pra_status_accu	OK
*/




--QUERY pra_last_trx_accu
SET TIMEZONE to 'UTC-07:00';
DELETE FROM public.pra_last_trx_accu;
INSERT INTO public.pra_last_trx_accu
EXPLAIN ANALYZE
SELECT now()insertdate,idcabang,namacabang,nopolisi,category,tanggal
FROM(
	SELECT a.tanggal,a.nopolisi,a.idcabang,a.namacabang,a.kodetoko,a.category,a.nomortransaksi,row_number() OVER (PARTITION BY a.nopolisi,a.category ORDER BY a.tanggal DESC) AS myrank
	FROM public.smi_rms10_Transaksi_Toko_Perjenis_Member_v3 a
	WHERE a.idcabang = 2
	AND a.tanggal >= (current_date - interval '1 day') - interval '36 months'
	AND a.tanggal <= current_date - interval '1 day'
	AND a.category in ('ACCU')
	AND (a.nopolisi IS NOT NULL AND a.nopolisi <> '')
	) as category_oil
where myrank=1;

--TEMP TABLE pra_last_trx_accu1
CREATE TABLE public.pra_last_trx_accu (
	insertdate timestamptz DEFAULT now() NOT NULL,
	idcabang INT8 NOT NULL,
	namacabang varchar(20) NOT NULL,
	nopolisi varchar(20) NOT NULL,
	category varchar(50) NOT NULL,
	tanggal date NOT NULL,
	CONSTRAINT pra_last_trx_accu_pk PRIMARY KEY (idcabang, nopolisi, category, tanggal)
);
CREATE INDEX pra_last_trx_accu_namacabang_idx ON public.pra_last_trx_accu USING btree (idcabang, nopolisi, category, tanggal);

SELECT * FROM public.pra_last_trx_accu;

/*
Scheduled DML Querypra_last_trx_accu
Name	pra_last_trx_accu
  
Schedule Actions	pra_last_trx_accu
Execution Date	09/02/2025 07:00:00
Interval Unit	Days
 
DML Query Jobs
pra_last_trx_accu_rms10	POOL/localhost <DATARMS>	09/01/2025 14:44:36		pra_last_trx_accu	OK
*/




--BUILD DATA
WITH 
cte_transaksi AS (
    SELECT DISTINCT
        a.tanggal,
        a.idcabang,
        a.namacabang,
        a.kodetoko,
        a.nomortransaksi,
        a.nopolisi,
        a.idjenismember,
        a.namajenismember
    FROM PUBLIC.smi_rms10_transaksi_toko_perjenis_member_v3 a
    WHERE a.kodetoko in ('3021003','3021004','3021005','3021021','3021028','3021033','3021065','3021068','3021069','3021074','3021080','3021095','3021111','3021140','3021177','3021240','3021288','3021382') 
    AND a.tanggal::date=current_date-1
),
cte_mstmember AS (
    SELECT 
        b.nopolisi,
        b.namamember,
        b.notelp
    FROM public.mstmember b
),
cte_statusaccu AS (
    SELECT 
        c.tglbisnis,
        c.idcabang,
        c.kodetoko,
        c.nomortransaksi,
        c.nomorimen,
        c.statusaccu 
    FROM public.pra_status_accu c
    WHERE c.statusaccu LIKE 'LEMAH%' OR c.statusaccu LIKE 'RUSAK%'
),
cte_lasttrxaccu AS (
	SELECT insertdate, idcabang, nopolisi, category, tanggal
	FROM public.pra_last_trx_accu
)
SELECT 
    x.tanggal::date as transactiondate,
    NOW()::date as reportdate,
    x.namacabang,
    x.kodetoko,
    x.nomortransaksi,
    x.nopolisi,
    m.namamember,
    '62' || SUBSTRING(m.notelp FROM 2) AS notelp,
    x.idjenismember,
    x.namajenismember,
    s.statusaccu,
    l.tanggal::date as lasttrxaccu1,
    CASE
        WHEN (x.tanggal::date > l.tanggal::date AND s.statusaccu IS NOT NULL AND s.statusaccu <> '')
          OR (l.tanggal IS NULL AND s.statusaccu IS NOT NULL AND s.statusaccu <> '')
        THEN 'YES'
        ELSE 'NO'
    END AS wareminder1,
    CASE
        WHEN 
            ((x.tanggal::date > l.tanggal::date AND s.statusaccu IS NOT NULL AND s.statusaccu <> '')
            OR (l.tanggal IS NULL AND s.statusaccu IS NOT NULL AND s.statusaccu <> ''))
        THEN 
            CASE
                WHEN s.statusaccu LIKE 'RUSAK%' THEN (x.tanggal + INTERVAL '7 days')::date
                WHEN s.statusaccu LIKE 'LEMAH%' THEN (x.tanggal + INTERVAL '14 days')::date
                ELSE x.tanggal::date
            END
        ELSE NULL
    END AS reminder1,
    0 as wa_status1,
    null as wa_send_date1,
    null as wa_xid1,
    null as wa_status_data1,
    null as lasttrxaccu2,
    null as wareminder2,
    CASE
        WHEN 
            ((x.tanggal::date > l.tanggal::date AND s.statusaccu IS NOT NULL AND s.statusaccu <> '')
            OR (l.tanggal IS NULL AND s.statusaccu IS NOT NULL AND s.statusaccu <> ''))
        THEN 
            CASE
                WHEN s.statusaccu LIKE 'RUSAK%' THEN (x.tanggal + INTERVAL '21 days')::date
                WHEN s.statusaccu LIKE 'LEMAH%' THEN (x.tanggal + INTERVAL '28 days')::date
                ELSE (x.tanggal + INTERVAL '14 days')::date
            END
        ELSE NULL
    END AS reminder2,
    0 as wa_status2,
    null as wa_send_date2,
    null as wa_xid2,
    null as wa_status_data2
FROM cte_transaksi x
LEFT JOIN cte_mstmember m ON x.nopolisi = m.nopolisi
LEFT JOIN cte_statusaccu s ON x.idcabang = s.idcabang AND x.kodetoko = s.kodetoko AND x.nomortransaksi = s.nomortransaksi
LEFT JOIN cte_lasttrxaccu l ON x.idcabang = l.idcabang AND x.nopolisi = l.nopolisi ;


--
--wareminder1 = statusaccu+lastrxaccualter = YES 
--			= statusaccu+kosongalter 	 = YES		
--			reminder1 mengikuti formula
			

--TEMP TABLE pra_his_all
CREATE TABLE public.pra_his_all (
	transactiondate date NOT NULL,
	reportdate date NOT NULL,
	namacabang varchar(20) NOT NULL,
	kodetoko BIGINT NOT NULL,
	nomortransaksi BIGINT NOT NULL,
	nopolisi varchar(20) NOT NULL,
	namamember varchar(30),
	notelp varchar(15),
	idjenismember INT,
	namajenismember varchar(30),
	statusaccu varchar(50),
	lasttrxaccu date NULL,
	CONSTRAINT pra_his_all_pk PRIMARY KEY (transactiondate,namacabang,nopolisi,kodetoko,nomortransaksi)
);

SELECT * FROM public.pra_his_all;

--TEMP TABLE pra_his
CREATE TABLE public.pra_his (
	transactiondate date NOT NULL,
	reportdate date NOT NULL,
	namacabang varchar(20) NOT NULL,
	kodetoko INT8 NOT NULL,
	nomortransaksi INT8 NOT NULL,
	nopolisi varchar(20) NOT NULL,
	namamember varchar(30) NULL,
	notelp varchar(15) NULL,
	idjenismember int4 NULL,
	namajenismember varchar(30) NULL,
	statusaccu varchar(50) NULL,
	lasttrxaccu1 date NULL,
	wareminder1 varchar(3) NULL,
	reminder1 date NULL,
	wa_status1 int4 DEFAULT 0 NULL,
	wa_send_date1 timestamp NULL,
	wa_xid1 varchar(150) NULL,
	wa_status_data1 varchar(10) NULL,
	lasttrxaccu2 date NULL,
	wareminder2 varchar(3) NULL,
	reminder2 date NULL,
	wa_status2 int4 DEFAULT 0 NULL,
	wa_send_date2 timestamp NULL,
	wa_xid2 varchar(150) NULL,
	wa_status_data2 varchar(10) NULL,
	send1 varchar(10) NULL,
	send2 varchar(10) NULL,
	CONSTRAINT pra_his_pk PRIMARY KEY (transactiondate,namacabang,nopolisi,kodetoko,nomortransaksi )
);

SELECT * FROM public.pra_his;
	
	
SELECT * FROM public.pra_status_accu;
SELECT * FROM public.pra_last_trx_accu;
SELECT * FROM public.pra_his_all;
SELECT * FROM public.pra_his;

--INSERT INTO public.pra_his
--SELECT * FROM public.pra_his_test;

--CEK UPDATE SEND1
select GREATEST((ph.reminder1::date - CURRENT_DATE), 0) as send1
FROM public.pra_his_test ph
WHERE ph.reminder1 IS NOT NULL
AND (ph.reminder1::date - CURRENT_DATE) > 0;

--UPDATE SEND1
UPDATE public.pra_his_test ph
SET send1 = GREATEST((ph.reminder1::date - CURRENT_DATE), 0)
WHERE ph.reminder1 IS NOT NULL
AND (ph.reminder1::date - CURRENT_DATE) > 0;

--CEK UPDATE SEND1
select GREATEST((ph.reminder2::date - CURRENT_DATE), 0) as send2
FROM public.pra_his_test ph
WHERE ph.reminder2 IS NOT NULL
AND (ph.reminder2::date - CURRENT_DATE) > 0;

--UPDATE SEND2
UPDATE public.pra_his_test ph
SET send2 = GREATEST((ph.reminder2::date - CURRENT_DATE), 0)
WHERE ph.reminder2 IS NOT null
AND (ph.reminder2::date - CURRENT_DATE) > 0;














--
----Versi Optimized pakai DISTINCT ON
--EXPLAIN ANALYZE
--SELECT 
--    now() AS insertdate,
--    a.namacabang,
--    a.nopolisi,
--    a.category,
--    a.tanggal
--FROM (
--    SELECT DISTINCT ON (a.nopolisi, a.category)
--        a.tanggal,
--        a.nopolisi,
--        a.namacabang,
--        a.kodetoko,
--        a.category,
--        a.nomortransaksi
--    FROM public.smi_rms10_transaksi_toko_perjenis_member_v3 a
--    WHERE a.idcabang = 2
--      AND a.tanggal >= (current_date - interval '1 day') - interval '36 months'
--      AND a.tanggal <= current_date - interval '1 day'
--      AND a.category = 'ACCU'
--      AND a.nopolisi IS NOT NULL 
--      AND a.nopolisi <> ''
--    ORDER BY a.nopolisi, a.category, a.tanggal DESC
--) a;
