--------------------------------------------------------------------------------------------------------------------------POSTGRESQL


-- public.mb_rpt_goliaht_last_trx_monthly_temp definition

-- Drop table

-- DROP TABLE public.mb_rpt_goliaht_last_trx_monthly_temp;

CREATE TABLE public.mb_rpt_goliaht_last_trx_monthly_temp (
	insertdate timestamptz NOT NULL,
	tanggal date NULL,
	nopolisi varchar(20) NULL,
	namacabang varchar(40) NULL,
	kodetoko int8 NULL,
	nomortransaksi int8 NULL
);
CREATE UNIQUE INDEX mb_rpt_goliaht_last_trx_monthly_temp_idx ON public.mb_rpt_goliaht_last_trx_monthly_temp USING btree (tanggal, nopolisi, namacabang);


--------------------------------------------------------------------------------------------------------------------------


-- DROP FUNCTION public.fc_mb_rpt_goliaht_last_trx_monthly(text, text);

CREATE OR REPLACE FUNCTION public.fc_mb_rpt_goliaht_last_trx_monthly(temp_table text, source_table text)
 RETURNS void
 LANGUAGE plpgsql
AS $function$
DECLARE 
    dynamic_query TEXT;
BEGIN 
    dynamic_query := format(
        'INSERT INTO %I (insertdate, tanggal, nopolisi, namacabang, kodetoko, nomortransaksi)
         SELECT now() AS insertdate, b.tanggal, b.nopolisi, b.namacabang, b.kodetoko, b.nomortransaksi
         FROM (
            SELECT DISTINCT a.tanggal, a.nopolisi, a.namacabang, a.kodetoko, a.nomortransaksi, 
                   row_number() OVER (PARTITION BY a.nopolisi, a.namacabang ORDER BY a.tanggal DESC) AS myrank
            FROM public.mb_%s_transaksi_toko_perjenis_member_v3 a
            WHERE (a.tanggal >= (date_trunc(''month'', current_date) - interval ''1 month'')::date
              AND a.tanggal <= (date_trunc(''month'', current_date) - interval ''1 day'')::date)
              AND a.idjenisproduk <> 4 
              AND a.statusproduk <> ''K''::bpchar
              AND (a.nopolisi IS NOT NULL AND a.nopolisi <> '''')
         ) AS b
         WHERE b.myrank = 1', temp_table, quote_ident(source_table));

    EXECUTE dynamic_query;
END;
$function$
;


--------------------------------------------------------------------------------------------------------------------------


Open: Scheduled DML Query Job
Jobs name
1.delete_mb_rpt_goliaht_last_trx_monthly_temp
Target	POOL/localhost <DATARMS>
  
Last Run	05/03/2025 13:00:27
Schedule DML Query	MB_RPT_GOLIAHT_M1_V2
 
DML Query
delete from public.mb_rpt_goliaht_last_trx_monthly_temp;


--------------------------------------------------------------------------------------------------------------------------


Open: Scheduled DML Query Job
Jobs name
8.insert_fc_mb_rpt_goliaht_last_trx_monthly_rms10
Target	POOL/localhost <DATARMS>
  
Last Run	05/03/2025 13:01:06
Schedule DML Query	MB_RPT_GOLIAHT_M1_V2
 
DML Query
SELECT * from public.fc_mb_rpt_goliaht_last_trx_monthly('mb_rpt_goliaht_last_trx_monthly_temp','rms10');


--------------------------------------------------------------------------------------------------------------------------


Open: Scheduled DML Query Job
Jobs name
4.insert_fc_mb_rpt_goliaht_last_trx_monthly_rms20
Target	POOL/localhost <DATARMS>
  
Last Run	05/03/2025 13:00:41
Schedule DML Query	MB_RPT_GOLIAHT_M1_V2
 
DML Query
SELECT * from public.fc_mb_rpt_goliaht_last_trx_monthly('mb_rpt_goliaht_last_trx_monthly_temp','rms20');


--------------------------------------------------------------------------------------------------------------------------


Name	REPORT MONITORING GOLIAHT V2 SUMMARY
Title	
Source	POOL/localhost <DATARMS>
Query	
WITH
summarydetail as
(
	SELECT 
	    x.namacabang AS "Nama Cabang",
	    x.kolom1 AS "Akumulasi N/A",  -- Menghitung N/A
	    x.kolom2 AS "Akumulasi Oke",  -- Menghitung Oke
	    x.kolom3 AS "Akumulasi Suspect",  -- Menghitung Suspect
	    x.kolom4 AS "Akumulasi Total All",  -- Menghitung Total all
	    (x.kolom2 + x.kolom3) AS "Akumulasi Total eligible remark",  -- Menghitung Total eligible remarks
	    ROUND(COALESCE((CAST(x.kolom2 AS float) / NULLIF((x.kolom2 + x.kolom3), 0)) * 100, 0)::numeric, 2) AS "Akumulasi % Oke vs eligible data",  -- Menghitung % Oke vs eligible data
	    ROUND(COALESCE((CAST(x.kolom3 AS float) / NULLIF((x.kolom2 + x.kolom3), 0)) * 100, 0)::numeric, 2) AS "Akumulasi % Suspect vs eligible data",  -- % Suspect vs eligible data
	    x.kolom5 AS "Summary Suspect Terlalu Besar",  -- Menghitung Terlalu Besar
	    x.kolom6 AS "Summary Suspect Negatif",  -- Menghitung Negatif
	    x.kolom7 AS "Summary Suspect Tidak Bergerak",  -- Menghitung Tidak Bergerak
	    x.kolom8 AS "Summary Suspect Total",  -- Menghitung Total
	    ROUND(COALESCE((CAST(x.kolom5 AS float) / NULLIF(CAST(x.kolom8 AS float), 0)) * 100, 0)::numeric, 2) AS "Summary Suspect % terlalu besar",  -- % terlalu besar	
	    ROUND(COALESCE((CAST(x.kolom6 AS float) / NULLIF(CAST(x.kolom8 AS float), 0)) * 100, 0)::numeric, 2) AS "Summary Suspect % Negatif",  -- % Negatif	
	    ROUND(COALESCE((CAST(x.kolom7 AS float) / NULLIF(CAST(x.kolom8 AS float), 0)) * 100, 0)::numeric, 2) AS "Summary Suspect % Tidak bergerak"  -- % Tidak bergerak
	FROM (	
	    SELECT 
	        a.namacabang,
	        COUNT(CASE WHEN a.kolom_nrpt = 'N/A' THEN 1 END) AS kolom1,  -- Menghitung N/A
	        COUNT(CASE WHEN a.kolom_nrpt = 'Oke' THEN 1 END) AS kolom2,  -- Menghitung Oke
	        COUNT(CASE WHEN a.kolom_nrpt = 'Suspect' THEN 1 END) AS kolom3,  -- Menghitung Suspect
	        COUNT(CASE WHEN a.kolom_nrpt IN ('N/A', 'Oke', 'Suspect') THEN 1 END) AS kolom4,  -- Menghitung Total all
	        COUNT(CASE WHEN a.kolom_orpt = 'Suspect' THEN 1 END) AS kolom5,  -- Menghitung Terlalu Besar
	        COUNT(CASE WHEN a.kolom_prpt = 'Suspect' THEN 1 END) AS kolom6,  -- Menghitung Negatif
	        COUNT(CASE WHEN a.kolom_qrpt = 'Suspect' THEN 1 END) AS kolom7,  -- Menghitung Tidak Bergerak
	        COUNT(CASE WHEN a.kolom_orpt = 'Suspect' THEN 1 END) + COUNT(CASE WHEN a.kolom_prpt = 'Suspect' THEN 1 END) + COUNT(CASE WHEN a.kolom_qrpt = 'Suspect' THEN 1 END) AS kolom8  -- Menghitung Total
	    FROM public.mb_trx_oil_goliaht_monitoring_all a
	    JOIN public.mb_rpt_goliaht_last_trx_monthly_temp b on b.namacabang=a.namacabang and b.nopolisi=a.kolom_c
	    WHERE a.kolom_d::date = current_date-1
	   -- WHERE a.kolom_d::date='2025-03-31'
	    GROUP BY a.namacabang
	) AS x
),
summarytotal as 
	(
	SELECT 
	    'Total' AS "Nama Cabang",
	    SUM(kolom1) AS "Akumulasi N/A",
	    SUM(kolom2) AS "Akumulasi Oke",
	    SUM(kolom3) AS "Akumulasi Suspect",
	    SUM(kolom4) AS "Akumulasi Total All",
	    SUM(kolom2 + kolom3) AS "Akumulasi Total eligible remark",
	    ROUND(COALESCE((SUM(kolom2) / NULLIF((SUM(kolom2) + SUM(kolom3)), 0)) * 100, 0)::numeric, 2) AS "Akumulasi % Oke vs eligible data",
	    ROUND(COALESCE((SUM(kolom3) / NULLIF((SUM(kolom2) + SUM(kolom3)), 0)) * 100, 0)::numeric, 2) AS "Akumulasi % Suspect vs eligible data",
	    SUM(kolom5) AS "Summary Suspect Terlalu Besar",
	    SUM(kolom6) AS "Summary Suspect Negatif",
	    SUM(kolom7) AS "Summary Suspect Tidak Bergerak",
	    SUM(kolom8) AS "Summary Suspect Total",
	    ROUND(COALESCE((SUM(kolom5) / NULLIF(SUM(kolom8), 0)) * 100, 0)::numeric, 2) AS "Summary Suspect % terlalu besar",
	    ROUND(COALESCE((SUM(kolom6) / NULLIF(SUM(kolom8), 0)) * 100, 0)::numeric, 2) AS "Summary Suspect % Negatif",
	    ROUND(COALESCE((SUM(kolom7) / NULLIF(SUM(kolom8), 0)) * 100, 0)::numeric, 2) AS "Summary Suspect % Tidak bergerak"
	FROM (
	    SELECT a.namacabang,a.kolom_c,
	        COUNT(CASE WHEN a.kolom_nrpt = 'N/A' THEN 1 END) AS kolom1,
	        COUNT(CASE WHEN a.kolom_nrpt = 'Oke' THEN 1 END) AS kolom2,
	        COUNT(CASE WHEN a.kolom_nrpt = 'Suspect' THEN 1 END) AS kolom3,
	        COUNT(CASE WHEN a.kolom_nrpt IN ('N/A', 'Oke', 'Suspect') THEN 1 END) AS kolom4,
	        COUNT(CASE WHEN a.kolom_orpt = 'Suspect' THEN 1 END) AS kolom5,
	        COUNT(CASE WHEN a.kolom_prpt = 'Suspect' THEN 1 END) AS kolom6,
	        COUNT(CASE WHEN a.kolom_qrpt = 'Suspect' THEN 1 END) AS kolom7,
	        COUNT(CASE WHEN a.kolom_orpt = 'Suspect' THEN 1 END) + COUNT(CASE WHEN a.kolom_prpt = 'Suspect' THEN 1 END) + COUNT(CASE WHEN a.kolom_qrpt = 'Suspect' THEN 1 END) AS kolom8  
	    FROM public.mb_trx_oil_goliaht_monitoring_all a
	    JOIN public.mb_rpt_goliaht_last_trx_monthly_temp b on b.namacabang=a.namacabang and b.nopolisi=a.kolom_c
	    WHERE a.kolom_d::date = current_date-1
	   -- WHERE a.kolom_d::date='2025-03-31'
	    GROUP BY a.namacabang,a.kolom_c
	) AS y
)
SELECT --PerCabang
    x."Nama Cabang",
    x."Akumulasi N/A",  -- Menghitung N/A
    x."Akumulasi Oke",  -- Menghitung Oke
    x."Akumulasi Suspect",  -- Menghitung Suspect
    x."Akumulasi Total All",  -- Menghitung Total all
    x."Akumulasi Total eligible remark",  -- Menghitung Total eligible remarks
    x."Akumulasi % Oke vs eligible data",  -- Menghitung % Oke vs eligible data
    x."Akumulasi % Suspect vs eligible data",  -- % Suspect vs eligible data
    x."Summary Suspect Terlalu Besar",  -- Menghitung Terlalu Besar
    x."Summary Suspect Negatif",  -- Menghitung Negatif
    x."Summary Suspect Tidak Bergerak",  -- Menghitung Tidak Bergerak
    x."Summary Suspect Total",  -- Menghitung Total
    x."Summary Suspect % terlalu besar",  -- % terlalu besar	
    x."Summary Suspect % Negatif",  -- % Negatif	
    x."Summary Suspect % Tidak bergerak"  -- % Tidak bergerak
FROM summarydetail x
UNION ALL
SELECT --SummaryAllCabang 
    y."Nama Cabang",
    y."Akumulasi N/A",  -- Menghitung N/A
    y."Akumulasi Oke",  -- Menghitung Oke
    y."Akumulasi Suspect",  -- Menghitung Suspect
    y."Akumulasi Total All",  -- Menghitung Total all
    y."Akumulasi Total eligible remark",  -- Menghitung Total eligible remarks
    y."Akumulasi % Oke vs eligible data",  -- Menghitung % Oke vs eligible data
    y."Akumulasi % Suspect vs eligible data",  -- % Suspect vs eligible data
    y."Summary Suspect Terlalu Besar",  -- Menghitung Terlalu Besar
    y."Summary Suspect Negatif",  -- Menghitung Negatif
    y."Summary Suspect Tidak Bergerak",  -- Menghitung Tidak Bergerak
    y."Summary Suspect Total",  -- Menghitung Total
    y."Summary Suspect % terlalu besar",  -- % terlalu besar	
    y."Summary Suspect % Negatif",  -- % Negatif	
    y."Summary Suspect % Tidak bergerak"  -- % Tidak bergerak
FROM summarytotal y;
Context	{}
Description	 


--------------------------------------------------------------------------------------------------------------------------


Name	REPORT MONITORING GOLIAHT V2 DETAIL JKT
Title	
Source	POOL/localhost <DATARMS>
Query	
WITH
mb_trx_oil_goliaht_monitoring_all as 
(SELECT 
a.namacabang,a.wa,a.kolom_b, 
a.kolom_c,a.kolom_d,a.kolom_e,a.kolom_f,a.kolom_g,a.kolom_h,a.kolom_i,a.kolom_j,a.kolom_k,a.kolom_l,a.kolom_m,a.kolom_nrpt, 
a.kolom_orpt,a.kolom_prpt,a.kolom_qrpt,a.kolom_rrpt,a.kolom_srpt,a.kolom_trpt,a.kolom_urpt,a.kolom_vrpt, 
a.kolom_n,a.kolom_o,a.kolom_p1,a.kolom_p10,a.kolom_aarpt,a.kolom_abrpt,a.kolom_p,a.kolom_q,a.kolom_r,a.kolom_s, 
a.kolom_t1,a.kolom_t10,a.kolom_airpt,a.kolom_ajrpt,a.kolom_t,a.kolom_u,a.kolom_u1,a.kolom_u4, 
a.kolom_u2,a.kolom_u10,a.kolom_aqrpt,a.kolom_arrpt,a.kolom_v,a.kolom_w,a.kolom_x,a.kolom_y, 
a.kolom_z,a.kolom_aa,a.kolom_ab,a.kolom_ac,a.kolom_ad,a.kolom_ae,a.kolom_af,a.kolom_ag, 
a.kolom_ah,a.kolom_ai,a.kolom_aj,a.kolom_ak,a.kolom_al,a.kolom_am
FROM public.mb_trx_oil_goliaht_monitoring_all a
where a.kolom_d::date=current_date-1
-- WHERE a.kolom_d::date='2025-03-31'
and a.namacabang='Jakarta Baru'
),
lasttrx as 
(
SELECT tanggal, nopolisi, namacabang, kodetoko, nomortransaksi FROM mb_rpt_goliaht_last_trx_monthly_temp
)
select 
b.tanggal as "Tgl Transaksi Terkahir",
a.namacabang as "Nama Cabang", 
--a.wa as "No. Telp", 
a.kolom_b as "Nama Pelanggan", 
a.kolom_c as "Plat Nomor", 
a.kolom_d as "Tanggal Report (Hari Ini)", 
a.kolom_e as "Tipe Oli Terakhir Penggantian", 
a.kolom_f as "Ketahanan Oli (KM)", 
a.kolom_g as "Jumlah Hari Maksimal Untuk Penggantian Oli", 
a.kolom_h as "90% Ketahanan Oli (KM)", 
a.kolom_i as "Minimal Jarak Berkendara Per Hari (KM)", 
a.kolom_j as "Tipe Pekerjaan", 
a.kolom_k as "Estimasi Jarak Berkendara Ke Tempat Kerja (KM) Fase 1", 
a.kolom_l as "Est Jarak Berkendara / Day Terkait Pekerjaan Fase 1", 
a.kolom_m as "Jumlah hari reminder (Oli Terakhir vs Jarak Tempuh Fase 1)", 
a.kolom_nrpt as "Status Keseluruhan (Suspect/Oke)", 
a.kolom_orpt as "Suspect Odometer Bergerak Terlalu Besar (Suspect/Oke)", 
a.kolom_prpt as "Suspect Odometer Bergerak Negatif (Suspect/Oke)", 
a.kolom_qrpt as "Suspect Odometer Tidak Bergerak (Suspect/Oke)", 
a.kolom_rrpt as "Batas Maksimal Pergerakan Odometer (KM)", 
a.kolom_srpt as "Pergerakan Odometer (KM) Berdasarkan 2 Transaksi Terakhir", 
a.kolom_trpt as "Jarak Hari Transaksi Yang Digunakan", 
a.kolom_urpt as "Jarak Hari (Last Trans Oli vs Trans 2 Terakhir Oli / Non Oli)", 
a.kolom_vrpt as "Jarak Hari (Last Trans Oli vs Trans Terakhir Oli / Non Oli) ", 
a.kolom_n as "Tanggal Transaksi Oli Terakhir", 
a.kolom_o as "Odometer Saat Transaksi Oli Terakhir", 
a.kolom_p1 as "Store Code Transaksi Oli Atau Non-Oli Terakhir", 
a.kolom_p10 as "Nama Toko Transaksi Oli Atau Non-Oli Terakhir", 
a.kolom_aarpt as "Nama RH Transaksi Oli Atau Non-Oli Terakhir", 
a.kolom_abrpt as "Cabang Toko Transaksi Oli Atau Non-Oli Terakhir", 
a.kolom_p as "Tanggal Transaksi Oli Atau Non-Oli Terakhir", 
a.kolom_q as "Odometer Saat Tanggal Transaksi Oli Atau Non-Oli Terakhir", 
a.kolom_r as "Tanggal Saat Transaksi Oli Kedua Dari Terakhir", 
a.kolom_s as "Odometer Saat Transaksi Oli Kedua Dari Terakhir", 
a.kolom_t1 as "Store Code Transaksi Oli Atau Non-Oli Kedua Dari Terakhir", 
a.kolom_t10 as "Nama Toko Transaksi Oli Atau Non-Oli Kedua Dari Terakhir", 
a.kolom_airpt as "Nama RH Transaksi Oli Atau Non-Oli Kedua Dari Terakhir", 
a.kolom_ajrpt as "Cabang Toko Transaksi Oli Atau Non-Oli Kedua Dari Terakhir", 
a.kolom_t as "Tanggal Transaksi Oli Atau Non-Oli Kedua Dari Terakhir", 
a.kolom_u as "Odometer Trans Oli / Non Oli ke 2 Terakhir", 
a.kolom_u1 as "Tanggal Transaksi Oli Atau Non-Oli Ketiga Dari Terakhir", 
a.kolom_u4 as "Odometer Trans Oli / Non Oli ke 3 Terakhir", 
a.kolom_u2 as "Store Code Transaksi Oli Atau Non-Oli Ketiga Dari Terakhir", 
a.kolom_u10 as "Nama Toko Transaksi Oli Atau Non-Oli Ketiga Dari Terakhir", 
a.kolom_aqrpt as "Nama RH Transaksi Oli Atau Non-Oli Ketiga Dari Terakhir", 
a.kolom_arrpt as "Cabang Toko Transaksi Oli Atau Non-Oli Ketiga Dari Terakhir", 
a.kolom_v as "Asumsi Maksimal Rata2 Berkendara Per Hari (KM)", 
a.kolom_w as "Asumsi Minimal Hari Reminder", 
a.kolom_x as "Est Jarak Berkendara / Day (KM) (Antara Metode Fase 1 & 2)", 
a.kolom_y as "Estimasi Jarak Berkendara Per Hari (KM) Yang Digunakan (Antara Metode Fase 1 & 2)", 
a.kolom_z as "Jumlah Hari Reminder (Oli Terakhir vs Jarak Tempuh Fase 2)",
a.kolom_aa as "Tanggal Reminder Pertama", 
a.kolom_ab as "Jumlah Hari Reminder Sejak Penggantian Oli Terakhir", 
a.kolom_ac as "Jumlah Hari Untuk ke Reminder Pertama", 
a.kolom_ad as "Status Reminder Pertama", 
a.kolom_ae as "Tanggal Reminder Pertama Dilakukan", 
a.kolom_af as "Jarak Hari Dari Reminder Pertama ke Reminder Kedua", 
a.kolom_ag as "Tanggal Reminder Kedua", 
a.kolom_ah as "Jumlah Hari Untuk ke Reminder Kedua", 
a.kolom_ai as "Status Reminder Kedua", 
a.kolom_aj as "Tanggal Reminder Kedua Dilakukan", 
a.kolom_ak as "Jarak Hari Dari Reminder Kedua ke CVI Loyalty", 
a.kolom_al as "Jumlah Hari Untuk ke CVI Loyalty", 
a.kolom_am as "Masuk CVI Loyalty (Masuk/Tidak Masuk)"
from mb_trx_oil_goliaht_monitoring_all a
join lasttrx b on b.namacabang=a.namacabang and b.nopolisi=a.kolom_c;
Context	{}
Description	 


--------------------------------------------------------------------------------------------------------------------------


Name	REPORT MONITORING GOLIAHT V2 DETAIL SBY
Title	
Source	POOL/localhost <DATARMS>
Query	
WITH
mb_trx_oil_goliaht_monitoring_all as 
(SELECT 
a.namacabang,a.wa,a.kolom_b, 
a.kolom_c,a.kolom_d,a.kolom_e,a.kolom_f,a.kolom_g,a.kolom_h,a.kolom_i,a.kolom_j,a.kolom_k,a.kolom_l,a.kolom_m,a.kolom_nrpt, 
a.kolom_orpt,a.kolom_prpt,a.kolom_qrpt,a.kolom_rrpt,a.kolom_srpt,a.kolom_trpt,a.kolom_urpt,a.kolom_vrpt, 
a.kolom_n,a.kolom_o,a.kolom_p1,a.kolom_p10,a.kolom_aarpt,a.kolom_abrpt,a.kolom_p,a.kolom_q,a.kolom_r,a.kolom_s, 
a.kolom_t1,a.kolom_t10,a.kolom_airpt,a.kolom_ajrpt,a.kolom_t,a.kolom_u,a.kolom_u1,a.kolom_u4, 
a.kolom_u2,a.kolom_u10,a.kolom_aqrpt,a.kolom_arrpt,a.kolom_v,a.kolom_w,a.kolom_x,a.kolom_y, 
a.kolom_z,a.kolom_aa,a.kolom_ab,a.kolom_ac,a.kolom_ad,a.kolom_ae,a.kolom_af,a.kolom_ag, 
a.kolom_ah,a.kolom_ai,a.kolom_aj,a.kolom_ak,a.kolom_al,a.kolom_am
FROM public.mb_trx_oil_goliaht_monitoring_all a
where a.kolom_d::date=current_date-1
-- WHERE a.kolom_d::date='2025-03-31'
and a.namacabang='Surabaya'
),
lasttrx as 
(
SELECT tanggal, nopolisi, namacabang, kodetoko, nomortransaksi FROM mb_rpt_goliaht_last_trx_monthly_temp
)
select 
b.tanggal as "Tgl Transaksi Terkahir",
a.namacabang as "Nama Cabang", 
--a.wa as "No. Telp", 
a.kolom_b as "Nama Pelanggan", 
a.kolom_c as "Plat Nomor", 
a.kolom_d as "Tanggal Report (Hari Ini)", 
a.kolom_e as "Tipe Oli Terakhir Penggantian", 
a.kolom_f as "Ketahanan Oli (KM)", 
a.kolom_g as "Jumlah Hari Maksimal Untuk Penggantian Oli", 
a.kolom_h as "90% Ketahanan Oli (KM)", 
a.kolom_i as "Minimal Jarak Berkendara Per Hari (KM)", 
a.kolom_j as "Tipe Pekerjaan", 
a.kolom_k as "Estimasi Jarak Berkendara Ke Tempat Kerja (KM) Fase 1", 
a.kolom_l as "Est Jarak Berkendara / Day Terkait Pekerjaan Fase 1", 
a.kolom_m as "Jumlah hari reminder (Oli Terakhir vs Jarak Tempuh Fase 1)", 
a.kolom_nrpt as "Status Keseluruhan (Suspect/Oke)", 
a.kolom_orpt as "Suspect Odometer Bergerak Terlalu Besar (Suspect/Oke)", 
a.kolom_prpt as "Suspect Odometer Bergerak Negatif (Suspect/Oke)", 
a.kolom_qrpt as "Suspect Odometer Tidak Bergerak (Suspect/Oke)", 
a.kolom_rrpt as "Batas Maksimal Pergerakan Odometer (KM)", 
a.kolom_srpt as "Pergerakan Odometer (KM) Berdasarkan 2 Transaksi Terakhir", 
a.kolom_trpt as "Jarak Hari Transaksi Yang Digunakan", 
a.kolom_urpt as "Jarak Hari (Last Trans Oli vs Trans 2 Terakhir Oli / Non Oli)", 
a.kolom_vrpt as "Jarak Hari (Last Trans Oli vs Trans Terakhir Oli / Non Oli) ", 
a.kolom_n as "Tanggal Transaksi Oli Terakhir", 
a.kolom_o as "Odometer Saat Transaksi Oli Terakhir", 
a.kolom_p1 as "Store Code Transaksi Oli Atau Non-Oli Terakhir", 
a.kolom_p10 as "Nama Toko Transaksi Oli Atau Non-Oli Terakhir", 
a.kolom_aarpt as "Nama RH Transaksi Oli Atau Non-Oli Terakhir", 
a.kolom_abrpt as "Cabang Toko Transaksi Oli Atau Non-Oli Terakhir", 
a.kolom_p as "Tanggal Transaksi Oli Atau Non-Oli Terakhir", 
a.kolom_q as "Odometer Saat Tanggal Transaksi Oli Atau Non-Oli Terakhir", 
a.kolom_r as "Tanggal Saat Transaksi Oli Kedua Dari Terakhir", 
a.kolom_s as "Odometer Saat Transaksi Oli Kedua Dari Terakhir", 
a.kolom_t1 as "Store Code Transaksi Oli Atau Non-Oli Kedua Dari Terakhir", 
a.kolom_t10 as "Nama Toko Transaksi Oli Atau Non-Oli Kedua Dari Terakhir", 
a.kolom_airpt as "Nama RH Transaksi Oli Atau Non-Oli Kedua Dari Terakhir", 
a.kolom_ajrpt as "Cabang Toko Transaksi Oli Atau Non-Oli Kedua Dari Terakhir", 
a.kolom_t as "Tanggal Transaksi Oli Atau Non-Oli Kedua Dari Terakhir", 
a.kolom_u as "Odometer Trans Oli / Non Oli ke 2 Terakhir", 
a.kolom_u1 as "Tanggal Transaksi Oli Atau Non-Oli Ketiga Dari Terakhir", 
a.kolom_u4 as "Odometer Trans Oli / Non Oli ke 3 Terakhir", 
a.kolom_u2 as "Store Code Transaksi Oli Atau Non-Oli Ketiga Dari Terakhir", 
a.kolom_u10 as "Nama Toko Transaksi Oli Atau Non-Oli Ketiga Dari Terakhir", 
a.kolom_aqrpt as "Nama RH Transaksi Oli Atau Non-Oli Ketiga Dari Terakhir", 
a.kolom_arrpt as "Cabang Toko Transaksi Oli Atau Non-Oli Ketiga Dari Terakhir", 
a.kolom_v as "Asumsi Maksimal Rata2 Berkendara Per Hari (KM)", 
a.kolom_w as "Asumsi Minimal Hari Reminder", 
a.kolom_x as "Est Jarak Berkendara / Day (KM) (Antara Metode Fase 1 & 2)", 
a.kolom_y as "Estimasi Jarak Berkendara Per Hari (KM) Yang Digunakan (Antara Metode Fase 1 & 2)", 
a.kolom_z as "Jumlah Hari Reminder (Oli Terakhir vs Jarak Tempuh Fase 2)",
a.kolom_aa as "Tanggal Reminder Pertama", 
a.kolom_ab as "Jumlah Hari Reminder Sejak Penggantian Oli Terakhir", 
a.kolom_ac as "Jumlah Hari Untuk ke Reminder Pertama", 
a.kolom_ad as "Status Reminder Pertama", 
a.kolom_ae as "Tanggal Reminder Pertama Dilakukan", 
a.kolom_af as "Jarak Hari Dari Reminder Pertama ke Reminder Kedua", 
a.kolom_ag as "Tanggal Reminder Kedua", 
a.kolom_ah as "Jumlah Hari Untuk ke Reminder Kedua", 
a.kolom_ai as "Status Reminder Kedua", 
a.kolom_aj as "Tanggal Reminder Kedua Dilakukan", 
a.kolom_ak as "Jarak Hari Dari Reminder Kedua ke CVI Loyalty", 
a.kolom_al as "Jumlah Hari Untuk ke CVI Loyalty", 
a.kolom_am as "Masuk CVI Loyalty (Masuk/Tidak Masuk)"
from mb_trx_oil_goliaht_monitoring_all a
join lasttrx b on b.namacabang=a.namacabang and b.nopolisi=a.kolom_c;
Context	{}
Description	 
--------------------------------------------------------------------------------------------------------------------------
