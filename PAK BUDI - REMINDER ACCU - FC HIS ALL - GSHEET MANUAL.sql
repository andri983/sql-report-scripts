--RMS
INSERT INTO public.pra_rms_all
(namacabang, tanggal, nopolisi, kodetoko, nomortransaksi, insertdate)
select distinct namacabang,tanggal,nopolisi,kodetoko,nomortransaksi FROM public.smi_rms10_transaksi_toko_perjenis_member_v3
where tanggal between '2025-09-26' and '2025-09-30'
union all
select distinct namacabang,tanggal,nopolisi,kodetoko,nomortransaksi FROM public.smi_rms11_transaksi_toko_perjenis_member_v3
where tanggal between '2025-09-26' and '2025-09-30'
union all
select distinct namacabang,tanggal,nopolisi,kodetoko,nomortransaksi FROM public.smi_rms12_transaksi_toko_perjenis_member_v3
where tanggal between '2025-09-26' and '2025-09-30'
union all
select distinct namacabang,tanggal,nopolisi,kodetoko,nomortransaksi FROM public.smi_rms15_transaksi_toko_perjenis_member_v3
where tanggal between '2025-09-26' and '2025-09-30'
union all
select distinct namacabang,tanggal,nopolisi,kodetoko,nomortransaksi FROM public.smi_rms20_transaksi_toko_perjenis_member_v3
where tanggal between '2025-09-26' and '2025-09-30'
union all
select distinct namacabang,tanggal,nopolisi,kodetoko,nomortransaksi FROM public.smi_rms21_transaksi_toko_perjenis_member_v3
where tanggal between '2025-09-26' and '2025-09-30'
union all
select distinct namacabang,tanggal,nopolisi,kodetoko,nomortransaksi FROM public.smi_rms22_transaksi_toko_perjenis_member_v3
where tanggal between '2025-09-26' and '2025-09-30';

--INSERT INTO public.pra_rms_all
--(namacabang, tanggal, nopolisi, namamember, notelp, kodetoko, nomortransaksi)
--select distinct a.namacabang,a.tanggal,a.nopolisi, b.namamember, b.notelp, a.kodetoko,a.nomortransaksi 
--FROM public.smi_rms22_transaksi_toko_perjenis_member_v3 a
--LEFT JOIN PB_DC.dbo.MstMember b WITH (NOLOCK) ON b.NoPolisi = a.nopolisi
--where a.tanggal between '2025-09-18' and '2025-10-08' and a.nopolisi is not null;

select * from public.pra_rms_all;
select distinct namacabang from public.pra_rms_all;


--GSHEET
SELECT 
distinct
	site_name,
	"Timestamp"::date as tanggal,
	"No. Polisi" as nopolisi,  
    "Kode Toko POS" as kodetoko,
    "No. Transaksi" as nomortransaksi,
    "Motor dalam kondisi Mati" as motordalamkondisimati,
    "Motor dalam kondisi Hidup" as motordalamkondisihidup,
    "Starter Motor" as startermotor
FROM public.gsheet_results
--FROM public.pra_gsheet_results
WHERE "Timestamp"::date between '2025-09-18' and '2025-10-08' and site_name='JAKARTA';

--create table public.gsheet_results_20251001 as
--select * from public.gsheet_results;

---------------------------------------------------------------------------------------------------------------------------
SELECT 
    a.tanggal,
    a.idcabang,
    a.namacabang,
    a.kodetoko,
    a.nomortransaksi,
    a.nopolisi,
    a.idjenismember,
    a.namajenismember,
    ROW_NUMBER() OVER (
        PARTITION BY a.kodetoko, a.nomortransaksi, a.nopolisi
        ORDER BY a.tanggal ASC
    ) AS rn
FROM public.smi_rms10_transaksi_toko_perjenis_member_v3 a
WHERE a.tanggal = current_date - 1 and a.nopolisi='F5160WAG'

select * from public.pra_rms_all definition;
select distinct namacabang from public.pra_rms_all;
select distinct tanggal from public.pra_rms_all;

select nopolisi,notelp,wa_send_date,CONCAT(nopolisi, notelp) AS uniq,'Reminder Offering' as ket 
from public.car_his where wa_status=1 order by nopolisi;

select nopolisi,notelp,wa_send_date1,CONCAT(nopolisi, notelp) AS uniq,'Reminder Accu' as ket 
from public.pra_his where wa_status1=1 order by nopolisi;

select kolom_c,notelp,send_date,CONCAT(kolom_c, notelp) AS uniq,'Reminder Goliaht' as ket 
from smi_trx_oil_goliaht_his where wa_status=1 order by kolom_c;

select * 
from MB_trx_oil_goliaht_his where wa_status=1 
                
select * from public.pra_gsheet_results where "No. Polisi"='B3708BAD';

select * from public.pra_gsheet_results where "Kode Toko POS"='3021349' and "No. Transaksi"='202509230003'; ---F3748FAP

select * from public.pra_sigap_store;

select * from public.pra_his_all WHERE transactiondate::date between '2025-10-01' and '2025-10-07';

select * from public.pra_status_accu where kodetoko='3021349' and nomortransaksi='202509230003'; ---B3708BAD

select * from public.pra_status_accu where nopolisi='F3748FAP';--3021349	202509230005
select * from public.pra_gsheet_results where "No. Polisi"='F3748FAP';

select * FROM public.smi_rms10_transaksi_toko_perjenis_member_v3 where kodetoko=3021004 limit 5;

select * FROM public.smi_rms10_transaksi_toko_perjenis_member_v3
where kodetoko='3021075' and nomortransaksi='202509230017'

select distinct namacabang,tanggal,nopolisi,kodetoko,nomortransaksi FROM public.smi_rms22_transaksi_toko_perjenis_member_v3
where tanggal between '2025-09-18' and '2025-10-08' and nopolisi is not null;

select distinct namacabang,tanggal,nopolisi,kodetoko,nomortransaksi FROM public.smi_rms10_transaksi_toko_perjenis_member_v3
where tanggal between '2025-09-26' and '2025-09-30' and nopolisi is not null;

-- select distinct namacabang from public.pra_rms_all definition;
-- select * from public.pra_rms_all definition;
-- truncate table public.pra_gsheet_results;
-- truncate table public.pra_rms_all;
-- DROP TABLE public.pra_rms_all;
CREATE TABLE public.pra_rms_all (
	namacabang varchar(40) NULL,
	tanggal date NULL,
	nopolisi varchar(40) NULL,
	namamember varchar(30) NULL,
	notelp varchar(15) NULL,
	kodetoko int8 NULL,
	nomortransaksi int8 NULL,
	insertdate timestamp DEFAULT now() NULL,
	CONSTRAINT pra_rms_all_unique UNIQUE (namacabang, tanggal, kodetoko, nomortransaksi)
);


CREATE TABLE public.pra_sigap_store (
    company_id        VARCHAR(10)   NOT NULL,
    site_id           VARCHAR(10)   NOT NULL,
    location_id       BIGINT        PRIMARY KEY,
    location_name     VARCHAR(200)  NOT NULL,
    enabled_flag      BOOLEAN       NOT NULL,
    address_line1     VARCHAR(200),
    address_line2     VARCHAR(200),
    address_line3     VARCHAR(200),
    sub_district      VARCHAR(100),
    district          VARCHAR(100),
    city              VARCHAR(100),
    zip               VARCHAR(20),
    province          VARCHAR(100),
    country           VARCHAR(100),
    phone             VARCHAR(50),
    ownership_flag    VARCHAR(10),
    loc_latitude      BIGINT,
    loc_longitude     BIGINT,
    last_update_date  TIMESTAMP,
    last_updated_by   VARCHAR(50),
    creation_date     TIMESTAMP,
    created_by        VARCHAR(50)
);


CREATE TABLE public.pra_gsheet_results (
	id serial4 NOT NULL,
	site_name varchar(50) NULL,
	gsheet_code varchar(50) NULL,
	gsheet_id varchar(255) NULL,
	sheet_name varchar(100) NULL,
	"Kode Toko POS" varchar(50) NULL,
	"No. Polisi" varchar(100) NULL,
	"No. Transaksi" varchar(100) NULL,
	"Timestamp" varchar(50) NULL,
	"Motor dalam kondisi Mati" varchar(255) NULL,
	"Motor dalam kondisi Hidup" varchar(255) NULL,
	"Starter Motor" varchar(255) NULL,
	updated_at timestamp DEFAULT now() NULL,
	row_key text NULL,
	CONSTRAINT pra_gsheet_results_pkey PRIMARY KEY (id),
	CONSTRAINT pra_gsheet_results_unique UNIQUE (site_name, gsheet_code, gsheet_id, "Kode Toko POS", "No. Polisi", "No. Transaksi")
);
CREATE UNIQUE INDEX pra_gsheet_results_row_key_idx ON public.pra_gsheet_results USING btree (row_key);