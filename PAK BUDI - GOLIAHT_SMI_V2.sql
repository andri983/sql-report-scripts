-- TRANSFER DATA
select idvoucher, idgroupvoucher, kodejenisvoucher, idmarchant, nomorserivoucher, tglaktif, tglakhir, iduser, tglcreate, tglupdate, flagaktif, statusdata 
from smimstvoucherpersen where idMarchant=49 and statusdata=1;





Transfer Jobs
Jobs name
smi_goliaht_mstvoucherdiskonpersen-transfer
Search...
	goliaht_smimstvoucherdiskonpersen-transfer	rms01.planetban.co.id <PB_HO>	POOL/localhost <DATARMS>	10/24/2025 13:29:56		 	OK


	
	
	
Transfer Jobssmi_goliaht_mstvoucherdiskonpersen-transfer
Jobs name
goliaht_smimstvoucherdiskonpersen-transfer
Source Server	rms01.planetban.co.id <PB_HO>
Destination Server	POOL/localhost <DATARMS>
  
Last Run	10/24/2025 13:29:56
Schedule Transfer	
 
Source Query
Destination
select idvoucher, idgroupvoucher, kodejenisvoucher, idmarchant, nomorserivoucher, tglaktif, tglakhir, iduser, tglcreate, tglupdate, flagaktif, statusdata 
from smimstvoucherpersen where idMarchant=49 and statusdata=1;
Destination
Fields	idvoucher, idgroupvoucher, kodejenisvoucher, idmarchant, nomorserivoucher, tglaktif, tglakhir, iduser, tglcreate, tglupdate, flagaktif, statusdata
Table	public.smi_goliaht_mstvoucherdiskonpersen





-- CREATE TABLE VOUCHER UNTUK MENYIMPAN - TRANSFER DATA DARI rms01 smimstvoucherdiskon
-- select * from public.smi_goliaht_mstvoucherdiskonpersen
-- public.smi_goliaht_mstvoucherdiskonpersen definition
-- Drop table
-- DROP TABLE public.smi_goliaht_mstvoucherdiskonpersen;
CREATE TABLE public.smi_goliaht_mstvoucherdiskonpersen (
	insertdate timestamptz DEFAULT now() NOT NULL,
	idvoucher int8 NOT NULL,
	idgroupvoucher int4 NOT NULL,
	kodejenisvoucher bpchar(1) NOT NULL,
	idmarchant int4 NOT NULL,
	nomorserivoucher varchar(50) NOT NULL,
	tglaktif date NOT NULL,
	tglakhir date NOT NULL,
	iduser varchar(20) NOT NULL,
	tglcreate timestamp NOT NULL,
	tglupdate timestamp NOT NULL,
	flagaktif int4 NOT NULL,
	statusdata int4 NOT NULL,
	CONSTRAINT goliaht_smimstvoucherdiskonpersen_pkey PRIMARY KEY (idvoucher)
);
CREATE INDEX idx_goliaht_idvoucher ON public.smi_goliaht_mstvoucherdiskonpersen USING btree (idvoucher);
CREATE INDEX idx_goliaht_nomorserivoucher ON public.smi_goliaht_mstvoucherdiskonpersen USING btree (nomorserivoucher);





------CREATE TABLE VOUCHER UNTUK MENYIMPAN - UPDATE DATA DARI goliaht_smimstvoucherdiskonpersen
-- public.smi_goliaht_voucher definition
-- Drop table
-- DROP TABLE public.smi_goliaht_voucher;
CREATE TABLE public.smi_goliaht_voucher (
	insertdate timestamptz DEFAULT now() NOT NULL,
	idvoucher int8 NOT NULL,
	idgroupvoucher int4 NOT NULL,
	idmarchant int4 NOT NULL,
	nomorserivoucher varchar(50) NOT NULL,
	statuskirim int4 DEFAULT 0 NULL,
	CONSTRAINT smi_goliaht_voucher_pkey PRIMARY KEY (idvoucher)
);
CREATE INDEX idx_smi_goliaht_voucher_nomorserivoucher ON public.smi_goliaht_voucher USING btree (nomorserivoucher);





------CREATE INSERT INTO (public.goliaht_smimstvoucherdiskonpersen-public.smi_goliaht_voucher)
INSERT INTO public.smi_goliaht_voucher (insertdate,idvoucher,idgroupvoucher,idmarchant,nomorserivoucher,statuskirim)
SELECT NOW() AS insertdate,s.idvoucher,s.idgroupvoucher,s.idmarchant,s.nomorserivoucher,0 AS statuskirim
FROM public.smi_goliaht_mstvoucherdiskonpersen s
WHERE s.statusdata=1 and s.idmarchant=49
AND NOT EXISTS (
    SELECT 1
    FROM public.smi_goliaht_voucher c
    WHERE c.nomorserivoucher = s.nomorserivoucher
);


select * from public.smi_goliaht_voucher where statuskirim=1;--1130
select * from public.smi_goliaht_voucher where statuskirim=9;--1711--2870
select * from public.smi_goliaht_voucher where statuskirim=0;--1159

--update public.smi_goliaht_voucher set statuskirim =9 where statuskirim=0;

--select * from public.smi_goliaht_voucher where statuskirim=0;
--update public.smi_goliaht_voucher set statuskirim=9 where statuskirim=0;
--select * from public.smi_trx_oil_goliaht_his;

--create table public.smi_goliaht_voucher_backup as
--select * from public.smi_goliaht_voucher




DML Query Jobs
Jobs name
goliaht_voucher
Search...
 	goliaht_voucher-insert	POOL/localhost <DATARMS>	10/24/2025 13:37:30		 	OK
	

 	
 	
 	
-----BACKUP smi_trx_oil_goliaht_his
 	
create table public.smi_trx_oil_goliaht_his_20251030 as
select * from public.smi_trx_oil_goliaht_his;

select * from public.smi_trx_oil_goliaht_his_20251030;--459089
select * from public.smi_trx_oil_goliaht_his;

-----ADD NEW KOLOM smi_trx_oil_goliaht_his

ALTER TABLE public.smi_trx_oil_goliaht_his
ADD COLUMN wa_status_data VARCHAR(10) NULL,
ADD COLUMN tglakhirvoucher DATE NULL,
ADD COLUMN nomorserivoucher VARCHAR(50) NULL;




-- NOL
-----CREATE SELECT-INSERT-UPDATE
-- Step 1: Create temp table berisi final data
CREATE TEMP TABLE temp_goliaht_voucher_smi AS
WITH monitoring_data AS (
    SELECT 
        insert_date, namacabang, wa AS notelp,
        kolom_b, kolom_c, kolom_d, kolom_e, kolom_f, 
        kolom_g, kolom_h, kolom_i, kolom_j, kolom_k, kolom_l, kolom_m, kolom_n, 
        kolom_o, kolom_p, kolom_q, kolom_r, kolom_s, kolom_t, kolom_u, kolom_v, 
        kolom_w, kolom_x, kolom_y, kolom_z, kolom_aa, kolom_ab, kolom_ac, 
        kolom_ad, kolom_ae, kolom_af, kolom_ag, kolom_ah, kolom_ai, kolom_aj, 
        kolom_ak, kolom_al, kolom_am,
        ROW_NUMBER() OVER (ORDER BY kolom_d ASC, kolom_c ASC) AS rn
    FROM public.smi_trx_oil_goliaht_monitoring_all
    WHERE kolom_d::date = current_date
      AND kolom_nrpt = 'Oke'
      AND kolom_ac::numeric = 0
),
voucher_data AS (
    SELECT 
        nomorserivoucher,
        ROW_NUMBER() OVER (ORDER BY idvoucher) AS rn
    FROM public.smi_goliaht_voucher
    WHERE statuskirim = 0
),
final_data AS (
    SELECT 
        m.insert_date, m.namacabang, m.notelp,
        m.kolom_b, m.kolom_c, m.kolom_d, m.kolom_e, m.kolom_f, 
        m.kolom_g, m.kolom_h, m.kolom_i, m.kolom_j, m.kolom_k, m.kolom_l, m.kolom_m, m.kolom_n, 
        m.kolom_o, m.kolom_p, m.kolom_q, m.kolom_r, m.kolom_s, m.kolom_t, m.kolom_u, m.kolom_v, 
        m.kolom_w, m.kolom_x, m.kolom_y, m.kolom_z, m.kolom_aa, m.kolom_ab, m.kolom_ac, 
        m.kolom_ad, m.kolom_ae, m.kolom_af, m.kolom_ag, m.kolom_ah, m.kolom_ai, m.kolom_aj, 
        m.kolom_ak, m.kolom_al, m.kolom_am,
        (m.kolom_d::date + INTERVAL '30 day')::date AS tglakhirvoucher,
        v.nomorserivoucher
    FROM monitoring_data m
    JOIN voucher_data v ON m.rn = v.rn
)
SELECT * FROM final_data;

-- Step 2: Insert dari temp table
INSERT INTO public.smi_trx_oil_goliaht_his (
    insert_date, namacabang, notelp,
    kolom_b, kolom_c, kolom_d, kolom_e, kolom_f, 
    kolom_g, kolom_h, kolom_i, kolom_j, kolom_k, kolom_l, kolom_m, kolom_n, 
    kolom_o, kolom_p, kolom_q, kolom_r, kolom_s, kolom_t, kolom_u, kolom_v, 
    kolom_w, kolom_x, kolom_y, kolom_z, kolom_aa, kolom_ab, kolom_ac, 
    kolom_ad, kolom_ae, kolom_af, kolom_ag, kolom_ah, kolom_ai, kolom_aj, 
    kolom_ak, kolom_al, kolom_am,
    wa_status,
    send_date,
    xid,
    wa_status_data,
    tglakhirvoucher,
    nomorserivoucher
)
SELECT 
    insert_date, namacabang, notelp,
    kolom_b, kolom_c, kolom_d, kolom_e, kolom_f, 
    kolom_g, kolom_h, kolom_i, kolom_j, kolom_k, kolom_l, kolom_m, kolom_n, 
    kolom_o, kolom_p, kolom_q, kolom_r, kolom_s, kolom_t, kolom_u, kolom_v, 
    kolom_w, kolom_x, kolom_y, kolom_z, kolom_aa, kolom_ab, kolom_ac, 
    kolom_ad, kolom_ae, kolom_af, kolom_ag, kolom_ah, kolom_ai, kolom_aj, 
    kolom_ak, kolom_al, kolom_am,
    0 AS wa_status,
    NULL AS send_date,
    NULL AS xid,
    NULL AS wa_status_data,
    tglakhirvoucher::date,
    nomorserivoucher
FROM temp_goliaht_voucher_smi;

-- Step 3: Update statuskirim di tabel voucher
UPDATE public.smi_goliaht_voucher
SET statuskirim = 1
WHERE nomorserivoucher IN (
    SELECT nomorserivoucher FROM temp_goliaht_voucher_smi
);

-- Step 4: Hapus temp table
DROP TABLE IF EXISTS temp_goliaht_voucher_smi;






-- MINUS
-----CREATE SELECT-INSERT-UPDATE
-- Step 1: Create temp table berisi final data
CREATE TEMP TABLE temp_goliaht_voucher_smi AS
WITH monitoring_data AS (
    SELECT 
        insert_date, namacabang, wa AS notelp,
        kolom_b, kolom_c, kolom_d, kolom_e, kolom_f, 
        kolom_g, kolom_h, kolom_i, kolom_j, kolom_k, kolom_l, kolom_m, kolom_n, 
        kolom_o, kolom_p, kolom_q, kolom_r, kolom_s, kolom_t, kolom_u, kolom_v, 
        kolom_w, kolom_x, kolom_y, kolom_z, kolom_aa, kolom_ab, kolom_ac, 
        kolom_ad, kolom_ae, kolom_af, kolom_ag, kolom_ah, kolom_ai, kolom_aj, 
        kolom_ak, kolom_al, kolom_am,
        ROW_NUMBER() OVER (ORDER BY kolom_d ASC, kolom_c ASC) AS rn
    FROM public.smi_trx_oil_goliaht_monitoring_all
    WHERE kolom_d::date = current_date
      AND kolom_nrpt = 'Oke'
      AND kolom_ah IS NOT NULL
      AND kolom_ah <> 'N/A'
      AND kolom_ah::numeric = 0
),
voucher_data AS (
    SELECT 
        nomorserivoucher,
        ROW_NUMBER() OVER (ORDER BY idvoucher) AS rn
    FROM public.smi_goliaht_voucher
    WHERE statuskirim = 0
),
final_data AS (
    SELECT 
        m.insert_date, m.namacabang, m.notelp,
        m.kolom_b, m.kolom_c, m.kolom_d, m.kolom_e, m.kolom_f, 
        m.kolom_g, m.kolom_h, m.kolom_i, m.kolom_j, m.kolom_k, m.kolom_l, m.kolom_m, m.kolom_n, 
        m.kolom_o, m.kolom_p, m.kolom_q, m.kolom_r, m.kolom_s, m.kolom_t, m.kolom_u, m.kolom_v, 
        m.kolom_w, m.kolom_x, m.kolom_y, m.kolom_z, m.kolom_aa, m.kolom_ab, m.kolom_ac, 
        m.kolom_ad, m.kolom_ae, m.kolom_af, m.kolom_ag, m.kolom_ah, m.kolom_ai, m.kolom_aj, 
        m.kolom_ak, m.kolom_al, m.kolom_am,
        (m.kolom_d::date + INTERVAL '14 day')::date AS tglakhirvoucher,
        v.nomorserivoucher
    FROM monitoring_data m
    JOIN voucher_data v ON m.rn = v.rn
)
SELECT * FROM final_data;

-- Step 2: Insert dari temp table
INSERT INTO public.smi_trx_oil_goliaht_his (
    insert_date, namacabang, notelp,
    kolom_b, kolom_c, kolom_d, kolom_e, kolom_f, 
    kolom_g, kolom_h, kolom_i, kolom_j, kolom_k, kolom_l, kolom_m, kolom_n, 
    kolom_o, kolom_p, kolom_q, kolom_r, kolom_s, kolom_t, kolom_u, kolom_v, 
    kolom_w, kolom_x, kolom_y, kolom_z, kolom_aa, kolom_ab, kolom_ac, 
    kolom_ad, kolom_ae, kolom_af, kolom_ag, kolom_ah, kolom_ai, kolom_aj, 
    kolom_ak, kolom_al, kolom_am,
    wa_status,
    send_date,
    xid,
    wa_status_data,
    tglakhirvoucher,
    nomorserivoucher
)
SELECT 
    insert_date, namacabang, notelp,
    kolom_b, kolom_c, kolom_d, kolom_e, kolom_f, 
    kolom_g, kolom_h, kolom_i, kolom_j, kolom_k, kolom_l, kolom_m, kolom_n, 
    kolom_o, kolom_p, kolom_q, kolom_r, kolom_s, kolom_t, kolom_u, kolom_v, 
    kolom_w, kolom_x, kolom_y, kolom_z, kolom_aa, kolom_ab, kolom_ac, 
    kolom_ad, kolom_ae, kolom_af, kolom_ag, kolom_ah, kolom_ai, kolom_aj, 
    kolom_ak, kolom_al, kolom_am,
    0 AS wa_status,
    NULL AS send_date,
    NULL AS xid,
    NULL AS wa_status_data,
    tglakhirvoucher::date,
    nomorserivoucher
FROM temp_goliaht_voucher_smi;

-- Step 3: Update statuskirim di tabel voucher
UPDATE public.smi_goliaht_voucher
SET statuskirim = 1
WHERE nomorserivoucher IN (
    SELECT nomorserivoucher FROM temp_goliaht_voucher_smi
);

-- Step 4: Hapus temp table
DROP TABLE IF EXISTS temp_goliaht_voucher_smi;

