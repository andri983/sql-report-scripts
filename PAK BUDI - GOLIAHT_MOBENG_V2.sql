-----CREATE FOREIGN TABLE (select * from MB_HO.dbo.SMIMstVoucherDiskon;)

CREATE FOREIGN TABLE mb_rms01_mbho.smimstvoucherdiskon (
	idvoucher int8 OPTIONS(column_name 'idvoucher') NULL,
	idgroupvoucher int4 OPTIONS(column_name 'idgroupvoucher') NULL,
	kodejenisvoucher char(1) OPTIONS(column_name 'kodejenisvoucher') NULL,
	idmarchant int4 OPTIONS(column_name 'idmarchant') NULL,
	nomorserivoucher varchar(50) OPTIONS(column_name 'nomorserivoucher') NULL,
	tglaktif date OPTIONS(column_name 'tglaktif') NULL,
	tglakhir date OPTIONS(column_name 'tglakhir') NULL,
	iduser varchar(20) OPTIONS(column_name 'iduser') NULL,
	tglcreate timestamp(0) OPTIONS(column_name 'tglcreate') NULL,
	tglupdate timestamp(0) OPTIONS(column_name 'tglupdate') NULL,
	flagaktif int4 OPTIONS(column_name 'flagaktif') NULL,
	statusdata int4 OPTIONS(column_name 'statusdata') NULL
)
SERVER mobeng_rms01
OPTIONS (schema_name 'dbo', table_name 'SMIMstVoucherDiskon');

select * from mb_rms01_mbho.smimstvoucherdiskon





------CREATE TABLE VOUCHER UNTUK MENYIMPAN - UPDATE DATA DARI mb_rms01_mbho.smimstvoucherdiskon
--DROP TABLE public.goliaht_voucher
CREATE TABLE public.mb_goliaht_voucher (
	insertdate timestamptz DEFAULT now() NOT NULL,
    idvoucher INT8 PRIMARY KEY,
    idgroupvoucher INT4 NOT NULL,
    idmarchant INT4 NOT NULL,
    nomorserivoucher VARCHAR(50) NOT NULL,
    statuskirim INT DEFAULT 0
);
CREATE INDEX idx_mb_goliaht_voucher_nomorserivoucher ON public.mb_goliaht_voucher(nomorserivoucher);





------CREATE INSERT INTO (mb_rms01_mbho.smimstvoucherdiskon-public.goliaht_voucher)

INSERT INTO public.mb_goliaht_voucher (insertdate,idvoucher,idgroupvoucher,idmarchant,nomorserivoucher,statuskirim)
SELECT NOW() AS insertdate,s.idvoucher,s.idgroupvoucher,s.idmarchant,s.nomorserivoucher,0 AS statuskirim
FROM mb_rms01_mbho.smimstvoucherdiskon s
WHERE s.statusdata=1 and s.idmarchant=35
AND NOT EXISTS (
    SELECT 1
    FROM public.mb_goliaht_voucher c
    WHERE c.nomorserivoucher = s.nomorserivoucher
);

--select * from public.mb_goliaht_voucher;

--truncate table public.mb_goliaht_voucher;




-- NOL
-----CREATE SELECT-INSERT-UPDATE
-- Step 1: Create temp table berisi final data
CREATE TEMP TABLE temp_goliaht_voucher_mobeng AS
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
    FROM public.mb_trx_oil_goliaht_monitoring_all
    WHERE kolom_d::date = current_date
      AND kolom_nrpt = 'Oke'
      AND kolom_ac::numeric = 0
),
voucher_data AS (
    SELECT 
        nomorserivoucher,
        ROW_NUMBER() OVER (ORDER BY idvoucher) AS rn
    FROM public.mb_goliaht_voucher
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
        (m.kolom_d::date + INTERVAL '30 day') AS tglakhirvoucher,
        v.nomorserivoucher
    FROM monitoring_data m
    JOIN voucher_data v ON m.rn = v.rn
)
SELECT * FROM final_data;

-- Step 2: Insert dari temp table
INSERT INTO public.mb_trx_oil_goliaht_his_test (
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
    null AS send_date,
    null AS xid,
    null AS wa_status_data,
    tglakhirvoucher::date,
    nomorserivoucher
FROM temp_goliaht_voucher_mobeng;

-- Step 3: Update statuskirim di tabel voucher
UPDATE public.mb_goliaht_voucher
SET statuskirim = 1
WHERE nomorserivoucher IN (
    SELECT nomorserivoucher FROM temp_goliaht_voucher_mobeng
);

-- Step 4: Hapus temp table
DROP TABLE temp_goliaht_voucher_mobeng;





-- MINUS
-----CREATE SELECT-INSERT-UPDATE
-- Step 1: Create temp table berisi final data
CREATE TEMP TABLE temp_goliaht_voucher_mobeng AS
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
    FROM public.mb_trx_oil_goliaht_monitoring_all
    WHERE kolom_d::date = current_date-1
      AND kolom_nrpt = 'Oke'
      AND kolom_ah IS NOT NULL
      AND kolom_ah <> 'N/A'
      AND kolom_ah::numeric = 0
),
voucher_data AS (
    SELECT 
        nomorserivoucher,
        ROW_NUMBER() OVER (ORDER BY idvoucher) AS rn
    FROM public.mb_goliaht_voucher
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
        (m.kolom_d::date + INTERVAL '30 day') AS tglakhirvoucher,
        v.nomorserivoucher
    FROM monitoring_data m
    JOIN voucher_data v ON m.rn = v.rn
)
SELECT * FROM final_data;

-- Step 2: Insert dari temp table
INSERT INTO public.mb_trx_oil_goliaht_his_test (
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
    null AS send_date,
    null AS xid,
    null AS wa_status_data,
    tglakhirvoucher::date,
    nomorserivoucher
FROM temp_goliaht_voucher_mobeng;

-- Step 3: Update statuskirim di tabel voucher
UPDATE public.mb_goliaht_voucher
SET statuskirim = 1
WHERE nomorserivoucher IN (
    SELECT nomorserivoucher FROM temp_goliaht_voucher_mobeng
);

-- Step 4: Hapus temp table
DROP TABLE temp_goliaht_voucher_mobeng;


