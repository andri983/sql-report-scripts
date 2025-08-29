----1.TABLE public.smimstvoucherdiskonpersen TRANSFER DARI RMS01 SEMUA FIELD, AKAN DIDELETE SETIAP AKHIR BULAN
----select * from public.car_smimstvoucherdiskonpersen;
----truncate table public.car_smimstvoucherdiskonpersen;
CREATE TABLE public.car_smimstvoucherdiskonpersen (
	insertdate timestamptz DEFAULT now() NOT NULL,
    idvoucher INT8 PRIMARY KEY,
    idgroupvoucher INT4 NOT NULL,
    kodejenisvoucher CHAR(1) NOT NULL,
    idmarchant INT4 NOT NULL,
    nomorserivoucher VARCHAR(50) NOT NULL,
    tglaktif DATE NOT NULL,
    tglakhir DATE NOT NULL,
    iduser VARCHAR(20) NOT NULL,
    tglcreate TIMESTAMP NOT NULL,
    tglupdate TIMESTAMP NOT NULL,
    flagaktif INT4 NOT NULL,
    statusdata INT4 NOT null
);
CREATE INDEX idx_car_idvoucher ON public.car_smimstvoucherdiskonpersen(idvoucher);
CREATE INDEX idx_car_nomorserivoucher ON public.car_smimstvoucherdiskonpersen(nomorserivoucher);
---------1a. JOB TRANSFER
				Transfer JobsTransfer car_smimstvoucherdiskonpersen-transfer
				Jobs name
				Transfer car_smimstvoucherdiskonpersen-transfer
				Source Server	rms01.planetban.co.id <PB_HO>
				Destination Server	POOL/localhost <DATARMS>
				  
				Last Run	07/22/2025 14:05:43
				Schedule Transfer	
				 
				Source Query
				select * from smimstvoucherdiskonpersen where statusdata=1 and tglakhir>'2025-07-17'-- and idgroupviucher='';
				
				Destination
				Fields	:	idvoucher, idgroupvoucher, kodejenisvoucher, idmarchant, nomorserivoucher, tglaktif, tglakhir, iduser, tglcreate, tglupdate, flagaktif, statusdata
				table	:	public.car_smimstvoucherdiskonpersen


				
----2.TABLE public.car_voucher MENYIMPAN DATA AKHIR POINT 1
----select * from public.car_voucher;
----truncate table public.car_voucher;
CREATE TABLE public.car_voucher (
	insertdate timestamptz DEFAULT now() NOT NULL,
    idvoucher INT8 PRIMARY KEY,
    idgroupvoucher INT4 NOT NULL,
    idmarchant INT4 NOT NULL,
    nomorserivoucher VARCHAR(50) NOT NULL,
    statuskirim INT DEFAULT 0
);
CREATE INDEX idx_car_voucher_nomorserivoucher ON public.car_voucher(nomorserivoucher);
---------2a. JOB DML
				DML Query Jobscar_voucher-insert
				Jobs name
				car_voucher-insert
				Target	POOL/localhost <DATARMS>
				  
				Last Run	07/22/2025 14:24:21
				Schedule DML Query	
				 
				DML Query
				INSERT INTO public.car_voucher (insertdate,idvoucher,idgroupvoucher,idmarchant,nomorserivoucher,statuskirim)
				SELECT s.insertdate,s.idvoucher,s.idgroupvoucher,s.idmarchant,s.nomorserivoucher,0 AS statuskirim
				FROM public.car_smimstvoucherdiskonpersen s
				WHERE NOT EXISTS (
				    SELECT 1
				    FROM public.car_voucher c
				    WHERE c.nomorserivoucher = s.nomorserivoucher
				);

				

----3. TABLE public.car_his UNTUK MENYIMPAN HASIL QUERY AKHIR (POINT 4)
----select * from public.car_his;
CREATE TABLE public.car_his (
	tgltransaksi date NULL,
	tglreport date NULL,
	tglakhirvoucher date NULL,
	nomorserivoucher varchar(50) NULL,
	namacabang varchar(40) NULL,
	kodetoko int8 NULL,
	nomortransaksi int8 NULL,
	brand varchar(30) NULL,
	category varchar(30) NULL,
	idproduk int4 NULL,
	kodeproduk varchar(10) NULL,
	namapanjang varchar(100) NULL,
	qty numeric(18, 2) NULL,
	subtotal numeric(18, 2) NULL,
	nopolisi varchar(10) NULL,
	namamember varchar(30) NULL,
	notelp varchar(15) NULL,
	idjenismember int4 NULL,
	namajenismember varchar(30) NULL,
	wa_status int4 DEFAULT 0 NULL,
	wa_send_date timestamp NULL,
	wa_xid varchar(150) NULL,
	wa_status_data varchar(10) NULL,
	CONSTRAINT car_his_unique UNIQUE (tgltransaksi, nomorserivoucher, kodetoko, nomortransaksi)
);
CREATE INDEX idx_car_his_tgltransaksi ON public.car_his USING btree (tgltransaksi);
CREATE INDEX idx_car_his_wa_xid ON public.car_his USING btree (wa_xid);



----4. QUERY UNTUK MENDAPATKAN TRANSAKSI BAN H-1 (TANPA OLI DAN SERVICE)
----   QUERY UNTUK MEMASUKAN DATA public.car_his
----   QUERY UNTUK MENGUPDATE DATA public.car_voucher
-- STEP 1: Buat temporary table dari data final
--DROP TABLE IF EXISTS tmp_final_data;
--
--CREATE TEMP TABLE tmp_final_data AS
--WITH transaksi AS (
--  SELECT 
--    *,
--    ROW_NUMBER() OVER (ORDER BY tgltransaksi, nomortransaksi) AS rn
--  FROM (
--    SELECT DISTINCT ON (a.kodetoko, a.nomortransaksi)
--      a.tanggal AS tgltransaksi, 
--      current_date AS tglreport,
--      current_date + 30 AS tglakhirvoucher,
--      '' AS nomorserivoucher,
--      a.idcabang, a.namacabang, a.kodetoko, a.nomortransaksi, a.brand, a.category, 
--      a.idproduk, a.kodeproduk, a.namapanjang, a.qty, a.subtotal, a.nopolisi,
--      b.namamember, b.notelp, a.idjenismember, a.namajenismember
--    FROM public.smi_rms15_transaksi_toko_perjenis_member_v3 a
--    LEFT JOIN public.mstmember b ON b.nopolisi = a.nopolisi
--    WHERE a.iddivisi = 1 
--      AND a.tanggal = current_date - 1
--      AND b.notelp IS NOT NULL
--      AND REGEXP_REPLACE(b.notelp, '[^0-9]', '', 'g') <> ''
--      AND TRIM(BOTH '0' FROM b.notelp) <> ''
--      AND NOT EXISTS (SELECT 1 FROM public.smi_mstplatblacklist bl WHERE bl.nopolisi = a.nopolisi)
--      AND NOT EXISTS (SELECT 1 FROM public.car_base_nopol c WHERE c.nopolisi = a.nopolisi)
--      AND NOT EXISTS (
--        SELECT 1
--        FROM public.smi_rms15_transaksi_toko_perjenis_member_v3 x
--        JOIN public.smi_mst_oli oli ON oli.kodeproduk = x.kodeproduk
--        WHERE x.kodetoko = a.kodetoko 
--          AND x.nomortransaksi = a.nomortransaksi
--          AND x.tanggal = current_date - 1
--      )
--      AND NOT EXISTS (
--        SELECT 1
--        FROM public.smi_rms15_transaksi_toko_perjenis_member_v3 x
--        JOIN public.smi_mst_servis serv ON serv.kodeproduk = x.kodeproduk
--        WHERE x.kodetoko = a.kodetoko 
--          AND x.nomortransaksi = a.nomortransaksi
--          AND x.tanggal = current_date - 1
--      )
--  ) AS sub
--),
--voucher AS (
--  SELECT 
--    nomorserivoucher,
--    ROW_NUMBER() OVER (ORDER BY idvoucher) AS rn
--  FROM public.car_voucher
--  WHERE statuskirim = 0
--)
--SELECT 
--  t.tgltransaksi, t.tglreport, t.tglakhirvoucher, v.nomorserivoucher,
--  t.namacabang, t.kodetoko, t.nomortransaksi, t.brand, t.category,
--  t.idproduk, t.kodeproduk, t.namapanjang, t.qty, t.subtotal, t.nopolisi,
--  t.namamember, t.notelp, t.idjenismember, t.namajenismember
--FROM transaksi t
--LEFT JOIN voucher v ON t.rn = v.rn
--WHERE v.nomorserivoucher IS NOT NULL;
--
---- STEP 2: Masukkan data ke car_his
--INSERT INTO public.car_his (
--  tgltransaksi, tglreport, tglakhirvoucher, nomorserivoucher,
--  namacabang, kodetoko, nomortransaksi, brand, category,
--  idproduk, kodeproduk, namapanjang, qty, subtotal, nopolisi,
--  namamember, notelp, idjenismember, namajenismember
--)
--SELECT 
--  tgltransaksi, tglreport, tglakhirvoucher, nomorserivoucher,
--  namacabang, kodetoko, nomortransaksi, brand, category,
--  idproduk, kodeproduk, namapanjang, qty, subtotal, nopolisi,
--  namamember, notelp, idjenismember, namajenismember
--FROM tmp_final_data;
--
---- STEP 3: Masukkan nopol ke car_base_nopol
--INSERT INTO public.car_base_nopol (namacabang, nopolisi)
--SELECT DISTINCT namacabang, nopolisi
--FROM tmp_final_data;
--
---- STEP 4: Update status voucher
--UPDATE public.car_voucher
--SET statuskirim = 1
--WHERE nomorserivoucher IN (
--  SELECT nomorserivoucher
--  FROM tmp_final_data
--);


--ver1
CREATE OR REPLACE FUNCTION public.fn_car_his(temp_table text)
RETURNS void
LANGUAGE plpgsql
AS $function$
DECLARE
    dynamic_sql TEXT;
BEGIN
    dynamic_sql := format(
$$
-- STEP 1: Buat TEMP TABLE
DROP TABLE IF EXISTS tmp_final_data;

CREATE TEMP TABLE tmp_final_data AS
WITH transaksi AS (
  SELECT 
    *,
    ROW_NUMBER() OVER (ORDER BY tgltransaksi, nomortransaksi) AS rn
  FROM (
    SELECT DISTINCT ON (a.kodetoko, a.nomortransaksi)
      a.tanggal AS tgltransaksi, 
      current_date AS tglreport,
      current_date + 30 AS tglakhirvoucher,
      '' AS nomorserivoucher,
      a.idcabang, a.namacabang, a.kodetoko, a.nomortransaksi, a.brand, a.category, 
      a.idproduk, a.kodeproduk, a.namapanjang, a.qty, a.subtotal, a.nopolisi,
      b.namamember, b.notelp, a.idjenismember, a.namajenismember
    FROM public.smi_%I_transaksi_toko_perjenis_member_v3 a
    LEFT JOIN public.mstmember b ON b.nopolisi = a.nopolisi
    WHERE a.iddivisi = 1 
      AND a.tanggal = current_date - 1
      AND b.notelp IS NOT NULL
      AND REGEXP_REPLACE(b.notelp, '[^0-9]', '', 'g') <> ''
      AND TRIM(BOTH '0' FROM b.notelp) <> ''
      AND NOT EXISTS (
        SELECT 1 FROM public.smi_mstplatblacklist bl 
        WHERE bl.nopolisi = a.nopolisi
      )
      AND NOT EXISTS (
        SELECT 1 FROM public.car_base_nopol c 
        WHERE c.nopolisi = a.nopolisi
      )
      AND NOT EXISTS (
        SELECT 1
        FROM public.smi_%I_transaksi_toko_perjenis_member_v3 x
        JOIN public.smi_mst_oli oli ON oli.kodeproduk = x.kodeproduk
        WHERE x.kodetoko = a.kodetoko 
          AND x.nomortransaksi = a.nomortransaksi
          AND x.tanggal = current_date - 1
      )
      AND NOT EXISTS (
        SELECT 1
        FROM public.smi_%I_transaksi_toko_perjenis_member_v3 x
        JOIN public.smi_mst_servis serv ON serv.kodeproduk = x.kodeproduk
        WHERE x.kodetoko = a.kodetoko 
          AND x.nomortransaksi = a.nomortransaksi
          AND x.tanggal = current_date - 1
      )
  ) AS sub
),
voucher AS (
  SELECT 
    nomorserivoucher,
    ROW_NUMBER() OVER (ORDER BY idvoucher) AS rn
  FROM public.car_voucher
  WHERE statuskirim = 0
)
SELECT 
  t.tgltransaksi, t.tglreport, t.tglakhirvoucher, v.nomorserivoucher,
  t.namacabang, t.kodetoko, t.nomortransaksi, t.brand, t.category,
  t.idproduk, t.kodeproduk, t.namapanjang, t.qty, t.subtotal, t.nopolisi,
  t.namamember, t.notelp, t.idjenismember, t.namajenismember
FROM transaksi t
LEFT JOIN voucher v ON t.rn = v.rn
WHERE v.nomorserivoucher IS NOT NULL;

-- STEP 2: Insert ke car_his
INSERT INTO public.car_his (
  tgltransaksi, tglreport, tglakhirvoucher, nomorserivoucher,
  namacabang, kodetoko, nomortransaksi, brand, category,
  idproduk, kodeproduk, namapanjang, qty, subtotal, nopolisi,
  namamember, notelp, idjenismember, namajenismember
)
SELECT 
  tgltransaksi, tglreport, tglakhirvoucher, nomorserivoucher,
  namacabang, kodetoko, nomortransaksi, brand, category,
  idproduk, kodeproduk, namapanjang, qty, subtotal, nopolisi,
  namamember, notelp, idjenismember, namajenismember
FROM tmp_final_data;

-- STEP 3: Insert ke car_base_nopol
INSERT INTO public.car_base_nopol (namacabang, nopolisi)
SELECT DISTINCT namacabang, nopolisi
FROM tmp_final_data;

-- STEP 4: Update status voucher
UPDATE public.car_voucher
SET statuskirim = 1
WHERE nomorserivoucher IN (
  SELECT nomorserivoucher
  FROM tmp_final_data
);
$$, temp_table, temp_table, temp_table);

    -- Eksekusi query dinamis
    EXECUTE dynamic_sql;
END;
$function$;


--ver2
CREATE OR REPLACE FUNCTION public.fn_car_his(temp_table text)
RETURNS void
LANGUAGE plpgsql
AS $function$
DECLARE
    dynamic_sql TEXT;
BEGIN
    dynamic_sql := format(
$$
-- STEP 1: Buat TEMP TABLE
DROP TABLE IF EXISTS tmp_final_data;

CREATE TEMP TABLE tmp_final_data AS
WITH transaksi AS (
SELECT
x.tgltransaksi, x.tglreport, x.tglakhirvoucher, x.nomorserivoucher, x.idcabang, x.namacabang, x.kodetoko, x.nomortransaksi, x.brand, x.category, x.idproduk, x.kodeproduk, x.namapanjang, 
x.qty, x.subtotal, x.nopolisi,x.namamember, '62' || SUBSTRING(x.notelp FROM 2) AS notelp, x.idjenismember, x.namajenismember, x.string_length, x.first_character, x.second_character, x.third_character, x.rn
FROM
(
  SELECT 
    *,
    ROW_NUMBER() OVER (ORDER BY tgltransaksi, nomortransaksi) AS rn
	  FROM (
	    SELECT DISTINCT ON (a.kodetoko, a.nomortransaksi)
	      a.tanggal AS tgltransaksi, 
	      current_date AS tglreport,
	      current_date + 30 AS tglakhirvoucher,
	      '' AS nomorserivoucher,
	      a.idcabang, a.namacabang, a.kodetoko, a.nomortransaksi, a.brand, a.category, 
	      a.idproduk, a.kodeproduk, a.namapanjang, a.qty, a.subtotal, a.nopolisi,
	      b.namamember, b.notelp, a.idjenismember, a.namajenismember
		  ,coalesce(LENGTH(b.notelp),'2') AS string_length
		  ,coalesce(LEFT(b.notelp, 1),'1') AS first_character
		  ,coalesce(SUBSTRING(b.notelp FROM 2 FOR 1),'1') AS second_character
		  ,coalesce(SUBSTRING(b.notelp FROM 3 FOR 1),'1') AS third_character
	    FROM public.smi_%I_transaksi_toko_perjenis_member_v3 a
	    LEFT JOIN public.mstmember b ON b.nopolisi = a.nopolisi
	    WHERE a.iddivisi = 1 
	      AND a.tanggal = current_date - 1
	      AND b.notelp IS NOT NULL
	      AND REGEXP_REPLACE(b.notelp, '[^0-9]', '', 'g') <> ''
	      AND TRIM(BOTH '0' FROM b.notelp) <> ''
	      AND NOT EXISTS (
	        SELECT 1 FROM public.smi_mstplatblacklist bl 
	        WHERE bl.nopolisi = a.nopolisi
	      )
	      AND NOT EXISTS (
	        SELECT 1 FROM public.car_base_nopol c 
	        WHERE c.nopolisi = a.nopolisi
	      )
	      AND NOT EXISTS (
	        SELECT 1
	        FROM public.smi_%I_transaksi_toko_perjenis_member_v3 x
	        JOIN public.smi_mst_oli oli ON oli.kodeproduk = x.kodeproduk
	        WHERE x.kodetoko = a.kodetoko 
	          AND x.nomortransaksi = a.nomortransaksi
	          AND x.tanggal = current_date - 1
	      )
	      AND NOT EXISTS (
	        SELECT 1
	        FROM public.smi_%I_transaksi_toko_perjenis_member_v3 x
	        JOIN public.smi_mst_servis serv ON serv.kodeproduk = x.kodeproduk
	        WHERE x.kodetoko = a.kodetoko 
	          AND x.nomortransaksi = a.nomortransaksi
	          AND x.tanggal = current_date - 1
	      )
	  ) AS sub
  	WHERE string_length IN ('10','11','12','13','14')
	AND first_character = '0'
	AND second_character = '8'
	AND third_character IN ('1','2','3','5','6','7','8','9')
) AS x
),
voucher AS (
  SELECT 
    nomorserivoucher,
    ROW_NUMBER() OVER (ORDER BY idvoucher) AS rn
  FROM public.car_voucher
  WHERE statuskirim = 0
)
SELECT 
  t.tgltransaksi, t.tglreport, t.tglakhirvoucher, v.nomorserivoucher,
  t.namacabang, t.kodetoko, t.nomortransaksi, t.brand, t.category,
  t.idproduk, t.kodeproduk, t.namapanjang, t.qty, t.subtotal, t.nopolisi,
  t.namamember, t.notelp, t.idjenismember, t.namajenismember
FROM transaksi t
LEFT JOIN voucher v ON t.rn = v.rn
WHERE v.nomorserivoucher IS NOT NULL;

-- STEP 2: Insert ke car_his
INSERT INTO public.car_his (
  tgltransaksi, tglreport, tglakhirvoucher, nomorserivoucher,
  namacabang, kodetoko, nomortransaksi, brand, category,
  idproduk, kodeproduk, namapanjang, qty, subtotal, nopolisi,
  namamember, notelp, idjenismember, namajenismember
)
SELECT 
  tgltransaksi, tglreport, tglakhirvoucher, nomorserivoucher,
  namacabang, kodetoko, nomortransaksi, brand, category,
  idproduk, kodeproduk, namapanjang, qty, subtotal, nopolisi,
  namamember, notelp, idjenismember, namajenismember
FROM tmp_final_data;

-- STEP 3: Insert ke car_base_nopol
INSERT INTO public.car_base_nopol (namacabang, nopolisi)
SELECT DISTINCT namacabang, nopolisi
FROM tmp_final_data;

-- STEP 4: Update status voucher
UPDATE public.car_voucher
SET statuskirim = 1
WHERE nomorserivoucher IN (
  SELECT nomorserivoucher
  FROM tmp_final_data
);
$$, temp_table, temp_table, temp_table);

    -- Eksekusi query dinamis
    EXECUTE dynamic_sql;
END;
$function$;


--ver3
CREATE OR REPLACE FUNCTION public.fn_car_his(temp_table text)
RETURNS void
LANGUAGE plpgsql
AS $function$
DECLARE
    dynamic_sql TEXT;
BEGIN
    dynamic_sql := format(
$$
-- STEP 1: Buat TEMP TABLE
DROP TABLE IF EXISTS tmp_final_data;

CREATE TEMP TABLE tmp_final_data as

WITH
oli_serv_trans AS (
  SELECT DISTINCT kodetoko, nomortransaksi
  FROM smi_%I_transaksi_toko_perjenis_member_v3 x
  WHERE tanggal = current_date - 2
    AND EXISTS (
      SELECT 1 FROM smi_mst_oli oli WHERE oli.kodeproduk = x.kodeproduk
      UNION
      SELECT 1 FROM smi_mst_servis s WHERE s.kodeproduk = x.kodeproduk
    )
),
transaksi AS (
  SELECT *
  FROM (
    SELECT DISTINCT ON (a.kodetoko, a.nomortransaksi)
      a.*,
      b.namamember, b.notelp,							  
      ROW_NUMBER() OVER (ORDER BY a.tanggal, a.nomortransaksi) AS rn,
      COALESCE(LENGTH(b.notelp), 2) AS string_length,
      COALESCE(LEFT(b.notelp, 1), '1') AS first_character,
      COALESCE(SUBSTRING(b.notelp FROM 2 FOR 1), '1') AS second_character,
      COALESCE(SUBSTRING(b.notelp FROM 3 FOR 1), '1') AS third_character
    FROM smi_%I_transaksi_toko_perjenis_member_v3 a
    LEFT JOIN mstmember b ON b.nopolisi = a.nopolisi
    WHERE a.iddivisi = 1
      AND a.tanggal = current_date - 2
      AND b.notelp IS NOT NULL
      AND REGEXP_REPLACE(b.notelp, '[^0-9]', '', 'g') <> ''
      AND TRIM(BOTH '0' FROM b.notelp) <> ''
      AND NOT EXISTS (
        SELECT 1 FROM smi_mstplatblacklist bl WHERE bl.nopolisi = a.nopolisi
      )
      AND NOT EXISTS (
        SELECT 1 FROM car_base_nopol c WHERE c.nopolisi = a.nopolisi
      )
  ) AS sub
  WHERE 
    string_length IN ('10','11','12','13','14')
    AND first_character = '0'
    AND second_character = '8'
    AND third_character IN ('1','2','3','5','6','7','8','9')
    AND NOT EXISTS (
      SELECT 1 FROM oli_serv_trans ost 
      WHERE ost.kodetoko = sub.kodetoko AND ost.nomortransaksi = sub.nomortransaksi
    )
),
voucher AS (
  SELECT 
    nomorserivoucher,
    ROW_NUMBER() OVER (ORDER BY idvoucher) AS rn
  FROM public.car_voucher
  WHERE statuskirim = 0
)
SELECT 
  t.tanggal AS tgltransaksi,
  current_date AS tglreport,
  current_date + 30 AS tglakhirvoucher,
  v.nomorserivoucher,
  t.namacabang, t.kodetoko, t.nomortransaksi, t.brand, t.category,
  t.idproduk, t.kodeproduk, t.namapanjang, t.qty, t.subtotal, t.nopolisi,
  t.namamember, '62' || SUBSTRING(t.notelp FROM 2) AS notelp,
  t.idjenismember, t.namajenismember
FROM transaksi t
JOIN voucher v ON t.rn = v.rn
WHERE v.nomorserivoucher IS NOT NULL;

-- STEP 2: Insert ke car_his
INSERT INTO public.car_his (
  tgltransaksi, tglreport, tglakhirvoucher, nomorserivoucher,
  namacabang, kodetoko, nomortransaksi, brand, category,
  idproduk, kodeproduk, namapanjang, qty, subtotal, nopolisi,
  namamember, notelp, idjenismember, namajenismember
)
SELECT 
  tgltransaksi, tglreport, tglakhirvoucher, nomorserivoucher,
  namacabang, kodetoko, nomortransaksi, brand, category,
  idproduk, kodeproduk, namapanjang, qty, subtotal, nopolisi,
  namamember, notelp, idjenismember, namajenismember
FROM tmp_final_data;

-- STEP 3: Insert ke car_base_nopol
INSERT INTO public.car_base_nopol (namacabang, nopolisi)
SELECT DISTINCT namacabang, nopolisi
FROM tmp_final_data;

-- STEP 4: Update status voucher
UPDATE public.car_voucher
SET statuskirim = 1
WHERE nomorserivoucher IN (
  SELECT nomorserivoucher
  FROM tmp_final_data
);
$$, temp_table, temp_table, temp_table);

-- Eksekusi query dinamis
    EXECUTE dynamic_sql;
END;
$function$;





--ver4
-- DROP FUNCTION public.fn_car_his(text);

CREATE OR REPLACE FUNCTION public.fn_car_his(temp_table text)
 RETURNS void
 LANGUAGE plpgsql
AS $function$
DECLARE
    dynamic_sql TEXT;
BEGIN
    dynamic_sql := format(
$$
-- STEP 1: Buat TEMP TABLE
DROP TABLE IF EXISTS tmp_final_data;

CREATE TEMP TABLE tmp_final_data as

WITH
oli_serv_trans AS (
  SELECT DISTINCT kodetoko, nomortransaksi
  FROM smi_%I_transaksi_toko_perjenis_member_v3 x
  WHERE tanggal = current_date - 1
    AND EXISTS (
      SELECT 1 FROM smi_mst_oli oli WHERE oli.kodeproduk = x.kodeproduk
      UNION
      SELECT 1 FROM smi_mst_servis s WHERE s.kodeproduk = x.kodeproduk
    )
),
transaksi AS (
SELECT *
  FROM (
	SELECT 
	    a.*,
	    b.namamember,
	    b.notelp,
	    ROW_NUMBER() OVER (ORDER BY a.tanggal DESC, a.nomortransaksi DESC) AS rn,
	    COALESCE(LENGTH(b.notelp), 2) AS string_length,
	    COALESCE(LEFT(b.notelp, 1), '1') AS first_character,
	    COALESCE(SUBSTRING(b.notelp FROM 2 FOR 1), '1') AS second_character,
	    COALESCE(SUBSTRING(b.notelp FROM 3 FOR 1), '1') AS third_character
	FROM (
	    SELECT DISTINCT ON (a.nopolisi)
	        *
	    FROM smi_%I_transaksi_toko_perjenis_member_v3 a
	    WHERE a.iddivisi = 1
	      AND a.tanggal = current_date - 1
		  AND a.qty > 0
	      AND NOT EXISTS (SELECT 1 FROM smi_mstplatblacklist bl WHERE bl.nopolisi = a.nopolisi)
	      AND NOT EXISTS (SELECT 1 FROM car_base_nopol c WHERE c.nopolisi = a.nopolisi)
	    ORDER BY a.nopolisi, a.tanggal DESC, a.nomortransaksi DESC
	) a
	LEFT JOIN mstmember b ON b.nopolisi = a.nopolisi
	WHERE b.notelp IS NOT NULL
	  AND REGEXP_REPLACE(b.notelp, '[^0-9]', '', 'g') <> ''
	  AND TRIM(BOTH '0' FROM b.notelp) <> ''
	)AS sub
  WHERE 
    string_length IN ('10','11','12','13','14')
    AND first_character = '0'
    AND second_character = '8'
    AND third_character IN ('1','2','3','5','6','7','8','9')
    AND NOT EXISTS (SELECT 1 FROM oli_serv_trans ost WHERE ost.kodetoko = sub.kodetoko AND ost.nomortransaksi = sub.nomortransaksi)
),
voucher AS (
  SELECT 
    nomorserivoucher,
    ROW_NUMBER() OVER (ORDER BY idvoucher) AS rn
  FROM public.car_voucher
  WHERE statuskirim = 0
)
SELECT 
  t.tanggal AS tgltransaksi,
  current_date AS tglreport,
  current_date + 30 AS tglakhirvoucher,
  v.nomorserivoucher,
  t.namacabang, t.kodetoko, t.nomortransaksi, t.brand, t.category,
  t.idproduk, t.kodeproduk, t.namapanjang, t.qty, t.subtotal, t.nopolisi,
  t.namamember, '62' || SUBSTRING(t.notelp FROM 2) AS notelp,
  t.idjenismember, t.namajenismember
FROM transaksi t
JOIN voucher v ON t.rn = v.rn
WHERE v.nomorserivoucher IS NOT NULL;

-- STEP 2: Insert ke car_his
INSERT INTO public.car_his (
  tgltransaksi, tglreport, tglakhirvoucher, nomorserivoucher,
  namacabang, kodetoko, nomortransaksi, brand, category,
  idproduk, kodeproduk, namapanjang, qty, subtotal, nopolisi,
  namamember, notelp, idjenismember, namajenismember
)
SELECT 
  tgltransaksi, tglreport, tglakhirvoucher, nomorserivoucher,
  namacabang, kodetoko, nomortransaksi, brand, category,
  idproduk, kodeproduk, namapanjang, qty, subtotal, nopolisi,
  namamember, notelp, idjenismember, namajenismember
FROM tmp_final_data;

-- STEP 3: Insert ke car_base_nopol
INSERT INTO public.car_base_nopol (namacabang, nopolisi)
SELECT DISTINCT namacabang, nopolisi
FROM tmp_final_data;

-- STEP 4: Update status voucher
UPDATE public.car_voucher
SET statuskirim = 1
WHERE nomorserivoucher IN (
  SELECT nomorserivoucher
  FROM tmp_final_data
);
$$, temp_table, temp_table, temp_table);

-- Eksekusi query dinamis
    EXECUTE dynamic_sql;
END;
$function$
;





select * from public.car_his where qty<0;
select * from public.car_his where tglreport=current_date and nopolisi='L6370AAK';
select distinct namacabang from public.car_his where tglreport=current_date;
select nopolisi,count(*) from public.car_his where tglreport=current_date group by nopolisi HAVING COUNT(*) > 1;
select * from public.car_voucher where nomorserivoucher in ('6222BRD9W7VR','62226887T5MM') order by idvoucher;--OK
select * from public.car_voucher where statuskirim=1;--OK
select * from public.car_base_nopol order by insertdate desc;--OK
select distinct namacabang from public.car_base_nopol;--
select * from public.car_smimstvoucherdiskonpersen;--OK
h-2:D5047UDD
B3894CVE
B6290ZFJ
W4268QP
h-1:Z5762RG
select * from public.car_his where tglreport=current_date and nopolisi in ('Z5762RG','T5911RT');
select * from public.car_his where tglreport=current_date and qty<0;
select * from public.car_voucher where nomorserivoucher='625ZKJFH2QPM';
--truncate table public.car_his
--truncate table public.car_voucher;
--truncate table public.car_smimstvoucherdiskonpersen;
--truncate table public.car_base_nopol;

create table public.car_his_backup as
select * from public.car_his;

insert into public.car_his
select * from public.car_his_backup where nopolisi in ('DK4995DZ','N5714MR');

update public.car_his set wa_status=1;
select * from public.car_his order by car_his.nomorserivoucher;

----5.TABLE public.car_base_nopol
----select * from public.car_base_nopol;
----select distinct namacabang from public.car_base_nopol;
----truncate table public.car_base_nopol;
CREATE TABLE public.car_base_nopol (
	insertdate timestamptz DEFAULT now() NOT NULL,
    namacabang VARCHAR(20) NOT NULL,
    nopolisi VARCHAR(10) NOT NULL
);
CREATE INDEX idx_car_base_nopol ON public.car_base_nopol(nopolisi);

