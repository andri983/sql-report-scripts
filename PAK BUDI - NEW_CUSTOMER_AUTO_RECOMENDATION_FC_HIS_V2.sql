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
--	FROM smi_rms10_transaksi_toko_perjenis_member_v3 x
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
--		FROM smi_rms10_transaksi_toko_perjenis_member_v3 a
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
  t.idjenismember, t.namajenismember,(t.tanggal + INTERVAL '7 days')::date as reminder
FROM transaksi t
JOIN voucher v ON t.rn = v.rn
WHERE v.nomorserivoucher IS NOT NULL;

-- STEP 2: Insert ke car_his
INSERT INTO public.car_his (
  tgltransaksi, tglreport, tglakhirvoucher, nomorserivoucher,
  namacabang, kodetoko, nomortransaksi, brand, category,
  idproduk, kodeproduk, namapanjang, qty, subtotal, nopolisi,
  namamember, notelp, idjenismember, namajenismember, reminder
)
SELECT 
  tgltransaksi, tglreport, tglakhirvoucher, nomorserivoucher,
  namacabang, kodetoko, nomortransaksi, brand, category,
  idproduk, kodeproduk, namapanjang, qty, subtotal, nopolisi,
  namamember, notelp, idjenismember, namajenismember, reminder
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
