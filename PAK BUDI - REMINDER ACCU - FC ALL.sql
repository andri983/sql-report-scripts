CREATE OR REPLACE FUNCTION public.fn_pra_his_all(p_table_name text)
RETURNS void
LANGUAGE plpgsql
AS $$
DECLARE
    sql text;
BEGIN
    -- rakit query dinamis ke temp table
    sql := format($q$
        CREATE TEMP TABLE tmp_pra_his_all AS
        WITH 
        cte_transaksi AS (
            SELECT 
                x.tanggal,
                x.idcabang,
                x.namacabang,
                x.kodetoko,
                x.nomortransaksi,
                x.nopolisi,
                x.idjenismember,
                x.namajenismember
            FROM (
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
                FROM %I a
                WHERE a.tanggal = current_date - 1
                  AND a.kodetoko IN (
                      '3021003','3021004','3021005','3021021','3021028','3021033',
                      '3021065','3021068','3021069','3021074','3021080','3021095',
                      '3021111','3021140','3021177','3021240','3021288','3021382'
                  )
            ) AS x
            WHERE x.rn = 1
--        ),
--        cte_mstmember AS (
--            SELECT 
--                b.nopolisi,
--                b.namamember,
--                b.notelp
--            FROM public.mstmember b
        ),
        cte_statusaccu AS (
            SELECT 
                c.tglbisnis,
                c.idcabang,
				c.nopolisi,
				c.namamember,
				c.notelp,
                c.kodetoko,
                c.nomortransaksi,
                c.nomorimen,
                c.statusaccu 
            FROM public.pra_status_accu c
        ),
        cte_lasttrxaccu AS (
            SELECT 
                l.insertdate,
                l.idcabang,
                l.nopolisi,
                l.category,
                l.tanggal
            FROM public.pra_last_trx_accu l
        )
        SELECT 
            x.tanggal::date AS transactiondate,
            NOW()::date AS reportdate,
            x.namacabang,
            x.kodetoko,
            x.nomortransaksi,
            x.nopolisi,
            s.namamember,
            CASE 
                WHEN s.notelp IS NOT NULL AND s.notelp ~ '^[0-9]+$'
                THEN '62' || SUBSTRING(s.notelp FROM 2)
                ELSE NULL
            END AS notelp,
            x.idjenismember,
            x.namajenismember,
            s.statusaccu,
            l.tanggal::date AS lasttrxaccu
        FROM cte_transaksi x
--        LEFT JOIN cte_mstmember m 
--               ON x.nopolisi = m.nopolisi
        LEFT JOIN cte_statusaccu s 
               ON x.idcabang = s.idcabang 
              AND x.kodetoko = s.kodetoko 
              AND x.nomortransaksi = s.nomortransaksi
        LEFT JOIN cte_lasttrxaccu l 
               ON x.idcabang = l.idcabang 
              AND x.nopolisi = l.nopolisi;
    $q$, p_table_name);

    -- jalankan query dinamis
    EXECUTE sql;

    -- insert ke tabel tujuan
    INSERT INTO public.pra_his_all
    (transactiondate, reportdate, namacabang, kodetoko, nomortransaksi, 
     nopolisi, namamember, notelp, idjenismember, namajenismember, statusaccu, lasttrxaccu)
    SELECT * FROM tmp_pra_his_all;

    -- optional: drop temp table biar bersih
    DROP TABLE IF EXISTS tmp_pra_his_all;
END;
$$;