-- DROP FUNCTION public.fn_pra_his_all(text);

--CREATE OR REPLACE FUNCTION public.fn_pra_his_all(p_table_name text)
-- RETURNS void
-- LANGUAGE plpgsql
--AS $function$
--DECLARE
--    sql text;
--BEGIN
--    -- rakit query dinamis ke temp table
--    sql := format($q$
--        CREATE TEMP TABLE tmp_pra_his_all AS
        WITH 
        cte_transaksi_gsheet AS (
       		SELECT 
	        y.tanggal,
	        y.kodetoko::int8,
	        y.nomortransaksi::int8,
	        y.nopolisi,
	        y.motoroff,
	        y.motoron,
	        y.startermotor
	        FROM (
	            SELECT 
	                x.tanggal,
	                x.kodetoko,
	                x.nomortransaksi,
	                x.nopolisi,
	                x.motoroff,
	                x.motoron,
	                x.startermotor
	            FROM (
	                SELECT 
	                	"Timestamp"::date as tanggal,
	                    "Kode Toko POS" as kodetoko,
	                    "No. Transaksi" as nomortransaksi, 
	                    "No. Polisi" as nopolisi,  
	                    "Motor dalam kondisi Mati" as motoroff, 
	                    "Motor dalam kondisi Hidup" as motoron, 
	                    "Starter Motor" as startermotor
	                FROM public.pra_gsheet_results
	                WHERE "Timestamp"::date = current_date - 1
	            ) AS x
	              WHERE x.kodetoko::text ~ '^[0-9]+$' 
				  AND length(x.kodetoko::text) = 7
				  AND x.nomortransaksi::text ~ '^[0-9]+$'
				  AND length(x.nomortransaksi::text) = 12
			) AS y
        ),
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
                        PARTITION BY a.kodetoko, a.nopolisi ORDER BY a.nomortransaksi DESC
                    ) AS rn
--                FROM %I a
                FROM public.smi_rms10_transaksi_toko_perjenis_member_v3 a
                WHERE a.tanggal = current_date - 1
            ) AS x
            WHERE x.rn = 1
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
                l.nopolisi,
                l.category,
                l.tanggal
            FROM public.pra_last_trx_accu l
        )
        select 
        a.tanggal AS transactiondate,
        NOW()::date AS reportdate,
        a.kodetoko,a.nomortransaksi,a.nopolisi,a.motoroff,a.motoron,a.startermotor,
        b.idjenismember,b.namajenismember,
        d.tanggal::date AS lasttrxaccu
        from cte_transaksi_gsheet a
        left join cte_transaksi b on b.kodetoko=a.kodetoko and b.nomortransaksi=a.nomortransaksi and b.nopolisi=a.nopolisi
        left join cte_statusaccu c on c.kodetoko=a.kodetoko and c.nomortransaksi=a.nomortransaksi and c.nopolisi=a.nopolisi
        left join cte_lasttrxaccu d on d.nopolisi=a.nopolisi
        
--        SELECT 
--            x.tanggal::date AS transactiondate,
--            NOW()::date AS reportdate,
--            x.namacabang,
--            x.kodetoko,
--            x.nomortransaksi,
--            x.nopolisi,
--            s.namamember,
--            CASE 
--                WHEN s.notelp IS NOT NULL AND s.notelp ~ '^[0-9]+$'
--                THEN '62' || SUBSTRING(s.notelp FROM 2)
--                ELSE NULL
--            END AS notelp,
--            x.idjenismember,
--            x.namajenismember,
--            s.statusaccu,
--            l.tanggal::date AS lasttrxaccu
--        FROM cte_transaksi x
--        LEFT JOIN cte_statusaccu s 
--               ON x.idcabang = s.idcabang 
--              AND x.kodetoko = s.kodetoko 
--              AND x.nomortransaksi = s.nomortransaksi
--        LEFT JOIN cte_lasttrxaccu l 
--               ON x.idcabang = l.idcabang 
--              AND x.nopolisi = l.nopolisi;
--    $q$, p_table_name);
--
--    -- jalankan query dinamis
--    EXECUTE sql;
--
--    -- insert ke tabel tujuan
--    INSERT INTO public.pra_his_all
--    (transactiondate, reportdate, namacabang, kodetoko, nomortransaksi, 
--     nopolisi, namamember, notelp, idjenismember, namajenismember, statusaccu, lasttrxaccu)
--    SELECT * FROM tmp_pra_his_all;
--
--    -- optional: drop temp table biar bersih
--    DROP TABLE IF EXISTS tmp_pra_his_all;
--END;
--$function$
--;
