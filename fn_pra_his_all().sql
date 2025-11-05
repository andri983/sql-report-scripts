-- DROP FUNCTION public.fn_pra_his_all();

CREATE OR REPLACE FUNCTION public.fn_pra_his_all()
 RETURNS void
 LANGUAGE plpgsql
AS $function$
BEGIN
    -- Buat temp table dengan data yang difilter
    CREATE TEMP TABLE tmp_pra_his_all AS
    WITH 
    cte_transaksi AS (
        SELECT
            x.tglbisnis,
            x.idcabang,
            x.namacabang,
            x.nopolisi,
            x.namamember,
            x.notelp,
            x.idjenismember,
            x.namajenismember,
            x.kodetoko,
            x.nomortransaksi,
            x.nomorimen,
            x.statusaccuoff,
            x.statusaccuon,
            x.flagstarter,
            x.rn
        FROM (
            SELECT
                c.tglbisnis,
                c.idcabang,
                c.namacabang,
                c.nopolisi,
                c.namamember,
                c.notelp,
                c.idjenismember,
                c.namajenismember,
                c.kodetoko,
                c.nomortransaksi,
                c.nomorimen,
                c.statusaccuoff,
                c.statusaccuon,
                c.flagstarter,
                ROW_NUMBER() OVER (
                    PARTITION BY c.kodetoko, c.nopolisi ORDER BY c.nomortransaksi DESC
                ) AS rn
            FROM public.pra_status_accu_all c
			WHERE c.versiname='New' --update 31 oktober 2025
--            WHERE c.kodetoko IN ( --update 31 oktober 2025
--				'3021001','3021003','3021004','3021005','3021011','3021012','3021015','3021016','3021021','3021026','3021028','3021032',
--				'3021033','3021034','3021038','3021039','3021041','3021048','3021050','3021059','3021065','3021068','3021069','3021070',
--				'3021074','3021075','3021079','3021080','3021081','3021082','3021086','3021095','3021108','3021111','3021112','3021113',
--				'3021120','3021124','3021128','3021129','3021134','3021135','3021140','3021142','3021151','3021160','3021165','3021167',
--				'3021172','3021177','3021178','3021185','3021189','3021197','3021202','3021207','3021217','3021220','3021238','3021240',
--				'3021254','3021258','3021288','3021301','3021304','3021306','3021307','3021310','3021311','3021314','3021319','3021321',
--				'3021322','3021324','3021329','3021330','3021332','3021337','3021339','3021341','3021353','3021356','3021363','3021366',
--				'3021367','3021368','3021373','3021380','3021382','3021385','3021386','3021387','3021388','3021389','3021390','3021394',
--				'3021395','3021403','3021404','3021413','3022004','3022018','3022021','3022023','3022024','3023003',
--				'3031204','3031124','3031009','3031008','3031339','3031035','3031334','3031116'
--            )
        ) AS x
        WHERE x.rn = 1
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
        x.tglbisnis::date AS transactiondate,
        x.tglbisnis::date+1 AS reportdate,
		x.idcabang,
        x.namacabang,
        x.kodetoko,
        x.nomortransaksi,
        x.nopolisi,
        x.namamember,
        x.notelp,
        x.idjenismember,
        x.namajenismember,
        x.statusaccuoff,
        x.statusaccuon,
		x.flagstarter,
        l.tanggal::date AS lasttrxaccu
    FROM cte_transaksi x
    LEFT JOIN cte_lasttrxaccu l 
           ON x.idcabang = l.idcabang 
          AND x.nopolisi = l.nopolisi;

    -- Insert hasil ke tabel tujuan
    INSERT INTO public.pra_his_all (
        transactiondate, reportdate, idcabang, namacabang, kodetoko, nomortransaksi, 
        nopolisi, namamember, notelp, idjenismember, namajenismember, statusaccuoff, statusaccuon, flagstarter, lasttrxaccu
    )
    SELECT * FROM tmp_pra_his_all;

    -- Hapus temp table setelah selesai
    DROP TABLE IF EXISTS tmp_pra_his_all;
END;
$function$
;
