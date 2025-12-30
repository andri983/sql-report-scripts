CREATE OR REPLACE FUNCTION smi_trx_qty_category_mtd_temp
(
    tablename TEXT,
    p_idcabang INT
)
RETURNS VOID AS $$
BEGIN
    EXECUTE format($f$
        INSERT INTO public.smi_trx_qty_category_mtd_temp (
            insertdate,
            namacabang,
            nopolisi,
            namamember,
            notelp,
			accu,
			bodythrottle,
			engineflush,
			injectorcleaner,
			oil,
			tiresealant,
			tubeless,
			tubetype
        )
			SELECT 
                now(),
                a.namacabang,
                a.nopolisi,
                b.namamember,
                b.notelp,
                SUM(CASE WHEN a.category='ACCU' THEN a.qty ELSE 0 END) AS accu,
                SUM(CASE WHEN a.category='BODY THROTTLE' THEN a.qty ELSE 0 END) AS bodythrottle,
                SUM(CASE WHEN a.category='ENGINE FLUSH' THEN a.qty ELSE 0 END) AS engineflush,
                SUM(CASE WHEN a.category='INJECTOR CLEANER' THEN a.qty ELSE 0 END) AS injectorcleaner,
                SUM(CASE WHEN a.category='OIL' THEN a.qty ELSE 0 END) AS oil,
                SUM(CASE WHEN a.category='TIRE SEALANT' THEN a.qty ELSE 0 END) AS tiresealant,
                SUM(CASE WHEN a.category='TUBELESS' THEN a.qty ELSE 0 END) AS tubeless,
                SUM(CASE WHEN a.category='TUBETYPE' THEN a.qty ELSE 0 END) AS tubetype 
            FROM public.%I  a
            LEFT JOIN public.mstmember b ON b.nopolisi = a.nopolisi
            WHERE a.idcabang = %L
              AND a.tanggal >= date_trunc('month', CURRENT_DATE) - INTERVAL '36 months'
              AND a.tanggal <= (CURRENT_DATE - INTERVAL '1 day')::date
              AND a.nopolisi IS NOT NULL
              AND a.nopolisi <> ''
              AND a.idjenisproduk <> 4
              AND a.statusproduk <> 'K'
              AND a.kodeproduk <> '3091844001'
              AND a.category IN (
                    'ACCU','BODY THROTTLE','ENGINE FLUSH','INJECTOR CLEANER',
                    'OIL','TIRE SEALANT','TUBELESS','TUBETYPE'
              )
            GROUP BY 
                a.namacabang,
                a.nopolisi,
                b.namamember,
                b.notelp
    $f$, tablename, p_idcabang);

END;
$$ LANGUAGE plpgsql;

SELECT * FROM public.smi_trx_qty_category_mtd_temp where namacabang='Denpasar';
--DELETE FROM public.smi_trx_qty_category_mtd_temp where namacabang='Denpasar';
SELECT smi_trx_qty_category_mtd_temp('smi_rms22_transaksi_toko_perjenis_member_v3',5);