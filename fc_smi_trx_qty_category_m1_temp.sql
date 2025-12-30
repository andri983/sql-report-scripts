CREATE OR REPLACE FUNCTION smi_trx_qty_category_m1_temp
(
    tablename TEXT,
    p_idcabang INT
)
RETURNS VOID AS $$
BEGIN
    EXECUTE format($f$
        INSERT INTO public.smi_trx_qty_category_m1_temp (
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
		    now() AS insertdate,
		    a.namacabang,
		    a.nopolisi,
		    b.namamember,
		    b.notelp,
		
		    SUM(CASE WHEN a.category='ACCU' THEN a.qty ELSE 0 END) AS ACCU,
		    SUM(CASE WHEN a.category='BODY THROTTLE' THEN a.qty ELSE 0 END) AS BODYTHROTTLE,
		    SUM(CASE WHEN a.category='ENGINE FLUSH' THEN a.qty ELSE 0 END) AS ENGINEFLUSH,
		    SUM(CASE WHEN a.category='INJECTOR CLEANER' THEN a.qty ELSE 0 END) AS INJECTORCLEANER,
		    SUM(CASE WHEN a.category='OIL' THEN a.qty ELSE 0 END) AS OIL,
		    SUM(CASE WHEN a.category='TIRE SEALANT' THEN a.qty ELSE 0 END) AS TIRESEALANT,
		    SUM(CASE WHEN a.category='TUBELESS' THEN a.qty ELSE 0 END) AS TUBELESS,
		    SUM(CASE WHEN a.category='TUBETYPE' THEN a.qty ELSE 0 END) AS TUBETYPE
		
		FROM public.%I a
		LEFT JOIN public.mstmember b ON b.nopolisi = a.nopolisi
		WHERE a.idcabang = %L
		  AND a.tanggal >= date_trunc('month', CURRENT_DATE) - INTERVAL '36 months'
		  AND a.tanggal < date_trunc('month', CURRENT_DATE)
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


select * from public.smi_trx_qty_category_m1_temp;
SELECT smi_trx_qty_category_m1_temp('smi_rms15_transaksi_toko_perjenis_member_v3',9);