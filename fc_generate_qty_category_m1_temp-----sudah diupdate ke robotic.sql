CREATE OR REPLACE FUNCTION generate_qty_category_m1_temp(
    p_schema   text,
    p_table    text,
    p_idcabang int
)
RETURNS void AS
$$
DECLARE
    v_sql text;
BEGIN
    -------------------------------------------------------------
    -- 1. DROP TEMP TABLE
    -------------------------------------------------------------
    DROP TABLE IF EXISTS temp_src_part;

    -------------------------------------------------------------
    -- 2. CREATE TEMP TABLE AS SELECT (CTAS)
    -------------------------------------------------------------
    v_sql := '
        CREATE TEMP TABLE temp_src_part AS
        SELECT 
            a.tanggal,
            a.namacabang,
            a.nopolisi,
            a.category,
            SUM(a.qty::numeric) AS qty,
            b.namamember,
            b.notelp
        FROM ' || quote_ident(p_schema) || '.' || quote_ident(p_table) || ' a
        LEFT JOIN public.mstmember b 
            ON b.nopolisi = a.nopolisi
        WHERE a.idcabang = ' || p_idcabang || '
          AND a.tanggal >= date_trunc(''month'', CURRENT_DATE) - INTERVAL ''37 month''
	      AND a.tanggal <  date_trunc(''month'', CURRENT_DATE)
          AND a.nopolisi IS NOT NULL 
          AND a.nopolisi <> ''''
          AND a.idjenisproduk <> 4
          AND a.statusproduk <> ''K''
          AND a.kodeproduk <> ''3091844001''
          AND a.category IN (
                ''ACCU'', ''BODY THROTTLE'', ''ENGINE FLUSH'',
                ''INJECTOR CLEANER'', ''OIL'',
                ''TIRE SEALANT'', ''TUBELESS'', ''TUBETYPE''
          )
        GROUP BY 
            a.tanggal,
            a.namacabang,
            a.nopolisi,
            a.category,
            b.namamember,
            b.notelp
    ';

    EXECUTE v_sql;

    -------------------------------------------------------------
    -- 3. INDEX (mempercepat agregasi)
    -------------------------------------------------------------
    CREATE INDEX idx_temp_src_part ON temp_src_part (nopolisi, category);

    -------------------------------------------------------------
    -- 4. INSERT FINAL KE OUTPUT TABLE
    -------------------------------------------------------------
    INSERT INTO public.smi_trx_qty_category_m1_temp (
        insertdate,
        namacabang,
        nopolisi,
        namamember,
        notelp,
        ACCU,
        BODYTHROTTLE,
        ENGINEFLUSH,
        INJECTORCLEANER,
        OIL,
        TIRESEALANT,
        TUBELESS,
        TUBETYPE
    )
    SELECT
        now(),
        namacabang,
        nopolisi,
        MAX(namamember),
        MAX(notelp),

        SUM(CASE WHEN category = 'ACCU'             THEN qty ELSE 0 END),
        SUM(CASE WHEN category = 'BODY THROTTLE'    THEN qty ELSE 0 END),
        SUM(CASE WHEN category = 'ENGINE FLUSH'     THEN qty ELSE 0 END),
        SUM(CASE WHEN category = 'INJECTOR CLEANER' THEN qty ELSE 0 END),
        SUM(CASE WHEN category = 'OIL'              THEN qty ELSE 0 END),
        SUM(CASE WHEN category = 'TIRE SEALANT'     THEN qty ELSE 0 END),
        SUM(CASE WHEN category = 'TUBELESS'         THEN qty ELSE 0 END),
        SUM(CASE WHEN category = 'TUBETYPE'         THEN qty ELSE 0 END)
    FROM temp_src_part
    GROUP BY namacabang, nopolisi;

END;
$$ LANGUAGE plpgsql;

	



	-----------------------------------------------------------------------------------------------------
	SELECT * FROM public.smi_trx_qty_category_m1_temp;
--	TRUNCATE TABLE public.smi_trx_qty_category_m1_temp;
	SELECT generate_qty_category_m1_temp('public','smi_rms10_transaksi_toko_perjenis_member_v3',2);
	SELECT generate_qty_category_m1_temp('public','smi_rms11_transaksi_toko_perjenis_member_v3',6);
	SELECT generate_qty_category_m1_temp('public','smi_rms12_transaksi_toko_perjenis_member_v3',7);
	SELECT generate_qty_category_m1_temp('public','smi_rms15_transaksi_toko_perjenis_member_v3',9);
	SELECT generate_qty_category_m1_temp('public','smi_rms20_transaksi_toko_perjenis_member_v3',3);
	SELECT generate_qty_category_m1_temp('public','smi_rms20_transaksi_toko_perjenis_member_v3',8);
	SELECT generate_qty_category_m1_temp('public','smi_rms21_transaksi_toko_perjenis_member_v3',4);
	SELECT generate_qty_category_m1_temp('public','smi_rms22_transaksi_toko_perjenis_member_v3',5);
	-----------------------------------------------------------------------------------------------------





	-----
	QUERY
	-----
	DO $$
	DECLARE
	    v_table_name text := 'smi_rms15_transaksi_toko_perjenis_member_v3';  
	    v_idcabang   int  := 9;                                             
	BEGIN
	    -------------------------------------------------------------
	    -- 1. DROP TEMP TABLE
	    -------------------------------------------------------------
	    DROP TABLE IF EXISTS temp_src_part;
	
	    -------------------------------------------------------------
	    -- 2. CREATE TEMP TABLE AS SELECT (CTAS) → 5-10x lebih cepat
	    -------------------------------------------------------------
	    EXECUTE format($SQL$
	        CREATE TEMP TABLE temp_src_part AS
	        SELECT 
	            a.tanggal,
	            a.namacabang,
	            a.nopolisi,
	            a.category,
	            SUM(a.qty::numeric) AS qty,
	            b.namamember,
	            b.notelp
	        FROM public.%I a
	        LEFT JOIN public.mstmember b 
	            ON b.nopolisi = a.nopolisi
	        WHERE a.idcabang = %s
	          AND a.tanggal >= date_trunc('month', CURRENT_DATE) - INTERVAL '37 month'
		      AND a.tanggal <  date_trunc('month', CURRENT_DATE)
	          AND a.nopolisi IS NOT NULL 
	          AND a.nopolisi <> ''
	          AND a.idjenisproduk <> 4
	          AND a.statusproduk <> 'K'
	          AND a.kodeproduk <> '3091844001'
	          AND a.category IN (
	                'ACCU', 'BODY THROTTLE', 'ENGINE FLUSH',
	                'INJECTOR CLEANER', 'OIL',
	                'TIRE SEALANT', 'TUBELESS', 'TUBETYPE'
	          )
			  GROUP BY a.tanggal,a.namacabang,a.nopolisi,a.category,b.namamember,b.notelp;
	    $SQL$, 
	        v_table_name,
	        v_idcabang
	    );
	
	    -------------------------------------------------------------
	    -- 3. INDEX setelah CTAS (sangat mempercepat aggregasi)
	    -------------------------------------------------------------
	    CREATE INDEX idx_temp_src_part ON temp_src_part (nopolisi, category);
	
	    -------------------------------------------------------------
	    -- 4. INSERT FINAL KE OUTPUT (clean)
	    -------------------------------------------------------------
	    INSERT INTO public.smi_trx_qty_category_m1_temp (
	        insertdate,
	        namacabang,
	        nopolisi,
	        namamember,
	        notelp,
	        ACCU,
	        BODYTHROTTLE,
	        ENGINEFLUSH,
	        INJECTORCLEANER,
	        OIL,
	        TIRESEALANT,
	        TUBELESS,
	        TUBETYPE
	    )
	    SELECT
	        now(),
	        namacabang,
	        nopolisi,
	        MAX(namamember),
	        MAX(notelp),
	
	        SUM(CASE WHEN category = 'ACCU'             THEN qty ELSE 0 END),
	        SUM(CASE WHEN category = 'BODY THROTTLE'    THEN qty ELSE 0 END),
	        SUM(CASE WHEN category = 'ENGINE FLUSH'     THEN qty ELSE 0 END),
	        SUM(CASE WHEN category = 'INJECTOR CLEANER' THEN qty ELSE 0 END),
	        SUM(CASE WHEN category = 'OIL'              THEN qty ELSE 0 END),
	        SUM(CASE WHEN category = 'TIRE SEALANT'     THEN qty ELSE 0 END),
	        SUM(CASE WHEN category = 'TUBELESS'         THEN qty ELSE 0 END),
	        SUM(CASE WHEN category = 'TUBETYPE'         THEN qty ELSE 0 END)
	    FROM temp_src_part
	    GROUP BY namacabang, nopolisi;
	
	END $$;
