CREATE OR REPLACE FUNCTION generate_qty_value_category_m1_temp(
    p_schema TEXT,
    p_table TEXT,
    p_idcabang INT
)
RETURNS void AS
$$
DECLARE
    col_list TEXT = '';
    select_list TEXT = '';
    sql TEXT;
    i INT;
BEGIN
    --------------------------------------------------------------------
    -- Generate column list for m_1..m_36
    --------------------------------------------------------------------
    FOR i IN 1..36 LOOP
        col_list := col_list ||
            format(', m_%1$s, m_%1$s_qty, m_%1$s_value', i);

        select_list := select_list ||
            format('
                , COUNT(DISTINCT trxid) FILTER (WHERE bulan_ke = %1$s) AS m_%1$s
                , SUM(qty)              FILTER (WHERE bulan_ke = %1$s) AS m_%1$s_qty
                , SUM(subtotal)         FILTER (WHERE bulan_ke = %1$s) AS m_%1$s_value', i);
    END LOOP;

    --------------------------------------------------------------------
    -- Build full dynamic SQL
    --------------------------------------------------------------------
    sql := '
        INSERT INTO public.smi_trx_qty_value_category_m1_temp
        (
            insertdate, idcabang, namacabang, nopolisi,
            mtd_h_1, mtd_h_1_qty, mtd_h_1_value
            ' || col_list || '
        )
        WITH prep AS (
            SELECT 
                idcabang,
                namacabang,
                UPPER(nopolisi::text) AS nopolisi,
                concat(kodetoko, nomortransaksi) AS trxid,
                qty,
                subtotal,
                (EXTRACT(YEAR FROM age(date_trunc(''month'', now()), date_trunc(''month'', tanggal))) * 12 +
                 EXTRACT(MONTH FROM age(date_trunc(''month'', now()), date_trunc(''month'', tanggal))) - 1)
                 AS bulan_ke
            FROM ' || quote_ident(p_schema) || '.' || quote_ident(p_table) || '
            WHERE idcabang = ' || p_idcabang || '
              AND idjenisproduk <> 4
              AND statusproduk <> ''K''
              AND kodeproduk <> ''3091844001''
              AND category IN (''ACCU'', ''BODY THROTTLE'', ''ENGINE FLUSH'', ''INJECTOR CLEANER'',
                               ''OIL'', ''TIRE SEALANT'', ''TUBETYPE'', ''TUBELESS'')
              AND nopolisi IS NOT NULL AND nopolisi <> ''''
              AND tanggal >= date_trunc(''month'', now() - interval ''37 months'')
              AND tanggal <  date_trunc(''month'', now())
        )
        SELECT
            now(),
            idcabang,
            namacabang,
            nopolisi,

            -- MTD H-1
            COUNT(DISTINCT trxid) FILTER (WHERE bulan_ke = 0) AS mtd_h_1,
            SUM(qty)              FILTER (WHERE bulan_ke = 0) AS mtd_h_1_qty,
            SUM(subtotal)         FILTER (WHERE bulan_ke = 0) AS mtd_h_1_value

            ' || select_list || '

        FROM prep
        GROUP BY idcabang, namacabang, nopolisi
        HAVING COUNT(DISTINCT trxid) FILTER (WHERE bulan_ke = 0) > 0
    ';

    --------------------------------------------------------------------
    -- Execute the SQL
    --------------------------------------------------------------------
    EXECUTE sql;

END;
$$ LANGUAGE plpgsql;




	
	-----------------------------------------------------------------------------------------------------
	SELECT * FROM public.smi_trx_qty_value_category_m1_temp;
--	DELETE FROM public.smi_trx_qty_value_category_m1_temp;
	SELECT generate_qty_value_category_m1_temp('public','smi_rms10_transaksi_toko_perjenis_member_v3',2);
	SELECT generate_qty_value_category_m1_temp('public','smi_rms11_transaksi_toko_perjenis_member_v3',6);
	SELECT generate_qty_value_category_m1_temp('public','smi_rms12_transaksi_toko_perjenis_member_v3',7);
	SELECT generate_qty_value_category_m1_temp('public','smi_rms15_transaksi_toko_perjenis_member_v3',9);
	SELECT generate_qty_value_category_m1_temp('public','smi_rms20_transaksi_toko_perjenis_member_v3',3);
	SELECT generate_qty_value_category_m1_temp('public','smi_rms20_transaksi_toko_perjenis_member_v3',8);
	SELECT generate_qty_value_category_m1_temp('public','smi_rms21_transaksi_toko_perjenis_member_v3',4);
	SELECT generate_qty_value_category_m1_temp('public','smi_rms22_transaksi_toko_perjenis_member_v3',5);
	-----------------------------------------------------------------------------------------------------
	
	
	
	
	
	-----
	QUERY
	-----
	INSERT INTO public.smi_trx_qty_value_category_m1_temp
	(
    insertdate, idcabang, namacabang, nopolisi,
    mtd_h_1, mtd_h_1_qty, mtd_h_1_value,
    m_1, m_1_qty, m_1_value,
    m_2, m_2_qty, m_2_value,
    m_3, m_3_qty, m_3_value,
    m_4, m_4_qty, m_4_value,
    m_5, m_5_qty, m_5_value,
    m_6, m_6_qty, m_6_value,
    m_7, m_7_qty, m_7_value,
    m_8, m_8_qty, m_8_value,
    m_9, m_9_qty, m_9_value,
    m_10, m_10_qty, m_10_value,
    m_11, m_11_qty, m_11_value,
    m_12, m_12_qty, m_12_value,
    m_13, m_13_qty, m_13_value,
    m_14, m_14_qty, m_14_value,
    m_15, m_15_qty, m_15_value,
    m_16, m_16_qty, m_16_value,
    m_17, m_17_qty, m_17_value,
    m_18, m_18_qty, m_18_value,
    m_19, m_19_qty, m_19_value,
    m_20, m_20_qty, m_20_value,
    m_21, m_21_qty, m_21_value,
    m_22, m_22_qty, m_22_value,
    m_23, m_23_qty, m_23_value,
    m_24, m_24_qty, m_24_value,
    m_25, m_25_qty, m_25_value,
    m_26, m_26_qty, m_26_value,
    m_27, m_27_qty, m_27_value,
    m_28, m_28_qty, m_28_value,
    m_29, m_29_qty, m_29_value,
    m_30, m_30_qty, m_30_value,
    m_31, m_31_qty, m_31_value,
    m_32, m_32_qty, m_32_value,
    m_33, m_33_qty, m_33_value,
    m_34, m_34_qty, m_34_value,
    m_35, m_35_qty, m_35_value,
    m_36, m_36_qty, m_36_value
	)
	WITH prep AS (
	    SELECT 
	        idcabang,
	        namacabang,
	        UPPER(nopolisi::text) AS nopolisi,
	        concat(kodetoko, nomortransaksi) AS trxid,
	        qty,
	        subtotal,
	        (EXTRACT(YEAR FROM age(date_trunc('month', now()), date_trunc('month', tanggal))) * 12 +
	         EXTRACT(MONTH FROM age(date_trunc('month', now()), date_trunc('month', tanggal))) - 1)
	         AS bulan_ke
	    FROM public.smi_rms22_transaksi_toko_perjenis_member_v3
	    WHERE idcabang = 5
	      AND idjenisproduk <> 4
	      AND statusproduk <> 'K'
	      AND kodeproduk != '3091844001'
	      AND category IN ('ACCU','BODY THROTTLE','ENGINE FLUSH','INJECTOR CLEANER',
	                       'OIL','TIRE SEALANT','TUBELESS','TUBETYPE')
	      AND nopolisi IS NOT NULL
	      AND nopolisi <> ''
	      AND tanggal >= date_trunc('month', now() - interval '37 months')
	      AND tanggal <  date_trunc('month', now())
	--      AND nopolisi='AD5930EFC'
	)
	SELECT
	    now(),
	    idcabang,
	    namacabang,
	    nopolisi,
	
	    -- bulan_ke = 0
	    COUNT(DISTINCT trxid) FILTER (WHERE bulan_ke = 0)  AS mtd_h_1,
	    SUM(qty)              FILTER (WHERE bulan_ke = 0)  AS mtd_h_1_qty,
	    SUM(subtotal)         FILTER (WHERE bulan_ke = 0)  AS mtd_h_1_value,
	
	    -- bulan_ke = 1 .. 36
		COUNT(DISTINCT trxid) FILTER (WHERE bulan_ke = 1)  AS m_1,
		SUM(qty)              FILTER (WHERE bulan_ke = 1)  AS m_1_qty,
		SUM(subtotal)         FILTER (WHERE bulan_ke = 1)  AS m_1_value,
		
		COUNT(DISTINCT trxid) FILTER (WHERE bulan_ke = 2)  AS m_2,
		SUM(qty)              FILTER (WHERE bulan_ke = 2)  AS m_2_qty,
		SUM(subtotal)         FILTER (WHERE bulan_ke = 2)  AS m_2_value,
		
		COUNT(DISTINCT trxid) FILTER (WHERE bulan_ke = 3)  AS m_3,
		SUM(qty)              FILTER (WHERE bulan_ke = 3)  AS m_3_qty,
		SUM(subtotal)         FILTER (WHERE bulan_ke = 3)  AS m_3_value,
		
		COUNT(DISTINCT trxid) FILTER (WHERE bulan_ke = 4)  AS m_4,
		SUM(qty)              FILTER (WHERE bulan_ke = 4)  AS m_4_qty,
		SUM(subtotal)         FILTER (WHERE bulan_ke = 4)  AS m_4_value,
		
		COUNT(DISTINCT trxid) FILTER (WHERE bulan_ke = 5)  AS m_5,
		SUM(qty)              FILTER (WHERE bulan_ke = 5)  AS m_5_qty,
		SUM(subtotal)         FILTER (WHERE bulan_ke = 5)  AS m_5_value,
		
		COUNT(DISTINCT trxid) FILTER (WHERE bulan_ke = 6)  AS m_6,
		SUM(qty)              FILTER (WHERE bulan_ke = 6)  AS m_6_qty,
		SUM(subtotal)         FILTER (WHERE bulan_ke = 6)  AS m_6_value,
		
		COUNT(DISTINCT trxid) FILTER (WHERE bulan_ke = 7)  AS m_7,
		SUM(qty)              FILTER (WHERE bulan_ke = 7)  AS m_7_qty,
		SUM(subtotal)         FILTER (WHERE bulan_ke = 7)  AS m_7_value,
		
		COUNT(DISTINCT trxid) FILTER (WHERE bulan_ke = 8)  AS m_8,
		SUM(qty)              FILTER (WHERE bulan_ke = 8)  AS m_8_qty,
		SUM(subtotal)         FILTER (WHERE bulan_ke = 8)  AS m_8_value,
		
		COUNT(DISTINCT trxid) FILTER (WHERE bulan_ke = 9)  AS m_9,
		SUM(qty)              FILTER (WHERE bulan_ke = 9)  AS m_9_qty,
		SUM(subtotal)         FILTER (WHERE bulan_ke = 9)  AS m_9_value,
		
		COUNT(DISTINCT trxid) FILTER (WHERE bulan_ke = 10) AS m_10,
		SUM(qty)              FILTER (WHERE bulan_ke = 10) AS m_10_qty,
		SUM(subtotal)         FILTER (WHERE bulan_ke = 10) AS m_10_value,
		
		COUNT(DISTINCT trxid) FILTER (WHERE bulan_ke = 11) AS m_11,
		SUM(qty)              FILTER (WHERE bulan_ke = 11) AS m_11_qty,
		SUM(subtotal)         FILTER (WHERE bulan_ke = 11) AS m_11_value,
		
		COUNT(DISTINCT trxid) FILTER (WHERE bulan_ke = 12) AS m_12,
		SUM(qty)              FILTER (WHERE bulan_ke = 12) AS m_12_qty,
		SUM(subtotal)         FILTER (WHERE bulan_ke = 12) AS m_12_value,
		
		COUNT(DISTINCT trxid) FILTER (WHERE bulan_ke = 13) AS m_13,
		SUM(qty)              FILTER (WHERE bulan_ke = 13) AS m_13_qty,
		SUM(subtotal)         FILTER (WHERE bulan_ke = 13) AS m_13_value,
		
		COUNT(DISTINCT trxid) FILTER (WHERE bulan_ke = 14) AS m_14,
		SUM(qty)              FILTER (WHERE bulan_ke = 14) AS m_14_qty,
		SUM(subtotal)         FILTER (WHERE bulan_ke = 14) AS m_14_value,
		
		COUNT(DISTINCT trxid) FILTER (WHERE bulan_ke = 15) AS m_15,
		SUM(qty)              FILTER (WHERE bulan_ke = 15) AS m_15_qty,
		SUM(subtotal)         FILTER (WHERE bulan_ke = 15) AS m_15_value,
		
		COUNT(DISTINCT trxid) FILTER (WHERE bulan_ke = 16) AS m_16,
		SUM(qty)              FILTER (WHERE bulan_ke = 16) AS m_16_qty,
		SUM(subtotal)         FILTER (WHERE bulan_ke = 16) AS m_16_value,
		
		COUNT(DISTINCT trxid) FILTER (WHERE bulan_ke = 17) AS m_17,
		SUM(qty)              FILTER (WHERE bulan_ke = 17) AS m_17_qty,
		SUM(subtotal)         FILTER (WHERE bulan_ke = 17) AS m_17_value,
		
		COUNT(DISTINCT trxid) FILTER (WHERE bulan_ke = 18) AS m_18,
		SUM(qty)              FILTER (WHERE bulan_ke = 18) AS m_18_qty,
		SUM(subtotal)         FILTER (WHERE bulan_ke = 18) AS m_18_value,
		
		COUNT(DISTINCT trxid) FILTER (WHERE bulan_ke = 19) AS m_19,
		SUM(qty)              FILTER (WHERE bulan_ke = 19) AS m_19_qty,
		SUM(subtotal)         FILTER (WHERE bulan_ke = 19) AS m_19_value,
		
		COUNT(DISTINCT trxid) FILTER (WHERE bulan_ke = 20) AS m_20,
		SUM(qty)              FILTER (WHERE bulan_ke = 20) AS m_20_qty,
		SUM(subtotal)         FILTER (WHERE bulan_ke = 20) AS m_20_value,
		
		COUNT(DISTINCT trxid) FILTER (WHERE bulan_ke = 21) AS m_21,
		SUM(qty)              FILTER (WHERE bulan_ke = 21) AS m_21_qty,
		SUM(subtotal)         FILTER (WHERE bulan_ke = 21) AS m_21_value,
		
		COUNT(DISTINCT trxid) FILTER (WHERE bulan_ke = 22) AS m_22,
		SUM(qty)              FILTER (WHERE bulan_ke = 22) AS m_22_qty,
		SUM(subtotal)         FILTER (WHERE bulan_ke = 22) AS m_22_value,
		
		COUNT(DISTINCT trxid) FILTER (WHERE bulan_ke = 23) AS m_23,
		SUM(qty)              FILTER (WHERE bulan_ke = 23) AS m_23_qty,
		SUM(subtotal)         FILTER (WHERE bulan_ke = 23) AS m_23_value,
		
		COUNT(DISTINCT trxid) FILTER (WHERE bulan_ke = 24) AS m_24,
		SUM(qty)              FILTER (WHERE bulan_ke = 24) AS m_24_qty,
		SUM(subtotal)         FILTER (WHERE bulan_ke = 24) AS m_24_value,
		
		COUNT(DISTINCT trxid) FILTER (WHERE bulan_ke = 25) AS m_25,
		SUM(qty)              FILTER (WHERE bulan_ke = 25) AS m_25_qty,
		SUM(subtotal)         FILTER (WHERE bulan_ke = 25) AS m_25_value,
		
		COUNT(DISTINCT trxid) FILTER (WHERE bulan_ke = 26) AS m_26,
		SUM(qty)              FILTER (WHERE bulan_ke = 26) AS m_26_qty,
		SUM(subtotal)         FILTER (WHERE bulan_ke = 26) AS m_26_value,
		
		COUNT(DISTINCT trxid) FILTER (WHERE bulan_ke = 27) AS m_27,
		SUM(qty)              FILTER (WHERE bulan_ke = 27) AS m_27_qty,
		SUM(subtotal)         FILTER (WHERE bulan_ke = 27) AS m_27_value,
		
		COUNT(DISTINCT trxid) FILTER (WHERE bulan_ke = 28) AS m_28,
		SUM(qty)              FILTER (WHERE bulan_ke = 28) AS m_28_qty,
		SUM(subtotal)         FILTER (WHERE bulan_ke = 28) AS m_28_value,
		
		COUNT(DISTINCT trxid) FILTER (WHERE bulan_ke = 29) AS m_29,
		SUM(qty)              FILTER (WHERE bulan_ke = 29) AS m_29_qty,
		SUM(subtotal)         FILTER (WHERE bulan_ke = 29) AS m_29_value,
		
		COUNT(DISTINCT trxid) FILTER (WHERE bulan_ke = 30) AS m_30,
		SUM(qty)              FILTER (WHERE bulan_ke = 30) AS m_30_qty,
		SUM(subtotal)         FILTER (WHERE bulan_ke = 30) AS m_30_value,
		
		COUNT(DISTINCT trxid) FILTER (WHERE bulan_ke = 31) AS m_31,
		SUM(qty)              FILTER (WHERE bulan_ke = 31) AS m_31_qty,
		SUM(subtotal)         FILTER (WHERE bulan_ke = 31) AS m_31_value,
		
		COUNT(DISTINCT trxid) FILTER (WHERE bulan_ke = 32) AS m_32,
		SUM(qty)              FILTER (WHERE bulan_ke = 32) AS m_32_qty,
		SUM(subtotal)         FILTER (WHERE bulan_ke = 32) AS m_32_value,
		
		COUNT(DISTINCT trxid) FILTER (WHERE bulan_ke = 33) AS m_33,
		SUM(qty)              FILTER (WHERE bulan_ke = 33) AS m_33_qty,
		SUM(subtotal)         FILTER (WHERE bulan_ke = 33) AS m_33_value,
		
		COUNT(DISTINCT trxid) FILTER (WHERE bulan_ke = 34) AS m_34,
		SUM(qty)              FILTER (WHERE bulan_ke = 34) AS m_34_qty,
		SUM(subtotal)         FILTER (WHERE bulan_ke = 34) AS m_34_value,
		
		COUNT(DISTINCT trxid) FILTER (WHERE bulan_ke = 35) AS m_35,
		SUM(qty)              FILTER (WHERE bulan_ke = 35) AS m_35_qty,
		SUM(subtotal)         FILTER (WHERE bulan_ke = 35) AS m_35_value,
		
		COUNT(DISTINCT trxid) FILTER (WHERE bulan_ke = 36) AS m_36,
		SUM(qty)              FILTER (WHERE bulan_ke = 36) AS m_36_qty,
		SUM(subtotal)         FILTER (WHERE bulan_ke = 36) AS m_36_value
	
	FROM prep
	GROUP BY idcabang, namacabang, nopolisi
	HAVING COUNT(DISTINCT trxid) FILTER (WHERE bulan_ke = 0) > 0;