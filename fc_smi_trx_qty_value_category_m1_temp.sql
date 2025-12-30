CREATE OR REPLACE FUNCTION fc_smi_trx_qty_value_category_m1_temp
(
    tablename TEXT,
    p_idcabang INT
)
RETURNS VOID AS $$
BEGIN
    EXECUTE format($f$
INSERT INTO public.smi_trx_qty_value_category_m1_temp
(insertdate, idcabang, namacabang, nopolisi, mtd_h_1, mtd_h_1_qty, mtd_h_1_value, m_1, m_1_qty, m_1_value, m_2, m_2_qty, m_2_value, m_3, m_3_qty, m_3_value, m_4, m_4_qty, m_4_value, m_5, m_5_qty, m_5_value, m_6, m_6_qty, m_6_value, m_7, m_7_qty, m_7_value, m_8, m_8_qty, m_8_value, m_9, m_9_qty, m_9_value, m_10, m_10_qty, m_10_value, m_11, m_11_qty, m_11_value, m_12, m_12_qty, m_12_value, m_13, m_13_qty, m_13_value, m_14, m_14_qty, m_14_value, m_15, m_15_qty, m_15_value, m_16, m_16_qty, m_16_value, m_17, m_17_qty, m_17_value, m_18, m_18_qty, m_18_value, m_19, m_19_qty, m_19_value, m_20, m_20_qty, m_20_value, m_21, m_21_qty, m_21_value, m_22, m_22_qty, m_22_value, m_23, m_23_qty, m_23_value, m_24, m_24_qty, m_24_value, m_25, m_25_qty, m_25_value, m_26, m_26_qty, m_26_value, m_27, m_27_qty, m_27_value, m_28, m_28_qty, m_28_value, m_29, m_29_qty, m_29_value, m_30, m_30_qty, m_30_value, m_31, m_31_qty, m_31_value, m_32, m_32_qty, m_32_value, m_33, m_33_qty, m_33_value, m_34, m_34_qty, m_34_value, m_35, m_35_qty, m_35_value, m_36, m_36_qty, m_36_value)
	SELECT
    now() AS insertdate,
    idcabang,
    namacabang,
    UPPER(nopolisi::text) AS nopolisi,
    COUNT(DISTINCT CASE WHEN bulan_ke = 0 THEN concat(kodetoko, nomortransaksi) END) AS mtd_h_1,
    SUM(CASE WHEN bulan_ke = 0 THEN qty END) AS mtd_h_1_qty,
    SUM(CASE WHEN bulan_ke = 0 THEN subtotal END) AS mtd_h_1_value,
    COUNT(DISTINCT CASE WHEN bulan_ke = 1 THEN concat(kodetoko, nomortransaksi) END) AS m_1,
    SUM(CASE WHEN bulan_ke = 1 THEN qty END) AS m_1_qty,
    SUM(CASE WHEN bulan_ke = 1 THEN subtotal END) AS m_1_value,
    COUNT(DISTINCT CASE WHEN bulan_ke = 2 THEN concat(kodetoko, nomortransaksi) END) AS m_2,
    SUM(CASE WHEN bulan_ke = 2 THEN qty END) AS m_2_qty,
    SUM(CASE WHEN bulan_ke = 2 THEN subtotal END) AS m_2_value,
    COUNT(DISTINCT CASE WHEN bulan_ke = 3 THEN concat(kodetoko, nomortransaksi) END) AS m_3,
    SUM(CASE WHEN bulan_ke = 3 THEN qty END) AS m_3_qty,
    SUM(CASE WHEN bulan_ke = 3 THEN subtotal END) AS m_3_value,
    COUNT(DISTINCT CASE WHEN bulan_ke = 4 THEN concat(kodetoko, nomortransaksi) END) AS m_4,
    SUM(CASE WHEN bulan_ke = 4 THEN qty END) AS m_4_qty,
    SUM(CASE WHEN bulan_ke = 4 THEN subtotal END) AS m_4_value,
    COUNT(DISTINCT CASE WHEN bulan_ke = 5 THEN concat(kodetoko, nomortransaksi) END) AS m_5,
    SUM(CASE WHEN bulan_ke = 5 THEN qty END) AS m_5_qty,
    SUM(CASE WHEN bulan_ke = 5 THEN subtotal END) AS m_5_value,
    COUNT(DISTINCT CASE WHEN bulan_ke = 6 THEN concat(kodetoko, nomortransaksi) END) AS m_6,
    SUM(CASE WHEN bulan_ke = 6 THEN qty END) AS m_6_qty,
    SUM(CASE WHEN bulan_ke = 6 THEN subtotal END) AS m_6_value,
    COUNT(DISTINCT CASE WHEN bulan_ke = 7 THEN concat(kodetoko, nomortransaksi) END) AS m_7,
    SUM(CASE WHEN bulan_ke = 7 THEN qty END) AS m_7_qty,
    SUM(CASE WHEN bulan_ke = 7 THEN subtotal END) AS m_7_value,
    COUNT(DISTINCT CASE WHEN bulan_ke = 8 THEN concat(kodetoko, nomortransaksi) END) AS m_8,
    SUM(CASE WHEN bulan_ke = 8 THEN qty END) AS m_8_qty,
    SUM(CASE WHEN bulan_ke = 8 THEN subtotal END) AS m_8_value,
    COUNT(DISTINCT CASE WHEN bulan_ke = 9 THEN concat(kodetoko, nomortransaksi) END) AS m_9,
    SUM(CASE WHEN bulan_ke = 9 THEN qty END) AS m_9_qty,
    SUM(CASE WHEN bulan_ke = 9 THEN subtotal END) AS m_9_value,
    COUNT(DISTINCT CASE WHEN bulan_ke = 10 THEN concat(kodetoko, nomortransaksi) END) AS m_10,
    SUM(CASE WHEN bulan_ke = 10 THEN qty END) AS m_10_qty,
    SUM(CASE WHEN bulan_ke = 10 THEN subtotal END) AS m_10_value,
    COUNT(DISTINCT CASE WHEN bulan_ke = 11 THEN concat(kodetoko, nomortransaksi) END) AS m_11,
    SUM(CASE WHEN bulan_ke = 11 THEN qty END) AS m_11_qty,
    SUM(CASE WHEN bulan_ke = 11 THEN subtotal END) AS m_11_value,
    COUNT(DISTINCT CASE WHEN bulan_ke = 12 THEN concat(kodetoko, nomortransaksi) END) AS m_12,
    SUM(CASE WHEN bulan_ke = 12 THEN qty END) AS m_12_qty,
    SUM(CASE WHEN bulan_ke = 12 THEN subtotal END) AS m_12_value,
    COUNT(DISTINCT CASE WHEN bulan_ke = 13 THEN concat(kodetoko, nomortransaksi) END) AS m_13,
    SUM(CASE WHEN bulan_ke = 13 THEN qty END) AS m_13_qty,
    SUM(CASE WHEN bulan_ke = 13 THEN subtotal END) AS m_13_value,
    COUNT(DISTINCT CASE WHEN bulan_ke = 14 THEN concat(kodetoko, nomortransaksi) END) AS m_14,
    SUM(CASE WHEN bulan_ke = 14 THEN qty END) AS m_14_qty,
    SUM(CASE WHEN bulan_ke = 14 THEN subtotal END) AS m_14_value,
    COUNT(DISTINCT CASE WHEN bulan_ke = 15 THEN concat(kodetoko, nomortransaksi) END) AS m_15,
    SUM(CASE WHEN bulan_ke = 15 THEN qty END) AS m_15_qty,
    SUM(CASE WHEN bulan_ke = 15 THEN subtotal END) AS m_15_value,
    COUNT(DISTINCT CASE WHEN bulan_ke = 16 THEN concat(kodetoko, nomortransaksi) END) AS m_16,
    SUM(CASE WHEN bulan_ke = 16 THEN qty END) AS m_16_qty,
    SUM(CASE WHEN bulan_ke = 16 THEN subtotal END) AS m_16_value,
    COUNT(DISTINCT CASE WHEN bulan_ke = 17 THEN concat(kodetoko, nomortransaksi) END) AS m_17,
    SUM(CASE WHEN bulan_ke = 17 THEN qty END) AS m_17_qty,
    SUM(CASE WHEN bulan_ke = 17 THEN subtotal END) AS m_17_value,
    COUNT(DISTINCT CASE WHEN bulan_ke = 18 THEN concat(kodetoko, nomortransaksi) END) AS m_18,
    SUM(CASE WHEN bulan_ke = 18 THEN qty END) AS m_18_qty,
    SUM(CASE WHEN bulan_ke = 18 THEN subtotal END) AS m_18_value,
    COUNT(DISTINCT CASE WHEN bulan_ke = 19 THEN concat(kodetoko, nomortransaksi) END) AS m_19,
    SUM(CASE WHEN bulan_ke = 19 THEN qty END) AS m_19_qty,
    SUM(CASE WHEN bulan_ke = 19 THEN subtotal END) AS m_19_value,
    COUNT(DISTINCT CASE WHEN bulan_ke = 20 THEN concat(kodetoko, nomortransaksi) END) AS m_20,
    SUM(CASE WHEN bulan_ke = 20 THEN qty END) AS m_20_qty,
    SUM(CASE WHEN bulan_ke = 20 THEN subtotal END) AS m_20_value,
    COUNT(DISTINCT CASE WHEN bulan_ke = 21 THEN concat(kodetoko, nomortransaksi) END) AS m_21,
    SUM(CASE WHEN bulan_ke = 21 THEN qty END) AS m_21_qty,
    SUM(CASE WHEN bulan_ke = 21 THEN subtotal END) AS m_21_value,
    COUNT(DISTINCT CASE WHEN bulan_ke = 22 THEN concat(kodetoko, nomortransaksi) END) AS m_22,
    SUM(CASE WHEN bulan_ke = 22 THEN qty END) AS m_22_qty,
    SUM(CASE WHEN bulan_ke = 22 THEN subtotal END) AS m_22_value,
    COUNT(DISTINCT CASE WHEN bulan_ke = 23 THEN concat(kodetoko, nomortransaksi) END) AS m_23,
    SUM(CASE WHEN bulan_ke = 23 THEN qty END) AS m_23_qty,
    SUM(CASE WHEN bulan_ke = 23 THEN subtotal END) AS m_23_value,
    COUNT(DISTINCT CASE WHEN bulan_ke = 24 THEN concat(kodetoko, nomortransaksi) END) AS m_24,
    SUM(CASE WHEN bulan_ke = 24 THEN qty END) AS m_24_qty,
    SUM(CASE WHEN bulan_ke = 24 THEN subtotal END) AS m_24_value,
    COUNT(DISTINCT CASE WHEN bulan_ke = 25 THEN concat(kodetoko, nomortransaksi) END) AS m_25,
    SUM(CASE WHEN bulan_ke = 25 THEN qty END) AS m_25_qty,
    SUM(CASE WHEN bulan_ke = 25 THEN subtotal END) AS m_25_value,
    COUNT(DISTINCT CASE WHEN bulan_ke = 26 THEN concat(kodetoko, nomortransaksi) END) AS m_26,
    SUM(CASE WHEN bulan_ke = 26 THEN qty END) AS m_26_qty,
    SUM(CASE WHEN bulan_ke = 26 THEN subtotal END) AS m_26_value,
    COUNT(DISTINCT CASE WHEN bulan_ke = 27 THEN concat(kodetoko, nomortransaksi) END) AS m_27,
    SUM(CASE WHEN bulan_ke = 27 THEN qty END) AS m_27_qty,
    SUM(CASE WHEN bulan_ke = 27 THEN subtotal END) AS m_27_value,
    COUNT(DISTINCT CASE WHEN bulan_ke = 28 THEN concat(kodetoko, nomortransaksi) END) AS m_28,
    SUM(CASE WHEN bulan_ke = 28 THEN qty END) AS m_28_qty,
    SUM(CASE WHEN bulan_ke = 28 THEN subtotal END) AS m_28_value,
    COUNT(DISTINCT CASE WHEN bulan_ke = 29 THEN concat(kodetoko, nomortransaksi) END) AS m_29,
    SUM(CASE WHEN bulan_ke = 29 THEN qty END) AS m_29_qty,
    SUM(CASE WHEN bulan_ke = 29 THEN subtotal END) AS m_29_value,
    COUNT(DISTINCT CASE WHEN bulan_ke = 30 THEN concat(kodetoko, nomortransaksi) END) AS m_30,
    SUM(CASE WHEN bulan_ke = 30 THEN qty END) AS m_30_qty,
    SUM(CASE WHEN bulan_ke = 30 THEN subtotal END) AS m_30_value,
    COUNT(DISTINCT CASE WHEN bulan_ke = 31 THEN concat(kodetoko, nomortransaksi) END) AS m_31,
    SUM(CASE WHEN bulan_ke = 31 THEN qty END) AS m_31_qty,
    SUM(CASE WHEN bulan_ke = 31 THEN subtotal END) AS m_31_value,
    COUNT(DISTINCT CASE WHEN bulan_ke = 32 THEN concat(kodetoko, nomortransaksi) END) AS m_32,
    SUM(CASE WHEN bulan_ke = 32 THEN qty END) AS m_32_qty,
    SUM(CASE WHEN bulan_ke = 32 THEN subtotal END) AS m_32_value,
    COUNT(DISTINCT CASE WHEN bulan_ke = 33 THEN concat(kodetoko, nomortransaksi) END) AS m_33,
    SUM(CASE WHEN bulan_ke = 33 THEN qty END) AS m_33_qty,
    SUM(CASE WHEN bulan_ke = 33 THEN subtotal END) AS m_33_value,
    COUNT(DISTINCT CASE WHEN bulan_ke = 34 THEN concat(kodetoko, nomortransaksi) END) AS m_34,
    SUM(CASE WHEN bulan_ke = 34 THEN qty END) AS m_34_qty,
    SUM(CASE WHEN bulan_ke = 34 THEN subtotal END) AS m_34_value,
    COUNT(DISTINCT CASE WHEN bulan_ke = 35 THEN concat(kodetoko, nomortransaksi) END) AS m_35,
    SUM(CASE WHEN bulan_ke = 35 THEN qty END) AS m_35_qty,
    SUM(CASE WHEN bulan_ke = 35 THEN subtotal END) AS m_35_value,
    COUNT(DISTINCT CASE WHEN bulan_ke = 36 THEN concat(kodetoko, nomortransaksi) END) AS m_36,
    SUM(CASE WHEN bulan_ke = 36 THEN qty END) AS m_36_qty,
    SUM(CASE WHEN bulan_ke = 36 THEN subtotal END) AS m_36_value
FROM (
    SELECT 
        *,
        (EXTRACT(YEAR FROM age(date_trunc('month', now()), date_trunc('month', tanggal))) * 12 +
         EXTRACT(MONTH FROM age(date_trunc('month', now()), date_trunc('month', tanggal))) - 1) AS bulan_ke
    FROM public.%I
    WHERE idcabang = %L
        AND idjenisproduk <> 4 
        AND statusproduk <> 'K'::bpchar 
        AND kodeproduk != '3091844001'
        AND category IN ('ACCU','BODY THROTTLE','ENGINE FLUSH','INJECTOR CLEANER','OIL','TIRE SEALANT','TUBELESS','TUBETYPE')
        AND nopolisi IS NOT NULL 
        AND nopolisi <> ''
        AND tanggal >= date_trunc('month', now() - interval '37 months')
        AND tanggal < date_trunc('month', now())
) x
GROUP BY idcabang, namacabang, nopolisi
HAVING COUNT(DISTINCT CASE WHEN bulan_ke = 0 THEN concat(kodetoko, nomortransaksi) END) <> 0;
$f$, tablename, p_idcabang);

END;
$$ LANGUAGE plpgsql;


select * from smi_trx_qty_value_category_m1_temp;
SELECT fc_smi_trx_qty_value_category_m1_temp('smi_rms10_transaksi_toko_perjenis_member_v3',2);
SELECT fc_smi_trx_qty_value_category_m1_temp('smi_rms11_transaksi_toko_perjenis_member_v3',6);
SELECT fc_smi_trx_qty_value_category_m1_temp('smi_rms12_transaksi_toko_perjenis_member_v3',7);
SELECT generate_qty_value_category_m1_temp('public','smi_rms15_transaksi_toko_perjenis_member_v3',9);
SELECT fc_smi_trx_qty_value_category_m1_temp('smi_rms20_transaksi_toko_perjenis_member_v3',3);
SELECT fc_smi_trx_qty_value_category_m1_temp('smi_rms20_transaksi_toko_perjenis_member_v3',8);
SELECT fc_smi_trx_qty_value_category_m1_temp('smi_rms21_transaksi_toko_perjenis_member_v3',4);
SELECT fc_smi_trx_qty_value_category_m1_temp('smi_rms22_transaksi_toko_perjenis_member_v3',5);