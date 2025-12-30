CREATE OR REPLACE FUNCTION generate_last_trx_category_m1_temp
(
    p_schema   TEXT,
    p_table    TEXT,
    p_idcabang INT
)
RETURNS VOID AS $$
BEGIN
    EXECUTE
        'INSERT INTO public.smi_last_trx_category_m1_temp (
            insertdate,
            namacabang,
            nopolisi,
            category,
            tanggal
        )
        SELECT 
            now() AS insertdate,
            namacabang,
            nopolisi,
            category,
            tanggal
        FROM (
            SELECT DISTINCT ON (a.nopolisi, a.category)
                a.tanggal,
                a.nopolisi,
                a.namacabang,
                a.category
            FROM '
            || quote_ident(p_schema) || '.' || quote_ident(p_table) || ' a
            WHERE a.idcabang = ' || p_idcabang || '
              AND a.tanggal >= date_trunc(''MONTH'', CURRENT_DATE) - INTERVAL ''37 months''
              AND a.tanggal <  date_trunc(''MONTH'', CURRENT_DATE)
              AND a.idjenisproduk <> 4 
              AND a.statusproduk <> ''K''
              AND a.category IN (''ACCU'',''BODY THROTTLE'',''ENGINE FLUSH'',
                                 ''INJECTOR CLEANER'',''OIL'',''TIRE SEALANT'',
                                 ''TUBETYPE'',''TUBELESS'')
              AND a.kodeproduk <> ''3091844001''
              AND a.nopolisi IS NOT NULL
              AND a.nopolisi <> ''''
            ORDER BY a.nopolisi, a.category, a.tanggal DESC
        ) x';
END;
$$ LANGUAGE plpgsql;





select * from public.smi_last_trx_category_m1_temp;
--SELECT smi_last_trx_category_m1_temp('smi_rms20_transaksi_toko_perjenis_member_v3',8);
SELECT generate_last_trx_category_m1_temp('public','smi_rms15_transaksi_toko_perjenis_member_v3',9);
