SELECT * FROM public.mb_transaksi_profile('2025-01-01', '2025-10-14');
DROP FUNCTION public.mb_transaksi_profile(date, date);

CREATE OR REPLACE FUNCTION public.mb_transaksi_profile(startdate date, enddate date)
RETURNS TABLE(tanggal date, namacabang character varying, kodetoko bigint, namatoko character varying, nomortransaksi bigint, brand character varying, jenis character varying, namasubdepartement character varying, category character varying, idproduk integer, kodeproduk character varying, namapanjang character varying, qty numeric, hpp numeric, subtotalhpp numeric, hargajualnormal numeric, disc numeric, hargajualtransaksi numeric, subtotal numeric, nopolisi character varying, namajenismember character varying, trxcode text, filtercustomer text, brandmotor character varying, typemotor character varying, varianmotor character varying, jenismotor character varying, tahunmotor character varying)
LANGUAGE plpgsql
AS $function$
BEGIN
   RETURN QUERY
    WITH 
    transaksitoko AS (
        SELECT
            a.tanggal, a.namacabang, a.kodetoko, a.namatoko, a.nomortransaksi, a.brand, a.jenis, 
            a.namasubdepartement, a.category, a.idproduk, a.kodeproduk, a.namapanjang, a.qty, 
            a.hpp, a.subtotalhpp, a.hargajualnormal, a.disc, a.hargajualtransaksi, a.subtotal, 
            a.nopolisi, a.namajenismember, 
            a.kodetoko || '' || a.nomortransaksi AS trxcode
        FROM public.mb_rms10_transaksi_toko_perjenis_member_v3 AS a
      	WHERE a.tanggal BETWEEN startdate AND enddate
		-- WHERE a.tanggal BETWEEN '2025-10-14' AND '2025-10-14'
        AND a.idcabang=2
--        AND a.idjenisproduk<>4
--        AND a.statusproduk<>'K'

        UNION ALL

        SELECT
            a.tanggal, a.namacabang, a.kodetoko, a.namatoko, a.nomortransaksi, a.brand, a.jenis, 
            a.namasubdepartement, a.category, a.idproduk, a.kodeproduk, a.namapanjang, a.qty, 
            a.hpp, a.subtotalhpp, a.hargajualnormal, a.disc, a.hargajualtransaksi, a.subtotal, 
            a.nopolisi, a.namajenismember, 
            a.kodetoko || '' || a.nomortransaksi AS trxcode
        FROM public.mb_rms20_transaksi_toko_perjenis_member_v3 AS a
       	WHERE a.tanggal BETWEEN startdate AND enddate
		-- WHERE a.tanggal BETWEEN '2025-10-14' AND '2025-10-14'
        AND a.idcabang=3
--        AND a.idjenisproduk<>4
--        AND a.statusproduk<>'K'
    ),
    filtercustomer AS (
        SELECT
            DISTINCT a.orderdate, a.store, a.nomortransaksi, a.nopolisi, a.filternewcustomer
        FROM mb_rms10_rpt.mb_transaksi_detail_produk_all AS a
--       WHERE a.orderdate BETWEEN startdate AND enddate
		-- WHERE a.orderdate BETWEEN '2025-10-14' AND '2025-10-14'

        UNION ALL

        SELECT
            DISTINCT a.orderdate, a.store, a.nomortransaksi, a.nopolisi, a.filternewcustomer
        FROM mb_rms20_rpt.mb_transaksi_detail_produk_all AS a
--       WHERE a.orderdate BETWEEN startdate AND enddate
		-- WHERE a.orderdate BETWEEN '2025-10-14' AND '2025-10-14'
    ),    
    profilekendaraan AS (
        SELECT
            m.nopolisi,
            m.namamember,
            bm.brandmotor,
            tm.typemotor,
            vm.varianmotor,
            tmotor.tahunmotor,
            jm.jenismotor
        FROM mb_rms01_mbho.mstmember AS m
        LEFT JOIN mb_rms01_mbho.SMIMstTahunMotor AS tmotor 
            ON tmotor.idTahunMotor = m.idTahunMotor
        LEFT JOIN mb_rms01_mbho.SMIMstTypeMotor AS tm 
            ON tm.idTypeMotor = m.idTypeMotor
        LEFT JOIN mb_rms01_mbho.SMIMstBrandMotor AS bm 
            ON bm.idBrandMotor = tm.idBrandMotor
        LEFT JOIN mb_rms01_mbho.SMIMstVarianMotor AS vm 
            ON vm.idVarianMotor = tmotor.idVarianMotor 
            AND vm.idTypeMotor = m.idTypeMotor 
            AND vm.idTypeMotor = tm.idTypeMotor
        LEFT JOIN mb_rms01_mbho.SMIMstJenisMotor AS jm 
            ON jm.idJenisMotor = tm.idJenisMotor
    )
    SELECT 
        a.tanggal, a.namacabang, a.kodetoko, a.namatoko, a.nomortransaksi, a.brand, a.jenis, 
        a.namasubdepartement, a.category, a.idproduk, a.kodeproduk, a.namapanjang, a.qty, 
        a.hpp, a.subtotalhpp, a.hargajualnormal, a.disc, a.hargajualtransaksi, a.subtotal, 
        a.nopolisi, a.namajenismember, a.trxcode, c.filternewcustomer as filtercustomer,
        b.brandmotor, b.typemotor, b.varianmotor, b.jenismotor, b.tahunmotor
    FROM transaksitoko a
    LEFT JOIN profilekendaraan b 
        ON b.nopolisi = a.nopolisi
    LEFT JOIN filtercustomer c
        ON c.nopolisi = a.nopolisi 
		AND c.orderdate = a.tanggal
		AND c.store = a.namatoko
		AND c.nomortransaksi = a.nomortransaksi
        ;
END;
$function$
;
