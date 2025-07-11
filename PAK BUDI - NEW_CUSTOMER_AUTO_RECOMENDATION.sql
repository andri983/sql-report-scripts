-- Transaksi BAN tanpa OLI dan tanpa SERVICE
SELECT a.tanggal, a.namacabang, a.kodetoko, a.namatoko, a.nomortransaksi, a.brand, a.category, a.idproduk, a.kodeproduk, a.namapanjang, a.qty, a.hpp, a.subtotalhpp, a.hargajualnormal, a.pdisc1, a.pdisc2, a.disc, a.hargajualtransaksi, a.subtotal, a.nopolisi, a.idjenismember, a.namajenismember
FROM public.smi_rms22_transaksi_toko_perjenis_member_v3 a
LEFT JOIN public.smi_mst_oli oli ON oli.kodeproduk = a.kodeproduk
LEFT JOIN public.smi_mst_servis serv ON serv.kodeproduk = a.kodeproduk
WHERE a.tanggal BETWEEN '2025-07-02' AND '2025-07-02'
GROUP BY a.tanggal, a.namacabang, a.kodetoko, a.namatoko, a.nomortransaksi, a.brand, a.category, a.idproduk, a.kodeproduk, a.namapanjang, a.qty, a.hpp, a.subtotalhpp, a.hargajualnormal, a.pdisc1, a.pdisc2, a.disc, a.hargajualtransaksi, a.subtotal, a.nopolisi, a.idjenismember, a.namajenismember
HAVING
    SUM(CASE WHEN a.iddivisi = 1 THEN 1 ELSE 0 END) > 0 AND
    SUM(CASE WHEN oli.kodeproduk IS NOT NULL THEN 1 ELSE 0 END) = 0 AND
    SUM(CASE WHEN serv.kodeproduk IS NOT NULL THEN 1 ELSE 0 END) = 0
;


--TRANSAKSI BAN
SELECT a.kodetoko,a.nomortransaksi
FROM public.smi_rms22_transaksi_toko_perjenis_member_v3 a
WHERE a.tanggal BETWEEN '2025-07-02' AND '2025-07-02'
AND a.iddivisi = 1;

--TRANSAKSI OLI
SELECT a.kodetoko,a.nomortransaksi
FROM public.smi_rms22_transaksi_toko_perjenis_member_v3 a
join public.smi_mst_oli b on b.kodeproduk=a.kodeproduk
WHERE a.tanggal BETWEEN '2025-07-02' AND '2025-07-02';

--TRANSAKSI SERVICE
SELECT a.kodetoko,a.nomortransaksi
FROM public.smi_rms22_transaksi_toko_perjenis_member_v3 a
join public.smi_mst_servis b on b.kodeproduk=a.kodeproduk
WHERE a.tanggal BETWEEN '2025-07-02' AND '2025-07-02';
