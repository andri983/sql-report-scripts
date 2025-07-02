--POSTGRESQL
SELECT 
	a.tanggal as "TANGGAL",
	a.namacabang as "NAMA CABANG",
	a.kodetoko as "KODE TOKO",
	a.nomortransaksi as "NOMOR TRANSAKSI",
	a.namajenismember as "JENIS MEMBER"
FROM public.SMI_rms22_Transaksi_Toko_Perjenis_Member_v3 AS a
WHERE a.tanggal BETWEEN '2024-10-01' AND '2024-10-31'
AND a.idcabang=5
GROUP BY a.tanggal, a.namacabang, a.kodetoko, a.nomortransaksi, a.namajenismember
UNION ALL
SELECT 
	a.tanggal as "TANGGAL",
	a.namacabang as "NAMA CABANG",
	a.kodetoko as "KODE TOKO",
	a.nomortransaksi as "NOMOR TRANSAKSI",
	a.namajenismember as "JENIS MEMBER"
FROM public.SMI_rms21_Transaksi_Toko_Perjenis_Member_v3 AS a
WHERE a.tanggal BETWEEN '2024-10-01' AND '2024-10-31'
AND a.idcabang=4
GROUP BY a.tanggal, a.namacabang, a.kodetoko, a.nomortransaksi, a.namajenismember
UNION ALL
SELECT 
	a.tanggal as "TANGGAL",
	a.namacabang as "NAMA CABANG",
	a.kodetoko as "KODE TOKO",
	a.nomortransaksi as "NOMOR TRANSAKSI",
	a.namajenismember as "JENIS MEMBER"
FROM public.SMI_rms20_Transaksi_Toko_Perjenis_Member_v3 AS a
WHERE a.tanggal BETWEEN '2024-10-01' AND '2024-10-31'
--AND a.idcabang in (3,8)
GROUP BY a.tanggal, a.namacabang, a.kodetoko, a.nomortransaksi, a.namajenismember
UNION ALL
SELECT 
	a.tanggal as "TANGGAL",
	a.namacabang as "NAMA CABANG",
	a.kodetoko as "KODE TOKO",
	a.nomortransaksi as "NOMOR TRANSAKSI",
	a.namajenismember as "JENIS MEMBER"
FROM public.SMI_rms15_Transaksi_Toko_Perjenis_Member_v3 AS a
WHERE a.tanggal BETWEEN '2024-10-01' AND '2024-10-31'
AND a.idcabang=9
GROUP BY a.tanggal, a.namacabang, a.kodetoko, a.nomortransaksi, a.namajenismember
UNION ALL
SELECT 
	a.tanggal as "TANGGAL",
	a.namacabang as "NAMA CABANG",
	a.kodetoko as "KODE TOKO",
	a.nomortransaksi as "NOMOR TRANSAKSI",
	a.namajenismember as "JENIS MEMBER"
FROM public.SMI_rms12_Transaksi_Toko_Perjenis_Member_v3 AS a
WHERE a.tanggal BETWEEN '2024-10-01' AND '2024-10-31'
AND a.idcabang=7
GROUP BY a.tanggal, a.namacabang, a.kodetoko, a.nomortransaksi, a.namajenismember
UNION ALL
SELECT 
	a.tanggal as "TANGGAL",
	a.namacabang as "NAMA CABANG",
	a.kodetoko as "KODE TOKO",
	a.nomortransaksi as "NOMOR TRANSAKSI",
	a.namajenismember as "JENIS MEMBER"
FROM public.SMI_rms11_Transaksi_Toko_Perjenis_Member_v3 AS a
WHERE a.tanggal BETWEEN '2024-10-01' AND '2024-10-31'
AND a.idcabang=6
GROUP BY a.tanggal, a.namacabang, a.kodetoko, a.nomortransaksi, a.namajenismember
UNION ALL
SELECT 
	a.tanggal as "TANGGAL",
	a.namacabang as "NAMA CABANG",
	a.kodetoko as "KODE TOKO",
	a.nomortransaksi as "NOMOR TRANSAKSI",
	a.namajenismember as "JENIS MEMBER"
FROM public.SMI_rms10_Transaksi_Toko_Perjenis_Member_v3 AS a
WHERE a.tanggal BETWEEN '2024-10-01' AND '2024-10-31'
AND a.idcabang=2
GROUP BY a.tanggal, a.namacabang, a.kodetoko, a.nomortransaksi, a.namajenismember