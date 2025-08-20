SELECT 
x.Nama_Cabang,
x.Nomor_Transaksi,
x.Tanggal_Transaksi,
x.Kode_Toko,
x.Nama_Toko,
x.Nopol,
x.Kode_Barang,
x.Nama_Barang,
x.Qty,
x.Sub_Total,
x.Id_Login_Imen,
x.Nama_Login_Imen,
x.Jabatan_Login_Imen,				
x.Id_Tek_Instalasi,
x.Nama_Tek_Instalasi,
x.Jabatan_Tek_Instalasi,
x.namaposisi,
x.nomorurut,
x.jenismotor,
x.idTahunMotor--,
--x.Transaksi_Count,
 --CASE
 --       WHEN x.Transaksi_Count = 1 THEN 'New'
 --       ELSE 'Existing'
 --   END AS Type_Member
FROM (
		SELECT 
			J.NamaCabang AS Nama_Cabang,
			Q.nomortransaksi AS Nomor_Transaksi,
			CONVERT(DATE, Q.Tanggal) AS Tanggal_Transaksi,
			G.Kodetoko AS Kode_Toko,
			CONVERT(VARCHAR, G.namatoko) AS Nama_Toko,
			CONVERT(VARCHAR, K.nopolisi) AS Nopol,
			-- CONVERT(INT, ITH.trans_pit_line) AS Line_No,
			CONVERT(VARCHAR(10), C.KodeProduk) AS Kode_Barang,
			CONVERT(VARCHAR, C.Namapanjang) AS Nama_Barang,
			Q.Qty,
			Q.Subtotal AS Sub_Total,
			CONVERT(VARCHAR, ITH.id_login_imen) AS Id_Login_Imen,
			CONVERT(VARCHAR, ITH.nama_login_imen) AS Nama_Login_Imen,
			CONVERT(VARCHAR, ITH.jabatan_login_imen) AS Jabatan_Login_Imen,				
			CONVERT(VARCHAR, ITH.id_tek_instalasi) AS Id_Tek_Instalasi,
			CONVERT(VARCHAR, ITH.nama_tek_instalasi) AS Nama_Tek_Instalasi,
			CONVERT(VARCHAR, ITH.jabatan_tek_instalasi) AS Jabatan_Tek_Instalasi,
			m.namaposisi,
			A.nomorurut,
			P.jenismotor,
			R.idTahunMotor--,
			 --null AS nomorRefTransaksi,
			--NC.count AS Transaksi_Count
		FROM 
			PB_DC.dbo.TransaksiTokoDetail AS A WITH (NOLOCK)
			JOIN PB_DC.dbo.TransaksiTokoHeader AS B WITH (NOLOCK) ON B.kodetoko = A.kodetoko AND B.NomorTransaksi = A.NomorTransaksi
			JOIN SMI_DB_Reporting_JKT.dbo.SMI_Daily_Nett_Sales_Cab_JKT_MTD_3 AS Q WITH (NOLOCK) ON 
				Q.kodetoko = A.kodetoko AND Q.NomorTransaksi = A.NomorTransaksi AND Q.idproduk = A.idProduk
				AND Q.kodetoko = B.kodetoko AND Q.NomorTransaksi = B.NomorTransaksi --AND Q.nomorurut = A.nomorUrut
			JOIN PB_DC.dbo.smitransaksitokoperjenismember AS K WITH (NOLOCK) ON 
				K.kodetoko = A.kodetoko AND K.kodetoko = B.kodetoko AND K.kodetoko = Q.kodetoko 
				AND K.NomorTransaksi = A.NomorTransaksi AND K.NomorTransaksi = B.NomorTransaksi 
				AND K.NomorTransaksi = Q.NomorTransaksi
			JOIN PB_DC.dbo.mstproduk AS C WITH (NOLOCK) ON C.idproduk = A.Idproduk AND C.idproduk = Q.Idproduk
			JOIN PB_DC.dbo.msttoko AS G WITH (NOLOCK) ON G.kodetoko = A.kodetoko AND G.kodetoko = B.kodetoko AND G.kodetoko = Q.kodetoko
			JOIN PB_DC.dbo.mastertoolstoko AS H WITH (NOLOCK) ON H.kodetoko = A.kodetoko AND H.kodetoko = B.kodetoko AND H.kodetoko = Q.kodetoko
			JOIN PB_DC.dbo.mstcabang AS J WITH (NOLOCK) ON H.idcabang = J.idcabang
			LEFT JOIN PB_DC.dbo.mstmember AS N WITH (NOLOCK) ON N.nopolisi = K.nopolisi
			LEFT JOIN PB_DC.dbo.Smimsttypemotor AS O WITH (NOLOCK) ON O.idTypeMotor = N.idTypeMotor
			LEFT JOIN PB_DC.dbo.Smimstjenismotor AS P WITH (NOLOCK) ON P.idJenisMotor = O.idJenisMotor
			LEFT JOIN PB_DC.dbo.smimsttahunmotor AS R WITH (NOLOCK) ON R.idtahunmotor = N.idtahunmotor
			LEFT JOIN (
				SELECT
					ITH1.trans_id,
					ITH1.trans_date,
					ITH1.trans_status,
					ITH1.kode_toko,
					ITH1.trans_nopol_all,
					ITH1.trans_pit_line,
					CASE 
						WHEN ITH1.trans_users_tek_idjabatan = 596 THEN ITH1.trans_users_tek
						WHEN ITH1.trans_users_idjabatan = 596 THEN ITH1.trans_users
					END AS IdTL,
					CASE
						WHEN ITH1.trans_users_tek_idjabatan = 596 THEN ITH1.trans_users_tek_name
						WHEN ITH1.trans_users_idjabatan = 596 THEN ITH1.trans_users_name
					END AS TL,
					CASE
						WHEN ITH1.trans_users_tek_idjabatan = 210 THEN ITH1.trans_users_tek
						WHEN ITH1.trans_users_idjabatan = 210 THEN ITH1.trans_users
					END AS IdATL,
					CASE 
						WHEN ITH1.trans_users_tek_idjabatan = 210 THEN ITH1.trans_users_tek_name
						WHEN ITH1.trans_users_idjabatan = 210 THEN ITH1.trans_users_name
					END AS ATL,
					CASE 
						WHEN ITH1.trans_users_tek_idjabatan = 322 THEN ITH1.trans_users_tek
						WHEN ITH1.trans_users_idjabatan = 322 THEN ITH1.trans_users
					END AS IdMekanik,
					CASE 
						WHEN ITH1.trans_users_tek_idjabatan = 322 THEN ITH1.trans_users_tek_name
						WHEN ITH1.trans_users_idjabatan = 322 THEN ITH1.trans_users_name
					END AS Mekanik,
					ITH1.trans_users AS id_login_imen,
					ITH1.trans_users_name AS nama_login_imen,
					ITH1.trans_users_job_name AS jabatan_login_imen,
					ITH1.trans_users_tek AS id_tek_instalasi,
					ITH1.trans_users_tek_name AS nama_tek_instalasi,
					ITH1.trans_users_tek_job_name AS jabatan_tek_instalasi
				FROM (
					SELECT 
						ROW_NUMBER() OVER (PARTITION BY x1.kode_toko, x1.trans_id ORDER BY x1.trans_date DESC) AS myrank,
						x1.trans_id, x1.trans_date, x1.trans_status,
						x1.kode_toko,
						x1.trans_nopol_all,
						x1.trans_pit_line,
						x1.trans_users,
						y.name AS trans_users_name,
						y.idjabatan AS trans_users_idjabatan,
						y.job AS trans_users_job,
						y.job_name AS trans_users_job_name,			
						x1.trans_users_tek,
						z.name AS trans_users_tek_name,
						z.idjabatan AS trans_users_tek_idjabatan,
						z.job AS trans_users_tek_job,
						z.job_name AS trans_users_tek_job_name
					FROM PB_DC.dbo.imen_trans_header AS x1 WITH (NOLOCK) 
					LEFT JOIN (
						SELECT 
							y1.kodetoko, y1.empid, y1.idjabatan, y2.job, y2.job_name, y1.name, y1.statusdata
						FROM PB_DC.dbo.Smimsttokomekanik AS y1 WITH (NOLOCK)
						JOIN PB_DC.dbo.SMIMstJabatanIMen AS y2 WITH (NOLOCK) ON y2.idjabatan = y1.idjabatan
						GROUP BY y1.kodetoko, y1.empid, y1.idjabatan, y2.job, y2.job_name, y1.name, y1.statusdata
					) AS y ON y.kodetoko = x1.kode_toko AND y.empid = x1.trans_users
					LEFT JOIN (
						SELECT 
							z1.kodetoko, z1.empid, z1.idjabatan, z2.job, z2.job_name, z1.name, z1.statusdata
						FROM PB_DC.dbo.Smimsttokomekanik AS z1 WITH (NOLOCK)
						JOIN PB_DC.dbo.SMIMstJabatanIMen AS z2 WITH (NOLOCK) ON z2.idjabatan = z1.idjabatan
						GROUP BY z1.kodetoko, z1.empid, z1.idjabatan, z2.job, z2.job_name, z1.name, z1.statusdata
					) AS z ON z.kodetoko = x1.kode_toko AND z.empid = x1.trans_users_tek
					WHERE 
						x1.trans_status = 1000000004
				) AS ITH1
				WHERE ITH1.myrank = 1
			) AS ITH ON 
				CONVERT(DATE, ITH.trans_date) = CONVERT(DATE, B.tglbisnis) 
				AND ITH.kode_toko = B.kodetoko 
				AND ITH.trans_nopol_all = k.nopolisi 
				AND ITH.trans_id = B.nomorImen
			LEFT JOIN (
				SELECT 
					kodetoko, nomortransaksi, idproduk, namaposisi 
				FROM PB_DC.dbo.V_SMI_POSISI_PRODUK_SPA
			) AS m ON m.kodetoko = Q.kodetoko AND m.nomortransaksi = Q.nomortransaksi AND m.idproduk = Q.idproduk
			--JOIN NopolisiCount AS NC ON NC.nopolisi = K.nopolisi
		WHERE
			Q.statusproduk <> 'K'
			AND Q.idjenisproduk <> 4
			AND CONCAT(A.kodetoko, '-', A.nomortransaksi, '-', A.nomorurut, '-', A.idproduk) = Q.notrans
			--AND CONVERT(DATE, Q.Tanggal) BETWEEN @StartDate AND @EndDate
			--AND J.idcabang = 5
			--AND A.nomorTransaksi = '202507030004' AND A.kodeToko = '3021065'
			AND ((A.nomorTransaksi = '202507010036' AND A.kodeToko = '3021082' ) OR
(A.nomorTransaksi = '202507030004' AND A.kodeToko = '3021065' ) OR
(A.nomorTransaksi = '202507030005' AND A.kodeToko = '3021172' ) OR
(A.nomorTransaksi = '202507030012' AND A.kodeToko = '3021301' ) OR
(A.nomorTransaksi = '202507040020' AND A.kodeToko = '3021111' ) OR
(A.nomorTransaksi = '202507080024' AND A.kodeToko = '3021074' ) OR
(A.nomorTransaksi = '202507080014' AND A.kodeToko = '3021341' ) OR
(A.nomorTransaksi = '202507120008' AND A.kodeToko = '3021380' ) OR
(A.nomorTransaksi = '202507120016' AND A.kodeToko = '3021380' ) OR
(A.nomorTransaksi = '202507120020' AND A.kodeToko = '3021059' ) OR
(A.nomorTransaksi = '202507130020' AND A.kodeToko = '3021074' ) OR
(A.nomorTransaksi = '202507130025' AND A.kodeToko = '3021095' ) OR
(A.nomorTransaksi = '202507130023' AND A.kodeToko = '3021403' ) OR
(A.nomorTransaksi = '202507140020' AND A.kodeToko = '3021217' ) OR
(A.nomorTransaksi = '202507150007' AND A.kodeToko = '3021048' ))
) AS x
