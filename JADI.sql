
SELECT 
x.Tanggal_Transaksi,
x.Kode_Toko,
x.Nomor_Transaksi,
x.Id_Login_Imen,
x.Nama_Login_Imen,
x.Jabatan_Login_Imen,				
x.Id_Tek_Instalasi,
x.Nama_Tek_Instalasi,
x.Jabatan_Tek_Instalasi
FROM (
		SELECT 
			A.nomortransaksi AS Nomor_Transaksi,
			CONVERT(DATE, A.tglBisnis) AS Tanggal_Transaksi,
			A.Kodetoko AS Kode_Toko,
			CONVERT(VARCHAR, d.namatoko) AS Nama_Toko,
			CONVERT(VARCHAR, b.nopolisi) AS Nopol,
			CONVERT(VARCHAR, ITH.id_login_imen) AS Id_Login_Imen,
			CONVERT(VARCHAR, ITH.nama_login_imen) AS Nama_Login_Imen,
			CONVERT(VARCHAR, ITH.jabatan_login_imen) AS Jabatan_Login_Imen,				
			CONVERT(VARCHAR, ITH.id_tek_instalasi) AS Id_Tek_Instalasi,
			CONVERT(VARCHAR, ITH.nama_tek_instalasi) AS Nama_Tek_Instalasi,
			CONVERT(VARCHAR, ITH.jabatan_tek_instalasi) AS Jabatan_Tek_Instalasi
		FROM MB_DC_SBY.dbo.transaksitokoheader AS a WITH (NOLOCK)
	    JOIN MB_DC_SBY.dbo.smitransaksitokoperjenismember AS b WITH (NOLOCK) ON b.kodetoko = a.kodetoko AND b.NomorTransaksi = a.nomorTransaksi
	    JOIN MB_DC_SBY.dbo.msttoko AS d WITH (NOLOCK)
	        ON d.kodetoko = a.kodetoko
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
					FROM MB_DC_SBY.dbo.imen_trans_header AS x1 WITH (NOLOCK) 
					LEFT JOIN (
						SELECT 
							y1.kodetoko, y1.empid, y1.idjabatan, y2.job, y2.job_name, y1.name, y1.statusdata
						FROM MB_DC_SBY.dbo.Smimsttokomekanik AS y1 WITH (NOLOCK)
						JOIN MB_DC_SBY.dbo.SMIMstJabatanIMen AS y2 WITH (NOLOCK) ON y2.idjabatan = y1.idjabatan
						GROUP BY y1.kodetoko, y1.empid, y1.idjabatan, y2.job, y2.job_name, y1.name, y1.statusdata
					) AS y ON y.kodetoko = x1.kode_toko AND y.empid = x1.trans_users
					LEFT JOIN (
						SELECT 
							z1.kodetoko, z1.empid, z1.idjabatan, z2.job, z2.job_name, z1.name, z1.statusdata
						FROM MB_DC_SBY.dbo.Smimsttokomekanik AS z1 WITH (NOLOCK)
						JOIN MB_DC_SBY.dbo.SMIMstJabatanIMen AS z2 WITH (NOLOCK) ON z2.idjabatan = z1.idjabatan
						GROUP BY z1.kodetoko, z1.empid, z1.idjabatan, z2.job, z2.job_name, z1.name, z1.statusdata
					) AS z ON z.kodetoko = x1.kode_toko AND z.empid = x1.trans_users_tek
					WHERE 
						x1.trans_status = 1000000004
				) AS ITH1
				WHERE ITH1.myrank = 1
			) AS ITH ON 
				CONVERT(DATE, ITH.trans_date) = CONVERT(DATE, A.tglbisnis) 
				AND ITH.kode_toko = B.kodetoko 
				AND ITH.trans_nopol_all = b.nopolisi 
				AND ITH.trans_id = A.nomorImen
		WHERE
			CONVERT(DATE, A.tglBisnis) BETWEEN '2025-01-01' AND '2025-07-31'
) AS x;