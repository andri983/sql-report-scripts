--------------------------------------------------------------------------------------------------------------------------SQL SERVER

USE [MB_DB_Reporting_JKT] --- CREATE RMS10
USE [MB_DB_Reporting_SBY] --- CREATE RMS20
GO

/****** Object:  Table [dbo].[MB_trx_oil_others]    Script Date: 5/28/2025 9:07:38 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [dbo].[MB_trx_oil_others](
	[tanggal] [date] NULL,
	[namacabang] [varchar](40) NULL,
	[nopolisi] [varchar](15) NULL,
	[namajenismember] [varchar](30) NULL,
	[kodetoko] [bigint] NULL,
	[nomortransaksi] [bigint] NULL,
	[oli] [int] NULL,
	[idproduk] [int] NULL,
	[kodeproduk] [varchar](10) NULL,
	[namapanjang] [varchar](100) NULL,
	[durabilitykm] [int] NULL,
	[durabilityday] [int] NULL
) ON [PRIMARY]
GO


--------------------------------------------------------------------------------------------------------------------------


USE [MB_DB_Reporting_JKT] --- CREATE RMS10
USE [MB_DB_Reporting_SBY] --- CREATE RMS20
GO

/****** Object:  StoredProcedure [dbo].[GetMBTrxGoliahtData]    Script Date: 4/29/2025 11:43:28 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO



CREATE PROCEDURE [dbo].[GetMBTrxGoliahtData] (@date1 date,@date2 date)
AS
BEGIN

INSERT INTO mb_trx_oil_others
SELECT 
z.tanggal, 
z.namacabang,
z.nopolisi,
z.namaJenisMember,
z.kodetoko,
z.nomortransaksi,
z.oli,
z.idproduk,
z1.kodeproduk,
z1.namapanjang,
z2.durabilitykm,
z2.durabilityday
FROM (
	SELECT 
		y.tanggal, 
		y.namacabang,
		y.nopolisi,
		y.kodetoko,
		y.nomortransaksi,
		SUM(y.oli) AS oli,
		SUM(y.idproduk) AS idproduk,
		y.idjenismember,
		y.namajenismember
	FROM (
		SELECT 
		c.tanggal, 
		c.namacabang,
		d.nopolisi,
		c.kodetoko,
		c.nomortransaksi,
		CASE 
			WHEN c.kodeproduk in ('2051201001','2051202002') THEN 1
			ELSE 0
		END as oli,
		   CASE 
			  WHEN c.kodeproduk in ('2051201001','2051202002') THEN c.idproduk
			  ELSE 0
		  END as idproduk,
		e.idjenismember,
		e.namajenismember
		FROM 
			MB_DC_SBY.dbo.TransaksiTokoDetail AS A WITH (NOLOCK)
			JOIN MB_DC_SBY.dbo.TransaksiTokoHeader AS B WITH (NOLOCK) ON B.kodetoko = A.kodetoko AND B.NomorTransaksi = A.NomorTransaksi
			JOIN MB_DB_Reporting_SBY.[dbo].MB_rms10_Transaksi_Toko_Perjenis_Member_v3 AS C WITH (NOLOCK) ON 
				C.kodetoko = A.kodetoko AND C.NomorTransaksi = A.NomorTransaksi AND C.idproduk = A.idProduk
				AND C.kodetoko = B.kodetoko AND C.NomorTransaksi = B.NomorTransaksi
			JOIN MB_DC_SBY.dbo.smitransaksitokoperjenismember AS D WITH (NOLOCK) ON 
				D.kodetoko = A.kodetoko AND D.kodetoko = B.kodetoko AND D.kodetoko = C.kodetoko 
				AND D.NomorTransaksi = A.NomorTransaksi AND D.NomorTransaksi = B.NomorTransaksi 
				AND D.NomorTransaksi = C.NomorTransaksi
			JOIN MB_DC_SBY.dbo.mstJenisMember AS E WITH (NOLOCK) ON E.idjenismember = D.idjenismember
			  	WHERE NOT EXISTS (
					SELECT 1
					FROM MB_DC_SBY.dbo.SMIMstPlatBlackList AS b
					WHERE b.nopolisi = d.nopolisi
					)  
				AND c.idjenisproduk <> 4 
				AND c.statusproduk <> 'K'
				AND d.idjenismember in (2,23,24,25)
				--AND c.tanggal BETWEEN '2025-01-01' AND '2025-04-24'
				AND c.tanggal BETWEEN @date1 AND @date2
				--AND d.nopolisi ='DK1220DK'
		GROUP BY 
				c.tanggal, 
				c.namacabang,
				d.nopolisi,
				c.kodetoko,
				c.nomortransaksi, c.iddivisi, c.idsubdepartement, c.iddepartement, c.idbrand, c.kodeproduk, c.idproduk,
				e.idjenismember,
				e.namajenismember
	) AS y
	GROUP BY 
		y.tanggal, y.namacabang, y.nopolisi, y.kodetoko, y.nomortransaksi, y.idjenismember,	y.namajenismember
) AS z
LEFT JOIN 
	MB_DC_SBY.dbo.MstProduk z1 WITH (NOLOCK) ON z1.idproduk = z.idproduk
LEFT JOIN 
	MB_DC_SBY.dbo.SMIMstProdukDurability z2 WITH (NOLOCK) ON z2.idproduk = z.idproduk
ORDER BY  
      z.nopolisi,
	  z.tanggal,
      z.kodetoko,
      z.nomortransaksi
END;
GO


--------------------------------------------------------------------------------------------------------------------------


USE [MB_DB_Reporting_JKT] --- CREATE RMS10
USE [MB_DB_Reporting_SBY] --- CREATE RMS20
GO

/****** Object:  Table [dbo].[MB_trx_oil_goliaht_monitoring]    Script Date: 5/28/2025 9:11:07 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [dbo].[MB_trx_oil_goliaht_monitoring](
	[insert_date] [datetime] NOT NULL,
	[namacabang] [varchar](40) NOT NULL,
	[notelp] [varchar](15) NOT NULL,
	[kolom_b] [varchar](30) NOT NULL,
	[kolom_c] [varchar](15) NOT NULL,
	[kolom_d] [date] NOT NULL,
	[kolom_e] [varchar](100) NULL,
	[kolom_f] [int] NULL,
	[kolom_g] [int] NULL,
	[kolom_h] [int] NULL,
	[kolom_i] [decimal](18, 2) NULL,
	[kolom_j] [varchar](30) NULL,
	[kolom_k] [decimal](18, 2) NULL,
	[kolom_l] [decimal](18, 2) NULL,
	[kolom_m] [int] NULL,
	[kolom_n] [date] NULL,
	[kolom_n1] [bigint] NULL,
	[kolom_n2] [bigint] NULL,
	[kolom_o] [bigint] NULL,
	[kolom_p] [date] NULL,
	[kolom_p1] [bigint] NULL,
	[kolom_p2] [bigint] NULL,
	[kolom_q] [bigint] NULL,
	[kolom_r] [date] NULL,
	[kolom_r1] [bigint] NULL,
	[kolom_r2] [bigint] NULL,
	[kolom_s] [bigint] NULL,
	[kolom_t] [date] NULL,
	[kolom_t1] [bigint] NULL,
	[kolom_t2] [bigint] NULL,
	[kolom_u] [bigint] NULL,
	[kolom_u1] [date] NULL,
	[kolom_u2] [bigint] NULL,
	[kolom_u3] [bigint] NULL,
	[kolom_u4] [bigint] NULL,
	[kolom_v] [int] NULL,
	[kolom_w] [float] NULL,
	[kolom_x] [decimal](18, 2) NULL,
	[kolom_y] [varchar](30) NULL,
	[kolom_z] [varchar](30) NULL,
	[kolom_aa] [date] NULL,
	[kolom_ab] [varchar](30) NULL,
	[kolom_ac] [varchar](30) NULL,
 CONSTRAINT [PK_MB_trx_oil_goliaht_monitoring] PRIMARY KEY CLUSTERED 
(
	[namacabang] ASC,
	[notelp] ASC,
	[kolom_b] ASC,
	[kolom_c] ASC,
	[kolom_d] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY]
GO

ALTER TABLE [dbo].[MB_trx_oil_goliaht_monitoring] ADD  DEFAULT (getdate()) FOR [insert_date]
GO


--------------------------------------------------------------------------------------------------------------------------


USE [MB_DB_Reporting_JKT] --- CREATE RMS10
USE [MB_DB_Reporting_SBY] --- CREATE RMS20
GO

/****** Object:  StoredProcedure [dbo].[GetMBTrxGoliahtReportMonitoring]    Script Date: 4/29/2025 11:43:56 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE PROCEDURE [dbo].[GetMBTrxGoliahtReportMonitoring]
AS
BEGIN
WITH transactions AS (
	SELECT 
		b.tanggal, b.namacabang, b.nopolisi, b.namamember, b.notelp, b.kodetoko, b.nomortransaksi, b.oli, b.odometer, b.trx_number
	FROM (
		SELECT 
			a.tanggal, a.namacabang, a.nopolisi, c.namamember, c.notelp, a.kodetoko, a.nomortransaksi, a.oli, b.odometer,
			ROW_NUMBER() OVER (PARTITION BY a.nopolisi ORDER BY a.tanggal DESC, a.nomortransaksi DESC) AS trx_number
		FROM 
			mb_trx_oil_others a
		LEFT JOIN MB_DC_SBY.DBO.SMITransaksiOdometerMember b ON b.noPolisi = a.nopolisi AND b.kodetoko = a.kodetoko AND b.nomortransaksi = a.nomortransaksi
		LEFT JOIN MB_DC_SBY.DBO.MstMember c ON c.noPolisi = a.nopolisi
		WHERE a.oli = 1
		--AND a.nopolisi IN ('DK0000SNG', 'DK2005AG', 'DK2238XY','DK2251ACL','DK2253OW','AE3710VR')
	) AS b
	WHERE b.trx_number <= 3
		UNION ALL
	SELECT 
		b.tanggal, b.namacabang, b.nopolisi, b.namamember, b.notelp, b.kodetoko, b.nomortransaksi, b.oli, b.odometer, b.trx_number
	FROM (
		SELECT 
			a.tanggal, a.namacabang, a.nopolisi, c.namamember, c.notelp, a.kodetoko, a.nomortransaksi, a.oli, b.odometer,
			ROW_NUMBER() OVER (PARTITION BY a.nopolisi ORDER BY a.tanggal DESC, a.nomortransaksi DESC) AS trx_number
		FROM 
			mb_trx_oil_others a
		LEFT JOIN MB_DC_SBY.DBO.SMITransaksiOdometerMember b ON b.noPolisi = a.nopolisi AND b.kodetoko = a.kodetoko AND b.nomortransaksi = a.nomortransaksi
		LEFT JOIN MB_DC_SBY.DBO.MstMember c ON c.noPolisi = a.nopolisi
		WHERE a.oli = 0
		--AND a.nopolisi IN ('DK0000SNG', 'DK2005AG', 'DK2238XY','DK2251ACL','DK2253OW','AE3710VR')
	) AS b
	WHERE b.trx_number <= 3
),
durability AS (
		SELECT 
			DISTINCT
			a.tanggal, 
			a.kodetoko, 
			a.nomortransaksi, 
			d.nopolisi, 
			e.namajenismember,
			a.kodeproduk, 
			a.namapanjang, 
			c.durabilitykm, 
			c.durabilityday
		FROM 
			MB_DB_Reporting_SBY.[dbo].MB_rms10_Transaksi_Toko_Perjenis_Member_v3 a
		JOIN 
			mb_trx_oil_others b 
			ON b.kodetoko = a.kodetoko AND b.nomortransaksi = a.nomortransaksi AND b.tanggal = a.tanggal
		JOIN 
			MB_DC_SBY.dbo.SMIMstProdukDurability c 
			ON c.idproduk = a.idproduk
		JOIN MB_DC_SBY.dbo.smitransaksitokoperjenismember AS D WITH (NOLOCK) ON 
			D.kodetoko = A.kodetoko AND D.kodetoko = B.kodetoko --AND D.kodetoko = C.kodetoko 
			AND D.NomorTransaksi = A.NomorTransaksi AND D.NomorTransaksi = B.NomorTransaksi --AND E.NomorTransaksi = C.NomorTransaksi
		JOIN MB_DC_SBY.dbo.mstJenisMember AS E WITH (NOLOCK) ON E.idjenismember = D.idjenismember 
		WHERE NOT EXISTS (
				SELECT 1
				FROM MB_DC_SBY.dbo.SMIMstPlatBlackList AS b
				WHERE b.nopolisi = d.nopolisi
				)   
			AND a.kodeproduk in ('2051201001','2051202002')
),
questioner AS (
	SELECT
		--a1.trans_nopol_all, a1.resultquestioner, a1.createdate, a1.trx_number
		a1.trans_nopol_all,
		CASE 
			WHEN convert(bigint,a1.resultquestioner) > 100 THEN 100 
			ELSE a1.resultquestioner 
		END AS resultquestioner,
		a1.createdate,
		a1.trx_number
	FROM (
			SELECT a.trans_nopol_all, a.resultquestioner, a.createdate,
			ROW_NUMBER() OVER (PARTITION BY a.trans_nopol_all ORDER BY a.createdate ASC) AS trx_number
			FROM MB_DC_SBY.dbo.Imen_Trans_Questioner a
		)AS a1
	WHERE  a1.trx_number=1
)
insert into mb_trx_oil_goliaht_monitoring([namacabang],[notelp],[kolom_b],[kolom_c],[kolom_d],[kolom_e],[kolom_f],[kolom_g],[kolom_h],[kolom_i],[kolom_j],[kolom_k],
[kolom_l],[kolom_m],[kolom_n],[kolom_n1],[kolom_n2],[kolom_o],[kolom_p],[kolom_p1],[kolom_p2],[kolom_q],[kolom_r],[kolom_r1],[kolom_r2],[kolom_s],[kolom_t],[kolom_t1],
[kolom_t2],[kolom_u],[kolom_u1],[kolom_u2],[kolom_u3],[kolom_u4],[kolom_v],[kolom_w],[kolom_x],[kolom_y],[kolom_z],[kolom_aa],[kolom_ab],[kolom_ac])
SELECT
y4.namacabang,
y4.notelp,
y4.kolom_b,		--B-->>Nama Pelanggan
y4.kolom_c,		--C-->>Plat Nomor
y4.kolom_d,		--D-->>Tanggal Report (Hari Ini)
y4.kolom_e,		--E-->>Tipe Oli Terakhir Penggantian
y4.kolom_f,		--F-->>Ketahanan Oli (KM)
y4.kolom_g,		--G-->>Jumlah Hari Maksimal Untuk Penggantian Oli
y4.kolom_h,		--H-->>90% Ketahanan Oli (KM)
y4.kolom_i,		--I-->>Minimal Jarak Berkendara Per Hari (KM)
y4.kolom_j,		--J-->>Tipe Pekerjaan
y4.kolom_k,		--K-->>Estimasi Jarak Berkendara Per Hari Terkait Pekerjaan (KM) 'Fase 1'
y4.kolom_l,		--L-->>Estimasi Jarak Berkendara Per Hari Terkait Pekerjaan (KM) 'Fase 1' (Round Trip Dari Tempat Kerja)		-----TABLE BARU BELUM ADA REVISI EXCEL TGL 22082024
y4.kolom_m,		--M-->>Hari Untuk Reminder Berikutnya Sejak Penggantian Oli Terakhir Dengan Estimasi Jarak Tempuh 'Fase 1'
y4.kolom_n,		--N-->>Tanggal Transaksi Oli Terakhir
y4.kolom_n1,
y4.kolom_n2,
y4.kolom_o,		--O-->>Odometer Saat Transaksi Oli Terakhir
y4.kolom_p,		--P-->>Tanggal Transaksi Oli Atau Non-Oli Terakhir
y4.kolom_p1,
y4.kolom_p2,
y4.kolom_q,		--Q-->>Odometer Saat Tanggal Transaksi Oli Atau Non-Oli Terakhir
y4.kolom_r,		--R-->>Tanggal Saat Transaksi Oli Kedua Dari Terakhir
y4.kolom_r1,
y4.kolom_r2,
y4.kolom_s,		--S-->>Odometer Saat Transaksi Oli Kedua Dari Terakhir
y4.kolom_t,		--T-->>Tanggal Transaksi Oli Atau Non-Oli Kedua Dari Terakhir
y4.kolom_t1,
y4.kolom_t2,
y4.kolom_u,		--U-->>Odometer Saat Tanggal Transaksi Oli Atau Non-Oli Kedua Dari Terakhir
y4.kolom_u1,
y4.kolom_u2,
y4.kolom_u3,
y4.kolom_u4,
y4.kolom_v,		--V-->>Asumsi Maksimal Rata2 Berkendara Per Hari (KM)
y4.kolom_w,		--W-->>Asumsi Minimal Hari Reminder
--y4.kolom_x,		--X-->>Estimasi Jarak Berkendara Per Hari (KM) Fase 2
CAST(y4.kolom_x AS DECIMAL(10,1)) AS kolom_x,	--X-->>Estimasi Jarak Berkendara Per Hari (KM) Fase 2
y4.kolom_y,		--Y-->>Estimasi Jarak Berkendara Per Hari (KM) Yang Digunakan (Antara Metode Fase 1 & 2)
y4.kolom_z,		--Z-->>Jumlah Hari Untuk Reminder Selanjutnya Sejak Penggantian Oli Terakhir Dengan Estimasi Jarak Berkendara 'Fase 2'
y4.kolom_aa,	--AA-->>Tanggal Reminder Pertama
y4.kolom_ab,	--AB-->>Jumlah Hari Untuk Reminder Selanjutnya Sejak Penggantian Oli Terakhir Yang Digunakan
y4.kolom_ac		--AC-->>Jumlah Hari Untuk ke Reminder Pertama
FROM (
	SELECT
	y3.namacabang,
	y3.notelp,
	y3.namamember AS kolom_b,		--B-->>Nama Pelanggan
	y3.nopolisi AS kolom_c,			--C-->>Plat Nomor
	y3.tglreport AS kolom_d,		--D-->>Tanggal Report (Hari Ini)
	y3.namapanjang AS kolom_e,		--E-->>Tipe Oli Terakhir Penggantian
	y3.durabilitykm AS kolom_f,		--F-->>Ketahanan Oli (KM)
	y3.durabilityday AS kolom_g,	--G-->>Jumlah Hari Maksimal Untuk Penggantian Oli
	y3.ketahananoli AS kolom_h,		--H-->>90% Ketahanan Oli (KM)
	y3.minimaljarakperhari AS kolom_i,		--I-->>Minimal Jarak Berkendara Per Hari (KM)
	y3.typepekerjaan AS kolom_j,			--J-->>Tipe Pekerjaan
	y3.estimasijarakberkendara AS kolom_k,	--K-->>Estimasi Jarak Berkendara Per Hari Terkait Pekerjaan (KM) 'Fase 1'
	y3.roundtrip AS kolom_L,				--L-->>Estimasi Jarak Berkendara Per Hari Terkait Pekerjaan (KM) 'Fase 1' (Round Trip Dari Tempat Kerja)		-----TABLE BARU BELUM ADA REVISI EXCEL TGL 22082024
	y3.reminderoliterakhr AS kolom_M,		--M-->>Hari Untuk Reminder Berikutnya Sejak Penggantian Oli Terakhir Dengan Estimasi Jarak Tempuh 'Fase 1'
	y3.trx1_tanggal AS kolom_n,		--N-->>Tanggal Transaksi Oli Terakhir
	y3.trx1_kodetoko AS kolom_n1,
	y3.trx1_nomortransaksi AS kolom_n2,
	y3.trx1_odometer AS kolom_o,	--O-->>Odometer Saat Transaksi Oli Terakhir
	y3.trx2_tanggal AS kolom_p,		--P-->>Tanggal Transaksi Oli Atau Non-Oli Terakhir
	y3.trx2_kodetoko AS kolom_p1,
	y3.trx2_nomortransaksi AS kolom_p2,
	y3.trx2_odometer AS kolom_q,	--Q-->>Odometer Saat Tanggal Transaksi Oli Atau Non-Oli Terakhir
	y3.trx3_tanggal AS kolom_r,		--R-->>Tanggal Saat Transaksi Oli Kedua Dari Terakhir
	y3.trx3_kodetoko AS kolom_r1,
	y3.trx3_nomortransaksi AS kolom_r2,
	y3.trx3_odometer AS kolom_s,	--S-->>Odometer Saat Transaksi Oli Kedua Dari Terakhir
	y3.trx4_tanggal AS kolom_t,		--T-->>Tanggal Transaksi Oli Atau Non-Oli Kedua Dari Terakhir
	y3.trx4_kodetoko AS kolom_t1,
	y3.trx4_nomortransaksi AS kolom_t2,
	y3.trx4_odometer AS kolom_u,	--U-->>Odometer Saat Tanggal Transaksi Oli Atau Non-Oli Kedua Dari Terakhir
	y3.trx5_tanggal AS kolom_u1,
	y3.trx5_kodetoko AS kolom_u2,
	y3.trx5_nomortransaksi AS kolom_u3,
	y3.trx5_odometer AS kolom_u4,
	y3.asumsimax AS kolom_v,		--V-->>Asumsi Maksimal Rata2 Berkendara Per Hari (KM)
	y3.asumsimin AS kolom_w,		--W-->>Asumsi Minimal Hari Reminder
	y3.estjarakfase2 AS kolom_x,	--X-->>Estimasi Jarak Berkendara Per Hari (KM) Fase 2
	CASE 
		WHEN y3.trx1_tanggal IS NULL OR y3.tglremindpertama IS NULL THEN 'N/A'
		WHEN DATEDIFF(day, y3.trx1_tanggal, y3.tglremindpertama) = 0 THEN 'N/A'
		ELSE CONVERT(varchar, y3.ketahananoli / DATEDIFF(day, y3.trx1_tanggal, y3.tglremindpertama))
	END AS kolom_y,			--Y-->>Estimasi Jarak Berkendara Per Hari (KM) Yang Digunakan (Antara Metode Fase 1 & 2)
	y3.jmlhariremindselanjutnya AS kolom_z,	--Z-->>Jumlah Hari Untuk Reminder Selanjutnya Sejak Penggantian Oli Terakhir Dengan Estimasi Jarak Berkendara 'Fase 2'
	y3.tglremindpertama AS kolom_aa,			--AA-->>Tanggal Reminder Pertama
	CASE 
		WHEN y3.tglremindpertama IS NOT NULL AND y3.trx1_tanggal IS NOT NULL 
			THEN ISNULL(CONVERT(varchar, DATEDIFF(day, y3.trx1_tanggal, y3.tglremindpertama)), 'N/A')
		ELSE 'N/A'
	END AS kolom_ab,	--AB-->>Jumlah Hari Untuk Reminder Selanjutnya Sejak Penggantian Oli Terakhir Yang Digunakan
	CASE 
		WHEN y3.tglremindpertama IS NOT NULL AND y3.tglreport IS NOT NULL 
			THEN ISNULL(CONVERT(varchar, DATEDIFF(day, y3.tglreport, y3.tglremindpertama)), 'N/A')
		ELSE 'N/A'
	END AS kolom_ac,		--AC-->>Jumlah Hari Untuk ke Reminder Pertama
	'' kolom_ae ---TEST
	FROM (
		SELECT
		y2.namacabang,
		y2.notelp,
		y2.namamember,	--B-->>Nama Pelanggan
		y2.nopolisi,	--C-->>Plat Nomor
		y2.tglreport,	--D-->>Tanggal Report (Hari Ini)
		y2.namapanjang,		--E-->>Tipe Oli Terakhir Penggantian
		y2.durabilitykm,	--F-->>Ketahanan Oli (KM)
		y2.durabilityday,	--G-->>Jumlah Hari Maksimal Untuk Penggantian Oli
		y2.ketahananoli,	--H-->>90% Ketahanan Oli (KM)
		y2.minimaljarakperhari,		--I-->>Minimal Jarak Berkendara Per Hari (KM)
		y2.typepekerjaan,			--J-->>Tipe Pekerjaan
		y2.estimasijarakberkendara,	--K-->>Estimasi Jarak Berkendara Per Hari Terkait Pekerjaan (KM) 'Fase 1'
		y2.roundtrip,			--L-->>Estimasi Jarak Berkendara Per Hari Terkait Pekerjaan (KM) 'Fase 1' (Round Trip Dari Tempat Kerja)		-----TABLE BARU BELUM ADA REVISI EXCEL TGL 22082024
		y2.reminderoliterakhr,		--M-->>Hari Untuk Reminder Berikutnya Sejak Penggantian Oli Terakhir Dengan Estimasi Jarak Tempuh 'Fase 1'
		y2.trx1_tanggal,	--N-->>Tanggal Transaksi Oli Terakhir
		y2.trx1_kodetoko,
		y2.trx1_nomortransaksi,
		y2.trx1_odometer,	--O-->>Odometer Saat Transaksi Oli Terakhir
		y2.trx2_tanggal,	--P-->>Tanggal Transaksi Oli Atau Non-Oli Terakhir
		y2.trx2_kodetoko,
		y2.trx2_nomortransaksi,
		y2.trx2_odometer,	--Q-->>Odometer Saat Tanggal Transaksi Oli Atau Non-Oli Terakhir
		y2.trx3_tanggal,	--R-->>Tanggal Saat Transaksi Oli Kedua Dari Terakhir
		y2.trx3_kodetoko,
		y2.trx3_nomortransaksi,
		y2.trx3_odometer,	--S-->>Odometer Saat Transaksi Oli Kedua Dari Terakhir
		y2.trx4_tanggal,	--T-->>Tanggal Transaksi Oli Atau Non-Oli Kedua Dari Terakhir
		y2.trx4_kodetoko,
		y2.trx4_nomortransaksi,
		y2.trx4_odometer,	--U-->>Odometer Saat Tanggal Transaksi Oli Atau Non-Oli Kedua Dari Terakhir
		y2.trx5_tanggal,
		y2.trx5_kodetoko,
		y2.trx5_nomortransaksi,
		y2.trx5_odometer,
		y2.asumsimax,		--V-->>Asumsi Maksimal Rata2 Berkendara Per Hari (KM)
		y2.asumsimin,		--W-->>Asumsi Minimal Hari Reminder
		y2.estjarakfase2,	--X-->>Estimasi Jarak Berkendara Per Hari (KM) Fase 2
		CASE 
			WHEN y2.estjarakfase2 = 0 THEN 'N/A' 
			ELSE CAST(ROUND(CAST(y2.ketahananoli AS FLOAT) / y2.estjarakfase2, 0) AS VARCHAR)
		END AS jmlhariremindselanjutnya,		--Z-->>Jumlah Hari Untuk Reminder Selanjutnya Sejak Penggantian Oli Terakhir Dengan Estimasi Jarak Berkendara 'Fase 2'
		CASE 
			WHEN y2.estjarakfase2 = 0 
					AND y2.estimasijarakberkendara >= y2.minimaljarakperhari 
					AND y2.estimasijarakberkendara <= y2.asumsimax THEN DATEADD(DAY, y2.reminderoliterakhr, y2.trx1_tanggal)
			WHEN y2.estjarakfase2 = 0 
					AND y2.estimasijarakberkendara > y2.asumsimax THEN DATEADD(DAY, y2.asumsimin, y2.trx1_tanggal)
			WHEN y2.estjarakfase2 = 0 
					AND y2.estimasijarakberkendara < y2.minimaljarakperhari THEN DATEADD(DAY, y2.durabilityday, y2.trx1_tanggal)
			WHEN y2.estjarakfase2 <> 0 THEN 
				CASE 
					WHEN ROUND(CAST(y2.ketahananoli AS FLOAT) / y2.estjarakfase2, 0) < y2.durabilityday 
							AND ROUND(CAST(y2.ketahananoli AS FLOAT) / y2.estjarakfase2, 0) > 0 THEN DATEADD(DAY, ROUND(CAST(y2.ketahananoli AS FLOAT) / y2.estjarakfase2, 0), y2.trx1_tanggal)
					WHEN y2.reminderoliterakhr < y2.durabilityday 
							AND y2.reminderoliterakhr > 0 THEN DATEADD(DAY, y2.reminderoliterakhr, y2.trx1_tanggal)
					ELSE DATEADD(DAY, y2.durabilityday, y2.trx1_tanggal)
				END
		END AS tglremindpertama		--AA-->>Tanggal Reminder Pertama
		FROM (
			SELECT 
				y.namacabang,
				y.notelp,
				y.namamember,		--KOLOM B >>Nama Pelanggan
				y.nopolisi,			--KOLOM C >>Plat Nomor
				CAST(GETDATE() AS Date) AS tglreport,	--KOLOM D >>Tanggal Report (Hari Ini)
				y.namapanjang,		--KOLOM E >>Tipe Oli Terakhir Penggantian
				y.durabilitykm,		--KOLOM F >>Ketahanan Oli (KM)
				y.durabilityday,	--KOLOM G >>Jumlah Hari Maksimal Untuk Penggantian Oli
				((y.durabilitykm * 95) / 100) AS ketahananoli,		--KOLOM H >>90% Ketahanan Oli (KM)
				--(((y.durabilitykm * 90) / 100) / NULLIF(y.durabilityday, 0)) AS minimaljarakperhari,	--KOLOM I >>Minimal Jarak Berkendara Per Hari (KM)
				CAST(((y.durabilitykm * 95) / 100) AS DECIMAL) / NULLIF(CAST(y.durabilityday AS DECIMAL), 0) AS minimaljarakperhari,
				CASE 
					WHEN y.namajenismember = 'COMMERCIAL DRIVER' OR y.namajenismember = 'COMMERCIAL DRIVER NON MEMBER' THEN 'Ojol'
					ELSE 'Pengendara Biasa'
				END AS typepekerjaan,		--KOLOM J >>Tipe Pekerjaan
				--y.resultquestioner AS estimasijarakberkendara,			--KOLOM K >>Estimasi Jarak Berkendara Ke Tempat Kerja (KM) 'Fase 1' **TABLE BARU BELUM ADA
				CAST(((y.durabilitykm * 95) / 100) AS DECIMAL) / NULLIF(CAST(y.durabilityday AS DECIMAL), 0) AS estimasijarakberkendara,			--KOLOM K >>Estimasi Jarak Berkendara Ke Tempat Kerja (KM) 'Fase 1' **TABLE BARU BELUM ADA
				--CASE 
				--	WHEN y.namajenismember = 'COMMERCIAL DRIVER' OR y.namajenismember = 'COMMERCIAL DRIVER NON MEMBER' THEN y.resultquestioner * 1 --Ojol
				--	ELSE y.resultquestioner * 2  --Pengendara Biasa
				--END AS roundtrip,	--KOLOM L >>Estimasi Jarak Berkendara Per Hari Terkait Pekerjaan (KM) 'Fase 1' (Round Trip Dari Tempat Kerja)
				CASE 
					WHEN y.namajenismember = 'COMMERCIAL DRIVER' OR y.namajenismember = 'COMMERCIAL DRIVER NON MEMBER' THEN CAST(((y.durabilitykm * 95) / 100) AS DECIMAL) / NULLIF(CAST(y.durabilityday AS DECIMAL), 0) * 1 --Ojol
					ELSE CAST(((y.durabilitykm * 95) / 100) AS DECIMAL) / NULLIF(CAST(y.durabilityday AS DECIMAL), 0) * 2  --Pengendara Biasa
				END AS roundtrip,	--KOLOM L >>Estimasi Jarak Berkendara Per Hari Terkait Pekerjaan (KM) 'Fase 1' (Round Trip Dari Tempat Kerja)
				--COALESCE(ROUND(((y.durabilitykm * 90) / 100) / NULLIF(
				--														CASE 
				--															WHEN y.namajenismember = 'COMMERCIAL DRIVER' OR y.namajenismember = 'COMMERCIAL DRIVER NON MEMBER' THEN y.resultquestioner * 1 --Ojol
				--															ELSE y.resultquestioner * 2  --Pengendara Biasa
				--														END
				--														, 0), 0), 0) AS reminderoliterakhr,	--KOLOM M>>Hari Untuk Reminder Berikutnya Sejak Penggantian Oli Terakhir Dengan Estimasi Jarak Tempuh 'Fase 1'
				COALESCE(ROUND(((y.durabilitykm * 95) / 100) / NULLIF(
																		CASE 
																			WHEN y.namajenismember = 'COMMERCIAL DRIVER' OR y.namajenismember = 'COMMERCIAL DRIVER NON MEMBER' THEN CAST(((y.durabilitykm * 95) / 100) AS DECIMAL) / NULLIF(CAST(y.durabilityday AS DECIMAL), 0) * 1 --Ojol
																			ELSE CAST(((y.durabilitykm * 95) / 100) AS DECIMAL) / NULLIF(CAST(y.durabilityday AS DECIMAL), 0) * 2  --Pengendara Biasa
																		END
																		, 0), 0), 0) AS reminderoliterakhr,	--KOLOM M>>Hari Untuk Reminder Berikutnya Sejak Penggantian Oli Terakhir Dengan Estimasi Jarak Tempuh 'Fase 1'
				y.trx1_tanggal,		--KOLOM N>>Tanggal Transaksi Oli Terakhir
				y.trx1_kodetoko,
				y.trx1_nomortransaksi,
				y.trx1_odometer,	--KOLOM 0>>Odometer Saat Transaksi Oli Terakhir
				y.trx2_tanggal,		--KOLOM P>>Tanggal Transaksi Oli Atau Non-Oli Terakhir
				y.trx2_kodetoko,
				y.trx2_nomortransaksi,
				y.trx2_odometer,	--KOLOM Q>>Odometer Saat Tanggal Transaksi Oli Atau Non-Oli Terakhir
				y.trx3_tanggal,		--KOLOM R>>Tanggal Saat Transaksi Oli Kedua Dari Terakhir
				y.trx3_kodetoko,
				y.trx3_nomortransaksi,
				y.trx3_odometer,	--KOLOM S>>Odometer Saat Transaksi Oli Kedua Dari Terakhir
				y.trx4_tanggal,		--KOLOM T>>Tanggal Transaksi Oli Atau Non-Oli Kedua Dari Terakhir
				y.trx4_kodetoko,
				y.trx4_nomortransaksi,
				y.trx4_odometer,	--KOLOM U>>Odometer Saat Tanggal Transaksi Oli Atau Non-Oli Kedua Dari Terakhir
				y.trx5_tanggal,
				y.trx5_kodetoko,
				y.trx5_nomortransaksi,
				y.trx5_odometer,
				300 AS asumsimax,	--KOLOM V>>Asumsi Maksimal Rata2 Berkendara Per Hari (KM) **hardcode 300 INFO PAK BUDI
				--ROUND(CAST(((y.durabilitykm * 90) / 100) AS FLOAT) / CAST(200 AS FLOAT), 0) AS asumsimin,	--KOLOM W>>Asumsi Minimal Hari Reminder
				CASE 
					WHEN CAST(((y.durabilitykm * 95) / 100) AS FLOAT) / NULLIF(CAST(y.durabilityday AS FLOAT), 0) IS NULL THEN NULL
					ELSE ((y.durabilitykm * 95) / 100) / 
						 (CAST(((y.durabilitykm * 95) / 100) AS FLOAT) / NULLIF(CAST(y.durabilityday AS FLOAT), 0))
				END AS asumsimin,	--KOLOM W>>Asumsi Minimal Hari Reminder Update 20241016 Req Pak Jordi
				CASE 
					--WHEN y.trx3_tanggal IS NULL AND y.trx4_tanggal IS NULL AND y.trx2_tanggal = y.trx1_tanggal THEN 0
					WHEN (y.trx3_tanggal IS NULL OR y.trx3_tanggal = '1900-01-01') 
					AND (y.trx4_tanggal IS NULL OR y.trx4_tanggal = '1900-01-01') 
					AND y.trx2_tanggal = y.trx1_tanggal THEN 0.0	--Update 20241018 Req Pak Jordi
					ELSE 
						CASE 
							WHEN y.trx2_tanggal > y.trx1_tanggal AND y.trx2_odometer > y.trx1_odometer THEN 
								CASE 
									WHEN ROUND(CAST(y.trx2_odometer - y.trx1_odometer AS DECIMAL(38, 2)) / NULLIF(DATEDIFF(DAY, y.trx1_tanggal, y.trx2_tanggal), 0), 2) < 300 THEN 
										ROUND(CAST(y.trx2_odometer - y.trx1_odometer AS DECIMAL(38, 2)) / NULLIF(DATEDIFF(DAY, y.trx1_tanggal, y.trx2_tanggal), 0), 2)
									ELSE 300 
								END
							WHEN 
								(CASE WHEN y.trx1_tanggal > y.trx2_tanggal THEN y.trx1_tanggal ELSE y.trx2_tanggal END) > (CASE WHEN y.trx3_tanggal > y.trx4_tanggal THEN y.trx3_tanggal ELSE y.trx4_tanggal END) THEN 
								CASE 
									WHEN ROUND(CAST(
										(CASE WHEN y.trx2_odometer > y.trx1_odometer THEN y.trx2_odometer ELSE y.trx1_odometer END) - 
										(CASE WHEN y.trx4_odometer > y.trx3_odometer THEN y.trx4_odometer ELSE y.trx3_odometer END) AS DECIMAL(38, 1)) 
										/ NULLIF(DATEDIFF(DAY, 
											(CASE WHEN y.trx4_tanggal > y.trx3_tanggal THEN y.trx4_tanggal ELSE y.trx3_tanggal END), 
											(CASE WHEN y.trx2_tanggal > y.trx1_tanggal THEN y.trx2_tanggal ELSE y.trx1_tanggal END)), 0), 1) < 300 THEN 
										ROUND(CAST(
											(CASE WHEN y.trx2_odometer > y.trx1_odometer THEN y.trx2_odometer ELSE y.trx1_odometer END) - 
											(CASE WHEN y.trx4_odometer > y.trx3_odometer THEN y.trx4_odometer ELSE y.trx3_odometer END) AS DECIMAL(38, 1)) 
										/ NULLIF(DATEDIFF(DAY, 
											(CASE WHEN y.trx4_tanggal > y.trx3_tanggal THEN y.trx4_tanggal ELSE y.trx3_tanggal END), 
											(CASE WHEN y.trx2_tanggal > y.trx1_tanggal THEN y.trx2_tanggal ELSE y.trx1_tanggal END)), 0), 1)
									ELSE 300 
								END
							ELSE 300 
						END
				END AS estjarakfase2
			FROM (
				SELECT 
					x2.namacabang, 
					x2.namamember,
					x2.notelp,
					x2.nopolisi,
					x2.tglreport,
					x2.namapanjang,
					x2.durabilitykm,
					x2.durabilityday,
					x2.namaJenisMember,
					x2.trx1_tanggal,
					x2.trx1_kodetoko,
					x2.trx1_nomortransaksi,
					x2.trx1_odometer,
					x2.trx2_tanggal,
					x2.trx2_kodetoko,
					x2.trx2_nomortransaksi,
					x2.trx2_odometer,
					COALESCE(x2.trx3_tanggal, '') AS trx3_tanggal,
					x2.trx3_kodetoko,
					x2.trx3_nomortransaksi,
					COALESCE(x2.trx3_odometer, '') AS trx3_odometer,
					COALESCE(x2.trx4_tanggal, '') AS trx4_tanggal,
					x2.trx4_kodetoko,
					x2.trx4_nomortransaksi,
					COALESCE(x2.trx4_odometer, '') AS trx4_odometer,
					x2.trx5_tanggal,
					x2.trx5_kodetoko,
					x2.trx5_nomortransaksi,
					x2.trx5_odometer--,
					--x2.resultquestioner
				FROM (
					SELECT 
						x1.namacabang, 
						x1.namamember,
						x1.notelp,
						x1.nopolisi,
						CAST(GETDATE() AS Date) AS tglreport,
						x2.namapanjang,
						x2.durabilitykm,
						x2.durabilityday,
						x2.namaJenisMember,
						x1.trx1_tanggal,
						x1.trx1_kodetoko,
						x1.trx1_nomortransaksi,
						x1.trx1_odometer,
						--x1.trx2_tanggal,
						CASE
							WHEN x1.trx2_tanggal IS NULL THEN x1.trx1_tanggal
							WHEN x1.trx2_tanggal < x1.trx1_tanggal THEN x1.trx1_tanggal
							ELSE x1.trx2_tanggal
						END AS trx2_tanggal,
						--x1.trx2_kodetoko,
						CASE
							WHEN x1.trx2_kodetoko IS NULL THEN x1.trx1_kodetoko
							WHEN x1.trx2_tanggal < x1.trx1_tanggal THEN x1.trx1_kodetoko
							ELSE x1.trx2_kodetoko
						END AS trx2_kodetoko,
						--x1.trx2_nomortransaksi,
						CASE
							WHEN x1.trx2_nomortransaksi IS NULL THEN x1.trx1_nomortransaksi
							WHEN x1.trx2_tanggal < x1.trx1_tanggal THEN x1.trx1_nomortransaksi
							ELSE x1.trx2_nomortransaksi
						END AS trx2_nomortransaksi,
						--x1.trx2_odometer,
						CASE
							WHEN x1.trx2_odometer IS NULL THEN x1.trx1_odometer
							WHEN x1.trx2_tanggal < x1.trx1_tanggal THEN x1.trx1_odometer
							ELSE x1.trx2_odometer
						END AS trx2_odometer,
						x1.trx3_tanggal,
						x1.trx3_kodetoko,
						x1.trx3_nomortransaksi,
						x1.trx3_odometer,
						--x1.trx4_tanggal,
						CASE
							WHEN x1.trx4_tanggal IS NULL THEN x1.trx3_tanggal
							WHEN x1.trx4_tanggal < x1.trx3_tanggal THEN x1.trx3_tanggal
							ELSE x1.trx4_tanggal
						END AS trx4_tanggal,
						--x1.trx4_kodetoko,
						CASE
							WHEN x1.trx4_kodetoko IS NULL THEN x1.trx3_kodetoko
							WHEN x1.trx4_tanggal < x1.trx3_tanggal THEN x1.trx3_kodetoko
							ELSE x1.trx4_kodetoko
						END AS trx4_kodetoko,
						--x1.trx4_nomortransaksi,
						CASE
							WHEN x1.trx4_nomortransaksi IS NULL THEN x1.trx3_nomortransaksi
							WHEN x1.trx4_tanggal < x1.trx3_tanggal THEN x1.trx3_nomortransaksi
							ELSE x1.trx4_nomortransaksi
						END AS trx4_nomortransaksi,
						--x1.trx4_odometer,
						CASE
							WHEN x1.trx4_odometer IS NULL THEN x1.trx3_odometer
							WHEN x1.trx4_tanggal < x1.trx3_tanggal THEN x1.trx3_odometer
							ELSE x1.trx4_odometer
						END AS trx4_odometer,
						x1.trx5_tanggal,
						x1.trx5_kodetoko,
						x1.trx5_nomortransaksi,
						x1.trx5_odometer--,
						--x1.resultquestioner
					FROM (
						SELECT 
							x.namacabang, 
							x.namamember,
							x.notelp,
							x.nopolisi,
							--CASE 
							--	WHEN x0.resultquestioner IS NULL OR x0.resultquestioner = '' THEN 0
							--	ELSE x0.resultquestioner
							--END AS resultquestioner,
							---------63.3 AS resultquestioner,
							MAX(COALESCE(CASE WHEN x.trx_number = 1 AND x.oli = 1 THEN x.tanggal END, '1900-01-01')) AS trx1_tanggal,
							MAX(COALESCE(CASE WHEN x.trx_number = 1 AND x.oli = 1 THEN x.kodetoko END, '')) AS trx1_kodetoko,
							MAX(COALESCE(CASE WHEN x.trx_number = 1 AND x.oli = 1 THEN x.nomortransaksi END, '')) AS trx1_nomortransaksi,
							MAX(COALESCE(CASE WHEN x.trx_number = 1 AND x.oli = 1 THEN x.odometer END, '')) AS trx1_odometer,
							MAX(COALESCE(CASE WHEN x.trx_number = 1 AND x.oli = 0 THEN x.tanggal END, '1900-01-01')) AS trx2_tanggal,
							MAX(COALESCE(CASE WHEN x.trx_number = 1 AND x.oli = 0 THEN x.kodetoko END, '')) AS trx2_kodetoko,
							MAX(COALESCE(CASE WHEN x.trx_number = 1 AND x.oli = 0 THEN x.nomortransaksi END, '')) AS trx2_nomortransaksi,
							MAX(COALESCE(CASE WHEN x.trx_number = 1 AND x.oli = 0 THEN x.odometer END, '')) AS trx2_odometer,
							MAX(COALESCE(CASE WHEN x.trx_number = 2 AND x.oli = 1 THEN x.tanggal END, '1900-01-01')) AS trx3_tanggal,
							MAX(COALESCE(CASE WHEN x.trx_number = 2 AND x.oli = 1 THEN x.kodetoko END, '')) AS trx3_kodetoko,
							MAX(COALESCE(CASE WHEN x.trx_number = 2 AND x.oli = 1 THEN x.nomortransaksi END, '')) AS trx3_nomortransaksi,
							MAX(COALESCE(CASE WHEN x.trx_number = 2 AND x.oli = 1 THEN x.odometer END, '')) AS trx3_odometer,
							MAX(COALESCE(CASE WHEN x.trx_number = 2 AND x.oli = 0 THEN x.tanggal END, '1900-01-01')) AS trx4_tanggal,
							MAX(COALESCE(CASE WHEN x.trx_number = 2 AND x.oli = 0 THEN x.kodetoko END, '')) AS trx4_kodetoko,
							MAX(COALESCE(CASE WHEN x.trx_number = 2 AND x.oli = 0 THEN x.nomortransaksi END, '')) AS trx4_nomortransaksi,
							MAX(COALESCE(CASE WHEN x.trx_number = 2 AND x.oli = 0 THEN x.odometer END, '')) AS trx4_odometer,
							MAX(COALESCE(CASE WHEN x.trx_number = 3  THEN x.tanggal END, '1900-01-01')) AS trx5_tanggal,
							MAX(COALESCE(CASE WHEN x.trx_number = 3  THEN x.kodetoko END, '')) AS trx5_kodetoko,
							MAX(COALESCE(CASE WHEN x.trx_number = 3  THEN x.nomortransaksi END, '')) AS trx5_nomortransaksi,
							MAX(COALESCE(CASE WHEN x.trx_number = 3  THEN x.odometer END, '')) AS trx5_odometer
						FROM 
							transactions x
						--JOIN questioner x0 ON x0.trans_nopol_all = x.nopolisi
						GROUP BY 
							x.namacabang, x.namamember, x.notelp, x.nopolisi--, x0.resultquestioner
					) AS x1
					JOIN 
						durability x2 ON x1.trx1_kodetoko = x2.kodetoko AND x1.trx1_nomortransaksi = x2.nomortransaksi
				)AS x2
			)AS y
		)AS y2
	)AS y3
)AS y4
WHERE y4.notelp IS NOT NULL AND notelp <> ''
--AND y4.kolom_c='B1743UOY'
;
end;
GO


--------------------------------------------------------------------------------------------------------------------------POSTGRESQL


-- mb_rms10_rpt.mb_trx_oil_goliaht_monitoring_jkt definition

-- Drop table

-- DROP FOREIGN TABLE mb_rms10_rpt.mb_trx_oil_goliaht_monitoring_jkt;

CREATE FOREIGN TABLE mb_rms10_rpt.mb_trx_oil_goliaht_monitoring_jkt (
	insert_date timestamp OPTIONS(column_name 'insert_date') NOT NULL,
	namacabang varchar(40) OPTIONS(column_name 'namacabang') NULL,
	notelp varchar(15) OPTIONS(column_name 'notelp') NULL,
	kolom_b varchar(30) OPTIONS(column_name 'kolom_b') NULL,
	kolom_c varchar(15) OPTIONS(column_name 'kolom_c') NULL,
	kolom_d date OPTIONS(column_name 'kolom_d') NULL,
	kolom_e varchar(100) OPTIONS(column_name 'kolom_e') NULL,
	kolom_f int4 OPTIONS(column_name 'kolom_f') NULL,
	kolom_g int4 OPTIONS(column_name 'kolom_g') NULL,
	kolom_h int4 OPTIONS(column_name 'kolom_h') NULL,
	kolom_i float8 OPTIONS(column_name 'kolom_i') NULL,
	kolom_j varchar(30) OPTIONS(column_name 'kolom_j') NULL,
	kolom_k float8 OPTIONS(column_name 'kolom_k') NULL,
	kolom_l float8 OPTIONS(column_name 'kolom_l') NULL,
	kolom_m int4 OPTIONS(column_name 'kolom_m') NULL,
	kolom_n date OPTIONS(column_name 'kolom_n') NULL,
	kolom_n1 int8 OPTIONS(column_name 'kolom_n1') NULL,
	kolom_n2 int8 OPTIONS(column_name 'kolom_n2') NULL,
	kolom_o int8 OPTIONS(column_name 'kolom_o') NULL,
	kolom_p date OPTIONS(column_name 'kolom_p') NULL,
	kolom_p1 int8 OPTIONS(column_name 'kolom_p1') NULL,
	kolom_p2 int8 OPTIONS(column_name 'kolom_p2') NULL,
	kolom_q int8 OPTIONS(column_name 'kolom_q') NULL,
	kolom_r date OPTIONS(column_name 'kolom_r') NULL,
	kolom_r1 int8 OPTIONS(column_name 'kolom_r1') NULL,
	kolom_r2 int8 OPTIONS(column_name 'kolom_r2') NULL,
	kolom_s int8 OPTIONS(column_name 'kolom_s') NULL,
	kolom_t date OPTIONS(column_name 'kolom_t') NULL,
	kolom_t1 int8 OPTIONS(column_name 'kolom_t1') NULL,
	kolom_t2 int8 OPTIONS(column_name 'kolom_t2') NULL,
	kolom_u int8 OPTIONS(column_name 'kolom_u') NULL,
	kolom_u1 date OPTIONS(column_name 'kolom_u1') NULL,
	kolom_u2 int8 OPTIONS(column_name 'kolom_u2') NULL,
	kolom_u3 int8 OPTIONS(column_name 'kolom_u3') NULL,
	kolom_u4 int8 OPTIONS(column_name 'kolom_u4') NULL,
	kolom_v int4 OPTIONS(column_name 'kolom_v') NULL,
	kolom_w float8 OPTIONS(column_name 'kolom_w') NULL,
	kolom_x float8 OPTIONS(column_name 'kolom_x') NULL,
	kolom_y varchar(30) OPTIONS(column_name 'kolom_y') NULL,
	kolom_z varchar(30) OPTIONS(column_name 'kolom_z') NULL,
	kolom_aa date OPTIONS(column_name 'kolom_aa') NULL,
	kolom_ab varchar(30) OPTIONS(column_name 'kolom_ab') NULL,
	kolom_ac varchar(30) OPTIONS(column_name 'kolom_ac') NULL
)
SERVER mobeng_rms10_rpt
OPTIONS (schema_name 'dbo', table_name 'mb_trx_oil_goliaht_monitoring');


select * from mb_rms10_rpt.mb_trx_oil_goliaht_monitoring_jkt;


--------------------------------------------------------------------------------------------------------------------------


-- mb_rms20_rpt.mb_trx_oil_goliaht_monitoring_sby definition

-- Drop table

-- DROP FOREIGN TABLE mb_rms20_rpt.mb_trx_oil_goliaht_monitoring_sby;

CREATE FOREIGN TABLE mb_rms20_rpt.mb_trx_oil_goliaht_monitoring_sby(
	insert_date timestamp OPTIONS(column_name 'insert_date') NOT NULL,
	namacabang varchar(40) OPTIONS(column_name 'namacabang') NULL,
	notelp varchar(15) OPTIONS(column_name 'notelp') NULL,
	kolom_b varchar(30) OPTIONS(column_name 'kolom_b') NULL,
	kolom_c varchar(15) OPTIONS(column_name 'kolom_c') NULL,
	kolom_d date OPTIONS(column_name 'kolom_d') NULL,
	kolom_e varchar(100) OPTIONS(column_name 'kolom_e') NULL,
	kolom_f int4 OPTIONS(column_name 'kolom_f') NULL,
	kolom_g int4 OPTIONS(column_name 'kolom_g') NULL,
	kolom_h int4 OPTIONS(column_name 'kolom_h') NULL,
	kolom_i float8 OPTIONS(column_name 'kolom_i') NULL,
	kolom_j varchar(30) OPTIONS(column_name 'kolom_j') NULL,
	kolom_k float8 OPTIONS(column_name 'kolom_k') NULL,
	kolom_l float8 OPTIONS(column_name 'kolom_l') NULL,
	kolom_m int4 OPTIONS(column_name 'kolom_m') NULL,
	kolom_n date OPTIONS(column_name 'kolom_n') NULL,
	kolom_n1 int8 OPTIONS(column_name 'kolom_n1') NULL,
	kolom_n2 int8 OPTIONS(column_name 'kolom_n2') NULL,
	kolom_o int8 OPTIONS(column_name 'kolom_o') NULL,
	kolom_p date OPTIONS(column_name 'kolom_p') NULL,
	kolom_p1 int8 OPTIONS(column_name 'kolom_p1') NULL,
	kolom_p2 int8 OPTIONS(column_name 'kolom_p2') NULL,
	kolom_q int8 OPTIONS(column_name 'kolom_q') NULL,
	kolom_r date OPTIONS(column_name 'kolom_r') NULL,
	kolom_r1 int8 OPTIONS(column_name 'kolom_r1') NULL,
	kolom_r2 int8 OPTIONS(column_name 'kolom_r2') NULL,
	kolom_s int8 OPTIONS(column_name 'kolom_s') NULL,
	kolom_t date OPTIONS(column_name 'kolom_t') NULL,
	kolom_t1 int8 OPTIONS(column_name 'kolom_t1') NULL,
	kolom_t2 int8 OPTIONS(column_name 'kolom_t2') NULL,
	kolom_u int8 OPTIONS(column_name 'kolom_u') NULL,
	kolom_u1 date OPTIONS(column_name 'kolom_u1') NULL,
	kolom_u2 int8 OPTIONS(column_name 'kolom_u2') NULL,
	kolom_u3 int8 OPTIONS(column_name 'kolom_u3') NULL,
	kolom_u4 int8 OPTIONS(column_name 'kolom_u4') NULL,
	kolom_v int4 OPTIONS(column_name 'kolom_v') NULL,
	kolom_w float8 OPTIONS(column_name 'kolom_w') NULL,
	kolom_x float8 OPTIONS(column_name 'kolom_x') NULL,
	kolom_y varchar(30) OPTIONS(column_name 'kolom_y') NULL,
	kolom_z varchar(30) OPTIONS(column_name 'kolom_z') NULL,
	kolom_aa date OPTIONS(column_name 'kolom_aa') NULL,
	kolom_ab varchar(30) OPTIONS(column_name 'kolom_ab') NULL,
	kolom_ac varchar(30) OPTIONS(column_name 'kolom_ac') NULL
)
)
SERVER mobeng_rms20_rpt
OPTIONS (schema_name 'dbo', table_name 'mb_trx_oil_goliaht_monitoring');


select * from mb_rms20_rpt.mb_trx_oil_goliaht_monitoring_sby;



--------------------------------------------------------------------------------------------------------------------------


-- public.mb_trx_oil_goliaht_his definition

-- Drop table

-- DROP TABLE public.mb_trx_oil_goliaht_his;

CREATE TABLE public.mb_trx_oil_goliaht_his (
	insert_date timestamp NULL,
	namacabang varchar(40) NOT NULL,
	notelp varchar(15) NOT NULL,
	kolom_b varchar(30) NOT NULL,
	kolom_c varchar(15) NOT NULL,
	kolom_d date NOT NULL,
	kolom_e varchar(100) NULL,
	kolom_f int4 NULL,
	kolom_g int4 NULL,
	kolom_h int4 NULL,
	kolom_i float8 NULL,
	kolom_j varchar(30) NULL,
	kolom_k float8 NULL,
	kolom_l int4 NULL,
	kolom_m int4 NULL,
	kolom_n date NULL,
	kolom_o int8 NULL,
	kolom_p date NULL,
	kolom_q int8 NULL,
	kolom_r date NULL,
	kolom_s int8 NULL,
	kolom_t date NULL,
	kolom_u int8 NULL,
	kolom_v int4 NULL,
	kolom_w float8 NULL,
	kolom_x float8 NULL,
	kolom_y varchar(30) NULL,
	kolom_z varchar(30) NULL,
	kolom_aa date NULL,
	kolom_ab varchar(30) NULL,
	kolom_ac varchar(30) NULL,
	kolom_ad text NULL,
	kolom_ae date NULL,
	kolom_af int4 NULL,
	kolom_ag text NULL,
	kolom_ah text NULL,
	kolom_ai text NULL,
	kolom_aj date NULL,
	kolom_ak int4 NULL,
	kolom_al text NULL,
	kolom_am text NULL,
	wa_status int4 DEFAULT 0 NULL,
	send_date timestamp NULL,
	xid varchar(150) NULL,
	CONSTRAINT mb_trx_oil_goliaht_his_pkey PRIMARY KEY (namacabang, notelp, kolom_b, kolom_c, kolom_d)
);


select * from public.mb_trx_oil_goliaht_his;


--------------------------------------------------------------------------------------------------------------------------


-- public.mb_trx_oil_goliaht_monitoring_all definition

-- Drop table

-- DROP TABLE public.mb_trx_oil_goliaht_monitoring_all;

CREATE TABLE public.mb_trx_oil_goliaht_monitoring_all (
	insert_date timestamp NOT NULL,
	namacabang varchar(40) NOT NULL,
	wa text NOT NULL,
	kolom_b varchar(30) NOT NULL,
	kolom_c varchar(15) NOT NULL,
	kolom_d date NOT NULL,
	kolom_e varchar(100) NULL,
	kolom_f int4 NULL,
	kolom_g int4 NULL,
	kolom_h int4 NULL,
	kolom_i float8 NULL,
	kolom_j varchar(30) NULL,
	kolom_k float8 NULL,
	kolom_l int4 NULL,
	kolom_m int4 NULL,
	kolom_nrpt text NULL,
	kolom_orpt text NULL,
	kolom_prpt text NULL,
	kolom_qrpt text NULL,
	kolom_rrpt int4 NULL,
	kolom_srpt int8 NULL,
	kolom_trpt int4 NULL,
	kolom_urpt int4 NULL,
	kolom_vrpt int4 NULL,
	kolom_n date NULL,
	kolom_o int8 NULL,
	kolom_p1 int8 NULL,
	kolom_p10 varchar(100) NULL,
	kolom_aarpt varchar(40) NULL,
	kolom_abrpt varchar(40) NULL,
	kolom_p date NULL,
	kolom_q int8 NULL,
	kolom_r date NULL,
	kolom_s int8 NULL,
	kolom_t1 int8 NULL,
	kolom_t10 varchar(100) NULL,
	kolom_airpt varchar(40) NULL,
	kolom_ajrpt varchar(40) NULL,
	kolom_t date NULL,
	kolom_u int8 NULL,
	kolom_u1 date NULL,
	kolom_u4 int8 NULL,
	kolom_u2 int8 NULL,
	kolom_u10 varchar(100) NULL,
	kolom_aqrpt varchar(40) NULL,
	kolom_arrpt varchar(40) NULL,
	kolom_v int4 NULL,
	kolom_w float8 NULL,
	kolom_x float8 NULL,
	kolom_y varchar(30) NULL,
	kolom_z varchar(30) NULL,
	kolom_aa date NULL,
	kolom_ab varchar(30) NULL,
	kolom_ac varchar(30) NULL,
	kolom_ad text NULL,
	kolom_ae date NULL,
	kolom_af int4 NULL,
	kolom_ag date NULL,
	kolom_ah text NULL,
	kolom_ai text NULL,
	kolom_aj date NULL,
	kolom_ak int4 NULL,
	kolom_al text NULL,
	kolom_am text NULL,
	CONSTRAINT mb_trx_oil_goliaht_monitoring_all_pkey PRIMARY KEY (namacabang, wa, kolom_b, kolom_c, kolom_d)
);
CREATE INDEX idx_mb1 ON public.mb_trx_oil_goliaht_monitoring_all USING btree (kolom_d);

select * from public.mb_trx_oil_goliaht_monitoring_all;


--------------------------------------------------------------------------------------------------------------------------


-- public.mb_alokasi_ac_rh definition

-- Drop table

-- DROP TABLE public.mb_alokasi_ac_rh;

CREATE TABLE public.mb_alokasi_ac_rh (
	kodetoko int4 NULL,
	nik_ac int4 NULL,
	ac varchar NULL,
	nik_rh int4 NULL,
	rh varchar NULL,
	tahun int4 NULL,
	bulan int4 NULL,
	periode date NULL,
	insertdate timestamp NULL,
	CONSTRAINT mb_alokasi_ac_rh_unique UNIQUE (kodetoko, nik_ac, tahun, bulan)
);

select * from public.mb_alokasi_ac_rh;

--------------------------------------------------------------------------------------------------------------------------


--INSERT Function to mb_trx_oil_goliaht_monitoring_all
insert into public.mb_trx_oil_goliaht_monitoring_all
select * from public.fc_smi_trx_oil_goliaht_monitoring_mobeng_jkt();

insert into public.mb_trx_oil_goliaht_monitoring_all
select * from public.fc_smi_trx_oil_goliaht_monitoring_mobeng_sby();

select * from public.mb_trx_oil_goliaht_monitoring_all;
select distinct namacabang from public.mb_trx_oil_goliaht_monitoring_all;
select distinct kolom_d from public.mb_trx_oil_goliaht_monitoring_all;


--------------------------------------------------------------------------------------------------------------------------


-- DROP FUNCTION public.fc_smi_trx_oil_goliaht_monitoring_mobeng_jkt();

CREATE OR REPLACE FUNCTION public.fc_smi_trx_oil_goliaht_monitoring_mobeng_jkt()
 RETURNS TABLE(insert_date timestamp without time zone, namacabang character varying, wa text, kb character varying, kc character varying, kd date, ke character varying, kf integer, kg integer, kh integer, ki double precision, kj character varying, kk double precision, kl double precision, km integer, kolom_nrpt text, kolom_orpt text, kolom_prpt text, kolom_qrpt text, kolom_rrpt integer, kolom_srpt bigint, kolom_trpt integer, kolom_urpt integer, kolom_vrpt integer, kolom_n date, kolom_o bigint, kolom_p1 bigint, kolom_p10 text, kolom_aarpt character varying, kolom_abrpt character varying, kolom_p date, kolom_q bigint, kolom_r date, kolom_s bigint, kolom_t1 bigint, kolom_t10 text, kolom_airpt character varying, kolom_ajrpt character varying, kolom_t date, kolom_u bigint, kolom_u1 date, kolom_u4 bigint, kolom_u2 bigint, kolom_u10 text, kolom_aqrpt character varying, kolom_arrpt character varying, kv integer, kw double precision, kx double precision, ky character varying, kz character varying, kaa date, kab character varying, kac character varying, kad text, kae date, kaf integer, kag date, kah text, kai text, kaj date, kak integer, kal text, kam text)
 LANGUAGE plpgsql
AS $function$
BEGIN
    RETURN QUERY
(
select
x9.insert_date,
x9.namacabang, 
--x9.wa, x9.kolom_b, x9.kolom_c, x9.kolom_d, x9.kolom_e, x9.kolom_f, x9.kolom_g, x9.kolom_h, x9.kolom_i, x9.kolom_j, x9.kolom_k, x9.kolom_l, x9.kolom_m, 
x9.wa, 
x9.kolom_b as kb, 
x9.kolom_c as kc, 
x9.kolom_d as kd, 
x9.kolom_e as ke, 
x9.kolom_f as kf, 
x9.kolom_g as kg, 
x9.kolom_h as kh, 
x9.kolom_i as ki, 
x9.kolom_j as kj, 
x9.kolom_k as kk, 
x9.kolom_l as kl, 
x9.kolom_m as km, 
x9.kolom_nrpt,
x9.kolom_orpt,
x9.kolom_prpt,
x9.kolom_qrpt,
x9.kolom_rrpt,
x9.kolom_srpt,
x9.kolom_trpt,
x9.kolom_urpt,
x9.kolom_vrpt,
x9.kolom_n,
x9.kolom_o,
x9.kolom_p1,
x9.kolom_p10,
x9.kolom_aarpt,
x9.kolom_abrpt,
x9.kolom_p,
x9.kolom_q,
x9.kolom_r,
x9.kolom_s,
x9.kolom_t1,
x9.kolom_t10,
x9.kolom_airpt,
x9.kolom_ajrpt,
x9.kolom_t,
x9.kolom_u,
x9.kolom_u1,
x9.kolom_u4,
x9.kolom_u2,
x9.kolom_u10,
x9.kolom_aqrpt,
x9.kolom_arrpt,
x9.kolom_v as kv, 
x9.kolom_w as kw, 
x9.kolom_x as kx, 
x9.kolom_y as ky, 
x9.kolom_z as kz, 
x9.kolom_aa as kaa, 
x9.kolom_ab as kab, 
x9.kolom_ac as kac, 
x9.kolom_ad as kad, 
x9.kolom_ae as kae, 
x9.kolom_af as kaf, 
x9.kolom_ag as kag, 
x9.kolom_ah1 as kah, 
x9.kolom_ai as kai, 
x9.kolom_aj as kaj, 
x9.kolom_ak as kak, 
x9.kolom_al as kal, 
x9.kolom_am as kam
from (
	select
	x5.insert_date,
	x5.namacabang, 
	x5.wa, x5.kolom_b, x5.kolom_c, x5.kolom_d, x5.kolom_e, x5.kolom_f, x5.kolom_g, x5.kolom_h, x5.kolom_i, x5.kolom_j, x5.kolom_k, x5.kolom_l, x5.kolom_m, 
	nrpt as kolom_nrpt,		--Status Keseluruhan (Suspect/Oke)
	orpt as kolom_orpt,		--Suspect Odometer Bergerak Terlalu Besar (Suspect/Oke)
	prpt as kolom_prpt,		--Suspect Odometer Bergerak Negatif (Suspect/Oke)
	qrpt as kolom_qrpt,		--Suspect Odometer Tidak Bergerak (Suspect/Oke)
	rrpt as kolom_rrpt,		--Batas Maksimal Pergerakan Odometer (KM)
	srpt as kolom_srpt,		--Pergerakan Odometer (KM) Berdasarkan 2 Transaksi Terakhir
	trpt as kolom_trpt,		--Jarak Hari Transaksi Yang Digunakan
	urpt as kolom_urpt,		--Jarak Hari Dari Transaksi Oli Terakhir ke Transaksi Oli Atau Non-Oli Kedua Dari Terakhir
	vrpt as kolom_vrpt,		--Jarak Hari Dari Transaksi Oli Terakhir ke Transaksi Oli Atau Non-Oli Terakhir
	x5.kolom_n,				--Tanggal Transaksi Oli Terakhir
	x5.kolom_o,				--Odometer Saat Transaksi Oli Terakhir
	'========' as "X",
	--x5.kolom_n1, x5.kolom_n10, x5.kolom_n2,  
	x5.kolom_p1,			--Store Code Transaksi Oli Atau Non-Oli Terakhir
	x5.kolom_p10,			--Nama Toko Transaksi Oli Atau Non-Oli Terakhir
	x6.rh as kolom_aarpt,			--Nama RH Transaksi Oli Atau Non-Oli Terakhir	
	x5.namacabang as kolom_abrpt,	--Cabang Toko Transaksi Oli Atau Non-Oli Terakhir
	x5.kolom_p,				--Tanggal Transaksi Oli Atau Non-Oli Terakhir
	x5.kolom_q,				--Odometer Saat Tanggal Transaksi Oli Atau Non-Oli Terakhir  
	--x5.kolom_p2,  
	'========' as "X",
	x5.kolom_r,				--Tanggal Saat Transaksi Oli Kedua Dari Terakhir
	x5.kolom_s, 			--Odometer Saat Transaksi Oli Kedua Dari Terakhir
	--x5.kolom_r1, x5.kolom_r10, --x5.kolom_r2, 
	'========' as "X",
	x5.kolom_t1,			--Store Code Transaksi Oli Atau Non-Oli Kedua Dari Terakhir
	x5.kolom_t10,			--Nama Toko Transaksi Oli Atau Non-Oli Kedua Dari Terakhir
	x7.rh as kolom_airpt,			--Nama RH Transaksi Oli Atau Non-Oli Terakhir	
--	x5.namacabang as kolom_ajrpt,	--Cabang Toko Transaksi Oli Atau Non-Oli Terakhir
	case when x5.kolom_t1=0 then null else x5.namacabang end as kolom_ajrpt,	--Cabang Toko Transaksi Oli Atau Non-Oli Terakhir
	x5.kolom_t,				--Tanggal Transaksi Oli Atau Non-Oli Kedua Dari Terakhir
	x5.kolom_u,  			--Odometer Saat Tanggal Transaksi Oli Atau Non-Oli Kedua Dari Terakhir
	--x5.kolom_t2, 
	'========' as "X",
	x5.kolom_u1, 			--Tanggal Transaksi Oli Atau Non-Oli Ketiga Dari Terakhir
	x5.kolom_u4,			--Odometer Saat Tanggal Transaksi Oli Atau Non-Oli Ketiga Dari Terakhir
	x5.kolom_u2, 			--Store Code Transaksi Oli Atau Non-Oli Ketiga Dari Terakhir
	x5.kolom_u10,			--Nama Toko Transaksi Oli Atau Non-Oli Ketiga Dari Terakhir
	x8.rh as kolom_aqrpt,			--Nama RH Transaksi Oli Atau Non-Oli Ketiga Dari Terakhir
--	x5.namacabang as kolom_arrpt,	--Cabang Toko Transaksi Oli Atau Non-Oli Ketiga Dari Terakhir
	case when x5.kolom_u2=0 then null else x5.namacabang end as kolom_arrpt,	--Cabang Toko Transaksi Oli Atau Non-Oli Ketiga Dari Terakhir
	  --x5.kolom_u3, 
	'========' as "X",
	x5.kolom_v, x5.kolom_w, x5.kolom_x, x5.kolom_y, x5.kolom_z, x5.kolom_aa, x5.kolom_ab, x5.kolom_ac, x5.kolom_ad, x5.kolom_ae, x5.kolom_af, x5.kolom_ag, 
	x5.kolom_ah1, x5.kolom_ai, x5.kolom_aj, x5.kolom_ak, x5.kolom_al, x5.kolom_am
	from (
		select
		x4.insert_date,
		x4.namacabang, x4.wa, x4.kolom_b, x4.kolom_c, x4.kolom_d, x4.kolom_e, x4.kolom_f, x4.kolom_g, x4.kolom_h, x4.kolom_i, x4.kolom_j, x4.kolom_k, 
		x4.kolom_l, x4.kolom_m, 
		CASE 
		    WHEN (CASE 
		    WHEN (CASE WHEN (x4.kolom_p - x4.kolom_n) > 0 THEN (x4.kolom_p - x4.kolom_n)
		    ELSE (CASE WHEN x4.kolom_t > '1900-01-01' THEN x4.kolom_n - x4.kolom_t ELSE 0 END) 
				END) > 0 THEN 
		        CASE 
		            WHEN (CASE WHEN (CASE WHEN (x4.kolom_p - x4.kolom_n) > 0 THEN (x4.kolom_p - x4.kolom_n)
					    ELSE (CASE WHEN x4.kolom_t > '1900-01-01' THEN x4.kolom_n - x4.kolom_t ELSE 0 END) 
						END) = 0 THEN 0 
					    ELSE 
					        CASE 
					            WHEN (CASE WHEN x4.kolom_t > '1900-01-01' THEN x4.kolom_n - x4.kolom_t 
								    ELSE 0 
									END) = (CASE WHEN (x4.kolom_p - x4.kolom_n) > 0 THEN (x4.kolom_p - x4.kolom_n)
										    ELSE (CASE WHEN x4.kolom_t > '1900-01-01' THEN x4.kolom_n - x4.kolom_t ELSE 0 END) 
										END) THEN x4.kolom_q - x4.kolom_u
						    ELSE x4.kolom_q - x4.kolom_o
						END 
					END) > ((CASE WHEN (x4.kolom_p - x4.kolom_n) > 0 THEN (x4.kolom_p - x4.kolom_n)
							    ELSE (CASE WHEN x4.kolom_t > '1900-01-01' THEN x4.kolom_n - x4.kolom_t ELSE 0 END) 
							END) * x4.kolom_v) THEN 'Suspect' 
		            ELSE 'Oke' 
		        END 
		    ELSE 'N/A' 
		END) = 'Suspect' OR (CASE 
		    WHEN (CASE WHEN (x4.kolom_p - x4.kolom_n) > 0 THEN (x4.kolom_p - x4.kolom_n)
		    ELSE (CASE WHEN x4.kolom_t > '1900-01-01' THEN x4.kolom_n - x4.kolom_t ELSE 0 END) 
				END) > 0 THEN 
		        CASE 
		            WHEN (CASE WHEN (CASE WHEN (x4.kolom_p - x4.kolom_n) > 0 THEN (x4.kolom_p - x4.kolom_n)
					    ELSE (CASE WHEN x4.kolom_t > '1900-01-01' THEN x4.kolom_n - x4.kolom_t ELSE 0 END) 
						END) = 0 THEN 0 
					    ELSE 
					        CASE 
					            WHEN (CASE WHEN x4.kolom_t > '1900-01-01' THEN x4.kolom_n - x4.kolom_t 
								    ELSE 0 
									END) = (CASE WHEN (x4.kolom_p - x4.kolom_n) > 0 THEN (x4.kolom_p - x4.kolom_n)
										    ELSE (CASE WHEN x4.kolom_t > '1900-01-01' THEN x4.kolom_n - x4.kolom_t ELSE 0 END) 
										END) THEN x4.kolom_q - x4.kolom_u
						    ELSE x4.kolom_q - x4.kolom_o
						END 
						END) < 0 THEN 'Suspect' 
		            ELSE 'Oke' 
		        END 
		    ELSE 'N/A' 
		END) = 'Suspect' OR (CASE 
		    WHEN (CASE WHEN (x4.kolom_p - x4.kolom_n) > 0 THEN (x4.kolom_p - x4.kolom_n)
		    ELSE (CASE WHEN x4.kolom_t > '1900-01-01' THEN x4.kolom_n - x4.kolom_t ELSE 0 END) 
				END) > 0 THEN 
		        CASE 
		            WHEN (CASE WHEN (CASE WHEN (x4.kolom_p - x4.kolom_n) > 0 THEN (x4.kolom_p - x4.kolom_n)
					    ELSE (CASE WHEN x4.kolom_t > '1900-01-01' THEN x4.kolom_n - x4.kolom_t ELSE 0 END) 
						END) = 0 THEN 0 
					    ELSE 
					        CASE 
					            WHEN (CASE WHEN x4.kolom_t > '1900-01-01' THEN x4.kolom_n - x4.kolom_t 
								    ELSE 0 
									END) = (CASE WHEN (x4.kolom_p - x4.kolom_n) > 0 THEN (x4.kolom_p - x4.kolom_n)
										    ELSE (CASE WHEN x4.kolom_t > '1900-01-01' THEN x4.kolom_n - x4.kolom_t ELSE 0 END) 
										END) THEN x4.kolom_q - x4.kolom_u
						    ELSE x4.kolom_q - x4.kolom_o
						END 
					END) = 0 THEN 'Suspect' 
		            ELSE 'Oke' 
		        END 
		    ELSE 'N/A' 
		END) = 'Suspect' THEN 'Suspect' ----------- 
					    WHEN (CASE 
		    WHEN (CASE WHEN (x4.kolom_p - x4.kolom_n) > 0 THEN (x4.kolom_p - x4.kolom_n)
		    ELSE (CASE WHEN x4.kolom_t > '1900-01-01' THEN x4.kolom_n - x4.kolom_t ELSE 0 END) 
				END) > 0 THEN 
		        CASE 
		            WHEN (CASE WHEN (CASE WHEN (x4.kolom_p - x4.kolom_n) > 0 THEN (x4.kolom_p - x4.kolom_n)
					    ELSE (CASE WHEN x4.kolom_t > '1900-01-01' THEN x4.kolom_n - x4.kolom_t ELSE 0 END) 
						END) = 0 THEN 0 
					    ELSE 
					        CASE 
					            WHEN (CASE WHEN x4.kolom_t > '1900-01-01' THEN x4.kolom_n - x4.kolom_t 
								    ELSE 0 
									END) = (CASE WHEN (x4.kolom_p - x4.kolom_n) > 0 THEN (x4.kolom_p - x4.kolom_n)
										    ELSE (CASE WHEN x4.kolom_t > '1900-01-01' THEN x4.kolom_n - x4.kolom_t ELSE 0 END) 
										END) THEN x4.kolom_q - x4.kolom_u
						    ELSE x4.kolom_q - x4.kolom_o
						END 
					END) > ((CASE WHEN (x4.kolom_p - x4.kolom_n) > 0 THEN (x4.kolom_p - x4.kolom_n)
							    ELSE (CASE WHEN x4.kolom_t > '1900-01-01' THEN x4.kolom_n - x4.kolom_t ELSE 0 END) 
							END) * x4.kolom_v) THEN 'Suspect' 
		            ELSE 'Oke' 
		        END 
		    ELSE 'N/A' -----------
		END) = 'Oke' OR (CASE 
		    WHEN (CASE WHEN (x4.kolom_p - x4.kolom_n) > 0 THEN (x4.kolom_p - x4.kolom_n)
		    ELSE (CASE WHEN x4.kolom_t > '1900-01-01' THEN x4.kolom_n - x4.kolom_t ELSE 0 END) 
				END) > 0 THEN 
		        CASE 
		            WHEN (CASE WHEN (CASE WHEN (x4.kolom_p - x4.kolom_n) > 0 THEN (x4.kolom_p - x4.kolom_n)
					    ELSE (CASE WHEN x4.kolom_t > '1900-01-01' THEN x4.kolom_n - x4.kolom_t ELSE 0 END) 
						END) = 0 THEN 0 
					    ELSE 
					        CASE 
					            WHEN (CASE WHEN x4.kolom_t > '1900-01-01' THEN x4.kolom_n - x4.kolom_t 
								    ELSE 0 
									END) = (CASE WHEN (x4.kolom_p - x4.kolom_n) > 0 THEN (x4.kolom_p - x4.kolom_n)
										    ELSE (CASE WHEN x4.kolom_t > '1900-01-01' THEN x4.kolom_n - x4.kolom_t ELSE 0 END) 
										END) THEN x4.kolom_q - x4.kolom_u
						    ELSE x4.kolom_q - x4.kolom_o
						END 
						END) < 0 THEN 'Suspect' 
		            ELSE 'Oke' 
		        END 
		    ELSE 'N/A'  -----------
		END) = 'Oke' OR (CASE 
		    WHEN (CASE WHEN (x4.kolom_p - x4.kolom_n) > 0 THEN (x4.kolom_p - x4.kolom_n)
		    ELSE (CASE WHEN x4.kolom_t > '1900-01-01' THEN x4.kolom_n - x4.kolom_t ELSE 0 END) 
				END) > 0 THEN 
		        CASE 
		            WHEN (CASE WHEN (CASE WHEN (x4.kolom_p - x4.kolom_n) > 0 THEN (x4.kolom_p - x4.kolom_n)
					    ELSE (CASE WHEN x4.kolom_t > '1900-01-01' THEN x4.kolom_n - x4.kolom_t ELSE 0 END) 
						END) = 0 THEN 0 
					    ELSE 
					        CASE 
					            WHEN (CASE WHEN x4.kolom_t > '1900-01-01' THEN x4.kolom_n - x4.kolom_t 
								    ELSE 0 
									END) = (CASE WHEN (x4.kolom_p - x4.kolom_n) > 0 THEN (x4.kolom_p - x4.kolom_n)
										    ELSE (CASE WHEN x4.kolom_t > '1900-01-01' THEN x4.kolom_n - x4.kolom_t ELSE 0 END) 
										END) THEN x4.kolom_q - x4.kolom_u
						    ELSE x4.kolom_q - x4.kolom_o
						END 
					END) = 0 THEN 'Suspect' 
		            ELSE 'Oke' 
		        END 
		    ELSE 'N/A' 
		END) = 'Oke' THEN 'Oke' 
		    ELSE 'N/A' 
		END AS nrpt,	--Status Keseluruhan (Suspect/Oke)
		CASE 
		    WHEN (CASE WHEN (x4.kolom_p - x4.kolom_n) > 0 THEN (x4.kolom_p - x4.kolom_n)
		    ELSE (CASE WHEN x4.kolom_t > '1900-01-01' THEN x4.kolom_n - x4.kolom_t ELSE 0 END) 
				END) > 0 THEN 
		        CASE 
		            WHEN (CASE WHEN (CASE WHEN (x4.kolom_p - x4.kolom_n) > 0 THEN (x4.kolom_p - x4.kolom_n)
					    ELSE (CASE WHEN x4.kolom_t > '1900-01-01' THEN x4.kolom_n - x4.kolom_t ELSE 0 END) 
						END) = 0 THEN 0 
					    ELSE 
					        CASE 
					            WHEN (CASE WHEN x4.kolom_t > '1900-01-01' THEN x4.kolom_n - x4.kolom_t 
								    ELSE 0 
									END) = (CASE WHEN (x4.kolom_p - x4.kolom_n) > 0 THEN (x4.kolom_p - x4.kolom_n)
										    ELSE (CASE WHEN x4.kolom_t > '1900-01-01' THEN x4.kolom_n - x4.kolom_t ELSE 0 END) 
										END) THEN x4.kolom_q - x4.kolom_u
						    ELSE x4.kolom_q - x4.kolom_o
						END 
					END) > ((CASE WHEN (x4.kolom_p - x4.kolom_n) > 0 THEN (x4.kolom_p - x4.kolom_n)
							    ELSE (CASE WHEN x4.kolom_t > '1900-01-01' THEN x4.kolom_n - x4.kolom_t ELSE 0 END) 
							END) * x4.kolom_v) THEN 'Suspect' 
		            ELSE 'Oke' 
		        END 
		    ELSE 'N/A' 
		END AS orpt,	--Suspect Odometer Bergerak Terlalu Besar (Suspect/Oke)	-------//Perbaikan 20241125
		CASE 
		    WHEN (CASE WHEN (x4.kolom_p - x4.kolom_n) > 0 THEN (x4.kolom_p - x4.kolom_n)
		    ELSE (CASE WHEN x4.kolom_t > '1900-01-01' THEN x4.kolom_n - x4.kolom_t ELSE 0 END) 
				END) > 0 THEN 
		        CASE 
		            WHEN (CASE WHEN (CASE WHEN (x4.kolom_p - x4.kolom_n) > 0 THEN (x4.kolom_p - x4.kolom_n)
					    ELSE (CASE WHEN x4.kolom_t > '1900-01-01' THEN x4.kolom_n - x4.kolom_t ELSE 0 END) 
						END) = 0 THEN 0 
					    ELSE 
					        CASE 
					            WHEN (CASE WHEN x4.kolom_t > '1900-01-01' THEN x4.kolom_n - x4.kolom_t 
								    ELSE 0 
									END) = (CASE WHEN (x4.kolom_p - x4.kolom_n) > 0 THEN (x4.kolom_p - x4.kolom_n)
										    ELSE (CASE WHEN x4.kolom_t > '1900-01-01' THEN x4.kolom_n - x4.kolom_t ELSE 0 END) 
										END) THEN x4.kolom_q - x4.kolom_u
						    ELSE x4.kolom_q - x4.kolom_o
						END 
						END) < 0 THEN 'Suspect' 
		            ELSE 'Oke' 
		        END 
		    ELSE 'N/A' 
		END AS prpt,	--Suspect Odometer Bergerak Negatif (Suspect/Oke)	-------//Perbaikan 20241125
		CASE 
		    WHEN (CASE WHEN (x4.kolom_p - x4.kolom_n) > 0 THEN (x4.kolom_p - x4.kolom_n)
		    ELSE (CASE WHEN x4.kolom_t > '1900-01-01' THEN x4.kolom_n - x4.kolom_t ELSE 0 END) 
				END) > 0 THEN 
		        CASE 
		            WHEN (CASE WHEN (CASE WHEN (x4.kolom_p - x4.kolom_n) > 0 THEN (x4.kolom_p - x4.kolom_n)
					    ELSE (CASE WHEN x4.kolom_t > '1900-01-01' THEN x4.kolom_n - x4.kolom_t ELSE 0 END) 
						END) = 0 THEN 0 
					    ELSE 
					        CASE 
					            WHEN (CASE WHEN x4.kolom_t > '1900-01-01' THEN x4.kolom_n - x4.kolom_t 
								    ELSE 0 
									END) = (CASE WHEN (x4.kolom_p - x4.kolom_n) > 0 THEN (x4.kolom_p - x4.kolom_n)
										    ELSE (CASE WHEN x4.kolom_t > '1900-01-01' THEN x4.kolom_n - x4.kolom_t ELSE 0 END) 
										END) THEN x4.kolom_q - x4.kolom_u
						    ELSE x4.kolom_q - x4.kolom_o
						END 
					END) = 0 THEN 'Suspect' 
		            ELSE 'Oke' 
		        END 
		    ELSE 'N/A' 
		END AS qrpt,	--Suspect Odometer Tidak Bergerak (Suspect/Oke)	-------//Perbaikan 20241125
		(CASE 
			WHEN (x4.kolom_p - x4.kolom_n) > 0 THEN (x4.kolom_p - x4.kolom_n)
			--(CASE WHEN x4.kolom_t > '1900-01-01' THEN x4.kolom_n - x4.kolom_t ELSE 0 END)
		    ELSE (CASE WHEN x4.kolom_t > '1900-01-01' THEN x4.kolom_n - x4.kolom_t ELSE 0 END) 
		END) * x4.kolom_v as rrpt,	--Batas Maksimal Pergerakan Odometer (KM)	-------//Perbaikan 20241125
		CASE 
		    WHEN (CASE WHEN (x4.kolom_p - x4.kolom_n) > 0 THEN (x4.kolom_p - x4.kolom_n)
			    ELSE (CASE WHEN x4.kolom_t > '1900-01-01' THEN x4.kolom_n - x4.kolom_t ELSE 0 END) 
			END) = 0 THEN 0 
		    ELSE 
		        CASE 
		            WHEN (CASE WHEN x4.kolom_t > '1900-01-01' THEN x4.kolom_n - x4.kolom_t 
					    ELSE 0 
						END) = (CASE WHEN (x4.kolom_p - x4.kolom_n) > 0 THEN (x4.kolom_p - x4.kolom_n)
							    ELSE (CASE WHEN x4.kolom_t > '1900-01-01' THEN x4.kolom_n - x4.kolom_t ELSE 0 END) 
							END) THEN x4.kolom_q - x4.kolom_u
			    ELSE x4.kolom_q - x4.kolom_o
			END 
		END AS srpt,	--Pergerakan Odometer (KM) Berdasarkan 2 Transaksi Terakhir	-------//Perbaikan 20241125
		CASE 
			WHEN (x4.kolom_p - x4.kolom_n) > 0 THEN (x4.kolom_p - x4.kolom_n)
			--(CASE WHEN x4.kolom_t > '1900-01-01' THEN x4.kolom_n - x4.kolom_t ELSE 0 END)
		    ELSE (CASE WHEN x4.kolom_t > '1900-01-01' THEN x4.kolom_n - x4.kolom_t ELSE 0 END) 
		END AS trpt,	--Jarak Hari Transaksi Yang Digunakan	-------//Perbaikan 20241125-OK
		CASE 
		    WHEN x4.kolom_t > '1900-01-01' THEN x4.kolom_n - x4.kolom_t 
		    ELSE 0 
		END AS urpt,	--Jarak Hari Dari Transaksi Oli Terakhir ke Transaksi Oli Atau Non-Oli Kedua Dari Terakhir
		x4.kolom_p - x4.kolom_n as vrpt,	--Jarak Hari Dari Transaksi Oli Terakhir ke Transaksi Oli Atau Non-Oli Terakhir -------//Perbaikan 20241125-OK
		x4.kolom_n, x4.kolom_n1, x4.kolom_n10, x4.kolom_n2, x4.kolom_o, 
		x4.kolom_p, x4.kolom_p1, x4.kolom_p10, x4.kolom_p2, x4.kolom_q, 
		x4.kolom_r, x4.kolom_r1, x4.kolom_r10, x4.kolom_r2, x4.kolom_s, 
		x4.kolom_t, x4.kolom_t1, x4.kolom_t10, x4.kolom_t2, x4.kolom_u, 
		x4.kolom_u1, x4.kolom_u2, x4.kolom_u10, x4.kolom_u3, x4.kolom_u4,
		x4.kolom_v, x4.kolom_w, 
		x4.kolom_x, x4.kolom_y, x4.kolom_z, x4.kolom_aa, x4.kolom_ab, x4.kolom_ac, x4.kolom_ad, x4.kolom_ae, x4.kolom_af, x4.kolom_ag, x4.kolom_ah1, 
		x4.kolom_ai, x4.kolom_aj, x4.kolom_ak, x4.kolom_al, x4.kolom_am
		from ( 
			select
			x3.insert_date,
			x3.namacabang, x3.wa, x3.kolom_b, x3.kolom_c, x3.kolom_d, x3.kolom_e, x3.kolom_f, x3.kolom_g, x3.kolom_h, x3.kolom_i, x3.kolom_j, x3.kolom_k, 
			x3.kolom_l, x3.kolom_m, 
			x3.kolom_n, x3.kolom_n1, x3.kolom_n10, x3.kolom_n2, x3.kolom_o, 
			x3.kolom_p, x3.kolom_p1, x3.kolom_p10, x3.kolom_p2, x3.kolom_q, 
			x3.kolom_r, x3.kolom_r1, x3.kolom_r10, x3.kolom_r2, x3.kolom_s, 
			x3.kolom_t, x3.kolom_t1, x3.kolom_t10, x3.kolom_t2, x3.kolom_u, 
			x3.kolom_u1, x3.kolom_u2, x3.kolom_u10, x3.kolom_u3, x3.kolom_u4,
			x3.kolom_v, x3.kolom_w, 
			x3.kolom_x, x3.kolom_y, x3.kolom_z, x3.kolom_aa, x3.kolom_ab, x3.kolom_ac, x3.kolom_ad, x3.kolom_ae, x3.kolom_af, x3.kolom_ag, x3.kolom_ah1, 
			x3.kolom_ai, x3.kolom_aj, x3.kolom_ak, x3.kolom_al, x3.kolom_am
			from (
			select
				x2.insert_date,
				x2.namacabang, '62' || SUBSTRING(x2.notelp FROM 2) AS wa, x2.kolom_b, x2.kolom_c, x2.kolom_d, x2.kolom_e, x2.kolom_f, x2.kolom_g, x2.kolom_h, x2.kolom_i, x2.kolom_j, x2.kolom_k, 
				x2.kolom_l, x2.kolom_m, 
				x2.kolom_n, x2.kolom_n1, x2.kolom_n10, x2.kolom_n2, x2.kolom_o, 
				x2.kolom_p, x2.kolom_p1, x2.kolom_p10, x2.kolom_p2, x2.kolom_q, 
				x2.kolom_r, x2.kolom_r1, x2.kolom_r10, x2.kolom_r2, x2.kolom_s, 
				x2.kolom_t, x2.kolom_t1, x2.kolom_t10, x2.kolom_t2, x2.kolom_u, 
				x2.kolom_u1, x2.kolom_u2, x2.kolom_u10, x2.kolom_u3, x2.kolom_u4,
				x2.kolom_v, x2.kolom_w, 
				x2.kolom_x, x2.kolom_y, x2.kolom_z, x2.kolom_aa, x2.kolom_ab, x2.kolom_ac, x2.kolom_ad, x2.kolom_ae, x2.kolom_af, x2.kolom_ag, x2.kolom_ah1, 
				--x2.kolom_ai,
				CASE
				    -- Jika kolom_ah = 0, status adalah 'Reminded'
				    WHEN COALESCE(
				            CASE 
				                WHEN x2.kolom_ah1 ~ '^\d+$' THEN x2.kolom_ah1::integer
				                ELSE NULL
				            END, 
				            -1
				         ) = 0 THEN 'Reminded'
				    -- Mengecek jika kolom_ac valid sebagai integer dan lebih kecil dari 1, serta kolom_ae < kolom_aa
				    WHEN COALESCE(
				            CASE 
				                WHEN x2.kolom_ah1 ~ '^\d+$' THEN x2.kolom_ah1::integer
				                ELSE NULL
				            END, 
				            -1
				         ) < 1
				         AND COALESCE(
				                 NULLIF(x2.kolom_aj, null)::date, 
				                 '1900-01-01'::date
				             ) < x2.kolom_ag::date THEN 'To Remind'
				    -- Mengecek jika kolom_ae >= kolom_aa dan kolom_aa > '1900-01-01'
				    WHEN COALESCE(
				            NULLIF(x2.kolom_aj, null)::date, 
				            '1900-01-01'::date
				         ) >= x2.kolom_ag::date
				         AND x2.kolom_ag::date > '1900-01-01'::date THEN 'Reminded'
				    ELSE 'Do Not Remind Yet'
				END AS kolom_ai,
			--	x2.kolom_aj, 
				CASE 
					WHEN x2.kolom_ah1 = 'N/A' THEN NULL
					WHEN CAST(x2.kolom_ah1 AS INTEGER) < 0 THEN x2.kolom_aj
				    WHEN CAST(x2.kolom_ah1 AS INTEGER) > 0 THEN NULL
				    WHEN CAST(x2.kolom_ah1 AS INTEGER) = 0 THEN x2.kolom_ag
				END AS kolom_aj,
				x2.kolom_ak,
				CASE 
				    WHEN x2.kolom_ah1 ~ '^\d+$' THEN
				        COALESCE(
				            ((x2.kolom_ah1::integer) + x2.kolom_ak)::text,
				            'N/A'
				        )
				    ELSE 'N/A'
					END AS kolom_al,
			    CASE
			        WHEN (CASE
			            WHEN CASE 
					    WHEN x2.kolom_ah1 ~ '^\d+$' THEN
					        COALESCE(
					            ((kolom_ah1::integer) + x2.kolom_ak)::text,
					            'N/A'
					        )
					    ELSE 'N/A'
					END ~ '^\d+$' THEN (CASE 
					    WHEN x2.kolom_ah1 ~ '^\d+$' THEN
					        COALESCE(
					            ((x2.kolom_ah1::integer) + x2.kolom_ak)::text,
					            'N/A'
					        )
					    ELSE 'N/A'
					END::integer) + x2.kolom_ak
					            ELSE NULL
					        END) < 0 THEN 'Masuk'
				    ELSE 'Tidak Masuk'
				END AS kolom_am
				FROM(
					select
					x1.insert_date,
					x1.namacabang, x1.notelp, x1.kolom_b, x1.kolom_c, x1.kolom_d, x1.kolom_e, x1.kolom_f, x1.kolom_g, x1.kolom_h, x1.kolom_i, x1.kolom_j, x1.kolom_k, x1.kolom_l, x1.kolom_m, 
					x1.kolom_n, x1.kolom_n1, x1.kolom_n10, x1.kolom_n2, x1.kolom_o, 
					x1.kolom_p, x1.kolom_p1, x1.kolom_p10, x1.kolom_p2, x1.kolom_q, 
					x1.kolom_r, x1.kolom_r1, x1.kolom_r10, x1.kolom_r2, x1.kolom_s, 
					x1.kolom_t, x1.kolom_t1, x1.kolom_t10, x1.kolom_t2, x1.kolom_u, 
					x1.kolom_u1, x1.kolom_u2, x1.kolom_u10, x1.kolom_u3, x1.kolom_u4,			
					x1.kolom_v, x1.kolom_w, x1.kolom_x, x1.kolom_y, x1.kolom_z, x1.kolom_aa, x1.kolom_ab, x1.kolom_ac, x1.kolom_ad, 
					--x1.kolom_ae, 
					CASE 
						WHEN x1.kolom_ac = 'N/A' THEN NULL
						WHEN CAST(x1.kolom_ac AS INTEGER) < 0 THEN x1.kolom_ae
				        WHEN CAST(x1.kolom_ac AS INTEGER) > 0 THEN NULL
				        WHEN CAST(x1.kolom_ac AS INTEGER) = 0 THEN x1.kolom_aa
					END AS kolom_ae,
					x1.kolom_af, 
					--x1.kolom_ag, 
					((CASE 
						WHEN x1.kolom_ac = 'N/A' THEN NULL
					    WHEN CAST(x1.kolom_ac AS INTEGER) < 0 THEN x1.kolom_ae
				        WHEN CAST(x1.kolom_ac AS INTEGER) > 0 THEN NULL
				        WHEN CAST(x1.kolom_ac AS INTEGER) = 0 THEN x1.kolom_aa
					end) + x1.kolom_af) as kolom_ag,
					--x1.kolom_ah,
					CASE 
					    WHEN ((CASE 
						WHEN x1.kolom_ac = 'N/A' THEN NULL
					    WHEN CAST(x1.kolom_ac AS INTEGER) < 0 THEN x1.kolom_ae
				        WHEN CAST(x1.kolom_ac AS INTEGER) > 0 THEN NULL
				        WHEN CAST(x1.kolom_ac AS INTEGER) = 0 THEN x1.kolom_aa
					end) + x1.kolom_af) IS NOT NULL AND x1.kolom_d IS NOT NULL THEN (((CASE 
					    WHEN CAST(x1.kolom_ac AS INTEGER) < 0 THEN x1.kolom_ae
				        WHEN CAST(x1.kolom_ac AS INTEGER) > 0 THEN NULL
				        WHEN CAST(x1.kolom_ac AS INTEGER) = 0 THEN x1.kolom_aa
					end) + x1.kolom_af) - x1.kolom_d)::text 
					    ELSE 'N/A' 
					end as kolom_ah1,
					x1.kolom_aj, 
					x1.kolom_ak
					FROM(
						SELECT 
						x.insert_date,
						x.namacabang, x.notelp, x.kolom_b, x.kolom_c, x.kolom_d, x.kolom_e, x.kolom_f, x.kolom_g, x.kolom_h, x.kolom_i, x.kolom_j, x.kolom_k, x.kolom_l, x.kolom_m, 
						x.kolom_n, x.kolom_n1, x.kolom_n10, x.kolom_n2, x.kolom_o, 
						x.kolom_p, x.kolom_p1, x.kolom_p10, x.kolom_p2, x.kolom_q, 
						x.kolom_r, x.kolom_r1, x.kolom_r10, x.kolom_r2, x.kolom_s, 
						x.kolom_t, x.kolom_t1, x.kolom_t10, x.kolom_t2, x.kolom_u, 
						x.kolom_u1, x.kolom_u2, x.kolom_u10, x.kolom_u3, x.kolom_u4, 
						x.kolom_v, x.kolom_w, x.kolom_x, x.kolom_y, x.kolom_z, x.kolom_aa, x.kolom_ab, x.kolom_ac,
						CASE
						    -- Jika kolom_ac = 0, status adalah 'Reminded'
						    WHEN COALESCE(
						            CASE 
						                WHEN x.kolom_ac ~ '^\d+$' THEN x.kolom_ac::integer
						                ELSE NULL
						            END, 
						            -1
						         ) = 0 THEN 'Reminded'
						    -- Mengecek jika kolom_ac valid sebagai integer dan lebih kecil dari 1, serta kolom_ae < kolom_aa
						    WHEN COALESCE(
						            CASE 
						                WHEN x.kolom_ac ~ '^\d+$' THEN x.kolom_ac::integer
						                ELSE NULL
						            END, 
						            -1
						         ) < 1
						         AND COALESCE(
						                 NULLIF(x.kolom_ae, null)::date,
						                 '1900-01-01'::date
						             ) < x.kolom_aa THEN 'To Remind'
						    -- Mengecek jika kolom_ae >= kolom_aa dan kolom_aa > '1900-01-01'
						    WHEN COALESCE(
						            NULLIF(x.kolom_ae, null)::date,
						            '1900-01-01'::date
						         ) >= x.kolom_aa
						         AND x.kolom_aa > '1900-01-01'::date THEN 'Reminded'
						    ELSE 'Do Not Remind Yet'
						END AS kolom_ad,
						x.kolom_ae,
						x.kolom_af,
						CASE
						    WHEN x.kolom_af > 0 THEN (x.kolom_ae + INTERVAL '1 day' * x.kolom_af)::date::text
						    ELSE 'N/A'
						END AS kolom_ag,
						x.kolom_ak,
						x.kolom_aj,
						x.string_length,--update 3-mar-2025
						x.first_character,--update 3-mar-2025
						x.second_character,--update 3-mar-2025
						x.third_character--update 3-mar-2025
						FROM(
							SELECT 
								a.insert_date,
								a.namacabang, a.notelp, a.kolom_b, a.kolom_c, a.kolom_d, a.kolom_e, a.kolom_f, a.kolom_g, a.kolom_h, a.kolom_i, a.kolom_j, a.kolom_k, a.kolom_l, a.kolom_m, 
								a.kolom_n, a.kolom_n1, c.namatoko as kolom_n10, a.kolom_n2, a.kolom_o, 
								a.kolom_p, a.kolom_p1, d.namatoko as kolom_p10, a.kolom_p2, a.kolom_q, 
								a.kolom_r, a.kolom_r1, e.namatoko as kolom_r10, a.kolom_r2, a.kolom_s,
								a.kolom_t, a.kolom_t1, f.namatoko as kolom_t10, a.kolom_t2, a.kolom_u,
								a.kolom_u1, a.kolom_u2, g.namatoko as kolom_u10, a.kolom_u3, a.kolom_u4,
								a.kolom_v, a.kolom_w, a.kolom_x, a.kolom_y, a.kolom_z, a.kolom_aa, a.kolom_ab, a.kolom_ac, b.aehis as kolom_ae,
								30 kolom_af, --hardcode senin 20240902
								30 kolom_ak, --hardcode senin 20240902
								b.ajhis as kolom_aj
								,coalesce(LENGTH(a.notelp),'2') AS string_length--update 3-mar-2025
								,coalesce(LEFT(a.notelp, 1),'1') AS first_character--update 3-mar-2025
								,coalesce(SUBSTRING(a.notelp FROM 2 FOR 1),'1') AS second_character--update 3-mar-2025
								,coalesce(SUBSTRING(a.notelp FROM 3 FOR 1),'1') AS third_character--update 3-mar-2025
							FROM mb_rms10_rpt.mb_trx_oil_goliaht_monitoring_jkt a
							LEFT JOIN (
								select 
								x1.cabhis, 
								x1.chis, 
								x1.aehis, 
								x1.ajhis,
								my_rank
								from (
									SELECT 
									x0.namacabang as cabhis, 
									x0.kolom_c as chis, 
									x0.kolom_ae as aehis, 
									x0.kolom_aj as ajhis, 
--									RANK() OVER (ORDER BY kolom_d DESC) AS my_rank
									RANK() OVER (PARTITION BY x0.kolom_c ORDER BY x0.kolom_d DESC) AS my_rank
									from public.mb_trx_oil_goliaht_his x0
									)as x1 where x1.my_rank=1
							) b on a.kolom_c = b.chis AND a.namacabang = b.cabhis
--							LEFT JOIN public.smi_trx_oil_goliaht_his b on a.kolom_c = b.kolom_c
							LEFT JOIN mb_rms01_mbho.mv_msttokoho_rms c on c.kodetoko = a.kolom_n1
							LEFT JOIN mb_rms01_mbho.mv_msttokoho_rms d on d.kodetoko = a.kolom_p1
							LEFT JOIN mb_rms01_mbho.mv_msttokoho_rms e on e.kodetoko = a.kolom_r1
							LEFT JOIN mb_rms01_mbho.mv_msttokoho_rms f on f.kodetoko = a.kolom_t1
							LEFT JOIN mb_rms01_mbho.mv_msttokoho_rms g on g.kodetoko = a.kolom_u2
							WHERE a.notelp IN (
												SELECT notelp 
												FROM mb_rms10_rpt.mb_trx_oil_goliaht_monitoring_jkt
												GROUP BY notelp 
												HAVING COUNT(*) = 1
												)
						)as x
						WHERE x.string_length IN ('10','11','12','13','14')--update 3-mar-2025
						AND x.first_character = '0'--update 3-mar-2025
						AND x.second_character = '8'--update 3-mar-2025
						AND x.third_character IN ('1','2','3','5','6','7','8','9')--update 3-mar-2025
--						AND x.kolom_ac='0'
					)as x1
				)as x2
--				WHERE x2.kolom_ac='0'
--				WHERE x2.kolom_ah1='0'
--				WHERE x2.kolom_c in ('B2043KFR')
--				where x2.kolom_c in ('A2876MN','F4818WAT','D3683XGK','DK4688LS','A2678EW','H4149ACW')
				)as x3
			)as x4
		)as x5
		LEFT JOIN public.mb_alokasi_ac_rh x6 ON x6.kodetoko = x5.kolom_p1
--			AND x6.periode >= DATE_TRUNC('month', CURRENT_DATE) 
--			AND x6.periode < DATE_TRUNC('month', CURRENT_DATE) + INTERVAL '1 month'
		LEFT JOIN public.mb_alokasi_ac_rh x7 ON x7.kodetoko = x5.kolom_t1 
			AND x5.kolom_t1 <> 0
--			AND x7.periode >= DATE_TRUNC('month', CURRENT_DATE) 
--			AND x7.periode < DATE_TRUNC('month', CURRENT_DATE) + INTERVAL '1 month'
		LEFT JOIN public.mb_alokasi_ac_rh x8 ON x8.kodetoko = x5.kolom_u2 
			AND x5.kolom_u2 <> 0
--			AND x8.periode >= DATE_TRUNC('month', CURRENT_DATE) 
--			AND x8.periode < DATE_TRUNC('month', CURRENT_DATE) + INTERVAL '1 month'
	)as x9
	);
END;
$function$
;


--------------------------------------------------------------------------------------------------------------------------


-- DROP FUNCTION public.fc_smi_trx_oil_goliaht_monitoring_mobeng_sby();

CREATE OR REPLACE FUNCTION public.fc_smi_trx_oil_goliaht_monitoring_mobeng_sby()
 RETURNS TABLE(insert_date timestamp without time zone, namacabang character varying, wa text, kb character varying, kc character varying, kd date, ke character varying, kf integer, kg integer, kh integer, ki double precision, kj character varying, kk double precision, kl double precision, km integer, kolom_nrpt text, kolom_orpt text, kolom_prpt text, kolom_qrpt text, kolom_rrpt integer, kolom_srpt bigint, kolom_trpt integer, kolom_urpt integer, kolom_vrpt integer, kolom_n date, kolom_o bigint, kolom_p1 bigint, kolom_p10 text, kolom_aarpt character varying, kolom_abrpt character varying, kolom_p date, kolom_q bigint, kolom_r date, kolom_s bigint, kolom_t1 bigint, kolom_t10 text, kolom_airpt character varying, kolom_ajrpt character varying, kolom_t date, kolom_u bigint, kolom_u1 date, kolom_u4 bigint, kolom_u2 bigint, kolom_u10 text, kolom_aqrpt character varying, kolom_arrpt character varying, kv integer, kw double precision, kx double precision, ky character varying, kz character varying, kaa date, kab character varying, kac character varying, kad text, kae date, kaf integer, kag date, kah text, kai text, kaj date, kak integer, kal text, kam text)
 LANGUAGE plpgsql
AS $function$
BEGIN
    RETURN QUERY
(
select
x9.insert_date,
x9.namacabang, 
--x9.wa, x9.kolom_b, x9.kolom_c, x9.kolom_d, x9.kolom_e, x9.kolom_f, x9.kolom_g, x9.kolom_h, x9.kolom_i, x9.kolom_j, x9.kolom_k, x9.kolom_l, x9.kolom_m, 
x9.wa, 
x9.kolom_b as kb, 
x9.kolom_c as kc, 
x9.kolom_d as kd, 
x9.kolom_e as ke, 
x9.kolom_f as kf, 
x9.kolom_g as kg, 
x9.kolom_h as kh, 
x9.kolom_i as ki, 
x9.kolom_j as kj, 
x9.kolom_k as kk, 
x9.kolom_l as kl, 
x9.kolom_m as km, 
x9.kolom_nrpt,
x9.kolom_orpt,
x9.kolom_prpt,
x9.kolom_qrpt,
x9.kolom_rrpt,
x9.kolom_srpt,
x9.kolom_trpt,
x9.kolom_urpt,
x9.kolom_vrpt,
x9.kolom_n,
x9.kolom_o,
x9.kolom_p1,
x9.kolom_p10,
x9.kolom_aarpt,
x9.kolom_abrpt,
x9.kolom_p,
x9.kolom_q,
x9.kolom_r,
x9.kolom_s,
x9.kolom_t1,
x9.kolom_t10,
x9.kolom_airpt,
x9.kolom_ajrpt,
x9.kolom_t,
x9.kolom_u,
x9.kolom_u1,
x9.kolom_u4,
x9.kolom_u2,
x9.kolom_u10,
x9.kolom_aqrpt,
x9.kolom_arrpt,
x9.kolom_v as kv, 
x9.kolom_w as kw, 
x9.kolom_x as kx, 
x9.kolom_y as ky, 
x9.kolom_z as kz, 
x9.kolom_aa as kaa, 
x9.kolom_ab as kab, 
x9.kolom_ac as kac, 
x9.kolom_ad as kad, 
x9.kolom_ae as kae, 
x9.kolom_af as kaf, 
x9.kolom_ag as kag, 
x9.kolom_ah1 as kah, 
x9.kolom_ai as kai, 
x9.kolom_aj as kaj, 
x9.kolom_ak as kak, 
x9.kolom_al as kal, 
x9.kolom_am as kam
from (
	select
	x5.insert_date,
	x5.namacabang, 
	x5.wa, x5.kolom_b, x5.kolom_c, x5.kolom_d, x5.kolom_e, x5.kolom_f, x5.kolom_g, x5.kolom_h, x5.kolom_i, x5.kolom_j, x5.kolom_k, x5.kolom_l, x5.kolom_m, 
	nrpt as kolom_nrpt,		--Status Keseluruhan (Suspect/Oke)
	orpt as kolom_orpt,		--Suspect Odometer Bergerak Terlalu Besar (Suspect/Oke)
	prpt as kolom_prpt,		--Suspect Odometer Bergerak Negatif (Suspect/Oke)
	qrpt as kolom_qrpt,		--Suspect Odometer Tidak Bergerak (Suspect/Oke)
	rrpt as kolom_rrpt,		--Batas Maksimal Pergerakan Odometer (KM)
	srpt as kolom_srpt,		--Pergerakan Odometer (KM) Berdasarkan 2 Transaksi Terakhir
	trpt as kolom_trpt,		--Jarak Hari Transaksi Yang Digunakan
	urpt as kolom_urpt,		--Jarak Hari Dari Transaksi Oli Terakhir ke Transaksi Oli Atau Non-Oli Kedua Dari Terakhir
	vrpt as kolom_vrpt,		--Jarak Hari Dari Transaksi Oli Terakhir ke Transaksi Oli Atau Non-Oli Terakhir
	x5.kolom_n,				--Tanggal Transaksi Oli Terakhir
	x5.kolom_o,				--Odometer Saat Transaksi Oli Terakhir
	'========' as "X",
	--x5.kolom_n1, x5.kolom_n10, x5.kolom_n2,  
	x5.kolom_p1,			--Store Code Transaksi Oli Atau Non-Oli Terakhir
	x5.kolom_p10,			--Nama Toko Transaksi Oli Atau Non-Oli Terakhir
	x6.rh as kolom_aarpt,			--Nama RH Transaksi Oli Atau Non-Oli Terakhir	
	x5.namacabang as kolom_abrpt,	--Cabang Toko Transaksi Oli Atau Non-Oli Terakhir
	x5.kolom_p,				--Tanggal Transaksi Oli Atau Non-Oli Terakhir
	x5.kolom_q,				--Odometer Saat Tanggal Transaksi Oli Atau Non-Oli Terakhir  
	--x5.kolom_p2,  
	'========' as "X",
	x5.kolom_r,				--Tanggal Saat Transaksi Oli Kedua Dari Terakhir
	x5.kolom_s, 			--Odometer Saat Transaksi Oli Kedua Dari Terakhir
	--x5.kolom_r1, x5.kolom_r10, --x5.kolom_r2, 
	'========' as "X",
	x5.kolom_t1,			--Store Code Transaksi Oli Atau Non-Oli Kedua Dari Terakhir
	x5.kolom_t10,			--Nama Toko Transaksi Oli Atau Non-Oli Kedua Dari Terakhir
	x7.rh as kolom_airpt,			--Nama RH Transaksi Oli Atau Non-Oli Terakhir	
--	x5.namacabang as kolom_ajrpt,	--Cabang Toko Transaksi Oli Atau Non-Oli Terakhir
	case when x5.kolom_t1=0 then null else x5.namacabang end as kolom_ajrpt,	--Cabang Toko Transaksi Oli Atau Non-Oli Terakhir
	x5.kolom_t,				--Tanggal Transaksi Oli Atau Non-Oli Kedua Dari Terakhir
	x5.kolom_u,  			--Odometer Saat Tanggal Transaksi Oli Atau Non-Oli Kedua Dari Terakhir
	--x5.kolom_t2, 
	'========' as "X",
	x5.kolom_u1, 			--Tanggal Transaksi Oli Atau Non-Oli Ketiga Dari Terakhir
	x5.kolom_u4,			--Odometer Saat Tanggal Transaksi Oli Atau Non-Oli Ketiga Dari Terakhir
	x5.kolom_u2, 			--Store Code Transaksi Oli Atau Non-Oli Ketiga Dari Terakhir
	x5.kolom_u10,			--Nama Toko Transaksi Oli Atau Non-Oli Ketiga Dari Terakhir
	x8.rh as kolom_aqrpt,			--Nama RH Transaksi Oli Atau Non-Oli Ketiga Dari Terakhir
--	x5.namacabang as kolom_arrpt,	--Cabang Toko Transaksi Oli Atau Non-Oli Ketiga Dari Terakhir
	case when x5.kolom_u2=0 then null else x5.namacabang end as kolom_arrpt,	--Cabang Toko Transaksi Oli Atau Non-Oli Ketiga Dari Terakhir
	  --x5.kolom_u3, 
	'========' as "X",
	x5.kolom_v, x5.kolom_w, x5.kolom_x, x5.kolom_y, x5.kolom_z, x5.kolom_aa, x5.kolom_ab, x5.kolom_ac, x5.kolom_ad, x5.kolom_ae, x5.kolom_af, x5.kolom_ag, 
	x5.kolom_ah1, x5.kolom_ai, x5.kolom_aj, x5.kolom_ak, x5.kolom_al, x5.kolom_am
	from (
		select
		x4.insert_date,
		x4.namacabang, x4.wa, x4.kolom_b, x4.kolom_c, x4.kolom_d, x4.kolom_e, x4.kolom_f, x4.kolom_g, x4.kolom_h, x4.kolom_i, x4.kolom_j, x4.kolom_k, 
		x4.kolom_l, x4.kolom_m, 
		CASE 
		    WHEN (CASE 
		    WHEN (CASE WHEN (x4.kolom_p - x4.kolom_n) > 0 THEN (x4.kolom_p - x4.kolom_n)
		    ELSE (CASE WHEN x4.kolom_t > '1900-01-01' THEN x4.kolom_n - x4.kolom_t ELSE 0 END) 
				END) > 0 THEN 
		        CASE 
		            WHEN (CASE WHEN (CASE WHEN (x4.kolom_p - x4.kolom_n) > 0 THEN (x4.kolom_p - x4.kolom_n)
					    ELSE (CASE WHEN x4.kolom_t > '1900-01-01' THEN x4.kolom_n - x4.kolom_t ELSE 0 END) 
						END) = 0 THEN 0 
					    ELSE 
					        CASE 
					            WHEN (CASE WHEN x4.kolom_t > '1900-01-01' THEN x4.kolom_n - x4.kolom_t 
								    ELSE 0 
									END) = (CASE WHEN (x4.kolom_p - x4.kolom_n) > 0 THEN (x4.kolom_p - x4.kolom_n)
										    ELSE (CASE WHEN x4.kolom_t > '1900-01-01' THEN x4.kolom_n - x4.kolom_t ELSE 0 END) 
										END) THEN x4.kolom_q - x4.kolom_u
						    ELSE x4.kolom_q - x4.kolom_o
						END 
					END) > ((CASE WHEN (x4.kolom_p - x4.kolom_n) > 0 THEN (x4.kolom_p - x4.kolom_n)
							    ELSE (CASE WHEN x4.kolom_t > '1900-01-01' THEN x4.kolom_n - x4.kolom_t ELSE 0 END) 
							END) * x4.kolom_v) THEN 'Suspect' 
		            ELSE 'Oke' 
		        END 
		    ELSE 'N/A' 
		END) = 'Suspect' OR (CASE 
		    WHEN (CASE WHEN (x4.kolom_p - x4.kolom_n) > 0 THEN (x4.kolom_p - x4.kolom_n)
		    ELSE (CASE WHEN x4.kolom_t > '1900-01-01' THEN x4.kolom_n - x4.kolom_t ELSE 0 END) 
				END) > 0 THEN 
		        CASE 
		            WHEN (CASE WHEN (CASE WHEN (x4.kolom_p - x4.kolom_n) > 0 THEN (x4.kolom_p - x4.kolom_n)
					    ELSE (CASE WHEN x4.kolom_t > '1900-01-01' THEN x4.kolom_n - x4.kolom_t ELSE 0 END) 
						END) = 0 THEN 0 
					    ELSE 
					        CASE 
					            WHEN (CASE WHEN x4.kolom_t > '1900-01-01' THEN x4.kolom_n - x4.kolom_t 
								    ELSE 0 
									END) = (CASE WHEN (x4.kolom_p - x4.kolom_n) > 0 THEN (x4.kolom_p - x4.kolom_n)
										    ELSE (CASE WHEN x4.kolom_t > '1900-01-01' THEN x4.kolom_n - x4.kolom_t ELSE 0 END) 
										END) THEN x4.kolom_q - x4.kolom_u
						    ELSE x4.kolom_q - x4.kolom_o
						END 
						END) < 0 THEN 'Suspect' 
		            ELSE 'Oke' 
		        END 
		    ELSE 'N/A' 
		END) = 'Suspect' OR (CASE 
		    WHEN (CASE WHEN (x4.kolom_p - x4.kolom_n) > 0 THEN (x4.kolom_p - x4.kolom_n)
		    ELSE (CASE WHEN x4.kolom_t > '1900-01-01' THEN x4.kolom_n - x4.kolom_t ELSE 0 END) 
				END) > 0 THEN 
		        CASE 
		            WHEN (CASE WHEN (CASE WHEN (x4.kolom_p - x4.kolom_n) > 0 THEN (x4.kolom_p - x4.kolom_n)
					    ELSE (CASE WHEN x4.kolom_t > '1900-01-01' THEN x4.kolom_n - x4.kolom_t ELSE 0 END) 
						END) = 0 THEN 0 
					    ELSE 
					        CASE 
					            WHEN (CASE WHEN x4.kolom_t > '1900-01-01' THEN x4.kolom_n - x4.kolom_t 
								    ELSE 0 
									END) = (CASE WHEN (x4.kolom_p - x4.kolom_n) > 0 THEN (x4.kolom_p - x4.kolom_n)
										    ELSE (CASE WHEN x4.kolom_t > '1900-01-01' THEN x4.kolom_n - x4.kolom_t ELSE 0 END) 
										END) THEN x4.kolom_q - x4.kolom_u
						    ELSE x4.kolom_q - x4.kolom_o
						END 
					END) = 0 THEN 'Suspect' 
		            ELSE 'Oke' 
		        END 
		    ELSE 'N/A' 
		END) = 'Suspect' THEN 'Suspect' ----------- 
					    WHEN (CASE 
		    WHEN (CASE WHEN (x4.kolom_p - x4.kolom_n) > 0 THEN (x4.kolom_p - x4.kolom_n)
		    ELSE (CASE WHEN x4.kolom_t > '1900-01-01' THEN x4.kolom_n - x4.kolom_t ELSE 0 END) 
				END) > 0 THEN 
		        CASE 
		            WHEN (CASE WHEN (CASE WHEN (x4.kolom_p - x4.kolom_n) > 0 THEN (x4.kolom_p - x4.kolom_n)
					    ELSE (CASE WHEN x4.kolom_t > '1900-01-01' THEN x4.kolom_n - x4.kolom_t ELSE 0 END) 
						END) = 0 THEN 0 
					    ELSE 
					        CASE 
					            WHEN (CASE WHEN x4.kolom_t > '1900-01-01' THEN x4.kolom_n - x4.kolom_t 
								    ELSE 0 
									END) = (CASE WHEN (x4.kolom_p - x4.kolom_n) > 0 THEN (x4.kolom_p - x4.kolom_n)
										    ELSE (CASE WHEN x4.kolom_t > '1900-01-01' THEN x4.kolom_n - x4.kolom_t ELSE 0 END) 
										END) THEN x4.kolom_q - x4.kolom_u
						    ELSE x4.kolom_q - x4.kolom_o
						END 
					END) > ((CASE WHEN (x4.kolom_p - x4.kolom_n) > 0 THEN (x4.kolom_p - x4.kolom_n)
							    ELSE (CASE WHEN x4.kolom_t > '1900-01-01' THEN x4.kolom_n - x4.kolom_t ELSE 0 END) 
							END) * x4.kolom_v) THEN 'Suspect' 
		            ELSE 'Oke' 
		        END 
		    ELSE 'N/A' -----------
		END) = 'Oke' OR (CASE 
		    WHEN (CASE WHEN (x4.kolom_p - x4.kolom_n) > 0 THEN (x4.kolom_p - x4.kolom_n)
		    ELSE (CASE WHEN x4.kolom_t > '1900-01-01' THEN x4.kolom_n - x4.kolom_t ELSE 0 END) 
				END) > 0 THEN 
		        CASE 
		            WHEN (CASE WHEN (CASE WHEN (x4.kolom_p - x4.kolom_n) > 0 THEN (x4.kolom_p - x4.kolom_n)
					    ELSE (CASE WHEN x4.kolom_t > '1900-01-01' THEN x4.kolom_n - x4.kolom_t ELSE 0 END) 
						END) = 0 THEN 0 
					    ELSE 
					        CASE 
					            WHEN (CASE WHEN x4.kolom_t > '1900-01-01' THEN x4.kolom_n - x4.kolom_t 
								    ELSE 0 
									END) = (CASE WHEN (x4.kolom_p - x4.kolom_n) > 0 THEN (x4.kolom_p - x4.kolom_n)
										    ELSE (CASE WHEN x4.kolom_t > '1900-01-01' THEN x4.kolom_n - x4.kolom_t ELSE 0 END) 
										END) THEN x4.kolom_q - x4.kolom_u
						    ELSE x4.kolom_q - x4.kolom_o
						END 
						END) < 0 THEN 'Suspect' 
		            ELSE 'Oke' 
		        END 
		    ELSE 'N/A'  -----------
		END) = 'Oke' OR (CASE 
		    WHEN (CASE WHEN (x4.kolom_p - x4.kolom_n) > 0 THEN (x4.kolom_p - x4.kolom_n)
		    ELSE (CASE WHEN x4.kolom_t > '1900-01-01' THEN x4.kolom_n - x4.kolom_t ELSE 0 END) 
				END) > 0 THEN 
		        CASE 
		            WHEN (CASE WHEN (CASE WHEN (x4.kolom_p - x4.kolom_n) > 0 THEN (x4.kolom_p - x4.kolom_n)
					    ELSE (CASE WHEN x4.kolom_t > '1900-01-01' THEN x4.kolom_n - x4.kolom_t ELSE 0 END) 
						END) = 0 THEN 0 
					    ELSE 
					        CASE 
					            WHEN (CASE WHEN x4.kolom_t > '1900-01-01' THEN x4.kolom_n - x4.kolom_t 
								    ELSE 0 
									END) = (CASE WHEN (x4.kolom_p - x4.kolom_n) > 0 THEN (x4.kolom_p - x4.kolom_n)
										    ELSE (CASE WHEN x4.kolom_t > '1900-01-01' THEN x4.kolom_n - x4.kolom_t ELSE 0 END) 
										END) THEN x4.kolom_q - x4.kolom_u
						    ELSE x4.kolom_q - x4.kolom_o
						END 
					END) = 0 THEN 'Suspect' 
		            ELSE 'Oke' 
		        END 
		    ELSE 'N/A' 
		END) = 'Oke' THEN 'Oke' 
		    ELSE 'N/A' 
		END AS nrpt,	--Status Keseluruhan (Suspect/Oke)
		CASE 
		    WHEN (CASE WHEN (x4.kolom_p - x4.kolom_n) > 0 THEN (x4.kolom_p - x4.kolom_n)
		    ELSE (CASE WHEN x4.kolom_t > '1900-01-01' THEN x4.kolom_n - x4.kolom_t ELSE 0 END) 
				END) > 0 THEN 
		        CASE 
		            WHEN (CASE WHEN (CASE WHEN (x4.kolom_p - x4.kolom_n) > 0 THEN (x4.kolom_p - x4.kolom_n)
					    ELSE (CASE WHEN x4.kolom_t > '1900-01-01' THEN x4.kolom_n - x4.kolom_t ELSE 0 END) 
						END) = 0 THEN 0 
					    ELSE 
					        CASE 
					            WHEN (CASE WHEN x4.kolom_t > '1900-01-01' THEN x4.kolom_n - x4.kolom_t 
								    ELSE 0 
									END) = (CASE WHEN (x4.kolom_p - x4.kolom_n) > 0 THEN (x4.kolom_p - x4.kolom_n)
										    ELSE (CASE WHEN x4.kolom_t > '1900-01-01' THEN x4.kolom_n - x4.kolom_t ELSE 0 END) 
										END) THEN x4.kolom_q - x4.kolom_u
						    ELSE x4.kolom_q - x4.kolom_o
						END 
					END) > ((CASE WHEN (x4.kolom_p - x4.kolom_n) > 0 THEN (x4.kolom_p - x4.kolom_n)
							    ELSE (CASE WHEN x4.kolom_t > '1900-01-01' THEN x4.kolom_n - x4.kolom_t ELSE 0 END) 
							END) * x4.kolom_v) THEN 'Suspect' 
		            ELSE 'Oke' 
		        END 
		    ELSE 'N/A' 
		END AS orpt,	--Suspect Odometer Bergerak Terlalu Besar (Suspect/Oke)	-------//Perbaikan 20241125
		CASE 
		    WHEN (CASE WHEN (x4.kolom_p - x4.kolom_n) > 0 THEN (x4.kolom_p - x4.kolom_n)
		    ELSE (CASE WHEN x4.kolom_t > '1900-01-01' THEN x4.kolom_n - x4.kolom_t ELSE 0 END) 
				END) > 0 THEN 
		        CASE 
		            WHEN (CASE WHEN (CASE WHEN (x4.kolom_p - x4.kolom_n) > 0 THEN (x4.kolom_p - x4.kolom_n)
					    ELSE (CASE WHEN x4.kolom_t > '1900-01-01' THEN x4.kolom_n - x4.kolom_t ELSE 0 END) 
						END) = 0 THEN 0 
					    ELSE 
					        CASE 
					            WHEN (CASE WHEN x4.kolom_t > '1900-01-01' THEN x4.kolom_n - x4.kolom_t 
								    ELSE 0 
									END) = (CASE WHEN (x4.kolom_p - x4.kolom_n) > 0 THEN (x4.kolom_p - x4.kolom_n)
										    ELSE (CASE WHEN x4.kolom_t > '1900-01-01' THEN x4.kolom_n - x4.kolom_t ELSE 0 END) 
										END) THEN x4.kolom_q - x4.kolom_u
						    ELSE x4.kolom_q - x4.kolom_o
						END 
						END) < 0 THEN 'Suspect' 
		            ELSE 'Oke' 
		        END 
		    ELSE 'N/A' 
		END AS prpt,	--Suspect Odometer Bergerak Negatif (Suspect/Oke)	-------//Perbaikan 20241125
		CASE 
		    WHEN (CASE WHEN (x4.kolom_p - x4.kolom_n) > 0 THEN (x4.kolom_p - x4.kolom_n)
		    ELSE (CASE WHEN x4.kolom_t > '1900-01-01' THEN x4.kolom_n - x4.kolom_t ELSE 0 END) 
				END) > 0 THEN 
		        CASE 
		            WHEN (CASE WHEN (CASE WHEN (x4.kolom_p - x4.kolom_n) > 0 THEN (x4.kolom_p - x4.kolom_n)
					    ELSE (CASE WHEN x4.kolom_t > '1900-01-01' THEN x4.kolom_n - x4.kolom_t ELSE 0 END) 
						END) = 0 THEN 0 
					    ELSE 
					        CASE 
					            WHEN (CASE WHEN x4.kolom_t > '1900-01-01' THEN x4.kolom_n - x4.kolom_t 
								    ELSE 0 
									END) = (CASE WHEN (x4.kolom_p - x4.kolom_n) > 0 THEN (x4.kolom_p - x4.kolom_n)
										    ELSE (CASE WHEN x4.kolom_t > '1900-01-01' THEN x4.kolom_n - x4.kolom_t ELSE 0 END) 
										END) THEN x4.kolom_q - x4.kolom_u
						    ELSE x4.kolom_q - x4.kolom_o
						END 
					END) = 0 THEN 'Suspect' 
		            ELSE 'Oke' 
		        END 
		    ELSE 'N/A' 
		END AS qrpt,	--Suspect Odometer Tidak Bergerak (Suspect/Oke)	-------//Perbaikan 20241125
		(CASE 
			WHEN (x4.kolom_p - x4.kolom_n) > 0 THEN (x4.kolom_p - x4.kolom_n)
			--(CASE WHEN x4.kolom_t > '1900-01-01' THEN x4.kolom_n - x4.kolom_t ELSE 0 END)
		    ELSE (CASE WHEN x4.kolom_t > '1900-01-01' THEN x4.kolom_n - x4.kolom_t ELSE 0 END) 
		END) * x4.kolom_v as rrpt,	--Batas Maksimal Pergerakan Odometer (KM)	-------//Perbaikan 20241125
		CASE 
		    WHEN (CASE WHEN (x4.kolom_p - x4.kolom_n) > 0 THEN (x4.kolom_p - x4.kolom_n)
			    ELSE (CASE WHEN x4.kolom_t > '1900-01-01' THEN x4.kolom_n - x4.kolom_t ELSE 0 END) 
			END) = 0 THEN 0 
		    ELSE 
		        CASE 
		            WHEN (CASE WHEN x4.kolom_t > '1900-01-01' THEN x4.kolom_n - x4.kolom_t 
					    ELSE 0 
						END) = (CASE WHEN (x4.kolom_p - x4.kolom_n) > 0 THEN (x4.kolom_p - x4.kolom_n)
							    ELSE (CASE WHEN x4.kolom_t > '1900-01-01' THEN x4.kolom_n - x4.kolom_t ELSE 0 END) 
							END) THEN x4.kolom_q - x4.kolom_u
			    ELSE x4.kolom_q - x4.kolom_o
			END 
		END AS srpt,	--Pergerakan Odometer (KM) Berdasarkan 2 Transaksi Terakhir	-------//Perbaikan 20241125
		CASE 
			WHEN (x4.kolom_p - x4.kolom_n) > 0 THEN (x4.kolom_p - x4.kolom_n)
			--(CASE WHEN x4.kolom_t > '1900-01-01' THEN x4.kolom_n - x4.kolom_t ELSE 0 END)
		    ELSE (CASE WHEN x4.kolom_t > '1900-01-01' THEN x4.kolom_n - x4.kolom_t ELSE 0 END) 
		END AS trpt,	--Jarak Hari Transaksi Yang Digunakan	-------//Perbaikan 20241125-OK
		CASE 
		    WHEN x4.kolom_t > '1900-01-01' THEN x4.kolom_n - x4.kolom_t 
		    ELSE 0 
		END AS urpt,	--Jarak Hari Dari Transaksi Oli Terakhir ke Transaksi Oli Atau Non-Oli Kedua Dari Terakhir
		x4.kolom_p - x4.kolom_n as vrpt,	--Jarak Hari Dari Transaksi Oli Terakhir ke Transaksi Oli Atau Non-Oli Terakhir -------//Perbaikan 20241125-OK
		x4.kolom_n, x4.kolom_n1, x4.kolom_n10, x4.kolom_n2, x4.kolom_o, 
		x4.kolom_p, x4.kolom_p1, x4.kolom_p10, x4.kolom_p2, x4.kolom_q, 
		x4.kolom_r, x4.kolom_r1, x4.kolom_r10, x4.kolom_r2, x4.kolom_s, 
		x4.kolom_t, x4.kolom_t1, x4.kolom_t10, x4.kolom_t2, x4.kolom_u, 
		x4.kolom_u1, x4.kolom_u2, x4.kolom_u10, x4.kolom_u3, x4.kolom_u4,
		x4.kolom_v, x4.kolom_w, 
		x4.kolom_x, x4.kolom_y, x4.kolom_z, x4.kolom_aa, x4.kolom_ab, x4.kolom_ac, x4.kolom_ad, x4.kolom_ae, x4.kolom_af, x4.kolom_ag, x4.kolom_ah1, 
		x4.kolom_ai, x4.kolom_aj, x4.kolom_ak, x4.kolom_al, x4.kolom_am
		from ( 
			select
			x3.insert_date,
			x3.namacabang, x3.wa, x3.kolom_b, x3.kolom_c, x3.kolom_d, x3.kolom_e, x3.kolom_f, x3.kolom_g, x3.kolom_h, x3.kolom_i, x3.kolom_j, x3.kolom_k, 
			x3.kolom_l, x3.kolom_m, 
			x3.kolom_n, x3.kolom_n1, x3.kolom_n10, x3.kolom_n2, x3.kolom_o, 
			x3.kolom_p, x3.kolom_p1, x3.kolom_p10, x3.kolom_p2, x3.kolom_q, 
			x3.kolom_r, x3.kolom_r1, x3.kolom_r10, x3.kolom_r2, x3.kolom_s, 
			x3.kolom_t, x3.kolom_t1, x3.kolom_t10, x3.kolom_t2, x3.kolom_u, 
			x3.kolom_u1, x3.kolom_u2, x3.kolom_u10, x3.kolom_u3, x3.kolom_u4,
			x3.kolom_v, x3.kolom_w, 
			x3.kolom_x, x3.kolom_y, x3.kolom_z, x3.kolom_aa, x3.kolom_ab, x3.kolom_ac, x3.kolom_ad, x3.kolom_ae, x3.kolom_af, x3.kolom_ag, x3.kolom_ah1, 
			x3.kolom_ai, x3.kolom_aj, x3.kolom_ak, x3.kolom_al, x3.kolom_am
			from (
			select
				x2.insert_date,
				x2.namacabang, '62' || SUBSTRING(x2.notelp FROM 2) AS wa, x2.kolom_b, x2.kolom_c, x2.kolom_d, x2.kolom_e, x2.kolom_f, x2.kolom_g, x2.kolom_h, x2.kolom_i, x2.kolom_j, x2.kolom_k, 
				x2.kolom_l, x2.kolom_m, 
				x2.kolom_n, x2.kolom_n1, x2.kolom_n10, x2.kolom_n2, x2.kolom_o, 
				x2.kolom_p, x2.kolom_p1, x2.kolom_p10, x2.kolom_p2, x2.kolom_q, 
				x2.kolom_r, x2.kolom_r1, x2.kolom_r10, x2.kolom_r2, x2.kolom_s, 
				x2.kolom_t, x2.kolom_t1, x2.kolom_t10, x2.kolom_t2, x2.kolom_u, 
				x2.kolom_u1, x2.kolom_u2, x2.kolom_u10, x2.kolom_u3, x2.kolom_u4,
				x2.kolom_v, x2.kolom_w, 
				x2.kolom_x, x2.kolom_y, x2.kolom_z, x2.kolom_aa, x2.kolom_ab, x2.kolom_ac, x2.kolom_ad, x2.kolom_ae, x2.kolom_af, x2.kolom_ag, x2.kolom_ah1, 
				--x2.kolom_ai,
				CASE
				    -- Jika kolom_ah = 0, status adalah 'Reminded'
				    WHEN COALESCE(
				            CASE 
				                WHEN x2.kolom_ah1 ~ '^\d+$' THEN x2.kolom_ah1::integer
				                ELSE NULL
				            END, 
				            -1
				         ) = 0 THEN 'Reminded'
				    -- Mengecek jika kolom_ac valid sebagai integer dan lebih kecil dari 1, serta kolom_ae < kolom_aa
				    WHEN COALESCE(
				            CASE 
				                WHEN x2.kolom_ah1 ~ '^\d+$' THEN x2.kolom_ah1::integer
				                ELSE NULL
				            END, 
				            -1
				         ) < 1
				         AND COALESCE(
				                 NULLIF(x2.kolom_aj, null)::date, 
				                 '1900-01-01'::date
				             ) < x2.kolom_ag::date THEN 'To Remind'
				    -- Mengecek jika kolom_ae >= kolom_aa dan kolom_aa > '1900-01-01'
				    WHEN COALESCE(
				            NULLIF(x2.kolom_aj, null)::date, 
				            '1900-01-01'::date
				         ) >= x2.kolom_ag::date
				         AND x2.kolom_ag::date > '1900-01-01'::date THEN 'Reminded'
				    ELSE 'Do Not Remind Yet'
				END AS kolom_ai,
			--	x2.kolom_aj, 
				CASE 
					WHEN x2.kolom_ah1 = 'N/A' THEN NULL
					WHEN CAST(x2.kolom_ah1 AS INTEGER) < 0 THEN x2.kolom_aj
				    WHEN CAST(x2.kolom_ah1 AS INTEGER) > 0 THEN NULL
				    WHEN CAST(x2.kolom_ah1 AS INTEGER) = 0 THEN x2.kolom_ag
				END AS kolom_aj,
				x2.kolom_ak,
				CASE 
				    WHEN x2.kolom_ah1 ~ '^\d+$' THEN
				        COALESCE(
				            ((x2.kolom_ah1::integer) + x2.kolom_ak)::text,
				            'N/A'
				        )
				    ELSE 'N/A'
					END AS kolom_al,
			    CASE
			        WHEN (CASE
			            WHEN CASE 
					    WHEN x2.kolom_ah1 ~ '^\d+$' THEN
					        COALESCE(
					            ((kolom_ah1::integer) + x2.kolom_ak)::text,
					            'N/A'
					        )
					    ELSE 'N/A'
					END ~ '^\d+$' THEN (CASE 
					    WHEN x2.kolom_ah1 ~ '^\d+$' THEN
					        COALESCE(
					            ((x2.kolom_ah1::integer) + x2.kolom_ak)::text,
					            'N/A'
					        )
					    ELSE 'N/A'
					END::integer) + x2.kolom_ak
					            ELSE NULL
					        END) < 0 THEN 'Masuk'
				    ELSE 'Tidak Masuk'
				END AS kolom_am
				FROM(
					select
					x1.insert_date,
					x1.namacabang, x1.notelp, x1.kolom_b, x1.kolom_c, x1.kolom_d, x1.kolom_e, x1.kolom_f, x1.kolom_g, x1.kolom_h, x1.kolom_i, x1.kolom_j, x1.kolom_k, x1.kolom_l, x1.kolom_m, 
					x1.kolom_n, x1.kolom_n1, x1.kolom_n10, x1.kolom_n2, x1.kolom_o, 
					x1.kolom_p, x1.kolom_p1, x1.kolom_p10, x1.kolom_p2, x1.kolom_q, 
					x1.kolom_r, x1.kolom_r1, x1.kolom_r10, x1.kolom_r2, x1.kolom_s, 
					x1.kolom_t, x1.kolom_t1, x1.kolom_t10, x1.kolom_t2, x1.kolom_u, 
					x1.kolom_u1, x1.kolom_u2, x1.kolom_u10, x1.kolom_u3, x1.kolom_u4,			
					x1.kolom_v, x1.kolom_w, x1.kolom_x, x1.kolom_y, x1.kolom_z, x1.kolom_aa, x1.kolom_ab, x1.kolom_ac, x1.kolom_ad, 
					--x1.kolom_ae, 
					CASE 
						WHEN x1.kolom_ac = 'N/A' THEN NULL
						WHEN CAST(x1.kolom_ac AS INTEGER) < 0 THEN x1.kolom_ae
				        WHEN CAST(x1.kolom_ac AS INTEGER) > 0 THEN NULL
				        WHEN CAST(x1.kolom_ac AS INTEGER) = 0 THEN x1.kolom_aa
					END AS kolom_ae,
					x1.kolom_af, 
					--x1.kolom_ag, 
					((CASE 
						WHEN x1.kolom_ac = 'N/A' THEN NULL
					    WHEN CAST(x1.kolom_ac AS INTEGER) < 0 THEN x1.kolom_ae
				        WHEN CAST(x1.kolom_ac AS INTEGER) > 0 THEN NULL
				        WHEN CAST(x1.kolom_ac AS INTEGER) = 0 THEN x1.kolom_aa
					end) + x1.kolom_af) as kolom_ag,
					--x1.kolom_ah,
					CASE 
					    WHEN ((CASE 
						WHEN x1.kolom_ac = 'N/A' THEN NULL
					    WHEN CAST(x1.kolom_ac AS INTEGER) < 0 THEN x1.kolom_ae
				        WHEN CAST(x1.kolom_ac AS INTEGER) > 0 THEN NULL
				        WHEN CAST(x1.kolom_ac AS INTEGER) = 0 THEN x1.kolom_aa
					end) + x1.kolom_af) IS NOT NULL AND x1.kolom_d IS NOT NULL THEN (((CASE 
					    WHEN CAST(x1.kolom_ac AS INTEGER) < 0 THEN x1.kolom_ae
				        WHEN CAST(x1.kolom_ac AS INTEGER) > 0 THEN NULL
				        WHEN CAST(x1.kolom_ac AS INTEGER) = 0 THEN x1.kolom_aa
					end) + x1.kolom_af) - x1.kolom_d)::text 
					    ELSE 'N/A' 
					end as kolom_ah1,
					x1.kolom_aj, 
					x1.kolom_ak
					FROM(
						SELECT 
						x.insert_date,
						x.namacabang, x.notelp, x.kolom_b, x.kolom_c, x.kolom_d, x.kolom_e, x.kolom_f, x.kolom_g, x.kolom_h, x.kolom_i, x.kolom_j, x.kolom_k, x.kolom_l, x.kolom_m, 
						x.kolom_n, x.kolom_n1, x.kolom_n10, x.kolom_n2, x.kolom_o, 
						x.kolom_p, x.kolom_p1, x.kolom_p10, x.kolom_p2, x.kolom_q, 
						x.kolom_r, x.kolom_r1, x.kolom_r10, x.kolom_r2, x.kolom_s, 
						x.kolom_t, x.kolom_t1, x.kolom_t10, x.kolom_t2, x.kolom_u, 
						x.kolom_u1, x.kolom_u2, x.kolom_u10, x.kolom_u3, x.kolom_u4, 
						x.kolom_v, x.kolom_w, x.kolom_x, x.kolom_y, x.kolom_z, x.kolom_aa, x.kolom_ab, x.kolom_ac,
						CASE
						    -- Jika kolom_ac = 0, status adalah 'Reminded'
						    WHEN COALESCE(
						            CASE 
						                WHEN x.kolom_ac ~ '^\d+$' THEN x.kolom_ac::integer
						                ELSE NULL
						            END, 
						            -1
						         ) = 0 THEN 'Reminded'
						    -- Mengecek jika kolom_ac valid sebagai integer dan lebih kecil dari 1, serta kolom_ae < kolom_aa
						    WHEN COALESCE(
						            CASE 
						                WHEN x.kolom_ac ~ '^\d+$' THEN x.kolom_ac::integer
						                ELSE NULL
						            END, 
						            -1
						         ) < 1
						         AND COALESCE(
						                 NULLIF(x.kolom_ae, null)::date,
						                 '1900-01-01'::date
						             ) < x.kolom_aa THEN 'To Remind'
						    -- Mengecek jika kolom_ae >= kolom_aa dan kolom_aa > '1900-01-01'
						    WHEN COALESCE(
						            NULLIF(x.kolom_ae, null)::date,
						            '1900-01-01'::date
						         ) >= x.kolom_aa
						         AND x.kolom_aa > '1900-01-01'::date THEN 'Reminded'
						    ELSE 'Do Not Remind Yet'
						END AS kolom_ad,
						x.kolom_ae,
						x.kolom_af,
						CASE
						    WHEN x.kolom_af > 0 THEN (x.kolom_ae + INTERVAL '1 day' * x.kolom_af)::date::text
						    ELSE 'N/A'
						END AS kolom_ag,
						x.kolom_ak,
						x.kolom_aj,
						x.string_length,--update 3-mar-2025
						x.first_character,--update 3-mar-2025
						x.second_character,--update 3-mar-2025
						x.third_character--update 3-mar-2025
						FROM(
							SELECT 
								a.insert_date,
								a.namacabang, a.notelp, a.kolom_b, a.kolom_c, a.kolom_d, a.kolom_e, a.kolom_f, a.kolom_g, a.kolom_h, a.kolom_i, a.kolom_j, a.kolom_k, a.kolom_l, a.kolom_m, 
								a.kolom_n, a.kolom_n1, c.namatoko as kolom_n10, a.kolom_n2, a.kolom_o, 
								a.kolom_p, a.kolom_p1, d.namatoko as kolom_p10, a.kolom_p2, a.kolom_q, 
								a.kolom_r, a.kolom_r1, e.namatoko as kolom_r10, a.kolom_r2, a.kolom_s,
								a.kolom_t, a.kolom_t1, f.namatoko as kolom_t10, a.kolom_t2, a.kolom_u,
								a.kolom_u1, a.kolom_u2, g.namatoko as kolom_u10, a.kolom_u3, a.kolom_u4,
								a.kolom_v, a.kolom_w, a.kolom_x, a.kolom_y, a.kolom_z, a.kolom_aa, a.kolom_ab, a.kolom_ac, b.aehis as kolom_ae,
								30 kolom_af, --hardcode senin 20240902
								30 kolom_ak, --hardcode senin 20240902
								b.ajhis as kolom_aj
								,coalesce(LENGTH(a.notelp),'2') AS string_length--update 3-mar-2025
								,coalesce(LEFT(a.notelp, 1),'1') AS first_character--update 3-mar-2025
								,coalesce(SUBSTRING(a.notelp FROM 2 FOR 1),'1') AS second_character--update 3-mar-2025
								,coalesce(SUBSTRING(a.notelp FROM 3 FOR 1),'1') AS third_character--update 3-mar-2025
							FROM mb_rms20_rpt.mb_trx_oil_goliaht_monitoring_sby a
							LEFT JOIN (
								select 
								x1.cabhis, 
								x1.chis, 
								x1.aehis, 
								x1.ajhis,
								my_rank
								from (
									SELECT 
									x0.namacabang as cabhis, 
									x0.kolom_c as chis, 
									x0.kolom_ae as aehis, 
									x0.kolom_aj as ajhis, 
--									RANK() OVER (ORDER BY kolom_d DESC) AS my_rank
									RANK() OVER (PARTITION BY x0.kolom_c ORDER BY x0.kolom_d DESC) AS my_rank
									from public.mb_trx_oil_goliaht_his x0
									)as x1 where x1.my_rank=1
							) b on a.kolom_c = b.chis AND a.namacabang = b.cabhis
--							LEFT JOIN public.smi_trx_oil_goliaht_his b on a.kolom_c = b.kolom_c
							LEFT JOIN mb_rms01_mbho.mv_msttokoho_rms c on c.kodetoko = a.kolom_n1
							LEFT JOIN mb_rms01_mbho.mv_msttokoho_rms d on d.kodetoko = a.kolom_p1
							LEFT JOIN mb_rms01_mbho.mv_msttokoho_rms e on e.kodetoko = a.kolom_r1
							LEFT JOIN mb_rms01_mbho.mv_msttokoho_rms f on f.kodetoko = a.kolom_t1
							LEFT JOIN mb_rms01_mbho.mv_msttokoho_rms g on g.kodetoko = a.kolom_u2
							WHERE a.notelp IN (
												SELECT notelp 
												FROM mb_rms20_rpt.mb_trx_oil_goliaht_monitoring_sby
												GROUP BY notelp 
												HAVING COUNT(*) = 1
												)
						)as x
						WHERE x.string_length IN ('10','11','12','13','14')--update 3-mar-2025
						AND x.first_character = '0'--update 3-mar-2025
						AND x.second_character = '8'--update 3-mar-2025
						AND x.third_character IN ('1','2','3','5','6','7','8','9')--update 3-mar-2025
--						AND x.kolom_ac='0'
					)as x1
				)as x2
--				WHERE x2.kolom_ac='0'
--				WHERE x2.kolom_ah1='0'
--				WHERE x2.kolom_c in ('B2043KFR')
--				where x2.kolom_c in ('A2876MN','F4818WAT','D3683XGK','DK4688LS','A2678EW','H4149ACW')
				)as x3
			)as x4
		)as x5
		LEFT JOIN public.mb_alokasi_ac_rh x6 ON x6.kodetoko = x5.kolom_p1
--			AND x6.periode >= DATE_TRUNC('month', CURRENT_DATE) 
--			AND x6.periode < DATE_TRUNC('month', CURRENT_DATE) + INTERVAL '1 month'
		LEFT JOIN public.mb_alokasi_ac_rh x7 ON x7.kodetoko = x5.kolom_t1 
			AND x5.kolom_t1 <> 0
--			AND x7.periode >= DATE_TRUNC('month', CURRENT_DATE) 
--			AND x7.periode < DATE_TRUNC('month', CURRENT_DATE) + INTERVAL '1 month'
		LEFT JOIN public.mb_alokasi_ac_rh x8 ON x8.kodetoko = x5.kolom_u2 
			AND x5.kolom_u2 <> 0
--			AND x8.periode >= DATE_TRUNC('month', CURRENT_DATE) 
--			AND x8.periode < DATE_TRUNC('month', CURRENT_DATE) + INTERVAL '1 month'
	)as x9
	);
END;
$function$
;


--------------------------------------------------------------------------------------------------------------------------


INSERT INTO public.mb_trx_oil_goliaht_his (
    insert_date, namacabang, notelp, kolom_b, kolom_c, kolom_d, kolom_e, kolom_f, 
    kolom_g, kolom_h, kolom_i, kolom_j, kolom_k, kolom_l, kolom_m, kolom_n, 
    kolom_o, kolom_p, kolom_q, kolom_r, kolom_s, kolom_t, kolom_u, kolom_v, 
    kolom_w, kolom_x, kolom_y, kolom_z, kolom_aa, kolom_ab, kolom_ac, 
    kolom_ad, kolom_ae, kolom_af, kolom_ag, kolom_ah, kolom_ai, kolom_aj, 
    kolom_ak, kolom_al, kolom_am
)
SELECT 
    insert_date, namacabang, wa as notelp, kolom_b, kolom_c, kolom_d, kolom_e, kolom_f, 
    kolom_g, kolom_h, kolom_i, kolom_j, kolom_k, kolom_l, kolom_m, kolom_n, 
    kolom_o, kolom_p, kolom_q, kolom_r, kolom_s, kolom_t, kolom_u, kolom_v, 
    kolom_w, kolom_x, kolom_y, kolom_z, kolom_aa, kolom_ab, kolom_ac, 
    kolom_ad, kolom_ae, kolom_af, kolom_ag, kolom_ah, kolom_ai, kolom_aj, 
    kolom_ak, kolom_al, kolom_am
FROM public.mb_trx_oil_goliaht_monitoring_all
WHERE kolom_d::date = current_date AND kolom_nrpt = 'Oke' AND kolom_ac::numeric = 0;


--------------------------------------------------------------------------------------------------------------------------


INSERT INTO public.mb_trx_oil_goliaht_his (
    insert_date, namacabang, notelp, kolom_b, kolom_c, kolom_d, kolom_e, kolom_f, 
    kolom_g, kolom_h, kolom_i, kolom_j, kolom_k, kolom_l, kolom_m, kolom_n, 
    kolom_o, kolom_p, kolom_q, kolom_r, kolom_s, kolom_t, kolom_u, kolom_v, 
    kolom_w, kolom_x, kolom_y, kolom_z, kolom_aa, kolom_ab, kolom_ac, 
    kolom_ad, kolom_ae, kolom_af, kolom_ag, kolom_ah, kolom_ai, kolom_aj, 
    kolom_ak, kolom_al, kolom_am
)
SELECT 
    insert_date, namacabang, wa as notelp, kolom_b, kolom_c, kolom_d, kolom_e, kolom_f, 
    kolom_g, kolom_h, kolom_i, kolom_j, kolom_k, kolom_l, kolom_m, kolom_n, 
    kolom_o, kolom_p, kolom_q, kolom_r, kolom_s, kolom_t, kolom_u, kolom_v, 
    kolom_w, kolom_x, kolom_y, kolom_z, kolom_aa, kolom_ab, kolom_ac, 
    kolom_ad, kolom_ae, kolom_af, kolom_ag, kolom_ah, kolom_ai, kolom_aj, 
    kolom_ak, kolom_al, kolom_am
FROM public.mb_trx_oil_goliaht_monitoring_all
WHERE kolom_d::date = current_date AND kolom_nrpt = 'Oke' AND kolom_ah IS NOT NULL AND kolom_ah <> 'N/A' AND kolom_ah::numeric = 0;


--------------------------------------------------------------------------------------------------------------------------


DML Query Jobs
Jobs name
	insert_smi_trx_oil_goliaht_mobeng_0					POOL/localhost <DATARMS>	05/27/2025 14:30:06		insert_smi_trx_oil_goliaht_mobeng	OK
	insert_smi_trx_oil_goliaht_mobeng_minus				POOL/localhost <DATARMS>	05/27/2025 14:30:06		insert_smi_trx_oil_goliaht_mobeng	OK
	1.delete_mb_rpt_goliaht_last_trx_monthly_temp		POOL/localhost <DATARMS>	05/26/2025 10:59:39		MB_RPT_GOLIAHT_M1	OK
	2.insert_fc_mb_rpt_goliaht_last_trx_monthly_rms10	POOL/localhost <DATARMS>	05/26/2025 10:59:46		MB_RPT_GOLIAHT_M1	OK
	3.insert_fc_mb_rpt_goliaht_last_trx_monthly_rms20	POOL/localhost <DATARMS>	05/26/2025 10:59:50		MB_RPT_GOLIAHT_M1	OK

	
--------------------------------------------------------------------------------------------------------------------------

	
Scheduled DML Query
Name
	MB_RPT_GOLIAHT_M1									06/01/2025 08:30:00	Months
	insert_smi_trx_oil_goliaht_monitoring_all_mobeng	05/28/2025 11:30:00	Days


--------------------------------------------------------------------------------------------------------------------------