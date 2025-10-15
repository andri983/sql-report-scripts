USE [SMI_DB_Reporting_Bali]
GO

/****** Object:  StoredProcedure [dbo].[GetSMIStoreEOD]    Script Date: 9/11/2025 9:59:29 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

ALTER PROCEDURE [dbo].[GetSMIStoreEOD]
    @idcabang INT,
    @date1 DATE,
    @date2 DATE
AS
BEGIN
    SET NOCOUNT ON;
    
    SELECT 
		@date1 as tglbisnis,
        e.idcabang,
		a.kodeToko as kodetoko,
        a.namaToko as namatoko,
		g.NikRH,
        g.regional,
		g.nikac,
        g.area,
        CASE WHEN f.totalHeader = f.totalDetail AND f.totalDetail = f.totalRekapHeader THEN 'Ok' ELSE 'Not Ok' END as statussales,
        b.tglEod as tgleod,
        b.tglUpload as tglupload,
        CASE 
            WHEN d.date_1 < c.date_2 THEN CONCAT(CONVERT(DECIMAL(18, 0), (d.date_1 / NULLIF(c.date_2, 0)) * 100), ' %')
            WHEN d.date_1 >= c.date_2 THEN '100 %'
            ELSE '0 %'
        END as persentaseuploadstok,
        d.max_upload as maxupload 
    FROM PB_DC.dbo.MstToko a 
    LEFT JOIN (
				SELECT kodetoko, tglbisnis, tglEod, tglUpload 
					FROM PB_DC.dbo.tbltglbisnis 
				WHERE tglbisnis = @date1
			) b ON b.kodeToko = a.kodeToko
    LEFT JOIN (
				SELECT kodetoko, CONVERT(DECIMAL(18, 2), COUNT(*)) as date_2 
					FROM PB_DC.dbo.SaldoStokProdukToko 
				WHERE tglSaldo = @date2
				GROUP BY kodetoko
			) c ON c.kodeToko = a.kodeToko 
    LEFT JOIN (
				SELECT kodetoko, CONVERT(DECIMAL(18, 2), COUNT(*)) as date_1, MAX(tglUpload) max_upload 
					FROM PB_DC.dbo.SaldoStokProdukToko 
				WHERE tglSaldo = @date1
				GROUP BY kodetoko
			) d ON d.kodeToko = a.kodeToko
	LEFT JOIN PB_DC.dbo.MasterToolsToko e on e.kodetoko=a.kodetoko
    LEFT JOIN (
			SELECT 
				a.kodetoko,
				totalHeader,
				totalDetail,
				totalRekapHeader
			FROM (
					-- Sales Header
					SELECT a.kodetoko, tglbisnis, SUM(totalRpPenjualan) totalHeader 
						FROM PB_DC.dbo.TransaksiTokoHeader a WITH (NOLOCK)
					WHERE tglBisnis = @date1
					GROUP BY a.kodetoko, tglbisnis
				) a
			LEFT JOIN (
						-- Sales Detail
						SELECT b.kodetoko, tglbisnis, SUM(subtotal) totalDetail 
							FROM PB_DC.dbo.TransaksiTokoHeader a WITH (NOLOCK) 
							JOIN PB_DC.dbo.TransaksiTokoDetail b WITH (NOLOCK) ON b.kodetoko = a.kodetoko AND b.nomorTransaksi = a.nomorTransaksi 
						WHERE tglBisnis = @date1 AND idJenisProduk <> 4 AND statusProduk <> 'K'
						GROUP BY b.kodetoko, tglbisnis 
					) b ON b.kodeToko = a.kodeToko AND b.tglBisnis = a.tglBisnis
			LEFT JOIN (
						-- Rekap Header
						SELECT kodetoko, TglPembukuanTransaksi, TotalRpTransaksi totalRekapHeader 
							FROM PB_DC.dbo.RekapTransaksiTokoHeader WITH (NOLOCK) 
						WHERE tglpembukuanTransaksi = @date1
					) c ON c.KodeToko = a.kodeToko AND c.TglPembukuanTransaksi = a.tglBisnis
				) f ON f.kodetoko = a.kodetoko
    LEFT JOIN PB_DC.dbo.v_smi_pivot_area_toko g ON g.kodetoko = a.kodetoko
    WHERE a.statusData = 1 
        AND a.tglBuka <= CONVERT(DATE, GETDATE()) 
        AND e.idcabang = @idcabang
    ORDER BY a.kodetoko, persentaseuploadstok;
END
;
GO



----SP
-- Second SP;
DECLARE @yesterday DATE = DATEADD(DAY, -1, GETDATE())
DECLARE @thedayaftertomorrow DATE = DATEADD(DAY, -2, GETDATE())
EXEC GetSMIStoreEOD 6, @yesterday, @thedayaftertomorrow;
-- First SP;
-- DECLARE @yesterday DATE = DATEADD(DAY, -1, GETDATE());
-- EXEC GetSMIStoreEOD 6, @yesterday, @yesterday;



----TABLE POSTGRESQL
-- public.smi_monitoring_eod_toko definition

-- Drop table

-- DROP TABLE public.smi_monitoring_eod_toko;

CREATE TABLE public.smi_monitoring_eod_toko (
	insertdate timestamptz DEFAULT now() NOT NULL,
	tglbisnis date NULL,
	idcabang int4 NULL,
	kodetoko int8 NULL,
	namatoko varchar(100) NULL,
	nikrh varchar(20) NULL,
	regional varchar(30) NULL,
	nikac varchar(20) NULL,
	area varchar(30) NULL,
	statussales varchar(6) NULL,
	tgleod timestamp NULL,
	tgluploadeod timestamp NULL,
	persentaseuploadstok varchar(43) NULL,
	jamuploadterakhir timestamp NULL,
	CONSTRAINT smi_monitoring_eod_toko_unique UNIQUE (tglbisnis, kodetoko)
);



select * from public.smi_monitoring_eod_toko where nikrh='160600475';

--delete from public.smi_monitoring_eod_toko where tglbisnis='2025-08-24'

--truncate table public.smi_monitoring_eod_toko;



--Robotic Jobs Query template
--EOD Periode
SELECT
concat(min(a.tglbisnis),' ','s/d',' ',max(a.tglbisnis)) as periode
FROM public.smi_monitoring_eod_toko a
WHERE ((to_char(current_date, 'DD') = '01' AND a.tglbisnis BETWEEN date_trunc('month', current_date - interval '1 month') AND (date_trunc('month', current_date) - interval '1 day'))
OR (to_char(current_date, 'DD') <> '01' AND a.tglbisnis BETWEEN date_trunc('month', current_date) AND current_date))
limit 1;

--EOD Data
SELECT a.tglbisnis, a.kodetoko, a.namatoko, a.regional, a.area, a.statussales, a.tgleod, a.tgluploadeod, a.persentaseuploadstok, a.jamuploadterakhir
FROM public.smi_monitoring_eod_toko a
WHERE ((to_char(current_date, 'DD') = '01' AND a.tglbisnis BETWEEN date_trunc('month', current_date - interval '1 month') AND (date_trunc('month', current_date) - interval '1 day'))
OR (to_char(current_date, 'DD') <> '01' AND a.tglbisnis BETWEEN date_trunc('month', current_date) AND current_date))
AND a.idcabang='6'
AND a.nikrh='140900427'--JAJA
ORDER BY a.tglbisnis DESC;

--Robotic Jobs Query template :
	--BDG
	/ EOD Periode						EOD Periode
	/ EOD Data RH-INDARKO BAYU SAPUTRO	EOD Data RH-INDARKO BAYU SAPUTRO
	/ EOD Data RH-JAJA					EOD Data RH-JAJA
	/ EOD Data RH-TEDI SUTEDI			EOD Data RH-TEDI SUTEDI
	/ EOD Data RH-HEVY NEVALIS			EOD Data RH-HEVY NEVALIS
	/ EOD Data RH-HENGKI DWI SETIYANTO	EOD Data RH-HENGKI DWI SETIYANTO

	
--Robotic Jobs Template Sheet
	--BDG
	EOD Data RH-INDARKO BAYU SAPUTRO	EOD Data RH-INDARKO BAYU SAPUTRO / - / EOD Data RH-INDARKO BAYU SAPUTRO		EOD Data RH-INDARKO BAYU SAPUTRO	
	EOD Data RH-JAJA					EOD Data RH-JAJA / - / EOD Data RH-JAJA										EOD Data RH-JAJA	
	EOD Data RH-TEDI SUTEDI				EOD Data RH-TEDI SUTEDI / - / EOD Data RH-TEDI SUTEDI						EOD Data RH-TEDI SUTEDI	
	EOD Data RH-HEVY NEVALIS			EOD Data RH-HEVY NEVALIS / - / EOD Data RH-HEVY NEVALIS						EOD Data RH-HEVY NEVALIS	
	EOD Data RH-HENGKI DWI SETIYANTO	EOD Data RH-HENGKI DWI SETIYANTO / - / EOD Data RH-HENGKI DWI SETIYANTO		EOD Data RH-HENGKI DWI SETIYANTO

--Robotic Jobs
	--BDG
	Report Monitoring EOD RH INDARKO BAYU SAPUTRO	Robotic Report Mailer	Report Monitoring EOD MTD H-1 Per RH INDARKO BAYU SAPUTRO	OK
	Report Monitoring EOD RH JAJA					Robotic Report Mailer	Report Monitoring EOD MTD H-1 Per RH JAJA					OK
	Report Monitoring EOD RH TEDI SUTEDI			Robotic Report Mailer	Report Monitoring EOD MTD H-1 Per RH TEDI SUTEDI			OK
	Report Monitoring EOD RH HEVY NEVALIS			Robotic Report Mailer	Report Monitoring EOD MTD H-1 Per RH HEVY NEVALIS			OK
	Report Monitoring EOD RH HENGKI DWI SETIYANTO	Robotic Report Mailer	Report Monitoring EOD MTD H-1 Per RH HENGKI DWI SETIYANTO	OK


