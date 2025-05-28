--------------------------------------------------------------------------------------------------------------------------SQL SERVER

--RMS10
--FUNCTION
--select * from [MB_DB_Reporting_JKT].[dbo].[mb_transaksi_rmb] ('2025-02-01','2025-02-23') where rmb=1;

--USE MB_DB_Reporting_JKT;
--CREATE FUNCTION [dbo].[mb_transaksi_rmb](
--				@date1 date,
--				@date2 date
--				)
--
--RETURNS TABLE 
--AS
--RETURN (
--
--SELECT  x3.tanggal, 
--        x3.namacabang, 
--        x3.kodetoko, 
--        x3.kodetokolama, 
--        x3.namatoko, 
--        x3.nomortransaksi, 
--        SUM(x3.olimesin) AS olimesin,
--        SUM(x3.aicu) AS aicu,
--        SUM(x3.pcu) AS pcu,
--        SUM(x3.tbic) AS tbic,
--		CASE WHEN SUM(x3.olimesin)>0 and SUM(x3.aicu)>0 and SUM(x3.pcu)>0 and SUM(x3.tbic)>0 THEN 1
--		ELSE 0 END AS rmb
--FROM (
--    SELECT  x2.tanggal, 
--            x2.namacabang, 
--            x2.kodetoko, 
--            x2.kodetokolama, 
--            x2.namatoko, 
--            x2.nomortransaksi,
--            SUM(x2.olimesin) AS olimesin,
--            SUM(x2.aicu) AS aicu,
--            SUM(x2.pcu) AS pcu,
--            SUM(x2.tbic) AS tbic
--    FROM (
--        SELECT  x1.tanggal, 
--                x1.namacabang, 
--                x1.kodetoko, 
--                x1.kodetokolama, 
--                x1.namatoko, 
--                x1.nomortransaksi,
--                CASE WHEN x1.idjenisproduk <> 4 AND x1.idsubdepartement = 1 AND x1.iddepartement = 1 THEN x1.qty ELSE 0 END AS olimesin,
--                CASE WHEN x1.idjenisproduk <> 4 AND x1.idsubdepartement = 10 THEN x1.qty ELSE 0 END AS aicu,
--                CASE WHEN x1.idjenisproduk <> 4 AND x1.idsubdepartement = 11 THEN x1.qty ELSE 0 END AS pcu,
--                CASE WHEN x1.idjenisproduk <> 4 AND x1.idsubdepartement = 12 THEN x1.qty ELSE 0 END AS tbic
--        FROM mb_rms10_transaksi_toko_perjenis_member_v3 AS x1
--        WHERE x1.tanggal between @date1 AND @date2
--        AND x1.statusproduk <> 'K'
--        AND x1.namacabang IN ('Jakarta Baru') -- Filter data closer to the source
--        ) AS x2
--    GROUP BY x2.tanggal, x2.namacabang, x2.kodetoko, x2.kodetokolama, x2.namatoko, x2.nomortransaksi
--    ) AS x3
--GROUP BY x3.tanggal, x3.namacabang, x3.kodetoko, x3.kodetokolama, x3.namatoko, x3.nomortransaksi
--
--	);
--GO





--RMS10
USE MB_DC;
DECLARE @date1 date='2025-04-01'
DECLARE @date2 date='2025-04-30'

Select
	a.tglbisnis as transaction_date,
	--b.namacabang as branch,
	a.kodetoko as storecode,
	d.namatoko as storename,
	b.nopolisi as nopol,
	b.nomortransaksi as notrans,
	f.createdate as Create_Transaction, --ambil dr imen_trans_header (transatatus=1000000004)
	f.trans_last_km,
	f.check_time as Input_CheckUp_Awal,
	f.instalasi_time,
	f.finish_time as Input_CheckUp_Akhir,			
	g.trans_status_date as POS_Start_Transaction_time, -- posting (transatatus=1000000004) ambil dr Imen_Trans_Status_Hist
	a.tglbayar as Print_Struk
From dbo.transaksitokoheader as a with (NOLOCK)
join dbo.smitransaksitokoperjenismember as b with (NOLOCK) on b.kodetoko=a.kodetoko and b.NomorTransaksi=a.nomorTransaksi
--join dbo.transaksitokodetail as c with (NOLOCK) on c.kodeToko=a.kodetoko and c.nomorTransaksi=a.NomorTransaksi
join dbo.msttoko as d with (NOLOCK) on d.kodetoko=a.kodeToko
left join mb_db_reporting_jkt.dbo.mb_transaksi_rmb(@date1,@date2) as e on e.kodetoko=a.kodetoko and e.NomorTransaksi=a.nomorTransaksi
--and year(a.tglbisnis)=c.tahun and month(a.tglbisnis)=c.bulan
left join	(
			Select 
				row_number() OVER (PARTITION BY kode_toko,trans_id ORDER BY trans_date desc) as myrank,*
			From dbo.imen_trans_header with (NOLOCK)
			where 
			--convert(date,trans_date) between '2023-11-01' and '2023-11-01'
			convert(date,trans_date) between @date1 and @date2 
			and trans_status=1000000004
			) as f on
		f.myrank=1 and 
		convert(date,f.trans_date)=convert(date,a.tglbisnis) 
		and f.kode_toko=a.kodetoko 
		--and f.trans_nopol_all=b.nopolisi 
		and f.trans_id=a.nomorImen
		and f.trans_status=1000000004				
left join	(
			select 
			row_number() OVER (PARTITION BY kode_toko,trans_id ORDER BY trans_status_date desc) as myrank,*
			from dbo.Imen_Trans_Status_Hist with (NOLOCK)
			where 
			--convert(date,trans_status_date) between '2023-11-01' and '2023-11-01'
			convert(date,trans_status_date) between @date1 and @date2
			and trans_status=1000000004
			) as g on
		g.myrank=1 and 
		convert(date,g.trans_status_date)=convert(date,f.trans_date)
		and g.kode_toko=a.kodetoko 
		and g.trans_id=a.nomorImen 
		and g.trans_id=f.trans_id
		and g.trans_status=1000000004
WHERE convert(date,a.tglbisnis) between @date1 and @date2
--AND e.rmb=1
GROUP BY
	a.tglbisnis,a.kodetoko,d.namatoko,b.nopolisi,b.nomortransaksi,f.createdate,
	f.trans_last_km,f.check_time,f.instalasi_time,f.finish_time,g.trans_status_date,a.tglbayar;


--select * from [MB_DB_Reporting_JKT].[dbo].[mb_transaksi_rmb] ('2024-02-01','2025-02-23') where kodetoko=3021001 and nomortransaksi=202402010018





--RMS20
--FUNCTION
--select * from [MB_DB_Reporting_SBY].[dbo].[mb_transaksi_rmb] ('2024-01-01','2024-02-29') where rmb=1;

--USE MB_DB_Reporting_SBY;
--CREATE FUNCTION [dbo].[mb_transaksi_rmb](
--				@date1 date,
--				@date2 date
--				)
--
--RETURNS TABLE 
--AS
--RETURN (
--
--SELECT  x3.tanggal, 
--        x3.namacabang, 
--        x3.kodetoko, 
--        x3.kodetokolama, 
--        x3.namatoko, 
--        x3.nomortransaksi, 
--        SUM(x3.olimesin) AS olimesin,
--        SUM(x3.aicu) AS aicu,
--        SUM(x3.pcu) AS pcu,
--        SUM(x3.tbic) AS tbic,
--		CASE WHEN SUM(x3.olimesin)>0 and SUM(x3.aicu)>0 and SUM(x3.pcu)>0 and SUM(x3.tbic)>0 THEN 1
--		ELSE 0 END AS rmb
--FROM (
--    SELECT  x2.tanggal, 
--            x2.namacabang, 
--            x2.kodetoko, 
--            x2.kodetokolama, 
--            x2.namatoko, 
--            x2.nomortransaksi,
--            SUM(x2.olimesin) AS olimesin,
--            SUM(x2.aicu) AS aicu,
--            SUM(x2.pcu) AS pcu,
--            SUM(x2.tbic) AS tbic
--    FROM (
--        SELECT  x1.tanggal, 
--                x1.namacabang, 
--                x1.kodetoko, 
--                x1.kodetokolama, 
--                x1.namatoko, 
--                x1.nomortransaksi,
--                CASE WHEN x1.idjenisproduk <> 4 AND x1.idsubdepartement = 1 AND x1.iddepartement = 1 THEN x1.qty ELSE 0 END AS olimesin,
--                CASE WHEN x1.idjenisproduk <> 4 AND x1.idsubdepartement = 10 THEN x1.qty ELSE 0 END AS aicu,
--                CASE WHEN x1.idjenisproduk <> 4 AND x1.idsubdepartement = 11 THEN x1.qty ELSE 0 END AS pcu,
--                CASE WHEN x1.idjenisproduk <> 4 AND x1.idsubdepartement = 12 THEN x1.qty ELSE 0 END AS tbic
--        FROM mb_rms20_transaksi_toko_perjenis_member_v3 AS x1
--        WHERE x1.tanggal between @date1 AND @date2
--        AND x1.statusproduk <> 'K'
--        AND x1.namacabang IN ('Surabaya') -- Filter data closer to the source
--        ) AS x2
--    GROUP BY x2.tanggal, x2.namacabang, x2.kodetoko, x2.kodetokolama, x2.namatoko, x2.nomortransaksi
--    ) AS x3
--GROUP BY x3.tanggal, x3.namacabang, x3.kodetoko, x3.kodetokolama, x3.namatoko, x3.nomortransaksi
--
--	);
--GO





--RMS20
USE MB_DC_SBY;
DECLARE @date1 date='2025-04-01'
DECLARE @date2 date='2025-04-30'

Select
	a.tglbisnis as transaction_date,
	--b.namacabang as branch,
	a.kodetoko as storecode,
	d.namatoko as storename,
	b.nopolisi as nopol,
	b.nomortransaksi as notrans,
	f.createdate as Create_Transaction, --ambil dr imen_trans_header (transatatus=1000000004)
	f.trans_last_km,
	f.check_time as Input_CheckUp_Awal,
	f.instalasi_time,
	f.finish_time as Input_CheckUp_Akhir,			
	g.trans_status_date as POS_Start_Transaction_time, -- posting (transatatus=1000000004) ambil dr Imen_Trans_Status_Hist
	a.tglbayar as Print_Struk
From dbo.transaksitokoheader as a with (NOLOCK)
join dbo.smitransaksitokoperjenismember as b with (NOLOCK) on b.kodetoko=a.kodetoko and b.NomorTransaksi=a.nomorTransaksi
--join dbo.transaksitokodetail as c with (NOLOCK) on c.kodeToko=a.kodetoko and c.nomorTransaksi=a.NomorTransaksi
join dbo.msttoko as d with (NOLOCK) on d.kodetoko=a.kodeToko
left join mb_db_reporting_sby.dbo.mb_transaksi_rmb(@date1,@date2) as e on e.kodetoko=a.kodetoko and e.NomorTransaksi=a.nomorTransaksi
--and year(a.tglbisnis)=c.tahun and month(a.tglbisnis)=c.bulan
left join	(
			Select 
				row_number() OVER (PARTITION BY kode_toko,trans_id ORDER BY trans_date desc) as myrank,*
			From dbo.imen_trans_header with (NOLOCK)
			where 
--			convert(date,trans_date) between '2025-02-01' and '2025-02-28'
			convert(date,trans_date) between @date1 and @date2 
			and trans_status=1000000004
			) as f on
		f.myrank=1 and 
		convert(date,f.trans_date)=convert(date,a.tglbisnis) 
		and f.kode_toko=a.kodetoko 
		--and f.trans_nopol_all=b.nopolisi 
		and f.trans_id=a.nomorImen
		and f.trans_status=1000000004				
left join	(
			select 
			row_number() OVER (PARTITION BY kode_toko,trans_id ORDER BY trans_status_date desc) as myrank,*
			from dbo.Imen_Trans_Status_Hist with (NOLOCK)
			where 
			--convert(date,trans_status_date) between '2025-03-01' and '2025-03-31'
			convert(date,trans_status_date) between @date1 and @date2
			and trans_status=1000000004
			) as g on
		g.myrank=1 and 
		convert(date,g.trans_status_date)=convert(date,f.trans_date)
		and g.kode_toko=a.kodetoko 
		and g.trans_id=a.nomorImen 
		and g.trans_id=f.trans_id
		and g.trans_status=1000000004
WHERE convert(date,a.tglbisnis) between @date1 and @date2
--AND e.rmb=1
GROUP BY
	a.tglbisnis,a.kodetoko,d.namatoko,b.nopolisi,b.nomortransaksi,f.createdate,
	f.trans_last_km,f.check_time,f.instalasi_time,f.finish_time,g.trans_status_date,a.tglbayar;



--select * from [MB_DB_Reporting_SBY].[dbo].[mb_transaksi_rmb] ('2024-02-01','2024-02-29') where kodetoko=3031001 and nomortransaksi=202402220007;