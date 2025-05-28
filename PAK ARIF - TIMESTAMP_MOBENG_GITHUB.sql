--------------------------------------------------------------------------------------------------------------------------SQL SERVER


--RMS20
--FUNCTION RMB
--select * from [MB_DB_Reporting_SBY].[dbo].[MB_Transaksi_RMB] ('2024-01-01','2024-02-29') where rmb=1;

USE MB_DB_Reporting_SBY;
GO

CREATE FUNCTION [dbo].[MB_Transaksi_RMB]
(
    @date1 DATE,
    @date2 DATE
)
RETURNS TABLE 
AS
RETURN
(
    SELECT  
        x3.tanggal, 
        x3.namacabang, 
        x3.kodetoko, 
        x3.kodetokolama, 
        x3.namatoko, 
        x3.nomortransaksi, 
        SUM(x3.olimesin) AS olimesin,
        SUM(x3.aicu) AS aicu,
        SUM(x3.pcu) AS pcu,
        SUM(x3.tbic) AS tbic,
        CASE 
            WHEN SUM(x3.olimesin) > 0 
              AND SUM(x3.aicu) > 0 
              AND SUM(x3.pcu) > 0 
              AND SUM(x3.tbic) > 0 
            THEN 1 
            ELSE 0 
        END AS rmb
    FROM 
    (
        SELECT  
            x2.tanggal, 
            x2.namacabang, 
            x2.kodetoko, 
            x2.kodetokolama, 
            x2.namatoko, 
            x2.nomortransaksi,
            SUM(x2.olimesin) AS olimesin,
            SUM(x2.aicu) AS aicu,
            SUM(x2.pcu) AS pcu,
            SUM(x2.tbic) AS tbic
        FROM 
        (
            SELECT  
                x1.tanggal, 
                x1.namacabang, 
                x1.kodetoko, 
                x1.kodetokolama, 
                x1.namatoko, 
                x1.nomortransaksi,
                CASE 
                    WHEN x1.idjenisproduk <> 4 
                     AND x1.idsubdepartement = 1 
                     AND x1.iddepartement = 1 
                    THEN x1.qty 
                    ELSE 0 
                END AS olimesin,
                CASE 
                    WHEN x1.idjenisproduk <> 4 
                     AND x1.idsubdepartement = 10 
                    THEN x1.qty 
                    ELSE 0 
                END AS aicu,
                CASE 
                    WHEN x1.idjenisproduk <> 4 
                     AND x1.idsubdepartement = 11 
                    THEN x1.qty 
                    ELSE 0 
                END AS pcu,
                CASE 
                    WHEN x1.idjenisproduk <> 4 
                     AND x1.idsubdepartement = 12 
                    THEN x1.qty 
                    ELSE 0 
                END AS tbic
            FROM 
                MB_DB_Reporting_SBY.dbo.mb_rms20_transaksi_toko_perjenis_member_v3 AS x1
            WHERE 
                x1.tanggal BETWEEN @date1 AND @date2
                AND x1.statusproduk <> 'K'
                AND x1.namacabang = 'Surabaya'
        ) AS x2
        GROUP BY 
            x2.tanggal, 
            x2.namacabang, 
            x2.kodetoko, 
            x2.kodetokolama, 
            x2.namatoko, 
            x2.nomortransaksi
    ) AS x3
    GROUP BY 
        x3.tanggal, 
        x3.namacabang, 
        x3.kodetoko, 
        x3.kodetokolama, 
        x3.namatoko, 
        x3.nomortransaksi
);
GO



--------------------------------------------------------------------------------------------------------------------------


--RMS20
--TABLE TEMP
--DROP TABLE MB_DB_Reporting_SBY.dbo.MB_Transaksi_Timestamp;

CREATE TABLE MB_DB_Reporting_SBY.dbo.MB_Transaksi_Timestamp (
insert_date datetime NOT NULL,
transaction_date date NOT NULL,
storecode BIGINT NOT NULL,
storename VARCHAR(100) NOT NULL,
nopol VARCHAR(15) NOT NULL,
notrans BIGINT NOT NULL,
create_transaction datetime NULL,
trans_last_km INT NULL,
input_checkup_awal datetime NULL,
instalasi_time datetime NULL,
input_checkup_akhir datetime NULL,
pos_start_transaction_time datetime NULL,
print_struk datetime NULL
);


--------------------------------------------------------------------------------------------------------------------------


--RMS20
--FUNTION INSERT
SELECT * FROM dbo.MB_Trx_Timestamp('2025-05-01', '2025-05-01');

USE MB_DB_Reporting_SBY;
GO

CREATE FUNCTION dbo.MB_Trx_Timestamp
(
    @date1 DATE,
    @date2 DATE
)
RETURNS TABLE
AS
RETURN
(
    SELECT
		CONVERT(SMALLDATETIME, GETDATE()) AS insertdate,
        a.tglbisnis AS transaction_date,
        a.kodetoko AS storecode,
        d.namatoko AS storename,
        b.nopolisi AS nopol,
        b.nomortransaksi AS notrans,
        f.createdate AS Create_Transaction,
        f.trans_last_km,
        f.check_time AS Input_CheckUp_Awal,
        f.instalasi_time,
        f.finish_time AS Input_CheckUp_Akhir,
        g.trans_status_date AS POS_Start_Transaction_time,
        a.tglbayar AS Print_Struk
    FROM mb_dc_sby.dbo.transaksitokoheader AS a WITH (NOLOCK)
    JOIN mb_dc_sby.dbo.smitransaksitokoperjenismember AS b WITH (NOLOCK)
        ON b.kodetoko = a.kodetoko AND b.NomorTransaksi = a.nomorTransaksi
    JOIN mb_dc_sby.dbo.msttoko AS d WITH (NOLOCK)
        ON d.kodetoko = a.kodetoko
    LEFT JOIN mb_db_reporting_sby.dbo.mb_transaksi_rmb(@date1, @date2) AS e
        ON e.kodetoko = a.kodetoko AND e.nomortransaksi = a.nomorTransaksi
    LEFT JOIN 
    (
        SELECT 
            *,
            ROW_NUMBER() OVER (PARTITION BY kode_toko, trans_id ORDER BY trans_date DESC) AS myrank
        FROM mb_dc_sby.dbo.imen_trans_header WITH (NOLOCK)
        WHERE 
            CONVERT(DATE, trans_date) BETWEEN @date1 AND @date2
            AND trans_status = 1000000004
    ) AS f
        ON f.myrank = 1 
        AND CONVERT(DATE, f.trans_date) = CONVERT(DATE, a.tglbisnis)
        AND f.kode_toko = a.kodetoko 
        AND f.trans_id = a.nomorImen
        AND f.trans_status = 1000000004
    LEFT JOIN 
    (
        SELECT 
            *,
            ROW_NUMBER() OVER (PARTITION BY kode_toko, trans_id ORDER BY trans_status_date DESC) AS myrank
        FROM mb_dc_sby.dbo.Imen_Trans_Status_Hist WITH (NOLOCK)
        WHERE 
            CONVERT(DATE, trans_status_date) BETWEEN @date1 AND @date2
            AND trans_status = 1000000004
    ) AS g
        ON g.myrank = 1
        AND CONVERT(DATE, g.trans_status_date) = CONVERT(DATE, f.trans_date)
        AND g.kode_toko = a.kodetoko
        AND g.trans_id = a.nomorImen
        AND g.trans_id = f.trans_id
        AND g.trans_status = 1000000004
    WHERE 
        CONVERT(DATE, a.tglbisnis) BETWEEN @date1 AND @date2
        --AND e.rmb = 1 -- Optional filter
    GROUP BY
        a.tglbisnis, a.kodetoko, d.namatoko, b.nopolisi, b.nomortransaksi,
        f.createdate, f.trans_last_km, f.check_time, f.instalasi_time, 
        f.finish_time, g.trans_status_date, a.tglbayar
);
GO


--------------------------------------------------------------------------------------------------------------------------


--RMS20
--INSERT
--SELECT * FROM MB_DB_Reporting_SBY.dbo.MB_Transaksi_Timestamp;

DECLARE @date1 DATE=convert(date,DATEADD(MONTH, DATEDIFF(MONTH, 0, GETDATE())-1, 0)) -- periode awal data M-1
DECLARE @date2 DATE=convert(date,dateadd(d,-(day(getdate())),getdate()))	-- periode akhir data M-1

INSERT INTO MB_DB_Reporting_SBY.dbo.MB_Transaksi_Timestamp
SELECT * FROM dbo.MB_Trx_Timestamp(@date1, @date2);


--------------------------------------------------------------------------------------------------------------------------POSTGRESQL

-- MB_RMS20_RPT
-- FOREIGN TABLE
-- SELECT * FROM mb_rms20_rpt.mb_transaksi_timestamp definition
-- Drop table
-- DROP FOREIGN TABLE mb_rms20_rpt.mb_transaksi_timestamp;

CREATE FOREIGN TABLE mb_rms20_rpt.mb_transaksi_timestamp (
	insert_date timestamp OPTIONS(column_name 'insert_date') NOT NULL,
	transaction_date date OPTIONS(column_name 'transaction_date') NOT NULL,
	storecode int8 OPTIONS(column_name 'storecode') NOT NULL,
	storename varchar(100) OPTIONS(column_name 'storename') NOT NULL,
	nopol varchar(15) OPTIONS(column_name 'nopol') NOT NULL,
	notrans int8 OPTIONS(column_name 'notrans') NOT NULL,
	create_transaction timestamp OPTIONS(column_name 'create_transaction') NULL,
	trans_last_km int4 OPTIONS(column_name 'trans_last_km') NULL,
	input_checkup_awal timestamp OPTIONS(column_name 'input_checkup_awal') NULL,
	instalasi_time timestamp OPTIONS(column_name 'instalasi_time') NULL,
	input_checkup_akhir timestamp OPTIONS(column_name 'input_checkup_akhir') NULL,
	pos_start_transaction_time timestamp OPTIONS(column_name 'pos_start_transaction_time') NULL,
	print_struk timestamp OPTIONS(column_name 'print_struk') NULL
)
SERVER mobeng_rms20_rpt
OPTIONS (schema_name 'dbo', table_name 'MB_Transaksi_Timestamp');

