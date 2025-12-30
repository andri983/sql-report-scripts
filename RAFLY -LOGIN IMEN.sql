-------------------------
-- ROBOTIC - JOB QUERY --
-------------------------
SELECT 
idcabang as "ID Cabang", 
kodetoko as "Kode Toko", 
namatoko as "Nama Toko", 
tglsales as "Tgl Sales", 
timelogin as "Time Login", 
jambukatoko as "Jam Buka Toko", 
kesesuaian as "Kesesuaian"
FROM public.smi_monitoring_login_toko
WHERE idcabang = 2
AND tglsales = current_date;

-----------------------------
-- ROBOTIC - TRANSFER DATA --
-----------------------------
CREATE TABLE public.smi_monitoring_login_toko (
	insertdate timestamptz DEFAULT now() NOT NULL,
	idcabang int4 NULL,
	kodetoko int8 NULL,
	namatoko varchar(100) NULL,
	tglsales date NULL,
	timelogin varchar(20) NULL,
	jambukatoko varchar(20) NULL,
	kesesuaian varchar(25) NULL,
	CONSTRAINT smi_monitoring_login_toko_unique UNIQUE (idcabang,kodetoko,tglsales)
);

SELECT * FROM public.smi_monitoring_login_toko;


-------------------
-- RMS - SP EXEC --
-------------------
EXEC GetSMIStoreLogin;





-------------------------------
-- RMS - SP GetSMIStoreLogin --
-------------------------------
CREATE PROCEDURE GetSMIStoreLogin
AS
BEGIN
    SET NOCOUNT ON;

    SELECT 
    	idcabang,
        kodetoko,
        namatoko,
        CONVERT(DATE, tglproses) as tglsales,
        CONVERT(VARCHAR, CONVERT(TIME, tglProses), 108) as timelogin,
        CONVERT(VARCHAR,(jam_buka_toko), 108) as jambukatoko,
        CASE 
        WHEN jam_buka_toko IS NULL THEN 'Unknown'
        WHEN DATEDIFF(MINUTE, CONVERT(TIME, tglProses), jam_buka_toko) >= 30 THEN 'OK - Tepat Waktu'
        WHEN DATEDIFF(MINUTE, CONVERT(TIME, tglProses), jam_buka_toko) >= 0 THEN 'Not OK - Sebelum Jam Buka'
        ELSE 'Not OK - Setelah Jam Buka'
    END AS status
    FROM (
        SELECT 
            ROW_NUMBER() OVER (PARTITION BY a.kodetoko ORDER BY a.kodetoko, tglproses ASC) AS myRank,
            a.*,
            c.namatoko,
            d.idcabang,
            e.jam_buka_toko
        FROM PB_DC.DBO.SMITblHistoryInOut a 
        JOIN PB_DC.DBO.MstToko c ON c.kodetoko = a.kodetoko
        JOIN PB_DC.dbo.MasterToolsToko d ON d.kodetoko = c.kodeToko 
        LEFT JOIN [RMS01.PLANETBAN.CO.ID].PB_HO.DBO.smi_mst_jam_opr_toko e ON e.kode_toko = a.kodetoko
        WHERE CONVERT(DATE, tglproses) = CONVERT(DATE, GETDATE())
            AND statusProses = 'IN'
    ) b
    WHERE myRank = 1
    ORDER BY kodetoko ASC;
END;





------------------------------------------------
-- JIKA MENGGUNAKAN FDW --
------------------------------------------------
-- Pertahankan foreign table dengan timestamp --
------------------------------------------------
DROP FOREIGN TABLE IF EXISTS smi_rms01_pbho.smi_mst_jam_opr_toko;
CREATE FOREIGN TABLE IF NOT EXISTS smi_rms01_pbho.smi_mst_jam_opr_toko (
    kode_toko bigint OPTIONS (column_name 'kode_toko'),
    nama_toko character varying(100) OPTIONS (column_name 'nama_toko'),
    jam_buka_toko timestamp without time zone OPTIONS (column_name 'jam_buka_toko'),
    jam_tutup_toko timestamp without time zone OPTIONS (column_name 'jam_tutup_toko'),
    tglcreate timestamp without time zone OPTIONS (column_name 'tglcreate'),
    tglupdate timestamp without time zone OPTIONS (column_name 'tglupdate')
)
SERVER rms01_pb_ho
OPTIONS (schema_name 'dbo', table_name 'smi_mst_jam_opr_toko');





----------------------------
-- Buat Materialized View --
----------------------------
DROP MATERIALIZED VIEW smi_rms01_pbho.mv_jam_opr_toko;
CREATE MATERIALIZED VIEW smi_rms01_pbho.mv_jam_opr_toko AS
SELECT 
    kode_toko,
    nama_toko,
    jam_buka_toko::time as jam_buka_toko,
    jam_tutup_toko::time as jam_tutup_toko,
    tglcreate,
    tglupdate
FROM smi_rms01_pbho.smi_mst_jam_opr_toko;





-------------------------------------
-- Buat index untuk performa query --
-------------------------------------
CREATE UNIQUE INDEX CONCURRENTLY mv_jam_opr_toko_pk 
ON smi_rms01_pbho.mv_jam_opr_toko (kode_toko);

CREATE INDEX CONCURRENTLY mv_jam_opr_toko_jam_buka_idx 
ON smi_rms01_pbho.mv_jam_opr_toko (jam_buka_toko);

CREATE INDEX CONCURRENTLY mv_jam_opr_toko_jam_tutup_idx 
ON smi_rms01_pbho.mv_jam_opr_toko (jam_tutup_toko);


SELECT * FROM smi_rms01_pbho.mv_jam_opr_toko;

