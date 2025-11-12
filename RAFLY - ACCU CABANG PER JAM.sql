-------------------------------
-----STORE PROCEDURE (RMS)-----
-------------------------------
USE [SMI_DB_Reporting_JKT]
GO
/****** Object:  StoredProcedure [dbo].[GetSMIPraStatusAccuAllHours]    Script Date: 11/11/2025 9:09:01 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO



CREATE PROCEDURE [dbo].[GetSMIPraStatusAccuAllHours]
AS
BEGIN
    SET NOCOUNT ON;

    WITH cteMotor AS (
        SELECT
            m.nopolisi,
            bm.brandMotor,
            tm.typeMotor,
            vm.varianMotor,
            tmotor.tahunMotor,
            jm.jenisMotor,
            tmotor.flagStarter
        FROM PB_DC.dbo.mstmember AS m WITH (NOLOCK)
        LEFT JOIN PB_DC.dbo.SMIMstTahunMotor AS tmotor WITH (NOLOCK)
            ON tmotor.idTahunMotor = m.idTahunMotor
        LEFT JOIN PB_DC.dbo.SMIMstTypeMotor AS tm WITH (NOLOCK)
            ON tm.idTypeMotor = m.idTypeMotor
        LEFT JOIN PB_DC.dbo.SMIMstBrandMotor AS bm WITH (NOLOCK)
            ON bm.idBrandMotor = tm.idBrandMotor
        LEFT JOIN PB_DC.dbo.SMIMstVarianMotor AS vm WITH (NOLOCK)
            ON vm.idVarianMotor = tmotor.idVarianMotor 
            AND vm.idTypeMotor = m.idTypeMotor
        LEFT JOIN PB_DC.dbo.SMIMstJenisMotor AS jm WITH (NOLOCK)
            ON jm.idJenisMotor = tm.idJenisMotor
    ),
    cteTransaksi AS (
        SELECT 
			a.tglBisnis,
			a.tglBayar,
			c.idcabang,
			h.NamaCabang,
			d.nopolisi,
			e.namaMember,
			e.notelp,
			g.idJenisMember,
			g.namaJenisMember,
			a.kodeToko,
			a.nomorTransaksi,
			a.nomorImen,
            CASE
                WHEN b.trans_polutan_awal = '1' THEN 'BAGUS Diatas 12.5 V'
                WHEN b.trans_polutan_awal = '2' THEN 'LEMAH Antara 12.2 - 12.4 V'
                WHEN b.trans_polutan_awal = '3' THEN 'RUSAK Dibawah 12.2 V'
                WHEN b.trans_polutan_awal = '4' THEN 'Cust. Tolak Cek Voltase'
                WHEN b.trans_polutan_awal = '5' THEN 'Motor Listrik'
                WHEN b.trans_polutan_awal = '6' THEN 'Dibawa Pulang (Tidak Cek Aki)'
                WHEN b.trans_polutan_awal IS NULL THEN 'Tidak Mengisi'
                ELSE 'Imen Versi Lama'
            END AS statusakioff,
            CASE
                WHEN b.trans_polutan_akhir = '1' THEN 'BAGUS Antara 13.5 - 14.8 V'
                WHEN b.trans_polutan_akhir = '2' THEN 'LEMAH Dibawah 13.5 V'
                WHEN b.trans_polutan_akhir = '3' THEN 'BERLEBIH Diatas 14.8 V'
                WHEN b.trans_polutan_akhir = '4' THEN 'Cust. Tolak Cek Voltase'
                WHEN b.trans_polutan_akhir = '5' THEN 'Motor Listrik'
                WHEN b.trans_polutan_akhir = '6' THEN 'Dibawa Pulang (Tidak Cek Aki)'
                WHEN b.trans_polutan_akhir IS NULL THEN 'Tidak Mengisi'
                ELSE 'Imen Versi Lama'
            END AS statusakion,
            f.versiSekarang AS versiImen,
            CASE
                WHEN f.versiSekarang >= '1.0.3.7' THEN 'New'
                ELSE 'Old'
            END AS versiName
        FROM PB_DC.dbo.TransaksiTokoHeader a WITH (NOLOCK)
        LEFT JOIN PB_DC.dbo.imen_trans_header b WITH (NOLOCK)
            ON b.kode_toko = a.kodeToko AND b.trans_id = a.nomorImen
        LEFT JOIN PB_DC.dbo.mastertoolstoko c WITH (NOLOCK)
            ON c.kodetoko = a.kodeToko
        LEFT JOIN PB_DC.dbo.SMITransaksiTokoPerjenisMember d WITH (NOLOCK)
            ON d.kodetoko = a.kodeToko AND d.nomortransaksi = a.nomortransaksi
        LEFT JOIN PB_DC.dbo.MstMember e WITH (NOLOCK)
            ON e.NoPolisi = d.nopolisi
        LEFT JOIN PB_DC.dbo.SMITblUpdVersiPOS f WITH (NOLOCK)
            ON f.kodetoko = a.kodeToko
        LEFT JOIN PB_DC.dbo.mstJenisMember g WITH (NOLOCK) 
        	ON g.idjenismember=d.idjenismember
		LEFT JOIN PB_DC.dbo.MstCabang h WITH (NOLOCK) 
        	ON h.idCabang=c.idcabang
        WHERE a.tglBisnis = CONVERT(date, GETDATE())
		--WHERE a.tglBisnis BETWEEN '2025-10-01' AND '2025-10-14'
    )
    SELECT 
        t.tglBisnis,
		t.tglBayar,
        t.idcabang,
		t.NamaCabang,
        t.nopolisi,
        t.namaMember,
		CASE
			WHEN t.notelp IS NOT NULL 
				 AND LEN(t.notelp) > 1
				 AND t.notelp NOT LIKE '%[^0-9]%'
			THEN '62' + SUBSTRING(t.notelp, 2, LEN(t.notelp) - 1)
			ELSE NULL
		END AS notelp,
		t.idJenisMember,
		t.namaJenisMember,
        t.kodeToko,
        t.nomorTransaksi,
        t.nomorImen,
        CASE 
            WHEN TRIM(UPPER(t.versiName)) = 'OLD' THEN 'Imen Versi Lama'
            ELSE t.statusakioff
        END AS statusakioff,
        CASE 
            WHEN TRIM(UPPER(t.versiName)) = 'OLD' THEN 'Imen Versi Lama'
            ELSE t.statusakion
        END AS statusakion,
        m.jenisMotor,
        m.tahunMotor,
        m.flagStarter,
        t.versiImen,
        t.versiName
    FROM cteTransaksi t
    LEFT JOIN cteMotor m ON m.nopolisi = t.nopolisi
    WHERE m.jenisMotor <> 'ELECTRIC';
END;
GO





-------------------------------------
-----TRANSFER DATA (RMS-ROBOTIC)-----
-------------------------------------

-- public.pra_status_accu_all_hours definition
-- Drop table
-- DROP TABLE public.pra_status_accu_all_hours;

CREATE TABLE public.pra_status_accu_all_hours (
	insertdate timestamptz DEFAULT now() NOT NULL,
	tglbisnis date NOT NULL,
	tglBayar timestamp NOT NULL,
	idcabang int8 NOT NULL,
	namacabang varchar(40) NULL,
	nopolisi varchar(20) NULL,
	namamember varchar(30) NULL,
	notelp varchar(20) NULL,
	idjenismember int4 NULL,
	namajenismember varchar(30) NULL,
	kodetoko int8 NULL,
	nomortransaksi int8 NOT NULL,
	nomorimen int8 NULL,
	statusaccuoff varchar(50) NOT NULL,
	statusaccuon varchar(50) NOT NULL,
	jenismotor varchar(50) NULL,
	tahunmotor varchar(10) NULL,
	flagstarter varchar(20) NULL,
	versiimen varchar(10) NOT NULL,
	versiname varchar(10) NOT NULL,
	CONSTRAINT pra_status_accu_all_hours_unique UNIQUE (tglbisnis, idcabang, nopolisi, kodetoko, nomortransaksi)
);
CREATE INDEX pra_status_accu_all_hours_idcabang_idx ON public.pra_status_accu_all_hours USING btree (idcabang, kodetoko, nomortransaksi);





---------------------------------------
-----JOBS QUERY TEMPLATE (ROBOTIC)-----
---------------------------------------
Robotic Jobs Query Template
Name
Report Check-Up Accu 12 Jakarta
Search...
 
	Display Name
	/ Report Check-Up Accu 12 Jakarta	  


SELECT 
a.tglbisnis as "Tgl Bisnis", 
(a.tglbayar AT TIME ZONE 'UTC')::timestamp AS "Tgl Bayar",
a.namacabang as "Nama Cabang", 
a.nopolisi as "No. Polisi", 
a.namajenismember as "Nama Jenis Member", 
a.kodetoko as "Kode Toko", 
b.namatoko as "NAma Toko",
a.nomortransaksi as "Nomor Transaksi", 
a.nomorimen as "Nomor Imen",
a.jenismotor, 
a.tahunmotor,
a.statusaccuoff as "Status Accu Off",
a.statusaccuon as "Status Accu On",
a.flagstarter as "Starter Motor",
a.versiname as "Versi Imen"
FROM public.pra_status_accu_all_hours a
LEFT JOIN public.smi_msttokoho_rmsv3 b on b.kodetoko = a.kodetoko
WHERE a.idcabang=2
ORDER BY a.tglbayar,a.kodetoko,a.nomortransaksi;





---------------------------------
-----JOBS TEMPLATE (ROBOTIC)-----
---------------------------------
Robotic Jobs Template Sheet
Sheet name
CHECK-UP ACCU 12 JKT
Search...
 
	Display Name
	CHECK-UP ACCU 12 JKT	CHECK-UP ACCU 12 JKT / / Report Check-Up Accu H-1 Denpasar	  	

	
	
	
	
-----------------------------
-----JOBS MAIL (ROBOTIC)-----
-----------------------------
Robotic Jobs
Jobs name
Report_CheckUp_Accu_12
Search...
 

	Report_CheckUp_Accu_12_Jakarta_	Robotic Report Mailer	Report Check-Up Accu Per Jam 12 Jakarta	11/10/2025 09:32:22		pra_status_accu_all_hours_12	OK



	

--------------------------------------
-----JOBS MAIL SCHEDULE (ROBOTIC)-----
--------------------------------------
Scheduled Jobspra_status_accu_all_hours_12
Name	pra_status_accu_all_hours_12
  
Schedule Actions	pra_status_accu_all_hours_12
Execution Date	11/11/2025 12:00:00
Interval Unit	Days
 
Robotic Jobs
Report_CheckUp_Accu_12_Jakarta_	Robotic Report Mailer	Report Check-Up Accu Per Jam 12 Jakarta	11/10/2025 09:32:22		OK





----------------------------
-----SCHEDULE (ROBOTIC)-----
----------------------------
Scheduled DML Query
Name
accu_all_hours
Search...
	Execution Date	Interval Unit
	pra_status_accu_all_hours_30_delete	11/13/2025 20:45:00	Days
	pra_status_accu_all_hours_delete	11/12/2025 10:15:00	Hours ----setiap menit ke 15


Scheduled Transfer
Name
all_ho
Search...
	Execution Date	Interval Unit
	pra_status_accu_all_hours		11/12/2025 10:50:00	Hours ----setiap menit ke 50
	pra_status_accu_all_hours_30	11/13/2025 20:20:00	Days
	

Scheduled Jobs
Name
accu_all
Search...
	Execution Date	Interval Unit
	pra_status_accu_all_hours_12	11/12/2025 12:00:00	Days
	pra_status_accu_all_hours_15	11/12/2025 15:00:00	Days
	pra_status_accu_all_hours_17	11/12/2025 17:00:00	Days
	pra_status_accu_all_hours_19	11/12/2025 19:00:00	Days
	pra_status_accu_all_hours_20_30	11/12/2025 20:30:00	Days
	pra_status_accu_all_hours_22	11/12/2025 22:00:00	Days


