---SQL SERVER
---SP GET DATA STATUS ACCU ALL

USE [SMI_DB_Reporting_JKT]
GO

/****** Object:  StoredProcedure [dbo].[GetSMIPraStatusAccuAll]    Script Date: 10/9/2025 1:42:13 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE PROCEDURE [dbo].[GetSMIPraStatusAccuAll]
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
        WHERE a.tglBisnis = CONVERT(date, GETDATE() - 1)
    )
    SELECT 
        t.tglBisnis,
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


--POSTGRESQL
-- select * from public.pra_status_accu_all;
-- public.pra_status_accu_all definition
-- Drop table
-- DROP TABLE public.pra_status_accu_all;
CREATE TABLE public.pra_status_accu_all (
	insertdate timestamptz DEFAULT now() NOT NULL,
	tglbisnis date NOT NULL,
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
	CONSTRAINT pra_status_accu_all_unique UNIQUE (tglbisnis, idcabang, nopolisi, kodetoko, nomortransaksi)
);
CREATE INDEX pra_status_accu_all_idcabang_idx ON public.pra_status_accu_all USING btree (idcabang, kodetoko, nomortransaksi);