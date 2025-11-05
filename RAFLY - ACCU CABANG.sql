
--Robotic
SELECT 
a.tglbisnis as "Tgl Bisnis", 
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
FROM public.pra_status_accu_all a
LEFT JOIN public.smi_msttokoho_rmsv3 b on b.kodetoko = a.kodetoko
WHERE a.tglbisnis::date=current_date-1
AND a.idcabang=2
--CASE
--	when to_char(current_date::date,'dd')::character varying = '01'  then (tglbisnis between date_trunc('month',current_date - interval '1' month)::date and ((date_trunc('month', current_date) - interval '0 month' - interval '1 day')::date))
--	else(tglbisnis::date between date_trunc('month', current_date)::date and current_date::date) 
--END
ORDER BY a.tglbisnis::date;


--select * FROM public.pra_status_accu_all where tglbisnis::date=current_date-1;
--select distinct namacabang FROM public.pra_status_accu_all;
--select distinct tglbisnis FROM public.pra_status_accu_all;
--select * FROM public.pra_his_all where transactiondate::date=current_date-1;


Robotic Jobs Query Template
Name
check-up
Search...
	Display Name
	/ Report Check-Up Accu H-1 Jakarta	  
	
Robotic Jobs Template Sheet
Sheet name
check-up
Search...
	Display Name
	CHECK-UP ACCU H-1 JKT	CHECK-UP ACCU H-1 JKT / / Report Check-Up Accu H-1 JAkarta	  
	
Robotic Jobs
Jobs name
checkup
Search...
	Report_CheckUp_Accu_H-1_Jakarta_	Robotic Report Mailer	Report Check-Up Accu H-1 Jakarta	11/01/2025 17:51:39		 	OK


--SP
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
--        WHERE a.tglBisnis = CONVERT(date, GETDATE() - 1)
		WHERE a.tglBisnis BETWEEN '2025-10-01' AND '2025-10-31'
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