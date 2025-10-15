SELECT 
    A.Tanggal,
    A.Tglbayar,
    A.idcabang,
    A.namacabang,
    A.kodetokolama,
    A.kodetoko,
    A.namatoko,
    A.Nomortransaksi,
    A.Totalitem,
    A.nomorurut,
    A.idbrand,
    A.Brand,
    A.Iddivisi,
    A.Jenis,
    A.idSubDepartement,
    A.namaSubDepartement,
    A.iddepartement,
    A.Category,
    A.Kodeproduklama,
    A.idproduk,
    A.KodeProduk,
    A.namapendek,
    A.Namapanjang,
    A.qty,
    A.hpp,
    A.Subtotalhpp,
    A.HargaJualNormal,
    A.pdisc1,
    A.pdisc2,
    A.disc,
    A.HargaJualTransaksi,
    A.subtotal,
    A.statusProduk,
    A.idjenisproduk,
    A.kodemember,
    A.nopolisi,
    A.idJenisMember,
    A.NamaJenismember,
    A.notrans, 
    D.namamember,
    D.Notelp,
    CONVERT(date, D.tglcreate) AS tglcreate_member,
    CASE 
        WHEN CONVERT(date, D.tglcreate) = CONVERT(date, A.Tanggal) 
            THEN 'New' 
        ELSE 'Old' 
    END AS CategoryName
FROM SMI_rms15_Transaksi_Toko_Perjenis_Member_v3 AS A WITH (NOLOCK)
LEFT JOIN (
    SELECT nopolisi,namamember,NoTelp,tglcreate
    FROM (
        SELECT 
            ROW_NUMBER() OVER (PARTITION BY Nopolisi ORDER BY tglupdate DESC) AS MyRank,
            nopolisi,namamember,NoTelp,tglcreate
        FROM PB_DC.dbo.mstmember
    ) AS data
    WHERE MyRank = 1
) AS D 
    ON D.Nopolisi = A.Nopolisi
WHERE A.tanggal BETWEEN '2025-08-01' AND '2025-08-31';
