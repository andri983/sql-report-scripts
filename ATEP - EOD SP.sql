CREATE PROCEDURE sp_GetStoreUploadStatus 
    @idcabang INT,
    @date1 DATE,
    @date2 DATE
AS
BEGIN
    SET NOCOUNT ON;
    
    SELECT 
        a.kodeToko [Kode Toko],
        a.namaToko [Nama Toko],
        g.regional,
        g.area,
        CASE WHEN f.totalHeader = f.totalDetail AND f.totalDetail = f.totalRekapHeader THEN 'Ok' ELSE 'Not Ok' END as [Status Sales],
        b.tglEod [Tgl. EOD],
        b.tglUpload [Tgl. Upload EOD],
        CASE 
            WHEN d.date_1 < c.date_2 THEN CONCAT(CONVERT(DECIMAL(18, 0), (d.date_1 / NULLIF(c.date_2, 0)) * 100), ' %')
            WHEN d.date_1 >= c.date_2 THEN '100 %'
            ELSE '0 %'
        END as [Persentase upload Stok],
        d.max_upload [Jam Upload Terakhir] 
    FROM MstToko a 
    LEFT JOIN (
        SELECT kodetoko, tglbisnis, tglEod, tglUpload 
        FROM tbltglbisnis 
        WHERE tglbisnis = @date1
    ) b ON b.kodeToko = a.kodeToko
    LEFT JOIN (
        SELECT kodetoko, CONVERT(DECIMAL(18, 2), COUNT(*)) as date_2 
        FROM SaldoStokProdukToko 
        WHERE tglSaldo = @date2
        GROUP BY kodetoko
    ) c ON c.kodeToko = a.kodeToko 
    LEFT JOIN (
        SELECT kodetoko, CONVERT(DECIMAL(18, 2), COUNT(*)) as date_1, MAX(tglUpload) max_upload 
        FROM SaldoStokProdukToko 
        WHERE tglSaldo = @date1
        GROUP BY kodetoko
    ) d ON d.kodeToko = a.kodeToko
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
        AND a.idcabang = @idcabang
    ORDER BY a.kodetoko, [Persentase upload Stok];
END
GO