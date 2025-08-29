SELECT 
    x.tgltransaksi as "Tgl Transaksi", 
    x.tglreport as "Tgl Report", 
    x.tglakhirvoucher as "Tgl Akhir Voucher", 
    x.nomorserivoucher as "Nomor Seri VOucher", 
    x.namacabang as "Nama Cabang", 
    x.kodetoko as "Kode Toko", 
    x.nomortransaksi as "Nomor Transaksi", 
    x.brand as "Brand", 
    x.category as "Category", 
    x.idproduk as "Id Produk", 
    x.kodeproduk as "Kode Produk", 
    x.namapanjang as "Nama Panjang", 
    x.qty as "Qty", 
    x.subtotal as "Subtotal", 
    x.nopolisi as "No. POlisi", 
    x.namamember as "Nama Member", 
    x.notelp as "No. Hp/WA", 
    x.idjenismember as "Id Jenis Member", 
    x.namajenismember as "Nama Jenis Member", 
    x.wa_status as "WA Status", 
    x.wa_send_date as "WA Send Date", 
    x.wa_xid as "WA XID", 
    x.wa_status_data as "WA Status Data",
    a.tglbayar as "Tgl Transaksi", 
    a.kodetoko as "Kode Toko", 
    a.nomortransaksi as "Nomor Transaksi", 
    a.nomorserivoucher as "Nomor Serial Vuocher"
FROM 
    public.car_his x
LEFT JOIN (
    SELECT tglbayar,kodetoko,nomortransaksi,nomorserivoucher 
    FROM smi_rms22_sales_voucher_cab
    UNION ALL
    SELECT tglbayar,kodetoko,nomortransaksi,nomorserivoucher
    FROM smi_rms21_sales_voucher_cab
    UNION ALL
    SELECT tglbayar,kodetoko,nomortransaksi,nomorserivoucher
    FROM smi_rms20_sales_voucher_cab_smd
    UNION ALL
    SELECT tglbayar,kodetoko,nomortransaksi,nomorserivoucher
    FROM smi_rms20_sales_voucher_cab_sby
    UNION ALL
    SELECT tglbayar,kodetoko,nomortransaksi,nomorserivoucher
    FROM smi_rms15_sales_voucher_cab
    UNION ALL
    SELECT tglbayar,kodetoko,nomortransaksi,nomorserivoucher
    FROM smi_rms12_sales_voucher_cab
    UNION ALL
    SELECT tglbayar,kodetoko,nomortransaksi,nomorserivoucher
    FROM smi_rms11_sales_voucher_cab
    UNION ALL
    SELECT tglbayar,kodetoko,nomortransaksi,nomorserivoucher
    FROM smi_rms10_sales_voucher_cab
) AS a ON x.nomorserivoucher = a.nomorserivoucher
ORDER BY x.tgltransaksi;