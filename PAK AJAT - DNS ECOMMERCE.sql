SELECT 
--		A.insertDate,
		A.tglBisnis as "TANGGAL",
--		A.tglBayar,A.idcabang,
		A.namacabang as "NAMA CABANG",
--		A.KodeTokoLama,
		A.Kodetoko as "KODE TOKO",
		A.namatoko as "NAMA TOKO",
		A.nomortransaksi as "NOMOR TRANSAKSI",
--		A.totalitem,
--		A.nomorurut,A.idbrAND,
		A.Brand as "BRAND",
--		A.iddivisi,
		A.Jenis as "JENIS",
--		A.idSubDepartement,
--		A.namaSubDepartement,
--		A.iddepartement,
		A.Category as "CATEGORY",
		A.KodeProdukLama as "SKU LAMA",
--		A.idproduk,
		A.KodeProduk as "KODE PRODUK",
--		A.NamaPendek,
		A.Namapanjang as "NAMA PANJANG",
		A.qty as "QTY",
		A.hpp as "HPP",
--		(A.qty*A.Hpp) as SubtotalHpp,
		A.hargaJualNormal as "HARGA JUAL NORMAL",
		A.pdisc1,
		A.pdisc2,
		A.disc,
--		A.HargaJualTransaksi,
		A.subtotal as "SUBTOTAL",
--		A.StatusProduk,A.idjenisProduk,
--		A.kodemember,A.nopolisi,A.idJenisMember,A.namaJenisMember,
		A.noReferensiTransaksi as "NO. REFERENSI TRANSAKSI",
		A.namaBank as "NAMA BANK"
FROM (
	SELECT
	Getdate() as insertDate,
	B.tglBisnis,B.tglBayar,datepart(hour,B.tglBayar) as pukul,
	H.idcabang,J.namacabang,G.iddc,G.singkatantoko as KodeTokoLama,
	A.Kodetoko,G.namatoko,H.idcluster,K.namaCluster,
	Concat(A.Kodetoko,'-',A.nomortransaksi,'-',A.nomorurut,'-',A.idproduk) as notrans,
	A.nomortransaksi,B.totalitem,A.nomorurut,A.idproduk,d.idbrAND,D.namaBrAND as BrAND,
	E.iddivisi,E.namaDivisi as Jenis,L.idSubDepartement,L.namaSubDepartement,
	F.iddepartement,F.namaDepartement as Category,C.Barcode3 KodeProdukLama,
	A.KodeProduk,A.NamaPendek,C.Namapanjang,
	A.qty,A.hpp,A.hargaJualNormal,0 as pdisc1,0 as pdisc2,A.diskon as disc,A.HargaJualTransaksi,A.subtotal,
	A.StatusProduk,A.idjenisProduk,A.idkemasan,B.pointReedem,B.tglSettle,G.idlkj,H.idPAC,
	G.kodePriceIndex, M.noReferensiTransaksi, P.namaBank
		From PB_DC.DBO.TransaksiTokoDetail As A WITH (NOLOCK)
			LEFT JOIN PB_DC.DBO.TransaksiTokoHeader As B WITH (NOLOCK) ON A.kodetoko=B.kodetoko AND A.NomorTransaksi=B.NomorTransaksi
			LEFT JOIN PB_DC.DBO.mstproduk As C WITH (NOLOCK) ON A.idproduk=C.Idproduk
			LEFT JOIN PB_DC.DBO.mstbrAND AS D WITH (NOLOCK) ON C.idbrAND=D.idbrAND
			LEFT JOIN PB_DC.DBO.mstdivisi As E WITH (NOLOCK) ON C.iddivisi=E.iddivisi
			LEFT JOIN PB_DC.DBO.mstdepartement As F WITH (NOLOCK) ON C.iddivisi=F.iddivisi AND E.iddivisi=F.iddivisi AND C.iddepartement=F.iddepartement
			LEFT JOIN PB_DC.DBO.mstsubdepartement As L WITH (NOLOCK) ON C.iddivisi=L.iddivisi AND E.iddivisi=L.iddivisi AND F.iddivisi=L.iddivisi 
			AND C.iddepartement=L.iddepartement AND F.iddepartement=L.iddepartement
			AND C.idsubdepartement=L.idsubdepartement 
			LEFT JOIN PB_DC.DBO.msttoko As G WITH (NOLOCK) ON A.kodetoko=G.kodetoko AND B.kodetoko=G.kodetoko
			LEFT JOIN PB_DC.DBO.mastertoolstoko as H WITH (NOLOCK) ON A.kodetoko=H.kodetoko AND B.kodetoko=H.kodetoko AND G.kodetoko=H.kodetoko 
			LEFT JOIN PB_DC.DBO.mstcabang AS J WITH (NOLOCK) ON H.idcabang=J.idcabang
			LEFT JOIN PB_DC.DBO.mstcluster AS k WITH (NOLOCK) ON K.idcluster=H.idcluster
			LEFT JOIN PB_DC.DBO.TransaksiTokoPembayaranPerCaraBayarNonTunaiKartu As M WITH (NOLOCK) ON A.kodetoko=M.kodetoko AND A.NomorTransaksi=M.NomorTransaksi
--			LEFT JOIN PB_DC.DBO.SMITransaksiTokoPerjenisMember as N with (NOLOCK) on A.kodetoko=N.kodetoko AND A.NomorTransaksi=N.NomorTransaksi
--			LEFT JOIN PB_DC.DBO.mstjenismember as O with (NOLOCK) on N.IdJenisMember=O.IdJenisMember
			LEFT JOIN PB_DC.DBO.MstBin as P WITH (NOLOCK) ON SUBSTRING(M.nomorKartu, 1, 6) = P.kodebin
			
	WHERE 
	CONVERT(date,B.tglBisnis) BETWEEN '2025-06-01' AND '2025-06-30'
	AND B.kodetoko='3021409'
--				CONVERT(date,B.tglBisnis) BETWEEN @date1 AND @date2
) AS A
--
--select * from MstBin where kodebin=1111
--select * from TransaksiTokoPembayaranPerCaraBayarNonTunaiKartu