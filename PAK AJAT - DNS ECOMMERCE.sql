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
--		A.KodeProdukLama as "SKU LAMA",
--		A.idproduk,
		A.KodeProduk as "KODE PRODUK",
--		A.NamaPendek,
		A.Namapanjang as "NAMA PANJANG",
		A.qty as "QTY",
		A.hpp as "HPP",
--		(A.qty*A.Hpp) as SubtotalHpp,
		A.hargaJualNormal as "HARGA JUAL NORMAL",
--		A.pdisc1,
--		A.pdisc2,
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
			
	WHERE B.kodetoko='3021409'
	AND CONVERT(date,B.tglBisnis) BETWEEN '2025-08-01' AND '2025-08-11' 
) AS A

--select * from PB_DC.DBO.TransaksiTokoPembayaranPerCaraBayarNonTunaiKartu where kodetoko='3021409' and nomortransaksi=202508110001


--select * from MstBin where kodebin=1111
--select * from TransaksiTokoPembayaranPerCaraBayarNonTunaiKartu



----CREATE SP
CREATE PROCEDURE spDataSalesEcommerce
    @date1 DATE = DATE,
    @date2 DATE = DATE
AS
BEGIN
    SET NOCOUNT ON;
SELECT 
		A.tglBisnis,
		A.namacabang,
		A.kodetoko,
		A.namatoko,
		A.nomortransaksi,
		A.brand,
		A.jenis,
		A.category,
		A.kodeproduk,
		A.namapanjang,
		A.qty,
		A.hpp,
		A.hargajualnormal,
		A.disc,
		A.subtotal,
		A.noreferensitransaksi,
		A.namaBank
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
			
	WHERE B.kodetoko='3021409'
	AND CONVERT(DATE, B.tglBisnis) BETWEEN @date1 AND @date2
) AS A
END;



----CREATE TRANSFER DATA
DECLARE @date1 date
DECLARE @date2 date
DECLARE @tglHariIni INT

set @tglHariIni = DAY(CURRENT_TIMESTAMP)
		if @tglHariIni>1 ---Data yg diambil dari tgl 1 - H-1
			begin
				set @date1 = convert(date,DATEADD(mm, DATEDIFF(mm, 0, GETDATE()), 0))
				set @date2 = convert(date,getdate()-1)
			end
		else             ---Data yang diambil 1 bulan full, Month-1 (Contoh : Jika hari ini tgl 1 februari, maka yg diambil full 1 bulan januari)
			begin
				set @date1 = convert(date,DATEADD(MONTH, DATEDIFF(MONTH, 0, GETDATE())-1, 0))
				set @date2 = convert(date,dateadd(d,-(day(getdate())),getdate()))
			end

exec spDataSalesEcommerce @date1, @date2;



----CREATE TABLE TEMP TRANSFER DATA
-- public.smi_sales_ecommerce_detail definition

-- Drop table

-- DROP TABLE public.smi_sales_ecommerce_detail;

CREATE TABLE public.smi_sales_ecommerce_detail (
	insertdate timestamptz DEFAULT now() NOT NULL,
	tglbisnis date NOT NULL,
	namacabang varchar(20) NULL,
	kodetoko int8 NULL,
	namatoko varchar(100) NULL,
	nomortransaksi int8 NULL,
	brand varchar(30) NULL,
	jenis varchar(30) NULL,
	category varchar(30) NULL,
	kodeproduk varchar(10) NULL,
	namapanjang varchar(100) NULL,
	qty numeric(18, 2) NULL,
	hpp numeric(18, 2) NULL,
	hargajualnormal numeric(18, 2) NULL,
	disc numeric(18, 2) NULL,
	subtotal numeric(18, 2) NULL,
	noreferensitransaksi varchar(30) NULL,
	namabank varchar(100) NULL,
	CONSTRAINT smi_sales_ecommerce_detail_unique UNIQUE (tglbisnis, kodetoko, nomortransaksi, kodeproduk, qty, noreferensitransaksi, namabank)
);




----CREATE JOB QUERY
SELECT tglbisnis, namacabang, kodetoko, namatoko, nomortransaksi::text, brand, jenis, category, kodeproduk, namapanjang, qty, hpp, hargajualnormal, disc, subtotal, noreferensitransaksi, namabank
FROM public.smi_sales_ecommerce_detail;