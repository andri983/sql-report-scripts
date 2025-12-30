USE [SMI_DB_Reporting_SMG]
GO

/****** Object:  StoredProcedure [dbo].[GetSMISalesOrder]    Script Date: 12/22/2025 2:58:13 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE PROCEDURE [dbo].[GetSMIDigunggungPos]
AS
BEGIN
    SET NOCOUNT ON;

    -- POS
SELECT
	H.namacabang as NamaDCCabang,
	'07' AS TrxCode,
	D.namaMember AS BuyerName,
	'NIK' AS BuyerIdOpt,
	'0000000000000000' AS BuyerIdNumber,
	'A' AS GoodServiceOpt,
	B.nomortransaksi AS SerialNo,
	A.TglBisnis AS TransactionDate,
	A.totalRpPenjualan/1.11 AS TaxBaseSellingPrice,
	((A.totalRpPenjualan/1.11)*(CAST (11 AS float)/CAST (12 AS float))) AS OtherTaxBaseSellingPrice,
	((A.totalRpPenjualan/1.11)*(CAST (11 AS float)/CAST (12 AS float)))*0.12 AS VAT,
	'0' AS STLG,
	'Ok' AS Info,
	'POS' AS Modul,
	CONVERT(VARCHAR(10), A.kodetoko) AS KodeTokocabang,
		CASE 
		WHEN A.totalRpPenjualan < 0 THEN 'Retur'
		WHEN A.totalRpPenjualan > 0 THEN 'Sales'
		ELSE '0'
	END AS jenisTransaksi,
	F.NomorRefTransaksi
FROM PB_DC.DBO.TransaksiTokoheader AS A with (NOLOCK)
LEFT JOIN PB_DC.DBO.SMITransaksiTokoPerjenisMember AS B with (NOLOCK) ON A.kodetoko=B.kodetoko AND A.nomortransaksi=B.nomortransaksi
LEFT JOIN PB_DC.DBO.mstJenisMember AS C with (NOLOCK) ON B.IdJenisMember=C.IdJenisMember
LEFT JOIN PB_DC.DBO.MstMember AS D with (NOLOCK) ON D.NoPolisi=b.NoPolisi
LEFT JOIN PB_DC.DBO.MstToko AS E with (NOLOCK) ON E.kodetoko=A.kodetoko
LEFT JOIN PB_DC.DBO.TransaksiReturSalesHeader AS F with (NOLOCK) ON F.kodetoko=A.kodetoko AND F.nomorRefTransaksi2=A.nomorTransaksi
LEFT JOIN PB_DC.DBO.MasterToolsToko AS G with (NOLOCK) ON G.kodetoko=A.kodeToko
LEFT JOIN PB_DC.DBO.MstCabang AS H with (NOLOCK) ON H.idcabang=G.idcabang
WHERE E.kodestatustoko='R' 
AND A.TglBisnis >= DATEADD(MONTH, DATEDIFF(MONTH, 0, GETDATE()) - 1, 0)
AND A.TglBisnis <  DATEADD(MONTH, DATEDIFF(MONTH, 0, GETDATE()), 0)
--AND A.TglBisnis BETWEEN '2025-11-01' AND '2025-11-30'
--AND A.kodetoko=3021297 AND A.nomorTransaksi=202511160005;
--SELECT * FROM PB_DC.dbo.TransaksiTokoHeader where kodetoko=3021297 and nomorTransaksi=202511160005;
END
GO


EXEC GetSMIDigunggungPos;


-----POSTGRES
-- SELECT * FROM public.tax_digunggung_pos;
-- DROP TABLE public.tax_digunggung_pos;

CREATE TABLE public.tax_digunggung_pos (
	insertdate timestamptz DEFAULT now() NOT NULL,
	namadccabang varchar(20) NULL,
	trxcode varchar(2) NULL,
	buyername varchar(40) NULL,
	buyeridopt varchar(3) NULL,
	buyeridnumber varchar(16) NULL,
	goodserviceopt varchar(1) NULL,
	serialno int8 NULL,
	transactiondate date NULL,
	taxbasesellingprice numeric(24, 2) NULL,
	othertaxbasesellingprice numeric(24, 2) NULL,
	vat numeric(24, 2) NULL,
	stlg varchar(1) NULL,
	info varchar(2) NULL,
	modul varchar(20) NULL,
	kodetokocabang varchar(20) NULL,
	jenistransaksi varchar(10) NULL,
	nomorreftransaksi int8 NULL,
	CONSTRAINT tax_digunggung_pos_unique UNIQUE (namadccabang, serialno, kodetokocabang)
);
CREATE INDEX tax_digunggung_pos_idcabang_idx ON public.tax_digunggung_pos USING btree (namadccabang);


--- TRANSFER DATA
EXEC GetSMIDigunggungPos;

SELECT insertdate, namadccabang, trxcode, buyername, buyeridopt, buyeridnumber, goodserviceopt, serialno, transactiondate, taxbasesellingprice, othertaxbasesellingprice, vat, stlg, info, modul, kodetokocabang, jenistransaksi, nomorreftransaksi
FROM public.tax_digunggung_pos;


--- JOB QUERY
SELECT 
trxcode, 
buyername, 
buyeridopt, 
buyeridnumber, 
goodserviceopt, 
serialno::text, 
transactiondate, 
taxbasesellingprice, 
othertaxbasesellingprice, 
vat, 
stlg, 
info, 
modul, 
kodetokocabang, 
jenistransaksi, 
nomorreftransaksi
FROM public.tax_digunggung_pos
where namadccabang='Semarang';