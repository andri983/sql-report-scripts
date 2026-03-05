USE [SMI_DB_Reporting_SMG]
GO

/****** Object:  StoredProcedure [dbo].[GetSMISalesOrder]    Script Date: 12/22/2025 2:58:13 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE PROCEDURE [dbo].[GetSMIDigunggungFrc]
AS
BEGIN
    SET NOCOUNT ON;

--Franchise : TO ke Toko/Retur barang ke DC

WITH invoicemitra AS (
	SELECT 
	a.KodeToko,a.NoInvoice,b.NoDokumen,
	convert(date,a.TglApprove) as TglApprove,
	sum(b.Qty) as Qty,
	sum(b.TotalRp) as TotalRp
	FROM PB_DC_JKT.DBO.InvoiceMitraHeader a 
	JOIN PB_DC_JKT.DBO.InvoiceMitraDetail b on b.KodeToko=a.KodeToko and b.NOInvoice=a.NoInvoice 
	WHERE a.StatusProses=1 
	AND convert(date,a.TglApprove) >= DATEADD(MONTH, DATEDIFF(MONTH, 0, GETDATE()) - 1, 0)
	AND convert(date,a.TglApprove) <  DATEADD(MONTH, DATEDIFF(MONTH, 0, GETDATE()), 0)
	GROUP BY a.KodeToko,a.NoInvoice,b.NoDokumen,a.TglApprove
) 
SELECT 
	d.namadc as NamaDCCabang,
	'07' as TrxCode,
	C.NamaPemilik as BuyerName,
	'NIK' as BuyerIdOpt,
	'0000000000000000' as BuyerIdNumber,
	'A' as GoodServiceOpt,
	A.NoInvoice as SerialNo,
	A.NoDokumen as SerialNo2,
	A.tglApprove as TransactionDate,
	A.TotalRp/1.11 as TaxBaseSellingPrice,
--	CAST(A.TotalRp / 1.11 AS DECIMAL(18,2)) AS TaxBaseSellingPrice,
	((A.TotalRp/1.11)*(cast (11 as float)/cast (12 as float))) as OtherTaxBaseSellingPrice,
--	CAST((A.TotalRp / 1.11) * (CAST(11 AS FLOAT) / CAST(12 AS FLOAT)) AS DECIMAL(18,2)) AS OtherTaxBaseSellingPrice,
	((A.TotalRp/1.11)*(cast (11 as float)/cast (12 as float)))*0.12 as VAT,
--	CAST(((A.TotalRp / 1.11) * (CAST(11 AS FLOAT) / CAST(12 AS FLOAT))) * 0.12 AS DECIMAL(18,2)) AS VAT,
	'0' as STLG,
	'Ok' as Info,
	'TO Franchise' as Modul,
	a.kodetoko as KodeTokocabang,
	CASE 
		WHEN A.TotalRp < 0 THEN 'Retur'
		WHEN A.TotalRp > 0 THEN 'Sales'
		ELSE '0'
	END AS jenisTransaksi,
	NULL AS NomorRefTransaksi
FROM invoicemitra A
LEFT JOIN PB_DC_JKT.DBO.MstToko as B with (NOLOCK) on B.kodetoko=A.kodetoko
LEFT JOIN PB_DC_JKT.DBO.mastertoolstoko as C with (NOLOCK) on C.kodetoko=A.kodetoko
LEFT JOIN PB_DC_JKT.DBO.MstDC as D with (NOLOCK) on D.idDc=C.iddc
WHERE B.kodeStatusToko<>'R';
END;


EXEC GetSMIDigunggungFrc;


--FINAL
CREATE PROCEDURE [dbo].[GetSMIDigunggungFrc]
AS
BEGIN
    SET NOCOUNT ON;

--Franchise : TO ke Toko/Retur barang ke DC

WITH invoicemitra AS (
    SELECT 
        a.KodeToko, 
        a.NoInvoice, 
        b.NoDokumen,
        CONVERT(DATE, a.TglApprove) AS TglApprove,
        SUM(b.Qty) AS Qty,
        SUM(b.TotalRp) AS TotalRp
    FROM PB_DC_JKT.DBO.InvoiceMitraHeader a 
    JOIN PB_DC_JKT.DBO.InvoiceMitraDetail b ON b.KodeToko = a.KodeToko AND b.NOInvoice = a.NoInvoice 
    WHERE a.StatusProses = 1 
    AND CONVERT(DATE, a.TglApprove) >= DATEADD(MONTH, DATEDIFF(MONTH, 0, GETDATE()) - 1, 0)
    AND CONVERT(DATE, a.TglApprove) <  DATEADD(MONTH, DATEDIFF(MONTH, 0, GETDATE()), 0)
    GROUP BY a.KodeToko, a.NoInvoice, b.NoDokumen, a.TglApprove
),
all_to_data AS (
    -- Menggabungkan TO Reguler dan Manual
    SELECT h.kodeToko, h.nomorTo, h.tglApprove 
    FROM PB_DC_JKT.DBO.ToKeTokoHeader h WITH (NOLOCK)
    WHERE h.statusdata IN (1,2)
    UNION ALL
    SELECT h.kodeToko, h.nomorTo, h.tglApprove 
    FROM PB_DC_JKT.DBO.ToKeTokoHeaderManual h WITH (NOLOCK)
    WHERE h.statusdata IN (1,2)
),
last_to_ranked AS (
    -- Menentukan TO terakhir yang tanggalnya <= tanggal Invoice
    SELECT 
        inv.NoInvoice,
        inv.KodeToko,
        to_data.nomorTo AS NomorRefTerakhir,
        ROW_NUMBER() OVER (
            PARTITION BY inv.NoInvoice, inv.KodeToko 
            ORDER BY to_data.tglApprove DESC, to_data.nomorTo DESC
        ) AS myrank
    FROM invoicemitra inv
    LEFT JOIN all_to_data to_data ON inv.KodeToko = to_data.kodeToko 
        AND CONVERT(DATE, to_data.tglApprove) <= inv.TglApprove
)
SELECT 
    d.namadc AS NamaDCCabang,
    '07' AS TrxCode,
    C.NamaPemilik AS BuyerName,
    'NIK' AS BuyerIdOpt,
    '0000000000000000' AS BuyerIdNumber,
    'A' AS GoodServiceOpt,
    A.NoInvoice AS SerialNo,
    A.NoDokumen AS SerialNo2,
    A.tglApprove AS TransactionDate,
    A.TotalRp / 1.11 AS TaxBaseSellingPrice,
    ((A.TotalRp / 1.11) * (CAST(11 AS FLOAT) / CAST(12 AS FLOAT))) AS OtherTaxBaseSellingPrice,
    ((A.TotalRp / 1.11) * (CAST(11 AS FLOAT) / CAST(12 AS FLOAT))) * 0.12 AS VAT,
    '0' AS STLG,
    'Ok' AS Info,
    'TO Franchise' AS Modul,
    A.kodetoko AS KodeTokocabang,
    CASE 
        WHEN A.TotalRp < 0 THEN 'Retur'
        WHEN A.TotalRp > 0 THEN 'Sales'
        ELSE '0'
    END AS jenisTransaksi,
    CASE 
        WHEN A.TotalRp < 0 THEN LTR.NomorRefTerakhir 
        ELSE NULL 
    END AS NomorRefTransaksi
FROM invoicemitra A
LEFT JOIN last_to_ranked LTR ON A.NoInvoice = LTR.NoInvoice 
    AND A.KodeToko = LTR.KodeToko 
    AND LTR.myrank = 1
LEFT JOIN PB_DC_JKT.DBO.MstToko AS B WITH (NOLOCK) ON B.kodetoko = A.kodetoko
LEFT JOIN PB_DC_JKT.DBO.mastertoolstoko AS C WITH (NOLOCK) ON C.kodetoko = A.kodetoko
LEFT JOIN PB_DC_JKT.DBO.MstDC AS D WITH (NOLOCK) ON D.idDc = C.iddc
WHERE B.kodeStatusToko <> 'R';
END;

EXEC GetSMIDigunggungFrc;

--
	
-----POSTGRES
-- SELECT * FROM public.tax_digunggung_frc;
-- DROP TABLE public.tax_digunggung_frc;

CREATE TABLE public.tax_digunggung_frc (
	insertdate timestamptz DEFAULT now() NOT NULL,
	namadccabang varchar(20) NULL,
	trxcode varchar(2) NULL,
	buyername varchar(50) NULL,
	buyeridopt varchar(3) NULL,
	buyeridnumber varchar(16) NULL,
	goodserviceopt varchar(1) NULL,
	serialno int8 NULL,
	serialno2 int8 NULL,
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
	CONSTRAINT tax_digunggung_frc_unique UNIQUE (namadccabang, serialno, serialno2, kodetokocabang)
);
CREATE INDEX tax_digunggung_frc_idcabang_idx ON public.tax_digunggung_frc USING btree (namadccabang);

SELECT * FROM public.tax_digunggung_frc;


--- TRANSFER DATA
EXEC GetSMIDigunggungFrc;

SELECT insertdate, namadccabang, trxcode, buyername, buyeridopt, buyeridnumber, goodserviceopt, serialno, serialno2, transactiondate, taxbasesellingprice, othertaxbasesellingprice, vat, stlg, info, modul, kodetokocabang, jenistransaksi, nomorreftransaksi
FROM public.tax_digunggung_frc;



--- JOB QUERY
SELECT 
trxcode as "TrxCode", 
buyername as "BuyerName", 
buyeridopt as "BuyerIdOpt", 
buyeridnumber as "BuyerIdNumber", 
goodserviceopt as "GoodServiceOpt", 
serialno::text as "SerialNo", 
serialno2::text as "SerialNo2", 
transactiondate as "TransactionDate", 
taxbasesellingprice as "TaxBaseSellingPrice", 
othertaxbasesellingprice as "OtherTaxBaseSellingPrice", 
vat as "VAT", 
stlg as "STLG", 
info as "Info", 
modul as "Modul", 
kodetokocabang as "KodeTokocabang", 
jenistransaksi as "JenisTransaksi", 
nomorreftransaksi::text as "NomorRefTransaksi"
FROM public.tax_digunggung_frc
where namadccabang='DC JAKARTA';