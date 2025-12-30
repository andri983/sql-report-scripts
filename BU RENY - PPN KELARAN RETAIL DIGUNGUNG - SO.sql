USE [SMI_DB_Reporting_SMG]
GO

/****** Object:  StoredProcedure [dbo].[GetSMIDigunggungSalesOrder]    Script Date: 12/26/2025 8:58:49 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO


CREATE PROCEDURE [dbo].[GetSMIDigunggungSalesOrder]
AS
BEGIN
    SET NOCOUNT ON;

SELECT
		x.NamaDCCabang,
		x.TrxCode,
		x.BuyerName,
		x.BuyerIdOpt,
		x.BuyerIdNumber,
		x.GoodServiceOpt,
		x.SerialNo,
		x.TransactionDate,
		x.TaxBaseSellingPrice,
		x.OtherTaxBaseSellingPrice,
		x.VAT,
		x.STLG,
		x.Info,
		x.Modul,
		x.KodeTokocabang,
		x.jenisTransaksi,
		x.NomorRefTransaksi
FROM(
	SELECT 
	--//SALES ORDER
		f.namadc as NamaDCCabang,
		'07' as TrxCode,
		e.NamaCustumer  as BuyerName,
		'NIK' as BuyerIdOpt,
		'0000000000000000' as BuyerIdNumber,
		'A' as GoodServiceOpt,
		a.nomorshipment as SerialNo,
		convert(date,a.tglApprove) as TransactionDate,
		((a.totalRp)/1.11) as TaxBaseSellingPrice,
		((((a.totalRp)/1.11))*(cast (11 as float)/cast (12 as float))) as OtherTaxBaseSellingPrice,
		((((a.totalRp)/1.11))*(cast (11 as float)/cast (12 as float)))*0.12 as VAT,
		'0' as STLG,
		'Ok' as Info,
		'Sales Order' as Modul,
		F.namaDc as KodeTokocabang,
		'Sales' AS jenisTransaksi,
		' ' AS NomorRefTransaksi
	FROM PB_DC.DBO.shipmentHeader a
	JOIN PB_DC.DBO.SMISalesOrderHeader c with (NOLOCK)on a.nomorSo=c.nomorso AND a.nomorUSo=c.nomorUso AND a.idDc=c.idDc
	JOIN PB_DC.DBO.SMIMstCustumerSalesorder e with (NOLOCK) on c.idCustumer=e.idCustumer
	JOIN PB_DC.DBO.MstDC f with (NOLOCK) on f.idDc=a.idDc
	WHERE convert(date,a.tglApprove) >= DATEADD(MONTH, DATEDIFF(MONTH, 0, GETDATE()) - 1, 0)
	AND convert(date,a.tglApprove) <  DATEADD(MONTH, DATEDIFF(MONTH, 0, GETDATE()), 0)
	AND a.statusData=1
UNION ALL
	--//RETUR SALES ORDER
	SELECT 
		c.namadc as NamaDCCabang,
		'07' as TrxCode,
		B.NamaCustumer  as BuyerName,
		'NIK' as BuyerIdOpt,
		'0000000000000000' as BuyerIdNumber,
		'A' as GoodServiceOpt,
		a.nomorRetur as SerialNo,
		convert(date,a.TglReceipt) as TransactionDate,
		((a.totalRp)/1.11)*-1 as TaxBaseSellingPrice,	
		((((a.totalRp)/1.11))*(cast (11 as float)/cast (12 as float)))*-1 as OtherTaxBaseSellingPrice,
		(((((a.totalRp)/1.11))*(cast (11 as float)/cast (12 as float)))*0.12)*-1 as VAT,	
		'0' as STLG,
		'Ok' as Info,
		'Sales Order' as Modul,
		C.namaDc as KodeTokocabang,
		'Retur' AS jenisTransaksi,
		a.Nomorshipment AS NomorRefTransaksi
	FROM PB_DC.DBO.SmiReturSalesOrderHeader a
	JOIN PB_DC.DBO.SMIMstCustumerSalesorder b with (NOLOCK) on a.idCustomer=b.idCustumer
	JOIN PB_DC.DBO.MstDC c with (NOLOCK) on c.idDc=a.idDc
	WHERE convert(date,a.tglApprove) >= DATEADD(MONTH, DATEDIFF(MONTH, 0, GETDATE()) - 1, 0)
	AND convert(date,a.tglApprove) <  DATEADD(MONTH, DATEDIFF(MONTH, 0, GETDATE()), 0)
	AND a.StatusReceipt=1
	) as x
END
GO;


EXEC GetSMIDigunggungSalesOrder;


-----POSTGRES
-- SELECT * FROM public.tax_digunggung_salesorder;
-- DROP TABLE public.tax_digunggung_salesorder;

CREATE TABLE public.tax_digunggung_salesorder (
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
	CONSTRAINT tax_digunggung_salesorder_unique UNIQUE (namadccabang, serialno, kodetokocabang)
);
CREATE INDEX tax_digunggung_salesorder_idcabang_idx ON public.tax_digunggung_salesorder USING btree (namadccabang);


--- TRANSFER DATA
EXEC GetSMIDigunggungSalesOrder;

SELECT insertdate, namadccabang, trxcode, buyername, buyeridopt, buyeridnumber, goodserviceopt, serialno, transactiondate, taxbasesellingprice, othertaxbasesellingprice, vat, stlg, info, modul, kodetokocabang, jenistransaksi, nomorreftransaksi
FROM public.tax_digunggung_salesorder;