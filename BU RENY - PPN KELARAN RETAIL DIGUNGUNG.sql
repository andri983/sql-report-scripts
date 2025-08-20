--POS : Sales/Retur Customer Toko Reguler
--Sales Order : Penjualan barang DC & Retur Sales Order
--Franchise : TO ke Toko/Retur barang ke DC

SELECT a.TrxCode,a.BuyerName,a.BuyerIdOpt,a.BuyerIdNumber,a.GoodServiceOpt,a.SerialNo,a.TransactionDate,a.TaxBaseSellingPrice,a.OtherTaxBaseSellingPrice,a.VAT,a.STLG,a.Info,a.Modul,a.KodeTokocabang
	FROM (
---//POS
SELECT
	'07' as TrxCode,
	D.namaMember as BuyerName,
	'NIK' as BuyerIdOpt,
	'0000000000000000' as BuyerIdNumber,
	'A' as GoodServiceOpt,
	B.nomortransaksi as SerialNo,
	A.TglBisnis as TransactionDate,
	A.totalRpPenjualan/1.11 as TaxBaseSellingPrice,
	((A.totalRpPenjualan/1.11)*(cast (11 as float)/cast (12 as float))) as OtherTaxBaseSellingPrice,
	((A.totalRpPenjualan/1.11)*(cast (11 as float)/cast (12 as float)))*0.12 as VAT,
	'0' as STLG,
	'Ok' as Info,
	'POS' as Modul,
	CONVERT(VARCHAR(10), A.kodetoko) as KodeTokocabang
FROM PB_DC.dbo.TransaksiTokoheader as A with (NOLOCK)
left join PB_DC.dbo.SMITransaksiTokoPerjenisMember as B with (NOLOCK) on A.kodetoko=B.kodetoko and A.nomortransaksi=B.nomortransaksi
left join PB_DC.dbo.mstJenisMember as C with (NOLOCK) on B.IdJenisMember=C.IdJenisMember
left join PB_DC.dbo.MstMember as D with (NOLOCK) on D.NoPolisi=b.NoPolisi
left join PB_DC.dbo.MstToko as E with (NOLOCK) on E.kodetoko=A.kodetoko
WHERE E.kodestatustoko='R' 
and A.TglBisnis between '2025-07-01' and '2025-07-31'
UNION ALL
--//SALES ORDER
SELECT 
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
	F.namaDc as KodeTokocabang
FROM PB_DC.dbo.shipmentHeader a
JOIN PB_DC.dbo.SMISalesOrderHeader c with (NOLOCK)on a.nomorSo=c.nomorso and a.nomorUSo=c.nomorUso and a.idDc=c.idDc
JOIN PB_DC.dbo.SMIMstCustumerSalesorder e with (NOLOCK) on c.idCustumer=e.idCustumer
JOIN PB_DC.dbo.MstDC f with (NOLOCK) on f.idDc=a.idDc
WHERE convert(date,a.tglApprove) between '2025-07-01' and '2025-07-31'
AND a.statusData=1
UNION ALL
--//RETUR SALES ORDER
SELECT 
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
	C.namaDc as KodeTokocabang
FROM PB_DC.dbo.SmiReturSalesOrderHeader a
JOIN PB_DC.dbo.SMIMstCustumerSalesorder b with (NOLOCK) on a.idCustomer=b.idCustumer
JOIN PB_DC.dbo.MstDC c with (NOLOCK) on c.idDc=a.idDc
WHERE a.StatusReceipt=1
AND convert(date,a.TglReceipt ) between '2025-07-01' and '2025-07-31'
UNION ALL
--//TO Franchise
SELECT
	'07' as TrxCode,
	C.NamaPemilik as BuyerName,
	'NIK' as BuyerIdOpt,
	'0000000000000000' as BuyerIdNumber,
	'A' as GoodServiceOpt,
	A.nomorTo as SerialNo,
	CONVERT(date,A.tglApprove) as TransactionDate,
	A.totalRpTONet/1.11 as TaxBaseSellingPrice,
	((A.totalRpTONet/1.11)*(cast (11 as float)/cast (12 as float))) as OtherTaxBaseSellingPrice,
	((A.totalRpTONet/1.11)*(cast (11 as float)/cast (12 as float)))*0.12 as VAT,
	'0' as STLG,
	'Ok' as Info,
	'Franchise' as Modul,
	namadc as KodeTokocabang
FROM PB_DC.dbo.ToKeTokoHeader as A with (NOLOCK)
left join PB_DC.dbo.MstToko as B with (NOLOCK) on B.kodetoko=A.kodetoko
left join PB_DC.dbo.mastertoolstoko as C with (NOLOCK) on C.kodetoko=A.kodetoko
left join PB_DC.dbo.MstDC as D with (NOLOCK) on D.idDc=C.iddc
WHERE A.statusdata in (1,2) 
AND CONVERT(date,A.tglApprove) between '2025-07-01' and '2025-07-31' AND B.kodeStatusToko<>'R'
UNION ALL
--//TO Manual Franchise
SELECT
	'07' as TrxCode,
	C.NamaPemilik as BuyerName,
	'NIK' as BuyerIdOpt,
	'0000000000000000' as BuyerIdNumber,
	'A' as GoodServiceOpt,
	A.nomorTo as SerialNo,
	CONVERT(date,A.tglApprove) as TransactionDate,
	A.totalRpTONet/1.11 as TaxBaseSellingPrice,
	((A.totalRpTONet/1.11)*(cast (11 as float)/cast (12 as float))) as OtherTaxBaseSellingPrice,
	((A.totalRpTONet/1.11)*(cast (11 as float)/cast (12 as float)))*0.12 as VAT,
	'0' as STLG,
	'Ok' as Info,
	'Franchise' as Modul,
	namadc as KodeTokocabang
FROM PB_DC.dbo.ToKeTokoHeaderManual as A with (NOLOCK)
left join PB_DC.dbo.MstToko as B with (NOLOCK) on B.kodetoko=A.kodetoko
left join PB_DC.dbo.mastertoolstoko as C with (NOLOCK) on C.kodetoko=A.kodetoko
left join PB_DC.dbo.MstDC as D with (NOLOCK) on D.idDc=C.iddc
WHERE A.statusdata in (1,2) 
AND CONVERT(date,A.tglApprove) between '2025-07-01' and '2025-07-31' AND B.kodeStatusToko<>'R'
UNION ALL
--//RETUR Franchise
SELECT
	'07' as TrxCode,
	C.NamaPemilik as BuyerName,
	'NIK' as BuyerIdOpt,
	'0000000000000000' as BuyerIdNumber,
	'A' as GoodServiceOpt,
	A.nomorLpb as SerialNo,
	CONVERT(date,A.tglLpb) as TransactionDate,
	(A.totalRp/1.11)*-1 as TaxBaseSellingPrice,	
	((A.totalRp/1.11)*(cast (11 as float)/cast (12 as float)))*-1 as OtherTaxBaseSellingPrice,
	(((A.totalRp/1.11)*(cast (11 as float)/cast (12 as float)))*0.12)*-1 as VAT,
	'0' as STLG,
	'Ok' as Info,
	'Franchise' as Modul,
	namadc as KodeTokocabang
FROM PB_DC.dbo.LpbDariTokoHeader as A with (NOLOCK)
left join PB_DC.dbo.MstToko as B with (NOLOCK) on B.kodetoko=A.kodetoko
left join PB_DC.dbo.mastertoolstoko as C with (NOLOCK) on C.kodetoko=A.kodetoko
left join PB_DC.dbo.MstDC as D with (NOLOCK) on D.idDc=C.iddc
WHERE CONVERT(date,A.tglLpb) between '2025-07-01' and '2025-07-31' AND B.kodeStatusToko<>'R'
) as a
Order by a.kodetokocabang, a.transactiondate, a.serialno;






--
--select a.* FROM PB_DC.dbo.TransaksiTokoheader as A with (NOLOCK)
--left join PB_DC.dbo.MstToko as E with (NOLOCK) on E.kodetoko=A.kodetoko
--WHERE E.kodestatustoko='R' 
--and A.TglBisnis between '2025-07-01' and '2025-07-31'