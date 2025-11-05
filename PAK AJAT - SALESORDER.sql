--EXEC SP
EXEC GetSMISalesOrder '2025-09-01','2025-10-22';





--SQL-SERVER
USE [SMI_DB_Reporting_JKT]
GO

/****** Object:  StoredProcedure [dbo].[GetSMISalesOrder]    Script Date: 10/20/2025 2:54:31 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE PROCEDURE [dbo].[GetSMISalesOrder]
    @date1 DATE,
    @date2 DATE
AS
BEGIN
    SET NOCOUNT ON;

    SELECT 
		f.idDc AS iddc,
        f.namaDc AS namadc,
        c.tglApprove AS tanggalso,
        b.tglApprove AS tanggalshipment,
        e.NamaCustumer AS namacustomer,
        c.JenisStok AS jenisstok,
        a.nomorSo AS nomorso,
        a.nomorshipment AS nomorshipment,
        d.kodeProduk AS kodeproduk,
        d.namaPanjang AS namapanjang,
        a.qty AS qtyshipment,
        a.hpp AS hppshipment,
        a.Tothpp AS totalhppshipment,
        a.harga AS hargashipment,
        a.rpshipment AS subtotal,
        a.pajak AS pajakshipment,
        a.subTotal AS rpshipment
    FROM PB_DC.DBO.shipmentDetail a
    INNER JOIN PB_DC.DBO.shipmentHeader b 
        ON a.nomorSo = b.nomorso 
        AND a.nomorUSo = b.nomorUso 
        AND a.nomorshipment = b.nomorshipment 
        AND a.idDc = b.idDc
    INNER JOIN PB_DC.DBO.SMISalesOrderHeader c 
        ON a.nomorSo = c.nomorso 
        AND a.nomorUSo = c.nomorUso 
        AND a.idDc = c.idDc
    INNER JOIN PB_DC.DBO.MstProduk d 
        ON a.idProduk = d.idProduk
    INNER JOIN PB_DC.DBO.SMIMstCustumerSalesorder e 
        ON c.idCustumer = e.idCustumer
    INNER JOIN PB_DC.DBO.MstDC f WITH (NOLOCK) 
        ON f.idDc = b.idDc
    WHERE CONVERT(DATE, b.tglApprove) BETWEEN @date1 AND @date2
    ORDER BY 
        c.tglApprove, 
        b.tglApprove, 
        b.nomorshipment ASC;
END
GO





--POSTGRESQL
-- public.smi_salesorder_cab definition
-- Drop table
-- DROP TABLE public.smi_salesorder_cab;
CREATE TABLE public.smi_salesorder_cab (
	insertdate timestamptz DEFAULT now() NOT NULL,
	iddc int8 NOT NULL,
	namadc varchar(30) NOT NULL,
	tanggalso timestamp NOT NULL,
	tanggalshioment timestamp NOT NULL,
	namacustomer varchar(100) NOT NULL,
	jenisstok varchar(20) NOT NULL,
	nomorso int8 NOT NULL,
	nomorshipment int8 NOT NULL,
	kodeproduk varchar(20) NOT NULL,
	namapanjang varchar(100) NOT NULL,
	qtyshipment numeric(18, 2) NOT NULL,
	hppshipment numeric(18, 2) NOT NULL,
	totalhppshipment numeric(18, 2) NOT NULL,
	hargashipment numeric(18, 2) NOT NULL,
	subtotal numeric(18, 2) NOT NULL,
	pajakshipment numeric(18, 2) NOT NULL,
	rpshipment numeric(18, 2) NOT NULL,
	CONSTRAINT smi_salesorder_cab_unique UNIQUE (iddc, nomorso, nomorshipment, kodeproduk, qtyshipment)
);






Transfer Jobs
Jobs name
salesorder_cab
Search...
	smi_rms10_salesorder_cab	rms10.planetban.co.id <SMI_DB_Reporting_JKT>	POOL/localhost <DATARMS>	10/22/2025 09:25:28		smi_salesorder_cab	OK
	smi_rms11_salesorder_cab	rms11.planetban.co.id <SMI_DB_Reporting_BDG>	POOL/localhost <DATARMS>	 						smi_salesorder_cab	 
	smi_rms12_salesorder_cab	rms12.planetban.co.id <SMI_DB_Reporting_TNG>	POOL/localhost <DATARMS>	 						smi_salesorder_cab	 
	smi_rms15_salesorder_cab	rms15.planetban.co.id <SMI_DB_Reporting_PLG>	POOL/localhost <DATARMS>	 						smi_salesorder_cab	 
	smi_rms20_salesorder_cab	rms20.planetban.co.id <SMI_DB_Reporting_SBY>	POOL/localhost <DATARMS>	 						smi_salesorder_cab	 
	smi_rms21_salesorder_cab	rms21.planetban.co.id <SMI_DB_Reporting_SMG>	POOL/localhost <DATARMS>	 						smi_salesorder_cab	 
	smi_rms22_salesorder_cab	rms22.planetban.co.id <SMI_DB_Reporting_Bali>	POOL/localhost <DATARMS>	 						smi_salesorder_cab





Scheduled Transfersmi_salesorder_cab
Name	smi_salesorder_cab
Schedule Actions	smi_salesorder_cab
Execution Date	10/27/2025 04:00:00
Interval Unit	Weeks
Transfer Jobs
smi_rms10_salesorder_cab	rms10.planetban.co.id <SMI_DB_Reporting_JKT>	POOL/localhost <DATARMS>	10/22/2025 09:25:28		smi_salesorder_cab	OK
smi_rms11_salesorder_cab	rms11.planetban.co.id <SMI_DB_Reporting_BDG>	POOL/localhost <DATARMS>	 						smi_salesorder_cab	 
smi_rms12_salesorder_cab	rms12.planetban.co.id <SMI_DB_Reporting_TNG>	POOL/localhost <DATARMS>	 						smi_salesorder_cab	 
smi_rms15_salesorder_cab	rms15.planetban.co.id <SMI_DB_Reporting_PLG>	POOL/localhost <DATARMS>	 						smi_salesorder_cab	 
smi_rms20_salesorder_cab	rms20.planetban.co.id <SMI_DB_Reporting_SBY>	POOL/localhost <DATARMS>	 						smi_salesorder_cab	 
smi_rms21_salesorder_cab	rms21.planetban.co.id <SMI_DB_Reporting_SMG>	POOL/localhost <DATARMS>	 						smi_salesorder_cab	 
smi_rms22_salesorder_cab	rms22.planetban.co.id <SMI_DB_Reporting_Bali>	POOL/localhost <DATARMS>	 						smi_salesorder_cab	





DML Query Jobs
Jobs name
salesorder
Search...
 	delete-smi_salesorder_cab	POOL/localhost <DATARMS>	10/22/2025 10:42:55		delete-smi_salesorder_cab	OK





DML Query Jobsdelete-smi_salesorder_cab
Jobs name
delete-smi_salesorder_cab
Target	POOL/localhost <DATARMS>
  
Last Run	
Schedule DML Query	
 
DML Query
delete from public.smi_salesorder_cab;




Scheduled DML Querydelete-smi_salesorder_cab
Name	delete-smi_salesorder_cab
  
Schedule Actions	delete-smi_salesorder_cab
Execution Date	10/23/2025 01:00:00
Interval Unit	Days
 
DML Query Jobs
delete-smi_salesorder_cab	POOL/localhost <DATARMS>	 		delete-smi_salesorder_cab




--======================================================MONTHLY======================================================--

SELECT 
namadc  AS "Nama DC", 
tanggalso AS "Tanggal SO", 
tanggalshioment AS "Tanggal Shipment", 
namacustomer AS "Nama Customer", 
jenisstok AS "Jenis Stock",
nomorso AS "Nomo SO", 
nomorshipment AS "Nomor Shipment", 
kodeproduk AS "Kode Prduk", 
namapanjang AS "Nama Panjang", 
qtyshipment AS "Qty Shipment", 
hppshipment AS "HPP Shipment", 
totalhppshipment AS "Total HPP Shipment", 
hargashipment AS "Harga Shipment", 
subtotal AS "Subtotal", 
pajakshipment AS "Pajak Shipment", 
rpshipment AS "Rp Shipment"
FROM public.smi_salesorder_cab
WHERE iddc=1;




Robotic Jobs Query Template
Name
Report Sales Order Caban
Search...
 
	Display Name
	/ Report Sales Order Cabang MTD H-1 Jakarta	  
	/ Report Sales Order Cabang MTD H-1 Bandung	  
	/ Report Sales Order Cabang MTD H-1 Tangerang	  
	/ Report Sales Order Cabang MTD H-1 Palembang	  
	/ Report Sales Order Cabang MTD H-1 Surabaya	  
	/ Report Sales Order Cabang MTD H-1 Semarang	  
	/ Report Sales Order Cabang MTD H-1 Denpasar





Robotic Jobs Template Sheet
Sheet name
sales order
Search...
 
	Display Name
	Report Sales Order Cabang MTD H-1 Jakarta	Report Sales Order Cabang MTD H-1 Jakarta / / Report Sales Order Cabang MTD H-1 Jakarta	  	
	Report Sales Order Cabang MTD H-1 Bandung	Report Sales Order Cabang MTD H-1 Bandung / / Report Sales Order Cabang MTD H-1 Bandung	  	
	Report Sales Order Cabang MTD H-1 Tangerang	Report Sales Order Cabang MTD H-1 Tangerang / / Report Sales Order Cabang MTD H-1 Tangerang	  	
	Report Sales Order Cabang MTD H-1 Palembang	Report Sales Order Cabang MTD H-1 Palembang / / Report Sales Order Cabang MTD H-1 Palembang	  	
	Report Sales Order Cabang MTD H-1 Surabaya	Report Sales Order Cabang MTD H-1 Surabaya / / Report Sales Order Cabang MTD H-1 Surabaya	  	
	Report Sales Order Cabang MTD H-1 Semarang	Report Sales Order Cabang MTD H-1 Semarang / / Report Sales Order Cabang MTD H-1 Semarang	  	
	Report Sales Order Cabang MTD H-1 Denpasar	Report Sales Order Cabang MTD H-1 Denpasar / / Report Sales Order Cabang MTD H-1 Denpasar	  	





Robotic Jobs
Jobs name
sales_order
Search...
 

	Report_Sales_Order_MTD_H-1_Jakarta_		Robotic Report Mailer	Report Sales Order Cabang MTD H-1 Jakarta	10/22/2025 09:00:46		 	OK
	Report_Sales_Order_MTD_H-1_Bandung_		Robotic Report Mailer	Report Sales Order Cabang MTD H-1 Bandung	10/22/2025 09:00:46		 	OK
	Report_Sales_Order_MTD_H-1_Tangerang_	Robotic Report Mailer	Report Sales Order Cabang MTD H-1 Tangerang	10/22/2025 09:00:46		 	OK
	Report_Sales_Order_MTD_H-1_Palembang_	Robotic Report Mailer	Report Sales Order Cabang MTD H-1 Palembang	10/22/2025 09:00:46		 	OK
	Report_Sales_Order_MTD_H-1_Surabaya_	Robotic Report Mailer	Report Sales Order Cabang MTD H-1 Surabaya	10/22/2025 09:00:46		 	OK
	Report_Sales_Order_MTD_H-1_Semarang_	Robotic Report Mailer	Report Sales Order Cabang MTD H-1 Semarang	10/22/2025 09:00:46		 	OK
	Report_Sales_Order_MTD_H-1_Denpasar_	Robotic Report Mailer	Report Sales Order Cabang MTD H-1 Denpasar	10/22/2025 09:00:46		 	OK





Scheduled Jobssmi_salesorder_cab
Name	smi_salesorder_cab
  
Schedule Actions	smi_salesorder_cab
Execution Date	10/27/2025 11:00:00
Interval Unit	Weeks
 
Robotic Jobs
Report_Sales_Order_MTD_H-1_Jakarta_		Robotic Report Mailer	Report Sales Order Cabang MTD H-1 Jakarta	10/22/2025 09:00:46		OK
Report_Sales_Order_MTD_H-1_Bandung_		Robotic Report Mailer	Report Sales Order Cabang MTD H-1 Bandung	10/22/2025 09:00:46		OK
Report_Sales_Order_MTD_H-1_Tangerang_	Robotic Report Mailer	Report Sales Order Cabang MTD H-1 Tangerang	10/22/2025 09:00:46		OK
Report_Sales_Order_MTD_H-1_Palembang_	Robotic Report Mailer	Report Sales Order Cabang MTD H-1 Palembang	10/22/2025 09:00:46		OK
Report_Sales_Order_MTD_H-1_Surabaya_	Robotic Report Mailer	Report Sales Order Cabang MTD H-1 Surabaya	10/22/2025 09:00:46		OK
Report_Sales_Order_MTD_H-1_Semarang_	Robotic Report Mailer	Report Sales Order Cabang MTD H-1 Semarang	10/22/2025 09:00:46		OK
Report_Sales_Order_MTD_H-1_Denpasar_	Robotic Report Mailer	Report Sales Order Cabang MTD H-1 Denpasar	10/22/2025 09:00:46		OK






--======================================================MONTHLY======================================================--
SELECT 
    namadc AS "Nama DC", 
    tanggalso AS "Tanggal SO", 
    tanggalshioment AS "Tanggal Shipment", 
    namacustomer AS "Nama Customer", 
    jenisstok AS "Jenis Stock",
    nomorso AS "Nomor SO", 
    nomorshipment AS "Nomor Shipment", 
    kodeproduk AS "Kode Produk", 
    namapanjang AS "Nama Panjang", 
    qtyshipment AS "Qty Shipment", 
    hppshipment AS "HPP Shipment", 
    totalhppshipment AS "Total HPP Shipment", 
    hargashipment AS "Harga Shipment", 
    subtotal AS "Subtotal", 
    pajakshipment AS "Pajak Shipment", 
    rpshipment AS "Rp Shipment"
FROM public.smi_salesorder_cab
WHERE iddc = 1
    AND tanggalso >= DATE_TRUNC('month', CURRENT_DATE - INTERVAL '1 month')
    AND tanggalso < DATE_TRUNC('month', CURRENT_DATE);





Robotic Jobs Query Template
Name
Report Sales Order Cabang M-1
Search...
 
	Display Name
	/ Report Sales Order Cabang M-1 Jakarta	  
	/ Report Sales Order Cabang M-1 Bandung	  
	/ Report Sales Order Cabang M-1 Tangerang	  
	/ Report Sales Order Cabang M-1 Palembang	  
	/ Report Sales Order Cabang M-1 Surabaya	  
	/ Report Sales Order Cabang M-1 Semarang	  
	/ Report Sales Order Cabang M-1 Denpasar
	
	
	
	
	
Robotic Jobs Template Sheet
Sheet name
Report Sales Order Cabang M-1
Search...
 
	Display Name
	Report Sales Order Cabang M-1 Jakarta	Report Sales Order Cabang M-1 Jakarta / / Report Sales Order Cabang M-1 Jakarta	  	
	Report Sales Order Cabang M-1 Bandung	Report Sales Order Cabang M-1 Bandung / / Report Sales Order Cabang M-1 Bandung	  	
	Report Sales Order Cabang M-1 Tangerang	Report Sales Order Cabang M-1 Tangerang / / Report Sales Order Cabang M-1 Tangerang	  	
	Report Sales Order Cabang M-1 Palembang	Report Sales Order Cabang M-1 Palembang / / Report Sales Order Cabang M-1 Palembang	  	
	Report Sales Order Cabang M-1 Surabaya	Report Sales Order Cabang M-1 Surabaya / / Report Sales Order Cabang M-1 Surabaya	  	
	Report Sales Order Cabang M-1 Semarang	Report Sales Order Cabang M-1 Semarang / / Report Sales Order Cabang M-1 Semarang	  	
	Report Sales Order Cabang M-1 Denpasar	Report Sales Order Cabang M-1 Denpasar / / Report Sales Order Cabang M-1 Denpasar	  	

	
	
	
	
Robotic Jobs
Jobs name
sales_order
Search...
 

	Report_Sales_Order_M-1_Denpasar_	Robotic Report Mailer	Report Sales Order Cabang M-1 Denpasar	10/24/2025 09:08:43		smi_salesorder_cab_monthly	OK
	Report_Sales_Order_M-1_Jakarta_		Robotic Report Mailer	Report Sales Order Cabang M-1 Jakarta	10/24/2025 09:08:40		smi_salesorder_cab_monthly	OK
	Report_Sales_Order_M-1_Bandung_		Robotic Report Mailer	Report Sales Order Cabang M-1 Bandung	10/24/2025 09:08:40		smi_salesorder_cab_monthly	OK
	Report_Sales_Order_M-1_Tangerang_	Robotic Report Mailer	Report Sales Order Cabang M-1 Tangerang	10/24/2025 09:08:41		smi_salesorder_cab_monthly	OK
	Report_Sales_Order_M-1_Palembang_	Robotic Report Mailer	Report Sales Order Cabang M-1 Palembang	10/24/2025 09:08:41		smi_salesorder_cab_monthly	OK
	Report_Sales_Order_M-1_Surabaya_	Robotic Report Mailer	Report Sales Order Cabang M-1 Surabaya	10/24/2025 09:08:42		smi_salesorder_cab_monthly	OK
	Report_Sales_Order_M-1_Semarang_	Robotic Report Mailer	Report Sales Order Cabang M-1 Semarang	10/24/2025 09:08:43		smi_salesorder_cab_monthly	OK
	
	
	
	
	
Scheduled Jobssmi_salesorder_cab_monthly
Name	smi_salesorder_cab_monthly
  
Schedule Actions	smi_salesorder_cab_monthly
Execution Date	10/02/2025 12:00:00
Interval Unit	Months
 
Robotic Jobs
Report_Sales_Order_M-1_Jakarta_		Robotic Report Mailer	Report Sales Order Cabang M-1 Jakarta	10/24/2025 09:04:00		OK
Report_Sales_Order_M-1_Bandung_		Robotic Report Mailer	Report Sales Order Cabang M-1 Bandung	10/24/2025 09:04:00		OK
Report_Sales_Order_M-1_Tangerang_	Robotic Report Mailer	Report Sales Order Cabang M-1 Tangerang	10/24/2025 09:04:00		OK
Report_Sales_Order_M-1_Palembang_	Robotic Report Mailer	Report Sales Order Cabang M-1 Palembang	10/24/2025 09:04:00		OK
Report_Sales_Order_M-1_Surabaya_	Robotic Report Mailer	Report Sales Order Cabang M-1 Surabaya	10/24/2025 09:04:00		OK
Report_Sales_Order_M-1_Semarang_	Robotic Report Mailer	Report Sales Order Cabang M-1 Semarang	10/24/2025 09:04:00		OK
Report_Sales_Order_M-1_Denpasar_	Robotic Report Mailer	Report Sales Order Cabang M-1 Denpasar	10/24/2025 09:04:00		OK

