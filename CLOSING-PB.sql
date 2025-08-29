--- Query Summary Inv Movement BDP DC ke Toko --

Select
Tahun,Bulan,iddc,COA,
sum(Qty_SubTotal_SAW) as SAW_Qty,
sum(Value_SubTotal_SAW) as SAW_Value,
sum(Qty_Subtotal_TO_MTD) as DC_OUT_Qty,
sum(Value_Subtotal_TO_MTD) as DC_OUT_Value,
sum(Qty_subtotal_TI_MTD) as DC_Dest_IN_Qty,
sum(Value_subtotal_TI_MTD) as DC_Dest_IN_Value,
sum(SAK_Qty) as SAK_Qty,
sum(SAK_Value) as SAK_Value
from SMI_InvMove_BDPDCkeToko_Detail_non_kons
Where
tahun=2025                    -- input sesuai tahun perikan data
and bulan=07                -- input sesuai Bulan perikan data
and kodestatustoko='R'
and iddc=5                   -- diganti berdasarkan kodestatustoko yg akan ditarik datanya.
group by tahun,bulan,iddc,COA
order by tahun asc, bulan asc, COA asc;


/***************************************************************/


--- Query Detail Inv Movement BDP DC ke Toko --
use PB_DC;
select * from SMI_InvMove_BDPDCkeToko_Detail_non_kons
Where
tahun=2025                -- input sesuai tahun perikan data
and bulan=06            -- input sesuai Bulan perikan data
and kodestatustoko='R'
and iddc=2                -- diganti berdasarkan kodestatustoko yg akan ditarik datanya.
order by tahun asc, bulan asc, COA asc; 


/****************************************************************************************************************************/


-- Query Tarik Data Summary Inv. Mov BDP Toko ke DC (Toko Reguler)

Use PB_DC;
Declare @date1 date ='2025-07-01'    -- diganti berdasarkan tanggal awal bulan penarikan data
Declare @date2 date ='2025-07-31'    -- diganti berdasarkan tanggal akhir bulan penarikan data
Declare @iddc int=    -- diganti berdasarkan iddc data yang ditarik
Declare @kodestatustoko varchar='R'
SELECT * FROM SMI_InvMove_BDPTokokeDC_Summary_Reg_view_v5_fc (@date1,@date2,@iddc,@kodestatustoko);


/***************************************************************/


-- Query Tarik Data Detail Inv. Mov BDP Toko ke DC (Toko Reguler)

Use PB_DC;
Declare @date1 date ='2025-07-01'        -- diganti berdasarkan tanggal awal bulan penarikan data
Declare @date2 date ='2025-07-31'        -- diganti berdasarkan tanggal akhir bulan penarikan data
Declare @iddc int=2                    -- diganti berdasarkan iddc data yang ditarik
Declare @kodestatustoko varchar='R'
SELECT * FROM  SMI_InvMove_BDPTokokeDC_Detail_Reg_view_v5_fc (@date1,@date2,@iddc,@kodestatustoko); 


/****************************************************************************************************************************/


-- Query Tarik Data Summary Inv Movement BDP DC ke DC---

use PB_DC;
Declare @date1    date    ='2025-07-01'    -- periode awal data
Declare @date2    date    ='2025-07-31'    -- periode akhir data
Declare @iddc    int        =5               -- iddc

Declare @tahun    int        =2025            -- tahun penarikan data
Declare @Bulan    int        =07                -- bulan penarikan data

SELECT * FROM SMI_InvMove_BDPDCkeDC_Summary_view_v5_fc (@date1,@date2,@iddc,@tahun,@bulan);


/***************************************************************/


-- Query Tarik Data Detail Inv Movement BDP DC ke DC --

use PB_DC;
Declare @tahun    int		= 2025    -- periode tahun
Declare @bulan    int		= 6    -- periode bulan
Declare @idDcPengirim   int = 2        -- idDcPengirim

SELECT
tahun,bulan,
idDcPengirim, NamaDC_Pengirim, idjenisstok, iddcPenerima, NamaDC_Penerima,
NoMutasi, idproduk, kodeproduk, namapanjang, Category, COA,
sum(DC_OUT_Qty) as DC_OUT_Qty,
sum(DC_OUT_Value) as DC_OUT_Value,
sum(DC_Dest_IN_Qty) as DC_Dest_IN_Qty,
sum(DC_Dest_IN_Value) as DC_Dest_IN_Value
FROM PB_DC.dbo.SMI_InvMove_BDPDCkeDC_Detail_non_kons
where
tahun=@tahun and bulan=@bulan and idDcPengirim=@idDcPengirim
group by TglMutasi, idDcPengirim, NamaDC_Pengirim, idjenisstok, iddcPenerima, NamaDC_Penerima,
NoMutasi, idproduk, kodeproduk, namapanjang, Category, COA, Tahun, Bulan
order by tglmutasi asc, nomutasi asc;


/****************************************************************************************************************************/


--STOCK GOOD STOCK - BOOKING STOCK - BAD STOCK
/* SMI_SOH_DC_byformula_per_jenisstok_view */
--select * from mstjenisstok
Declare @date1 date	='2025-07-01'	-- diisi berdasarkan periode awal penarikan data
Declare @date2 date	='2025-07-31'	-- diisi berdasarkan periode Akhir penarikan data
Declare @iddc int	= 2			-- diisi berdasrakan iddc data yg akan ditarik
Declare @idjenisstok int	= 3		-- diisi berdasrakan idJenisStok (Jenis Gudang) data yg akan ditarik
EXEC sp_SMI_SOH_DC_byformula_per_jenisstok_view_v11 @date1,@date2,@iddc,@idjenisstok;


/****************************************************************************************************************************/


--STOCK GOOD STOCK TOKO REGULER
/* Query Tarik Saldo Stok Toko Reguler utk report Monitoring Stok */
DECLARE @date1 date='2025-07-01'
DECLARE @date2 date='2025-07-31'
DECLARE @idcabang int= 6
DECLARE @kodestatustoko varchar='R'
select 
a.tglsaldo,
--a.kodetoko,
b.idproduk,
a.kodeproduk,
a.description,
sum(a.soh_qty) as qty,
sum(a.soh_value) as value_hpp_toko,
(sum(a.soh_qty)*c.hpp) as value_hpp_dc
from smi_soh_toko_reg_byformula_view_10_fc(@date1,@date2,@idcabang,@kodestatustoko) as a
Join mstproduk as b with (nolock) on b.kodeproduk=a.kodeproduk
Join saldostokprodukdc as c with (nolock) on c.tglsaldo=a.tglsaldo and c.idproduk=b.idproduk
left join dbo.tblProduknonstock as d with (nolock) on d.idproduk=b.idproduk
where c.idjenisstok=1 
and ISNULL(d.nonstock,0)=0
group by a.tglsaldo,a.kodeproduk,a.description,b.idproduk,c.hpp;


/****************************************************************************************************************************/


-- Query Inv. Movement Detail DC v11.

USE [PB_DC]

DECLARE @date1 date='2025-07-01'    -- isi periode awal penarikan data
DECLARE @date2 date='2025-07-31'    -- isi periode akhir penarikan data
DECLARE @iddc int= 1                -- isi berdasarkan iddc

EXEC sp_SMI_InvMove_DC_detail_view_v11 @date1,@date2,@iddc;

/***************************************************************/

-- Query Inv. Movement Toko Detail v10

Use PB_DC;
Declare @date1 date ='2025-07-01'        -- diganti berdasarkan tanggal awal bulan penarikan data
Declare @date2 date ='2025-07-31'        -- diganti berdasarkan tanggal akhir bulan penarikan data
Declare @kodestatustoko varchar='R'        -- diganti berdasarkan kodestatustoko data yang diingikan
Declare @idcabang int=2                    -- diganti berdasarkan idcabang data yang diingikan

EXEC sp_SMI_InvMove_Toko_Detail_view_10 @date1,@date2,@kodestatustoko,@idcabang; 


















-- Query Inv. Movement DC Summary v11 :  perbaikan pada SO plus Minus dan Non konsinyasi

USE [PB_DC]

DECLARE @date1 date='2025-07-01'    -- isi periode awal penarikan data
DECLARE @date2 date='2025-07-31'    -- isi periode akhir penarikan data
DECLARE @iddc int= 1                -- isi berdasarkan iddc

EXEC sp_SMI_InvMove_DC_summary_view_v11 @date1,@date2,@iddc;

/****************************************************************************/

-- Query Inv. Movement Toko Summary v10 : perbaikan pada SO plus Minus dan Non konsinyasi

USE PB_DC;
DECLARE @date1 date='2025-03-01'    -- isi periode awal penarikan data
DECLARE @date2 date='2025-03-31'    -- isi periode akhir penarikan data
Declare @idcabang int=1                -- isi berdasarkan idcabang
Declare @kodestatustoko varchar='R'    -- isi berdasarkan Kodestatustoko

EXEC sp_SMI_InvMove_Toko_Summary_view_10 @date1,@date2,@kodestatustoko,@idcabang;

/****************************************************************************/

-- Query Inv. Movement Toko Detail v10 : perbaikan pada SO plus Minus dan Non konsinyasi

Use PB_DC;
Declare @date1 date ='2025-03-01'        -- diganti berdasarkan tanggal awal bulan penarikan data
Declare @date2 date ='2025-03-01'        -- diganti berdasarkan tanggal akhir bulan penarikan data
Declare @kodestatustoko varchar='R'        -- diganti berdasarkan kodestatustoko data yang diingikan
Declare @idcabang int=2                    -- diganti berdasarkan idcabang data yang diingikan

EXEC sp_SMI_InvMove_Toko_Detail_view_10 @date1,@date2,@kodestatustoko,@idcabang;

/****************************************************************************/ 