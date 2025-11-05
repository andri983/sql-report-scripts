/******************************************************************************************/
--DC to DC MOBENG
/******************************************************************************************/
-- Query Summary Inv Movement BDP DC ke DC --
--use MB_DC;        -- utk jalan di server JKT
--use MB_DC_SBY;    -- utk jalan di server SBY
Declare @date1		date	='2025-10-01'   -- periode awal data
Declare @date2		date    ='2025-10-31'   -- periode akhir data
Declare @iddc		int     =1				-- isi sesuai iddc data yg akan ditarik
Declare @tahun		int     =2025
Declare @bulan		int     =10
Select * from MB_InvMove_BDPDCkeDC_Summary_view_v5_fc(@date1,@date2,@iddc,@tahun,@bulan)
order by coa Asc;

-- Query Detail Inv Movement BDP DC ke DC --
--use MB_DC;        -- utk jalan di server JKT
--use MB_DC_SBY;    -- utk jalan di server SBY
--Declare @tahun		int=2025	-- tahun penarikan Data
--Declare @bulan		int=8	    -- bulan penarikan Data
--Declare @iddc		int=1		-- iddc disesuaikan dgn iddc data yang ditarik
--select * from MB_InvMove_BDPDCkeDC_Detail_view_v5_fc (@tahun,@bulan,@iddc) order by idproduk asc; 

Declare @date1		date='2025-10-01'   -- periode awal data
Declare @date2		date='2025-10-31'   -- periode akhir data
Declare @tahun		int=2025	-- tahun penarikan Data
Declare @bulan		int=10	    -- bulan penarikan Data
Declare @iddc		int=1		-- iddc disesuaikan dgn iddc data yang ditarik
select * from MB_InvMove_BDPDCkeDC_detail_sum_view_v5_fc(@date1,@date2,@iddc,@tahun,@bulan) order by idproduk asc;

/******************************************************************************************/
--DC to STORE MOBENG
/******************************************************************************************/
-- Query Summary Inv Movement BDP DC ke Toko --
--use MB_DC;        -- utk jalan di server JKT
--use MB_DC_SBY;    -- utk jalan di server SBY
Select
Tahun,Bulan,iddc,COA,
sum(Qty_SubTotal_SAW) as SAW_Qty,
sum(Value_SubTotal_SAW) as SAW_Value,
sum(Qty_Subtotal_TO_MTD) as TO_DCkeToko_MTD_Qty,
sum(Value_Subtotal_TO_MTD) as TO_DCkeToko_MTD_Value,
sum(Qty_subtotal_TI_MTD) as Ttl_Receive_Toko_MTD_Qty,
sum(Value_subtotal_TI_MTD) as Ttl_Receive_Toko_MTD_Value,
sum(SAK_Qty) as SAK_Qty,
sum(SAK_Value) as SAK_Value
from MB_InvMove_BDPDCkeToko_Detail_non_kons
Where
tahun=2025				-- input sesuai tahun perikan data
and bulan=10				-- input sesuai Bulan perikan data
and kodestatustoko='R'  -- diganti berdasarkan kodestatustoko yg akan ditarik datanya.
and iddc=1				-- diganti berdasarkan iddc yg akan ditarik datanya.
group by tahun,bulan,iddc,COA
order by tahun asc, bulan asc, COA asc;

-- Query Detail Inv Movement BDP DC ke Toko --
--use MB_DC;        -- utk jalan di server JKT
--use MB_DC_SBY;    -- utk jalan di server SBY
select
Tahun,Bulan,iddc,kodetoko,namatoko,kodeStatusToko,kodeproduk,namapanjang,category,COA,
sum(Qty_SubTotal_SAW) as SAW_Qty,
sum(Value_SubTotal_SAW) as SAW_Value,
sum(Qty_Subtotal_TO_MTD) as TO_DCkeToko_MTD_Qty,
sum(Value_Subtotal_TO_MTD) as TO_DCkeToko_MTD_Value,
sum(Qty_subtotal_TI_MTD) as Ttl_Receive_Toko_MTD_Qty,
sum(Value_subtotal_TI_MTD) as Ttl_Receive_Toko_MTD_Value,
sum(SAK_Qty) as SAK_Qty,
sum(SAK_Value) as SAK_Value
from MB_InvMove_BDPDCkeToko_Detail_non_kons
Where
tahun=2025					-- input sesuai tahun perikan data
and bulan=10					-- input sesuai Bulan perikan data
and kodestatustoko='R'		-- diganti berdasarkan kodestatustoko yg akan ditarik datanya.
and iddc=1					-- diganti berdasarkan iddc yg akan ditarik datanya.
Group by
Tahun,Bulan,iddc,kodetoko,namatoko,kodeStatusToko,kodeproduk,namapanjang,category,COA
order by tahun asc, bulan asc, COA asc;


/******************************************************************************************/
--STORE to DC MOBENG
/******************************************************************************************/
-- Query Summary Inv Movement BDP Toko ke DC --
--use MB_DC;
--use MB_DC_SBY;
Declare @date1 date = '2025-10-01'
Declare @date2 date = '2025-10-31'
Declare @iddc  int  = 1
Declare @kodestatustoko varchar='R'
select * from MB_InvMove_BDPTokoKeDC_Summary_view_v5_fc(@date1,@date2,@iddc,@kodestatustoko)
Order by COA asc;

/******************************************************************************************/
-- Query Detail Inv Movement BDP Toko ke DC --
--use MB_DC;
--use MB_DC_SBY;
Declare @date1 date = '2025-10-01'		-- diganti berdasarkan tanggal awal bulan penarikan data
Declare @date2 date = '2025-10-31'		-- diganti berdasarkan tanggal akhir bulan penarikan data
Declare @iddc  int    = 1				-- diganti berdasarkan iddc data yang ditarik
Declare @kodestatustoko varchar='R'		-- diganti berdasarkan kodestatustoko data yang ditarik
Select * from MB_InvMove_BDPTokoKeDC_Detail_view_v5_fc(@date1,@date2,@iddc,@kodestatustoko) 


/******************************************************************************************/
--Inv. Movement Toko
/******************************************************************************************/
-- Query Tarik Data Inv. Movement Toko Summary -- Mobeng -- v5

--USE [MB_DC_SBY]
--USE MB_DC
Declare @date1 date ='2025-10-01'			-- diganti berdasarkan tanggal awal bulan penarikan data
Declare @date2 date ='2025-10-31'			-- diganti berdasarkan tanggal akhir bulan penarikan data
Declare @kodestatustoko varchar='R'			-- diganti berdasarkan kodestatustoko data yang diingikan
Declare @idcabang int=2						-- diganti berdasarkan idcabang data yang diingikan

Select * From dbo.MB_InvMove_Toko_Summary_view_v5_fc(@date1,@date2,@kodestatustoko,@idcabang)
order by COA Asc;

/*******************************************************************/
-- Query Tarik Data Inv. Movement Toko Detail -- Mobeng -- v5

--USE [MB_DC_SBY]
--USE MB_DC
Declare @date1 date ='2025-10-01'			-- diganti berdasarkan tanggal awal bulan penarikan data
Declare @date2 date ='2025-10-31'			-- diganti berdasarkan tanggal akhir bulan penarikan data
Declare @kodestatustoko varchar='R'			-- diganti berdasarkan kodestatustoko data yang diingikan
Declare @idcabang int=2						-- diganti berdasarkan idcabang data yang diingikan

Select  * From dbo.MB_InvMove_Toko_Detail_view_v5_fc(@date1,@date2,@kodestatustoko,@idcabang)
order by kodetoko asc,COA asc,kodeproduk asc; 


/**************************************************************************************************/
--GOOD, BOOK, BAD STOCK
/**************************************************************************************************/
Declare @date1 date	='2025-10-01'	-- diisi berdasarkan periode awal penarikan data
Declare @date2 date	='2025-10-31'	-- diisi berdasarkan periode Akhir penarikan data
Declare @iddc int	= 1				-- diisi berdasrakan iddc data yg akan ditarik
Declare @idjenisstok int	= 3		-- diisi berdasrakan idJenisStok (Jenis Gudang) data yg akan ditarik
EXEC sp_SMI_SOH_DC_byformula_per_jenisstok_view_v5 @date1,@date2,@iddc,@idjenisstok;

/**************************************************************************************************/
--GOOD STOCK TOKO REGULER 
/**************************************************************************************************/
DECLARE @date1 date='2025-10-01'
DECLARE @date2 date='2025-10-31'
DECLARE @idcabang int=2
DECLARE @kodestatustoko varchar='R'
select 
a.tglsaldo,
--a.kodetoko,
--b.idproduk,
a.kodeproduk,
a.description,
sum(a.soh_qty) as qty,
sum(a.soh_value) as value_hpp_toko,
(sum(a.soh_qty)*c.hpp) as value_hpp_dc
from smi_soh_toko_reg_byformula_view_8_3_fc(@date1,@date2,@idcabang,@kodestatustoko) as a
Join mstproduk as b on b.kodeproduk=a.kodeproduk
Join saldostokprodukdc as c on c.tglsaldo=a.tglsaldo and c.idproduk=b.idproduk
where c.idjenisstok=1 
and b.flagKonsinyasi=0
--and a.kodetoko=3021004
--and a.kodeproduk='4174601001'
group by a.tglsaldo,a.kodeproduk,a.description,b.idproduk,c.hpp;