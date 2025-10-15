----STOCK GOOD STOCK - BOOKING STOCK - BAD STOCK---
USE [PB_DC]
Declare @date1 date	='2025-09-01'	-- diisi berdasarkan periode awal penarikan data
Declare @date2 date	='2025-09-30'	-- diisi berdasarkan periode Akhir penarikan data
Declare @iddc int	= 1			-- diisi berdasrakan iddc data yg akan ditarik
Declare @idjenisstok int	= 1	-- diisi berdasrakan idJenisStok (Jenis Gudang) data yg akan ditarik
EXEC sp_SMI_SOH_DC_byformula_per_jenisstok_view_v11 @date1,@date2,@iddc,@idjenisstok;

---STOCK GOOD STOCK TOKO REGULER---
DECLARE @date1 date='2025-09-01'
DECLARE @date2 date='2025-09-30'
DECLARE @idcabang int= 2
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

---BDP DC ke TOKO---
use PB_DC;
select
Tahun,Bulan,iddc,kodetoko,namatoko,kodeStatusToko,kodeproduk,namapanjang,category,COA,qty_subtotal_saw,value_subtotal_saw,qty_subtotal_to_mtd,value_subtotal_to_mtd,qty_subtotal_ti_mtd,value_subtotal_ti_mtd,SAK_Qty,SAK_Value
from SMI_InvMove_BDPDCkeToko_Detail_non_kons
Where tahun=2025                -- input sesuai tahun perikan data
and bulan=09            -- input sesuai Bulan perikan data
and kodestatustoko='R'
and iddc=1                -- diganti berdasarkan kodestatustoko yg akan ditarik datanya.
order by tahun asc, bulan asc, COA asc; 

---BDP DC ke DC---
use PB_DC;
Declare @date1    date    ='2025-09-01'    -- periode awal data
Declare @date2    date    ='2025-09-30'    -- periode akhir data
Declare @iddc    int      =1                -- iddc
Declare @tahun    int     =2025            -- tahun penarikan data
Declare @Bulan    int     =09                -- bulan penarikan data
SELECT * FROM SMI_InvMove_BDPDCkeDC_Detail_view_v5_fc_msh (@date1,@date2,@iddc,@tahun,@bulan);

---BDP TOKO ke DC---
Use PB_DC;
Declare @date1 date ='2025-09-01'    -- diganti berdasarkan tanggal awal bulan penarikan data
Declare @date2 date ='2025-09-30'    -- diganti berdasarkan tanggal akhir bulan penarikan data
Declare @iddc int=1					 -- diganti berdasarkan iddc data yang ditarik
Declare @kodestatustoko varchar='R'
SELECT * FROM SMI_InvMove_BDPTokokeDC_Detail_Reg_view_v5_fc_msh (@date1,@date2,@iddc,@kodestatustoko);