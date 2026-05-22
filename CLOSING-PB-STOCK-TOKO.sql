---JKT
DECLARE @date1 date='2026-04-01'
DECLARE @date2 date='2026-04-30'
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
Join PB_DC_DPR.DBO.mstproduk as b with (nolock) on b.kodeproduk=a.kodeproduk
Join PB_DC_DPR.DBO.saldostokprodukdc as c with (nolock) on c.tglsaldo=a.tglsaldo and c.idproduk=b.idproduk
left join PB_DC_DPR.DBO.tblProduknonstock as d with (nolock) on d.idproduk=b.idproduk
where c.idjenisstok=1 
and ISNULL(d.nonstock,0)=0
group by a.tglsaldo,a.kodeproduk,a.description,b.idproduk,c.hpp;

---BDG
DECLARE @date1 date='2026-04-01'
DECLARE @date2 date='2026-04-30'
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
Join PB_DC_DPR.DBO.mstproduk as b with (nolock) on b.kodeproduk=a.kodeproduk
Join PB_DC_DPR.DBO.saldostokprodukdc as c with (nolock) on c.tglsaldo=a.tglsaldo and c.idproduk=b.idproduk
left join PB_DC_DPR.DBO.tblProduknonstock as d with (nolock) on d.idproduk=b.idproduk
where c.idjenisstok=1 
and ISNULL(d.nonstock,0)=0
group by a.tglsaldo,a.kodeproduk,a.description,b.idproduk,c.hpp;

---TNG
DECLARE @date1 date='2026-04-01'
DECLARE @date2 date='2026-04-30'
DECLARE @idcabang int= 7
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
Join PB_DC_DPR.DBO.mstproduk as b with (nolock) on b.kodeproduk=a.kodeproduk
Join PB_DC_DPR.DBO.saldostokprodukdc as c with (nolock) on c.tglsaldo=a.tglsaldo and c.idproduk=b.idproduk
left join PB_DC_DPR.DBO.tblProduknonstock as d with (nolock) on d.idproduk=b.idproduk
where c.idjenisstok=1 
and ISNULL(d.nonstock,0)=0
group by a.tglsaldo,a.kodeproduk,a.description,b.idproduk,c.hpp;

---PLG
DECLARE @date1 date='2026-04-01'
DECLARE @date2 date='2026-04-30'
DECLARE @idcabang int= 9
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
Join PB_DC_DPR.DBO.mstproduk as b with (nolock) on b.kodeproduk=a.kodeproduk
Join PB_DC_DPR.DBO.saldostokprodukdc as c with (nolock) on c.tglsaldo=a.tglsaldo and c.idproduk=b.idproduk
left join PB_DC_DPR.DBO.tblProduknonstock as d with (nolock) on d.idproduk=b.idproduk
where c.idjenisstok=1 
and ISNULL(d.nonstock,0)=0
group by a.tglsaldo,a.kodeproduk,a.description,b.idproduk,c.hpp;

---SBY
DECLARE @date1 date='2026-04-01'
DECLARE @date2 date='2026-04-30'
DECLARE @idcabang int= 3
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
Join PB_DC_DPR.DBO.mstproduk as b with (nolock) on b.kodeproduk=a.kodeproduk
Join PB_DC_DPR.DBO.saldostokprodukdc as c with (nolock) on c.tglsaldo=a.tglsaldo and c.idproduk=b.idproduk
left join PB_DC_DPR.DBO.tblProduknonstock as d with (nolock) on d.idproduk=b.idproduk
where c.idjenisstok=1 
and ISNULL(d.nonstock,0)=0
group by a.tglsaldo,a.kodeproduk,a.description,b.idproduk,c.hpp;

---SMD
DECLARE @date1 date='2026-04-01'
DECLARE @date2 date='2026-04-30'
DECLARE @idcabang int= 8
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
Join PB_DC_DPR.DBO.mstproduk as b with (nolock) on b.kodeproduk=a.kodeproduk
Join PB_DC_DPR.DBO.saldostokprodukdc as c with (nolock) on c.tglsaldo=a.tglsaldo and c.idproduk=b.idproduk
left join PB_DC_DPR.DBO.tblProduknonstock as d with (nolock) on d.idproduk=b.idproduk
where c.idjenisstok=1 
and ISNULL(d.nonstock,0)=0
group by a.tglsaldo,a.kodeproduk,a.description,b.idproduk,c.hpp;

---SMG
DECLARE @date1 date='2026-04-01'
DECLARE @date2 date='2026-04-30'
DECLARE @idcabang int= 4
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
Join PB_DC_DPR.DBO.mstproduk as b with (nolock) on b.kodeproduk=a.kodeproduk
Join PB_DC_DPR.DBO.saldostokprodukdc as c with (nolock) on c.tglsaldo=a.tglsaldo and c.idproduk=b.idproduk
left join PB_DC_DPR.DBO.tblProduknonstock as d with (nolock) on d.idproduk=b.idproduk
where c.idjenisstok=1 
and ISNULL(d.nonstock,0)=0
group by a.tglsaldo,a.kodeproduk,a.description,b.idproduk,c.hpp;

---DPR
DECLARE @date1 date='2026-04-01'
DECLARE @date2 date='2026-04-30'
DECLARE @idcabang int= 5
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
Join PB_DC_DPR.DBO.mstproduk as b with (nolock) on b.kodeproduk=a.kodeproduk
Join PB_DC_DPR.DBO.saldostokprodukdc as c with (nolock) on c.tglsaldo=a.tglsaldo and c.idproduk=b.idproduk
left join PB_DC_DPR.DBO.tblProduknonstock as d with (nolock) on d.idproduk=b.idproduk
where c.idjenisstok=1 
and ISNULL(d.nonstock,0)=0
group by a.tglsaldo,a.kodeproduk,a.description,b.idproduk,c.hpp;

---BJM
DECLARE @date1 date='2026-04-01'
DECLARE @date2 date='2026-04-30'
DECLARE @idcabang int= 10
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
Join PB_DC_BJM.DBO.mstproduk as b with (nolock) on b.kodeproduk=a.kodeproduk
Join PB_DC_BJM.DBO.saldostokprodukdc as c with (nolock) on c.tglsaldo=a.tglsaldo and c.idproduk=b.idproduk
left join PB_DC_BJM.DBO.tblProduknonstock as d with (nolock) on d.idproduk=b.idproduk
where c.idjenisstok=1 
and ISNULL(d.nonstock,0)=0
group by a.tglsaldo,a.kodeproduk,a.description,b.idproduk,c.hpp;