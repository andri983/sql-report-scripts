--##=============
--##TRANSFER DATA
Jobs name	: smi_rms10_transaksi_member_commercial_driver
			  smi_rms11_transaksi_member_commercial_driver
			  smi_rms12_transaksi_member_commercial_driver
			  smi_rms14_transaksi_member_commercial_driver
			  smi_rms20_transaksi_member_commercial_driver
			  smi_rms21_transaksi_member_commercial_driver
			  smi_rms22_transaksi_member_commercial_driver
			  
Source Server		: rms10.planetban.co.id <SMI_DB_Reporting_JKT>
Destination Server	: POOL/localhost <DATARMS>
 
Source Query
SELECT Tanggal,Tglbayar,idcabang,namacabang,kodetokolama,kodetoko,namatoko,Nomortransaksi,Totalitem,nomorurut,idbrand,Brand,Iddivisi,Jenis,idSubDepartement,namaSubDepartement,iddepartement,Category,Kodeproduklama,idproduk,KodeProduk,namapendek,Namapanjang,qty,hpp,Subtotalhpp,HargaJualNormal,pdisc1,pdisc2,disc,HargaJualTransaksi,subtotal,statusProduk,idjenisproduk,kodemember,nopolisi,idJenisMember,NamaJenismember,notrans,namamember,Notelp,tglcreate_member,CategoryName
FROM V_SMI_Transaksi_Toko_Perjenis_Member_JKT_V3 as A
WHERE A.statusproduk<>'K'
AND A.idjenisproduk<>4
AND A.namacabang in ('Jakarta Baru')
AND A.idJenisMember in (24,25)
AND convert(date,tanggal) between convert(date,DATEADD(mm, DATEDIFF(mm, 0, GETDATE()), 0)) and convert(date,getdate()-1)

Destination
Fields	: tanggal, tglbayar, idcabang, namacabang, kodetokolama, kodetoko, namatoko, nomortransaksi, totalitem, nomorurut, idbrand, brand, iddivisi, jenis, idsubdepartement, namasubdepartement, iddepartement, category, kodeproduklama, idproduk, kodeproduk, namapendek, namapanjang, qty, hpp, subtotalhpp, hargajualnormal, pdisc1, pdisc2, disc, hargajualtransaksi, subtotal, statusproduk, idjenisproduk, kodemember, nopolisi, idjenismember, namajenismember, notrans, namamember, notelp, tglcreate_member, categoryname
table	: smi_rms10_transaksi_member_commercial_driver

--##=============
--##JOB QUERY
Name	: Report Sales Member Commarcial Driver MTD H-1
select 
tanggal as "TANGGAL", 
namacabang as "NAMA CABANG",
kodetoko as "KODE TOKO",
namatoko as "NAMA TOKO",
nomortransaksi as "NOMOR TRANSAKSI",
jenis as "JENIS", 
category as "CATEGORY", 
brand as "BRAND",
kodeproduk as "KODE PRODUK",
namapanjang as "NAMA PANJANG",
qty as "QTY",
hargajualnormal as "HARGA JUAL NORMAL",
disc as "SUBTOTAL DISC",
hargajualtransaksi as "HARGA JUAL TRANSAKSI",
subtotal as "SUBTOTAL",
namamember as "NAMA MEMBER",
kodemember as "KODE MEMBER",
nopolisi as "NO. POLISI",
notelp as "NO. TELP",
namajenismember as "NAMA JENIS MEMBER"
from (
	select tanggal, tglbayar, idcabang, namacabang, kodetokolama, kodetoko, namatoko, nomortransaksi, totalitem, nomorurut, idbrand, brand, iddivisi, jenis, idsubdepartement, namasubdepartement, iddepartement, category, kodeproduklama, idproduk, kodeproduk, namapendek, namapanjang, qty, hpp, subtotalhpp, hargajualnormal, pdisc1, pdisc2, disc, hargajualtransaksi, subtotal, statusproduk, idjenisproduk, kodemember, nopolisi, idjenismember, namajenismember, notrans, namamember, notelp, tglcreate_member, categoryname 
	from public.smi_rms10_transaksi_member_commercial_driver
	union all
	select tanggal, tglbayar, idcabang, namacabang, kodetokolama, kodetoko, namatoko, nomortransaksi, totalitem, nomorurut, idbrand, brand, iddivisi, jenis, idsubdepartement, namasubdepartement, iddepartement, category, kodeproduklama, idproduk, kodeproduk, namapendek, namapanjang, qty, hpp, subtotalhpp, hargajualnormal, pdisc1, pdisc2, disc, hargajualtransaksi, subtotal, statusproduk, idjenisproduk, kodemember, nopolisi, idjenismember, namajenismember, notrans, namamember, notelp, tglcreate_member, categoryname 
	from public.smi_rms11_transaksi_member_commercial_driver
	union all
	select tanggal, tglbayar, idcabang, namacabang, kodetokolama, kodetoko, namatoko, nomortransaksi, totalitem, nomorurut, idbrand, brand, iddivisi, jenis, idsubdepartement, namasubdepartement, iddepartement, category, kodeproduklama, idproduk, kodeproduk, namapendek, namapanjang, qty, hpp, subtotalhpp, hargajualnormal, pdisc1, pdisc2, disc, hargajualtransaksi, subtotal, statusproduk, idjenisproduk, kodemember, nopolisi, idjenismember, namajenismember, notrans, namamember, notelp, tglcreate_member, categoryname 
	from public.smi_rms12_transaksi_member_commercial_driver
	union all
	select tanggal, tglbayar, idcabang, namacabang, kodetokolama, kodetoko, namatoko, nomortransaksi, totalitem, nomorurut, idbrand, brand, iddivisi, jenis, idsubdepartement, namasubdepartement, iddepartement, category, kodeproduklama, idproduk, kodeproduk, namapendek, namapanjang, qty, hpp, subtotalhpp, hargajualnormal, pdisc1, pdisc2, disc, hargajualtransaksi, subtotal, statusproduk, idjenisproduk, kodemember, nopolisi, idjenismember, namajenismember, notrans, namamember, notelp, tglcreate_member, categoryname
	from public.smi_rms15_transaksi_member_commercial_driver
	union all
	select tanggal, tglbayar, idcabang, namacabang, kodetokolama, kodetoko, namatoko, nomortransaksi, totalitem, nomorurut, idbrand, brand, iddivisi, jenis, idsubdepartement, namasubdepartement, iddepartement, category, kodeproduklama, idproduk, kodeproduk, namapendek, namapanjang, qty, hpp, subtotalhpp, hargajualnormal, pdisc1, pdisc2, disc, hargajualtransaksi, subtotal, statusproduk, idjenisproduk, kodemember, nopolisi, idjenismember, namajenismember, notrans, namamember, notelp, tglcreate_member, categoryname
	from public.smi_rms20_transaksi_member_commercial_driver
	union all
	select tanggal, tglbayar, idcabang, namacabang, kodetokolama, kodetoko, namatoko, nomortransaksi, totalitem, nomorurut, idbrand, brand, iddivisi, jenis, idsubdepartement, namasubdepartement, iddepartement, category, kodeproduklama, idproduk, kodeproduk, namapendek, namapanjang, qty, hpp, subtotalhpp, hargajualnormal, pdisc1, pdisc2, disc, hargajualtransaksi, subtotal, statusproduk, idjenisproduk, kodemember, nopolisi, idjenismember, namajenismember, notrans, namamember, notelp, tglcreate_member, categoryname
	from public.smi_rms21_transaksi_member_commercial_driver
	union all
	select tanggal, tglbayar, idcabang, namacabang, kodetokolama, kodetoko, namatoko, nomortransaksi, totalitem, nomorurut, idbrand, brand, iddivisi, jenis, idsubdepartement, namasubdepartement, iddepartement, category, kodeproduklama, idproduk, kodeproduk, namapendek, namapanjang, qty, hpp, subtotalhpp, hargajualnormal, pdisc1, pdisc2, disc, hargajualtransaksi, subtotal, statusproduk, idjenisproduk, kodemember, nopolisi, idjenismember, namajenismember, notrans, namamember, notelp, tglcreate_member, categoryname
	from public.smi_rms22_transaksi_member_commercial_driver
) as x;

--##=============
--##JOB TEMPLATE
Sheet name	: Sales Member Commercial Driver MTD H-1
Title		:
Name		: Report Sales Member Commercial Driver MTD H-1

--##=============
--##JOB MAIL
Jobs name	: Report_Sales_Member_Commercial_Driver_& Non_Commercial_Driver_Nasional_MTD_H-1_
