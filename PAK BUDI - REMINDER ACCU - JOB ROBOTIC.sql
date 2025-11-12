--DML Query Jobs/pra_status_accu-delete
Jobs name
pra_status_accu-delete
Target	POOL/localhost <DATARMS>
  
Last Run	
Schedule DML Query	pra_status_accu-delete
 
DML Query
DELETE FROM public.pra_status_accu;
result

--Scheduled DML Query/pra_status_accu-delete
Name	pra_status_accu-delete
  
Schedule Actions	pra_status_accu-delete
Execution Date	09/03/2025 02:00:00
Interval Unit	Days
 
DML Query Jobs
pra_status_accu-delete	POOL/localhost <DATARMS>	 		pra_status_accu-delete	 

--DML Query Jobspra_last_trx_accu_rms10-insert
Jobs name
pra_last_trx_accu_rms10-insert
Target	POOL/localhost <DATARMS>
  
Last Run	09/02/2025 07:13:17
Schedule DML Query	pra_last_trx_accu
 
DML Query
SET TIMEZONE to 'UTC-07:00';
DELETE FROM public.pra_last_trx_accu;
INSERT INTO public.pra_last_trx_accu
SELECT now()insertdate,idcabang,nopolisi,category,tanggal
FROM(
	SELECT a.tanggal,a.nopolisi,a.idcabang,a.kodetoko,a.category,a.nomortransaksi,row_number() OVER (PARTITION BY a.nopolisi,a.category ORDER BY a.tanggal DESC) AS myrank
	FROM public.smi_rms10_Transaksi_Toko_Perjenis_Member_v3 a
	WHERE a.idcabang = 2
	AND a.tanggal >= (current_date - interval '1 day') - interval '36 months'
	AND a.tanggal <= current_date - interval '1 day'
	AND a.category in ('ACCU')
	AND (a.nopolisi IS NOT NULL AND a.nopolisi <> '')
	) as category_oil
where myrank=1;

--Scheduled DML Querypra_last_trx_accu
Name	pra_last_trx_accu
  
--Schedule Actions	pra_last_trx_accu
Execution Date	09/03/2025 07:00:00
Interval Unit	Days
 
DML Query Jobs
pra_last_trx_accu_rms10-insert	POOL/localhost <DATARMS>	09/02/2025 07:13:17		pra_last_trx_accu	OK

--DML Query Jobspra_his_all_rms10-insert
Jobs name
pra_his_all_rms10-insert
Target	POOL/localhost <DATARMS>
  
Last Run	09/02/2025 13:33:54
Schedule DML Query	
 
DML Query
SELECT fn_pra_his_all('smi_rms10_transaksi_toko_perjenis_member_v3');

--Scheduled DML Querypra_his_all_rms10-insert
Name	pra_his_all_rms10-insert
  
Schedule Actions	pra_his_all_rms10-insert
Execution Date	09/03/2025 08:00:00
Interval Unit	Days
 
DML Query Jobs
pra_his_all_rms10-insert	POOL/localhost <DATARMS>	09/02/2025 13:33:54		pra_his_all_rms10-insert	_do_jobs: canceling statement due to user request





Scheduled DML Query
Name
pra_
Search...
 
	Execution Date	Interval Unit
		1. pra_status_accu-delete	11/12/2025 02:00:00	Days
		2. pra_last_trx_accu		11/12/2025 07:00:00	Days
		3. pra_his_all_rms10-insert	11/12/2025 08:00:00	Days
		4. pra_his-insert			11/11/2025 09:15:00	Days
		5. pra_his-update			11/11/2025 09:30:00	Days

