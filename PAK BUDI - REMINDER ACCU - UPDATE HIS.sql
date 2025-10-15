--===================================================================================================================================

--UPDATE 1 [SEND1]
UPDATE public.pra_his ph
SET send1 = GREATEST((ph.reminder1::date - CURRENT_DATE), 0)
WHERE ph.reminder1 IS NOT NULL
AND (ph.send1 <> '0' OR ph.send1 IS NULL);

--UPDATE 2 [SEND2]
UPDATE public.pra_his ph
SET send2 = GREATEST((ph.reminder2::date - CURRENT_DATE), 0)
WHERE ph.reminder2 IS NOT NULL
AND (ph.send2 <> '0' OR ph.send2 IS NULL);

--UPDATE 3 [UPDATE LAST TRANSAKSI ACCU/TANGGAL]
UPDATE public.pra_his ph
SET lasttrxaccu2 = plta.tanggal
FROM public.pra_last_trx_accu plta
WHERE ph.nopolisi = plta.nopolisi
  AND ph.namacabang = plta.namacabang
  AND ph.reminder2 IS NOT NULL
  AND ph.send2 = '0';

--UPDATE 4 O[UPDATE WAREMINDER2 YES/NO]
UPDATE public.pra_his ph
SET wareminder2 = CASE
	                WHEN (ph.transactiondate::date > ph.lasttrxaccu2::date AND ph.statusaccu IS NOT NULL AND ph.statusaccu <> '')
	                  OR (ph.lasttrxaccu2::date IS NULL AND ph.statusaccu IS NOT NULL AND ph.statusaccu <> '')
	                	THEN 'YES'
	                ELSE 'NO'
            	END
WHERE ph.reminder2 IS NOT NULL
  AND ph.send2 = '0'
  
--create table public.pra_his_backup_20250922 as 
--select * from public.pra_his

--===================================================================================================================================
  
SELECT * FROM public.pra_status_accu;
SELECT * FROM public.pra_last_trx_accu;
SELECT * FROM public.pra_his_all where transactiondate=current_date-1 limit 20;
SELECT * FROM public.pra_his where transactiondate=current_date-1 and  wareminder1='YES' and send1='0';
SELECT * FROM public.pra_his where send1='0' and notelp='6289603025111'order by transactiondate;
SELECT * FROM public.pra_his where wa_status1=1;
SELECT * FROM public.pra_his where wareminder1='YES' and send1='0';
SELECT * FROM public.pra_his where wareminder1='YES' and send1='0' and send2='0';
SELECT distinct transactiondate FROM public.pra_his;
select nopolisi,count(*) from public.pra_his group by nopolisi HAVING COUNT(*) > 1;
SELECT * FROM public.pra_his where nopolisi in ('B5087TSH');

SELECT * FROM public.pra_his_all where transactiondate=current_date-2;
SELECT * FROM public.pra_his where transactiondate=current_date-4;

--create table public.pra_his_testing as
--SELECT * FROM public.pra_his where wareminder1='YES' and wa_status1='0' and send1='0'
--select * from public.pra_his_testing;
--update public.pra_his_testing set wa_status1='1';



--CEK UPDATE SEND1
select ph.*,GREATEST((ph.reminder1::date - CURRENT_DATE), 0) as send1
FROM public.pra_his ph
WHERE ph.reminder1 IS NOT NULL
--AND (ph.reminder1::date - CURRENT_DATE) > 0
AND (ph.send1 <> '0' OR ph.send1 IS NULL)
and ph.nopolisi in ('B3260ETD','G5537BNF','B4284EAE')
;

--UPDATE SEND1
--UPDATE public.pra_his ph
--SET send1 = GREATEST((ph.reminder1::date - CURRENT_DATE), 0)
--WHERE ph.reminder1 IS NOT NULL
--AND (ph.reminder1::date - CURRENT_DATE) > 0;

--UPDATE SEND1
UPDATE public.pra_his ph
SET send1 = GREATEST((ph.reminder1::date - CURRENT_DATE), 0)
WHERE ph.reminder1 IS NOT NULL
AND (ph.send1 <> '0' OR ph.send1 IS NULL);

--CEK UPDATE SEND2
select GREATEST((ph.reminder2::date - CURRENT_DATE), 0) as send2
FROM public.pra_his ph
WHERE ph.reminder2 IS NOT NULL
--AND (ph.reminder1::date - CURRENT_DATE) > 0
AND (ph.send2 <> '0' OR ph.send2 IS null)
AND ph.nopolisi in ('B4068KJZ')
;

--UPDATE SEND2
--UPDATE public.pra_his ph
--SET send2 = GREATEST((ph.reminder2::date - CURRENT_DATE), 0)
--WHERE ph.reminder2 IS NOT NULL
--AND (ph.reminder2::date - CURRENT_DATE) > 0;

--UPDATE SEND2
UPDATE public.pra_his ph
SET send2 = GREATEST((ph.reminder2::date - CURRENT_DATE), 0)
WHERE ph.reminder2 IS NOT NULL
AND (ph.send2 <> '0' OR ph.send2 IS NULL);

------------------------------------------------------------
--CEK UPDATE WAREMINDER2
SELECT 
	ph.*,
    CASE
        WHEN (ph.transactiondate::date > ph.lasttrxaccu2::date) THEN 'YES'
        ELSE 'NO'
    END AS wareminder2,
                CASE
                WHEN (ph.transactiondate::date > ph.lasttrxaccu2::date AND ph.statusaccu IS NOT NULL AND ph.statusaccu <> '')
                  OR (ph.lasttrxaccu2::date IS NULL AND ph.statusaccu IS NOT NULL AND ph.statusaccu <> '')
                THEN 'YES'
                ELSE 'NO'
            END AS wareminder2
FROM public.pra_his ph
WHERE ph.reminder2 IS NOT NULL
AND ph.send2 = '0'
and ph.nopolisi in ('B4068KJZ')
;


;


SELECT 
	plta.*
FROM public.pra_last_trx_accu plta where plta.nopolisi in ('B4068KJZ','F6537JT')
WHERE plta.reminder2 IS NOT NULL
AND plta.send2 = '0'
and plta.nopolisi in ('B4068KJZ','F6537JT')
;

SELECT 
	plta.*
FROM public.pra_his plta 
WHERE plta.reminder2 IS NOT NULL
--AND plta.send2 = '0'
and plta.nopolisi in ('B4068KJZ')
;

