-- DROP FUNCTION public.fn_pra_his();

CREATE OR REPLACE FUNCTION public.fn_pra_his()
 RETURNS void
 LANGUAGE plpgsql
AS $function$
BEGIN
    WITH 
    cte_nopolblacklist AS (
        SELECT y.nopolisi 
        FROM smi_mstplatblacklist y
    ),
    cte_transaksi AS (
        SELECT 
            x.transactiondate, 
            x.reportdate, 
            x.namacabang, 
            x.kodetoko, 
            x.nomortransaksi, 
            x.nopolisi, 
            x.namamember, 
            x.notelp, 
            x.idjenismember, 
            x.namajenismember, 
            x.statusaccuoff, 
            x.lasttrxaccu,
            CASE
                WHEN (x.transactiondate::date > x.lasttrxaccu::date AND x.statusaccuoff IS NOT NULL AND x.statusaccuoff <> '')
                  OR (x.lasttrxaccu::date IS NULL AND x.statusaccuoff IS NOT NULL AND x.statusaccuoff <> '')
                THEN 'YES'
                ELSE 'NO'
            END AS wareminder1,
            CASE
                WHEN ((x.transactiondate::date > x.lasttrxaccu::date AND x.statusaccuoff IS NOT NULL AND x.statusaccuoff <> '')
                   OR (x.lasttrxaccu::date IS NULL AND x.statusaccuoff IS NOT NULL AND x.statusaccuoff <> ''))
                THEN 
                    CASE
                        WHEN x.statusaccuoff LIKE 'RUSAK%' THEN (x.transactiondate::date + INTERVAL '14 days')::date
                        WHEN x.statusaccuoff LIKE 'LEMAH%' THEN (x.transactiondate::date + INTERVAL '21 days')::date
                        ELSE x.transactiondate::date
                    END
                ELSE NULL
            END AS reminder1,
            0 as wa_status1,
            NULL::timestamp as wa_send_date1,
            NULL as wa_xid1,
            NULL as wa_status_data1,
            NULL::date as lasttrxaccu2,
            NULL::text as wareminder2,
            CASE
                WHEN ((x.transactiondate::date > x.lasttrxaccu::date AND x.statusaccuoff IS NOT NULL AND x.statusaccuoff <> '')
                   OR (x.lasttrxaccu::date IS NULL AND x.statusaccuoff IS NOT NULL AND x.statusaccuoff <> ''))
                THEN 
					CASE
					    WHEN x.statusaccuoff LIKE 'RUSAK%' THEN (x.transactiondate::date + INTERVAL '28 days')::date -- 4 minggu
					    WHEN x.statusaccuoff LIKE 'LEMAH%' THEN (x.transactiondate::date + INTERVAL '112 days')::date -- 16 minggu
					END
                ELSE NULL
            END AS reminder2,
            0 as wa_status2,
            NULL::timestamp as wa_send_date2,
            NULL as wa_xid2,
            NULL as wa_status_data2
        FROM public.pra_his_all x
        WHERE  x.transactiondate=current_date-1
            AND (x.statusaccuoff LIKE 'LEMAH%' OR x.statusaccuoff LIKE 'RUSAK%')
            AND x.nopolisi NOT IN (SELECT nopolisi FROM cte_nopolblacklist)
        ORDER BY x.kodetoko, x.nomortransaksi
    )
    INSERT INTO public.pra_his(
									transactiondate, reportdate, namacabang, kodetoko, nomortransaksi, 
									nopolisi, namamember, notelp, idjenismember, namajenismember, 
									statusaccu, lasttrxaccu1, wareminder1, reminder1, wa_status1, wa_send_date1, wa_xid1, wa_status_data1, 
									lasttrxaccu2, wareminder2, reminder2, wa_status2, wa_send_date2, wa_xid2, wa_status_data2
									)
    SELECT 
        x.transactiondate, x.reportdate, x.namacabang, x.kodetoko, x.nomortransaksi, 
        x.nopolisi, x.namamember, x.notelp, x.idjenismember, x.namajenismember, 
        x.statusaccuoff as statusaccu, x.lasttrxaccu as lasttrxaccu1,
        x.wareminder1, x.reminder1, x.wa_status1, x.wa_send_date1, x.wa_xid1, x.wa_status_data1,
        x.lasttrxaccu2, x.wareminder2, x.reminder2, x.wa_status2, x.wa_send_date2, x.wa_xid2, x.wa_status_data2
    FROM cte_transaksi x
	WHERE NOT EXISTS (
    SELECT 1 
    FROM public.pra_his ph
    WHERE ph.nopolisi = x.nopolisi
	);
END;
$function$
;
    
    
--select * from public.pra_his;
--select * from public.pra_his_all;