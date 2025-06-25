-- 0.iso_history
 refresh materialized view concurrently smi_rms01_pbho.mv_msttoko;
 refresh materialized view concurrently smi_rms01_pbho.mv_mastertoolstoko;
 refresh materialized view concurrently smi_12up.smi_mv_mst_karyawan_toko;
 refresh materialized view concurrently smi_12up.smi_mv_mst_hc_pib;
 refresh materialized view concurrently smi_rms01_pbho.smi_mv_manpower;
  
 refresh materialized view concurrently mb_rms01_mbho.mv_msttoko;--OK
 refresh materialized view concurrently mb_rms01_mbho.mv_mastertoolstoko;--OK
 refresh materialized view concurrently smi_12up.smi_mv_mst_karyawan_toko;---tidak ada
 refresh materialized view concurrently smi_12up.smi_mv_mst_hc_pib;---tidak ada
 refresh materialized view concurrently mb_rms01_mbho.smi_mv_manpower;--OK
  ----------------------------------------------------------------------------------------
  ----------------------------------------------------------------------------------------
 -- 1.iso_shrinkage_bycategory_jkt
 refresh materialized view concurrently smi_rms10_rpt.smi_mv_shrinkage_bycategory_jkt;
 refresh materialized view concurrently smi_rms10_rpt.smi_mv_shrinkage_bycategory_jkt_w1;
 refresh materialized view concurrently smi_rms10_rpt.smi_mv_shrinkage_bycategory_jkt_w2;
 refresh materialized view concurrently smi_rms10_rpt.smi_mv_shrinkage_bycategory_jkt_w3;
 refresh materialized view concurrently smi_rms10_rpt.smi_mv_shrinkage_bycategory_jkt_w4;
  ----------------------------------------------------------------------------------------
  ----------------------------------------------------------------------------------------
 
 -- mb_rms10_rpt.smi_mv_shrinkage_bycategory_jkt source

CREATE MATERIALIZED VIEW mb_rms10_rpt.smi_mv_shrinkage_bycategory_jkt
TABLESPACE pg_default
AS SELECT shrbycategory.kodetoko,
    shrbycategory.spd_avg,
    shrbycategory.intoleran_qty,
    shrbycategory.categorya,
    shrbycategory.categoryb,
    shrbycategory.categoryc,
    shrbycategory.categoryd,
    shrbycategory.categorye,
    shrbycategory.categorya * 2 + shrbycategory.categoryb * 3 + shrbycategory.categoryc * 5 + shrbycategory.categoryd * 7 + shrbycategory.categorye * 9 AS point
   FROM ( SELECT shrbc.kodetoko,
            shrbc.spd_avg,
                CASE
                    WHEN abs(shrbc.selisihamount_min1) > shrbc.toleransiamount_min1 THEN 1
                    ELSE 0
                END +
                CASE
                    WHEN abs(shrbc.selisihamount_min2) > shrbc.toleransiamount_min2 THEN 1
                    ELSE 0
                END +
                CASE
                    WHEN abs(shrbc.selisihamount_min3) > shrbc.toleransiamount_min3 THEN 1
                    ELSE 0
                END AS intoleran_qty,
                CASE
                    WHEN abs(shrbc.selisihamount_min1) > shrbc.toleransiamount_min1 AND abs(shrbc.selisihamount_min1) <= 200000::numeric THEN 1
                    ELSE 0
                END +
                CASE
                    WHEN abs(shrbc.selisihamount_min2) > shrbc.toleransiamount_min2 AND abs(shrbc.selisihamount_min2) <= 200000::numeric THEN 1
                    ELSE 0
                END +
                CASE
                    WHEN abs(shrbc.selisihamount_min3) > shrbc.toleransiamount_min3 AND abs(shrbc.selisihamount_min3) <= 200000::numeric THEN 1
                    ELSE 0
                END AS categorya,
                CASE
                    WHEN abs(shrbc.selisihamount_min1) > 200000::numeric AND abs(shrbc.selisihamount_min1) <= 500000::numeric THEN 1
                    ELSE 0
                END +
                CASE
                    WHEN abs(shrbc.selisihamount_min2) > 200000::numeric AND abs(shrbc.selisihamount_min2) <= 500000::numeric THEN 1
                    ELSE 0
                END +
                CASE
                    WHEN abs(shrbc.selisihamount_min3) > 200000::numeric AND abs(shrbc.selisihamount_min3) <= 500000::numeric THEN 1
                    ELSE 0
                END AS categoryb,
                CASE
                    WHEN abs(shrbc.selisihamount_min1) > 500000::numeric AND abs(shrbc.selisihamount_min1) <= 750000::numeric THEN 1
                    ELSE 0
                END +
                CASE
                    WHEN abs(shrbc.selisihamount_min2) > 500000::numeric AND abs(shrbc.selisihamount_min2) <= 750000::numeric THEN 1
                    ELSE 0
                END +
                CASE
                    WHEN abs(shrbc.selisihamount_min3) > 500000::numeric AND abs(shrbc.selisihamount_min3) <= 750000::numeric THEN 1
                    ELSE 0
                END AS categoryc,
                CASE
                    WHEN abs(shrbc.selisihamount_min1) > 750000::numeric AND abs(shrbc.selisihamount_min1) <= 1000000::numeric THEN 1
                    ELSE 0
                END +
                CASE
                    WHEN abs(shrbc.selisihamount_min2) > 750000::numeric AND abs(shrbc.selisihamount_min2) <= 1000000::numeric THEN 1
                    ELSE 0
                END +
                CASE
                    WHEN abs(shrbc.selisihamount_min3) > 750000::numeric AND abs(shrbc.selisihamount_min3) <= 1000000::numeric THEN 1
                    ELSE 0
                END AS categoryd,
                CASE
                    WHEN abs(shrbc.selisihamount_min1) > 1000000::numeric THEN 1
                    ELSE 0
                END +
                CASE
                    WHEN abs(shrbc.selisihamount_min2) > 1000000::numeric THEN 1
                    ELSE 0
                END +
                CASE
                    WHEN abs(shrbc.selisihamount_min3) > 1000000::numeric THEN 1
                    ELSE 0
                END AS categorye
           FROM ( SELECT s.kodetoko,
                    s.spd_min1,
                    s.spd_min2,
                    s.spd_min3,
                    round((s.spd_min1 + s.spd_min2 + s.spd_min3) / 3::numeric) AS spd_avg,
                    s.toleransi_min1,
                    s.toleransi_min2,
                    s.toleransi_min3,
                    sh.selisihamount_min1,
                    sh.selisihamount_min2,
                    sh.selisihamount_min3,
                    round(s.spd_min1 * s.toleransi_min1 / 100::numeric) AS toleransiamount_min1,
                    round(s.spd_min2 * s.toleransi_min2 / 100::numeric) AS toleransiamount_min2,
                    round(s.spd_min3 * s.toleransi_min3 / 100::numeric) AS toleransiamount_min3
                   FROM ( SELECT sx.namacabang,
                            sx.kodetoko,
                            sum(
                                CASE
                                    WHEN sx.monthly = '-1'::integer::double precision THEN sx.toleransi
                                    ELSE 0::numeric
                                END) AS toleransi_min1,
                            sum(
                                CASE
                                    WHEN sx.monthly = '-2'::integer::double precision THEN sx.toleransi
                                    ELSE 0::numeric
                                END) AS toleransi_min2,
                            sum(
                                CASE
                                    WHEN sx.monthly = '-3'::integer::double precision THEN sx.toleransi
                                    ELSE 0::numeric
                                END) AS toleransi_min3,
                            sum(
                                CASE
                                    WHEN sx.monthly = '-1'::integer::double precision THEN sx.spd
                                    ELSE 0::numeric
                                END) AS spd_min1,
                            sum(
                                CASE
                                    WHEN sx.monthly = '-2'::integer::double precision THEN sx.spd
                                    ELSE 0::numeric
                                END) AS spd_min2,
                            sum(
                                CASE
                                    WHEN sx.monthly = '-3'::integer::double precision THEN sx.spd
                                    ELSE 0::numeric
                                END) AS spd_min3
                           FROM ( SELECT sy.iyear,
                                    sy.imonth,
                                    sy.monthly,
                                    sy.namacabang,
                                    sy.kodetoko,
                                    sy.spd_hpp,
                                    sy.spd,
									CASE
									 WHEN shr.toleransi IS NULL THEN 0.020
									 ELSE shr.toleransi
									END AS toleransi
                                   FROM ( SELECT date_part('year'::text, sls.tanggal) AS iyear,
												    date_part('month'::text, sls.tanggal) AS imonth,
												    sls.namacabang,
												    sls.kodetoko,
												    date_part('year'::text, age(date_trunc('month'::text, sls.tanggal::timestamp with time zone), date_trunc('month'::text, CURRENT_DATE::timestamp with time zone))) * 12::double precision + date_part('month'::text, age(date_trunc('month'::text, sls.tanggal::timestamp with time zone), date_trunc('month'::text, CURRENT_DATE::timestamp with time zone))) AS monthly,
												    sum(sls.qty * sls.hpp) AS spd_hpp,
												    sum(sls.subtotal) AS spd
   													FROM PUBLIC.mb_rms10_transaksi_toko_perjenis_member_v3 sls
  													WHERE sls.tanggal >= date_trunc('month'::text, CURRENT_DATE + '-3 mons'::interval) AND sls.tanggal <= (date_trunc('month'::text, CURRENT_DATE + '-1 mons'::interval) + '1 mon -1 days'::interval)::date
  													GROUP BY (date_part('year'::text, sls.tanggal)), (date_part('month'::text, sls.tanggal)), sls.namacabang, sls.kodetoko, (date_part('year'::text, age(date_trunc('month'::text, sls.tanggal::timestamp with time zone), date_trunc('month'::text, CURRENT_DATE::timestamp with time zone))) * 12::double precision + date_part('month'::text, age(date_trunc('month'::text, sls.tanggal::timestamp with time zone), date_trunc('month'::text, CURRENT_DATE::timestamp with time zone))))) sy
                                     LEFT JOIN smi_shrinkage_toleransi_v shr ON shr.iyear::double precision = sy.iyear AND shr.imonth::double precision = sy.imonth) sx
                          GROUP BY sx.namacabang, sx.kodetoko) s
                     LEFT JOIN ( SELECT shr.kodetoko,
                            sum(
                                CASE
                                    WHEN shr.monthly = '-1'::integer::double precision THEN shr.selisihamount
                                    ELSE 0::numeric
                                END) AS selisihamount_min1,
                            sum(
                                CASE
                                    WHEN shr.monthly = '-2'::integer::double precision THEN shr.selisihamount
                                    ELSE 0::numeric
                                END) AS selisihamount_min2,
                            sum(
                                CASE
                                    WHEN shr.monthly = '-3'::integer::double precision THEN shr.selisihamount
                                    ELSE 0::numeric
                                END) AS selisihamount_min3
                           FROM ( SELECT so.kodetoko,
                                    date_part('year'::text, age(date_trunc('month'::text, so.tglsoadjusment::timestamp with time zone), date_trunc('month'::text, CURRENT_DATE::timestamp with time zone))) * 12::double precision + date_part('month'::text, age(date_trunc('month'::text, so.tglsoadjusment::timestamp with time zone), date_trunc('month'::text, CURRENT_DATE::timestamp with time zone))) AS monthly,
                                    sum(so.totalhrgjual) AS selisihamount
                                   FROM mb_rms10_mbdc.mb_so_toko_cab_rpt_hist_rms10 so
                                  WHERE so.tglsoadjusment >= date_trunc('month'::text, CURRENT_DATE + '-3 mons'::interval) AND so.tglsoadjusment <= (date_trunc('month'::text, CURRENT_DATE + '-1 mons'::interval) + '1 mon -1 days'::interval)::date AND so.qtyselisih <> 0::numeric
                                  GROUP BY so.kodetoko, (date_part('year'::text, age(date_trunc('month'::text, so.tglsoadjusment::timestamp with time zone), date_trunc('month'::text, CURRENT_DATE::timestamp with time zone))) * 12::double precision + date_part('month'::text, age(date_trunc('month'::text, so.tglsoadjusment::timestamp with time zone), date_trunc('month'::text, CURRENT_DATE::timestamp with time zone))))) shr
                          GROUP BY shr.kodetoko) sh ON sh.kodetoko::numeric = s.kodetoko) shrbc) shrbycategory
WITH DATA;

-- View indexes:
CREATE UNIQUE INDEX smi_mv_shrinkage_bycategory_jkt_kodetoko_unique_idx ON mb_rms10_rpt.smi_mv_shrinkage_bycategory_jkt USING btree (kodetoko);

 ----------------------------------------------------------------------------------------

-- DROP FUNCTION mb_rms10_rpt.fn_smi_shrinkage_bycategory_jkt(timestamp, timestamp);

CREATE OR REPLACE FUNCTION mb_rms10_rpt.fn_smi_shrinkage_bycategory_jkt(start_date timestamp without time zone, end_date timestamp without time zone)
 RETURNS TABLE(kdtoko integer, spdavg numeric, intoleranqty integer, cata integer, catb integer, catc integer, catd integer, cate integer, pnt integer)
 LANGUAGE plpgsql
AS $function$
begin
    return query
    
    select kodetoko::int ikodetoko, spd_avg::numeric nspd_avg,
            intoleran_qty iintoleran_qty, categoryA icategoryA, categoryB icategoryB, categoryC icategoryC, categoryD icategoryD, categoryE icategoryE,
            (categoryA * 2) + (categoryB * 3) + (categoryC * 5) + (categoryD * 7) + (categoryE * 9) ipoint
    from(
            select kodetoko, 
                    spd_avg, 
                    (case when abs(selisihamount_min1) > toleransiamount_min1 then 1 else 0 end) + 
                    (case when abs(selisihamount_min2) > toleransiamount_min2 then 1 else 0 end) +
                    (case when abs(selisihamount_min3) > toleransiamount_min3 then 1 else 0 end) intoleran_qty,
                    (case when abs(selisihamount_min1) > toleransiamount_min1 and abs(selisihamount_min1) <= 200000 then 1 else 0 end) + 
                    (case when abs(selisihamount_min2) > toleransiamount_min2 and abs(selisihamount_min2) <= 200000 then 1 else 0 end) +
                    (case when abs(selisihamount_min3) > toleransiamount_min3 and abs(selisihamount_min3) <= 200000 then 1 else 0 end) categoryA,
                    (case when abs(selisihamount_min1) > 200000 and abs(selisihamount_min1) <= 500000 then 1 else 0 end) + 
                    (case when abs(selisihamount_min2) > 200000 and abs(selisihamount_min2) <= 500000 then 1 else 0 end) +
                    (case when abs(selisihamount_min3) > 200000 and abs(selisihamount_min3) <= 500000 then 1 else 0 end) categoryB,
                    (case when abs(selisihamount_min1) > 500000 and abs(selisihamount_min1) <= 750000 then 1 else 0 end) +
                    (case when abs(selisihamount_min2) > 500000 and abs(selisihamount_min2) <= 750000 then 1 else 0 end) +
                    (case when abs(selisihamount_min3) > 500000 and abs(selisihamount_min3) <= 750000 then 1 else 0 end) categoryC,
                    (case when abs(selisihamount_min1) > 750000 and abs(selisihamount_min1) <= 1000000 then 1 else 0 end) +
                    (case when abs(selisihamount_min2) > 750000 and abs(selisihamount_min2) <= 1000000 then 1 else 0 end) +
                    (case when abs(selisihamount_min3) > 750000 and abs(selisihamount_min3) <= 1000000 then 1 else 0 end) categoryD,
                    (case when abs(selisihamount_min1) > 1000000 then 1 else 0 end) +
                    (case when abs(selisihamount_min2) > 1000000 then 1 else 0 end) +
                    (case when abs(selisihamount_min3) > 1000000 then 1 else 0 end) categoryE	
            from(
                    select s.kodetoko, 
                            s.spd_min1, s.spd_min2, s.spd_min3, 
                            round((s.spd_min1 + s.spd_min2 + s.spd_min3)/3) spd_avg, 
                            s.toleransi_min1, s.toleransi_min2, s.toleransi_min3,
                            sh.selisihamount_min1, sh.selisihamount_min2, sh.selisihamount_min3,
                            round(s.spd_min1 * s.toleransi_min1 / 100) toleransiamount_min1,
                            round(s.spd_min2 * s.toleransi_min2 / 100) toleransiamount_min2,
                            round(s.spd_min3 * s.toleransi_min3 / 100) toleransiamount_min3
                    from(
                            select namacabang, kodetoko,
                                    sum(case when sx.monthly = -1 then sx.toleransi else 0 end) toleransi_min1,
                                    sum(case when sx.monthly = -2 then sx.toleransi else 0 end) toleransi_min2,
                                    sum(case when sx.monthly = -3 then sx.toleransi else 0 end) toleransi_min3,
                                    sum(case when sx.monthly = -1 then sx.spd else 0 end) spd_min1,
                                sum(case when sx.monthly = -2 then sx.spd else 0 end) spd_min2,
                                sum(case when sx.monthly = -3 then sx.spd else 0 end) spd_min3
                        from(
                                    select sy.iYear, sy.iMonth, monthly, namacabang, kodetoko, spd_hpp, spd, case when shr.toleransi is null then 0.020 else shr.toleransi end toleransi
                                    from
                                    (select date_part('year', sls.tanggal) iYear, date_part('month', sls.tanggal) iMonth, namacabang, sls.kodetoko, 
                                            extract(year from age(date_trunc('month', sls.tanggal) , date_trunc('month', current_date))) * 12 + extract(month from age(date_trunc('month', sls.tanggal) , date_trunc('month', current_date))) monthly,
                                       sum(sls.qty * sls.hpp) spd_hpp,
                                      sum(sls.subtotal) spd
                                    from PUBLIC.mb_rms10_transaksi_toko_perjenis_member_v3 sls 
                                    where (
                                            sls.tanggal between start_date and end_date
                                        )
                                    group by date_part('year', sls.tanggal), date_part('month', sls.tanggal), namacabang, sls.kodetoko, 
                                            extract(year from age(date_trunc('month', sls.tanggal) , date_trunc('month', current_date))) * 12 + extract(month from age(date_trunc('month', sls.tanggal) , date_trunc('month', current_date)))
                                    )sy
                                    left join public.smi_shrinkage_toleransi_v shr on shr.iYear = sy.iYear and shr.iMonth = sy.iMonth
                            )sx	group by namacabang, kodetoko
                    )s 
                    left join(
                            select kodetoko,
                                    sum(case when shr.monthly = -1 then shr.selisihamount else 0 end) selisihamount_min1,
                                    sum(case when shr.monthly = -2 then shr.selisihamount else 0 end) selisihamount_min2,
                                    sum(case when shr.monthly = -3 then shr.selisihamount else 0 end) selisihamount_min3
                            from(
                                    select 
                                            so.kodetoko,
                                            extract(year from age(date_trunc('month', so.tglsoadjusment) , date_trunc('month', current_date))) * 12 + extract(month from age(date_trunc('month', so.tglsoadjusment) , date_trunc('month', current_date))) monthly,
                                            sum(so.totalhrgjual) selisihamount
                                    FROM mb_rms10_mbdc.mb_so_toko_cab_rpt_hist_rms10 so
                                    where so.tglsoadjusment between start_date and end_date 
    --				and so.kodetoko in(3021001, 3021096)
                                    and so.qtyselisih != 0
    --				and so.idUser not like '%KSR%' 
    --				and so.idUser not like '%KTO%' 
    --				and so.idUser not like '%IC%' 
    --				and so.iduser not in ('rozano','ROZANO','KTIO','KTP')
                                    group by so.kodetoko, extract(year from age(date_trunc('month', so.tglsoadjusment) , date_trunc('month', current_date))) * 12 + extract(month from age(date_trunc('month', so.tglsoadjusment) , date_trunc('month', current_date)))
                            )shr group by kodetoko
                    )sh on sh.kodetoko = s.kodetoko
            )shrbc
    )shrbycategory;
end; $function$
;
 ----------------------------------------------------------------------------------------

-- mb_rms10_rpt.smi_mv_shrinkage_bycategory_jkt_w1 source

CREATE MATERIALIZED VIEW mb_rms10_rpt.smi_mv_shrinkage_bycategory_jkt_w1
TABLESPACE pg_default
AS SELECT fn_smi_shrinkage_bycategory_jkt.kdtoko,
    fn_smi_shrinkage_bycategory_jkt.spdavg,
    fn_smi_shrinkage_bycategory_jkt.intoleranqty,
    fn_smi_shrinkage_bycategory_jkt.cata,
    fn_smi_shrinkage_bycategory_jkt.catb,
    fn_smi_shrinkage_bycategory_jkt.catc,
    fn_smi_shrinkage_bycategory_jkt.catd,
    fn_smi_shrinkage_bycategory_jkt.cate,
    fn_smi_shrinkage_bycategory_jkt.pnt
   FROM mb_rms10_rpt.fn_smi_shrinkage_bycategory_jkt(date_trunc('month'::text, CURRENT_DATE + '-1 mons'::interval)::date::timestamp without time zone, (date_trunc('month'::text, CURRENT_DATE + '-1 mons'::interval) + '6 days'::interval)::date::timestamp without time zone) fn_smi_shrinkage_bycategory_jkt(kdtoko, spdavg, intoleranqty, cata, catb, catc, catd, cate, pnt)
WITH DATA;

-- View indexes:
CREATE UNIQUE INDEX smi_mv_shrinkage_bycategory_jkt_w1_kdtoko_unique_idx ON mb_rms10_rpt.smi_mv_shrinkage_bycategory_jkt_w1 USING btree (kdtoko);

 ----------------------------------------------------------------------------------------

-- mb_rms10_rpt.smi_mv_shrinkage_bycategory_jkt_w2 source

CREATE MATERIALIZED VIEW mb_rms10_rpt.smi_mv_shrinkage_bycategory_jkt_w2
TABLESPACE pg_default
AS SELECT fn_smi_shrinkage_bycategory_jkt.kdtoko,
    fn_smi_shrinkage_bycategory_jkt.spdavg,
    fn_smi_shrinkage_bycategory_jkt.intoleranqty,
    fn_smi_shrinkage_bycategory_jkt.cata,
    fn_smi_shrinkage_bycategory_jkt.catb,
    fn_smi_shrinkage_bycategory_jkt.catc,
    fn_smi_shrinkage_bycategory_jkt.catd,
    fn_smi_shrinkage_bycategory_jkt.cate,
    fn_smi_shrinkage_bycategory_jkt.pnt
   FROM mb_rms10_rpt.fn_smi_shrinkage_bycategory_jkt((date_trunc('month'::text, CURRENT_DATE + '-1 mons'::interval) + '7 days'::interval)::date::timestamp without time zone, (date_trunc('month'::text, CURRENT_DATE + '-1 mons'::interval) + '13 days'::interval)::date::timestamp without time zone) fn_smi_shrinkage_bycategory_jkt(kdtoko, spdavg, intoleranqty, cata, catb, catc, catd, cate, pnt)
WITH DATA;

-- View indexes:
CREATE UNIQUE INDEX smi_mv_shrinkage_bycategory_jkt_w2_kdtoko_unique_idx ON mb_rms10_rpt.smi_mv_shrinkage_bycategory_jkt_w2 USING btree (kdtoko);

 ----------------------------------------------------------------------------------------

 -- mb_rms10_rpt.smi_mv_shrinkage_bycategory_jkt_w3 source

CREATE MATERIALIZED VIEW mb_rms10_rpt.smi_mv_shrinkage_bycategory_jkt_w3
TABLESPACE pg_default
AS SELECT fn_smi_shrinkage_bycategory_jkt.kdtoko,
    fn_smi_shrinkage_bycategory_jkt.spdavg,
    fn_smi_shrinkage_bycategory_jkt.intoleranqty,
    fn_smi_shrinkage_bycategory_jkt.cata,
    fn_smi_shrinkage_bycategory_jkt.catb,
    fn_smi_shrinkage_bycategory_jkt.catc,
    fn_smi_shrinkage_bycategory_jkt.catd,
    fn_smi_shrinkage_bycategory_jkt.cate,
    fn_smi_shrinkage_bycategory_jkt.pnt
   FROM mb_rms10_rpt.fn_smi_shrinkage_bycategory_jkt((date_trunc('month'::text, CURRENT_DATE + '-1 mons'::interval) + '14 days'::interval)::date::timestamp without time zone, (date_trunc('month'::text, CURRENT_DATE + '-1 mons'::interval) + '20 days'::interval)::date::timestamp without time zone) fn_smi_shrinkage_bycategory_jkt(kdtoko, spdavg, intoleranqty, cata, catb, catc, catd, cate, pnt)
WITH DATA;

-- View indexes:
CREATE UNIQUE INDEX smi_mv_shrinkage_bycategory_jkt_w3_kdtoko_unique_idx ON mb_rms10_rpt.smi_mv_shrinkage_bycategory_jkt_w3 USING btree (kdtoko);

 ----------------------------------------------------------------------------------------

-- mb_rms10_rpt.smi_mv_shrinkage_bycategory_jkt_w4 source

CREATE MATERIALIZED VIEW mb_rms10_rpt.smi_mv_shrinkage_bycategory_jkt_w4
TABLESPACE pg_default
AS SELECT fn_smi_shrinkage_bycategory_jkt.kdtoko,
    fn_smi_shrinkage_bycategory_jkt.spdavg,
    fn_smi_shrinkage_bycategory_jkt.intoleranqty,
    fn_smi_shrinkage_bycategory_jkt.cata,
    fn_smi_shrinkage_bycategory_jkt.catb,
    fn_smi_shrinkage_bycategory_jkt.catc,
    fn_smi_shrinkage_bycategory_jkt.catd,
    fn_smi_shrinkage_bycategory_jkt.cate,
    fn_smi_shrinkage_bycategory_jkt.pnt
   FROM mb_rms10_rpt.fn_smi_shrinkage_bycategory_jkt((date_trunc('month'::text, CURRENT_DATE + '-1 mons'::interval) + '21 days'::interval)::date::timestamp without time zone, (date_trunc('month'::text, CURRENT_DATE + '-1 mons'::interval) + '1 mon'::interval - '1 day'::interval)::date::timestamp without time zone) fn_smi_shrinkage_bycategory_jkt(kdtoko, spdavg, intoleranqty, cata, catb, catc, catd, cate, pnt)
WITH DATA;

-- View indexes:
CREATE UNIQUE INDEX smi_mv_shrinkage_bycategory_jkt_w4_kdtoko_unique_idx ON mb_rms10_rpt.smi_mv_shrinkage_bycategory_jkt_w4 USING btree (kdtoko);

 ----------------------------------------------------------------------------------------
 ----------------------------------------------------------------------------------------
-- 5.iso_shrinkage_bycategory_sby
 refresh materialized view concurrently smi_rms20_rpt.smi_mv_shrinkage_bycategory_sby;
 refresh materialized view concurrently smi_rms20_rpt.smi_mv_shrinkage_bycategory_sby_w1;
 refresh materialized view concurrently smi_rms20_rpt.smi_mv_shrinkage_bycategory_sby_w2;
 refresh materialized view concurrently smi_rms20_rpt.smi_mv_shrinkage_bycategory_sby_w3;
 refresh materialized view concurrently smi_rms20_rpt.smi_mv_shrinkage_bycategory_sby_w4;
 ----------------------------------------------------------------------------------------
 ----------------------------------------------------------------------------------------
 
-- DROP FUNCTION mb_rms20_rpt.fn_smi_shrinkage_bycategory_sby(timestamp, timestamp);

CREATE OR REPLACE FUNCTION mb_rms20_rpt.fn_smi_shrinkage_bycategory_sby(start_date timestamp without time zone, end_date timestamp without time zone)
 RETURNS TABLE(kdtoko integer, spdavg numeric, intoleranqty integer, cata integer, catb integer, catc integer, catd integer, cate integer, pnt integer)
 LANGUAGE plpgsql
AS $function$
begin
    return query
    
    select kodetoko::int ikodetoko, spd_avg::numeric nspd_avg,
            intoleran_qty iintoleran_qty, categoryA icategoryA, categoryB icategoryB, categoryC icategoryC, categoryD icategoryD, categoryE icategoryE,
            (categoryA * 2) + (categoryB * 3) + (categoryC * 5) + (categoryD * 7) + (categoryE * 9) ipoint
    from(

        select kodetoko, 
                spd_avg, 
                (case when abs(selisihamount_min1) > toleransiamount_min1 then 1 else 0 end) + 
                (case when abs(selisihamount_min2) > toleransiamount_min2 then 1 else 0 end) +
                (case when abs(selisihamount_min3) > toleransiamount_min3 then 1 else 0 end) intoleran_qty,
                (case when abs(selisihamount_min1) > toleransiamount_min1 and abs(selisihamount_min1) <= 200000 then 1 else 0 end) + 
                (case when abs(selisihamount_min2) > toleransiamount_min2 and abs(selisihamount_min2) <= 200000 then 1 else 0 end) +
                (case when abs(selisihamount_min3) > toleransiamount_min3 and abs(selisihamount_min3) <= 200000 then 1 else 0 end) categoryA,
                (case when abs(selisihamount_min1) > 200000 and abs(selisihamount_min1) <= 500000 then 1 else 0 end) + 
                (case when abs(selisihamount_min2) > 200000 and abs(selisihamount_min2) <= 500000 then 1 else 0 end) +
                (case when abs(selisihamount_min3) > 200000 and abs(selisihamount_min3) <= 500000 then 1 else 0 end) categoryB,
                (case when abs(selisihamount_min1) > 500000 and abs(selisihamount_min1) <= 750000 then 1 else 0 end) +
                (case when abs(selisihamount_min2) > 500000 and abs(selisihamount_min2) <= 750000 then 1 else 0 end) +
                (case when abs(selisihamount_min3) > 500000 and abs(selisihamount_min3) <= 750000 then 1 else 0 end) categoryC,
                (case when abs(selisihamount_min1) > 750000 and abs(selisihamount_min1) <= 1000000 then 1 else 0 end) +
                (case when abs(selisihamount_min2) > 750000 and abs(selisihamount_min2) <= 1000000 then 1 else 0 end) +
                (case when abs(selisihamount_min3) > 750000 and abs(selisihamount_min3) <= 1000000 then 1 else 0 end) categoryD,
                (case when abs(selisihamount_min1) > 1000000 then 1 else 0 end) +
                (case when abs(selisihamount_min2) > 1000000 then 1 else 0 end) +
                (case when abs(selisihamount_min3) > 1000000 then 1 else 0 end) categoryE	
        from(
                select s.kodetoko, 
                        s.spd_min1, s.spd_min2, s.spd_min3, 
                        round((s.spd_min1 + s.spd_min2 + s.spd_min3)/3) spd_avg, 
                        s.toleransi_min1, s.toleransi_min2, s.toleransi_min3,
                        sh.selisihamount_min1, sh.selisihamount_min2, sh.selisihamount_min3,
                        round(s.spd_min1 * s.toleransi_min1 / 100) toleransiamount_min1,
                        round(s.spd_min2 * s.toleransi_min2 / 100) toleransiamount_min2,
                        round(s.spd_min3 * s.toleransi_min3 / 100) toleransiamount_min3
                from(
                        select namacabang, kodetoko,
                                sum(case when sx.monthly = -1 then sx.toleransi else 0 end) toleransi_min1,
                                sum(case when sx.monthly = -2 then sx.toleransi else 0 end) toleransi_min2,
                                sum(case when sx.monthly = -3 then sx.toleransi else 0 end) toleransi_min3,
                                sum(case when sx.monthly = -1 then sx.spd else 0 end) spd_min1,
                            sum(case when sx.monthly = -2 then sx.spd else 0 end) spd_min2,
                            sum(case when sx.monthly = -3 then sx.spd else 0 end) spd_min3
                    from(
                                select sy.iYear, sy.iMonth, monthly, namacabang, kodetoko, spd_hpp, spd, case when shr.toleransi is null then 0.020 else shr.toleransi end toleransi
                                from
                                (select date_part('year', sls.tanggal) iYear, date_part('month', sls.tanggal) iMonth, namacabang, sls.kodetoko, 
                                        extract(year from age(date_trunc('month', sls.tanggal) , date_trunc('month', current_date))) * 12 + extract(month from age(date_trunc('month', sls.tanggal) , date_trunc('month', current_date))) monthly,
                                   sum(sls.qty * sls.hpp) spd_hpp,
                                  sum(sls.subtotal) spd
                                from PUBLIC.mb_rms20_transaksi_toko_perjenis_member_v3 sls 
                                where (
                                        sls.tanggal between start_date and end_date
                                    )
                                group by date_part('year', sls.tanggal), date_part('month', sls.tanggal), namacabang, sls.kodetoko, 
                                        extract(year from age(date_trunc('month', sls.tanggal) , date_trunc('month', current_date))) * 12 + extract(month from age(date_trunc('month', sls.tanggal) , date_trunc('month', current_date)))
                                )sy
                                left join public.smi_shrinkage_toleransi_v shr on shr.iYear = sy.iYear and shr.iMonth = sy.iMonth
                        )sx	group by namacabang, kodetoko
                )s 
                left join(
                        select kodetoko,
                                sum(case when shr.monthly = -1 then shr.selisihamount else 0 end) selisihamount_min1,
                                sum(case when shr.monthly = -2 then shr.selisihamount else 0 end) selisihamount_min2,
                                sum(case when shr.monthly = -3 then shr.selisihamount else 0 end) selisihamount_min3
                        from(
                                select 
                                        so.kodetoko,
                                        extract(year from age(date_trunc('month', so.tglsoadjusment) , date_trunc('month', current_date))) * 12 + extract(month from age(date_trunc('month', so.tglsoadjusment) , date_trunc('month', current_date))) monthly,
                                        sum(so.totalhrgjual) selisihamount
                                FROM mb_rms20_mbdc.mb_so_toko_cab_rpt_hist_rms20 so
                                where so.tglsoadjusment between start_date and end_date 
--				and so.kodetoko in(3021001, 3021096)
                                and so.qtyselisih != 0
--				and so.idUser not like '%KSR%' 
--				and so.idUser not like '%KTO%' 
--				and so.idUser not like '%IC%' 
--				and so.iduser not in ('rozano','ROZANO','KTIO','KTP')
                                group by so.kodetoko, extract(year from age(date_trunc('month', so.tglsoadjusment) , date_trunc('month', current_date))) * 12 + extract(month from age(date_trunc('month', so.tglsoadjusment) , date_trunc('month', current_date)))
                        )shr group by kodetoko
                )sh on sh.kodetoko = s.kodetoko
        )shrbc
    )shrbycategory;
end; $function$
;

 ----------------------------------------------------------------------------------------

-- mb_rms20_rpt.smi_mv_shrinkage_bycategory_sby source

CREATE MATERIALIZED VIEW mb_rms20_rpt.smi_mv_shrinkage_bycategory_sby
TABLESPACE pg_default
AS SELECT fn_smi_shrinkage_bycategory_sby.kdtoko,
    fn_smi_shrinkage_bycategory_sby.spdavg,
    fn_smi_shrinkage_bycategory_sby.intoleranqty,
    fn_smi_shrinkage_bycategory_sby.cata,
    fn_smi_shrinkage_bycategory_sby.catb,
    fn_smi_shrinkage_bycategory_sby.catc,
    fn_smi_shrinkage_bycategory_sby.catd,
    fn_smi_shrinkage_bycategory_sby.cate,
    fn_smi_shrinkage_bycategory_sby.pnt
   FROM mb_rms20_rpt.fn_smi_shrinkage_bycategory_sby(date_trunc('month'::text, CURRENT_DATE + '-3 mons'::interval), (date_trunc('month'::text, CURRENT_DATE + '-1 mons'::interval) + '1 mon -1 days'::interval)::date::timestamp without time zone) fn_smi_shrinkage_bycategory_sby(kdtoko, spdavg, intoleranqty, cata, catb, catc, catd, cate, pnt)
WITH DATA;

-- View indexes:
CREATE UNIQUE INDEX smi_mv_shrinkage_bycategory_sby_kdtoko_unique_idx ON mb_rms20_rpt.smi_mv_shrinkage_bycategory_sby USING btree (kdtoko);

 ----------------------------------------------------------------------------------------

-- mb_rms20_rpt.smi_mv_shrinkage_bycategory_sby_w1 source

CREATE MATERIALIZED VIEW mb_rms20_rpt.smi_mv_shrinkage_bycategory_sby_w1
TABLESPACE pg_default
AS SELECT fn_smi_shrinkage_bycategory_sby.kdtoko,
    fn_smi_shrinkage_bycategory_sby.spdavg,
    fn_smi_shrinkage_bycategory_sby.intoleranqty,
    fn_smi_shrinkage_bycategory_sby.cata,
    fn_smi_shrinkage_bycategory_sby.catb,
    fn_smi_shrinkage_bycategory_sby.catc,
    fn_smi_shrinkage_bycategory_sby.catd,
    fn_smi_shrinkage_bycategory_sby.cate,
    fn_smi_shrinkage_bycategory_sby.pnt
   FROM mb_rms20_rpt.fn_smi_shrinkage_bycategory_sby(date_trunc('month'::text, CURRENT_DATE + '-1 mons'::interval)::date::timestamp without time zone, (date_trunc('month'::text, CURRENT_DATE + '-1 mons'::interval) + '6 days'::interval)::date::timestamp without time zone) fn_smi_shrinkage_bycategory_sby(kdtoko, spdavg, intoleranqty, cata, catb, catc, catd, cate, pnt)
WITH DATA;

-- View indexes:
CREATE UNIQUE INDEX smi_mv_shrinkage_bycategory_sby_w1_kdtoko_unique_idx ON mb_rms20_rpt.smi_mv_shrinkage_bycategory_sby_w1 USING btree (kdtoko);

 ----------------------------------------------------------------------------------------

-- mb_rms20_rpt.smi_mv_shrinkage_bycategory_sby_w2 source

CREATE MATERIALIZED VIEW mb_rms20_rpt.smi_mv_shrinkage_bycategory_sby_w2
TABLESPACE pg_default
AS SELECT fn_smi_shrinkage_bycategory_sby.kdtoko,
    fn_smi_shrinkage_bycategory_sby.spdavg,
    fn_smi_shrinkage_bycategory_sby.intoleranqty,
    fn_smi_shrinkage_bycategory_sby.cata,
    fn_smi_shrinkage_bycategory_sby.catb,
    fn_smi_shrinkage_bycategory_sby.catc,
    fn_smi_shrinkage_bycategory_sby.catd,
    fn_smi_shrinkage_bycategory_sby.cate,
    fn_smi_shrinkage_bycategory_sby.pnt
   FROM mb_rms20_rpt.fn_smi_shrinkage_bycategory_sby((date_trunc('month'::text, CURRENT_DATE + '-1 mons'::interval) + '7 days'::interval)::date::timestamp without time zone, (date_trunc('month'::text, CURRENT_DATE + '-1 mons'::interval) + '13 days'::interval)::date::timestamp without time zone) fn_smi_shrinkage_bycategory_sby(kdtoko, spdavg, intoleranqty, cata, catb, catc, catd, cate, pnt)
WITH DATA;

-- View indexes:
CREATE UNIQUE INDEX smi_mv_shrinkage_bycategory_sby_w2_kdtoko_unique_idx ON mb_rms20_rpt.smi_mv_shrinkage_bycategory_sby_w2 USING btree (kdtoko);

 ----------------------------------------------------------------------------------------

-- mb_rms20_rpt.smi_mv_shrinkage_bycategory_sby_w3 source

CREATE MATERIALIZED VIEW mb_rms20_rpt.smi_mv_shrinkage_bycategory_sby_w3
TABLESPACE pg_default
AS SELECT fn_smi_shrinkage_bycategory_sby.kdtoko,
    fn_smi_shrinkage_bycategory_sby.spdavg,
    fn_smi_shrinkage_bycategory_sby.intoleranqty,
    fn_smi_shrinkage_bycategory_sby.cata,
    fn_smi_shrinkage_bycategory_sby.catb,
    fn_smi_shrinkage_bycategory_sby.catc,
    fn_smi_shrinkage_bycategory_sby.catd,
    fn_smi_shrinkage_bycategory_sby.cate,
    fn_smi_shrinkage_bycategory_sby.pnt
   FROM mb_rms20_rpt.fn_smi_shrinkage_bycategory_sby((date_trunc('month'::text, CURRENT_DATE + '-1 mons'::interval) + '14 days'::interval)::date::timestamp without time zone, (date_trunc('month'::text, CURRENT_DATE + '-1 mons'::interval) + '20 days'::interval)::date::timestamp without time zone) fn_smi_shrinkage_bycategory_sby(kdtoko, spdavg, intoleranqty, cata, catb, catc, catd, cate, pnt)
WITH DATA;

-- View indexes:
CREATE UNIQUE INDEX smi_mv_shrinkage_bycategory_sby_w3_kdtoko_unique_idx ON mb_rms20_rpt.smi_mv_shrinkage_bycategory_sby_w3 USING btree (kdtoko);

 ----------------------------------------------------------------------------------------

-- mb_rms20_rpt.smi_mv_shrinkage_bycategory_sby_w4 source

CREATE MATERIALIZED VIEW mb_rms20_rpt.smi_mv_shrinkage_bycategory_sby_w4
TABLESPACE pg_default
AS SELECT fn_smi_shrinkage_bycategory_sby.kdtoko,
    fn_smi_shrinkage_bycategory_sby.spdavg,
    fn_smi_shrinkage_bycategory_sby.intoleranqty,
    fn_smi_shrinkage_bycategory_sby.cata,
    fn_smi_shrinkage_bycategory_sby.catb,
    fn_smi_shrinkage_bycategory_sby.catc,
    fn_smi_shrinkage_bycategory_sby.catd,
    fn_smi_shrinkage_bycategory_sby.cate,
    fn_smi_shrinkage_bycategory_sby.pnt
   FROM mb_rms20_rpt.fn_smi_shrinkage_bycategory_sby((date_trunc('month'::text, CURRENT_DATE + '-1 mons'::interval) + '21 days'::interval)::date::timestamp without time zone, (date_trunc('month'::text, CURRENT_DATE + '-1 mons'::interval) + '1 mon'::interval - '1 day'::interval)::date::timestamp without time zone) fn_smi_shrinkage_bycategory_sby(kdtoko, spdavg, intoleranqty, cata, catb, catc, catd, cate, pnt)
WITH DATA;

-- View indexes:
CREATE UNIQUE INDEX smi_mv_shrinkage_bycategory_sby_w4_kdtoko_unique_idx ON mb_rms20_rpt.smi_mv_shrinkage_bycategory_sby_w4 USING btree (kdtoko);

 ----------------------------------------------------------------------------------------
 ----------------------------------------------------------------------------------------
-- 8.iso_manpower_history_temp
 refresh materialized view concurrently smi_rms01_rpt.smi_mv_manpower_history_temp;
 ----------------------------------------------------------------------------------------
 ----------------------------------------------------------------------------------------
 
-- mb_rms01_mbho.smi_mv_manpower_history_temp source

CREATE MATERIALIZED VIEW mb_rms01_mbho.smi_mv_manpower_history_temp
TABLESPACE pg_default
AS SELECT mp.idcabang,
    b.namacabang,
    mp.nik,
    mp.nama,
    mp.posisi,
    mp.kodetoko,
    tk.namatoko,
    shr.spd_avg,
    shr.intoleran_qty,
    shr.categorya,
    shr.categoryb,
    shr.categoryc,
    shr.categoryd,
    shr.categorye,
    shr.point
   FROM ( SELECT DISTINCT selmp.idcabang,
            selmp.kodetoko,
            selmp.nik,
            selmp.nama,
            selmp.posisi,
            selmp.effective_date
           FROM ( SELECT slmp.idcabang,
                    slmp.kodetoko,
                    slmp.nik,
                    slmp.nama,
                    slmp.posisi,
                    slmp.effective_date,
                    rank() OVER (PARTITION BY slmp.kodetoko ORDER BY slmp.nik) AS finalrank_no
                   FROM ( SELECT pmp.idcabang,
                            pmp.kodetoko,
                            pmp.nik,
                            pmp.nama,
                            pmp.posisi,
                            pmp.effective_date,
                            rank() OVER (PARTITION BY pmp.kodetoko ORDER BY pmp.effective_date DESC) AS rank_no
                           FROM ( SELECT t.idcabang,
                                    t.kodetoko,
CASE
 WHEN mp1.nik IS NULL THEN mp2.nik
 ELSE mp1.nik
END AS nik,
CASE
 WHEN mp1.name IS NULL THEN mp2.name
 ELSE mp1.name
END AS nama,
CASE
 WHEN mp1.posisi IS NULL THEN mp2.posisi
 ELSE mp1.posisi
END AS posisi,
CASE
 WHEN mp1.effective_date IS NULL THEN mp2.effective_date
 ELSE mp1.effective_date
END AS effective_date
                                   FROM ( SELECT smi_mv_manpower.idcabang,
    smi_mv_manpower.kodetoko
   FROM mb_rms01_mbho.smi_mv_manpower
  GROUP BY smi_mv_manpower.idcabang, smi_mv_manpower.kodetoko) t
                                     LEFT JOIN mb_rms01_mbho.smi_mv_manpower mp1 ON mp1.idcabang = t.idcabang AND mp1.kodetoko = t.kodetoko AND mp1.posisi = 'Leader'::text
                                     LEFT JOIN mb_rms01_mbho.smi_mv_manpower mp2 ON mp2.idcabang = t.idcabang AND mp2.kodetoko = t.kodetoko AND mp2.posisi = 'Ass. Leader'::text) pmp) slmp
                  WHERE slmp.rank_no = 1) selmp
          WHERE selmp.finalrank_no = 1) mp
     JOIN mb_rms01_mbho.msttoko tk ON tk.kodetoko = mp.kodetoko
     JOIN mb_rms10_mbdc.mv_mstcabang b ON b.idcabang = mp.idcabang
     JOIN ( SELECT smi_mv_shrinkage_bycategory_jkt.kodetoko,
            smi_mv_shrinkage_bycategory_jkt.spd_avg,
            smi_mv_shrinkage_bycategory_jkt.intoleran_qty,
            smi_mv_shrinkage_bycategory_jkt.categorya,
            smi_mv_shrinkage_bycategory_jkt.categoryb,
            smi_mv_shrinkage_bycategory_jkt.categoryc,
            smi_mv_shrinkage_bycategory_jkt.categoryd,
            smi_mv_shrinkage_bycategory_jkt.categorye,
            smi_mv_shrinkage_bycategory_jkt.point
           FROM smi_rms10_rpt.smi_mv_shrinkage_bycategory_jkt
        UNION
         SELECT smi_mv_shrinkage_bycategory_sby.kdtoko,
            smi_mv_shrinkage_bycategory_sby.spdavg,
            smi_mv_shrinkage_bycategory_sby.intoleranqty,
            smi_mv_shrinkage_bycategory_sby.cata,
            smi_mv_shrinkage_bycategory_sby.catb,
            smi_mv_shrinkage_bycategory_sby.catc,
            smi_mv_shrinkage_bycategory_sby.catd,
            smi_mv_shrinkage_bycategory_sby.cate,
            smi_mv_shrinkage_bycategory_sby.pnt
           FROM smi_rms20_rpt.smi_mv_shrinkage_bycategory_sby) shr ON shr.kodetoko = mp.kodetoko::numeric
  ORDER BY mp.kodetoko
WITH DATA;

-- View indexes:
CREATE UNIQUE INDEX smi_mv_manpower_history_temp_kodetoko_unique_idx ON mb_rms01_mbho.smi_mv_manpower_history_temp USING btree (kodetoko);

 ----------------------------------------------------------------------------------------
 ----------------------------------------------------------------------------------------
-- 9.iso_sales_monitor_jkt
 refresh materialized view smi_rms10_pbdc.mv_smi_fastmoving_produk20Persen_jkt;
 refresh materialized view concurrently smi_rms10_pbdc.smi_mv_fastmoving_sales_mtd_jkt;
 refresh materialized view concurrently smi_rms10_pbdc.smi_mv_listitem_sales_mtd_jkt;
 refresh materialized view concurrently smi_rms10_pbdc.smi_mv_sales_mtd_jkt;

 refresh materialized view mb_rms10_mbdc.mv_smi_fastmoving_produk20Persen_jkt;--OK
 refresh materialized view concurrently mb_rms10_mbdc.smi_mv_fastmoving_sales_mtd_jkt;--OK
 refresh materialized view concurrently mb_rms10_mbdc.smi_mv_listitem_sales_mtd_jkt;--OK
 refresh materialized view concurrently mb_rms10_mbdc.smi_mv_sales_mtd_jkt;--OK
 ----------------------------------------------------------------------------------------
 ----------------------------------------------------------------------------------------
 
-- mb_rms10_mbdc.mv_smi_fastmoving_produk20persen_jkt source

CREATE MATERIALIZED VIEW mb_rms10_mbdc.mv_smi_fastmoving_produk20persen_jkt
TABLESPACE pg_default
AS SELECT fm.namacabang,
    fm.kodetoko,
    fm.namatoko,
    fm.kodeproduk,
    fm.monthly,
    fm.qty,
    fm.val,
    fm.qty_rank
   FROM ( SELECT f.namacabang,
            f.kodetoko,
            f.namatoko,
            f.monthly,
            f.kodeproduk,
            f.qty,
            f.val,
            rank() OVER (PARTITION BY f.monthly, f.kodetoko ORDER BY f.val DESC) AS qty_rank
           FROM ( SELECT sls.namacabang,
                    sls.kodetoko,
                    upper(sls.namatoko::text) AS namatoko,
                    date_part('year'::text, age(date_trunc('month'::text, sls.tanggal::timestamp with time zone), date_trunc('month'::text, CURRENT_DATE::timestamp with time zone))) * 12::double precision + date_part('month'::text, age(date_trunc('month'::text, sls.tanggal::timestamp with time zone), date_trunc('month'::text, CURRENT_DATE::timestamp with time zone))) AS monthly,
                    sls.kodeproduk,
                    sum(sls.qty) AS qty,
                    sum(sls.subtotal) AS val
                   FROM PUBLIC.mb_rms10_transaksi_toko_perjenis_member_v3 sls
                  WHERE sls.tanggal >= date_trunc('month'::text, CURRENT_DATE + '-4 mons'::interval) AND sls.tanggal <= (date_trunc('month'::text, CURRENT_DATE + '-1 mons'::interval) + '1 mon -1 days'::interval)::date OR sls.tanggal >= (date_trunc('month'::text, CURRENT_DATE + '-4 mons'::interval) + '-1 years'::interval) AND sls.tanggal <= ((date_trunc('month'::text, CURRENT_DATE + '-1 mons'::interval) + '1 mon -1 days'::interval)::date + '-1 years'::interval)
                  GROUP BY sls.namacabang, sls.kodetoko, (upper(sls.namatoko::text)), (date_part('year'::text, age(date_trunc('month'::text, sls.tanggal::timestamp with time zone), date_trunc('month'::text, CURRENT_DATE::timestamp with time zone))) * 12::double precision + date_part('month'::text, age(date_trunc('month'::text, sls.tanggal::timestamp with time zone), date_trunc('month'::text, CURRENT_DATE::timestamp with time zone)))), sls.kodeproduk) f) fm
     JOIN ( SELECT c.namacabang,
            c.kodetoko,
            c.namatoko,
            c.monthly,
            round(20::double precision / 100::double precision * count(c.kodeproduk)::double precision) AS maxcnt
           FROM ( SELECT sls.namacabang,
                    sls.kodetoko,
                    upper(sls.namatoko::text) AS namatoko,
                    date_part('year'::text, age(date_trunc('month'::text, sls.tanggal::timestamp with time zone), date_trunc('month'::text, CURRENT_DATE::timestamp with time zone))) * 12::double precision + date_part('month'::text, age(date_trunc('month'::text, sls.tanggal::timestamp with time zone), date_trunc('month'::text, CURRENT_DATE::timestamp with time zone))) AS monthly,
                    sls.kodeproduk
                   FROM PUBLIC.mb_rms10_transaksi_toko_perjenis_member_v3 sls
                  WHERE sls.tanggal >= date_trunc('month'::text, CURRENT_DATE + '-4 mons'::interval) AND sls.tanggal <= (date_trunc('month'::text, CURRENT_DATE + '-1 mons'::interval) + '1 mon -1 days'::interval)::date OR sls.tanggal >= (date_trunc('month'::text, CURRENT_DATE + '-4 mons'::interval) + '-1 years'::interval) AND sls.tanggal <= ((date_trunc('month'::text, CURRENT_DATE + '-1 mons'::interval) + '1 mon -1 days'::interval)::date + '-1 years'::interval)
                  GROUP BY sls.namacabang, sls.kodetoko, (upper(sls.namatoko::text)), (date_part('year'::text, age(date_trunc('month'::text, sls.tanggal::timestamp with time zone), date_trunc('month'::text, CURRENT_DATE::timestamp with time zone))) * 12::double precision + date_part('month'::text, age(date_trunc('month'::text, sls.tanggal::timestamp with time zone), date_trunc('month'::text, CURRENT_DATE::timestamp with time zone)))), sls.kodeproduk) c
          GROUP BY c.namacabang, c.kodetoko, c.namatoko, c.monthly) cnt ON cnt.namacabang::text = fm.namacabang::text AND cnt.kodetoko = fm.kodetoko AND cnt.monthly = fm.monthly
  WHERE fm.qty_rank::double precision < cnt.maxcnt
WITH DATA;

 ----------------------------------------------------------------------------------------

-- mb_rms10_mbdc.smi_mv_fastmoving_sales_mtd_jkt source

CREATE MATERIALIZED VIEW mb_rms10_mbdc.smi_mv_fastmoving_sales_mtd_jkt
TABLESPACE pg_default
AS SELECT sm.namacabang,
    sm.kodetoko,
    t.namatoko,
    sum(sm.wd_min1) AS wd_min1,
    sum(sm.wd_min2) AS wd_min2,
    sum(sm.wd_min3) AS wd_min3,
    sum(sm.wd_min4) AS wd_min4,
    sum(sm.qty_min1) AS qty_min1,
    sum(sm.qty_min2) AS qty_min2,
    sum(sm.qty_min3) AS qty_min3,
    sum(sm.qty_min4) AS qty_min4,
    sum(sm.spdhpp_min1) AS spdhpp_min1,
    sum(sm.spdhpp_min2) AS spdhpp_min2,
    sum(sm.spdhpp_min3) AS spdhpp_min3,
    sum(sm.spdhpp_min4) AS spdhpp_min4,
    sum(sm.spd_min1) AS spd_min1,
    sum(sm.spd_min2) AS spd_min2,
    sum(sm.spd_min3) AS spd_min3,
    sum(sm.spd_min4) AS spd_min4,
    sum(sm.spdhpp_min1ly) AS spdhpp_min1ly,
    sum(sm.spdhpp_min2ly) AS spdhpp_min2ly,
    sum(sm.spdhpp_min3ly) AS spdhpp_min3ly,
    sum(sm.spdhpp_min4ly) AS spdhpp_min4ly,
    sum(sm.spd_min1ly) AS spd_min1ly,
    sum(sm.spd_min2ly) AS spd_min2ly,
    sum(sm.spd_min3ly) AS spd_min3ly,
    sum(sm.spd_min4ly) AS spd_min4ly,
    sum(sm.qty_min1ly) AS qty_min1ly,
    sum(sm.qty_min2ly) AS qty_min2ly,
    sum(sm.qty_min3ly) AS qty_min3ly,
    sum(sm.qty_min4ly) AS qty_min4ly,
    sum(sm.totalstokhpp_min1) AS totalstokhpp_min1,
    sum(sm.totalstokhpp_min2) AS totalstokhpp_min2,
    sum(sm.totalstokhpp_min3) AS totalstokhpp_min3,
    sum(sm.totalstokhpp_min4) AS totalstokhpp_min4
   FROM ( SELECT s.namacabang,
            s.kodetoko,
                CASE
                    WHEN wd.monthly = '-1'::integer::double precision THEN wd.workdays
                    ELSE 0::bigint
                END AS wd_min1,
                CASE
                    WHEN wd.monthly = '-2'::integer::double precision THEN wd.workdays
                    ELSE 0::bigint
                END AS wd_min2,
                CASE
                    WHEN wd.monthly = '-3'::integer::double precision THEN wd.workdays
                    ELSE 0::bigint
                END AS wd_min3,
                CASE
                    WHEN wd.monthly = '-4'::integer::double precision THEN wd.workdays
                    ELSE 0::bigint
                END AS wd_min4,
                CASE
                    WHEN wd.monthly = '-1'::integer::double precision THEN s.qty
                    ELSE 0::numeric
                END AS qty_min1,
                CASE
                    WHEN wd.monthly = '-2'::integer::double precision THEN s.qty
                    ELSE 0::numeric
                END AS qty_min2,
                CASE
                    WHEN wd.monthly = '-3'::integer::double precision THEN s.qty
                    ELSE 0::numeric
                END AS qty_min3,
                CASE
                    WHEN wd.monthly = '-4'::integer::double precision THEN s.qty
                    ELSE 0::numeric
                END AS qty_min4,
                CASE
                    WHEN wd.monthly = '-1'::integer::double precision THEN s.spd_hpp
                    ELSE 0::numeric
                END AS spdhpp_min1,
                CASE
                    WHEN wd.monthly = '-2'::integer::double precision THEN s.spd_hpp
                    ELSE 0::numeric
                END AS spdhpp_min2,
                CASE
                    WHEN wd.monthly = '-3'::integer::double precision THEN s.spd_hpp
                    ELSE 0::numeric
                END AS spdhpp_min3,
                CASE
                    WHEN wd.monthly = '-4'::integer::double precision THEN s.spd_hpp
                    ELSE 0::numeric
                END AS spdhpp_min4,
                CASE
                    WHEN wd.monthly = '-1'::integer::double precision THEN s.spd
                    ELSE 0::numeric
                END AS spd_min1,
                CASE
                    WHEN wd.monthly = '-2'::integer::double precision THEN s.spd
                    ELSE 0::numeric
                END AS spd_min2,
                CASE
                    WHEN wd.monthly = '-3'::integer::double precision THEN s.spd
                    ELSE 0::numeric
                END AS spd_min3,
                CASE
                    WHEN wd.monthly = '-4'::integer::double precision THEN s.spd
                    ELSE 0::numeric
                END AS spd_min4,
                CASE
                    WHEN wd.monthly = '-1'::integer::double precision THEN s.spd_hpply
                    ELSE 0::numeric
                END AS spdhpp_min1ly,
                CASE
                    WHEN wd.monthly = '-2'::integer::double precision THEN s.spd_hpply
                    ELSE 0::numeric
                END AS spdhpp_min2ly,
                CASE
                    WHEN wd.monthly = '-3'::integer::double precision THEN s.spd_hpply
                    ELSE 0::numeric
                END AS spdhpp_min3ly,
                CASE
                    WHEN wd.monthly = '-4'::integer::double precision THEN s.spd_hpply
                    ELSE 0::numeric
                END AS spdhpp_min4ly,
                CASE
                    WHEN wd.monthly = '-1'::integer::double precision THEN s.spdly
                    ELSE 0::numeric
                END AS spd_min1ly,
                CASE
                    WHEN wd.monthly = '-2'::integer::double precision THEN s.spdly
                    ELSE 0::numeric
                END AS spd_min2ly,
                CASE
                    WHEN wd.monthly = '-3'::integer::double precision THEN s.spdly
                    ELSE 0::numeric
                END AS spd_min3ly,
                CASE
                    WHEN wd.monthly = '-4'::integer::double precision THEN s.spdly
                    ELSE 0::numeric
                END AS spd_min4ly,
                CASE
                    WHEN wd.monthly = '-1'::integer::double precision THEN s.qtyly
                    ELSE 0::numeric
                END AS qty_min1ly,
                CASE
                    WHEN wd.monthly = '-2'::integer::double precision THEN s.qtyly
                    ELSE 0::numeric
                END AS qty_min2ly,
                CASE
                    WHEN wd.monthly = '-3'::integer::double precision THEN s.qtyly
                    ELSE 0::numeric
                END AS qty_min3ly,
                CASE
                    WHEN wd.monthly = '-4'::integer::double precision THEN s.qtyly
                    ELSE 0::numeric
                END AS qty_min4ly,
                CASE
                    WHEN wd.monthly = '-1'::integer::double precision THEN sb.totalstokhpp
                    ELSE 0::numeric
                END AS totalstokhpp_min1,
                CASE
                    WHEN wd.monthly = '-2'::integer::double precision THEN sb.totalstokhpp
                    ELSE 0::numeric
                END AS totalstokhpp_min2,
                CASE
                    WHEN wd.monthly = '-3'::integer::double precision THEN sb.totalstokhpp
                    ELSE 0::numeric
                END AS totalstokhpp_min3,
                CASE
                    WHEN wd.monthly = '-4'::integer::double precision THEN sb.totalstokhpp
                    ELSE 0::numeric
                END AS totalstokhpp_min4
           FROM ( SELECT slshpp.namacabang,
                    slshpp.kodetoko,
                    slshpp.monthly,
                    sum(slshpp.qty) AS qty,
                    sum(slshpp.qtyly) AS qtyly,
                    sum(slshpp.spd_hpp) AS spd_hpp,
                    sum(slshpp.spd_hpply) AS spd_hpply,
                    sum(slshpp.spd) AS spd,
                    sum(slshpp.spdly) AS spdly
                   FROM ( SELECT s_1.namacabang,
                            s_1.kodetoko,
                                CASE
                                    WHEN s_1.monthly = '-13'::integer::double precision THEN '-1'::integer::double precision
                                    WHEN s_1.monthly = '-14'::integer::double precision THEN '-2'::integer::double precision
                                    WHEN s_1.monthly = '-15'::integer::double precision THEN '-3'::integer::double precision
                                    WHEN s_1.monthly = '-16'::integer::double precision THEN '-4'::integer::double precision
                                    ELSE s_1.monthly
                                END AS monthly,
                                CASE
                                    WHEN s_1.monthly >= '-4'::integer::double precision AND s_1.monthly <= '-1'::integer::double precision THEN s_1.qty
                                    ELSE 0::numeric
                                END AS qty,
                                CASE
                                    WHEN s_1.monthly >= '-16'::integer::double precision AND s_1.monthly <= '-13'::integer::double precision THEN s_1.qty
                                    ELSE 0::numeric
                                END AS qtyly,
                                CASE
                                    WHEN s_1.monthly >= '-4'::integer::double precision AND s_1.monthly <= '-1'::integer::double precision THEN s_1.spd_hpp
                                    ELSE 0::numeric
                                END AS spd_hpp,
                                CASE
                                    WHEN s_1.monthly >= '-16'::integer::double precision AND s_1.monthly <= '-13'::integer::double precision THEN s_1.spd_hpp
                                    ELSE 0::numeric
                                END AS spd_hpply,
                                CASE
                                    WHEN s_1.monthly >= '-4'::integer::double precision AND s_1.monthly <= '-1'::integer::double precision THEN s_1.spd
                                    ELSE 0::numeric
                                END AS spd,
                                CASE
                                    WHEN s_1.monthly >= '-16'::integer::double precision AND s_1.monthly <= '-13'::integer::double precision THEN s_1.spd
                                    ELSE 0::numeric
                                END AS spdly
                           FROM ( SELECT a.namacabang,
                                    a.kodetoko,
                                    a.monthly,
                                    sum(a.qty) AS qty,
                                    sum(a.spd_hpp) AS spd_hpp,
                                    sum(a.spd) AS spd
                                   FROM ( 
                                   			SELECT 
                                   				sls.namacabang,
											    sls.kodetoko,
											    sls.kodeproduk,
											    date_part('year'::text, age(date_trunc('month'::text, sls.tanggal::timestamp with time zone), date_trunc('month'::text, CURRENT_DATE::timestamp with time zone))) * 12::double precision + date_part('month'::text, age(date_trunc('month'::text, sls.tanggal::timestamp with time zone), date_trunc('month'::text, CURRENT_DATE::timestamp with time zone))) AS monthly,
											    sum(sls.qty) AS qty,
											    sum(sls.qty * sls.hpp) AS spd_hpp,
											    sum(sls.subtotal) AS spd
										   	FROM PUBLIC.mb_rms10_transaksi_toko_perjenis_member_v3 sls
										  	WHERE sls.tanggal >= date_trunc('month'::text, CURRENT_DATE + '-4 mons'::interval) AND sls.tanggal <= (date_trunc('month'::text, CURRENT_DATE + '-1 mons'::interval) + '1 mon -1 days'::interval)::date OR sls.tanggal >= (date_trunc('month'::text, CURRENT_DATE + '-4 mons'::interval) + '-1 years'::interval) AND sls.tanggal <= ((date_trunc('month'::text, CURRENT_DATE + '-1 mons'::interval) + '1 mon -1 days'::interval)::date + '-1 years'::interval)
										  	GROUP BY sls.namacabang, sls.kodetoko, sls.kodeproduk, (date_part('year'::text, age(date_trunc('month'::text, sls.tanggal::timestamp with time zone), date_trunc('month'::text, CURRENT_DATE::timestamp with time zone))) * 12::double precision + date_part('month'::text, age(date_trunc('month'::text, sls.tanggal::timestamp with time zone), date_trunc('month'::text, CURRENT_DATE::timestamp with time zone))))
										  ) a
                                     JOIN mb_rms10_mbdc.mv_smi_fastmoving_produk20persen_jkt p ON p.kodetoko = a.kodetoko AND p.namacabang::text = a.namacabang::text AND p.kodeproduk::text = a.kodeproduk::text AND p.monthly = a.monthly
                                  GROUP BY a.namacabang, a.kodetoko, a.monthly) s_1) slshpp
                  GROUP BY slshpp.namacabang, slshpp.kodetoko, slshpp.monthly) s
             JOIN ( SELECT slsday.namacabang,
                    slsday.kodetoko,
                    slsday.monthly,
                    count(slsday.tanggal) AS workdays
                   FROM ( SELECT a.namacabang,
                            a.kodetoko,
                            a.monthly,
                            a.tanggal
                           FROM (
                           			SELECT 
                           				sls.namacabang,
	                                    sls.kodetoko,
	                                    sls.kodeproduk,
	                                    date_part('year'::text, age(date_trunc('month'::text, sls.tanggal::timestamp with time zone), date_trunc('month'::text, CURRENT_DATE::timestamp with time zone))) * 12::double precision + date_part('month'::text, age(date_trunc('month'::text, sls.tanggal::timestamp with time zone), date_trunc('month'::text, CURRENT_DATE::timestamp with time zone))) AS monthly,
	                                    sls.tanggal
                                   	FROM PUBLIC.mb_rms10_transaksi_toko_perjenis_member_v3 sls
                                  	WHERE sls.tanggal >= date_trunc('month'::text, CURRENT_DATE + '-4 mons'::interval) AND sls.tanggal <= (date_trunc('month'::text, CURRENT_DATE + '-1 mons'::interval) + '1 mon -1 days'::interval)::date
                                  	GROUP BY sls.namacabang, sls.kodetoko, sls.kodeproduk, (date_part('year'::text, age(date_trunc('month'::text, sls.tanggal::timestamp with time zone), date_trunc('month'::text, CURRENT_DATE::timestamp with time zone))) * 12::double precision + date_part('month'::text, age(date_trunc('month'::text, sls.tanggal::timestamp with time zone), date_trunc('month'::text, CURRENT_DATE::timestamp with time zone)))), sls.tanggal
                                  ) a
                             JOIN mb_rms10_mbdc.mv_smi_fastmoving_produk20persen_jkt p ON p.kodetoko = a.kodetoko AND p.namacabang::text = a.namacabang::text AND p.kodeproduk::text = a.kodeproduk::text AND p.monthly = a.monthly
                          GROUP BY a.namacabang, a.kodetoko, a.monthly, a.tanggal) slsday
                  GROUP BY slsday.namacabang, slsday.kodetoko, slsday.monthly) wd ON s.namacabang::text = wd.namacabang::text AND s.kodetoko = wd.kodetoko AND s.monthly = wd.monthly
             JOIN ( SELECT psb.idcabang,
                    psb.namacabang,
                    psb.kodetoko,
                    psb.monthly,
                    sum(psb.totalstokhpp) AS totalstokhpp
                   FROM (
                   			SELECT a.idcabang,
                            a.namacabang,
                            a.kodetoko,
                            a.kodeproduk,
                                CASE
                                    WHEN a.tglsaldo = (date_trunc('month'::text, CURRENT_DATE + '-1 mons'::interval) + '1 mon -1 days'::interval)::date THEN '-1'::integer
                                    WHEN a.tglsaldo = (date_trunc('month'::text, CURRENT_DATE + '-2 mons'::interval) + '1 mon -1 days'::interval)::date THEN '-2'::integer
                                    WHEN a.tglsaldo = (date_trunc('month'::text, CURRENT_DATE + '-3 mons'::interval) + '1 mon -1 days'::interval)::date THEN '-3'::integer
                                    WHEN a.tglsaldo = (date_trunc('month'::text, CURRENT_DATE + '-4 mons'::interval) + '1 mon -1 days'::interval)::date THEN '-4'::integer
                                    ELSE NULL::integer
                                END AS monthly,
                            sum(a.qty * a.hpp) AS totalstokhpp
                           	FROM mb_rms10_mbdc.mb_rms_saldostokproduktoko_v2_cab_jkt a
                           	JOIN mb_rms01_mbho."MstProduk" b ON a.kodeproduk::text = b."kodeProduk"::text
                          	WHERE b."statusData" <> 9 
                          	AND b."idJenisProduk" <> 4 
                          	AND a.qty <> 0::numeric 
                          	AND (a.tglsaldo = (date_trunc('month'::text, CURRENT_DATE + '-1 mons'::interval) + '1 mon -1 days'::interval)::date OR a.tglsaldo = (date_trunc('month'::text, CURRENT_DATE + '-2 mons'::interval) + '1 mon -1 days'::interval)::date OR a.tglsaldo = (date_trunc('month'::text, CURRENT_DATE + '-3 mons'::interval) + '1 mon -1 days'::interval)::date OR a.tglsaldo = (date_trunc('month'::text, CURRENT_DATE + '-4 mons'::interval) + '1 mon -1 days'::interval)::date)
                          	GROUP BY a.idcabang, a.namacabang, a.kodetoko, a.kodeproduk, (
                                CASE
                                    WHEN a.tglsaldo = (date_trunc('month'::text, CURRENT_DATE + '-1 mons'::interval) + '1 mon -1 days'::interval)::date THEN '-1'::integer
                                    WHEN a.tglsaldo = (date_trunc('month'::text, CURRENT_DATE + '-2 mons'::interval) + '1 mon -1 days'::interval)::date THEN '-2'::integer
                                    WHEN a.tglsaldo = (date_trunc('month'::text, CURRENT_DATE + '-3 mons'::interval) + '1 mon -1 days'::interval)::date THEN '-3'::integer
                                    WHEN a.tglsaldo = (date_trunc('month'::text, CURRENT_DATE + '-4 mons'::interval) + '1 mon -1 days'::interval)::date THEN '-4'::integer
                                    ELSE NULL::integer
                                END)
                         ) psb
                     JOIN mb_rms10_mbdc.mv_smi_fastmoving_produk20persen_jkt p ON p.kodetoko::character varying::text = psb.kodetoko::text AND p.namacabang::text = psb.namacabang::text AND p.kodeproduk::text = psb.kodeproduk::text AND p.monthly = psb.monthly::double precision
                  GROUP BY psb.idcabang, psb.namacabang, psb.kodetoko, psb.monthly) sb ON sb.namacabang::text = wd.namacabang::text AND sb.kodetoko::integer::numeric = wd.kodetoko AND sb.monthly::double precision = wd.monthly
          GROUP BY s.namacabang, s.kodetoko, wd.monthly, wd.workdays, s.qty, s.spd_hpp, s.spd, s.spd_hpply, s.spdly, s.qtyly, sb.totalstokhpp) sm
     JOIN mb_rms10_mbdc.mv_msttoko t ON t.kodetoko::numeric = sm.kodetoko
  GROUP BY sm.namacabang, sm.kodetoko, t.namatoko
WITH DATA;

-- View indexes:
CREATE UNIQUE INDEX smi_mv_fastmoving_sales_mtd_jkt_kodetoko_unique_idx ON mb_rms10_mbdc.smi_mv_fastmoving_sales_mtd_jkt USING btree (kodetoko);

 ----------------------------------------------------------------------------------------

-- mb_rms10_mbdc.smi_mv_listitem_sales_mtd_jkt source

CREATE MATERIALIZED VIEW mb_rms10_mbdc.smi_mv_listitem_sales_mtd_jkt
TABLESPACE pg_default
AS SELECT sm.namacabang,
    sm.kodetoko,
    sum(sm.wd_min1) AS wd_min1,
    sum(sm.wd_min2) AS wd_min2,
    sum(sm.wd_min3) AS wd_min3,
    sum(sm.wd_min4) AS wd_min4,
    sum(sm.spdhpp_min1) AS spdhpp_min1,
    sum(sm.spdhpp_min2) AS spdhpp_min2,
    sum(sm.spdhpp_min3) AS spdhpp_min3,
    sum(sm.spdhpp_min4) AS spdhpp_min4,
    sum(sm.spd_min1) AS spd_min1,
    sum(sm.spd_min2) AS spd_min2,
    sum(sm.spd_min3) AS spd_min3,
    sum(sm.spd_min4) AS spd_min4,
    sum(sm.spdhpp_min1ly) AS spdhpp_min1ly,
    sum(sm.spdhpp_min2ly) AS spdhpp_min2ly,
    sum(sm.spdhpp_min3ly) AS spdhpp_min3ly,
    sum(sm.spdhpp_min4ly) AS spdhpp_min4ly,
    sum(sm.spd_min1ly) AS spd_min1ly,
    sum(sm.spd_min2ly) AS spd_min2ly,
    sum(sm.spd_min3ly) AS spd_min3ly,
    sum(sm.spd_min4ly) AS spd_min4ly,
    sum(sm.totalstokhpp_min1) AS totalstokhpp_min1,
    sum(sm.totalstokhpp_min2) AS totalstokhpp_min2,
    sum(sm.totalstokhpp_min3) AS totalstokhpp_min3,
    sum(sm.totalstokhpp_min4) AS totalstokhpp_min4
   FROM ( SELECT s.namacabang,
            s.kodetoko,
                CASE
                    WHEN wd.monthly = '-1'::integer::double precision THEN wd.workdays
                    ELSE 0::bigint
                END AS wd_min1,
                CASE
                    WHEN wd.monthly = '-2'::integer::double precision THEN wd.workdays
                    ELSE 0::bigint
                END AS wd_min2,
                CASE
                    WHEN wd.monthly = '-3'::integer::double precision THEN wd.workdays
                    ELSE 0::bigint
                END AS wd_min3,
                CASE
                    WHEN wd.monthly = '-4'::integer::double precision THEN wd.workdays
                    ELSE 0::bigint
                END AS wd_min4,
                CASE
                    WHEN wd.monthly = '-1'::integer::double precision THEN s.spd_hpp
                    ELSE 0::numeric
                END AS spdhpp_min1,
                CASE
                    WHEN wd.monthly = '-2'::integer::double precision THEN s.spd_hpp
                    ELSE 0::numeric
                END AS spdhpp_min2,
                CASE
                    WHEN wd.monthly = '-3'::integer::double precision THEN s.spd_hpp
                    ELSE 0::numeric
                END AS spdhpp_min3,
                CASE
                    WHEN wd.monthly = '-4'::integer::double precision THEN s.spd_hpp
                    ELSE 0::numeric
                END AS spdhpp_min4,
                CASE
                    WHEN wd.monthly = '-1'::integer::double precision THEN s.spd
                    ELSE 0::numeric
                END AS spd_min1,
                CASE
                    WHEN wd.monthly = '-2'::integer::double precision THEN s.spd
                    ELSE 0::numeric
                END AS spd_min2,
                CASE
                    WHEN wd.monthly = '-3'::integer::double precision THEN s.spd
                    ELSE 0::numeric
                END AS spd_min3,
                CASE
                    WHEN wd.monthly = '-4'::integer::double precision THEN s.spd
                    ELSE 0::numeric
                END AS spd_min4,
                CASE
                    WHEN wd.monthly = '-1'::integer::double precision THEN s.spd_hpply
                    ELSE 0::numeric
                END AS spdhpp_min1ly,
                CASE
                    WHEN wd.monthly = '-2'::integer::double precision THEN s.spd_hpply
                    ELSE 0::numeric
                END AS spdhpp_min2ly,
                CASE
                    WHEN wd.monthly = '-3'::integer::double precision THEN s.spd_hpply
                    ELSE 0::numeric
                END AS spdhpp_min3ly,
                CASE
                    WHEN wd.monthly = '-4'::integer::double precision THEN s.spd_hpply
                    ELSE 0::numeric
                END AS spdhpp_min4ly,
                CASE
                    WHEN wd.monthly = '-1'::integer::double precision THEN s.spdly
                    ELSE 0::numeric
                END AS spd_min1ly,
                CASE
                    WHEN wd.monthly = '-2'::integer::double precision THEN s.spdly
                    ELSE 0::numeric
                END AS spd_min2ly,
                CASE
                    WHEN wd.monthly = '-3'::integer::double precision THEN s.spdly
                    ELSE 0::numeric
                END AS spd_min3ly,
                CASE
                    WHEN wd.monthly = '-4'::integer::double precision THEN s.spdly
                    ELSE 0::numeric
                END AS spd_min4ly,
                CASE
                    WHEN wd.monthly = '-1'::integer::double precision THEN sb.totalstokhpp
                    ELSE 0::numeric
                END AS totalstokhpp_min1,
                CASE
                    WHEN wd.monthly = '-2'::integer::double precision THEN sb.totalstokhpp
                    ELSE 0::numeric
                END AS totalstokhpp_min2,
                CASE
                    WHEN wd.monthly = '-3'::integer::double precision THEN sb.totalstokhpp
                    ELSE 0::numeric
                END AS totalstokhpp_min3,
                CASE
                    WHEN wd.monthly = '-4'::integer::double precision THEN sb.totalstokhpp
                    ELSE 0::numeric
                END AS totalstokhpp_min4
           FROM ( SELECT slshpp.namacabang,
                    slshpp.kodetoko,
                    slshpp.monthly,
                    sum(slshpp.spd_hpp) AS spd_hpp,
                    sum(slshpp.spd_hpply) AS spd_hpply,
                    sum(slshpp.spd) AS spd,
                    sum(slshpp.spdly) AS spdly
                   FROM ( SELECT s_1.namacabang,
                            s_1.kodetoko,
                                CASE
                                    WHEN s_1.monthly = '-13'::integer::double precision THEN '-1'::integer::double precision
                                    WHEN s_1.monthly = '-14'::integer::double precision THEN '-2'::integer::double precision
                                    WHEN s_1.monthly = '-15'::integer::double precision THEN '-3'::integer::double precision
                                    WHEN s_1.monthly = '-16'::integer::double precision THEN '-4'::integer::double precision
                                    ELSE s_1.monthly
                                END AS monthly,
                                CASE
                                    WHEN s_1.monthly >= '-4'::integer::double precision AND s_1.monthly <= '-1'::integer::double precision THEN s_1.spd_hpp
                                    ELSE 0::numeric
                                END AS spd_hpp,
                                CASE
                                    WHEN s_1.monthly >= '-16'::integer::double precision AND s_1.monthly <= '-13'::integer::double precision THEN s_1.spd_hpp
                                    ELSE 0::numeric
                                END AS spd_hpply,
                                CASE
                                    WHEN s_1.monthly >= '-4'::integer::double precision AND s_1.monthly <= '-1'::integer::double precision THEN s_1.spd
                                    ELSE 0::numeric
                                END AS spd,
                                CASE
                                    WHEN s_1.monthly >= '-16'::integer::double precision AND s_1.monthly <= '-13'::integer::double precision THEN s_1.spd
                                    ELSE 0::numeric
                                END AS spdly
                           FROM ( 
                           			SELECT 
                           				sls.namacabang,
	                                    sls.kodetoko,
	                                    date_part('year'::text, age(date_trunc('month'::text, sls.tanggal::timestamp with time zone), date_trunc('month'::text, CURRENT_DATE::timestamp with time zone))) * 12::double precision + date_part('month'::text, age(date_trunc('month'::text, sls.tanggal::timestamp with time zone), date_trunc('month'::text, CURRENT_DATE::timestamp with time zone))) AS monthly,
	                                    sum(sls.qty * sls.hpp) AS spd_hpp,
	                                    sum(sls.subtotal) AS spd
                                   	FROM PUBLIC.mb_rms10_transaksi_toko_perjenis_member_v3 sls
                                     --JOIN smi_rms01_rpt.list_monitor_item_indicator_so it ON it.kodeproduk::text = sls.kodeproduk::text --KONFIRMASI KE IC ITEM APA SAJA YANG AKAN DIMASUKAN KE SIDAK
                                  	WHERE sls.tanggal >= date_trunc('month'::text, CURRENT_DATE + '-4 mons'::interval) AND sls.tanggal <= (date_trunc('month'::text, CURRENT_DATE + '-1 mons'::interval) + '1 mon -1 days'::interval)::date OR sls.tanggal >= (date_trunc('month'::text, CURRENT_DATE + '-4 mons'::interval) + '-1 years'::interval) AND sls.tanggal <= ((date_trunc('month'::text, CURRENT_DATE + '-1 mons'::interval) + '1 mon -1 days'::interval)::date + '-1 years'::interval)
                                  	GROUP BY sls.namacabang, sls.kodetoko, (date_part('year'::text, age(date_trunc('month'::text, sls.tanggal::timestamp with time zone), date_trunc('month'::text, CURRENT_DATE::timestamp with time zone))) * 12::double precision + date_part('month'::text, age(date_trunc('month'::text, sls.tanggal::timestamp with time zone), date_trunc('month'::text, CURRENT_DATE::timestamp with time zone))))
                                ) s_1) slshpp
                  GROUP BY slshpp.namacabang, slshpp.kodetoko, slshpp.monthly) s
             JOIN ( SELECT slsday.namacabang,
                    slsday.kodetoko,
                    slsday.monthly,
                    count(slsday.tanggal) AS workdays
                   FROM ( 
                   			SELECT 
	                   			sls.namacabang,
	                            sls.kodetoko,
	                            date_part('year'::text, age(date_trunc('month'::text, sls.tanggal::timestamp with time zone), date_trunc('month'::text, CURRENT_DATE::timestamp with time zone))) * 12::double precision + date_part('month'::text, age(date_trunc('month'::text, sls.tanggal::timestamp with time zone), date_trunc('month'::text, CURRENT_DATE::timestamp with time zone))) AS monthly,
	                            sls.tanggal
                          	FROM PUBLIC.mb_rms10_transaksi_toko_perjenis_member_v3 sls
--                          JOIN smi_rms01_rpt.list_monitor_item_indicator_so it ON it.kodeproduk::text = sls.kodeproduk::text --KONFIRMASI KE IC ITEM APA SAJA YANG AKAN DIMASUKAN KE SIDAK
                          	WHERE sls.tanggal >= date_trunc('month'::text, CURRENT_DATE + '-4 mons'::interval) AND sls.tanggal <= (date_trunc('month'::text, CURRENT_DATE + '-1 mons'::interval) + '1 mon -1 days'::interval)::date
                          	GROUP BY sls.namacabang, sls.kodetoko, (date_part('year'::text, age(date_trunc('month'::text, sls.tanggal::timestamp with time zone), date_trunc('month'::text, CURRENT_DATE::timestamp with time zone))) * 12::double precision + date_part('month'::text, age(date_trunc('month'::text, sls.tanggal::timestamp with time zone), date_trunc('month'::text, CURRENT_DATE::timestamp with time zone)))), sls.tanggal
                         ) slsday
                  GROUP BY slsday.namacabang, slsday.kodetoko, slsday.monthly) wd ON s.namacabang::text = wd.namacabang::text AND s.kodetoko = wd.kodetoko AND s.monthly = wd.monthly
             LEFT JOIN ( 
             			SELECT 
             				a.idcabang,
                    		a.namacabang,
                    		a.kodetoko,
	                        CASE
	                            WHEN a.tglsaldo = (date_trunc('month'::text, CURRENT_DATE + '-1 mons'::interval) + '1 mon -1 days'::interval)::date THEN '-1'::integer
	                            WHEN a.tglsaldo = (date_trunc('month'::text, CURRENT_DATE + '-2 mons'::interval) + '1 mon -1 days'::interval)::date THEN '-2'::integer
	                            WHEN a.tglsaldo = (date_trunc('month'::text, CURRENT_DATE + '-3 mons'::interval) + '1 mon -1 days'::interval)::date THEN '-3'::integer
	                            WHEN a.tglsaldo = (date_trunc('month'::text, CURRENT_DATE + '-4 mons'::interval) + '1 mon -1 days'::interval)::date THEN '-4'::integer
	                            ELSE NULL::integer
	                        END AS monthly,
	                    	sum(a.qty * a.hpp) AS totalstokhpp
                    	FROM mb_rms10_mbdc.mb_rms_saldostokproduktoko_v2_cab_jkt a
--                  	JOIN smi_rms01_rpt.list_monitor_item_indicator_so it ON it.kodeproduk::text = a.kodeproduk::text
                    	JOIN mb_rms01_mbho."MstProduk" b ON a.kodeproduk::text = b."kodeProduk"::text
                  		WHERE b."statusData" <> 9 
                  		AND b."idJenisProduk" <> 4 
                  		AND a.qty <> 0::numeric 
                  		AND (a.tglsaldo = (date_trunc('month'::text, CURRENT_DATE + '-1 mons'::interval) + '1 mon -1 days'::interval)::date OR a.tglsaldo = (date_trunc('month'::text, CURRENT_DATE + '-2 mons'::interval) + '1 mon -1 days'::interval)::date OR a.tglsaldo = (date_trunc('month'::text, CURRENT_DATE + '-3 mons'::interval) + '1 mon -1 days'::interval)::date OR a.tglsaldo = (date_trunc('month'::text, CURRENT_DATE + '-4 mons'::interval) + '1 mon -1 days'::interval)::date)
                  		GROUP BY a.idcabang, a.namacabang, a.kodetoko, (
										                        CASE
										                            WHEN a.tglsaldo = (date_trunc('month'::text, CURRENT_DATE + '-1 mons'::interval) + '1 mon -1 days'::interval)::date THEN '-1'::integer
										                            WHEN a.tglsaldo = (date_trunc('month'::text, CURRENT_DATE + '-2 mons'::interval) + '1 mon -1 days'::interval)::date THEN '-2'::integer
										                            WHEN a.tglsaldo = (date_trunc('month'::text, CURRENT_DATE + '-3 mons'::interval) + '1 mon -1 days'::interval)::date THEN '-3'::integer
										                            WHEN a.tglsaldo = (date_trunc('month'::text, CURRENT_DATE + '-4 mons'::interval) + '1 mon -1 days'::interval)::date THEN '-4'::integer
										                            ELSE NULL::integer
										                        END)
                        ) sb ON sb.namacabang::text = wd.namacabang::text AND sb.kodetoko::integer::numeric = wd.kodetoko AND sb.monthly::double precision = wd.monthly
          GROUP BY s.namacabang, s.kodetoko, wd.monthly, wd.workdays, s.spd_hpp, s.spd, s.spd_hpply, s.spdly, sb.totalstokhpp) sm
  GROUP BY sm.namacabang, sm.kodetoko
WITH DATA;

-- View indexes:
CREATE UNIQUE INDEX smi_mv_listitem_sales_mtd_jkt_kodetoko_unique_idx ON mb_rms10_mbdc.smi_mv_listitem_sales_mtd_jkt USING btree (kodetoko);

 ----------------------------------------------------------------------------------------

-- mb_rms10_mbdc.smi_mv_sales_mtd_jkt source

CREATE MATERIALIZED VIEW mb_rms10_mbdc.smi_mv_sales_mtd_jkt
TABLESPACE pg_default
AS SELECT slsmon.namacabang,
    slsmon.kodetoko,
    t.namatoko,
    slsmon.wd_min1,
    slsmon.wd_min2,
    slsmon.wd_min3,
    slsmon.wd_min4,
    slsmon.spdhpp_min1,
    slsmon.spdhpp_min2,
    slsmon.spdhpp_min3,
    slsmon.spdhpp_min4,
    slsmon.spd_min1,
    slsmon.spd_min2,
    slsmon.spd_min3,
    slsmon.spd_min4,
    slsmon.spdhpp_min1ly,
    slsmon.spdhpp_min2ly,
    slsmon.spdhpp_min3ly,
    slsmon.spdhpp_min4ly,
    slsmon.spd_min1ly,
    slsmon.spd_min2ly,
    slsmon.spd_min3ly,
    slsmon.spd_min4ly,
    slsmon.totalstokhpp_min1,
    slsmon.totalstokhpp_min2,
    slsmon.totalstokhpp_min3,
    slsmon.totalstokhpp_min4,
    sfm.wd_min1 AS fm_wd_min1,
    sfm.wd_min2 AS fm_wd_min2,
    sfm.wd_min3 AS fm_wd_min3,
    sfm.wd_min4 AS fm_wd_min4,
    sfm.qty_min1 AS fm_qty_min1,
    sfm.qty_min2 AS fm_qty_min2,
    sfm.qty_min3 AS fm_qty_min3,
    sfm.qty_min4 AS fm_qty_min4,
    sfm.spdhpp_min1 AS fm_spdhpp_min1,
    sfm.spdhpp_min2 AS fm_spdhpp_min2,
    sfm.spdhpp_min3 AS fm_spdhpp_min3,
    sfm.spdhpp_min4 AS fm_spdhpp_min4,
    sfm.spd_min1 AS fm_spd_min1,
    sfm.spd_min2 AS fm_spd_min2,
    sfm.spd_min3 AS fm_spd_min3,
    sfm.spd_min4 AS fm_spd_min4,
    sfm.qty_min1ly AS fm_qty_min1ly,
    sfm.qty_min2ly AS fm_qty_min2ly,
    sfm.qty_min3ly AS fm_qty_min3ly,
    sfm.qty_min4ly AS fm_qty_min4ly,
    sfm.spdhpp_min1ly AS fm_spdhpp_min1ly,
    sfm.spdhpp_min2ly AS fm_spdhpp_min2ly,
    sfm.spdhpp_min3ly AS fm_spdhpp_min3ly,
    sfm.spdhpp_min4ly AS fm_spdhpp_min4ly,
    sfm.spd_min1ly AS fm_spd_min1ly,
    sfm.spd_min2ly AS fm_spd_min2ly,
    sfm.spd_min3ly AS fm_spd_min3ly,
    sfm.spd_min4ly AS fm_spd_min4ly,
    sfm.totalstokhpp_min1 AS fm_totalstokhpp_min1,
    sfm.totalstokhpp_min2 AS fm_totalstokhpp_min2,
    sfm.totalstokhpp_min3 AS fm_totalstokhpp_min3,
    sfm.totalstokhpp_min4 AS fm_totalstokhpp_min4,
    slsli.wd_min1 AS li_wd_min1,
    slsli.wd_min2 AS li_wd_min2,
    slsli.wd_min3 AS li_wd_min3,
    slsli.wd_min4 AS li_wd_min4,
    slsli.spdhpp_min1 AS li_spdhpp_min1,
    slsli.spdhpp_min2 AS li_spdhpp_min2,
    slsli.spdhpp_min3 AS li_spdhpp_min3,
    slsli.spdhpp_min4 AS li_spdhpp_min4,
    slsli.spd_min1 AS li_spd_min1,
    slsli.spd_min2 AS li_spd_min2,
    slsli.spd_min3 AS li_spd_min3,
    slsli.spd_min4 AS li_spd_min4,
    slsli.spdhpp_min1ly AS li_spdhpp_min1ly,
    slsli.spdhpp_min2ly AS li_spdhpp_min2ly,
    slsli.spdhpp_min3ly AS li_spdhpp_min3ly,
    slsli.spdhpp_min4ly AS li_spdhpp_min4ly,
    slsli.spd_min1ly AS li_spd_min1ly,
    slsli.spd_min2ly AS li_spd_min2ly,
    slsli.spd_min3ly AS li_spd_min3ly,
    slsli.spd_min4ly AS li_spd_min4ly,
    slsli.totalstokhpp_min1 AS li_totalstokhpp_min1,
    slsli.totalstokhpp_min2 AS li_totalstokhpp_min2,
    slsli.totalstokhpp_min3 AS li_totalstokhpp_min3,
    slsli.totalstokhpp_min4 AS li_totalstokhpp_min4
   FROM ( SELECT sm.namacabang,
            sm.kodetoko,
            sum(sm.wd_min1) AS wd_min1,
            sum(sm.wd_min2) AS wd_min2,
            sum(sm.wd_min3) AS wd_min3,
            sum(sm.wd_min4) AS wd_min4,
            sum(sm.spdhpp_min1) AS spdhpp_min1,
            sum(sm.spdhpp_min2) AS spdhpp_min2,
            sum(sm.spdhpp_min3) AS spdhpp_min3,
            sum(sm.spdhpp_min4) AS spdhpp_min4,
            sum(sm.spd_min1) AS spd_min1,
            sum(sm.spd_min2) AS spd_min2,
            sum(sm.spd_min3) AS spd_min3,
            sum(sm.spd_min4) AS spd_min4,
            sum(sm.spdhpp_min1ly) AS spdhpp_min1ly,
            sum(sm.spdhpp_min2ly) AS spdhpp_min2ly,
            sum(sm.spdhpp_min3ly) AS spdhpp_min3ly,
            sum(sm.spdhpp_min4ly) AS spdhpp_min4ly,
            sum(sm.spd_min1ly) AS spd_min1ly,
            sum(sm.spd_min2ly) AS spd_min2ly,
            sum(sm.spd_min3ly) AS spd_min3ly,
            sum(sm.spd_min4ly) AS spd_min4ly,
            sum(sm.totalstokhpp_min1) AS totalstokhpp_min1,
            sum(sm.totalstokhpp_min2) AS totalstokhpp_min2,
            sum(sm.totalstokhpp_min3) AS totalstokhpp_min3,
            sum(sm.totalstokhpp_min4) AS totalstokhpp_min4
           FROM ( SELECT s.namacabang,
                    s.kodetoko,
                        CASE
                            WHEN wd.monthly = '-1'::integer::double precision THEN wd.workdays
                            ELSE 0::bigint
                        END AS wd_min1,
                        CASE
                            WHEN wd.monthly = '-2'::integer::double precision THEN wd.workdays
                            ELSE 0::bigint
                        END AS wd_min2,
                        CASE
                            WHEN wd.monthly = '-3'::integer::double precision THEN wd.workdays
                            ELSE 0::bigint
                        END AS wd_min3,
                        CASE
                            WHEN wd.monthly = '-4'::integer::double precision THEN wd.workdays
                            ELSE 0::bigint
                        END AS wd_min4,
                        CASE
                            WHEN wd.monthly = '-1'::integer::double precision THEN s.spd_hpp
                            ELSE 0::numeric
                        END AS spdhpp_min1,
                        CASE
                            WHEN wd.monthly = '-2'::integer::double precision THEN s.spd_hpp
                            ELSE 0::numeric
                        END AS spdhpp_min2,
                        CASE
                            WHEN wd.monthly = '-3'::integer::double precision THEN s.spd_hpp
                            ELSE 0::numeric
                        END AS spdhpp_min3,
                        CASE
                            WHEN wd.monthly = '-4'::integer::double precision THEN s.spd_hpp
                            ELSE 0::numeric
                        END AS spdhpp_min4,
                        CASE
                            WHEN wd.monthly = '-1'::integer::double precision THEN s.spd
                            ELSE 0::numeric
                        END AS spd_min1,
                        CASE
                            WHEN wd.monthly = '-2'::integer::double precision THEN s.spd
                            ELSE 0::numeric
                        END AS spd_min2,
                        CASE
                            WHEN wd.monthly = '-3'::integer::double precision THEN s.spd
                            ELSE 0::numeric
                        END AS spd_min3,
                        CASE
                            WHEN wd.monthly = '-4'::integer::double precision THEN s.spd
                            ELSE 0::numeric
                        END AS spd_min4,
                        CASE
                            WHEN wd.monthly = '-1'::integer::double precision THEN s.spd_hpply
                            ELSE 0::numeric
                        END AS spdhpp_min1ly,
                        CASE
                            WHEN wd.monthly = '-2'::integer::double precision THEN s.spd_hpply
                            ELSE 0::numeric
                        END AS spdhpp_min2ly,
                        CASE
                            WHEN wd.monthly = '-3'::integer::double precision THEN s.spd_hpply
                            ELSE 0::numeric
                        END AS spdhpp_min3ly,
                        CASE
                            WHEN wd.monthly = '-4'::integer::double precision THEN s.spd_hpply
                            ELSE 0::numeric
                        END AS spdhpp_min4ly,
                        CASE
                            WHEN wd.monthly = '-1'::integer::double precision THEN s.spdly
                            ELSE 0::numeric
                        END AS spd_min1ly,
                        CASE
                            WHEN wd.monthly = '-2'::integer::double precision THEN s.spdly
                            ELSE 0::numeric
                        END AS spd_min2ly,
                        CASE
                            WHEN wd.monthly = '-3'::integer::double precision THEN s.spdly
                            ELSE 0::numeric
                        END AS spd_min3ly,
                        CASE
                            WHEN wd.monthly = '-4'::integer::double precision THEN s.spdly
                            ELSE 0::numeric
                        END AS spd_min4ly,
                        CASE
                            WHEN wd.monthly = '-1'::integer::double precision THEN sb.totalstokhpp
                            ELSE 0::numeric
                        END AS totalstokhpp_min1,
                        CASE
                            WHEN wd.monthly = '-2'::integer::double precision THEN sb.totalstokhpp
                            ELSE 0::numeric
                        END AS totalstokhpp_min2,
                        CASE
                            WHEN wd.monthly = '-3'::integer::double precision THEN sb.totalstokhpp
                            ELSE 0::numeric
                        END AS totalstokhpp_min3,
                        CASE
                            WHEN wd.monthly = '-4'::integer::double precision THEN sb.totalstokhpp
                            ELSE 0::numeric
                        END AS totalstokhpp_min4
                   FROM ( SELECT slshpp.namacabang,
                            slshpp.kodetoko,
                            slshpp.monthly,
                            sum(slshpp.spd_hpp) AS spd_hpp,
                            sum(slshpp.spd_hpply) AS spd_hpply,
                            sum(slshpp.spd) AS spd,
                            sum(slshpp.spdly) AS spdly
                           FROM ( 
                           			SELECT 
                           				s_1.namacabang,
                                    	s_1.kodetoko,
										CASE
											WHEN s_1.monthly = '-13'::integer::double precision THEN '-1'::integer::double precision
											WHEN s_1.monthly = '-14'::integer::double precision THEN '-2'::integer::double precision
											WHEN s_1.monthly = '-15'::integer::double precision THEN '-3'::integer::double precision
											WHEN s_1.monthly = '-16'::integer::double precision THEN '-4'::integer::double precision
											ELSE s_1.monthly
										END AS monthly,
										CASE
											WHEN s_1.monthly >= '-4'::integer::double precision AND s_1.monthly <= '-1'::integer::double precision THEN s_1.spd_hpp
											ELSE 0::numeric
										END AS spd_hpp,
										CASE
											WHEN s_1.monthly >= '-16'::integer::double precision AND s_1.monthly <= '-13'::integer::double precision THEN s_1.spd_hpp
											ELSE 0::numeric
										END AS spd_hpply,
										CASE
											WHEN s_1.monthly >= '-4'::integer::double precision AND s_1.monthly <= '-1'::integer::double precision THEN s_1.spd
											ELSE 0::numeric
										END AS spd,
										CASE
											WHEN s_1.monthly >= '-16'::integer::double precision AND s_1.monthly <= '-13'::integer::double precision THEN s_1.spd
											ELSE 0::numeric
										END AS spdly
	                                   FROM ( 
	                                   			SELECT 
	                                   				sls.namacabang,
											    	sls.kodetoko,
											    	date_part('year'::text, age(date_trunc('month'::text, sls.tanggal::timestamp with time zone), date_trunc('month'::text, CURRENT_DATE::timestamp with time zone))) * 12::double precision + date_part('month'::text, age(date_trunc('month'::text, sls.tanggal::timestamp with time zone), date_trunc('month'::text, CURRENT_DATE::timestamp with time zone))) AS monthly,
											    	sum(sls.qty * sls.hpp) AS spd_hpp,
											    	sum(sls.subtotal) AS spd
											   	FROM PUBLIC.mb_rms10_transaksi_toko_perjenis_member_v3 sls
											  	WHERE sls.tanggal >= date_trunc('month'::text, CURRENT_DATE + '-4 mons'::interval) AND sls.tanggal <= (date_trunc('month'::text, CURRENT_DATE + '-1 mons'::interval) + '1 mon -1 days'::interval)::date OR sls.tanggal >= (date_trunc('month'::text, CURRENT_DATE + '-4 mons'::interval) + '-1 years'::interval) AND sls.tanggal <= ((date_trunc('month'::text, CURRENT_DATE + '-1 mons'::interval) + '1 mon -1 days'::interval)::date + '-1 years'::interval)
											  	GROUP BY sls.namacabang, sls.kodetoko, (date_part('year'::text, age(date_trunc('month'::text, sls.tanggal::timestamp with time zone), date_trunc('month'::text, CURRENT_DATE::timestamp with time zone))) * 12::double precision + date_part('month'::text, age(date_trunc('month'::text, sls.tanggal::timestamp with time zone), date_trunc('month'::text, CURRENT_DATE::timestamp with time zone))))
											  ) s_1
								) slshpp
                          		GROUP BY slshpp.namacabang, slshpp.kodetoko, slshpp.monthly) s
					JOIN ( 
                     		SELECT 
                     			slsday.namacabang,
                            	slsday.kodetoko,
                            	slsday.monthly,
                            	count(slsday.tanggal) AS workdays
                           	FROM ( 
                           			SELECT 
	                           			sls.namacabang,
	                                    sls.kodetoko,
	                                    date_part('year'::text, age(date_trunc('month'::text, sls.tanggal::timestamp with time zone), date_trunc('month'::text, CURRENT_DATE::timestamp with time zone))) * 12::double precision + date_part('month'::text, age(date_trunc('month'::text, sls.tanggal::timestamp with time zone), date_trunc('month'::text, CURRENT_DATE::timestamp with time zone))) AS monthly,
	                                    sls.tanggal
                                  	FROM PUBLIC.mb_rms10_transaksi_toko_perjenis_member_v3 sls
                                  	WHERE sls.tanggal >= date_trunc('month'::text, CURRENT_DATE + '-4 mons'::interval) AND sls.tanggal <= (date_trunc('month'::text, CURRENT_DATE + '-1 mons'::interval) + '1 mon -1 days'::interval)::date
                                  	GROUP BY sls.namacabang, sls.kodetoko, (date_part('year'::text, age(date_trunc('month'::text, sls.tanggal::timestamp with time zone), date_trunc('month'::text, CURRENT_DATE::timestamp with time zone))) * 12::double precision + date_part('month'::text, age(date_trunc('month'::text, sls.tanggal::timestamp with time zone), date_trunc('month'::text, CURRENT_DATE::timestamp with time zone)))), sls.tanggal
                                 ) slsday
                          GROUP BY slsday.namacabang, slsday.kodetoko, slsday.monthly) wd ON s.namacabang::text = wd.namacabang::text AND s.kodetoko = wd.kodetoko AND s.monthly = wd.monthly
                     LEFT JOIN (
		                     		SELECT 
			                     		a.idcabang,
			                            a.namacabang,
			                            a.kodetoko,
		                                CASE
		                                    WHEN a.tglsaldo = (date_trunc('month'::text, CURRENT_DATE + '-1 mons'::interval) + '1 mon -1 days'::interval)::date THEN '-1'::integer
		                                    WHEN a.tglsaldo = (date_trunc('month'::text, CURRENT_DATE + '-2 mons'::interval) + '1 mon -1 days'::interval)::date THEN '-2'::integer
		                                    WHEN a.tglsaldo = (date_trunc('month'::text, CURRENT_DATE + '-3 mons'::interval) + '1 mon -1 days'::interval)::date THEN '-3'::integer
		                                    WHEN a.tglsaldo = (date_trunc('month'::text, CURRENT_DATE + '-4 mons'::interval) + '1 mon -1 days'::interval)::date THEN '-4'::integer
		                                    ELSE NULL::integer
		                                END AS monthly,
			                            sum(a.qty * a.hpp) AS totalstokhpp
--		                           	FROM smi_stock_balance_toko_cab_jkt_v31_rms a
--		                            JOIN smi_mstprodukho_rms b ON a.kodeproduk::text = b.kodeproduk::text
		                               	FROM mb_rms10_mbdc.mb_rms_saldostokproduktoko_v2_cab_jkt a
				                    	JOIN mb_rms01_mbho."MstProduk" b ON a.kodeproduk::text = b."kodeProduk"::text
		                          	WHERE b."statusData" <> 9 
		                          	AND b."idJenisProduk" <> 4 
		                          	AND a.qty <> 0::numeric AND (a.tglsaldo = (date_trunc('month'::text, CURRENT_DATE + '-1 mons'::interval) + '1 mon -1 days'::interval)::date OR a.tglsaldo = (date_trunc('month'::text, CURRENT_DATE + '-2 mons'::interval) + '1 mon -1 days'::interval)::date OR a.tglsaldo = (date_trunc('month'::text, CURRENT_DATE + '-3 mons'::interval) + '1 mon -1 days'::interval)::date OR a.tglsaldo = (date_trunc('month'::text, CURRENT_DATE + '-4 mons'::interval) + '1 mon -1 days'::interval)::date)
		                          	GROUP BY a.idcabang, a.namacabang, a.kodetoko, (
													                                CASE
													                                    WHEN a.tglsaldo = (date_trunc('month'::text, CURRENT_DATE + '-1 mons'::interval) + '1 mon -1 days'::interval)::date THEN '-1'::integer
													                                    WHEN a.tglsaldo = (date_trunc('month'::text, CURRENT_DATE + '-2 mons'::interval) + '1 mon -1 days'::interval)::date THEN '-2'::integer
													                                    WHEN a.tglsaldo = (date_trunc('month'::text, CURRENT_DATE + '-3 mons'::interval) + '1 mon -1 days'::interval)::date THEN '-3'::integer
													                                    WHEN a.tglsaldo = (date_trunc('month'::text, CURRENT_DATE + '-4 mons'::interval) + '1 mon -1 days'::interval)::date THEN '-4'::integer
													                                    ELSE NULL::integer
													                                END)
                               ) sb ON sb.namacabang::text = wd.namacabang::text AND sb.kodetoko::integer::numeric = wd.kodetoko AND sb.monthly::double precision = wd.monthly
                  GROUP BY s.namacabang, s.kodetoko, wd.monthly, wd.workdays, s.spd_hpp, s.spd, s.spd_hpply, s.spdly, sb.totalstokhpp) sm
          GROUP BY sm.namacabang, sm.kodetoko) slsmon
     LEFT JOIN mb_rms10_mbdc.smi_mv_fastmoving_sales_mtd_jkt sfm ON sfm.kodetoko = slsmon.kodetoko
     LEFT JOIN mb_rms10_mbdc.smi_mv_listitem_sales_mtd_jkt slsli ON slsli.kodetoko = slsmon.kodetoko
     JOIN mb_rms10_mbdc.mv_msttoko t ON t.kodetoko::numeric = slsmon.kodetoko
WITH DATA;

-- View indexes:
CREATE UNIQUE INDEX smi_mv_sales_mtd_jkt_kodetoko_unique_idx ON mb_rms10_mbdc.smi_mv_sales_mtd_jkt USING btree (kodetoko);

 ----------------------------------------------------------------------------------------
 ----------------------------------------------------------------------------------------
-- 13.iso_sales_monitor_sby
 refresh materialized view smi_rms20_pbdc.mv_smi_fastmoving_produk20Persen_sby;
 refresh materialized view concurrently smi_rms20_pbdc.smi_mv_fastmoving_sales_mtd_sby;
 refresh materialized view concurrently smi_rms20_pbdc.smi_mv_listitem_sales_mtd_sby;
 refresh materialized view concurrently smi_rms20_pbdc.smi_mv_sales_mtd_sby;
 
 refresh materialized view mb_rms20_mbdc.mv_smi_fastmoving_produk20Persen_sby;--OK
 refresh materialized view concurrently mb_rms20_mbdc.smi_mv_fastmoving_sales_mtd_sby;--OK
 refresh materialized view concurrently mb_rms20_mbdc.smi_mv_listitem_sales_mtd_sby;--OK
 refresh materialized view concurrently mb_rms20_mbdc.smi_mv_sales_mtd_sby;--OK
 ----------------------------------------------------------------------------------------
 ----------------------------------------------------------------------------------------
 
-- mb_rms20_mbdc.mv_smi_fastmoving_produk20persen_sby source

CREATE MATERIALIZED VIEW mb_rms20_mbdc.mv_smi_fastmoving_produk20persen_sby
TABLESPACE pg_default
AS SELECT fm.namacabang,
    fm.kodetoko,
    fm.namatoko,
    fm.kodeproduk,
    fm.monthly,
    fm.qty,
    fm.val,
    fm.qty_rank
   FROM ( SELECT f.namacabang,
            f.kodetoko,
            f.namatoko,
            f.monthly,
            f.kodeproduk,
            f.qty,
            f.val,
            rank() OVER (PARTITION BY f.monthly, f.kodetoko ORDER BY f.val DESC) AS qty_rank
           FROM ( SELECT sls.namacabang,
                    sls.kodetoko,
                    upper(sls.namatoko::text) AS namatoko,
                    date_part('year'::text, age(date_trunc('month'::text, sls.tanggal::timestamp with time zone), date_trunc('month'::text, CURRENT_DATE::timestamp with time zone))) * 12::double precision + date_part('month'::text, age(date_trunc('month'::text, sls.tanggal::timestamp with time zone), date_trunc('month'::text, CURRENT_DATE::timestamp with time zone))) AS monthly,
                    sls.kodeproduk,
                    sum(sls.qty) AS qty,
                    sum(sls.subtotal) AS val
                   FROM PUBLIC.mb_rms20_transaksi_toko_perjenis_member_v3 sls
                  WHERE sls.tanggal >= date_trunc('month'::text, CURRENT_DATE + '-4 mons'::interval) AND sls.tanggal <= (date_trunc('month'::text, CURRENT_DATE + '-1 mons'::interval) + '1 mon -1 days'::interval)::date OR sls.tanggal >= (date_trunc('month'::text, CURRENT_DATE + '-4 mons'::interval) + '-1 years'::interval) AND sls.tanggal <= ((date_trunc('month'::text, CURRENT_DATE + '-1 mons'::interval) + '1 mon -1 days'::interval)::date + '-1 years'::interval)
                  GROUP BY sls.namacabang, sls.kodetoko, (upper(sls.namatoko::text)), (date_part('year'::text, age(date_trunc('month'::text, sls.tanggal::timestamp with time zone), date_trunc('month'::text, CURRENT_DATE::timestamp with time zone))) * 12::double precision + date_part('month'::text, age(date_trunc('month'::text, sls.tanggal::timestamp with time zone), date_trunc('month'::text, CURRENT_DATE::timestamp with time zone)))), sls.kodeproduk) f) fm
     JOIN ( SELECT c.namacabang,
            c.kodetoko,
            c.namatoko,
            c.monthly,
            round(20::double precision / 100::double precision * count(c.kodeproduk)::double precision) AS maxcnt
           FROM ( SELECT sls.namacabang,
                    sls.kodetoko,
                    upper(sls.namatoko::text) AS namatoko,
                    date_part('year'::text, age(date_trunc('month'::text, sls.tanggal::timestamp with time zone), date_trunc('month'::text, CURRENT_DATE::timestamp with time zone))) * 12::double precision + date_part('month'::text, age(date_trunc('month'::text, sls.tanggal::timestamp with time zone), date_trunc('month'::text, CURRENT_DATE::timestamp with time zone))) AS monthly,
                    sls.kodeproduk
                   FROM PUBLIC.mb_rms20_transaksi_toko_perjenis_member_v3 sls
                  WHERE sls.tanggal >= date_trunc('month'::text, CURRENT_DATE + '-4 mons'::interval) AND sls.tanggal <= (date_trunc('month'::text, CURRENT_DATE + '-1 mons'::interval) + '1 mon -1 days'::interval)::date OR sls.tanggal >= (date_trunc('month'::text, CURRENT_DATE + '-4 mons'::interval) + '-1 years'::interval) AND sls.tanggal <= ((date_trunc('month'::text, CURRENT_DATE + '-1 mons'::interval) + '1 mon -1 days'::interval)::date + '-1 years'::interval)
                  GROUP BY sls.namacabang, sls.kodetoko, (upper(sls.namatoko::text)), (date_part('year'::text, age(date_trunc('month'::text, sls.tanggal::timestamp with time zone), date_trunc('month'::text, CURRENT_DATE::timestamp with time zone))) * 12::double precision + date_part('month'::text, age(date_trunc('month'::text, sls.tanggal::timestamp with time zone), date_trunc('month'::text, CURRENT_DATE::timestamp with time zone)))), sls.kodeproduk) c
          GROUP BY c.namacabang, c.kodetoko, c.namatoko, c.monthly) cnt ON cnt.namacabang::text = fm.namacabang::text AND cnt.kodetoko = fm.kodetoko AND cnt.monthly = fm.monthly
  WHERE fm.qty_rank::double precision < cnt.maxcnt
WITH DATA;

 ----------------------------------------------------------------------------------------

-- mb_rms20_mbdc.smi_mv_fastmoving_sales_mtd_sby source

CREATE MATERIALIZED VIEW mb_rms20_mbdc.smi_mv_fastmoving_sales_mtd_sby
TABLESPACE pg_default
AS SELECT sm.namacabang,
    sm.kodetoko,
    t.namatoko,
    sum(sm.wd_min1) AS wd_min1,
    sum(sm.wd_min2) AS wd_min2,
    sum(sm.wd_min3) AS wd_min3,
    sum(sm.wd_min4) AS wd_min4,
    sum(sm.qty_min1) AS qty_min1,
    sum(sm.qty_min2) AS qty_min2,
    sum(sm.qty_min3) AS qty_min3,
    sum(sm.qty_min4) AS qty_min4,
    sum(sm.spdhpp_min1) AS spdhpp_min1,
    sum(sm.spdhpp_min2) AS spdhpp_min2,
    sum(sm.spdhpp_min3) AS spdhpp_min3,
    sum(sm.spdhpp_min4) AS spdhpp_min4,
    sum(sm.spd_min1) AS spd_min1,
    sum(sm.spd_min2) AS spd_min2,
    sum(sm.spd_min3) AS spd_min3,
    sum(sm.spd_min4) AS spd_min4,
    sum(sm.spdhpp_min1ly) AS spdhpp_min1ly,
    sum(sm.spdhpp_min2ly) AS spdhpp_min2ly,
    sum(sm.spdhpp_min3ly) AS spdhpp_min3ly,
    sum(sm.spdhpp_min4ly) AS spdhpp_min4ly,
    sum(sm.spd_min1ly) AS spd_min1ly,
    sum(sm.spd_min2ly) AS spd_min2ly,
    sum(sm.spd_min3ly) AS spd_min3ly,
    sum(sm.spd_min4ly) AS spd_min4ly,
    sum(sm.qty_min1ly) AS qty_min1ly,
    sum(sm.qty_min2ly) AS qty_min2ly,
    sum(sm.qty_min3ly) AS qty_min3ly,
    sum(sm.qty_min4ly) AS qty_min4ly,
    sum(sm.totalstokhpp_min1) AS totalstokhpp_min1,
    sum(sm.totalstokhpp_min2) AS totalstokhpp_min2,
    sum(sm.totalstokhpp_min3) AS totalstokhpp_min3,
    sum(sm.totalstokhpp_min4) AS totalstokhpp_min4
   FROM ( SELECT s.namacabang,
            s.kodetoko,
                CASE
                    WHEN wd.monthly = '-1'::integer::double precision THEN wd.workdays
                    ELSE 0::bigint
                END AS wd_min1,
                CASE
                    WHEN wd.monthly = '-2'::integer::double precision THEN wd.workdays
                    ELSE 0::bigint
                END AS wd_min2,
                CASE
                    WHEN wd.monthly = '-3'::integer::double precision THEN wd.workdays
                    ELSE 0::bigint
                END AS wd_min3,
                CASE
                    WHEN wd.monthly = '-4'::integer::double precision THEN wd.workdays
                    ELSE 0::bigint
                END AS wd_min4,
                CASE
                    WHEN wd.monthly = '-1'::integer::double precision THEN s.qty
                    ELSE 0::numeric
                END AS qty_min1,
                CASE
                    WHEN wd.monthly = '-2'::integer::double precision THEN s.qty
                    ELSE 0::numeric
                END AS qty_min2,
                CASE
                    WHEN wd.monthly = '-3'::integer::double precision THEN s.qty
                    ELSE 0::numeric
                END AS qty_min3,
                CASE
                    WHEN wd.monthly = '-4'::integer::double precision THEN s.qty
                    ELSE 0::numeric
                END AS qty_min4,
                CASE
                    WHEN wd.monthly = '-1'::integer::double precision THEN s.spd_hpp
                    ELSE 0::numeric
                END AS spdhpp_min1,
                CASE
                    WHEN wd.monthly = '-2'::integer::double precision THEN s.spd_hpp
                    ELSE 0::numeric
                END AS spdhpp_min2,
                CASE
                    WHEN wd.monthly = '-3'::integer::double precision THEN s.spd_hpp
                    ELSE 0::numeric
                END AS spdhpp_min3,
                CASE
                    WHEN wd.monthly = '-4'::integer::double precision THEN s.spd_hpp
                    ELSE 0::numeric
                END AS spdhpp_min4,
                CASE
                    WHEN wd.monthly = '-1'::integer::double precision THEN s.spd
                    ELSE 0::numeric
                END AS spd_min1,
                CASE
                    WHEN wd.monthly = '-2'::integer::double precision THEN s.spd
                    ELSE 0::numeric
                END AS spd_min2,
                CASE
                    WHEN wd.monthly = '-3'::integer::double precision THEN s.spd
                    ELSE 0::numeric
                END AS spd_min3,
                CASE
                    WHEN wd.monthly = '-4'::integer::double precision THEN s.spd
                    ELSE 0::numeric
                END AS spd_min4,
                CASE
                    WHEN wd.monthly = '-1'::integer::double precision THEN s.spd_hpply
                    ELSE 0::numeric
                END AS spdhpp_min1ly,
                CASE
                    WHEN wd.monthly = '-2'::integer::double precision THEN s.spd_hpply
                    ELSE 0::numeric
                END AS spdhpp_min2ly,
                CASE
                    WHEN wd.monthly = '-3'::integer::double precision THEN s.spd_hpply
                    ELSE 0::numeric
                END AS spdhpp_min3ly,
                CASE
                    WHEN wd.monthly = '-4'::integer::double precision THEN s.spd_hpply
                    ELSE 0::numeric
                END AS spdhpp_min4ly,
                CASE
                    WHEN wd.monthly = '-1'::integer::double precision THEN s.spdly
                    ELSE 0::numeric
                END AS spd_min1ly,
                CASE
                    WHEN wd.monthly = '-2'::integer::double precision THEN s.spdly
                    ELSE 0::numeric
                END AS spd_min2ly,
                CASE
                    WHEN wd.monthly = '-3'::integer::double precision THEN s.spdly
                    ELSE 0::numeric
                END AS spd_min3ly,
                CASE
                    WHEN wd.monthly = '-4'::integer::double precision THEN s.spdly
                    ELSE 0::numeric
                END AS spd_min4ly,
                CASE
                    WHEN wd.monthly = '-1'::integer::double precision THEN s.qtyly
                    ELSE 0::numeric
                END AS qty_min1ly,
                CASE
                    WHEN wd.monthly = '-2'::integer::double precision THEN s.qtyly
                    ELSE 0::numeric
                END AS qty_min2ly,
                CASE
                    WHEN wd.monthly = '-3'::integer::double precision THEN s.qtyly
                    ELSE 0::numeric
                END AS qty_min3ly,
                CASE
                    WHEN wd.monthly = '-4'::integer::double precision THEN s.qtyly
                    ELSE 0::numeric
                END AS qty_min4ly,
                CASE
                    WHEN wd.monthly = '-1'::integer::double precision THEN sb.totalstokhpp
                    ELSE 0::numeric
                END AS totalstokhpp_min1,
                CASE
                    WHEN wd.monthly = '-2'::integer::double precision THEN sb.totalstokhpp
                    ELSE 0::numeric
                END AS totalstokhpp_min2,
                CASE
                    WHEN wd.monthly = '-3'::integer::double precision THEN sb.totalstokhpp
                    ELSE 0::numeric
                END AS totalstokhpp_min3,
                CASE
                    WHEN wd.monthly = '-4'::integer::double precision THEN sb.totalstokhpp
                    ELSE 0::numeric
                END AS totalstokhpp_min4
           FROM ( SELECT slshpp.namacabang,
                    slshpp.kodetoko,
                    slshpp.monthly,
                    sum(slshpp.qty) AS qty,
                    sum(slshpp.qtyly) AS qtyly,
                    sum(slshpp.spd_hpp) AS spd_hpp,
                    sum(slshpp.spd_hpply) AS spd_hpply,
                    sum(slshpp.spd) AS spd,
                    sum(slshpp.spdly) AS spdly
                   FROM ( SELECT s_1.namacabang,
                            s_1.kodetoko,
                                CASE
                                    WHEN s_1.monthly = '-13'::integer::double precision THEN '-1'::integer::double precision
                                    WHEN s_1.monthly = '-14'::integer::double precision THEN '-2'::integer::double precision
                                    WHEN s_1.monthly = '-15'::integer::double precision THEN '-3'::integer::double precision
                                    WHEN s_1.monthly = '-16'::integer::double precision THEN '-4'::integer::double precision
                                    ELSE s_1.monthly
                                END AS monthly,
                                CASE
                                    WHEN s_1.monthly >= '-4'::integer::double precision AND s_1.monthly <= '-1'::integer::double precision THEN s_1.qty
                                    ELSE 0::numeric
                                END AS qty,
                                CASE
                                    WHEN s_1.monthly >= '-16'::integer::double precision AND s_1.monthly <= '-13'::integer::double precision THEN s_1.qty
                                    ELSE 0::numeric
                                END AS qtyly,
                                CASE
                                    WHEN s_1.monthly >= '-4'::integer::double precision AND s_1.monthly <= '-1'::integer::double precision THEN s_1.spd_hpp
                                    ELSE 0::numeric
                                END AS spd_hpp,
                                CASE
                                    WHEN s_1.monthly >= '-16'::integer::double precision AND s_1.monthly <= '-13'::integer::double precision THEN s_1.spd_hpp
                                    ELSE 0::numeric
                                END AS spd_hpply,
                                CASE
                                    WHEN s_1.monthly >= '-4'::integer::double precision AND s_1.monthly <= '-1'::integer::double precision THEN s_1.spd
                                    ELSE 0::numeric
                                END AS spd,
                                CASE
                                    WHEN s_1.monthly >= '-16'::integer::double precision AND s_1.monthly <= '-13'::integer::double precision THEN s_1.spd
                                    ELSE 0::numeric
                                END AS spdly
                           FROM ( SELECT a.namacabang,
                                    a.kodetoko,
                                    a.monthly,
                                    sum(a.qty) AS qty,
                                    sum(a.spd_hpp) AS spd_hpp,
                                    sum(a.spd) AS spd
                                   FROM ( 
                                   			SELECT 
                                   				sls.namacabang,
											    sls.kodetoko,
											    sls.kodeproduk,
											    date_part('year'::text, age(date_trunc('month'::text, sls.tanggal::timestamp with time zone), date_trunc('month'::text, CURRENT_DATE::timestamp with time zone))) * 12::double precision + date_part('month'::text, age(date_trunc('month'::text, sls.tanggal::timestamp with time zone), date_trunc('month'::text, CURRENT_DATE::timestamp with time zone))) AS monthly,
											    sum(sls.qty) AS qty,
											    sum(sls.qty * sls.hpp) AS spd_hpp,
											    sum(sls.subtotal) AS spd
										   	FROM PUBLIC.mb_rms20_transaksi_toko_perjenis_member_v3 sls
										  	WHERE sls.tanggal >= date_trunc('month'::text, CURRENT_DATE + '-4 mons'::interval) AND sls.tanggal <= (date_trunc('month'::text, CURRENT_DATE + '-1 mons'::interval) + '1 mon -1 days'::interval)::date OR sls.tanggal >= (date_trunc('month'::text, CURRENT_DATE + '-4 mons'::interval) + '-1 years'::interval) AND sls.tanggal <= ((date_trunc('month'::text, CURRENT_DATE + '-1 mons'::interval) + '1 mon -1 days'::interval)::date + '-1 years'::interval)
										  	GROUP BY sls.namacabang, sls.kodetoko, sls.kodeproduk, (date_part('year'::text, age(date_trunc('month'::text, sls.tanggal::timestamp with time zone), date_trunc('month'::text, CURRENT_DATE::timestamp with time zone))) * 12::double precision + date_part('month'::text, age(date_trunc('month'::text, sls.tanggal::timestamp with time zone), date_trunc('month'::text, CURRENT_DATE::timestamp with time zone))))
										  ) a
                                     JOIN mb_rms20_mbdc.mv_smi_fastmoving_produk20persen_sby p ON p.kodetoko = a.kodetoko AND p.namacabang::text = a.namacabang::text AND p.kodeproduk::text = a.kodeproduk::text AND p.monthly = a.monthly
                                  GROUP BY a.namacabang, a.kodetoko, a.monthly) s_1) slshpp
                  GROUP BY slshpp.namacabang, slshpp.kodetoko, slshpp.monthly) s
             JOIN ( SELECT slsday.namacabang,
                    slsday.kodetoko,
                    slsday.monthly,
                    count(slsday.tanggal) AS workdays
                   FROM ( SELECT a.namacabang,
                            a.kodetoko,
                            a.monthly,
                            a.tanggal
                           FROM (
                           			SELECT 
                           				sls.namacabang,
	                                    sls.kodetoko,
	                                    sls.kodeproduk,
	                                    date_part('year'::text, age(date_trunc('month'::text, sls.tanggal::timestamp with time zone), date_trunc('month'::text, CURRENT_DATE::timestamp with time zone))) * 12::double precision + date_part('month'::text, age(date_trunc('month'::text, sls.tanggal::timestamp with time zone), date_trunc('month'::text, CURRENT_DATE::timestamp with time zone))) AS monthly,
	                                    sls.tanggal
                                   	FROM PUBLIC.mb_rms20_transaksi_toko_perjenis_member_v3 sls
                                  	WHERE sls.tanggal >= date_trunc('month'::text, CURRENT_DATE + '-4 mons'::interval) AND sls.tanggal <= (date_trunc('month'::text, CURRENT_DATE + '-1 mons'::interval) + '1 mon -1 days'::interval)::date
                                  	GROUP BY sls.namacabang, sls.kodetoko, sls.kodeproduk, (date_part('year'::text, age(date_trunc('month'::text, sls.tanggal::timestamp with time zone), date_trunc('month'::text, CURRENT_DATE::timestamp with time zone))) * 12::double precision + date_part('month'::text, age(date_trunc('month'::text, sls.tanggal::timestamp with time zone), date_trunc('month'::text, CURRENT_DATE::timestamp with time zone)))), sls.tanggal
                                  ) a
                             JOIN mb_rms20_mbdc.mv_smi_fastmoving_produk20persen_sby p ON p.kodetoko = a.kodetoko AND p.namacabang::text = a.namacabang::text AND p.kodeproduk::text = a.kodeproduk::text AND p.monthly = a.monthly
                          GROUP BY a.namacabang, a.kodetoko, a.monthly, a.tanggal) slsday
                  GROUP BY slsday.namacabang, slsday.kodetoko, slsday.monthly) wd ON s.namacabang::text = wd.namacabang::text AND s.kodetoko = wd.kodetoko AND s.monthly = wd.monthly
             JOIN ( SELECT psb.idcabang,
                    psb.namacabang,
                    psb.kodetoko,
                    psb.monthly,
                    sum(psb.totalstokhpp) AS totalstokhpp
                   FROM (
                   			SELECT a.idcabang,
                            a.namacabang,
                            a.kodetoko,
                            a.kodeproduk,
                                CASE
                                    WHEN a.tglsaldo = (date_trunc('month'::text, CURRENT_DATE + '-1 mons'::interval) + '1 mon -1 days'::interval)::date THEN '-1'::integer
                                    WHEN a.tglsaldo = (date_trunc('month'::text, CURRENT_DATE + '-2 mons'::interval) + '1 mon -1 days'::interval)::date THEN '-2'::integer
                                    WHEN a.tglsaldo = (date_trunc('month'::text, CURRENT_DATE + '-3 mons'::interval) + '1 mon -1 days'::interval)::date THEN '-3'::integer
                                    WHEN a.tglsaldo = (date_trunc('month'::text, CURRENT_DATE + '-4 mons'::interval) + '1 mon -1 days'::interval)::date THEN '-4'::integer
                                    ELSE NULL::integer
                                END AS monthly,
                            sum(a.qty * a.hpp) AS totalstokhpp
                           	FROM mb_rms20_mbdc.mb_rms_saldostokproduktoko_v2_cab_sby a
                           	JOIN mb_rms01_mbho."MstProduk" b ON a.kodeproduk::text = b."kodeProduk"::text
                          	WHERE b."statusData" <> 9 
                          	AND b."idJenisProduk" <> 4 
                          	AND a.qty <> 0::numeric 
                          	AND (a.tglsaldo = (date_trunc('month'::text, CURRENT_DATE + '-1 mons'::interval) + '1 mon -1 days'::interval)::date OR a.tglsaldo = (date_trunc('month'::text, CURRENT_DATE + '-2 mons'::interval) + '1 mon -1 days'::interval)::date OR a.tglsaldo = (date_trunc('month'::text, CURRENT_DATE + '-3 mons'::interval) + '1 mon -1 days'::interval)::date OR a.tglsaldo = (date_trunc('month'::text, CURRENT_DATE + '-4 mons'::interval) + '1 mon -1 days'::interval)::date)
                          	GROUP BY a.idcabang, a.namacabang, a.kodetoko, a.kodeproduk, (
                                CASE
                                    WHEN a.tglsaldo = (date_trunc('month'::text, CURRENT_DATE + '-1 mons'::interval) + '1 mon -1 days'::interval)::date THEN '-1'::integer
                                    WHEN a.tglsaldo = (date_trunc('month'::text, CURRENT_DATE + '-2 mons'::interval) + '1 mon -1 days'::interval)::date THEN '-2'::integer
                                    WHEN a.tglsaldo = (date_trunc('month'::text, CURRENT_DATE + '-3 mons'::interval) + '1 mon -1 days'::interval)::date THEN '-3'::integer
                                    WHEN a.tglsaldo = (date_trunc('month'::text, CURRENT_DATE + '-4 mons'::interval) + '1 mon -1 days'::interval)::date THEN '-4'::integer
                                    ELSE NULL::integer
                                END)
                         ) psb
                     JOIN mb_rms20_mbdc.mv_smi_fastmoving_produk20persen_sby p ON p.kodetoko::character varying::text = psb.kodetoko::text AND p.namacabang::text = psb.namacabang::text AND p.kodeproduk::text = psb.kodeproduk::text AND p.monthly = psb.monthly::double precision
                  GROUP BY psb.idcabang, psb.namacabang, psb.kodetoko, psb.monthly) sb ON sb.namacabang::text = wd.namacabang::text AND sb.kodetoko::integer::numeric = wd.kodetoko AND sb.monthly::double precision = wd.monthly
          GROUP BY s.namacabang, s.kodetoko, wd.monthly, wd.workdays, s.qty, s.spd_hpp, s.spd, s.spd_hpply, s.spdly, s.qtyly, sb.totalstokhpp) sm
     JOIN mb_rms10_mbdc.mv_msttoko t ON t.kodetoko::numeric = sm.kodetoko
  GROUP BY sm.namacabang, sm.kodetoko, t.namatoko
WITH DATA;

-- View indexes:
CREATE UNIQUE INDEX smi_mv_fastmoving_sales_mtd_jkt_kodetoko_unique_idx ON mb_rms20_mbdc.smi_mv_fastmoving_sales_mtd_sby USING btree (kodetoko);

 ----------------------------------------------------------------------------------------

-- mb_rms20_mbdc.smi_mv_listitem_sales_mtd_sby source

CREATE MATERIALIZED VIEW mb_rms20_mbdc.smi_mv_listitem_sales_mtd_sby
TABLESPACE pg_default
AS SELECT sm.namacabang,
    sm.kodetoko,
    sum(sm.wd_min1) AS wd_min1,
    sum(sm.wd_min2) AS wd_min2,
    sum(sm.wd_min3) AS wd_min3,
    sum(sm.wd_min4) AS wd_min4,
    sum(sm.spdhpp_min1) AS spdhpp_min1,
    sum(sm.spdhpp_min2) AS spdhpp_min2,
    sum(sm.spdhpp_min3) AS spdhpp_min3,
    sum(sm.spdhpp_min4) AS spdhpp_min4,
    sum(sm.spd_min1) AS spd_min1,
    sum(sm.spd_min2) AS spd_min2,
    sum(sm.spd_min3) AS spd_min3,
    sum(sm.spd_min4) AS spd_min4,
    sum(sm.spdhpp_min1ly) AS spdhpp_min1ly,
    sum(sm.spdhpp_min2ly) AS spdhpp_min2ly,
    sum(sm.spdhpp_min3ly) AS spdhpp_min3ly,
    sum(sm.spdhpp_min4ly) AS spdhpp_min4ly,
    sum(sm.spd_min1ly) AS spd_min1ly,
    sum(sm.spd_min2ly) AS spd_min2ly,
    sum(sm.spd_min3ly) AS spd_min3ly,
    sum(sm.spd_min4ly) AS spd_min4ly,
    sum(sm.totalstokhpp_min1) AS totalstokhpp_min1,
    sum(sm.totalstokhpp_min2) AS totalstokhpp_min2,
    sum(sm.totalstokhpp_min3) AS totalstokhpp_min3,
    sum(sm.totalstokhpp_min4) AS totalstokhpp_min4
   FROM ( SELECT s.namacabang,
            s.kodetoko,
                CASE
                    WHEN wd.monthly = '-1'::integer::double precision THEN wd.workdays
                    ELSE 0::bigint
                END AS wd_min1,
                CASE
                    WHEN wd.monthly = '-2'::integer::double precision THEN wd.workdays
                    ELSE 0::bigint
                END AS wd_min2,
                CASE
                    WHEN wd.monthly = '-3'::integer::double precision THEN wd.workdays
                    ELSE 0::bigint
                END AS wd_min3,
                CASE
                    WHEN wd.monthly = '-4'::integer::double precision THEN wd.workdays
                    ELSE 0::bigint
                END AS wd_min4,
                CASE
                    WHEN wd.monthly = '-1'::integer::double precision THEN s.spd_hpp
                    ELSE 0::numeric
                END AS spdhpp_min1,
                CASE
                    WHEN wd.monthly = '-2'::integer::double precision THEN s.spd_hpp
                    ELSE 0::numeric
                END AS spdhpp_min2,
                CASE
                    WHEN wd.monthly = '-3'::integer::double precision THEN s.spd_hpp
                    ELSE 0::numeric
                END AS spdhpp_min3,
                CASE
                    WHEN wd.monthly = '-4'::integer::double precision THEN s.spd_hpp
                    ELSE 0::numeric
                END AS spdhpp_min4,
                CASE
                    WHEN wd.monthly = '-1'::integer::double precision THEN s.spd
                    ELSE 0::numeric
                END AS spd_min1,
                CASE
                    WHEN wd.monthly = '-2'::integer::double precision THEN s.spd
                    ELSE 0::numeric
                END AS spd_min2,
                CASE
                    WHEN wd.monthly = '-3'::integer::double precision THEN s.spd
                    ELSE 0::numeric
                END AS spd_min3,
                CASE
                    WHEN wd.monthly = '-4'::integer::double precision THEN s.spd
                    ELSE 0::numeric
                END AS spd_min4,
                CASE
                    WHEN wd.monthly = '-1'::integer::double precision THEN s.spd_hpply
                    ELSE 0::numeric
                END AS spdhpp_min1ly,
                CASE
                    WHEN wd.monthly = '-2'::integer::double precision THEN s.spd_hpply
                    ELSE 0::numeric
                END AS spdhpp_min2ly,
                CASE
                    WHEN wd.monthly = '-3'::integer::double precision THEN s.spd_hpply
                    ELSE 0::numeric
                END AS spdhpp_min3ly,
                CASE
                    WHEN wd.monthly = '-4'::integer::double precision THEN s.spd_hpply
                    ELSE 0::numeric
                END AS spdhpp_min4ly,
                CASE
                    WHEN wd.monthly = '-1'::integer::double precision THEN s.spdly
                    ELSE 0::numeric
                END AS spd_min1ly,
                CASE
                    WHEN wd.monthly = '-2'::integer::double precision THEN s.spdly
                    ELSE 0::numeric
                END AS spd_min2ly,
                CASE
                    WHEN wd.monthly = '-3'::integer::double precision THEN s.spdly
                    ELSE 0::numeric
                END AS spd_min3ly,
                CASE
                    WHEN wd.monthly = '-4'::integer::double precision THEN s.spdly
                    ELSE 0::numeric
                END AS spd_min4ly,
                CASE
                    WHEN wd.monthly = '-1'::integer::double precision THEN sb.totalstokhpp
                    ELSE 0::numeric
                END AS totalstokhpp_min1,
                CASE
                    WHEN wd.monthly = '-2'::integer::double precision THEN sb.totalstokhpp
                    ELSE 0::numeric
                END AS totalstokhpp_min2,
                CASE
                    WHEN wd.monthly = '-3'::integer::double precision THEN sb.totalstokhpp
                    ELSE 0::numeric
                END AS totalstokhpp_min3,
                CASE
                    WHEN wd.monthly = '-4'::integer::double precision THEN sb.totalstokhpp
                    ELSE 0::numeric
                END AS totalstokhpp_min4
           FROM ( SELECT slshpp.namacabang,
                    slshpp.kodetoko,
                    slshpp.monthly,
                    sum(slshpp.spd_hpp) AS spd_hpp,
                    sum(slshpp.spd_hpply) AS spd_hpply,
                    sum(slshpp.spd) AS spd,
                    sum(slshpp.spdly) AS spdly
                   FROM ( SELECT s_1.namacabang,
                            s_1.kodetoko,
                                CASE
                                    WHEN s_1.monthly = '-13'::integer::double precision THEN '-1'::integer::double precision
                                    WHEN s_1.monthly = '-14'::integer::double precision THEN '-2'::integer::double precision
                                    WHEN s_1.monthly = '-15'::integer::double precision THEN '-3'::integer::double precision
                                    WHEN s_1.monthly = '-16'::integer::double precision THEN '-4'::integer::double precision
                                    ELSE s_1.monthly
                                END AS monthly,
                                CASE
                                    WHEN s_1.monthly >= '-4'::integer::double precision AND s_1.monthly <= '-1'::integer::double precision THEN s_1.spd_hpp
                                    ELSE 0::numeric
                                END AS spd_hpp,
                                CASE
                                    WHEN s_1.monthly >= '-16'::integer::double precision AND s_1.monthly <= '-13'::integer::double precision THEN s_1.spd_hpp
                                    ELSE 0::numeric
                                END AS spd_hpply,
                                CASE
                                    WHEN s_1.monthly >= '-4'::integer::double precision AND s_1.monthly <= '-1'::integer::double precision THEN s_1.spd
                                    ELSE 0::numeric
                                END AS spd,
                                CASE
                                    WHEN s_1.monthly >= '-16'::integer::double precision AND s_1.monthly <= '-13'::integer::double precision THEN s_1.spd
                                    ELSE 0::numeric
                                END AS spdly
                           FROM ( 
                           			SELECT 
                           				sls.namacabang,
	                                    sls.kodetoko,
	                                    date_part('year'::text, age(date_trunc('month'::text, sls.tanggal::timestamp with time zone), date_trunc('month'::text, CURRENT_DATE::timestamp with time zone))) * 12::double precision + date_part('month'::text, age(date_trunc('month'::text, sls.tanggal::timestamp with time zone), date_trunc('month'::text, CURRENT_DATE::timestamp with time zone))) AS monthly,
	                                    sum(sls.qty * sls.hpp) AS spd_hpp,
	                                    sum(sls.subtotal) AS spd
                                   	FROM PUBLIC.mb_rms20_transaksi_toko_perjenis_member_v3 sls
                                     --JOIN smi_rms01_rpt.list_monitor_item_indicator_so it ON it.kodeproduk::text = sls.kodeproduk::text --KONFIRMASI KE IC ITEM APA SAJA YANG AKAN DIMASUKAN KE SIDAK
                                  	WHERE sls.tanggal >= date_trunc('month'::text, CURRENT_DATE + '-4 mons'::interval) AND sls.tanggal <= (date_trunc('month'::text, CURRENT_DATE + '-1 mons'::interval) + '1 mon -1 days'::interval)::date OR sls.tanggal >= (date_trunc('month'::text, CURRENT_DATE + '-4 mons'::interval) + '-1 years'::interval) AND sls.tanggal <= ((date_trunc('month'::text, CURRENT_DATE + '-1 mons'::interval) + '1 mon -1 days'::interval)::date + '-1 years'::interval)
                                  	GROUP BY sls.namacabang, sls.kodetoko, (date_part('year'::text, age(date_trunc('month'::text, sls.tanggal::timestamp with time zone), date_trunc('month'::text, CURRENT_DATE::timestamp with time zone))) * 12::double precision + date_part('month'::text, age(date_trunc('month'::text, sls.tanggal::timestamp with time zone), date_trunc('month'::text, CURRENT_DATE::timestamp with time zone))))
                                ) s_1) slshpp
                  GROUP BY slshpp.namacabang, slshpp.kodetoko, slshpp.monthly) s
             JOIN ( SELECT slsday.namacabang,
                    slsday.kodetoko,
                    slsday.monthly,
                    count(slsday.tanggal) AS workdays
                   FROM ( 
                   			SELECT 
	                   			sls.namacabang,
	                            sls.kodetoko,
	                            date_part('year'::text, age(date_trunc('month'::text, sls.tanggal::timestamp with time zone), date_trunc('month'::text, CURRENT_DATE::timestamp with time zone))) * 12::double precision + date_part('month'::text, age(date_trunc('month'::text, sls.tanggal::timestamp with time zone), date_trunc('month'::text, CURRENT_DATE::timestamp with time zone))) AS monthly,
	                            sls.tanggal
                          	FROM PUBLIC.mb_rms20_transaksi_toko_perjenis_member_v3 sls
--                          JOIN smi_rms01_rpt.list_monitor_item_indicator_so it ON it.kodeproduk::text = sls.kodeproduk::text --KONFIRMASI KE IC ITEM APA SAJA YANG AKAN DIMASUKAN KE SIDAK
                          	WHERE sls.tanggal >= date_trunc('month'::text, CURRENT_DATE + '-4 mons'::interval) AND sls.tanggal <= (date_trunc('month'::text, CURRENT_DATE + '-1 mons'::interval) + '1 mon -1 days'::interval)::date
                          	GROUP BY sls.namacabang, sls.kodetoko, (date_part('year'::text, age(date_trunc('month'::text, sls.tanggal::timestamp with time zone), date_trunc('month'::text, CURRENT_DATE::timestamp with time zone))) * 12::double precision + date_part('month'::text, age(date_trunc('month'::text, sls.tanggal::timestamp with time zone), date_trunc('month'::text, CURRENT_DATE::timestamp with time zone)))), sls.tanggal
                         ) slsday
                  GROUP BY slsday.namacabang, slsday.kodetoko, slsday.monthly) wd ON s.namacabang::text = wd.namacabang::text AND s.kodetoko = wd.kodetoko AND s.monthly = wd.monthly
             LEFT JOIN ( 
             			SELECT 
             				a.idcabang,
                    		a.namacabang,
                    		a.kodetoko,
	                        CASE
	                            WHEN a.tglsaldo = (date_trunc('month'::text, CURRENT_DATE + '-1 mons'::interval) + '1 mon -1 days'::interval)::date THEN '-1'::integer
	                            WHEN a.tglsaldo = (date_trunc('month'::text, CURRENT_DATE + '-2 mons'::interval) + '1 mon -1 days'::interval)::date THEN '-2'::integer
	                            WHEN a.tglsaldo = (date_trunc('month'::text, CURRENT_DATE + '-3 mons'::interval) + '1 mon -1 days'::interval)::date THEN '-3'::integer
	                            WHEN a.tglsaldo = (date_trunc('month'::text, CURRENT_DATE + '-4 mons'::interval) + '1 mon -1 days'::interval)::date THEN '-4'::integer
	                            ELSE NULL::integer
	                        END AS monthly,
	                    	sum(a.qty * a.hpp) AS totalstokhpp
                    	FROM mb_rms20_mbdc.mb_rms_saldostokproduktoko_v2_cab_sby a
--                  	JOIN smi_rms01_rpt.list_monitor_item_indicator_so it ON it.kodeproduk::text = a.kodeproduk::text
                    	JOIN mb_rms01_mbho."MstProduk" b ON a.kodeproduk::text = b."kodeProduk"::text
                  		WHERE b."statusData" <> 9 
                  		AND b."idJenisProduk" <> 4 
                  		AND a.qty <> 0::numeric 
                  		AND (a.tglsaldo = (date_trunc('month'::text, CURRENT_DATE + '-1 mons'::interval) + '1 mon -1 days'::interval)::date OR a.tglsaldo = (date_trunc('month'::text, CURRENT_DATE + '-2 mons'::interval) + '1 mon -1 days'::interval)::date OR a.tglsaldo = (date_trunc('month'::text, CURRENT_DATE + '-3 mons'::interval) + '1 mon -1 days'::interval)::date OR a.tglsaldo = (date_trunc('month'::text, CURRENT_DATE + '-4 mons'::interval) + '1 mon -1 days'::interval)::date)
                  		GROUP BY a.idcabang, a.namacabang, a.kodetoko, (
										                        CASE
										                            WHEN a.tglsaldo = (date_trunc('month'::text, CURRENT_DATE + '-1 mons'::interval) + '1 mon -1 days'::interval)::date THEN '-1'::integer
										                            WHEN a.tglsaldo = (date_trunc('month'::text, CURRENT_DATE + '-2 mons'::interval) + '1 mon -1 days'::interval)::date THEN '-2'::integer
										                            WHEN a.tglsaldo = (date_trunc('month'::text, CURRENT_DATE + '-3 mons'::interval) + '1 mon -1 days'::interval)::date THEN '-3'::integer
										                            WHEN a.tglsaldo = (date_trunc('month'::text, CURRENT_DATE + '-4 mons'::interval) + '1 mon -1 days'::interval)::date THEN '-4'::integer
										                            ELSE NULL::integer
										                        END)
                        ) sb ON sb.namacabang::text = wd.namacabang::text AND sb.kodetoko::integer::numeric = wd.kodetoko AND sb.monthly::double precision = wd.monthly
          GROUP BY s.namacabang, s.kodetoko, wd.monthly, wd.workdays, s.spd_hpp, s.spd, s.spd_hpply, s.spdly, sb.totalstokhpp) sm
  GROUP BY sm.namacabang, sm.kodetoko
WITH DATA;

-- View indexes:
CREATE UNIQUE INDEX smi_mv_listitem_sales_mtd_jkt_kodetoko_unique_idx ON mb_rms20_mbdc.smi_mv_listitem_sales_mtd_sby USING btree (kodetoko);

 ----------------------------------------------------------------------------------------

-- mb_rms20_mbdc.smi_mv_sales_mtd_sby source

CREATE MATERIALIZED VIEW mb_rms20_mbdc.smi_mv_sales_mtd_sby
TABLESPACE pg_default
AS SELECT slsmon.namacabang,
    slsmon.kodetoko,
    t.namatoko,
    slsmon.wd_min1,
    slsmon.wd_min2,
    slsmon.wd_min3,
    slsmon.wd_min4,
    slsmon.spdhpp_min1,
    slsmon.spdhpp_min2,
    slsmon.spdhpp_min3,
    slsmon.spdhpp_min4,
    slsmon.spd_min1,
    slsmon.spd_min2,
    slsmon.spd_min3,
    slsmon.spd_min4,
    slsmon.spdhpp_min1ly,
    slsmon.spdhpp_min2ly,
    slsmon.spdhpp_min3ly,
    slsmon.spdhpp_min4ly,
    slsmon.spd_min1ly,
    slsmon.spd_min2ly,
    slsmon.spd_min3ly,
    slsmon.spd_min4ly,
    slsmon.totalstokhpp_min1,
    slsmon.totalstokhpp_min2,
    slsmon.totalstokhpp_min3,
    slsmon.totalstokhpp_min4,
    sfm.wd_min1 AS fm_wd_min1,
    sfm.wd_min2 AS fm_wd_min2,
    sfm.wd_min3 AS fm_wd_min3,
    sfm.wd_min4 AS fm_wd_min4,
    sfm.qty_min1 AS fm_qty_min1,
    sfm.qty_min2 AS fm_qty_min2,
    sfm.qty_min3 AS fm_qty_min3,
    sfm.qty_min4 AS fm_qty_min4,
    sfm.spdhpp_min1 AS fm_spdhpp_min1,
    sfm.spdhpp_min2 AS fm_spdhpp_min2,
    sfm.spdhpp_min3 AS fm_spdhpp_min3,
    sfm.spdhpp_min4 AS fm_spdhpp_min4,
    sfm.spd_min1 AS fm_spd_min1,
    sfm.spd_min2 AS fm_spd_min2,
    sfm.spd_min3 AS fm_spd_min3,
    sfm.spd_min4 AS fm_spd_min4,
    sfm.qty_min1ly AS fm_qty_min1ly,
    sfm.qty_min2ly AS fm_qty_min2ly,
    sfm.qty_min3ly AS fm_qty_min3ly,
    sfm.qty_min4ly AS fm_qty_min4ly,
    sfm.spdhpp_min1ly AS fm_spdhpp_min1ly,
    sfm.spdhpp_min2ly AS fm_spdhpp_min2ly,
    sfm.spdhpp_min3ly AS fm_spdhpp_min3ly,
    sfm.spdhpp_min4ly AS fm_spdhpp_min4ly,
    sfm.spd_min1ly AS fm_spd_min1ly,
    sfm.spd_min2ly AS fm_spd_min2ly,
    sfm.spd_min3ly AS fm_spd_min3ly,
    sfm.spd_min4ly AS fm_spd_min4ly,
    sfm.totalstokhpp_min1 AS fm_totalstokhpp_min1,
    sfm.totalstokhpp_min2 AS fm_totalstokhpp_min2,
    sfm.totalstokhpp_min3 AS fm_totalstokhpp_min3,
    sfm.totalstokhpp_min4 AS fm_totalstokhpp_min4,
    slsli.wd_min1 AS li_wd_min1,
    slsli.wd_min2 AS li_wd_min2,
    slsli.wd_min3 AS li_wd_min3,
    slsli.wd_min4 AS li_wd_min4,
    slsli.spdhpp_min1 AS li_spdhpp_min1,
    slsli.spdhpp_min2 AS li_spdhpp_min2,
    slsli.spdhpp_min3 AS li_spdhpp_min3,
    slsli.spdhpp_min4 AS li_spdhpp_min4,
    slsli.spd_min1 AS li_spd_min1,
    slsli.spd_min2 AS li_spd_min2,
    slsli.spd_min3 AS li_spd_min3,
    slsli.spd_min4 AS li_spd_min4,
    slsli.spdhpp_min1ly AS li_spdhpp_min1ly,
    slsli.spdhpp_min2ly AS li_spdhpp_min2ly,
    slsli.spdhpp_min3ly AS li_spdhpp_min3ly,
    slsli.spdhpp_min4ly AS li_spdhpp_min4ly,
    slsli.spd_min1ly AS li_spd_min1ly,
    slsli.spd_min2ly AS li_spd_min2ly,
    slsli.spd_min3ly AS li_spd_min3ly,
    slsli.spd_min4ly AS li_spd_min4ly,
    slsli.totalstokhpp_min1 AS li_totalstokhpp_min1,
    slsli.totalstokhpp_min2 AS li_totalstokhpp_min2,
    slsli.totalstokhpp_min3 AS li_totalstokhpp_min3,
    slsli.totalstokhpp_min4 AS li_totalstokhpp_min4
   FROM ( SELECT sm.namacabang,
            sm.kodetoko,
            sum(sm.wd_min1) AS wd_min1,
            sum(sm.wd_min2) AS wd_min2,
            sum(sm.wd_min3) AS wd_min3,
            sum(sm.wd_min4) AS wd_min4,
            sum(sm.spdhpp_min1) AS spdhpp_min1,
            sum(sm.spdhpp_min2) AS spdhpp_min2,
            sum(sm.spdhpp_min3) AS spdhpp_min3,
            sum(sm.spdhpp_min4) AS spdhpp_min4,
            sum(sm.spd_min1) AS spd_min1,
            sum(sm.spd_min2) AS spd_min2,
            sum(sm.spd_min3) AS spd_min3,
            sum(sm.spd_min4) AS spd_min4,
            sum(sm.spdhpp_min1ly) AS spdhpp_min1ly,
            sum(sm.spdhpp_min2ly) AS spdhpp_min2ly,
            sum(sm.spdhpp_min3ly) AS spdhpp_min3ly,
            sum(sm.spdhpp_min4ly) AS spdhpp_min4ly,
            sum(sm.spd_min1ly) AS spd_min1ly,
            sum(sm.spd_min2ly) AS spd_min2ly,
            sum(sm.spd_min3ly) AS spd_min3ly,
            sum(sm.spd_min4ly) AS spd_min4ly,
            sum(sm.totalstokhpp_min1) AS totalstokhpp_min1,
            sum(sm.totalstokhpp_min2) AS totalstokhpp_min2,
            sum(sm.totalstokhpp_min3) AS totalstokhpp_min3,
            sum(sm.totalstokhpp_min4) AS totalstokhpp_min4
           FROM ( SELECT s.namacabang,
                    s.kodetoko,
                        CASE
                            WHEN wd.monthly = '-1'::integer::double precision THEN wd.workdays
                            ELSE 0::bigint
                        END AS wd_min1,
                        CASE
                            WHEN wd.monthly = '-2'::integer::double precision THEN wd.workdays
                            ELSE 0::bigint
                        END AS wd_min2,
                        CASE
                            WHEN wd.monthly = '-3'::integer::double precision THEN wd.workdays
                            ELSE 0::bigint
                        END AS wd_min3,
                        CASE
                            WHEN wd.monthly = '-4'::integer::double precision THEN wd.workdays
                            ELSE 0::bigint
                        END AS wd_min4,
                        CASE
                            WHEN wd.monthly = '-1'::integer::double precision THEN s.spd_hpp
                            ELSE 0::numeric
                        END AS spdhpp_min1,
                        CASE
                            WHEN wd.monthly = '-2'::integer::double precision THEN s.spd_hpp
                            ELSE 0::numeric
                        END AS spdhpp_min2,
                        CASE
                            WHEN wd.monthly = '-3'::integer::double precision THEN s.spd_hpp
                            ELSE 0::numeric
                        END AS spdhpp_min3,
                        CASE
                            WHEN wd.monthly = '-4'::integer::double precision THEN s.spd_hpp
                            ELSE 0::numeric
                        END AS spdhpp_min4,
                        CASE
                            WHEN wd.monthly = '-1'::integer::double precision THEN s.spd
                            ELSE 0::numeric
                        END AS spd_min1,
                        CASE
                            WHEN wd.monthly = '-2'::integer::double precision THEN s.spd
                            ELSE 0::numeric
                        END AS spd_min2,
                        CASE
                            WHEN wd.monthly = '-3'::integer::double precision THEN s.spd
                            ELSE 0::numeric
                        END AS spd_min3,
                        CASE
                            WHEN wd.monthly = '-4'::integer::double precision THEN s.spd
                            ELSE 0::numeric
                        END AS spd_min4,
                        CASE
                            WHEN wd.monthly = '-1'::integer::double precision THEN s.spd_hpply
                            ELSE 0::numeric
                        END AS spdhpp_min1ly,
                        CASE
                            WHEN wd.monthly = '-2'::integer::double precision THEN s.spd_hpply
                            ELSE 0::numeric
                        END AS spdhpp_min2ly,
                        CASE
                            WHEN wd.monthly = '-3'::integer::double precision THEN s.spd_hpply
                            ELSE 0::numeric
                        END AS spdhpp_min3ly,
                        CASE
                            WHEN wd.monthly = '-4'::integer::double precision THEN s.spd_hpply
                            ELSE 0::numeric
                        END AS spdhpp_min4ly,
                        CASE
                            WHEN wd.monthly = '-1'::integer::double precision THEN s.spdly
                            ELSE 0::numeric
                        END AS spd_min1ly,
                        CASE
                            WHEN wd.monthly = '-2'::integer::double precision THEN s.spdly
                            ELSE 0::numeric
                        END AS spd_min2ly,
                        CASE
                            WHEN wd.monthly = '-3'::integer::double precision THEN s.spdly
                            ELSE 0::numeric
                        END AS spd_min3ly,
                        CASE
                            WHEN wd.monthly = '-4'::integer::double precision THEN s.spdly
                            ELSE 0::numeric
                        END AS spd_min4ly,
                        CASE
                            WHEN wd.monthly = '-1'::integer::double precision THEN sb.totalstokhpp
                            ELSE 0::numeric
                        END AS totalstokhpp_min1,
                        CASE
                            WHEN wd.monthly = '-2'::integer::double precision THEN sb.totalstokhpp
                            ELSE 0::numeric
                        END AS totalstokhpp_min2,
                        CASE
                            WHEN wd.monthly = '-3'::integer::double precision THEN sb.totalstokhpp
                            ELSE 0::numeric
                        END AS totalstokhpp_min3,
                        CASE
                            WHEN wd.monthly = '-4'::integer::double precision THEN sb.totalstokhpp
                            ELSE 0::numeric
                        END AS totalstokhpp_min4
                   FROM ( SELECT slshpp.namacabang,
                            slshpp.kodetoko,
                            slshpp.monthly,
                            sum(slshpp.spd_hpp) AS spd_hpp,
                            sum(slshpp.spd_hpply) AS spd_hpply,
                            sum(slshpp.spd) AS spd,
                            sum(slshpp.spdly) AS spdly
                           FROM ( 
                           			SELECT 
                           				s_1.namacabang,
                                    	s_1.kodetoko,
										CASE
											WHEN s_1.monthly = '-13'::integer::double precision THEN '-1'::integer::double precision
											WHEN s_1.monthly = '-14'::integer::double precision THEN '-2'::integer::double precision
											WHEN s_1.monthly = '-15'::integer::double precision THEN '-3'::integer::double precision
											WHEN s_1.monthly = '-16'::integer::double precision THEN '-4'::integer::double precision
											ELSE s_1.monthly
										END AS monthly,
										CASE
											WHEN s_1.monthly >= '-4'::integer::double precision AND s_1.monthly <= '-1'::integer::double precision THEN s_1.spd_hpp
											ELSE 0::numeric
										END AS spd_hpp,
										CASE
											WHEN s_1.monthly >= '-16'::integer::double precision AND s_1.monthly <= '-13'::integer::double precision THEN s_1.spd_hpp
											ELSE 0::numeric
										END AS spd_hpply,
										CASE
											WHEN s_1.monthly >= '-4'::integer::double precision AND s_1.monthly <= '-1'::integer::double precision THEN s_1.spd
											ELSE 0::numeric
										END AS spd,
										CASE
											WHEN s_1.monthly >= '-16'::integer::double precision AND s_1.monthly <= '-13'::integer::double precision THEN s_1.spd
											ELSE 0::numeric
										END AS spdly
	                                   FROM ( 
	                                   			SELECT 
	                                   				sls.namacabang,
											    	sls.kodetoko,
											    	date_part('year'::text, age(date_trunc('month'::text, sls.tanggal::timestamp with time zone), date_trunc('month'::text, CURRENT_DATE::timestamp with time zone))) * 12::double precision + date_part('month'::text, age(date_trunc('month'::text, sls.tanggal::timestamp with time zone), date_trunc('month'::text, CURRENT_DATE::timestamp with time zone))) AS monthly,
											    	sum(sls.qty * sls.hpp) AS spd_hpp,
											    	sum(sls.subtotal) AS spd
											   	FROM PUBLIC.mb_rms20_transaksi_toko_perjenis_member_v3 sls
											  	WHERE sls.tanggal >= date_trunc('month'::text, CURRENT_DATE + '-4 mons'::interval) AND sls.tanggal <= (date_trunc('month'::text, CURRENT_DATE + '-1 mons'::interval) + '1 mon -1 days'::interval)::date OR sls.tanggal >= (date_trunc('month'::text, CURRENT_DATE + '-4 mons'::interval) + '-1 years'::interval) AND sls.tanggal <= ((date_trunc('month'::text, CURRENT_DATE + '-1 mons'::interval) + '1 mon -1 days'::interval)::date + '-1 years'::interval)
											  	GROUP BY sls.namacabang, sls.kodetoko, (date_part('year'::text, age(date_trunc('month'::text, sls.tanggal::timestamp with time zone), date_trunc('month'::text, CURRENT_DATE::timestamp with time zone))) * 12::double precision + date_part('month'::text, age(date_trunc('month'::text, sls.tanggal::timestamp with time zone), date_trunc('month'::text, CURRENT_DATE::timestamp with time zone))))
											  ) s_1
								) slshpp
                          		GROUP BY slshpp.namacabang, slshpp.kodetoko, slshpp.monthly) s
					JOIN ( 
                     		SELECT 
                     			slsday.namacabang,
                            	slsday.kodetoko,
                            	slsday.monthly,
                            	count(slsday.tanggal) AS workdays
                           	FROM ( 
                           			SELECT 
	                           			sls.namacabang,
	                                    sls.kodetoko,
	                                    date_part('year'::text, age(date_trunc('month'::text, sls.tanggal::timestamp with time zone), date_trunc('month'::text, CURRENT_DATE::timestamp with time zone))) * 12::double precision + date_part('month'::text, age(date_trunc('month'::text, sls.tanggal::timestamp with time zone), date_trunc('month'::text, CURRENT_DATE::timestamp with time zone))) AS monthly,
	                                    sls.tanggal
                                  	FROM PUBLIC.mb_rms20_transaksi_toko_perjenis_member_v3 sls
                                  	WHERE sls.tanggal >= date_trunc('month'::text, CURRENT_DATE + '-4 mons'::interval) AND sls.tanggal <= (date_trunc('month'::text, CURRENT_DATE + '-1 mons'::interval) + '1 mon -1 days'::interval)::date
                                  	GROUP BY sls.namacabang, sls.kodetoko, (date_part('year'::text, age(date_trunc('month'::text, sls.tanggal::timestamp with time zone), date_trunc('month'::text, CURRENT_DATE::timestamp with time zone))) * 12::double precision + date_part('month'::text, age(date_trunc('month'::text, sls.tanggal::timestamp with time zone), date_trunc('month'::text, CURRENT_DATE::timestamp with time zone)))), sls.tanggal
                                 ) slsday
                          GROUP BY slsday.namacabang, slsday.kodetoko, slsday.monthly) wd ON s.namacabang::text = wd.namacabang::text AND s.kodetoko = wd.kodetoko AND s.monthly = wd.monthly
                     LEFT JOIN (
		                     		SELECT 
			                     		a.idcabang,
			                            a.namacabang,
			                            a.kodetoko,
		                                CASE
		                                    WHEN a.tglsaldo = (date_trunc('month'::text, CURRENT_DATE + '-1 mons'::interval) + '1 mon -1 days'::interval)::date THEN '-1'::integer
		                                    WHEN a.tglsaldo = (date_trunc('month'::text, CURRENT_DATE + '-2 mons'::interval) + '1 mon -1 days'::interval)::date THEN '-2'::integer
		                                    WHEN a.tglsaldo = (date_trunc('month'::text, CURRENT_DATE + '-3 mons'::interval) + '1 mon -1 days'::interval)::date THEN '-3'::integer
		                                    WHEN a.tglsaldo = (date_trunc('month'::text, CURRENT_DATE + '-4 mons'::interval) + '1 mon -1 days'::interval)::date THEN '-4'::integer
		                                    ELSE NULL::integer
		                                END AS monthly,
			                            sum(a.qty * a.hpp) AS totalstokhpp
--		                           	FROM smi_stock_balance_toko_cab_jkt_v31_rms a
--		                            JOIN smi_mstprodukho_rms b ON a.kodeproduk::text = b.kodeproduk::text
		                               	FROM mb_rms20_mbdc.mb_rms_saldostokproduktoko_v2_cab_sby a
				                    	JOIN mb_rms01_mbho."MstProduk" b ON a.kodeproduk::text = b."kodeProduk"::text
		                          	WHERE b."statusData" <> 9 
		                          	AND b."idJenisProduk" <> 4 
		                          	AND a.qty <> 0::numeric AND (a.tglsaldo = (date_trunc('month'::text, CURRENT_DATE + '-1 mons'::interval) + '1 mon -1 days'::interval)::date OR a.tglsaldo = (date_trunc('month'::text, CURRENT_DATE + '-2 mons'::interval) + '1 mon -1 days'::interval)::date OR a.tglsaldo = (date_trunc('month'::text, CURRENT_DATE + '-3 mons'::interval) + '1 mon -1 days'::interval)::date OR a.tglsaldo = (date_trunc('month'::text, CURRENT_DATE + '-4 mons'::interval) + '1 mon -1 days'::interval)::date)
		                          	GROUP BY a.idcabang, a.namacabang, a.kodetoko, (
													                                CASE
													                                    WHEN a.tglsaldo = (date_trunc('month'::text, CURRENT_DATE + '-1 mons'::interval) + '1 mon -1 days'::interval)::date THEN '-1'::integer
													                                    WHEN a.tglsaldo = (date_trunc('month'::text, CURRENT_DATE + '-2 mons'::interval) + '1 mon -1 days'::interval)::date THEN '-2'::integer
													                                    WHEN a.tglsaldo = (date_trunc('month'::text, CURRENT_DATE + '-3 mons'::interval) + '1 mon -1 days'::interval)::date THEN '-3'::integer
													                                    WHEN a.tglsaldo = (date_trunc('month'::text, CURRENT_DATE + '-4 mons'::interval) + '1 mon -1 days'::interval)::date THEN '-4'::integer
													                                    ELSE NULL::integer
													                                END)
                               ) sb ON sb.namacabang::text = wd.namacabang::text AND sb.kodetoko::integer::numeric = wd.kodetoko AND sb.monthly::double precision = wd.monthly
                  GROUP BY s.namacabang, s.kodetoko, wd.monthly, wd.workdays, s.spd_hpp, s.spd, s.spd_hpply, s.spdly, sb.totalstokhpp) sm
          GROUP BY sm.namacabang, sm.kodetoko) slsmon
     LEFT JOIN mb_rms20_mbdc.smi_mv_fastmoving_sales_mtd_sby sfm ON sfm.kodetoko = slsmon.kodetoko
     LEFT JOIN mb_rms20_mbdc.smi_mv_listitem_sales_mtd_sby slsli ON slsli.kodetoko = slsmon.kodetoko
     JOIN mb_rms10_mbdc.mv_msttoko t ON t.kodetoko::numeric = slsmon.kodetoko
WITH DATA;

-- View indexes:
CREATE UNIQUE INDEX smi_mv_sales_mtd_jkt_kodetoko_unique_idx ON mb_rms20_mbdc.smi_mv_sales_mtd_sby USING btree (kodetoko);

 ----------------------------------------------------------------------------------------
 ----------------------------------------------------------------------------------------
-- 16.iso_indicatorsosidak_salesmonitor_rpt
refresh materialized view concurrently smi_rms01_rpt.mv_indicatorsosidak_salesmonitor_rpt;

refresh materialized view concurrently mb_rms01_mbho.mv_indicatorsosidak_salesmonitor_rpt;--OK
 ----------------------------------------------------------------------------------------
 ----------------------------------------------------------------------------------------

-- mb_rms01_mbho.mv_indicatorsosidak_salesmonitor_rpt source

CREATE MATERIALIZED VIEW mb_rms01_mbho.mv_indicatorsosidak_salesmonitor_rpt
TABLESPACE pg_default
AS 	SELECT 
		row_number() OVER () AS seq_no,
	    salesmonitor.namacabang,
	    salesmonitor.kodetoko,
	    salesmonitor.namatoko,
	    salesmonitor.dsi_store_min3,
	    salesmonitor.spd_value_store_min3,
	    salesmonitor.trend_salesly_store_min3,
	    salesmonitor.growth_value_store_min3,
	    salesmonitor.dsi_store_min2,
	    salesmonitor.spd_value_store_min2,
	    salesmonitor.trend_salesly_store_min2,
	    salesmonitor.growth_value_store_min2,
	    salesmonitor.dsi_store_min1,
	    salesmonitor.spd_value_store_min1,
	    salesmonitor.trend_salesly_store_min1,
	    salesmonitor.growth_value_store_min1,
	    salesmonitor.dsi_fastmoving_min3,
	    salesmonitor.spd_qty_fastmoving_min3,
	    salesmonitor.growth_qty_fastmoving_min3,
	    salesmonitor.growth_qtyly_fastmoving_min3,
	    salesmonitor.spd_value_fastmoving_min3,
	    salesmonitor.growth_value_fastmoving_min3,
	    salesmonitor.dsi_fastmoving_min2,
	    salesmonitor.spd_qty_fastmoving_min2,
	    salesmonitor.growth_qty_fastmoving_min2,
	    salesmonitor.growth_qtyly_fastmoving_min2,
	    salesmonitor.spd_value_fastmoving_min2,
	    salesmonitor.growth_value_fastmoving_min2,
	    salesmonitor.dsi_fastmoving_min1,
	    salesmonitor.spd_qty_fastmoving_min1,
	    salesmonitor.growth_qty_fastmoving_min1,
	    salesmonitor.growth_qtyly_fastmoving_min1,
	    salesmonitor.spd_value_fastmoving_min1,
	    salesmonitor.growth_value_fastmoving_min1ly,
	    salesmonitor.growth_value_fastmoving_min1,
	    salesmonitor.spd_itemmonitor_min3,
	    salesmonitor.dsi_itemmonitor_min3,
	    salesmonitor.trend_spdly_itemmonitor_min3,
	    salesmonitor.growth_itemmonitor_min3,
	    salesmonitor.spd_itemmonitor_min2,
	    salesmonitor.dsi_itemmonitor_min2,
	    salesmonitor.trend_spdly_itemmonitor_min2,
	    salesmonitor.growth_itemmonitor_min2,
	    salesmonitor.spd_itemmonitor_min1,
	    salesmonitor.dsi_itemmonitor_min1,
	    salesmonitor.trend_spdly_itemmonitor_min1,
	    salesmonitor.growth_itemmonitor_min1,
	    COALESCE((abs(salesmonitor.growth_value_store_min1 - salesmonitor.trend_salesly_store_min1) / 100::numeric)::double precision * 1000::double precision, 0::double precision) + COALESCE((abs(salesmonitor.growth_value_fastmoving_min1 - salesmonitor.growth_value_fastmoving_min1ly) / 100::numeric)::double precision * 1000::double precision, 0::double precision) + COALESCE((abs(salesmonitor.growth_itemmonitor_min1 - salesmonitor.trend_spdly_itemmonitor_min1) / 100::numeric)::double precision * 1000::double precision, 0::double precision) AS point
	FROM ( SELECT sm.namacabang,
            sm.kodetoko,
            sm.namatoko,
            round((sm.totalstokhpp_min4 + sm.totalstokhpp_min3) / 2::numeric /
                CASE
                    WHEN sm.spd_min3 = 0::numeric THEN NULL::numeric
                    ELSE sm.spd_min3
                END *
                CASE
                    WHEN sm.wd_min3 = 0::numeric THEN NULL::numeric
                    ELSE sm.wd_min3
                END) AS dsi_store_min3,
            sm.spd_min3 AS spd_value_store_min3,
            round((sm.spd_min3ly - sm.spd_min4ly) /
                CASE
                    WHEN sm.spd_min3ly = 0::numeric THEN NULL::numeric
                    ELSE sm.spd_min3ly
                END * 100::numeric) AS trend_salesly_store_min3,
            round((sm.spd_min3 - sm.spd_min4) /
                CASE
                    WHEN sm.spd_min3 = 0::numeric THEN NULL::numeric
                    ELSE sm.spd_min3
                END * 100::numeric) AS growth_value_store_min3,
            round((sm.totalstokhpp_min3 + sm.totalstokhpp_min2) / 2::numeric /
                CASE
                    WHEN sm.spd_min2 = 0::numeric THEN NULL::numeric
                    ELSE sm.spd_min2
                END *
                CASE
                    WHEN sm.wd_min2 = 0::numeric THEN NULL::numeric
                    ELSE sm.wd_min2
                END) AS dsi_store_min2,
            sm.spd_min2 AS spd_value_store_min2,
            round((sm.spd_min2ly - sm.spd_min3ly) /
                CASE
                    WHEN sm.spd_min2ly = 0::numeric THEN NULL::numeric
                    ELSE sm.spd_min2ly
                END * 100::numeric) AS trend_salesly_store_min2,
            round((sm.spd_min2 - sm.spd_min3) /
                CASE
                    WHEN sm.spd_min2 = 0::numeric THEN NULL::numeric
                    ELSE sm.spd_min2
                END * 100::numeric) AS growth_value_store_min2,
            round((sm.totalstokhpp_min2 + sm.totalstokhpp_min1) / 2::numeric /
                CASE
                    WHEN sm.spd_min1 = 0::numeric THEN NULL::numeric
                    ELSE sm.spd_min1
                END *
                CASE
                    WHEN sm.wd_min1 = 0::numeric THEN NULL::numeric
                    ELSE sm.wd_min1
                END) AS dsi_store_min1,
            sm.spd_min1 AS spd_value_store_min1,
            round((sm.spd_min1ly - sm.spd_min2ly) /
                CASE
                    WHEN sm.spd_min1ly = 0::numeric THEN NULL::numeric
                    ELSE sm.spd_min1ly
                END * 100::numeric) AS trend_salesly_store_min1,
            round((sm.spd_min1 - sm.spd_min2) /
                CASE
                    WHEN sm.spd_min1 = 0::numeric THEN NULL::numeric
                    ELSE sm.spd_min1
                END * 100::numeric) AS growth_value_store_min1,
            round((sm.fm_totalstokhpp_min4 + sm.fm_totalstokhpp_min3) / 2::numeric /
                CASE
                    WHEN sm.fm_spd_min3 = 0::numeric THEN NULL::numeric
                    ELSE sm.fm_spd_min3
                END *
                CASE
                    WHEN sm.fm_wd_min3 = 0::numeric THEN NULL::numeric
                    ELSE sm.fm_wd_min3
                END) AS dsi_fastmoving_min3,
            sm.fm_qty_min3 AS spd_qty_fastmoving_min3,
            round((sm.fm_qty_min3 - sm.fm_qty_min4) /
                CASE
                    WHEN sm.fm_qty_min3 = 0::numeric THEN NULL::numeric
                    ELSE sm.fm_qty_min3
                END * 100::numeric) AS growth_qty_fastmoving_min3,
            round((sm.fm_qty_min3ly - sm.fm_qty_min4ly) /
                CASE
                    WHEN sm.fm_qty_min3ly = 0::numeric THEN NULL::numeric
                    ELSE sm.fm_qty_min3ly
                END * 100::numeric) AS growth_qtyly_fastmoving_min3,
            sm.fm_spd_min3 AS spd_value_fastmoving_min3,
            round((sm.fm_spd_min3 - sm.fm_spd_min4) /
                CASE
                    WHEN sm.fm_spd_min3 = 0::numeric THEN NULL::numeric
                    ELSE sm.fm_spd_min3
                END * 100::numeric) AS growth_value_fastmoving_min3,
            round((sm.fm_totalstokhpp_min3 + sm.fm_totalstokhpp_min2) / 2::numeric /
                CASE
                    WHEN sm.fm_spd_min2 = 0::numeric THEN NULL::numeric
                    ELSE sm.fm_spd_min2
                END *
                CASE
                    WHEN sm.fm_wd_min2 = 0::numeric THEN NULL::numeric
                    ELSE sm.fm_wd_min2
                END) AS dsi_fastmoving_min2,
            sm.fm_qty_min2 AS spd_qty_fastmoving_min2,
            round((sm.fm_qty_min2 - sm.fm_qty_min3) /
                CASE
                    WHEN sm.fm_qty_min2 = 0::numeric THEN NULL::numeric
                    ELSE sm.fm_qty_min2
                END * 100::numeric) AS growth_qty_fastmoving_min2,
            round((sm.fm_qty_min2ly - sm.fm_qty_min3ly) /
                CASE
                    WHEN sm.fm_qty_min2ly = 0::numeric THEN NULL::numeric
                    ELSE sm.fm_qty_min2ly
                END * 100::numeric) AS growth_qtyly_fastmoving_min2,
            sm.fm_spd_min2 AS spd_value_fastmoving_min2,
            round((sm.fm_spd_min2 - sm.fm_spd_min3) /
                CASE
                    WHEN sm.fm_spd_min2 = 0::numeric THEN NULL::numeric
                    ELSE sm.fm_spd_min2
                END * 100::numeric) AS growth_value_fastmoving_min2,
            round((sm.fm_totalstokhpp_min2 + sm.fm_totalstokhpp_min1) / 2::numeric /
                CASE
                    WHEN sm.fm_spd_min1 = 0::numeric THEN NULL::numeric
                    ELSE sm.fm_spd_min1
                END *
                CASE
                    WHEN sm.fm_wd_min1 = 0::numeric THEN NULL::numeric
                    ELSE sm.fm_wd_min1
                END) AS dsi_fastmoving_min1,
            sm.fm_qty_min1 AS spd_qty_fastmoving_min1,
            round((sm.fm_qty_min1 - sm.fm_qty_min2) /
                CASE
                    WHEN sm.fm_qty_min1 = 0::numeric THEN NULL::numeric
                    ELSE sm.fm_qty_min1
                END * 100::numeric) AS growth_qty_fastmoving_min1,
            round((sm.fm_qty_min1ly - sm.fm_qty_min2ly) /
                CASE
                    WHEN sm.fm_qty_min1ly = 0::numeric THEN NULL::numeric
                    ELSE sm.fm_qty_min1ly
                END * 100::numeric) AS growth_qtyly_fastmoving_min1,
            sm.fm_spd_min1 AS spd_value_fastmoving_min1,
            round((sm.fm_spd_min1ly - sm.fm_spd_min2ly) /
                CASE
                    WHEN sm.fm_spd_min1ly = 0::numeric THEN NULL::numeric
                    ELSE sm.fm_spd_min1ly
                END * 100::numeric) AS growth_value_fastmoving_min1ly,
            round((sm.fm_spd_min1 - sm.fm_spd_min2) /
                CASE
                    WHEN sm.fm_spd_min1 = 0::numeric THEN NULL::numeric
                    ELSE sm.fm_spd_min1
                END * 100::numeric) AS growth_value_fastmoving_min1,
            sm.li_spd_min3 AS spd_itemmonitor_min3,
            round((sm.li_totalstokhpp_min4 + sm.li_totalstokhpp_min3) / 2::numeric /
                CASE
                    WHEN sm.li_spd_min3 = 0::numeric THEN NULL::numeric
                    ELSE sm.li_spd_min3
                END *
                CASE
                    WHEN sm.li_wd_min3 = 0::numeric THEN NULL::numeric
                    ELSE sm.li_wd_min3
                END) AS dsi_itemmonitor_min3,
            round((sm.li_spd_min3ly - sm.li_spd_min4ly) /
                CASE
                    WHEN sm.li_spd_min3ly = 0::numeric THEN NULL::numeric
                    ELSE sm.li_spd_min3ly
                END * 100::numeric) AS trend_spdly_itemmonitor_min3,
            round((sm.li_spd_min3 - sm.li_spd_min4) /
                CASE
                    WHEN sm.li_spd_min3 = 0::numeric THEN NULL::numeric
                    ELSE sm.li_spd_min3
                END * 100::numeric) AS growth_itemmonitor_min3,
            sm.li_spd_min2 AS spd_itemmonitor_min2,
            round((sm.li_totalstokhpp_min3 + sm.li_totalstokhpp_min2) / 2::numeric /
                CASE
                    WHEN sm.li_spd_min2 = 0::numeric THEN NULL::numeric
                    ELSE sm.li_spd_min2
                END *
                CASE
                    WHEN sm.li_wd_min2 = 0::numeric THEN NULL::numeric
                    ELSE sm.li_wd_min2
                END) AS dsi_itemmonitor_min2,
            round((sm.li_spd_min2ly - sm.li_spd_min3ly) /
                CASE
                    WHEN sm.li_spd_min2ly = 0::numeric THEN NULL::numeric
                    ELSE sm.li_spd_min2ly
                END * 100::numeric) AS trend_spdly_itemmonitor_min2,
            round((sm.li_spd_min2 - sm.li_spd_min3) /
                CASE
                    WHEN sm.li_spd_min2 = 0::numeric THEN NULL::numeric
                    ELSE sm.li_spd_min2
                END * 100::numeric) AS growth_itemmonitor_min2,
            sm.li_spd_min1 AS spd_itemmonitor_min1,
            round((sm.li_totalstokhpp_min2 + sm.li_totalstokhpp_min1) / 2::numeric /
                CASE
                    WHEN sm.li_spd_min1 = 0::numeric THEN NULL::numeric
                    ELSE sm.li_spd_min1
                END *
                CASE
                    WHEN sm.li_wd_min1 = 0::numeric THEN NULL::numeric
                    ELSE sm.li_wd_min1
                END) AS dsi_itemmonitor_min1,
            round((sm.li_spd_min1ly - sm.li_spd_min2ly) /
                CASE
                    WHEN sm.li_spd_min1ly = 0::numeric THEN NULL::numeric
                    ELSE sm.li_spd_min1ly
                END * 100::numeric) AS trend_spdly_itemmonitor_min1,
            round((sm.li_spd_min1 - sm.li_spd_min2) /
                CASE
                    WHEN sm.li_spd_min1 = 0::numeric THEN NULL::numeric
                    ELSE sm.li_spd_min1
                END * 100::numeric) AS growth_itemmonitor_min1
           FROM ( 
           			SELECT 
	           			smi_mv_sales_mtd_jkt.namacabang,
	                    smi_mv_sales_mtd_jkt.kodetoko,
	                    smi_mv_sales_mtd_jkt.namatoko,
	                    smi_mv_sales_mtd_jkt.wd_min1,
	                    smi_mv_sales_mtd_jkt.wd_min2,
	                    smi_mv_sales_mtd_jkt.wd_min3,
	                    smi_mv_sales_mtd_jkt.wd_min4,
	                    smi_mv_sales_mtd_jkt.spdhpp_min1,
	                    smi_mv_sales_mtd_jkt.spdhpp_min2,
	                    smi_mv_sales_mtd_jkt.spdhpp_min3,
	                    smi_mv_sales_mtd_jkt.spdhpp_min4,
	                    smi_mv_sales_mtd_jkt.spd_min1,
	                    smi_mv_sales_mtd_jkt.spd_min2,
	                    smi_mv_sales_mtd_jkt.spd_min3,
	                    smi_mv_sales_mtd_jkt.spd_min4,
	                    smi_mv_sales_mtd_jkt.spdhpp_min1ly,
	                    smi_mv_sales_mtd_jkt.spdhpp_min2ly,
	                    smi_mv_sales_mtd_jkt.spdhpp_min3ly,
	                    smi_mv_sales_mtd_jkt.spdhpp_min4ly,
	                    smi_mv_sales_mtd_jkt.spd_min1ly,
	                    smi_mv_sales_mtd_jkt.spd_min2ly,
	                    smi_mv_sales_mtd_jkt.spd_min3ly,
	                    smi_mv_sales_mtd_jkt.spd_min4ly,
	                    smi_mv_sales_mtd_jkt.totalstokhpp_min1,
	                    smi_mv_sales_mtd_jkt.totalstokhpp_min2,
	                    smi_mv_sales_mtd_jkt.totalstokhpp_min3,
	                    smi_mv_sales_mtd_jkt.totalstokhpp_min4,
	                    smi_mv_sales_mtd_jkt.fm_wd_min1,
	                    smi_mv_sales_mtd_jkt.fm_wd_min2,
	                    smi_mv_sales_mtd_jkt.fm_wd_min3,
	                    smi_mv_sales_mtd_jkt.fm_wd_min4,
	                    smi_mv_sales_mtd_jkt.fm_qty_min1,
	                    smi_mv_sales_mtd_jkt.fm_qty_min2,
	                    smi_mv_sales_mtd_jkt.fm_qty_min3,
	                    smi_mv_sales_mtd_jkt.fm_qty_min4,
	                    smi_mv_sales_mtd_jkt.fm_spdhpp_min1,
	                    smi_mv_sales_mtd_jkt.fm_spdhpp_min2,
	                    smi_mv_sales_mtd_jkt.fm_spdhpp_min3,
	                    smi_mv_sales_mtd_jkt.fm_spdhpp_min4,
	                    smi_mv_sales_mtd_jkt.fm_spd_min1,
	                    smi_mv_sales_mtd_jkt.fm_spd_min2,
	                    smi_mv_sales_mtd_jkt.fm_spd_min3,
	                    smi_mv_sales_mtd_jkt.fm_spd_min4,
	                    smi_mv_sales_mtd_jkt.fm_qty_min1ly,
	                    smi_mv_sales_mtd_jkt.fm_qty_min2ly,
	                    smi_mv_sales_mtd_jkt.fm_qty_min3ly,
	                    smi_mv_sales_mtd_jkt.fm_qty_min4ly,
	                    smi_mv_sales_mtd_jkt.fm_spdhpp_min1ly,
	                    smi_mv_sales_mtd_jkt.fm_spdhpp_min2ly,
	                    smi_mv_sales_mtd_jkt.fm_spdhpp_min3ly,
	                    smi_mv_sales_mtd_jkt.fm_spdhpp_min4ly,
	                    smi_mv_sales_mtd_jkt.fm_spd_min1ly,
	                    smi_mv_sales_mtd_jkt.fm_spd_min2ly,
	                    smi_mv_sales_mtd_jkt.fm_spd_min3ly,
	                    smi_mv_sales_mtd_jkt.fm_spd_min4ly,
	                    smi_mv_sales_mtd_jkt.fm_totalstokhpp_min1,
	                    smi_mv_sales_mtd_jkt.fm_totalstokhpp_min2,
	                    smi_mv_sales_mtd_jkt.fm_totalstokhpp_min3,
	                    smi_mv_sales_mtd_jkt.fm_totalstokhpp_min4,
	                    smi_mv_sales_mtd_jkt.li_wd_min1,
	                    smi_mv_sales_mtd_jkt.li_wd_min2,
	                    smi_mv_sales_mtd_jkt.li_wd_min3,
	                    smi_mv_sales_mtd_jkt.li_wd_min4,
	                    smi_mv_sales_mtd_jkt.li_spdhpp_min1,
	                    smi_mv_sales_mtd_jkt.li_spdhpp_min2,
	                    smi_mv_sales_mtd_jkt.li_spdhpp_min3,
	                    smi_mv_sales_mtd_jkt.li_spdhpp_min4,
	                    smi_mv_sales_mtd_jkt.li_spd_min1,
	                    smi_mv_sales_mtd_jkt.li_spd_min2,
	                    smi_mv_sales_mtd_jkt.li_spd_min3,
	                    smi_mv_sales_mtd_jkt.li_spd_min4,
	                    smi_mv_sales_mtd_jkt.li_spdhpp_min1ly,
	                    smi_mv_sales_mtd_jkt.li_spdhpp_min2ly,
	                    smi_mv_sales_mtd_jkt.li_spdhpp_min3ly,
	                    smi_mv_sales_mtd_jkt.li_spdhpp_min4ly,
	                    smi_mv_sales_mtd_jkt.li_spd_min1ly,
	                    smi_mv_sales_mtd_jkt.li_spd_min2ly,
	                    smi_mv_sales_mtd_jkt.li_spd_min3ly,
	                    smi_mv_sales_mtd_jkt.li_spd_min4ly,
	                    smi_mv_sales_mtd_jkt.li_totalstokhpp_min1,
	                    smi_mv_sales_mtd_jkt.li_totalstokhpp_min2,
	                    smi_mv_sales_mtd_jkt.li_totalstokhpp_min3,
	                    smi_mv_sales_mtd_jkt.li_totalstokhpp_min4
                  	FROM mb_rms10_mbdc.smi_mv_sales_mtd_jkt
               	UNION
                 	SELECT 
	                 	smi_mv_sales_mtd_sby.namacabang,
	                    smi_mv_sales_mtd_sby.kodetoko,
	                    smi_mv_sales_mtd_sby.namatoko,
	                    smi_mv_sales_mtd_sby.wd_min1,
	                    smi_mv_sales_mtd_sby.wd_min2,
	                    smi_mv_sales_mtd_sby.wd_min3,
	                    smi_mv_sales_mtd_sby.wd_min4,
	                    smi_mv_sales_mtd_sby.spdhpp_min1,
	                    smi_mv_sales_mtd_sby.spdhpp_min2,
	                    smi_mv_sales_mtd_sby.spdhpp_min3,
	                    smi_mv_sales_mtd_sby.spdhpp_min4,
	                    smi_mv_sales_mtd_sby.spd_min1,
	                    smi_mv_sales_mtd_sby.spd_min2,
	                    smi_mv_sales_mtd_sby.spd_min3,
	                    smi_mv_sales_mtd_sby.spd_min4,
	                    smi_mv_sales_mtd_sby.spdhpp_min1ly,
	                    smi_mv_sales_mtd_sby.spdhpp_min2ly,
	                    smi_mv_sales_mtd_sby.spdhpp_min3ly,
	                    smi_mv_sales_mtd_sby.spdhpp_min4ly,
	                    smi_mv_sales_mtd_sby.spd_min1ly,
	                    smi_mv_sales_mtd_sby.spd_min2ly,
	                    smi_mv_sales_mtd_sby.spd_min3ly,
	                    smi_mv_sales_mtd_sby.spd_min4ly,
	                    smi_mv_sales_mtd_sby.totalstokhpp_min1,
	                    smi_mv_sales_mtd_sby.totalstokhpp_min2,
	                    smi_mv_sales_mtd_sby.totalstokhpp_min3,
	                    smi_mv_sales_mtd_sby.totalstokhpp_min4,
	                    smi_mv_sales_mtd_sby.fm_wd_min1,
	                    smi_mv_sales_mtd_sby.fm_wd_min2,
	                    smi_mv_sales_mtd_sby.fm_wd_min3,
	                    smi_mv_sales_mtd_sby.fm_wd_min4,
	                    smi_mv_sales_mtd_sby.fm_qty_min1,
	                    smi_mv_sales_mtd_sby.fm_qty_min2,
	                    smi_mv_sales_mtd_sby.fm_qty_min3,
	                    smi_mv_sales_mtd_sby.fm_qty_min4,
	                    smi_mv_sales_mtd_sby.fm_spdhpp_min1,
	                    smi_mv_sales_mtd_sby.fm_spdhpp_min2,
	                    smi_mv_sales_mtd_sby.fm_spdhpp_min3,
	                    smi_mv_sales_mtd_sby.fm_spdhpp_min4,
	                    smi_mv_sales_mtd_sby.fm_spd_min1,
	                    smi_mv_sales_mtd_sby.fm_spd_min2,
	                    smi_mv_sales_mtd_sby.fm_spd_min3,
	                    smi_mv_sales_mtd_sby.fm_spd_min4,
	                    smi_mv_sales_mtd_sby.fm_qty_min1ly,
	                    smi_mv_sales_mtd_sby.fm_qty_min2ly,
	                    smi_mv_sales_mtd_sby.fm_qty_min3ly,
	                    smi_mv_sales_mtd_sby.fm_qty_min4ly,
	                    smi_mv_sales_mtd_sby.fm_spdhpp_min1ly,
	                    smi_mv_sales_mtd_sby.fm_spdhpp_min2ly,
	                    smi_mv_sales_mtd_sby.fm_spdhpp_min3ly,
	                    smi_mv_sales_mtd_sby.fm_spdhpp_min4ly,
	                    smi_mv_sales_mtd_sby.fm_spd_min1ly,
	                    smi_mv_sales_mtd_sby.fm_spd_min2ly,
	                    smi_mv_sales_mtd_sby.fm_spd_min3ly,
	                    smi_mv_sales_mtd_sby.fm_spd_min4ly,
	                    smi_mv_sales_mtd_sby.fm_totalstokhpp_min1,
	                    smi_mv_sales_mtd_sby.fm_totalstokhpp_min2,
	                    smi_mv_sales_mtd_sby.fm_totalstokhpp_min3,
	                    smi_mv_sales_mtd_sby.fm_totalstokhpp_min4,
	                    smi_mv_sales_mtd_sby.li_wd_min1,
	                    smi_mv_sales_mtd_sby.li_wd_min2,
	                    smi_mv_sales_mtd_sby.li_wd_min3,
	                    smi_mv_sales_mtd_sby.li_wd_min4,
	                    smi_mv_sales_mtd_sby.li_spdhpp_min1,
	                    smi_mv_sales_mtd_sby.li_spdhpp_min2,
	                    smi_mv_sales_mtd_sby.li_spdhpp_min3,
	                    smi_mv_sales_mtd_sby.li_spdhpp_min4,
	                    smi_mv_sales_mtd_sby.li_spd_min1,
	                    smi_mv_sales_mtd_sby.li_spd_min2,
	                    smi_mv_sales_mtd_sby.li_spd_min3,
	                    smi_mv_sales_mtd_sby.li_spd_min4,
	                    smi_mv_sales_mtd_sby.li_spdhpp_min1ly,
	                    smi_mv_sales_mtd_sby.li_spdhpp_min2ly,
	                    smi_mv_sales_mtd_sby.li_spdhpp_min3ly,
	                    smi_mv_sales_mtd_sby.li_spdhpp_min4ly,
	                    smi_mv_sales_mtd_sby.li_spd_min1ly,
	                    smi_mv_sales_mtd_sby.li_spd_min2ly,
	                    smi_mv_sales_mtd_sby.li_spd_min3ly,
	                    smi_mv_sales_mtd_sby.li_spd_min4ly,
	                    smi_mv_sales_mtd_sby.li_totalstokhpp_min1,
	                    smi_mv_sales_mtd_sby.li_totalstokhpp_min2,
	                    smi_mv_sales_mtd_sby.li_totalstokhpp_min3,
	                    smi_mv_sales_mtd_sby.li_totalstokhpp_min4
                  	FROM mb_rms20_mbdc.smi_mv_sales_mtd_sby) sm
          ORDER BY sm.namacabang, sm.namatoko) salesmonitor
WITH DATA;

-- View indexes:
CREATE UNIQUE INDEX mv_indicatorsosidak_salesmonitor_rpt_kodetoko_unique_idx ON mb_rms01_mbho.mv_indicatorsosidak_salesmonitor_rpt USING btree (kodetoko);

 ----------------------------------------------------------------------------------------
 ----------------------------------------------------------------------------------------
-- 17.iso_smimstplatblacklist
refresh materialized view concurrently smi_rms01_pbho.mv_smimstplatblacklist;

refresh materialized view concurrently select * from mb_rms01_mbho.mv_smimstplatblacklist;--OK
 ----------------------------------------------------------------------------------------
 ----------------------------------------------------------------------------------------
-- 18.iso_TblTglBisnis
refresh materialized view concurrently smi_rms10_pbdc.mv_TblTglBisnis;

refresh materialized view concurrently mb_rms10_mbdc.mv_TblTglBisnis;--OK
 ----------------------------------------------------------------------------------------
 ----------------------------------------------------------------------------------------
-- 19.iso_fn_getblacklistnopol_jkt
select * from public.fn_getblacklistnopol('rms10');

select * from public.fn_getblacklistnopolmb('rms10');--OK
select * from public.fn_getblacklistnopolmb('rms20');--OK
 ----------------------------------------------------------------------------------------
 ----------------------------------------------------------------------------------------
-- DROP FUNCTION public.fn_getblacklistnopolmb(varchar);

CREATE OR REPLACE FUNCTION public.fn_getblacklistnopolmb(svrname character varying)
 RETURNS boolean
 LANGUAGE plpgsql
AS $function$
begin
  execute format('
  create temp table nopolperjenismember_mb_temp_%1$s as
	select a.kodetoko, a.nopolisi
	from mb_%1$s_transaksi_toko_perjenis_member_v3 a
	where a.tanggal between (date_trunc(''month'', current_date + interval ''-1 month''))::date and (date_trunc(''month'', current_date + interval ''-1 month'') + interval ''6 day'')::date
	and a.idjenisproduk <>4
	and a.statusproduk<>''K''
	group by a.kodetoko, a.nopolisi;
  
	delete from public.nopolblacklistqty where svrName = ''%1$s'';
	insert into public.nopolblacklistqty
	select ''%1$s'' svrName, a.kodetoko, count(a.nopolisi) nopolblakclist_qty
	from nopolperjenismember_mb_temp_%1$s a
	join mb_rms01_mbho.mv_smimstplatblacklist b on b.nopolisi = a.nopolisi
	group by a.kodetoko;
  
  	drop table nopolperjenismember_mb_temp_%1$s;
  ', svrName);
  return true;
end;
$function$
;
 ----------------------------------------------------------------------------------------
 ----------------------------------------------------------------------------------------
-- 26.iso_indicator_sosidak_jkt
 refresh materialized view concurrently smi_rms10_rpt.mv_smi_indicator_sosidak_jkt;
 refresh materialized view concurrently smi_rms10_rpt.mv_smi_indicator_sosidak_w1_jkt;
 refresh materialized view concurrently smi_rms10_rpt.mv_smi_indicator_sosidak_w2_jkt;
 refresh materialized view concurrently smi_rms10_rpt.mv_smi_indicator_sosidak_w3_jkt;
 refresh materialized view concurrently smi_rms10_rpt.mv_smi_indicator_sosidak_w4_jkt;
 refresh materialized view concurrently smi_rms10_rpt.smi_mv_indicator_jkt;
 refresh materialized view concurrently smi_rms10_rpt.mv_indicatorsosidak_overview_jkt_rpt;
 refresh materialized view concurrently smi_rms10_pbdc.mv_SMITblLogImen;
 refresh materialized view concurrently smi_rms10_pbdc.mv_transaksitokoheader_3months_temp;

 refresh materialized view concurrently mb_rms10_rpt.mv_smi_indicator_sosidak_jkt;
 refresh materialized view concurrently mb_rms10_rpt.mv_smi_indicator_sosidak_w1_jkt;
 refresh materialized view concurrently mb_rms10_rpt.mv_smi_indicator_sosidak_w2_jkt;
 refresh materialized view concurrently mb_rms10_rpt.mv_smi_indicator_sosidak_w3_jkt;
 refresh materialized view concurrently mb_rms10_rpt.mv_smi_indicator_sosidak_w4_jkt;
 refresh materialized view concurrently mb_rms10_rpt.smi_mv_indicator_jkt;
 refresh materialized view concurrently mb_rms10_rpt.mv_indicatorsosidak_overview_jkt_rpt;
 refresh materialized view concurrently mb_rms10_mbdc.mv_SMITblLogImen;
 refresh materialized view concurrently mb_rms10_mbdc.mv_transaksitokoheader_3months_temp;
 ----------------------------------------------------------------------------------------
 ----------------------------------------------------------------------------------------
-- DROP FUNCTION smi_rms10_rpt.fn_smi_indicator_sosidak_jkt(date, date);

CREATE OR REPLACE FUNCTION smi_rms10_rpt.fn_smi_indicator_sosidak_jkt(start_date date, end_date date)
 RETURNS TABLE(snamacabang character varying, srh character varying, sac character varying, ikodetoko integer, snamatoko character varying, iintoleran_qty integer, ibalance_qty integer, iitemselisihqty integer, dshrinkagecategory double precision, dplus_qty double precision, dminus_qty double precision, dac_itemselisihqty double precision, davg_latencyqty double precision, deod_latencyqty double precision, inopolblakclist_qty integer, inopolrepeat integer, irrak_qty integer, idelimen_qty integer, dtotal_point double precision)
 LANGUAGE plpgsql
AS $function$
begin
	return query
		select namacabang, rh, ac, kodetoko, namatoko,
			intoleran_qty::int, balance_qty::int, itemselisihqty::int, shrinkagecategory::double precision, plus_qty::double precision, 
            minus_qty::double precision, ac_itemselisihqty::double precision, avg_latencyqty::double precision, 
            eod_latencyqty::double precision, nopolblakclist_qty::int, nopolrepeat::int, rrak_qty::int, delimen_qty::int,
            round( (intoleran_qty * 50) + (balance_qty * 50) + (itemselisihqty * 20) + (shrinkagecategory * 20) + (plus_qty * 0) + 
            (minus_qty * 0) + (ac_itemselisihqty * 0) + (avg_latencyqty * 35) + (eod_latencyqty * 8) + (nopolblakclist_qty * 6) +
            (nopolrepeat * 15) + (rrak_qty * 5) + (delimen_qty * 5) )::double precision total_point
            from(
                select mp.namacabang, mp.rh, mp.ac, mp.kodetoko, mp.namatoko, coalesce(shr.intoleran_qty, 0) intoleran_qty, coalesce(sos.balance_qty, 0) balance_qty, coalesce(sp.itemselisihqty, 0) itemselisihqty, 
                    coalesce(shr.point, 0) shrinkagecategory, coalesce(sos.plus_qty, 0) plus_qty, coalesce(sos.minus_qty, 0) minus_qty, coalesce(spac.itemselisihqty, 0) ac_itemselisihqty,
                    coalesce(ltc.avg_latencyqty, 0) avg_latencyqty, coalesce(eodltc.eod_latencyqty, 0) eod_latencyqty, coalesce(blnp.nopolblakclist_qty, 0) nopolblakclist_qty, coalesce(rpnp.nopolrepeat, 0) nopolrepeat, 
                    coalesce(rrak.qty, 0) rrak_qty, coalesce(deli.delimen_qty, 0) delimen_qty
                from
                (
--	        		select b.namacabang, a.rh, a.ac, a.singkatantoko, a.kodetoko, b.namatoko
--	                from smi_target_value_sales_perbulan_V2 a
--	                join smi_msttokoho_rmsv3 as b on b.kodetoko=a.kodetoko
--	                where a.tahuntarget=date_part('year', current_date + interval '-1 month')
--	                and a.bulantarget=date_part('month', current_date + interval '-1 month')
--	                and b.idcabang = 5 -- dps
--	                group by b.namacabang, a.rh, a.ac, a.singkatantoko, a.kodetoko, b.namatoko
                	-----#update target #Dec 2024
                    select b.namacabang, a.rh, a.ac, b.singkatantoko, a.kodetoko, b.namatoko
	                    from public.mb_alokasi_ac_rh a
                  	join mb_rms01_mbho.v_mb_msttokoho_rmsv3 as b on b.kodetoko=a.kodetoko
						where b.idcabang = 2 -- jkt
--	                    where a.tahun=date_part('year', current_date + interval '-1 month')
--	                    and a.bulan=date_part('month', current_date + interval '-1 month')
--	                    and b.idcabang = 2 -- jkt
--                    group by b.namacabang, a.rh, a.ac, b.singkatantoko, a.kodetoko, b.namatoko
                )mp
                left join (
                -- Selisih SO(balance, minus and plus)
                        select 
                                kodetoko, 
                                sum(case when qtyselisih = 0 then qty else 0 end) balance_qty,
                                round(sum(case when qtyselisih > 0 then qty else 0 end) / 3) plus_qty,
                                round(sum(case when qtyselisih < 0 then qty else 0 end) / 3) minus_qty
                        from(
                                select 
                                        kodetoko, 
                                        qtyselisih,
                                        count(distinct(nomorsoadjusment)) qty -- baseon nomor adjustment
                                from(
                                        select so.kodetoko, so.nomorsoadjusment, sum(so.qtyselisih) qtyselisih 
                                        FROM mb_rms10_mbdc.mb_so_toko_cab_rpt_hist_rms10 so
                                        where so.tglsoadjusment between start_date and end_date
--		                        and so.kodetoko in(3021096, 3021001)
                                        group by so.kodetoko, 
                                        so.nomorsoadjusment
                                )sob
                                group by kodetoko, qtyselisih 
                        )so
                        group by kodetoko
                )sos on sos.kodetoko = mp.kodetoko
                left join (
                -- selisih item qty
                        select 
                                kodetoko, 
                                round(count(distinct(nomorsoadjusment, kodeproduk)) / 3) itemselisihqty -- baseon item
                        from(
                                select so.kodetoko, so.nomorsoadjusment, so.kodeproduk, sum(so.qtyselisih) qtyselisih 
                                FROM mb_rms10_mbdc.mb_so_toko_cab_rpt_hist_rms10 so
                                where so.tglsoadjusment between start_date and end_date
--				 and so.kodetoko in(3021096, 3021001)
                                group by so.kodetoko, 
                                so.nomorsoadjusment,so.kodeproduk
                        )sob where sob.qtyselisih != 0
                        group by kodetoko
                )sp on sp.kodetoko = mp.kodetoko
                left join (
                -- selisih item qty by AC
                        select 
                                kodetoko, 
                                round(count(distinct(nomorsoadjusment, kodeproduk)) / 3) itemselisihqty -- baseon item
                        from(
                                select so.kodetoko, 
                                        so.nomorsoadjusment, 
                                        so.kodeproduk,
                                        sum(so.qtyselisih) qtyselisih 
                                FROM mb_rms10_mbdc.mb_so_toko_cab_rpt_hist_rms10 so
                                where so.tglsoadjusment between start_date and end_date 
--				 and so.kodetoko in(3021096, 3021001)
                                and so.idUser not like '%KSR%' 
                                and so.idUser not like '%KTO%' 
                                and so.idUser not like '%IC%' 
                                and so.iduser not in ('rozano','ROZANO','KTIO','KTP')
                                and so.alasan IN ('Selisih Stock|Selisih Struk Penjualan', 'Selisih Stock|Selisih Stock Opname') -----#update penambahan kondisi #Dec 2024
                                group by so.kodetoko, 
                                so.nomorsoadjusment,so.kodeproduk
                        )sob where sob.qtyselisih != 0
                        group by kodetoko
                )spac on spac.kodetoko = mp.kodetoko
                -- Man power
--                 left join smi_rms10_rpt.fn_smi_shrinkage_bycategory_jkt(start_date, end_date) shr on shr.kdtoko = mp.kodetoko
                left join(
                    select kodetoko::int, spd_avg::numeric,
                            intoleran_qty, categoryA, categoryB, categoryC, categoryD, categoryE,
                            (categoryA * 2) + (categoryB * 3) + (categoryC * 5) + (categoryD * 7) + (categoryE * 9) point
                    from(
                            select kodetoko, 
                                    spd_avg, 
                                    (case when abs(selisihamount_min1) > toleransiamount_min1 then 1 else 0 end) + 
                                    (case when abs(selisihamount_min2) > toleransiamount_min2 then 1 else 0 end) +
                                    (case when abs(selisihamount_min3) > toleransiamount_min3 then 1 else 0 end) intoleran_qty,
                                    (case when abs(selisihamount_min1) > toleransiamount_min1 and abs(selisihamount_min1) <= 200000 then 1 else 0 end) + 
                                    (case when abs(selisihamount_min2) > toleransiamount_min2 and abs(selisihamount_min2) <= 200000 then 1 else 0 end) +
                                    (case when abs(selisihamount_min3) > toleransiamount_min3 and abs(selisihamount_min3) <= 200000 then 1 else 0 end) categoryA,
                                    (case when abs(selisihamount_min1) > 200000 and abs(selisihamount_min1) <= 500000 then 1 else 0 end) + 
                                    (case when abs(selisihamount_min2) > 200000 and abs(selisihamount_min2) <= 500000 then 1 else 0 end) +
                                    (case when abs(selisihamount_min3) > 200000 and abs(selisihamount_min3) <= 500000 then 1 else 0 end) categoryB,
                                    (case when abs(selisihamount_min1) > 500000 and abs(selisihamount_min1) <= 750000 then 1 else 0 end) +
                                    (case when abs(selisihamount_min2) > 500000 and abs(selisihamount_min2) <= 750000 then 1 else 0 end) +
                                    (case when abs(selisihamount_min3) > 500000 and abs(selisihamount_min3) <= 750000 then 1 else 0 end) categoryC,
                                    (case when abs(selisihamount_min1) > 750000 and abs(selisihamount_min1) <= 1000000 then 1 else 0 end) +
                                    (case when abs(selisihamount_min2) > 750000 and abs(selisihamount_min2) <= 1000000 then 1 else 0 end) +
                                    (case when abs(selisihamount_min3) > 750000 and abs(selisihamount_min3) <= 1000000 then 1 else 0 end) categoryD,
                                    (case when abs(selisihamount_min1) > 1000000 then 1 else 0 end) +
                                    (case when abs(selisihamount_min2) > 1000000 then 1 else 0 end) +
                                    (case when abs(selisihamount_min3) > 1000000 then 1 else 0 end) categoryE	
                            from(
                                    select s.kodetoko, 
                                            s.spd_min1, s.spd_min2, s.spd_min3, 
                                            round((s.spd_min1 + s.spd_min2 + s.spd_min3)/3) spd_avg, 
                                            s.toleransi_min1, s.toleransi_min2, s.toleransi_min3,
                                            sh.selisihamount_min1, sh.selisihamount_min2, sh.selisihamount_min3,
                                            round(s.spd_min1 * s.toleransi_min1 / 100) toleransiamount_min1,
                                            round(s.spd_min2 * s.toleransi_min2 / 100) toleransiamount_min2,
                                            round(s.spd_min3 * s.toleransi_min3 / 100) toleransiamount_min3
                                    from(
                                            select namacabang, kodetoko,
                                                    sum(case when sx.monthly = -1 then sx.toleransi else 0 end) toleransi_min1,
                                                    sum(case when sx.monthly = -2 then sx.toleransi else 0 end) toleransi_min2,
                                                    sum(case when sx.monthly = -3 then sx.toleransi else 0 end) toleransi_min3,
                                                    sum(case when sx.monthly = -1 then sx.spd else 0 end) spd_min1,
                                                sum(case when sx.monthly = -2 then sx.spd else 0 end) spd_min2,
                                                sum(case when sx.monthly = -3 then sx.spd else 0 end) spd_min3
                                        from(
                                                    select sy.iYear, sy.iMonth, monthly, namacabang, kodetoko, spd_hpp, spd, case when shr.toleransi is null then 0.020 else shr.toleransi end toleransi
                                                    from
                                                    (select date_part('year', sls.tanggal) iYear, date_part('month', sls.tanggal) iMonth, namacabang, sls.kodetoko, 
                                                            extract(year from age(date_trunc('month', sls.tanggal) , date_trunc('month', current_date))) * 12 + extract(month from age(date_trunc('month', sls.tanggal) , date_trunc('month', current_date))) monthly,
                                                       sum(sls.qty * sls.hpp) spd_hpp,
                                                      sum(sls.subtotal) spd
                                                    from PUBLIC.mb_rms10_transaksi_toko_perjenis_member_v3 sls 
                                                    where (
                                                            sls.tanggal between start_date and end_date
                                                        )
                                                    group by date_part('year', sls.tanggal), date_part('month', sls.tanggal), namacabang, sls.kodetoko, 
                                                            extract(year from age(date_trunc('month', sls.tanggal) , date_trunc('month', current_date))) * 12 + extract(month from age(date_trunc('month', sls.tanggal) , date_trunc('month', current_date)))
                                                    )sy
                                                    left join public.smi_shrinkage_toleransi_v shr on shr.iYear = sy.iYear and shr.iMonth = sy.iMonth
                                            )sx	group by namacabang, kodetoko
                                    )s 
                                    left join(
                                            select kodetoko,
                                                    sum(case when shr.monthly = -1 then shr.selisihamount else 0 end) selisihamount_min1,
                                                    sum(case when shr.monthly = -2 then shr.selisihamount else 0 end) selisihamount_min2,
                                                    sum(case when shr.monthly = -3 then shr.selisihamount else 0 end) selisihamount_min3
                                            from(
                                                    select 
                                                            so.kodetoko,
                                                            extract(year from age(date_trunc('month', so.tglsoadjusment) , date_trunc('month', current_date))) * 12 + extract(month from age(date_trunc('month', so.tglsoadjusment) , date_trunc('month', current_date))) monthly,
                                                            sum(so.totalhrgjual) selisihamount
                                                    FROM mb_rms10_mbdc.mb_so_toko_cab_rpt_hist_rms10 so
                                                    where so.tglsoadjusment between start_date and end_date 
                    --				and so.kodetoko in(3021001, 3021096)
                                                    and so.qtyselisih != 0
                                                    and so.alasan = 'Selisih Stock|Selisih Struk Penjualan' -- tambahan filter tgl 20230824
                    --				and so.idUser not like '%KSR%' 
                    --				and so.idUser not like '%KTO%' 
                    --				and so.idUser not like '%IC%' 
                    --				and so.iduser not in ('rozano','ROZANO','KTIO','KTP')
                                                    group by so.kodetoko, extract(year from age(date_trunc('month', so.tglsoadjusment) , date_trunc('month', current_date))) * 12 + extract(month from age(date_trunc('month', so.tglsoadjusment) , date_trunc('month', current_date)))
                                            )shr group by kodetoko
                                    )sh on sh.kodetoko = s.kodetoko
                            )shrbc
                    )shrbycategory
                
                ) shr on shr.kodetoko = mp.kodetoko
                left join (
                -- latency setor
                        select kodetoko, round(sum(latency) / 3) avg_latencyqty
                        from(
                                select  
                                        substring((response_body::jsonb ->> 'order')::jsonb ->> 'invoice_number', 4, 7) kodetoko,
                                        case when (response_body::jsonb ->> 'virtual_account_info')::jsonb ->> 'how_to_pay_api' like '%doku%' and a.pay_in_date::date - a.sales_date::date = 1 then 1 else 0 end latency
                                FROM smi_12up.smi_jokul_service_payment_codes a
                                where a.sales_date between start_date and end_date
                                and a.va_payment_status ='SUCCESS'
                        )lt 
                        where kodetoko is not null
                        group by kodetoko
                )ltc on ltc.kodetoko::int = mp.kodetoko
                left join (
                    select a.kodetoko,count(a.tglbisnis) eod_latencyqty 
                    from mb_rms10_mbdc.mv_TblTglBisnis a
                    where a.tglbisnis between start_date and end_date
                    and a.tglbisnis::date <> a.tgleod::date -- and a.kodetoko in(3021012, 3021017)
                    group by a.kodetoko
                )eodltc on eodltc.kodetoko = mp.kodetoko 
                left join (
                -- black list nopol
                     select a.kodetoko, count(distinct (a.nopolisi)) nopolblakclist_qty
                     from mb_rms10_transaksi_toko_perjenis_member_v3 a
                     join mb_rms01_mbho.mv_smimstplatblacklist b on b.nopolisi = a.nopolisi
                     where --a.tanggal between start_date and end_date
                     a.idjenisproduk <>4
                     and a.statusproduk<>'K'
                     --and a.kodetoko in ('3021001','3021004','3021135')
                     group by a.kodetoko
                )blnp on blnp.kodetoko = mp.kodetoko
                left join(
                -- repeat nopol
                    select kodetoko, sum(qty) nopolrepeat
                    from (
                            select tanggal, kodetoko, nopolisi, count(*) qty
                            from(
	                            SELECT a.tanggal, a.kodetoko, a.nopolisi, a.nomortransaksi
	                            FROM mb_rms10_rpt.mb_rms10_transaksi_toko_perjenis_member_v3 a
	                            where a.tanggal between start_date and end_date
	                            and a.idjenisproduk <>4
	                            and a.statusproduk<>'K'
	                            --and a.kodetoko  = 3021001
	                            --and a.kodetoko in ('3021001','3021004','3021135')
	                            group by a.tanggal, a.kodetoko, a.nopolisi, a.nomortransaksi
                            )x group by tanggal, kodetoko, nopolisi having count(*) > 5 -----#update dari > 3 menjadi > 5 #Dec 2024
                    )rp group by kodetoko
                )rpnp on rpnp.kodetoko = mp.kodetoko
                left join(
                -- RRAK
                        select a.kodetoko,
                        count(distinct(a.tglapproverealisasi)) qty
                        from mb_rms10_mbdc.mv_tblrrakheader a
                        where a.tglcreate between start_date and end_date
--						where a.tglcreate between '2025-05-01' and '2025-05-31'
                        and  DATE_PART('day', a.tglapproverealisasi) >10
                        -- and a.kodetoko in ('3021001','3021004','3021135')
                        group by a.kodetoko
                )rrak on rrak.kodetoko = mp.kodetoko
                left join(
                -- Hapus imen
                    select a.kodetoko,count(DISTINCT concat(a.kodetoko, a.nomortransaksi, a.nomorimen))delimen_qty
                    from mb_rms10_mbdc.mv_smitbllogimen a
                        join mb_rms10_mbdc.mv_transaksitokoheader_3months_temp b 
                        on b.kodetoko=a.kodetoko and b.nomortransaksi=a.nomortransaksi
                    where b.tglbisnis between start_date and end_date
--                    where b.tglbisnis between '2025-05-01' and '2025-05-31'
--                    and a.kodetoko in ('3021001','3021004','3021135')
                    group by a.kodetoko
                )deli on deli.kodetoko = mp.kodetoko
            )ind;
	end;
$function$
;
 ----------------------------------------------------------------------------------------
CREATE MATERIALIZED VIEW mb_rms10_rpt.mv_smi_indicator_sosidak_jkt
TABLESPACE pg_default
AS SELECT mp.snamacabang,
    mp.srh,
    mp.sac,
    mp.ikodetoko,
    mp.snamatoko,
    mp.iintoleran_qty,
    mp.ibalance_qty,
    mp.iitemselisihqty,
    mp.dshrinkagecategory,
    mp.dplus_qty,
    mp.dminus_qty,
    mp.dac_itemselisihqty,
    mp.davg_latencyqty,
    mp.deod_latencyqty,
    mp.inopolblakclist_qty,
    mp.inopolrepeat,
    mp.irrak_qty,
    mp.idelimen_qty,
    mp.dtotal_point
   FROM mb_rms10_rpt.fn_smi_indicator_sosidak_jkt(date_trunc('month'::text, CURRENT_DATE + '-3 mons'::interval)::date, (date_trunc('month'::text, CURRENT_DATE + '-1 mons'::interval) + '1 mon -1 days'::interval)::date) mp(snamacabang, srh, sac, ikodetoko, snamatoko, iintoleran_qty, ibalance_qty, iitemselisihqty, dshrinkagecategory, dplus_qty, dminus_qty, dac_itemselisihqty, davg_latencyqty, deod_latencyqty, inopolblakclist_qty, inopolrepeat, irrak_qty, idelimen_qty, dtotal_point)
WITH DATA;

-- View indexes:
CREATE UNIQUE INDEX mv_smi_indicator_sosidak_jkt_ikodetoko_unique_idx ON mb_rms10_rpt.mv_smi_indicator_sosidak_jkt USING btree (ikodetoko);
 ----------------------------------------------------------------------------------------
 ----------------------------------------------------------------------------------------
 ----------------------------------------------------------------------------------------
 ----------------------------------------------------------------------------------------
 ----------------------------------------------------------------------------------------
 ----------------------------------------------------------------------------------------
 ---------------------------------------------------------------------------------------- ----------------------------------------------------------------------------------------
 ----------------------------------------------------------------------------------------