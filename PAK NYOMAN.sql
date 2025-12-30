SELECT x2.idcabang,x2.namacabang,x2.nopolisi,x2.mtd_h_1, x2.m_1, x2.m_2, x2.m_3, x2.m_4, x2.m_5, x2.m_6, x2.m_7, x2.m_8, x2.m_9, x2.m_10, x2.sum
FROM(
	SELECT x1.idcabang,x1.namacabang,x1.nopolisi,x1.mtd_h_1, x1.m_1, x1.m_2, x1.m_3, x1.m_4, x1.m_5, x1.m_6, x1.m_7, x1.m_8, x1.m_9, x1.m_10,
	SUM(x1.mtd_h_1 + x1.m_1 + x1.m_2 + x1.m_3 + x1.m_4 + x1.m_5 + x1.m_6 + x1.m_7 + x1.m_8 + x1.m_9 + x1.m_10) AS sum
		FROM( 
			SELECT x0.idcabang, x0.namacabang, x0.nopolisi,
			sum(x0.mtd_h_1) AS mtd_h_1,
			sum(x0.m_1) AS m_1,
			sum(x0.m_2) AS m_2,
			sum(x0.m_3) AS m_3,
			sum(x0.m_4) AS m_4,
			sum(x0.m_5) AS m_5,
			sum(x0.m_6) AS m_6,
			sum(x0.m_7) AS m_7,
			sum(x0.m_8) AS m_8,
			sum(x0.m_9) AS m_9,
			SUM(x0.m_10) AS m_10
				FROM( 
					SELECT x.tanggal, x.idcabang, x.namacabang,x.nopolisi,
						CASE
							WHEN date_trunc('month'::text, x.tanggal::timestamp with time zone) = date_trunc('month'::text, now()::date - '1 mon'::interval month) THEN count(DISTINCT concat(x.kodetoko, x.nomortransaksi))::numeric
							ELSE 0::numeric
						END AS mtd_h_1,
						CASE
							WHEN date_trunc('month'::text, x.tanggal::timestamp with time zone) = date_trunc('month'::text, now()::date - '2 mons'::interval month) THEN count(DISTINCT concat(x.kodetoko, x.nomortransaksi))::numeric
							ELSE 0::numeric
						END AS m_1,
						CASE
							WHEN date_trunc('month'::text, x.tanggal::timestamp with time zone) = date_trunc('month'::text, now()::date - '3 mons'::interval month) THEN count(DISTINCT concat(x.kodetoko, x.nomortransaksi))::numeric
							ELSE 0::numeric
						END AS m_2,
						CASE
							WHEN date_trunc('month'::text, x.tanggal::timestamp with time zone) = date_trunc('month'::text, now()::date - '4 mons'::interval month) THEN count(DISTINCT concat(x.kodetoko, x.nomortransaksi))::numeric
							ELSE 0::numeric
						END AS m_3,
						CASE
							WHEN date_trunc('month'::text, x.tanggal::timestamp with time zone) = date_trunc('month'::text, now()::date - '5 mons'::interval month) THEN count(DISTINCT concat(x.kodetoko, x.nomortransaksi))::numeric
							ELSE 0::numeric
						END AS m_4,
						CASE
							WHEN date_trunc('month'::text, x.tanggal::timestamp with time zone) = date_trunc('month'::text, now()::date - '6 mons'::interval month) THEN count(DISTINCT concat(x.kodetoko, x.nomortransaksi))::numeric
							ELSE 0::numeric
						END AS m_5,
						CASE
							WHEN date_trunc('month'::text, x.tanggal::timestamp with time zone) = date_trunc('month'::text, now()::date - '7 mons'::interval month) THEN count(DISTINCT concat(x.kodetoko, x.nomortransaksi))::numeric
							ELSE 0::numeric
						END AS m_6,
						CASE
							WHEN date_trunc('month'::text, x.tanggal::timestamp with time zone) = date_trunc('month'::text, now()::date - '8 mons'::interval month) THEN count(DISTINCT concat(x.kodetoko, x.nomortransaksi))::numeric
							ELSE 0::numeric
						END AS m_7,
						CASE
							WHEN date_trunc('month'::text, x.tanggal::timestamp with time zone) = date_trunc('month'::text, now()::date - '9 mons'::interval month) THEN count(DISTINCT concat(x.kodetoko, x.nomortransaksi))::numeric
							ELSE 0::numeric
						END AS m_8,
						CASE
							WHEN date_trunc('month'::text, x.tanggal::timestamp with time zone) = date_trunc('month'::text, now()::date - '10 mons'::interval month) THEN count(DISTINCT concat(x.kodetoko, x.nomortransaksi))::numeric
							ELSE 0::numeric
						END AS m_9,
						CASE
							WHEN date_trunc('month'::text, x.tanggal::timestamp with time zone) = date_trunc('month'::text, now()::date - '11 mons'::interval month) THEN count(DISTINCT concat(x.kodetoko, x.nomortransaksi))::numeric
							ELSE 0::numeric
						END AS m_10
					FROM smi_rms15_transaksi_toko_perjenis_member_v3 x
					WHERE x.idcabang = 9 
					AND x.idjenisproduk <> 4 AND x.statusproduk <> 'K'::bpchar
					AND x.nopolisi is not null
	-- 		AND x.nopolisi='A5388VAP'
					GROUP BY x.tanggal, x.idcabang, x.namacabang, x.nopolisi
					) x0
				GROUP BY x0.idcabang, x0.namacabang, x0.nopolisi
			) x1
	--		WHERE x1.mtd_h_1 > 0::numeric
		GROUP BY x1.idcabang, x1.namacabang, x1.nopolisi, x1.mtd_h_1, x1.m_1, x1.m_2, x1.m_3, x1.m_4, x1.m_5, x1.m_6, x1.m_7, x1.m_8, x1.m_9, x1.m_10
) x2
WHERE x2.sum > 0::numeric;		


-- select * from public.adhock_temp where idcabang=7;

-- select distinct idcabang from public.adhock_temp;


create TABLE public.adhock_temp (
idcabang int4 NULL,
	namacabang varchar(20) NULL,
	nopolisi varchar(10) NULL,
	mtd_h_1 numeric NULL,
	m_1 numeric NULL,
	m_2 numeric NULL,
	m_3 numeric null,
	m_4 numeric null,
	m_5 numeric null,
	m_6 numeric null,
	m_7 numeric null,
	m_8 numeric null,
	m_9 numeric null,
	m_10 numeric null,
	sum numeric NULL
);
