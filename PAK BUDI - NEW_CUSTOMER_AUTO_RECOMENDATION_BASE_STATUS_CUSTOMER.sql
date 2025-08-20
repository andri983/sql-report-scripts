WITH 
source (idcabang,namacabang,nopolisi,mtd_h_1,fraud,m_0,m_1,m_2,m_3,m_4,m_5,m_6,m_7,m_8,m_9,m_10,m_11,m_12,m_13, m_14, m_15, m_16, m_17, m_18, m_19, m_20, m_21, m_22, m_23, m_24, m_25, m_26, m_27, m_28, m_29, m_30, m_31, m_32, m_33, m_34, m_35, m_36,
		m7s12,m13s18,m19s24,m25s36,m1s36,new,repeat,m_00,
		m1,m2,m3,m4,m5,m6,m7,m8,m9,m10,m11,m12,m13, m14, m15, m16, m17, m18, m19, m20, m21, m22, m23, m24, m25, m26, m27, m28, m29, m30, m31, m32, m33, m34, m35, m36)
AS 
( 
			   SELECT x22.idcabang,x22.namacabang,x22.nopolisi,x22.mtd_h_1,x22.fraud,x22.m_0,x22.m_1,x22.m_2,x22.m_3,x22.m_4,x22.m_5,x22.m_6,x22.m_7,x22.m_8,x22.m_9,x22.m_10,x22.m_11,x22.m_12,x22.m_13, x22.m_14, x22.m_15, x22.m_16, x22.m_17, x22.m_18, x22.m_19, x22.m_20, x22.m_21, x22.m_22, x22.m_23, x22.m_24, x22.m_25, x22.m_26, x22.m_27, x22.m_28, x22.m_29, x22.m_30, x22.m_31, x22.m_32, x22.m_33, x22.m_34, x22.m_35, x22.m_36,
			   x22.m7s12,x22.m13s18,x22.m19s24,x22.m25s36,x22.m1s36,new,x22.repeat,x22.m_00,
			   x22.m1,x22.m2,x22.m3,x22.m4,x22.m5,x22.m6,x22.m7,x22.m8,x22.m9,x22.m10,x22.m11,x22.m12,x22.m13, x22.m14, x22.m15, x22.m16, x22.m17, x22.m18, x22.m19, x22.m20, x22.m21, x22.m22, x22.m23, x22.m24, x22.m25, x22.m26, x22.m27, x22.m28, x22.m29, x22.m30, x22.m31, x22.m32, x22.m33, x22.m34, x22.m35, x22.m36
					FROM ( 
						SELECT x2.idcabang,x2.namacabang,x2.nopolisi,x2.mtd_h_1,x2.fraud,x2.m_0,x2.m_1,x2.m_2,x2.m_3,x2.m_4,x2.m_5,x2.m_6,x2.m_7,x2.m_8,x2.m_9,x2.m_10,x2.m_11,x2.m_12,x2.m_13, x2.m_14, x2.m_15, x2.m_16, x2.m_17, x2.m_18, x2.m_19, x2.m_20, x2.m_21, x2.m_22, x2.m_23, x2.m_24, x2.m_25, x2.m_26, x2.m_27, x2.m_28, x2.m_29, x2.m_30, x2.m_31, x2.m_32, x2.m_33, x2.m_34, x2.m_35, x2.m_36,
							sum(x2.m_7 + x2.m_8 + x2.m_9 + x2.m_10 + x2.m_11 + x2.m_12) AS m7s12,
							sum(x2.m_13 + x2.m_14 + x2.m_15 + x2.m_16 + x2.m_17 + x2.m_18) AS m13s18,
							sum(x2.m_19 + x2.m_20 + x2.m_21 + x2.m_22 + x2.m_23 + x2.m_24) AS m19s24,
							sum(x2.m_25 + x2.m_26 + x2.m_27 + x2.m_28 + x2.m_29 + x2.m_30 + x2.m_31 + x2.m_32 + x2.m_33 + x2.m_34 + x2.m_35 + x2.m_36) AS m25s36,
							--##MENGHITUNG M1 S/D M36
							SUM(x2.m_1 + x2.m_2 + x2.m_3 + x2.m_4 + x2.m_5 + x2.m_6 + x2.m_7 + x2.m_8 + x2.m_9 + x2.m_10 + x2.m_11 + x2.m_12 + x2.m_13 + x2.m_14 + x2.m_15 + x2.m_16 + x2.m_17 + x2.m_18 + x2.m_19 + x2.m_20 + x2.m_21 + x2.m_22 + x2.m_23 + x2.m_24 + x2.m_25 + x2.m_26 + x2.m_27 + x2.m_28 + x2.m_29 + x2.m_30 + x2.m_31 + x2.m_32 + x2.m_33 + x2.m_34 + x2.m_35 + x2.m_36) AS m1s36,
							x2.new,
							CASE
							   WHEN sum(x2.m_0) > 3::numeric AND (sum(x2.m_1) + sum(x2.m_2) + sum(x2.m_3) + sum(x2.m_4) + sum(x2.m_5) + sum(x2.m_6) + sum(x2.m_7) + sum(x2.m_8) + sum(x2.m_9) + sum(x2.m_10) + sum(x2.m_11) + SUM(x2.m_12) + SUM(x2.m_13) + SUM(x2.m_14) + SUM(x2.m_15) + SUM(x2.m_16) + SUM(x2.m_17) + SUM(x2.m_18) + SUM(x2.m_19) + SUM(x2.m_20) + SUM(x2.m_21) + SUM(x2.m_22) + SUM(x2.m_23) + SUM(x2.m_24) + SUM(x2.m_25) + SUM(x2.m_26) + SUM(x2.m_27) + SUM(x2.m_28) + SUM(x2.m_29) + SUM(x2.m_30) + SUM(x2.m_31) + SUM(x2.m_32) + SUM(x2.m_33) + SUM(x2.m_34) + SUM(x2.m_35) + SUM(x2.m_36)) = 0::numeric THEN 2::numeric
							   WHEN sum(x2.m_0) = 3::numeric AND (sum(x2.m_1) + sum(x2.m_2) + sum(x2.m_3) + sum(x2.m_4) + sum(x2.m_5) + sum(x2.m_6) + sum(x2.m_7) + sum(x2.m_8) + sum(x2.m_9) + sum(x2.m_10) + sum(x2.m_11) + SUM(x2.m_12) + SUM(x2.m_13) + SUM(x2.m_14) + SUM(x2.m_15) + SUM(x2.m_16) + SUM(x2.m_17) + SUM(x2.m_18) + SUM(x2.m_19) + SUM(x2.m_20) + SUM(x2.m_21) + SUM(x2.m_22) + SUM(x2.m_23) + SUM(x2.m_24) + SUM(x2.m_25) + SUM(x2.m_26) + SUM(x2.m_27) + SUM(x2.m_28) + SUM(x2.m_29) + SUM(x2.m_30) + SUM(x2.m_31) + SUM(x2.m_32) + SUM(x2.m_33) + SUM(x2.m_34) + SUM(x2.m_35) + SUM(x2.m_36)) = 0::numeric THEN 2::numeric
							   WHEN sum(x2.m_0) > 2::numeric AND (sum(x2.m_1) + sum(x2.m_2) + sum(x2.m_3) + sum(x2.m_4) + sum(x2.m_5) + sum(x2.m_6) + sum(x2.m_7) + sum(x2.m_8) + sum(x2.m_9) + sum(x2.m_10) + sum(x2.m_11) + SUM(x2.m_12) + SUM(x2.m_13) + SUM(x2.m_14) + SUM(x2.m_15) + SUM(x2.m_16) + SUM(x2.m_17) + SUM(x2.m_18) + SUM(x2.m_19) + SUM(x2.m_20) + SUM(x2.m_21) + SUM(x2.m_22) + SUM(x2.m_23) + SUM(x2.m_24) + SUM(x2.m_25) + SUM(x2.m_26) + SUM(x2.m_27) + SUM(x2.m_28) + SUM(x2.m_29) + SUM(x2.m_30) + SUM(x2.m_31) + SUM(x2.m_32) + SUM(x2.m_33) + SUM(x2.m_34) + SUM(x2.m_35) + SUM(x2.m_36)) = 0::numeric THEN 1::numeric
							   WHEN sum(x2.m_0) >= 3::numeric AND (sum(x2.m_1) + sum(x2.m_2) + sum(x2.m_3) + sum(x2.m_4) + sum(x2.m_5) + sum(x2.m_6) + sum(x2.m_7) + sum(x2.m_8) + sum(x2.m_9) + sum(x2.m_10) + sum(x2.m_11) + SUM(x2.m_12) + SUM(x2.m_13) + SUM(x2.m_14) + SUM(x2.m_15) + SUM(x2.m_16) + SUM(x2.m_17) + SUM(x2.m_18) + SUM(x2.m_19) + SUM(x2.m_20) + SUM(x2.m_21) + SUM(x2.m_22) + SUM(x2.m_23) + SUM(x2.m_24) + SUM(x2.m_25) + SUM(x2.m_26) + SUM(x2.m_27) + SUM(x2.m_28) + SUM(x2.m_29) + SUM(x2.m_30) + SUM(x2.m_31) + SUM(x2.m_32) + SUM(x2.m_33) + SUM(x2.m_34) + SUM(x2.m_35) + SUM(x2.m_36)) = 0::numeric THEN 2::numeric
							   WHEN sum(x2.m_0) >= 3::numeric AND (sum(x2.m_1) + sum(x2.m_2) + sum(x2.m_3) + sum(x2.m_4) + sum(x2.m_5) + sum(x2.m_6) + sum(x2.m_7) + sum(x2.m_8) + sum(x2.m_9) + sum(x2.m_10) + sum(x2.m_11) + SUM(x2.m_12) + SUM(x2.m_13) + SUM(x2.m_14) + SUM(x2.m_15) + SUM(x2.m_16) + SUM(x2.m_17) + SUM(x2.m_18) + SUM(x2.m_19) + SUM(x2.m_20) + SUM(x2.m_21) + SUM(x2.m_22) + SUM(x2.m_23) + SUM(x2.m_24) + SUM(x2.m_25) + SUM(x2.m_26) + SUM(x2.m_27) + SUM(x2.m_28) + SUM(x2.m_29) + SUM(x2.m_30) + SUM(x2.m_31) + SUM(x2.m_32) + SUM(x2.m_33) + SUM(x2.m_34) + SUM(x2.m_35) + SUM(x2.m_36)) > 0::numeric THEN 3::numeric
							   WHEN sum(x2.m_0) = 1::numeric AND (sum(x2.m_1) + sum(x2.m_2) + sum(x2.m_3) + sum(x2.m_4) + sum(x2.m_5) + sum(x2.m_6) + sum(x2.m_7) + sum(x2.m_8) + sum(x2.m_9) + sum(x2.m_10) + sum(x2.m_11) + SUM(x2.m_12) + SUM(x2.m_13) + SUM(x2.m_14) + SUM(x2.m_15) + SUM(x2.m_16) + SUM(x2.m_17) + SUM(x2.m_18) + SUM(x2.m_19) + SUM(x2.m_20) + SUM(x2.m_21) + SUM(x2.m_22) + SUM(x2.m_23) + SUM(x2.m_24) + SUM(x2.m_25) + SUM(x2.m_26) + SUM(x2.m_27) + SUM(x2.m_28) + SUM(x2.m_29) + SUM(x2.m_30) + SUM(x2.m_31) + SUM(x2.m_32) + SUM(x2.m_33) + SUM(x2.m_34) + SUM(x2.m_35) + SUM(x2.m_36)) = 0::numeric THEN 0::numeric
							   WHEN sum(x2.m_0) = 2::numeric AND (sum(x2.m_1) + sum(x2.m_2) + sum(x2.m_3) + sum(x2.m_4) + sum(x2.m_5) + sum(x2.m_6) + sum(x2.m_7) + sum(x2.m_8) + sum(x2.m_9) + sum(x2.m_10) + sum(x2.m_11) + SUM(x2.m_12) + SUM(x2.m_13) + SUM(x2.m_14) + SUM(x2.m_15) + SUM(x2.m_16) + SUM(x2.m_17) + SUM(x2.m_18) + SUM(x2.m_19) + SUM(x2.m_20) + SUM(x2.m_21) + SUM(x2.m_22) + SUM(x2.m_23) + SUM(x2.m_24) + SUM(x2.m_25) + SUM(x2.m_26) + SUM(x2.m_27) + SUM(x2.m_28) + SUM(x2.m_29) + SUM(x2.m_30) + SUM(x2.m_31) + SUM(x2.m_32) + SUM(x2.m_33) + SUM(x2.m_34) + SUM(x2.m_35) + SUM(x2.m_36)) = 0::numeric THEN 1::numeric
							   ELSE sum(x2.m_0)
							END AS repeat,
							CASE
							   WHEN sum(x2.m_0) > 0::numeric AND (sum(x2.m_1) + sum(x2.m_2) + sum(x2.m_3) + sum(x2.m_4) + sum(x2.m_5) + sum(x2.m_6) + sum(x2.m_7) + sum(x2.m_8) + sum(x2.m_9) + sum(x2.m_10) + sum(x2.m_11) + SUM(x2.m_12) + SUM(x2.m_13) + SUM(x2.m_14) + SUM(x2.m_15) + SUM(x2.m_16) + SUM(x2.m_17) + SUM(x2.m_18) + SUM(x2.m_19) + SUM(x2.m_20) + SUM(x2.m_21) + SUM(x2.m_22) + SUM(x2.m_23) + SUM(x2.m_24) + SUM(x2.m_25) + SUM(x2.m_26) + SUM(x2.m_27) + SUM(x2.m_28) + SUM(x2.m_29) + SUM(x2.m_30) + SUM(x2.m_31) + SUM(x2.m_32) + SUM(x2.m_33) + SUM(x2.m_34) + SUM(x2.m_35) + SUM(x2.m_36)) = 0::numeric THEN sum(x2.m_0) - x2.new::numeric
							   WHEN sum(x2.m_0) > 0::numeric AND (sum(x2.m_1) + sum(x2.m_2) + sum(x2.m_3) + sum(x2.m_4) + sum(x2.m_5) + sum(x2.m_6) + sum(x2.m_7) + sum(x2.m_8) + sum(x2.m_9) + sum(x2.m_10) + sum(x2.m_11) + SUM(x2.m_12) + SUM(x2.m_13) + SUM(x2.m_14) + SUM(x2.m_15) + SUM(x2.m_16) + SUM(x2.m_17) + SUM(x2.m_18) + SUM(x2.m_19) + SUM(x2.m_20) + SUM(x2.m_21) + SUM(x2.m_22) + SUM(x2.m_23) + SUM(x2.m_24) + SUM(x2.m_25) + SUM(x2.m_26) + SUM(x2.m_27) + SUM(x2.m_28) + SUM(x2.m_29) + SUM(x2.m_30) + SUM(x2.m_31) + SUM(x2.m_32) + SUM(x2.m_33) + SUM(x2.m_34) + SUM(x2.m_35) + SUM(x2.m_36)) > 0::numeric THEN 0::numeric
							   ELSE 0::numeric
							END AS m_00,
							x2.m_1 AS m1,x2.m_2 AS m2,x2.m_3 AS m3,x2.m_4 AS m4,x2.m_5 AS m5,x2.m_6 AS m6,x2.m_7 AS m7,x2.m_8 AS m8,x2.m_9 AS m9,x2.m_10 AS m10,x2.m_11 AS m11, x2.m_12 AS m12, x2.m_13 AS m13, x2.m_14 AS m14, x2.m_15 AS m15, x2.m_16 AS m16, x2.m_17 AS m17, x2.m_18 AS m18, x2.m_19 AS m19, x2.m_20 AS m20, x2.m_21 AS m21, x2.m_22 AS m22, x2.m_23 AS m23, x2.m_24 AS m24, x2.m_25 AS m25, x2.m_26 AS m26, x2.m_27 AS m27, x2.m_28 AS m28, x2.m_29 AS m29, x2.m_30 AS m30, x2.m_31 AS m31, x2.m_32 AS m32, x2.m_33 AS m33, x2.m_34 AS m34, x2.m_35 AS m35, x2.m_36 AS m36
							FROM( 
								SELECT x1.idcabang,x1.namacabang,x1.nopolisi,x1.mtd_h_1,
									CASE
										WHEN sum(x1.mtd_h_1) > 3::numeric THEN sum(x1.mtd_h_1) - 3::numeric
										ELSE 0::numeric
									END AS fraud,
									CASE
										WHEN sum(x1.mtd_h_1) > 3::numeric THEN sum(x1.mtd_h_1) - (sum(x1.mtd_h_1) - 3::numeric)
										ELSE sum(x1.mtd_h_1) - 0::numeric
									END AS m_0,
										x1.m_1, x1.m_2, x1.m_3, x1.m_4, x1.m_5, x1.m_6, x1.m_7, x1.m_8, x1.m_9, x1.m_10, x1.m_11, x1.m_12, x1.m_13, x1.m_14, x1.m_15, x1.m_16, x1.m_17, x1.m_18, x1.m_19, x1.m_20, x1.m_21, x1.m_22, x1.m_23, x1.m_24, x1.m_25, x1.m_26, x1.m_27, x1.m_28, x1.m_29, x1.m_30, x1.m_31, x1.m_32, x1.m_33, x1.m_34, x1.m_35, x1.m_36,
									CASE
										WHEN sum(x1.mtd_h_1) > 0::numeric AND (SUM(x1.m_1) + SUM(x1.m_2) + SUM(x1.m_3) + SUM(x1.m_4) + SUM(x1.m_5) + SUM(x1.m_6) + SUM(x1.m_7) + SUM(x1.m_8) + SUM(x1.m_9) + SUM(x1.m_10) + SUM(x1.m_11) + SUM(x1.m_12) + SUM(x1.m_13) + SUM(x1.m_14) + SUM(x1.m_15) + SUM(x1.m_16) + SUM(x1.m_17) + SUM(x1.m_18) + SUM(x1.m_19) + SUM(x1.m_20) + SUM(x1.m_21) + SUM(x1.m_22) + SUM(x1.m_23) + SUM(x1.m_24) + SUM(x1.m_25) + SUM(x1.m_26) + SUM(x1.m_27) + SUM(x1.m_28) + SUM(x1.m_29) + SUM(x1.m_30) + SUM(x1.m_31) + SUM(x1.m_32) + SUM(x1.m_33) + SUM(x1.m_34) + SUM(x1.m_35) + SUM(x1.m_36)) = 0::numeric THEN 1
										WHEN sum(x1.mtd_h_1) > 0::numeric AND (SUM(x1.m_1) + SUM(x1.m_2) + SUM(x1.m_3) + SUM(x1.m_4) + SUM(x1.m_5) + SUM(x1.m_6) + SUM(x1.m_7) + SUM(x1.m_8) + SUM(x1.m_9) + SUM(x1.m_10) + SUM(x1.m_11) + SUM(x1.m_12) + SUM(x1.m_13) + SUM(x1.m_14) + SUM(x1.m_15) + SUM(x1.m_16) + SUM(x1.m_17) + SUM(x1.m_18) + SUM(x1.m_19) + SUM(x1.m_20) + SUM(x1.m_21) + SUM(x1.m_22) + SUM(x1.m_23) + SUM(x1.m_24) + SUM(x1.m_25) + SUM(x1.m_26) + SUM(x1.m_27) + SUM(x1.m_28) + SUM(x1.m_29) + SUM(x1.m_30) + SUM(x1.m_31) + SUM(x1.m_32) + SUM(x1.m_33) + SUM(x1.m_34) + SUM(x1.m_35) + SUM(x1.m_36)) > 0::numeric THEN 0
										ELSE 0
									END AS new
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
										SUM(x0.m_10) AS m_10,
										SUM(x0.m_11) AS m_11,
										SUM(x0.m_12) AS m_12,
										SUM(x0.m_13) AS m_13,
										SUM(x0.m_14) AS m_14,
										SUM(x0.m_15) AS m_15,
										SUM(x0.m_16) AS m_16,
										SUM(x0.m_17) AS m_17,
										SUM(x0.m_18) AS m_18,
										SUM(x0.m_19) AS m_19,
										SUM(x0.m_20) AS m_20,
										SUM(x0.m_21) AS m_21,
										SUM(x0.m_22) AS m_22,
										SUM(x0.m_23) AS m_23,
										SUM(x0.m_24) AS m_24,
										SUM(x0.m_25) AS m_25,
										SUM(x0.m_26) AS m_26,
										SUM(x0.m_27) AS m_27,
										SUM(x0.m_28) AS m_28,
										SUM(x0.m_29) AS m_29,
										SUM(x0.m_30) AS m_30,
										SUM(x0.m_31) AS m_31,
										SUM(x0.m_32) AS m_32,
										SUM(x0.m_33) AS m_33,
										SUM(x0.m_34) AS m_34,
										SUM(x0.m_35) AS m_35,
										SUM(x0.m_36) AS m_36
											FROM( 
 												SELECT x.tanggal, x.idcabang, x.namacabang,
 													upper(x.nopolisi::text) AS nopolisi,
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
 													END AS m_10,
 													CASE
 														WHEN date_trunc('month'::text, x.tanggal::timestamp with time zone) = date_trunc('month'::text, now()::date - '12 mons'::interval month) THEN count(DISTINCT concat(x.kodetoko, x.nomortransaksi))::numeric
 														ELSE 0::numeric
 													END AS m_11,
 													CASE
 														WHEN date_trunc('month'::text, x.tanggal::timestamp with time zone) = date_trunc('month'::text, now()::date - '13 mons'::interval month) THEN count(DISTINCT concat(x.kodetoko, x.nomortransaksi))::numeric
 														ELSE 0::numeric
 													END AS m_12,
 													CASE
 														WHEN date_trunc('month'::text, x.tanggal::timestamp with time zone) = date_trunc('month'::text, now()::date - '14 mons'::interval month) THEN count(DISTINCT concat(x.kodetoko, x.nomortransaksi))::numeric
 														ELSE 0::numeric
 													END AS m_13,
 													CASE
 														WHEN date_trunc('month'::text, x.tanggal::timestamp with time zone) = date_trunc('month'::text, now()::date - '15 mons'::interval month) THEN count(DISTINCT concat(x.kodetoko, x.nomortransaksi))::numeric
 														ELSE 0::numeric
 													END AS m_14,
 													CASE
 														WHEN date_trunc('month'::text, x.tanggal::timestamp with time zone) = date_trunc('month'::text, now()::date - '16 mons'::interval month) THEN count(DISTINCT concat(x.kodetoko, x.nomortransaksi))::numeric
 														ELSE 0::numeric
 													END AS m_15,
 													CASE
 														WHEN date_trunc('month'::text, x.tanggal::timestamp with time zone) = date_trunc('month'::text, now()::date - '17 mons'::interval month) THEN count(DISTINCT concat(x.kodetoko, x.nomortransaksi))::numeric
 														ELSE 0::numeric
 													END AS m_16,
 													CASE
 														WHEN date_trunc('month'::text, x.tanggal::timestamp with time zone) = date_trunc('month'::text, now()::date - '18 mons'::interval month) THEN count(DISTINCT concat(x.kodetoko, x.nomortransaksi))::numeric
 														ELSE 0::numeric
 													END AS m_17,
 													CASE
 														WHEN date_trunc('month'::text, x.tanggal::timestamp with time zone) = date_trunc('month'::text, now()::date - '19 mons'::interval month) THEN count(DISTINCT concat(x.kodetoko, x.nomortransaksi))::numeric
 														ELSE 0::numeric
 													END AS m_18,
 													CASE
 														WHEN date_trunc('month'::text, x.tanggal::timestamp with time zone) = date_trunc('month'::text, now()::date - '20 mons'::interval month) THEN count(DISTINCT concat(x.kodetoko, x.nomortransaksi))::numeric
 														ELSE 0::numeric
 													END AS m_19,
 													CASE
 														WHEN date_trunc('month'::text, x.tanggal::timestamp with time zone) = date_trunc('month'::text, now()::date - '21 mons'::interval month) THEN count(DISTINCT concat(x.kodetoko, x.nomortransaksi))::numeric
 														ELSE 0::numeric
 													END AS m_20,
 													CASE
 														WHEN date_trunc('month'::text, x.tanggal::timestamp with time zone) = date_trunc('month'::text, now()::date - '22 mons'::interval month) THEN count(DISTINCT concat(x.kodetoko, x.nomortransaksi))::numeric
 														ELSE 0::numeric
 													END AS m_21,
 													CASE
 														WHEN date_trunc('month'::text, x.tanggal::timestamp with time zone) = date_trunc('month'::text, now()::date - '23 mons'::interval month) THEN count(DISTINCT concat(x.kodetoko, x.nomortransaksi))::numeric
 														ELSE 0::numeric
 													END AS m_22,
 													CASE
 														WHEN date_trunc('month'::text, x.tanggal::timestamp with time zone) = date_trunc('month'::text, now()::date - '24 mons'::interval month) THEN count(DISTINCT concat(x.kodetoko, x.nomortransaksi))::numeric
 														ELSE 0::numeric
 													END AS m_23,
 													CASE
 														WHEN date_trunc('month'::text, x.tanggal::timestamp with time zone) = date_trunc('month'::text, now()::date - '25 mons'::interval month) THEN count(DISTINCT concat(x.kodetoko, x.nomortransaksi))::numeric
 														ELSE 0::numeric
 													END AS m_24,
 													CASE
 														WHEN date_trunc('month'::text, x.tanggal::timestamp with time zone) = date_trunc('month'::text, now()::date - '26 mons'::interval month) THEN count(DISTINCT concat(x.kodetoko, x.nomortransaksi))::numeric
 														ELSE 0::numeric
 													END AS m_25,
 													CASE
 														WHEN date_trunc('month'::text, x.tanggal::timestamp with time zone) = date_trunc('month'::text, now()::date - '27 mons'::interval month) THEN count(DISTINCT concat(x.kodetoko, x.nomortransaksi))::numeric
 														ELSE 0::numeric
 													END AS m_26,
 													CASE
 														WHEN date_trunc('month'::text, x.tanggal::timestamp with time zone) = date_trunc('month'::text, now()::date - '28 mons'::interval month) THEN count(DISTINCT concat(x.kodetoko, x.nomortransaksi))::numeric
 														ELSE 0::numeric
 													END AS m_27,
 													CASE
 														WHEN date_trunc('month'::text, x.tanggal::timestamp with time zone) = date_trunc('month'::text, now()::date - '29 mons'::interval month) THEN count(DISTINCT concat(x.kodetoko, x.nomortransaksi))::numeric
 														ELSE 0::numeric
 													END AS m_28,
 													CASE
 														WHEN date_trunc('month'::text, x.tanggal::timestamp with time zone) = date_trunc('month'::text, now()::date - '30 mons'::interval month) THEN count(DISTINCT concat(x.kodetoko, x.nomortransaksi))::numeric
 														ELSE 0::numeric
 													END AS m_29,
 													CASE
 														WHEN date_trunc('month'::text, x.tanggal::timestamp with time zone) = date_trunc('month'::text, now()::date - '31 mons'::interval month) THEN count(DISTINCT concat(x.kodetoko, x.nomortransaksi))::numeric
 														ELSE 0::numeric
 													END AS m_30,
 													CASE
 														WHEN date_trunc('month'::text, x.tanggal::timestamp with time zone) = date_trunc('month'::text, now()::date - '32 mons'::interval month) THEN count(DISTINCT concat(x.kodetoko, x.nomortransaksi))::numeric
 														ELSE 0::numeric
 													END AS m_31,
 													CASE
 														WHEN date_trunc('month'::text, x.tanggal::timestamp with time zone) = date_trunc('month'::text, now()::date - '33 mons'::interval month) THEN count(DISTINCT concat(x.kodetoko, x.nomortransaksi))::numeric
 														ELSE 0::numeric
 													END AS m_32,
 													CASE
 														WHEN date_trunc('month'::text, x.tanggal::timestamp with time zone) = date_trunc('month'::text, now()::date - '34 mons'::interval month) THEN count(DISTINCT concat(x.kodetoko, x.nomortransaksi))::numeric
 														ELSE 0::numeric
 													END AS m_33,
 													CASE
 														WHEN date_trunc('month'::text, x.tanggal::timestamp with time zone) = date_trunc('month'::text, now()::date - '35 mons'::interval month) THEN count(DISTINCT concat(x.kodetoko, x.nomortransaksi))::numeric
 														ELSE 0::numeric
 													END AS m_34,
 													CASE
 														WHEN date_trunc('month'::text, x.tanggal::timestamp with time zone) = date_trunc('month'::text, now()::date - '36 mons'::interval month) THEN count(DISTINCT concat(x.kodetoko, x.nomortransaksi))::numeric
 														ELSE 0::numeric
 													END AS m_35,
 													CASE
 														WHEN date_trunc('month'::text, x.tanggal::timestamp with time zone) = date_trunc('month'::text, now()::date - '37 mons'::interval month) THEN count(DISTINCT concat(x.kodetoko, x.nomortransaksi))::numeric
 														ELSE 0::numeric
 													END AS m_36
 												FROM smi_rms15_transaksi_toko_perjenis_member_v3 x
 												WHERE x.idcabang = 9 
 												AND x.idjenisproduk <> 4 AND x.statusproduk <> 'K'::bpchar
 												AND x.nopolisi is not NULL
 												GROUP BY x.tanggal, x.idcabang, x.namacabang, x.nopolisi
												) x0
											GROUP BY x0.idcabang, x0.namacabang, x0.nopolisi
										) x1
									WHERE x1.mtd_h_1 > 0::numeric
									GROUP BY x1.idcabang, x1.namacabang, x1.nopolisi, x1.mtd_h_1, x1.m_1, x1.m_2, x1.m_3, x1.m_4, x1.m_5, x1.m_6, x1.m_7, x1.m_8, x1.m_9, x1.m_10, x1.m_11, x1.m_12, x1.m_13, x1.m_14, x1.m_15, x1.m_16, x1.m_17, x1.m_18, x1.m_19, x1.m_20, x1.m_21, x1.m_22, x1.m_23, x1.m_24, x1.m_25, x1.m_26, x1.m_27, x1.m_28, x1.m_29, x1.m_30, x1.m_31, x1.m_32, x1.m_33, x1.m_34, x1.m_35, x1.m_36
								) x2
							GROUP BY x2.idcabang, x2.namacabang, x2.nopolisi, x2.mtd_h_1, x2.fraud, x2.m_0, x2.m_1, x2.m_2, x2.m_3, x2.new, x2.m_4, x2.m_5, x2.m_6, x2.m_7, x2.m_8, x2.m_9, x2.m_10, x2.m_11, x2.m_12, x2.m_13, x2.m_14, x2.m_15, x2.m_16, x2.m_17, x2.m_18, x2.m_19, x2.m_20, x2.m_21, x2.m_22, x2.m_23, x2.m_24, x2.m_25, x2.m_26, x2.m_27, x2.m_28, x2.m_29, x2.m_30, x2.m_31, x2.m_32, x2.m_33, x2.m_34, x2.m_35, x2.m_36
						) x22
),
result_m01(idcabang,namacabang,nopolisi,month1) as (
	SELECT 
		a2.idcabang,a2.namacabang,a2.nopolisi,
	CASE
		WHEN sum(a2.m1) >= 3::numeric AND sum(a2.repeat) <= 3::numeric THEN sum(repeat)
		WHEN sum(a2.m1) = 2::numeric AND sum(a2.repeat) = 1::numeric THEN 1::numeric
		WHEN sum(a2.m1) > 0::numeric AND sum(a2.repeat) < 3::numeric THEN sum(m1)
		WHEN sum(a2.m1) > 0::numeric AND sum(a2.repeat) >= 3::numeric THEN sum(m1)
		WHEN sum(a2.m1) = 0::numeric THEN 0::numeric
		ELSE 0::numeric
	END AS month1
	FROM source a2
	GROUP BY a2.idcabang,a2.namacabang,a2.nopolisi,a2.mtd_h_1
),
result_m02(idcabang, namacabang, nopolisi, m_00, month1, month2) AS (
    SELECT
        a2.idcabang, a2.namacabang, a2.nopolisi, a2.m_00, result_m01.month1,
        CASE
            WHEN sum(a2.m_2) > 0::numeric THEN sum(a2.repeat) - (a2.m_00 + result_m01.month1)
            WHEN sum(a2.m_2) = 0::numeric THEN 0::numeric
            ELSE 0::numeric
        END AS month2
    FROM source a2
    LEFT JOIN result_m01 ON result_m01.idcabang = a2.idcabang AND result_m01.namacabang = a2.namacabang AND result_m01.nopolisi = a2.nopolisi
    GROUP BY a2.idcabang, a2.namacabang, a2.nopolisi, a2.mtd_h_1, a2.m_00, result_m01.month1
)
,
result_m03(idcabang, namacabang, nopolisi, m_00, month1, month2, month3) AS (
    SELECT
        a2.idcabang, a2.namacabang, a2.nopolisi, a2.m_00, result_m01.month1, result_m02.month2,
        CASE
            WHEN sum(a2.m_3) > 0::numeric THEN sum(a2.repeat) - ((a2.m_00 + result_m01.month1) + (a2.m_00 + result_m02.month2))
            WHEN sum(a2.m_3) = 0::numeric THEN 0::numeric
            ELSE 0::numeric
        END AS month3
    FROM source a2
    LEFT JOIN result_m01 ON result_m01.idcabang = a2.idcabang AND result_m01.namacabang = a2.namacabang AND result_m01.nopolisi = a2.nopolisi
	LEFT JOIN result_m02 ON result_m02.idcabang = a2.idcabang AND result_m02.namacabang = a2.namacabang AND result_m02.nopolisi = a2.nopolisi
    GROUP BY a2.idcabang, a2.namacabang, a2.nopolisi, a2.mtd_h_1, a2.m_00, result_m01.month1, result_m02.month2
),
result_m04(idcabang, namacabang, nopolisi, m_00, month1, month2, month3, month4) AS (
    SELECT
        a2.idcabang, a2.namacabang, a2.nopolisi, a2.m_00, result_m01.month1, result_m02.month2, result_m03.month3,
        CASE
            WHEN sum(a2.m_4) > 0::numeric THEN sum(a2.repeat) - (a2.m_00 + result_m01.month1 + result_m02.month2 + result_m03.month3)
            WHEN sum(a2.m_4) = 0::numeric THEN 0::numeric
            ELSE 0::numeric
        END AS month4
    FROM source a2
    LEFT JOIN result_m01 ON result_m01.idcabang = a2.idcabang AND result_m01.namacabang = a2.namacabang AND result_m01.nopolisi = a2.nopolisi
	LEFT JOIN result_m02 ON result_m02.idcabang = a2.idcabang AND result_m02.namacabang = a2.namacabang AND result_m02.nopolisi = a2.nopolisi
	LEFT JOIN result_m03 ON result_m03.idcabang = a2.idcabang AND result_m03.namacabang = a2.namacabang AND result_m03.nopolisi = a2.nopolisi
    GROUP BY a2.idcabang, a2.namacabang, a2.nopolisi, a2.mtd_h_1, a2.m_00, result_m01.month1, result_m02.month2, result_m03.month3
),
result_m05(idcabang, namacabang, nopolisi, m_00, month1, month2, month3, month4, month5) AS (
    SELECT
        a2.idcabang, a2.namacabang, a2.nopolisi, a2.m_00, result_m01.month1, result_m02.month2, result_m03.month3, result_m04.month4,
        CASE
            WHEN sum(a2.m_5) > 0::numeric THEN sum(a2.repeat) - (a2.m_00 + result_m01.month1 + result_m02.month2 + result_m03.month3 + result_m04.month4)
            WHEN sum(a2.m_5) = 0::numeric THEN 0::numeric
            ELSE 0::numeric
        END AS month5
    FROM source a2
    LEFT JOIN result_m01 ON result_m01.idcabang = a2.idcabang AND result_m01.namacabang = a2.namacabang AND result_m01.nopolisi = a2.nopolisi
	LEFT JOIN result_m02 ON result_m02.idcabang = a2.idcabang AND result_m02.namacabang = a2.namacabang AND result_m02.nopolisi = a2.nopolisi
	LEFT JOIN result_m03 ON result_m03.idcabang = a2.idcabang AND result_m03.namacabang = a2.namacabang AND result_m03.nopolisi = a2.nopolisi
	LEFT JOIN result_m04 ON result_m04.idcabang = a2.idcabang AND result_m04.namacabang = a2.namacabang AND result_m04.nopolisi = a2.nopolisi
    GROUP BY a2.idcabang, a2.namacabang, a2.nopolisi, a2.mtd_h_1, a2.m_00, result_m01.month1, result_m02.month2, result_m03.month3, result_m04.month4
),
result_m06(idcabang, namacabang, nopolisi, m_00, month1, month2, month3, month4, month5, month6) AS (
    SELECT
        a2.idcabang, a2.namacabang, a2.nopolisi, a2.m_00, result_m01.month1, result_m02.month2, result_m03.month3, result_m04.month4, result_m05.month5,
        CASE
            WHEN sum(a2.m_6) > 0::numeric THEN sum(a2.repeat) - (a2.m_00 + result_m01.month1 + result_m02.month2 + result_m03.month3 + result_m04.month4 + result_m05.month5)
            WHEN sum(a2.m_6) = 0::numeric THEN 0::numeric
            ELSE 0::numeric
        END AS month6
    FROM source a2
    LEFT JOIN result_m01 ON result_m01.idcabang = a2.idcabang AND result_m01.namacabang = a2.namacabang AND result_m01.nopolisi = a2.nopolisi
	LEFT JOIN result_m02 ON result_m02.idcabang = a2.idcabang AND result_m02.namacabang = a2.namacabang AND result_m02.nopolisi = a2.nopolisi
	LEFT JOIN result_m03 ON result_m03.idcabang = a2.idcabang AND result_m03.namacabang = a2.namacabang AND result_m03.nopolisi = a2.nopolisi
	LEFT JOIN result_m04 ON result_m04.idcabang = a2.idcabang AND result_m04.namacabang = a2.namacabang AND result_m04.nopolisi = a2.nopolisi
	LEFT JOIN result_m05 ON result_m05.idcabang = a2.idcabang AND result_m05.namacabang = a2.namacabang AND result_m05.nopolisi = a2.nopolisi
    GROUP BY a2.idcabang, a2.namacabang, a2.nopolisi, a2.mtd_h_1, a2.m_00, result_m01.month1, result_m02.month2, result_m03.month3, result_m04.month4, result_m05.month5
),	
result_m7s12(idcabang, namacabang, nopolisi, m_00, month1, month2, month3, month4, month5, month6, m7s12) AS (
    SELECT
        a2.idcabang, a2.namacabang, a2.nopolisi, a2.m_00, result_m01.month1, result_m02.month2, result_m03.month3, result_m04.month4, result_m05.month5, result_m06.month6,
        CASE
            WHEN sum(a2.m7s12) > 0::numeric THEN sum(a2.repeat) - (a2.m_00 + result_m01.month1 + result_m02.month2 + result_m03.month3 + result_m04.month4 + result_m05.month5 + result_m06.month6)
            WHEN sum(a2.m7s12) = 0::numeric THEN 0::numeric
            ELSE 0::numeric
        END AS m7s12
    FROM source a2
    LEFT JOIN result_m01 ON result_m01.idcabang = a2.idcabang AND result_m01.namacabang = a2.namacabang AND result_m01.nopolisi = a2.nopolisi
	LEFT JOIN result_m02 ON result_m02.idcabang = a2.idcabang AND result_m02.namacabang = a2.namacabang AND result_m02.nopolisi = a2.nopolisi
	LEFT JOIN result_m03 ON result_m03.idcabang = a2.idcabang AND result_m03.namacabang = a2.namacabang AND result_m03.nopolisi = a2.nopolisi
	LEFT JOIN result_m04 ON result_m04.idcabang = a2.idcabang AND result_m04.namacabang = a2.namacabang AND result_m04.nopolisi = a2.nopolisi
	LEFT JOIN result_m05 ON result_m05.idcabang = a2.idcabang AND result_m05.namacabang = a2.namacabang AND result_m05.nopolisi = a2.nopolisi
	LEFT JOIN result_m06 ON result_m06.idcabang = a2.idcabang AND result_m06.namacabang = a2.namacabang AND result_m06.nopolisi = a2.nopolisi
    GROUP BY a2.idcabang, a2.namacabang, a2.nopolisi, a2.mtd_h_1, a2.m_00, result_m01.month1, result_m02.month2, result_m03.month3, result_m04.month4, result_m05.month5, result_m06.month6 ,m7s12
),	
result_m13s18(idcabang, namacabang, nopolisi, m_00, month1, month2, month3, month4, month5, month6, m7s12, m13s18) AS (
    SELECT
        a2.idcabang, a2.namacabang, a2.nopolisi, a2.m_00, result_m01.month1, result_m02.month2, result_m03.month3, result_m04.month4, result_m05.month5, result_m06.month6, result_m7s12.m7s12,
        CASE
            WHEN sum(a2.m13s18) > 0::numeric THEN sum(a2.repeat) - (a2.m_00 + result_m01.month1 + result_m02.month2 + result_m03.month3 + result_m04.month4 + result_m05.month5 + result_m06.month6 + result_m7s12.m7s12)
            WHEN sum(a2.m13s18) = 0::numeric THEN 0::numeric
            ELSE 0::numeric
        END AS m13s18
    FROM source a2
    LEFT JOIN result_m01 ON result_m01.idcabang = a2.idcabang AND result_m01.namacabang = a2.namacabang AND result_m01.nopolisi = a2.nopolisi
	LEFT JOIN result_m02 ON result_m02.idcabang = a2.idcabang AND result_m02.namacabang = a2.namacabang AND result_m02.nopolisi = a2.nopolisi
	LEFT JOIN result_m03 ON result_m03.idcabang = a2.idcabang AND result_m03.namacabang = a2.namacabang AND result_m03.nopolisi = a2.nopolisi
	LEFT JOIN result_m04 ON result_m04.idcabang = a2.idcabang AND result_m04.namacabang = a2.namacabang AND result_m04.nopolisi = a2.nopolisi
	LEFT JOIN result_m05 ON result_m05.idcabang = a2.idcabang AND result_m05.namacabang = a2.namacabang AND result_m05.nopolisi = a2.nopolisi
	LEFT JOIN result_m06 ON result_m06.idcabang = a2.idcabang AND result_m06.namacabang = a2.namacabang AND result_m06.nopolisi = a2.nopolisi
	LEFT JOIN result_m7s12 ON result_m7s12.idcabang = a2.idcabang AND result_m7s12.namacabang = a2.namacabang AND result_m7s12.nopolisi = a2.nopolisi
    GROUP BY a2.idcabang, a2.namacabang, a2.nopolisi, a2.mtd_h_1, a2.m_00, result_m01.month1, result_m02.month2, result_m03.month3, result_m04.month4, result_m05.month5, result_m06.month6, result_m7s12.m7s12, m13s18
),	
result_m19s24(idcabang, namacabang, nopolisi, m_00, month1, month2, month3, month4, month5, month6, m7s12, m13s18, m19s24) AS (
    SELECT
        a2.idcabang, a2.namacabang, a2.nopolisi, a2.m_00, result_m01.month1, result_m02.month2, result_m03.month3, result_m04.month4, result_m05.month5, result_m06.month6, result_m7s12.m7s12, result_m13s18.m13s18,
        CASE
            WHEN sum(a2.m19s24) > 0::numeric THEN sum(a2.repeat) - (a2.m_00 + result_m01.month1 + result_m02.month2 + result_m03.month3 + result_m04.month4 + result_m05.month5 + result_m06.month6 + result_m7s12.m7s12 + result_m13s18.m13s18)
            WHEN sum(a2.m19s24) = 0::numeric THEN 0::numeric
            ELSE 0::numeric
        END AS m19s24
    FROM source a2
    LEFT JOIN result_m01 ON result_m01.idcabang = a2.idcabang AND result_m01.namacabang = a2.namacabang AND result_m01.nopolisi = a2.nopolisi
	LEFT JOIN result_m02 ON result_m02.idcabang = a2.idcabang AND result_m02.namacabang = a2.namacabang AND result_m02.nopolisi = a2.nopolisi
	LEFT JOIN result_m03 ON result_m03.idcabang = a2.idcabang AND result_m03.namacabang = a2.namacabang AND result_m03.nopolisi = a2.nopolisi
	LEFT JOIN result_m04 ON result_m04.idcabang = a2.idcabang AND result_m04.namacabang = a2.namacabang AND result_m04.nopolisi = a2.nopolisi
	LEFT JOIN result_m05 ON result_m05.idcabang = a2.idcabang AND result_m05.namacabang = a2.namacabang AND result_m05.nopolisi = a2.nopolisi
	LEFT JOIN result_m06 ON result_m06.idcabang = a2.idcabang AND result_m06.namacabang = a2.namacabang AND result_m06.nopolisi = a2.nopolisi
	LEFT JOIN result_m7s12 ON result_m7s12.idcabang = a2.idcabang AND result_m7s12.namacabang = a2.namacabang AND result_m7s12.nopolisi = a2.nopolisi
 	LEFT JOIN result_m13s18 ON result_m13s18.idcabang = a2.idcabang AND result_m13s18.namacabang = a2.namacabang AND result_m13s18.nopolisi = a2.nopolisi
   GROUP BY a2.idcabang, a2.namacabang, a2.nopolisi, a2.mtd_h_1, a2.m_00, result_m01.month1, result_m02.month2, result_m03.month3, result_m04.month4, result_m05.month5, result_m06.month6, result_m7s12.m7s12, result_m13s18.m13s18, m19s24
),	
result_m25s36(idcabang, namacabang, nopolisi, m_00, month1, month2, month3, month4, month5, month6, m7s12, m13s18, m19s24, m25s36) AS (
    SELECT
        a2.idcabang, a2.namacabang, a2.nopolisi, a2.m_00, result_m01.month1, result_m02.month2, result_m03.month3, result_m04.month4, result_m05.month5, result_m06.month6, result_m7s12.m7s12, result_m13s18.m13s18,result_m19s24.m19s24,
        CASE
            WHEN sum(a2.m25s36) > 0::numeric THEN sum(a2.repeat) - (a2.m_00 + result_m01.month1 + result_m02.month2 + result_m03.month3 + result_m04.month4 + result_m05.month5 + result_m06.month6 + result_m7s12.m7s12 + result_m13s18.m13s18 + result_m19s24.m19s24)
            WHEN sum(a2.m25s36) = 0::numeric THEN 0::numeric
            ELSE 0::numeric
        END AS m25s36
    FROM source a2
    LEFT JOIN result_m01 ON result_m01.idcabang = a2.idcabang AND result_m01.namacabang = a2.namacabang AND result_m01.nopolisi = a2.nopolisi
	LEFT JOIN result_m02 ON result_m02.idcabang = a2.idcabang AND result_m02.namacabang = a2.namacabang AND result_m02.nopolisi = a2.nopolisi
	LEFT JOIN result_m03 ON result_m03.idcabang = a2.idcabang AND result_m03.namacabang = a2.namacabang AND result_m03.nopolisi = a2.nopolisi
	LEFT JOIN result_m04 ON result_m04.idcabang = a2.idcabang AND result_m04.namacabang = a2.namacabang AND result_m04.nopolisi = a2.nopolisi
	LEFT JOIN result_m05 ON result_m05.idcabang = a2.idcabang AND result_m05.namacabang = a2.namacabang AND result_m05.nopolisi = a2.nopolisi
	LEFT JOIN result_m06 ON result_m06.idcabang = a2.idcabang AND result_m06.namacabang = a2.namacabang AND result_m06.nopolisi = a2.nopolisi
	LEFT JOIN result_m7s12 ON result_m7s12.idcabang = a2.idcabang AND result_m7s12.namacabang = a2.namacabang AND result_m7s12.nopolisi = a2.nopolisi
 	LEFT JOIN result_m13s18 ON result_m13s18.idcabang = a2.idcabang AND result_m13s18.namacabang = a2.namacabang AND result_m13s18.nopolisi = a2.nopolisi
 	LEFT JOIN result_m19s24 ON result_m19s24.idcabang = a2.idcabang AND result_m19s24.namacabang = a2.namacabang AND result_m19s24.nopolisi = a2.nopolisi
   GROUP BY a2.idcabang, a2.namacabang, a2.nopolisi, a2.mtd_h_1, a2.m_00, result_m01.month1, result_m02.month2, result_m03.month3, result_m04.month4, result_m05.month5, result_m06.month6, result_m7s12.m7s12, result_m13s18.m13s18, result_m19s24.m19s24, m25s36
),	
result_mtd(idcabang, namacabang, nopolisi, m_00, month1, month2, month3, month4, month5, month6, m7s12, m13s18, m19s24, m25s36, mtd) AS (
    SELECT
        a2.idcabang, a2.namacabang, a2.nopolisi, a2.m_00, result_m01.month1, result_m02.month2, result_m03.month3, result_m04.month4, result_m05.month5, result_m06.month6, result_m7s12.m7s12, result_m13s18.m13s18,result_m19s24.m19s24, result_m25s36.m25s36,
        CASE
            WHEN sum(a2.repeat) > 0::numeric THEN sum(a2.repeat) - (result_m01.month1 + result_m02.month2 + result_m03.month3 + result_m04.month4 + result_m05.month5 + result_m06.month6 + result_m7s12.m7s12 + result_m13s18.m13s18 + result_m19s24.m19s24 + result_m25s36.m25s36)
            WHEN sum(a2.repeat) = 0::numeric THEN 0::numeric
            ELSE 0::numeric
        END AS mtd
    FROM source a2
    LEFT JOIN result_m01 ON result_m01.idcabang = a2.idcabang AND result_m01.namacabang = a2.namacabang AND result_m01.nopolisi = a2.nopolisi
	LEFT JOIN result_m02 ON result_m02.idcabang = a2.idcabang AND result_m02.namacabang = a2.namacabang AND result_m02.nopolisi = a2.nopolisi
	LEFT JOIN result_m03 ON result_m03.idcabang = a2.idcabang AND result_m03.namacabang = a2.namacabang AND result_m03.nopolisi = a2.nopolisi
	LEFT JOIN result_m04 ON result_m04.idcabang = a2.idcabang AND result_m04.namacabang = a2.namacabang AND result_m04.nopolisi = a2.nopolisi
	LEFT JOIN result_m05 ON result_m05.idcabang = a2.idcabang AND result_m05.namacabang = a2.namacabang AND result_m05.nopolisi = a2.nopolisi
	LEFT JOIN result_m06 ON result_m06.idcabang = a2.idcabang AND result_m06.namacabang = a2.namacabang AND result_m06.nopolisi = a2.nopolisi
	LEFT JOIN result_m7s12 ON result_m7s12.idcabang = a2.idcabang AND result_m7s12.namacabang = a2.namacabang AND result_m7s12.nopolisi = a2.nopolisi
 	LEFT JOIN result_m13s18 ON result_m13s18.idcabang = a2.idcabang AND result_m13s18.namacabang = a2.namacabang AND result_m13s18.nopolisi = a2.nopolisi
 	LEFT JOIN result_m19s24 ON result_m19s24.idcabang = a2.idcabang AND result_m19s24.namacabang = a2.namacabang AND result_m19s24.nopolisi = a2.nopolisi
	LEFT JOIN result_m25s36 ON result_m25s36.idcabang = a2.idcabang AND result_m25s36.namacabang = a2.namacabang AND result_m25s36.nopolisi = a2.nopolisi
   GROUP BY a2.idcabang, a2.namacabang, a2.nopolisi, a2.mtd_h_1, a2.m_00, result_m01.month1, result_m02.month2, result_m03.month3, result_m04.month4, result_m05.month5, result_m06.month6, result_m7s12.m7s12, result_m13s18.m13s18, result_m19s24.m19s24, result_m25s36.m25s36
),
trxnew(idcabang, namacabang, nopolisi, uniqnew) AS (
    SELECT a2.idcabang, a2.namacabang, a2.nopolisi, 
	CASE
	WHEN SUM(a2.m1s36)=0::NUMERIC THEN 1::NUMERIC
	ELSE 0
	END AS new
    FROM source a2
    GROUP BY a2.idcabang, a2.namacabang, a2.nopolisi
),
trxexisting(idcabang, namacabang, nopolisi, uniqexisting) AS (
    SELECT a2.idcabang, a2.namacabang, a2.nopolisi, 
	CASE
	WHEN SUM(a2.m1s36)>0::NUMERIC THEN 1::NUMERIC
	ELSE 0
	END AS existing
    FROM source a2
    GROUP BY a2.idcabang, a2.namacabang, a2.nopolisi
),
trxtotaltransaksi(idcabang, namacabang, nopolisi, uniqtotaltransaksi) AS (
    SELECT a2.idcabang, a2.namacabang, a2.nopolisi, 
	CASE
	WHEN SUM(a2.mtd_h_1)>0::NUMERIC THEN 1::NUMERIC
	ELSE 0
	END AS totaltransaksiuniq
    FROM source a2
    GROUP BY a2.idcabang, a2.namacabang, a2.nopolisi
),
trxm1(idcabang,namacabang,nopolisi,uniqtrxm1) as (
	SELECT a2.idcabang,a2.namacabang,a2.nopolisi,
	CASE
		WHEN sum(result_m01.month1)>0::numeric THEN 1::numeric
		WHEN sum(result_m01.month1)=0::numeric THEN 0::numeric
		ELSE 0::NUMERIC
	END AS uniqtrxm1
	FROM source a2
	join result_m01 on result_m01.idcabang = a2.idcabang and result_m01.namacabang = a2.namacabang and result_m01.nopolisi = a2.nopolisi
	GROUP BY a2.idcabang,a2.namacabang,a2.nopolisi
),
trxm2(idcabang,namacabang,nopolisi,uniqtrxm2) as (
	SELECT a2.idcabang,a2.namacabang,a2.nopolisi,
	CASE
		WHEN sum(result_m02.month2)>0::numeric THEN 1::numeric
		WHEN sum(result_m02.month2)=0::numeric THEN 0::numeric
		ELSE 0::NUMERIC
	END AS uniqtrxm2
	FROM source a2
	join result_m02 on result_m02.idcabang = a2.idcabang and result_m02.namacabang = a2.namacabang and result_m02.nopolisi = a2.nopolisi
	GROUP BY a2.idcabang,a2.namacabang,a2.nopolisi
),
trxm3(idcabang,namacabang,nopolisi,uniqtrxm3) as (
	SELECT a2.idcabang,a2.namacabang,a2.nopolisi,
	CASE
		WHEN sum(result_m03.month3)>0::numeric THEN 1::numeric
		WHEN sum(result_m03.month3)=0::numeric THEN 0::numeric
		ELSE 0::NUMERIC
	END AS uniqtrxm3
	FROM source a2
	join result_m03 on result_m03.idcabang = a2.idcabang and result_m03.namacabang = a2.namacabang and result_m03.nopolisi = a2.nopolisi
	GROUP BY a2.idcabang,a2.namacabang,a2.nopolisi
),
trxm4(idcabang,namacabang,nopolisi,uniqtrxm4) as (
	SELECT a2.idcabang,a2.namacabang,a2.nopolisi,
	CASE
		WHEN sum(result_m04.month4)>0::numeric THEN 1::numeric
		WHEN sum(result_m04.month4)=0::numeric THEN 0::numeric
		ELSE 0::NUMERIC
	END AS uniqtrxm4
	FROM source a2
	join result_m04 on result_m04.idcabang = a2.idcabang and result_m04.namacabang = a2.namacabang and result_m04.nopolisi = a2.nopolisi
	GROUP BY a2.idcabang,a2.namacabang,a2.nopolisi
),
trxm5(idcabang,namacabang,nopolisi,uniqtrxm5) as (
	SELECT a2.idcabang,a2.namacabang,a2.nopolisi,
	CASE
		WHEN sum(result_m05.month5)>0::numeric THEN 1::numeric
		WHEN sum(result_m05.month5)=0::numeric THEN 0::numeric
		ELSE 0::NUMERIC
	END AS uniqtrxm5
	FROM source a2
	join result_m05 on result_m05.idcabang = a2.idcabang and result_m05.namacabang = a2.namacabang and result_m05.nopolisi = a2.nopolisi
	GROUP BY a2.idcabang,a2.namacabang,a2.nopolisi
),
trxm6(idcabang,namacabang,nopolisi,uniqtrxm6) as (
	SELECT a2.idcabang,a2.namacabang,a2.nopolisi,
	CASE
		WHEN sum(result_m06.month6)>0::numeric THEN 1::numeric
		WHEN sum(result_m06.month6)=0::numeric THEN 0::numeric
		ELSE 0::NUMERIC
	END AS uniqtrxm6
	FROM source a2
	join result_m06 on result_m06.idcabang = a2.idcabang and result_m06.namacabang = a2.namacabang and result_m06.nopolisi = a2.nopolisi
	GROUP BY a2.idcabang,a2.namacabang,a2.nopolisi
)
,
trxm7s12(idcabang,namacabang,nopolisi,uniqtrxm7s12) as (
	SELECT a2.idcabang,a2.namacabang,a2.nopolisi,
	CASE
		WHEN sum(result_m7s12.m7s12)>0::numeric THEN 1::numeric
		WHEN sum(result_m7s12.m7s12)=0::numeric THEN 0::numeric
		ELSE 0::NUMERIC
	END AS uniqtrxm7s12
	FROM source a2
	join result_m7s12 on result_m7s12.idcabang = a2.idcabang and result_m7s12.namacabang = a2.namacabang and result_m7s12.nopolisi = a2.nopolisi
	GROUP BY a2.idcabang,a2.namacabang,a2.nopolisi
)
insert into public.car_base_nopol(namacabang,nopolisi)
	select 
	source.namacabang, source.nopolisi
	from source
	left join result_m01 on result_m01.idcabang = source.idcabang and result_m01.namacabang = source.namacabang and result_m01.nopolisi = source.nopolisi
	left join result_m02 on result_m02.idcabang = source.idcabang and result_m02.namacabang = source.namacabang and result_m02.nopolisi = source.nopolisi
	left join result_m03 on result_m03.idcabang = source.idcabang and result_m03.namacabang = source.namacabang and result_m03.nopolisi = source.nopolisi
	left join result_m04 on result_m04.idcabang = source.idcabang and result_m04.namacabang = source.namacabang and result_m04.nopolisi = source.nopolisi
	left join result_m05 on result_m05.idcabang = source.idcabang and result_m05.namacabang = source.namacabang and result_m05.nopolisi = source.nopolisi
	left join result_m06 on result_m06.idcabang = source.idcabang and result_m06.namacabang = source.namacabang and result_m06.nopolisi = source.nopolisi
	left join result_m7s12 on result_m7s12.idcabang = source.idcabang and result_m7s12.namacabang = source.namacabang and result_m7s12.nopolisi = source.nopolisi
	left join result_m13s18 on result_m13s18.idcabang = source.idcabang and result_m13s18.namacabang = source.namacabang and result_m13s18.nopolisi = source.nopolisi
	left join result_m19s24 on result_m19s24.idcabang = source.idcabang and result_m19s24.namacabang = source.namacabang and result_m19s24.nopolisi = source.nopolisi
	left join result_m25s36 on result_m25s36.idcabang = source.idcabang and result_m25s36.namacabang = source.namacabang and result_m25s36.nopolisi = source.nopolisi
	LEFT JOIN trxnew on trxnew.idcabang = source.idcabang and trxnew.namacabang = source.namacabang and trxnew.nopolisi = source.nopolisi
	LEFT JOIN trxexisting on trxexisting.idcabang = source.idcabang and trxexisting.namacabang = source.namacabang and trxexisting.nopolisi = source.nopolisi
	LEFT JOIN trxtotaltransaksi on trxtotaltransaksi.idcabang = source.idcabang and trxtotaltransaksi.namacabang = source.namacabang and trxtotaltransaksi.nopolisi = source.nopolisi
--	where source.nopolisi in ('A3124TS','A3127CT')
	group by source.idcabang,source.namacabang,source.nopolisi
