-- Transaksi BAN tanpa OLI dan tanpa SERVICE
SELECT a.tanggal as tgltransaksi, 
current_date AS tglreport,
current_date+30 AS tglakhirvoucher,
'' AS nomorserivoucher,
a.namacabang, a.kodetoko, a.nomortransaksi, a.brand, a.category, a.idproduk, a.kodeproduk, a.namapanjang, a.qty, a.subtotal, a.nopolisi, b.namamember, b.notelp, a.idjenismember, a.namajenismember
FROM public.smi_rms15_transaksi_toko_perjenis_member_v3 a
LEFT JOIN public.mstmember b ON b.nopolisi=a.nopolisi
LEFT JOIN public.smi_mst_oli oli ON oli.kodeproduk = a.kodeproduk
LEFT JOIN public.smi_mst_servis serv ON serv.kodeproduk = a.kodeproduk
--WHERE a.tanggal BETWEEN '2025-07-02' AND '2025-07-02'
WHERE a.tanggal=current_date-1
GROUP BY a.tanggal, a.namacabang, a.kodetoko, a.nomortransaksi, a.brand, a.category, a.idproduk, a.kodeproduk, a.namapanjang, a.qty, a.subtotal, a.nopolisi, b.namamember, b.notelp, a.idjenismember, a.namajenismember
HAVING
    SUM(CASE WHEN a.iddivisi = 1 THEN 1 ELSE 0 END) > 0 AND
    SUM(CASE WHEN oli.kodeproduk IS NOT NULL THEN 1 ELSE 0 END) = 0 AND
    SUM(CASE WHEN serv.kodeproduk IS NOT NULL THEN 1 ELSE 0 END) = 0
;

SELECT *
FROM public.smi_rms22_transaksi_toko_perjenis_member_v3
where kodetoko='3051002' and nomortransaksi='202507160002'

--TRANSAKSI BAN
SELECT a.kodetoko,a.nomortransaksi
FROM public.smi_rms22_transaksi_toko_perjenis_member_v3 a
WHERE a.tanggal BETWEEN '2025-07-02' AND '2025-07-02'
AND a.iddivisi = 1;

--TRANSAKSI OLI
SELECT a.kodetoko,a.nomortransaksi
FROM public.smi_rms22_transaksi_toko_perjenis_member_v3 a
join public.smi_mst_oli b on b.kodeproduk=a.kodeproduk
WHERE a.tanggal BETWEEN '2025-07-02' AND '2025-07-02';

--TRANSAKSI SERVICE
SELECT a.kodetoko,a.nomortransaksi
FROM public.smi_rms22_transaksi_toko_perjenis_member_v3 a
join public.smi_mst_servis b on b.kodeproduk=a.kodeproduk
WHERE a.tanggal BETWEEN '2025-07-02' AND '2025-07-02';


----TABLE VOUCHER TRANSFER DARI RMS01
CREATE TABLE public.smimstvoucherdiskonpersen (
	insertdate timestamptz DEFAULT now() NOT NULL,
    idvoucher INT8 PRIMARY KEY,
    idgroupvoucher INT4 NOT NULL,
    kodejenisvoucher CHAR(1) NOT NULL,
    idmarchant INT4 NOT NULL,
    nomorserivoucher VARCHAR(50) NOT NULL,
    tglaktif DATE NOT NULL,
    tglakhir DATE NOT NULL,
    iduser VARCHAR(20) NOT NULL,
    tglcreate TIMESTAMP NOT NULL,
    tglupdate TIMESTAMP NOT NULL,
    flagaktif INT4 NOT NULL,
    statusdata INT4 NOT null,
    statuskirim INT DEFAULT 0
);
CREATE INDEX idx_voucher_idvoucher ON public.smimstvoucherdiskonpersen(idvoucher);
CREATE INDEX idx_voucher_nomorserivoucher ON public.smimstvoucherdiskonpersen(nomorserivoucher);

--select * from public.smimstvoucherdiskonpersen;
--update public.smimstvoucherdiskonpersen set statuskirim=0;

CREATE TABLE public.smimstvoucherpersen (
	insertdate timestamptz DEFAULT now() NOT NULL,
    idvoucher INT8 NOT NULL,
    idgroupvoucher INT4 NOT NULL,
    kodejenisvoucher CHAR(1) NOT NULL,
    idmarchant INT4 NOT NULL,
    nomorserivoucher VARCHAR(50) NOT NULL,
    tglaktif DATE NOT NULL,
    tglakhir DATE NOT NULL,
    iduser VARCHAR(20) NOT NULL,
    tglcreate TIMESTAMP NOT NULL,
    tglupdate TIMESTAMP NOT NULL,
    flagaktif INT4 NOT NULL,
    statusdata INT4 NOT NULL,
    orderid TEXT,  -- TEXT digunakan sebagai pengganti VARCHAR(MAX)
    PRIMARY KEY (nomorerivoucher)
);
CREATE INDEX idx_voucher_idvoucher ON public.smimstvoucherpersen(idvoucher);
CREATE INDEX idx_voucher_nomorserivoucher ON public.smimstvoucherpersen(nomorserivoucher);

CREATE TABLE public.smimstvoucherdiskon (
	insertdate timestamptz DEFAULT now() NOT NULL,
    idvoucher INT8 PRIMARY KEY,
    idgroupvoucher INT4 NOT NULL,
    kodejenisvoucher CHAR(1) NOT NULL,
    idmarchant INT4 NOT NULL,
    nomorserivoucher VARCHAR(50) NOT NULL,
    tglaktif DATE NOT NULL,
    tglakhir DATE NOT NULL,
    iduser VARCHAR(20) NOT NULL,
    tglcreate TIMESTAMP NOT NULL,
    tglupdate TIMESTAMP NOT NULL,
    flagaktif INT4 NOT NULL,
    statusdata INT4 NOT NULL
);
CREATE INDEX idx_voucher_idvoucher ON public.smimstvoucherdiskon(idvoucher);
CREATE INDEX idx_voucher_nomorserivoucher ON public.smimstvoucherdiskon(nomorserivoucher);


CREATE TABLE public.smimstvoucher (
	insertdate timestamptz DEFAULT now() NOT NULL,
    idvoucher INT8 PRIMARY KEY,
    idgroupvoucher INT4 NOT NULL,
    kodejenisvoucher CHAR(1) NOT NULL,
    idmarchant INT4 NOT NULL,
    nomorserivoucher VARCHAR(50) NOT NULL,
    nilairpvoucher NUMERIC(18,2) NOT NULL,
    tglaktif DATE NOT NULL,
    tglakhir DATE NOT NULL,
    iduser VARCHAR(20) NOT NULL,
    tglcreate TIMESTAMP NOT NULL,
    tglupdate TIMESTAMP NOT NULL,
    flagaktif INT4 NOT NULL,
    statusdata INT4 NOT NULL,
    orderid TEXT,
    CONSTRAINT fk_smimstvoucher_mstjenisvoucher
        FOREIGN KEY (kodejenisvoucher) REFERENCES public.mstjenisvoucher (kodejenisvoucher),
    CONSTRAINT fk_smimstvoucher_mstmarchantvoucher
        FOREIGN KEY (idmarchant) REFERENCES public.mstmarchantvoucher (idmarchant)
);
CREATE INDEX idx_smimstvoucher_kodejenisvoucher ON public.smimstvoucher(kodejenisvoucher);
CREATE INDEX idx_smimstvoucher_idmarchant ON public.smimstvoucher(idmarchant);








========
--------
========

------------------------------
CREATE TABLE IF NOT EXISTS public.car_his (
  tgltransaksi DATE,
  tglreport DATE,
  tglakhirvoucher DATE,
  nomorserivoucher VARCHAR(50),
  namacabang VARCHAR(40),
  kodetoko INT8,
  nomortransaksi INT8,
  brand VARCHAR(30),
  category VARCHAR(30),
  idproduk INT4,
  kodeproduk VARCHAR(10),
  namapanjang VARCHAR(100),
  qty NUMERIC(18,2),
  subtotal NUMERIC(18,2),
  nopolisi VARCHAR(10),
  namamember VARCHAR(30),
  notelp VARCHAR(15),
  idjenismember INT4,
  namajenismember VARCHAR(30),
  wa_status int4 DEFAULT 0 NULL,
  send_date timestamp NULL,
  xid varchar(150) NULL
);
------------------------------

WITH transaksi AS (
  SELECT 
    *,
    ROW_NUMBER() OVER (ORDER BY tgltransaksi, nomortransaksi) AS rn
  FROM (
    SELECT 
        a.tanggal AS tgltransaksi, 
        current_date AS tglreport,
        current_date + 30 AS tglakhirvoucher,
        '' AS nomorserivoucher,
        a.namacabang, a.kodetoko, a.nomortransaksi, a.brand, a.category, 
        a.idproduk, a.kodeproduk, a.namapanjang, a.qty, a.subtotal, a.nopolisi,
        b.namamember, b.notelp, a.idjenismember, a.namajenismember
    FROM public.smi_rms15_transaksi_toko_perjenis_member_v3 a
    LEFT JOIN public.mstmember b ON b.nopolisi = a.nopolisi
    LEFT JOIN public.smi_mst_oli oli ON oli.kodeproduk = a.kodeproduk
    LEFT JOIN public.smi_mst_servis serv ON serv.kodeproduk = a.kodeproduk
    WHERE a.tanggal = current_date - 1
    GROUP BY 
        a.tanggal, a.namacabang, a.kodetoko, a.nomortransaksi, a.brand,
        a.category, a.idproduk, a.kodeproduk, a.namapanjang, a.qty, a.subtotal,
        a.nopolisi, b.namamember, b.notelp, a.idjenismember, a.namajenismember
    HAVING
        SUM(CASE WHEN a.iddivisi = 1 THEN 1 ELSE 0 END) > 0 AND
        SUM(CASE WHEN oli.kodeproduk IS NOT NULL THEN 1 ELSE 0 END) = 0 AND
        SUM(CASE WHEN serv.kodeproduk IS NOT NULL THEN 1 ELSE 0 END) = 0
  ) AS sub
),
voucher AS (
  SELECT 
    nomorserivoucher,
    ROW_NUMBER() OVER (ORDER BY tglcreate) AS rn
  FROM public.smimstvoucherdiskonpersen
  WHERE statuskirim = 0
),
final AS (
  SELECT 
    t.tgltransaksi,
    t.tglreport,
    t.tglakhirvoucher,
    v.nomorserivoucher,
    t.namacabang, t.kodetoko, t.nomortransaksi, t.brand, t.category,
    t.idproduk, t.kodeproduk, t.namapanjang, t.qty, t.subtotal, t.nopolisi,
    t.namamember, t.notelp, t.idjenismember, t.namajenismember
  FROM transaksi t
  LEFT JOIN voucher v ON t.rn = v.rn
)
INSERT INTO public.car_his (
  tgltransaksi, tglreport, tglakhirvoucher, nomorserivoucher,
  namacabang, kodetoko, nomortransaksi, brand, category,
  idproduk, kodeproduk, namapanjang, qty, subtotal, nopolisi,
  namamember, notelp, idjenismember, namajenismember
)
SELECT 
  tgltransaksi, tglreport, tglakhirvoucher, nomorserivoucher,
  namacabang, kodetoko, nomortransaksi, brand, category,
  idproduk, kodeproduk, namapanjang, qty, subtotal, nopolisi,
  namamember, notelp, idjenismember, namajenismember
FROM final
WHERE nomorserivoucher IS NOT NULL;

UPDATE public.smimstvoucherdiskonpersen
SET statuskirim = 1
WHERE nomorserivoucher IN (
  SELECT nomorserivoucher
  FROM public.car_his
--  WHERE tglreport = current_date  -- hanya update yang baru diinsert hari ini
);

select * from public.car_his;
--truncate table public.car_his
