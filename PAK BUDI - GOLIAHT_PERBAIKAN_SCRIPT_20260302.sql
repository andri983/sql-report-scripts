SELECT insert_date, namacabang, notelp, kolom_b, kolom_c, kolom_d, kolom_e, kolom_f, kolom_g, kolom_h, kolom_i, kolom_j, kolom_k, kolom_l, kolom_m, kolom_n, kolom_o, kolom_p, kolom_q, kolom_r, kolom_s, kolom_t, kolom_u, kolom_v, kolom_w, kolom_x, kolom_y, kolom_z, kolom_aa, kolom_ab, kolom_ac, kolom_ad, kolom_ae, kolom_af, kolom_ag, kolom_ah, kolom_ai, kolom_aj, kolom_ak, kolom_al, kolom_am, wa_status, send_date, xid, wa_status_data, tglakhirvoucher, nomorserivoucher
FROM public.smi_trx_oil_goliaht_his where namacabang='Jakarta Baru' and kolom_d='2026-03-02' limit 1;

select * from
smi_trx_oil_goliaht_monitoring_all
where namacabang='Jakarta Baru' and kolom_d='2026-03-02' limit 1;

select distinct namacabang from
smi_trx_oil_goliaht_monitoring_all
where kolom_d='2026-03-02';

select * from public.fc_smi_trx_oil_goliaht_monitoring_jkt();

select * from smi_trx_oil_goliaht_monitoring_jkt;

insert into public.smi_trx_oil_goliaht_nopolisi
SELECT distinct kolom_c
FROM public.smi_trx_oil_goliaht_his 
where kolom_ah='0'

-- 1. Membuat Tabel
CREATE TABLE public.smi_trx_oil_goliaht_nopolisi (
    -- Primary Key otomatis mencegah duplikat dan null
    nopolisi VARCHAR(15) PRIMARY KEY,
    
    -- Opsional: Tambahkan kolom timestamp untuk audit trail
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 2. Tambahkan Index untuk mempercepat pencarian (Opsional, PK sudah otomatis membuat index)
CREATE INDEX idx_nopolisi_goliath ON public.smi_trx_oil_goliaht_nopolisi (nopolisi);


select * from public.smi_trx_oil_goliaht_nopolisi;