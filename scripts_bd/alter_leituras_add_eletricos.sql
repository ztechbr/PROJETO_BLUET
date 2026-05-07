-- Migração: adiciona campos de comunicação / elétricos (instalações já existentes).
ALTER TABLE public.leituras
    ADD COLUMN IF NOT EXISTS scomunicacao REAL DEFAULT -9999,
    ADD COLUMN IF NOT EXISTS stensao REAL DEFAULT -9999,
    ADD COLUMN IF NOT EXISTS scorrente REAL DEFAULT -9999,
    ADD COLUMN IF NOT EXISTS spotencia REAL DEFAULT -9999,
    ADD COLUMN IF NOT EXISTS ref_rssi_dbm REAL DEFAULT -9999,
    ADD COLUMN IF NOT EXISTS rec_rssi_dbm REAL DEFAULT -9999,
    ADD COLUMN IF NOT EXISTS fator_n REAL DEFAULT -9999,
    ADD COLUMN IF NOT EXISTS distcalc_app REAL DEFAULT -9999,
    ADD COLUMN IF NOT EXISTS codsensor VARCHAR(20);
