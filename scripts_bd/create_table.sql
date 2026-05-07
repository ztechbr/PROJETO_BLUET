-- Tabela única `leituras`: schema atual (Bluetooth/elétricos, RSSI, codsensor, etc.).
-- Bancos legados criados só com o DDL antigo: use o bloco comentado no final como migração.
CREATE TABLE IF NOT EXISTS public.leituras (

    codplantacao VARCHAR(30) NOT NULL,
    codleitura VARCHAR(50) NOT NULL,

    lat DECIMAL(9,6) NOT NULL,
    lon DECIMAL(9,6) NOT NULL,

    dataleit DATE NOT NULL,
    horaleit TIME NOT NULL,

    temp_solo REAL DEFAULT -9999,
    temp_ar REAL DEFAULT -9999,

    umid_solo REAL DEFAULT -9999,
    umid_ar REAL DEFAULT -9999,

    luz REAL DEFAULT -9999,
    chuva REAL DEFAULT -9999,
    umid_folha REAL DEFAULT -9999,

    scomunicacao REAL DEFAULT -9999,
    stensao REAL DEFAULT -9999,
    scorrente REAL DEFAULT -9999,
    spotencia REAL DEFAULT -9999,

    -- Rádio / RSSI (dBm) e fatores de cálculo (APP)
    ref_rssi_dbm REAL DEFAULT -9999,
    rec_rssi_dbm REAL DEFAULT -9999,
    fator_n REAL DEFAULT -9999,
    distcalc_app REAL DEFAULT -9999,
    codsensor VARCHAR(20),

    hash_pk VARCHAR(32) GENERATED ALWAYS AS (
        md5(
            codplantacao ||
            codleitura ||
            extract(year from dataleit)::text ||
            extract(month from dataleit)::text ||
            extract(day from dataleit)::text ||
            extract(hour from horaleit)::text ||
            extract(minute from horaleit)::text ||
            extract(second from horaleit)::text
        )
    ) STORED,

    status_blockchain VARCHAR(20)
        DEFAULT 'PENDENTE'
        CHECK (status_blockchain IN ('PENDENTE', 'ENVIADO', 'CONFIRMADO')),

    hash_blockchain CHAR(64),
    tx_hash VARCHAR(100),
    criadoem TIMESTAMP DEFAULT NOW(),

    PRIMARY KEY (hash_pk)
);

-- ---------------------------------------------------------------------------
-- Migração (apenas bases que já tinham `leituras` sem estas colunas):
-- execute manualmente os ADD COLUMN aplicáveis ao seu erro; não rode em BD novo.
-- ---------------------------------------------------------------------------
-- ALTER TABLE public.leituras
--     ADD COLUMN IF NOT EXISTS scomunicacao REAL DEFAULT -9999,
--     ADD COLUMN IF NOT EXISTS stensao REAL DEFAULT -9999,
--     ADD COLUMN IF NOT EXISTS scorrente REAL DEFAULT -9999,
--     ADD COLUMN IF NOT EXISTS spotencia REAL DEFAULT -9999,
--     ADD COLUMN IF NOT EXISTS ref_rssi_dbm REAL DEFAULT -9999,
--     ADD COLUMN IF NOT EXISTS rec_rssi_dbm REAL DEFAULT -9999,
--     ADD COLUMN IF NOT EXISTS fator_n REAL DEFAULT -9999,
--     ADD COLUMN IF NOT EXISTS distcalc_app REAL DEFAULT -9999,
--     ADD COLUMN IF NOT EXISTS codsensor VARCHAR(20);