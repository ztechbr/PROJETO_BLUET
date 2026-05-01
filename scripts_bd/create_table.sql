CREATE TABLE leituras (

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