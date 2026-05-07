from __future__ import annotations


def allowed_numeric_cols():
    # (nome, tem_sentinela_-9999)
    return [
        ("lat", False),
        ("lon", False),
        ("temp_solo", True),
        ("temp_ar", True),
        ("umid_solo", True),
        ("umid_ar", True),
        ("luz", True),
        ("chuva", True),
        ("umid_folha", True),
        ("scomunicacao", True),
        ("stensao", True),
        ("scorrente", True),
        ("spotencia", True),
        ("ref_rssi_dbm", True),
        ("rec_rssi_dbm", True),
        ("fator_n", True),
        ("distcalc_app", True),
    ]

