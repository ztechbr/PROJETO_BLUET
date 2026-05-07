from __future__ import annotations

from datetime import date, datetime, time

import psycopg2
from flasgger import swag_from
from flask import Blueprint, jsonify, request
from psycopg2 import errors as pg_errors

from database import get_connection
from leituras_query import ConsultaLeiturasError, consulta_leituras_desde_strings


rest_bp = Blueprint("rest", __name__)


# Exemplo JSON completo para POST /leituras (Swagger UI / testes).
_SWAGGER_POST_LEITURA_EXAMPLE = {
    "codplantacao": "PLANTDEMO",
    "codleitura": "LEIT-EXEMPLO-001",
    "codsensor": "SENS01",
    "lat": -23.2101,
    "lon": -50.1234,
    "dataleit": "2026-05-07",
    "horaleit": "14:30:00",
    "temp_solo": 22.5,
    "temp_ar": 24.0,
    "umid_solo": 55.0,
    "umid_ar": 68.0,
    "luz": 450.0,
    "chuva": 0.0,
    "umid_folha": 71.0,
    "scomunicacao": 2,
    "stensao": 3.7,
    "scorrente": 0.12,
    "spotencia": 0.444,
    "ref_rssi_dbm": -65.0,
    "rec_rssi_dbm": -72.0,
    "fator_n": 2.0,
    "distcalc_app": 12.34,
    "status_blockchain": "PENDENTE",
    "hash_blockchain": None,
    "tx_hash": None,
}


swagger_template = {
    "swagger": "2.0",
    "info": {
        "title": "Servidor API — BlueSensores (UTFPR)",
        "description": (
            "Projeto BlueSensores — recebe leituras em JSON e persiste na tabela `leituras`. "
            "Consulta SOAP 1.1 (mesmos filtros): `/soap/?wsdl`; GET `/soap?format=json|xml` com "
            "filtros (sem API_TOKEN). "
            "Com `API_TOKEN` configurado no servidor, GET e POST `/leituras` exigem "
            "`Authorization: Bearer <token>`, só `<token>` no mesmo cabeçalho (Swagger UI), ou `X-API-Key`."
        ),
        "version": "1.0.0",
    },
    "tags": [{"name": "leituras", "description": "Operações de leitura"}],
    "securityDefinitions": {
        "ApiKeyAuth": {
            "type": "apiKey",
            "name": "Authorization",
            "in": "header",
            "description": (
                "Digite o valor de API_TOKEN ou `Bearer ` + token. "
                "O Swagger envia apenas o texto no header Authorization; "
                "alternativa: `X-API-Key`."
            ),
        }
    },
}


def _parse_date(value):
    if isinstance(value, date) and not isinstance(value, datetime):
        return value
    if isinstance(value, str):
        return date.fromisoformat(value)
    raise ValueError("dataleit inválida")


def _parse_time(value):
    if isinstance(value, time):
        return value
    if isinstance(value, str):
        parts = value.strip().split(":")
        h, m = int(parts[0]), int(parts[1])
        s = int(parts[2]) if len(parts) > 2 else 0
        return time(h, m, s)
    raise ValueError("horaleit inválida")


@rest_bp.route("/health", methods=["GET"])
def health():
    return jsonify({"status": "ok"})


@rest_bp.route("/leituras", methods=["GET"])
@swag_from(
    {
        "tags": ["leituras"],
        "summary": "Lista leituras com filtros",
        "description": (
            "Filtra por `codplantacao` e/ou período em `dataleit`. "
            "É obrigatório informar pelo menos um filtro.\n\n"
            "**Exemplo de consulta (todos os parâmetros):** "
            "`?codplantacao=PLANTDEMO&dataleit_inicio=2026-05-01&dataleit_fim=2026-05-07&limit=50&offset=0`"
        ),
        "parameters": [
            {
                "name": "codplantacao",
                "in": "query",
                "type": "string",
                "required": False,
                "description": "Filtra pela plantação. Ex.: PLANTDEMO",
                "default": "PLANTDEMO",
            },
            {
                "name": "dataleit_inicio",
                "in": "query",
                "type": "string",
                "format": "date",
                "required": False,
                "description": "Início inclusive (YYYY-MM-DD). Ex.: 2026-05-01",
                "default": "2026-05-01",
            },
            {
                "name": "dataleit_fim",
                "in": "query",
                "type": "string",
                "format": "date",
                "required": False,
                "description": "Fim inclusive (YYYY-MM-DD). Ex.: 2026-05-07",
                "default": "2026-05-07",
            },
            {
                "name": "limit",
                "in": "query",
                "type": "integer",
                "required": False,
                "default": 100,
                "description": "Máximo de registros (1–500). Ex.: 50",
                "maximum": 500,
                "minimum": 1,
            },
            {
                "name": "offset",
                "in": "query",
                "type": "integer",
                "required": False,
                "default": 0,
                "description": "Deslocamento de paginação. Ex.: 0",
                "minimum": 0,
            },
        ],
        "responses": {
            "200": {"description": "Lista de leituras"},
            "400": {"description": "Parâmetros inválidos ou nenhum filtro informado"},
            "401": {"description": "API_TOKEN configurado e token ausente ou inválido"},
            "500": {"description": "Erro interno ou falha de conexão com o banco"},
        },
        "security": [{"ApiKeyAuth": []}],
    }
)
def listar_leituras():
    cod = request.args.get("codplantacao", type=str)
    d_ini_raw = request.args.get("dataleit_inicio")
    d_fim_raw = request.args.get("dataleit_fim")
    limit = request.args.get("limit", default=100, type=int) or 100
    offset = request.args.get("offset", default=0, type=int) or 0

    try:
        payload = consulta_leituras_desde_strings(
            codplantacao_raw=cod,
            dataleit_inicio_raw=d_ini_raw,
            dataleit_fim_raw=d_fim_raw,
            limit=limit,
            offset=offset,
        )
    except ConsultaLeiturasError as e:
        body = {"error": e.message}
        if e.http_status >= 500:
            body["detail"] = e.detail
        return jsonify(body), e.http_status

    return jsonify(payload)


_POST_LEITURA_SWAGGER_SCHEMA = {
    "type": "object",
    "required": [
        "codplantacao",
        "codleitura",
        "lat",
        "lon",
        "dataleit",
        "horaleit",
    ],
    "properties": {
        "codplantacao": {"type": "string", "maxLength": 30, "description": "Código da plantação (obrigatório)."},
        "codleitura": {"type": "string", "maxLength": 50, "description": "Identificador da leitura (obrigatório)."},
        "codsensor": {"type": "string", "maxLength": 20, "description": "Código do sensor (opcional). Aliases: `codSensor`.", "x-nullable": True},
        "lat": {"type": "number", "description": "Latitude (obrigatório)."},
        "lon": {"type": "number", "description": "Longitude (obrigatório)."},
        "dataleit": {"type": "string", "format": "date", "description": "Data da leitura YYYY-MM-DD (obrigatório)."},
        "horaleit": {"type": "string", "description": "Hora HH:MM ou HH:MM:SS (obrigatório)."},
        "temp_solo": {"type": "number", "description": "Temperatura do solo (-9999 = ausente)."},
        "temp_ar": {"type": "number", "description": "Temperatura do ar (-9999 = ausente)."},
        "umid_solo": {"type": "number", "description": "Umidade do solo (-9999 = ausente)."},
        "umid_ar": {"type": "number", "description": "Umidade do ar (-9999 = ausente)."},
        "luz": {"type": "number", "description": "Luminosidade (-9999 = ausente)."},
        "chuva": {"type": "number", "description": "Chuva (-9999 = ausente)."},
        "umid_folha": {"type": "number", "description": "Umidade de folha (-9999 = ausente)."},
        "scomunicacao": {
            "type": "number",
            "description": "Tipo de Bluetooth: 0 ND, 1 BTLowPower, 2 BTNormal (-9999 = ausente).",
        },
        "stensao": {"type": "number", "description": "Tensão (-9999 = ausente)."},
        "scorrente": {"type": "number", "description": "Corrente (-9999 = ausente)."},
        "spotencia": {"type": "number", "description": "Potência (-9999 = ausente)."},
        "ref_rssi_dbm": {"type": "number", "description": "RSSI referência em dBm. Aliases: `RefRSSIdBm`.", "x-nullable": True},
        "rec_rssi_dbm": {"type": "number", "description": "RSSI recebido em dBm. Aliases: `RecRSSIdBm`.", "x-nullable": True},
        "fator_n": {"type": "number", "description": "Fator N. Aliases: `FatorN`.", "x-nullable": True},
        "distcalc_app": {"type": "number", "description": "Distância estimada pela APP. Aliases: `DistCalcAPP`.", "x-nullable": True},
        "status_blockchain": {
            "type": "string",
            "enum": ["PENDENTE", "ENVIADO", "CONFIRMADO"],
            "description": "Estado blockchain (padrão PENDENTE).",
        },
        "hash_blockchain": {"type": "string", "description": "Hash on-chain (opcional).", "x-nullable": True},
        "tx_hash": {"type": "string", "description": "Hash da transação (opcional).", "x-nullable": True},
        "criadoem": {
            "type": "string",
            "format": "date-time",
            "description": "Carimbo de criação ISO 8601 (opcional; senão usa o servidor/DB).",
            "x-nullable": True,
        },
    },
    "example": _SWAGGER_POST_LEITURA_EXAMPLE,
}


@rest_bp.route("/leituras", methods=["POST"])
@swag_from(
    {
        "tags": ["leituras"],
        "summary": "Insere uma leitura",
        "description": (
            "Corpo JSON com campos obrigatórios e opcionais. "
            "Aliases camelCase aceitos: `RefRSSIdBm`, `RecRSSIdBm`, `FatorN`, `DistCalcAPP`, `codSensor` "
            "(equivalentes a `ref_rssi_dbm`, `rec_rssi_dbm`, `fator_n`, `distcalc_app`, `codsensor`).\n\n"
            "**Após clicar em Execute:** Role a página até a seção **Responses** dentro desta mesma "
            "operação — lá aparecem o **código HTTP** (ex.: 201) e o **Response body** em JSON."
        ),
        "consumes": ["application/json"],
        "parameters": [
            {
                "name": "body",
                "in": "body",
                "required": True,
                "schema": _POST_LEITURA_SWAGGER_SCHEMA,
            }
        ],
        "responses": {
            "201": {
                "description": "Leitura inserida (confira Response body logo abaixo do botão Execute).",
                "schema": {
                    "type": "object",
                    "properties": {
                        "message": {"type": "string", "example": "Leitura inserida"},
                        "hash_pk": {
                            "type": "string",
                            "example": "a1b2c3d4e5f6708091a2b3c4d5e6f70891a2b3c",
                            "description": "SHA-256 hex (PRIMARY KEY)",
                        },
                    },
                },
            },
            "400": {
                "description": "JSON inválido ou campos obrigatórios ausentes",
                "schema": {
                    "type": "object",
                    "properties": {
                        "error": {"type": "string"},
                        "missing": {"type": "array", "items": {"type": "string"}},
                    },
                },
            },
            "401": {"description": "API_TOKEN configurado e token ausente ou inválido"},
            "409": {"description": "Conflito de chave primária (leitura duplicada)"},
            "500": {"description": "Erro interno ou falha de conexão com o banco"},
        },
        "security": [{"ApiKeyAuth": []}],
    }
)
def criar_leitura():
    if not request.is_json:
        return jsonify({"error": "Envie Content-Type: application/json"}), 400
    data = request.get_json(silent=True)
    if data is None:
        return jsonify({"error": "Corpo JSON inválido"}), 400

    # aliases camelCase
    alias_to_key = {
        "RefRSSIdBm": "ref_rssi_dbm",
        "RecRSSIdBm": "rec_rssi_dbm",
        "FatorN": "fator_n",
        "DistCalcAPP": "distcalc_app",
        "codSensor": "codsensor",
    }
    for alias, key in alias_to_key.items():
        if alias in data and key not in data:
            data[key] = data.get(alias)

    required = ("codplantacao", "codleitura", "lat", "lon", "dataleit", "horaleit")
    missing = [k for k in required if k not in data]
    if missing:
        return jsonify({"error": "Campos obrigatórios ausentes", "missing": missing}), 400

    try:
        d_leit = _parse_date(data["dataleit"])
        h_leit = _parse_time(data["horaleit"])
    except (ValueError, TypeError) as e:
        return jsonify({"error": str(e)}), 400

    optional_float_keys = (
        "temp_solo",
        "temp_ar",
        "umid_solo",
        "umid_ar",
        "luz",
        "chuva",
        "umid_folha",
        "scomunicacao",
        "stensao",
        "scorrente",
        "spotencia",
        "ref_rssi_dbm",
        "rec_rssi_dbm",
        "fator_n",
        "distcalc_app",
    )
    floats = {}
    for key in optional_float_keys:
        if key in data and data[key] is not None:
            try:
                floats[key] = float(data[key])
            except (TypeError, ValueError):
                return jsonify({"error": f"Campo '{key}' deve ser numérico"}), 400

    status_blockchain = data.get("status_blockchain", "PENDENTE")
    if status_blockchain not in ("PENDENTE", "ENVIADO", "CONFIRMADO"):
        return jsonify({"error": "status_blockchain inválido"}), 400

    codsensor = data.get("codsensor")
    hash_blockchain = data.get("hash_blockchain")
    tx_hash = data.get("tx_hash")
    criadoem = data.get("criadoem")

    insert_sql_base = """
        INSERT INTO public.leituras (
            codplantacao,
            codleitura,
            codsensor,
            lat,
            lon,
            dataleit,
            horaleit,
            temp_solo,
            temp_ar,
            umid_solo,
            umid_ar,
            luz,
            chuva,
            umid_folha,
            scomunicacao,
            stensao,
            scorrente,
            spotencia,
            ref_rssi_dbm,
            rec_rssi_dbm,
            fator_n,
            distcalc_app,
            status_blockchain,
            hash_blockchain,
            tx_hash{extra_cols}
        )
        VALUES (
            %(codplantacao)s,
            %(codleitura)s,
            %(codsensor)s,
            %(lat)s,
            %(lon)s,
            %(dataleit)s,
            %(horaleit)s,
            %(temp_solo)s,
            %(temp_ar)s,
            %(umid_solo)s,
            %(umid_ar)s,
            %(luz)s,
            %(chuva)s,
            %(umid_folha)s,
            %(scomunicacao)s,
            %(stensao)s,
            %(scorrente)s,
            %(spotencia)s,
            %(ref_rssi_dbm)s,
            %(rec_rssi_dbm)s,
            %(fator_n)s,
            %(distcalc_app)s,
            %(status_blockchain)s,
            %(hash_blockchain)s,
            %(tx_hash)s{extra_vals}
        )
        RETURNING hash_pk
    """

    params = {
        "codplantacao": str(data["codplantacao"])[:30],
        "codleitura": str(data["codleitura"])[:50],
        "codsensor": str(codsensor)[:20] if codsensor not in (None, "") else None,
        "lat": float(data["lat"]),
        "lon": float(data["lon"]),
        "dataleit": d_leit,
        "horaleit": h_leit,
        "temp_solo": floats.get("temp_solo", -9999),
        "temp_ar": floats.get("temp_ar", -9999),
        "umid_solo": floats.get("umid_solo", -9999),
        "umid_ar": floats.get("umid_ar", -9999),
        "luz": floats.get("luz", -9999),
        "chuva": floats.get("chuva", -9999),
        "umid_folha": floats.get("umid_folha", -9999),
        "scomunicacao": floats.get("scomunicacao", -9999),
        "stensao": floats.get("stensao", -9999),
        "scorrente": floats.get("scorrente", -9999),
        "spotencia": floats.get("spotencia", -9999),
        "ref_rssi_dbm": floats.get("ref_rssi_dbm", -9999),
        "rec_rssi_dbm": floats.get("rec_rssi_dbm", -9999),
        "fator_n": floats.get("fator_n", -9999),
        "distcalc_app": floats.get("distcalc_app", -9999),
        "status_blockchain": status_blockchain,
        "hash_blockchain": hash_blockchain,
        "tx_hash": tx_hash,
    }

    if criadoem is not None:
        if isinstance(criadoem, str):
            params["criadoem"] = datetime.fromisoformat(criadoem.replace("Z", "+00:00"))
        else:
            params["criadoem"] = criadoem
        insert_sql = insert_sql_base.format(
            extra_cols=",\n            criadoem",
            extra_vals=",\n            %(criadoem)s",
        )
    else:
        insert_sql = insert_sql_base.format(extra_cols="", extra_vals="")

    try:
        conn = get_connection()
    except Exception as e:
        return jsonify({"error": "Falha ao conectar ao banco", "detail": str(e)}), 500

    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute(insert_sql, params)
                row = cur.fetchone()
        hash_pk = row[0] if row else None
    except pg_errors.UniqueViolation:
        conn.rollback()
        return jsonify({"error": "Leitura já existe (mesmo hash_pk / chave duplicada)"}), 409
    except psycopg2.Error as e:
        conn.rollback()
        return jsonify({"error": "Erro ao inserir", "detail": str(e)}), 500
    finally:
        conn.close()

    return jsonify({"message": "Leitura inserida", "hash_pk": hash_pk}), 201

