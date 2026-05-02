import os
import secrets
from datetime import date, datetime, time

import psycopg2
from psycopg2 import errors as pg_errors
from dotenv import load_dotenv
from flasgger import Swagger, swag_from
from flask import Flask, jsonify, request
from werkzeug.middleware.dispatcher import DispatcherMiddleware

from database import get_connection
from leituras_query import ConsultaLeiturasError, consulta_leituras_desde_strings
from soap_service import soap_wsgi_app

load_dotenv()

app = Flask(__name__)
app.config["JSON_SORT_KEYS"] = False

swagger_template = {
    "swagger": "2.0",
    "info": {
        "title": "Servidor API — BlueSensores (UTFPR)",
        "description": (
            "Projeto BlueSensores — recebe leituras em JSON e persiste na tabela `leituras`. "
            "Consulta SOAP 1.1 (mesmos filtros do GET `/leituras`): `/soap/?wsdl`. "
            "Com `API_TOKEN` configurado no servidor, GET e POST `/leituras` exigem "
            "`Authorization: Bearer <token>` ou `X-API-Key`."
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
                "Valor: `Bearer <API_TOKEN>` (variável de ambiente no servidor). "
                "Alternativa: cabeçalho `X-API-Key` com o mesmo segredo."
            ),
        }
    },
}
Swagger(app, template=swagger_template)


def _rest_api_token_configured():
    raw = os.getenv("API_TOKEN")
    if raw is None:
        return None
    token = raw.strip()
    return token if token else None


def _token_from_request():
    auth = request.headers.get("Authorization", "")
    if auth.lower().startswith("bearer "):
        return auth[7:].strip()
    if auth.lower().startswith("token "):
        return auth[6:].strip()
    x = request.headers.get("X-API-Key", "").strip()
    return x if x else None


@app.before_request
def _require_rest_api_token_for_leituras():
    if request.path != "/leituras":
        return None
    expected = _rest_api_token_configured()
    if not expected:
        return None
    got = _token_from_request()
    if got is None or len(got) != len(expected):
        return (
            jsonify(
                {
                    "error": "Não autorizado",
                    "detail": (
                        "Informe o token configurado em API_TOKEN: "
                        "Authorization: Bearer <token> ou cabeçalho X-API-Key"
                    ),
                }
            ),
            401,
        )
    if not secrets.compare_digest(got, expected):
        return (
            jsonify(
                {
                    "error": "Não autorizado",
                    "detail": (
                        "Token inválido. Use Authorization: Bearer ou X-API-Key "
                        "com o valor de API_TOKEN"
                    ),
                }
            ),
            401,
        )
    return None


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


@app.route("/health", methods=["GET"])
def health():
    """Verifica se o serviço está no ar."""
    return jsonify({"status": "ok"})


@app.route("/leituras", methods=["GET"])
@swag_from(
    {
        "tags": ["leituras"],
        "summary": "Lista leituras com filtros",
        "description": (
            "Filtra por `codplantacao` e/ou período em `dataleit`. "
            "É obrigatório informar pelo menos um filtro."
        ),
        "parameters": [
            {
                "name": "codplantacao",
                "in": "query",
                "type": "string",
                "required": False,
                "description": "Código da plantação",
            },
            {
                "name": "dataleit_inicio",
                "in": "query",
                "type": "string",
                "format": "date",
                "required": False,
                "description": "Início do período (dataleit >=), inclusive (YYYY-MM-DD)",
            },
            {
                "name": "dataleit_fim",
                "in": "query",
                "type": "string",
                "format": "date",
                "required": False,
                "description": "Fim do período (dataleit <=), inclusive (YYYY-MM-DD)",
            },
            {
                "name": "limit",
                "in": "query",
                "type": "integer",
                "required": False,
                "default": 100,
                "description": "Máximo de registros (1–500)",
            },
            {
                "name": "offset",
                "in": "query",
                "type": "integer",
                "required": False,
                "default": 0,
                "description": "Deslocamento para paginação",
            },
        ],
        "responses": {
            "200": {
                "description": "Lista de leituras",
                "schema": {
                    "type": "object",
                    "properties": {
                        "total": {"type": "integer"},
                        "limit": {"type": "integer"},
                        "offset": {"type": "integer"},
                        "items": {"type": "array", "items": {"type": "object"}},
                    },
                },
            },
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
    limit = request.args.get("limit", default=100, type=int)
    offset = request.args.get("offset", default=0, type=int)
    if limit is None:
        limit = 100
    if offset is None:
        offset = 0

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


@app.route("/leituras", methods=["POST"])
@swag_from(
    {
        "tags": ["leituras"],
        "summary": "Insere uma leitura",
        "consumes": ["application/json"],
        "parameters": [
            {
                "name": "body",
                "in": "body",
                "required": True,
                "schema": {
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
                        "codplantacao": {"type": "string", "example": "PLANTDEMO"},
                        "codleitura": {"type": "string", "example": "LEITDEMO"},
                        "lat": {"type": "number", "example": -22.9068},
                        "lon": {"type": "number", "example": -43.1729},
                        "dataleit": {
                            "type": "string",
                            "format": "date",
                            "example": "2026-05-01",
                        },
                        "horaleit": {
                            "type": "string",
                            "example": "14:30:00",
                        },
                        "temp_solo": {"type": "number", "example": 25.5},
                        "temp_ar": {"type": "number", "example": 28.3},
                        "umid_solo": {"type": "number", "example": 60.2},
                        "umid_ar": {"type": "number", "example": 55.1},
                        "luz": {"type": "number", "example": 800.0},
                        "chuva": {"type": "number", "example": 0.0},
                        "umid_folha": {"type": "number", "example": 10.5},
                        "scomunicacao": {"type": "number", "example": 1.0},
                        "stensao": {"type": "number", "example": 220.0},
                        "scorrente": {"type": "number", "example": 0.5},
                        "spotencia": {"type": "number", "example": 110.0},
                        "status_blockchain": {
                            "type": "string",
                            "enum": ["PENDENTE", "ENVIADO", "CONFIRMADO"],
                            "example": "PENDENTE",
                        },
                        "hash_blockchain": {"type": "string", "x-nullable": True},
                        "tx_hash": {"type": "string", "x-nullable": True},
                        "criadoem": {
                            "type": "string",
                            "format": "date-time",
                            "description": "Opcional; se omitido, usa NOW() no banco.",
                        },
                    },
                },
            }
        ],
        "responses": {
            "201": {
                "description": "Leitura inserida",
                "schema": {
                    "type": "object",
                    "properties": {
                        "hash_pk": {"type": "string"},
                        "message": {"type": "string"},
                    },
                },
            },
            "400": {"description": "JSON inválido ou campos obrigatórios ausentes"},
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

    required = (
        "codplantacao",
        "codleitura",
        "lat",
        "lon",
        "dataleit",
        "horaleit",
    )
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

    hash_blockchain = data.get("hash_blockchain")
    tx_hash = data.get("tx_hash")
    criadoem = data.get("criadoem")

    insert_sql_base = """
        INSERT INTO public.leituras (
            codplantacao,
            codleitura,
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
            status_blockchain,
            hash_blockchain,
            tx_hash{extra_cols}
        )
        VALUES (
            %(codplantacao)s,
            %(codleitura)s,
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
            %(status_blockchain)s,
            %(hash_blockchain)s,
            %(tx_hash)s{extra_vals}
        )
        RETURNING hash_pk
    """

    params = {
        "codplantacao": str(data["codplantacao"])[:30],
        "codleitura": str(data["codleitura"])[:50],
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
        "status_blockchain": status_blockchain,
        "hash_blockchain": hash_blockchain,
        "tx_hash": tx_hash,
    }
    if criadoem is not None:
        if isinstance(criadoem, str):
            params["criadoem"] = datetime.fromisoformat(
                criadoem.replace("Z", "+00:00")
            )
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
        return jsonify(
            {"error": "Leitura já existe (mesmo hash_pk / chave duplicada)"}
        ), 409
    except psycopg2.Error as e:
        conn.rollback()
        return jsonify({"error": "Erro ao inserir", "detail": str(e)}), 500
    finally:
        conn.close()

    return (
        jsonify(
            {
                "message": "Leitura inserida",
                "hash_pk": hash_pk,
            }
        ),
        201,
    )


app.wsgi_app = DispatcherMiddleware(app.wsgi_app, {"/soap": soap_wsgi_app})


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", "8001")), debug=True)
