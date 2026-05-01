import os
from datetime import date, datetime, time
from decimal import Decimal

import psycopg2
from psycopg2.extras import RealDictCursor
from psycopg2 import errors as pg_errors
from dotenv import load_dotenv
from flasgger import Swagger, swag_from
from flask import Flask, jsonify, request

load_dotenv()

app = Flask(__name__)
app.config["JSON_SORT_KEYS"] = False

swagger_template = {
    "swagger": "2.0",
    "info": {
        "title": "Servidor API — BlueSensores (UTFPR)",
        "description": "Projeto BlueSensores — recebe leituras em JSON e persiste na tabela `leituras`.",
        "version": "1.0.0",
    },
    "tags": [{"name": "leituras", "description": "Operações de leitura"}],
}
Swagger(app, template=swagger_template)


def get_connection():
    url = os.getenv("DATABASE_URL")
    if url:
        return psycopg2.connect(url)
    host = os.getenv("DB_HOST")
    name = os.getenv("DB_NAME")
    user = os.getenv("DB_USER")
    password = os.getenv("DB_PASSWORD", "")
    port = os.getenv("DB_PORT", "5432")
    if not all([host, name, user is not None]):
        raise RuntimeError(
            "Configure DATABASE_URL ou DB_HOST, DB_NAME, DB_USER (e opcionalmente DB_PASSWORD, DB_PORT) no .env"
        )
    return psycopg2.connect(
        host=host,
        port=port,
        dbname=name,
        user=user,
        password=password,
    )


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


def _serialize_value(v):
    if v is None:
        return None
    if isinstance(v, datetime):
        return v.isoformat()
    if isinstance(v, date):
        return v.isoformat()
    if isinstance(v, time):
        return v.isoformat()
    if isinstance(v, Decimal):
        return float(v)
    return v


def _serialize_row(row):
    return {k: _serialize_value(v) for k, v in row.items()}


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
            "500": {"description": "Erro interno ou falha de conexão com o banco"},
        },
    }
)
def listar_leituras():
    cod = request.args.get("codplantacao", type=str)
    if cod is not None:
        cod = cod.strip()
        if cod == "":
            cod = None

    d_ini_raw = request.args.get("dataleit_inicio")
    d_fim_raw = request.args.get("dataleit_fim")

    dataleit_inicio = None
    dataleit_fim = None
    if d_ini_raw:
        try:
            dataleit_inicio = _parse_date(d_ini_raw.strip())
        except (ValueError, TypeError):
            return jsonify({"error": "dataleit_inicio inválida (use YYYY-MM-DD)"}), 400
    if d_fim_raw:
        try:
            dataleit_fim = _parse_date(d_fim_raw.strip())
        except (ValueError, TypeError):
            return jsonify({"error": "dataleit_fim inválida (use YYYY-MM-DD)"}), 400

    if dataleit_inicio and dataleit_fim and dataleit_inicio > dataleit_fim:
        return jsonify(
            {"error": "dataleit_inicio não pode ser posterior a dataleit_fim"}
        ), 400

    if cod is None and dataleit_inicio is None and dataleit_fim is None:
        return jsonify(
            {
                "error": (
                    "Informe pelo menos um filtro: codplantacao e/ou "
                    "dataleit_inicio e/ou dataleit_fim"
                )
            }
        ), 400

    limit = request.args.get("limit", default=100, type=int)
    offset = request.args.get("offset", default=0, type=int)
    if limit is None or limit < 1 or limit > 500:
        return jsonify({"error": "limit deve ser entre 1 e 500"}), 400
    if offset is None or offset < 0:
        return jsonify({"error": "offset deve ser >= 0"}), 400

    conds = []
    params = []
    if cod is not None:
        conds.append("codplantacao = %s")
        params.append(cod)
    if dataleit_inicio is not None:
        conds.append("dataleit >= %s")
        params.append(dataleit_inicio)
    if dataleit_fim is not None:
        conds.append("dataleit <= %s")
        params.append(dataleit_fim)

    where_sql = " AND ".join(conds)

    count_sql = f"SELECT COUNT(*) AS c FROM public.leituras WHERE {where_sql}"
    select_sql = f"""
        SELECT
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
            hash_pk,
            status_blockchain,
            hash_blockchain,
            tx_hash,
            criadoem
        FROM public.leituras
        WHERE {where_sql}
        ORDER BY dataleit DESC, horaleit DESC
        LIMIT %s OFFSET %s
    """

    try:
        conn = get_connection()
    except Exception as e:
        return jsonify({"error": "Falha ao conectar ao banco", "detail": str(e)}), 500

    try:
        with conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(count_sql, params)
                total = cur.fetchone()["c"]
                cur.execute(select_sql, params + [limit, offset])
                rows = cur.fetchall()
    except psycopg2.Error as e:
        return jsonify({"error": "Erro ao consultar", "detail": str(e)}), 500
    finally:
        conn.close()

    items = [_serialize_row(r) for r in rows]
    return jsonify(
        {
            "total": total,
            "limit": limit,
            "offset": offset,
            "items": items,
        }
    )


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
            "409": {"description": "Conflito de chave primária (leitura duplicada)"},
            "500": {"description": "Erro interno ou falha de conexão com o banco"},
        },
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


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", "8001")), debug=True)
