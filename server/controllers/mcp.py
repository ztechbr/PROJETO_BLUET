from __future__ import annotations

from flask import Blueprint, jsonify, request

from leituras_query import ConsultaLeiturasError, consulta_leituras_desde_strings
from server.middleware.auth import rest_api_token_configured
from server.services.measures import allowed_numeric_cols


mcp_bp = Blueprint("mcp", __name__, url_prefix="/mcp")


@mcp_bp.get("/health")
def health():
    return jsonify({"status": "ok", "mode": "read-only"})


@mcp_bp.get("/leituras")
def leituras():
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

    return jsonify(payload), 200


@mcp_bp.get("/schema")
def schema():
    numeric_measures = []
    for name, has_sentinel in allowed_numeric_cols():
        numeric_measures.append(
            {"name": name, "type": "number", "sentinel": -9999 if has_sentinel else None}
        )

    other_fields = [
        {"name": "codplantacao", "type": "string"},
        {"name": "codleitura", "type": "string"},
        {"name": "codsensor", "type": "string"},
        {"name": "dataleit", "type": "string", "format": "date"},
        {"name": "horaleit", "type": "string", "format": "time"},
        {"name": "hash_pk", "type": "string"},
        {"name": "status_blockchain", "type": "string"},
        {"name": "hash_blockchain", "type": "string"},
        {"name": "tx_hash", "type": "string"},
        {"name": "criadoem", "type": "string", "format": "date-time"},
    ]

    query_params = [
        {"name": "codplantacao", "type": "string", "required": False},
        {"name": "dataleit_inicio", "type": "string", "format": "date", "required": False},
        {"name": "dataleit_fim", "type": "string", "format": "date", "required": False},
        {"name": "limit", "type": "integer", "required": False, "default": 100, "min": 1, "max": 500},
        {"name": "offset", "type": "integer", "required": False, "default": 0, "min": 0},
    ]

    return jsonify(
        {
            "name": "BlueSensores MCP",
            "mode": "read-only",
            "auth": {
                "type": "api_token",
                "enabled": bool(rest_api_token_configured()),
                "header_options": ["Authorization: Bearer <API_TOKEN>", "X-API-Key: <API_TOKEN>"],
            },
            "endpoints": {
                "/mcp/health": {"method": "GET"},
                "/mcp/schema": {"method": "GET"},
                "/mcp/leituras": {"method": "GET", "query_params": query_params},
            },
            "fields": other_fields + numeric_measures,
        }
    ), 200


@mcp_bp.get("/info")
def info():
    # Mantemos HTML simples aqui para não depender do dashboard.
    enabled = bool(rest_api_token_configured())
    return (
        jsonify(
            {
                "title": "MCP (somente leitura)",
                "enabled": enabled,
                "how_to_auth": [
                    "Authorization: Bearer <API_TOKEN>",
                    "X-API-Key: <API_TOKEN>",
                ],
                "endpoints": ["/mcp/health", "/mcp/schema", "/mcp/leituras"],
                "test_script": "python3 testes/mcptest.py --base-url http://127.0.0.1:8001 --codplantacao PLANTDEMO --schema",
            }
        ),
        200,
    )

