from __future__ import annotations

import os
import secrets

from flask import jsonify, request


def rest_api_token_configured() -> str | None:
    raw = os.getenv("API_TOKEN")
    if raw is None:
        return None
    token = raw.strip()
    return token if token else None


def token_from_request() -> str | None:
    auth = (request.headers.get("Authorization") or "").strip()
    if auth:
        low = auth.lower()
        if low.startswith("bearer "):
            t = auth[7:].strip()
            return t if t else None
        if low.startswith("token "):
            t = auth[6:].strip()
            return t if t else None
        if low.startswith(("basic ", "digest ", "negotiate ")):
            pass  # ignora — use X-API-Key ou Bearer
        else:
            # Swagger UI (`apiKey` em Authorization) envia só o segredo, sem "Bearer "
            return auth if auth else None
    x = request.headers.get("X-API-Key", "").strip()
    return x if x else None


def require_rest_api_token_for_leituras():
    if request.path != "/leituras":
        return None
    expected = rest_api_token_configured()
    if not expected:
        return None
    got = token_from_request()
    if got is None or len(got) != len(expected) or not secrets.compare_digest(got, expected):
        return (
            jsonify(
                {
                    "error": "Não autorizado",
                    "detail": (
                        "Informe o token de API_TOKEN: cabeçalho "
                        "`Authorization: Bearer <token>` ou apenas `Authorization: <token>` "
                        "(como o Swagger UI costuma enviar), ou `X-API-Key`."
                    ),
                }
            ),
            401,
        )
    return None


def _is_mcp_public_doc_path(path: str) -> bool:
    """Página de ajuda aberta no navegador (sem cabeçalhos de API)."""
    p = (path or "").rstrip("/") or "/"
    return p == "/mcp/info"


def require_api_token_for_mcp():
    if not request.path.startswith("/mcp"):
        return None

    # Documentação "como usar" — não exige token (dados sensíveis ficam em /mcp/leituras etc.)
    if _is_mcp_public_doc_path(request.path):
        return None

    expected = rest_api_token_configured()
    if not expected:
        return (
            jsonify(
                {
                    "error": "MCP desabilitado",
                    "detail": "Defina API_TOKEN no .env para habilitar /mcp/*",
                }
            ),
            503,
        )

    got = token_from_request()
    if got is None or len(got) != len(expected) or not secrets.compare_digest(got, expected):
        return (
            jsonify(
                {
                    "error": "Não autorizado",
                    "detail": (
                        "Informe o token de API_TOKEN: cabeçalho "
                        "`Authorization: Bearer <token>` ou apenas `Authorization: <token>` "
                        "(como o Swagger UI costuma enviar), ou `X-API-Key`."
                    ),
                }
            ),
            401,
        )
    return None

