#!/usr/bin/env python3
"""
Teste simples do "MCP" (rotas /mcp/*) usando LangChain como pipeline (sem LLM).

O objetivo é validar rapidamente:
  - autenticação via API_TOKEN
  - /mcp/health responde
  - /mcp/leituras responde (leitura simples)
  - /mcp/schema responde (opcional)

Uso:
  python3 testes/mcptest.py --help
  python3 testes/mcptest.py --base-url http://127.0.0.1:8001 --codplantacao PLANTDEMO
"""

from __future__ import annotations

import argparse
import json
import os
import ssl
import sys
from typing import Any, Dict, Optional, Tuple
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode, urljoin
from urllib.request import Request, urlopen

try:
    from dotenv import load_dotenv
except Exception:  # pragma: no cover
    load_dotenv = None

try:
    from langchain_core.runnables import RunnableLambda
except Exception:  # pragma: no cover
    RunnableLambda = None


def _print(s: str = "") -> None:
    sys.stdout.write(s + "\n")


def _eprint(s: str = "") -> None:
    sys.stderr.write(s + "\n")


def _default_base_url() -> str:
    public = (os.getenv("SOAP_PUBLIC_URL") or "").strip().rstrip("/")
    if public.endswith("/soap"):
        public = public[: -len("/soap")]
    if public:
        return public
    return "http://127.0.0.1:8001"


def _token_from_env() -> Optional[str]:
    tok = (os.getenv("API_TOKEN") or "").strip()
    return tok or None


def _read_url(
    url: str,
    method: str = "GET",
    headers: Optional[Dict[str, str]] = None,
    data: Optional[bytes] = None,
    timeout_s: int = 20,
    insecure_tls: bool = False,
) -> Tuple[int, Dict[str, str], bytes]:
    req = Request(url, method=method)
    for k, v in (headers or {}).items():
        req.add_header(k, v)

    ctx = None
    if insecure_tls and url.lower().startswith("https://"):
        ctx = ssl.create_default_context()
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_NONE

    try:
        with urlopen(req, data=data, timeout=timeout_s, context=ctx) as resp:
            status = getattr(resp, "status", 200)
            resp_headers = {k.lower(): v for (k, v) in resp.headers.items()}
            body = resp.read()
            return status, resp_headers, body
    except HTTPError as e:
        body = e.read() if hasattr(e, "read") else b""
        resp_headers = {k.lower(): v for (k, v) in (e.headers.items() if e.headers else [])}
        return int(getattr(e, "code", 0) or 0), resp_headers, body
    except URLError as e:
        raise RuntimeError(f"Falha de conexão: {e}") from e


def _pretty_json(body: bytes) -> str:
    txt = body.decode("utf-8", errors="replace")
    try:
        obj = json.loads(txt)
        return json.dumps(obj, ensure_ascii=False, indent=2, sort_keys=False)
    except Exception:
        return txt


def _mcp_url(base_url: str, path: str) -> str:
    return urljoin(base_url.rstrip("/") + "/", path.lstrip("/"))


def _headers_with_token(token: str, auth_mode: str) -> Dict[str, str]:
    h: Dict[str, str] = {"Accept": "application/json"}
    if auth_mode == "x-api-key":
        h["X-API-Key"] = token
    else:
        h["Authorization"] = f"Bearer {token}"
    return h


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Teste do MCP (rotas /mcp/*) com LangChain (sem LLM)")
    p.add_argument("--base-url", default=None, help="Base da API (ex.: http://127.0.0.1:8001). Default: SOAP_PUBLIC_URL (sem /soap) ou http://127.0.0.1:8001")
    p.add_argument("--timeout", type=int, default=20, help="Timeout (segundos)")
    p.add_argument("--insecure-tls", action="store_true", help="Ignora validação TLS (apenas HTTPS)")
    p.add_argument("--auth", choices=["bearer", "x-api-key"], default="bearer", help="Como enviar API_TOKEN (padrão bearer)")
    p.add_argument("--codplantacao", default="PLANTDEMO", help="Filtro simples para /mcp/leituras")
    p.add_argument("--schema", action="store_true", help="Também testa /mcp/schema")
    return p


def main(argv: Optional[list[str]] = None) -> int:
    if load_dotenv is not None:
        load_dotenv()

    args = build_parser().parse_args(argv)
    base_url = (args.base_url or _default_base_url()).rstrip("/")
    token = _token_from_env()

    if RunnableLambda is None:
        _eprint("LangChain não está instalado. Rode: pip install -r requirements.txt")
        return 2

    if not token:
        _eprint("API_TOKEN não está definido no .env. O /mcp/* fica desabilitado sem API_TOKEN.")
        return 2

    headers = _headers_with_token(token, args.auth)

    def step_health(_: Any) -> Dict[str, Any]:
        url = _mcp_url(base_url, "/mcp/health")
        status, _h, body = _read_url(url, headers=headers, timeout_s=args.timeout, insecure_tls=args.insecure_tls)
        return {"name": "health", "url": url, "status": status, "body": body}

    def step_schema(prev: Dict[str, Any]) -> Dict[str, Any]:
        if not args.schema:
            return prev
        url = _mcp_url(base_url, "/mcp/schema")
        status, _h, body = _read_url(url, headers=headers, timeout_s=args.timeout, insecure_tls=args.insecure_tls)
        prev["schema"] = {"url": url, "status": status, "body": body}
        return prev

    def step_read(prev: Dict[str, Any]) -> Dict[str, Any]:
        qs = urlencode({"codplantacao": args.codplantacao, "limit": 5, "offset": 0})
        url = _mcp_url(base_url, "/mcp/leituras") + "?" + qs
        status, _h, body = _read_url(url, headers=headers, timeout_s=args.timeout, insecure_tls=args.insecure_tls)
        prev["read"] = {"url": url, "status": status, "body": body}
        return prev

    chain = RunnableLambda(step_health) | RunnableLambda(step_schema) | RunnableLambda(step_read)
    result = chain.invoke(None)

    _print(f"Base URL: {base_url}")
    _print(f"Auth: {args.auth} (API_TOKEN do .env enviado)")
    _print("")

    # health
    _print(f"[health] {result['status']} {result['url']}")
    _print(_pretty_json(result["body"]))
    _print("")

    if args.schema and "schema" in result:
        sc = result["schema"]
        _print(f"[schema] {sc['status']} {sc['url']}")
        _print(_pretty_json(sc["body"]))
        _print("")

    rd = result["read"]
    _print(f"[leituras] {rd['status']} {rd['url']}")
    _print(_pretty_json(rd["body"]))

    ok = 200 <= int(result["status"]) < 400 and 200 <= int(rd["status"]) < 400
    if args.schema and "schema" in result:
        ok = ok and (200 <= int(result["schema"]["status"]) < 400)

    return 0 if ok else 2


if __name__ == "__main__":
    raise SystemExit(main())

