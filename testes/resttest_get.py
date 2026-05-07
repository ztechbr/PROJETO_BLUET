#!/usr/bin/env python3
"""
Teste simples de REST (GET /leituras) para depurar no console.

- Lê `.env` automaticamente (python-dotenv), e usa API_TOKEN se estiver definido.
- Mostra HTTP status, headers principais e o corpo (incluindo `error`/`detail`).

Uso:
  python3 testes/resttest_get.py --help
  python3 testes/resttest_get.py --base-url http://127.0.0.1:8001 --codplantacao PLANTDEMO
  python3 testes/resttest_get.py --codplantacao PLANTDEMO --dataleit-inicio 2026-05-01 --dataleit-fim 2026-05-07
"""

from __future__ import annotations

import argparse
import json
import os
import ssl
import sys
from typing import Dict, Optional, Tuple
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode, urljoin
from urllib.request import Request, urlopen

try:
    from dotenv import load_dotenv
except Exception:  # pragma: no cover
    load_dotenv = None


def _print(s: str = "") -> None:
    sys.stdout.write(s + "\n")


def _eprint(s: str = "") -> None:
    sys.stderr.write(s + "\n")


def _default_base_url() -> str:
    # REST não tem REST_PUBLIC_URL hoje; usa o mesmo SOAP_PUBLIC_URL se existir (sem /soap)
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


def _truncate(s: str, max_chars: int) -> str:
    if max_chars <= 0 or len(s) <= max_chars:
        return s
    return s[: max_chars - 20] + "\n... (truncado) ...\n"


def _pretty_body(body: bytes) -> str:
    txt = body.decode("utf-8", errors="replace")
    # tenta pretty-print de JSON
    try:
        obj = json.loads(txt)
        return json.dumps(obj, ensure_ascii=False, indent=2, sort_keys=False)
    except Exception:
        return txt


def _print_suggested_help(base_url: str) -> None:
    token = _token_from_env()
    endpoint = urljoin(base_url.rstrip("/") + "/", "leituras")
    _print("Exemplos (ajuste datas/URL se necessário):")
    _print(f"  python3 resttest_get.py --base-url {base_url} --codplantacao PLANTDEMO")
    _print(f"  python3 resttest_get.py --base-url {base_url} --dataleit-inicio 2026-05-01 --dataleit-fim 2026-05-07")
    _print(f"  python3 resttest_get.py --base-url {base_url} --codplantacao PLANTDEMO --dataleit-inicio 2026-05-01 --dataleit-fim 2026-05-07")
    _print("")
    _print("O que conferir:")
    _print(f"  - Endpoint: {endpoint}")
    if token:
        _print(f"  - API_TOKEN está definido no .env ({token!r}). O servidor exige o MESMO valor em GET /leituras.")
        _print("    Formas aceitas:")
        _print("      - Authorization: Bearer <API_TOKEN>")
        _print("      - X-API-Key: <API_TOKEN>")
        _print("")
        _print("    Exemplos enviando token (recomendado):")
        _print(f"      python3 resttest_get.py --base-url {base_url} --codplantacao PLANTDEMO --auth bearer")
        _print(f"      python3 resttest_get.py --base-url {base_url} --codplantacao PLANTDEMO --auth x-api-key")
        _print("")
        _print("    Se você estiver recebendo 401 e quer confirmar, rode SEM token:")
        _print(f"      python3 resttest_get.py --base-url {base_url} --codplantacao PLANTDEMO --auth none")
        _print("")
        _print("    Dica: o token NÃO é 'gerado' automaticamente; é um segredo configurado no servidor via API_TOKEN.")
    else:
        _print("  - API_TOKEN está vazio/ausente; GET /leituras não exige token.")
    _print("")
    _print("Obs.: a resposta do GET /leituras inclui também os campos novos quando existirem no banco:")
    _print("  - codsensor, ref_rssi_dbm, rec_rssi_dbm, fator_n, distcalc_app")
    _print("")


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="REST debug: GET /leituras")
    p.add_argument("--base-url", default=None, help="Base da API (ex.: http://127.0.0.1:8001). Default: SOAP_PUBLIC_URL (se apontar para /soap) ou http://127.0.0.1:8001")
    p.add_argument("--timeout", type=int, default=20, help="Timeout (segundos)")
    p.add_argument("--insecure-tls", action="store_true", help="Ignora validação TLS (apenas HTTPS)")
    p.add_argument("--max-chars", type=int, default=8000, help="Máximo de caracteres no console (0 = sem truncar)")

    p.add_argument("--codplantacao", default=None, help="Filtro por plantação")
    p.add_argument("--dataleit-inicio", dest="dataleit_inicio", default=None, help="Data inicial inclusiva (YYYY-MM-DD)")
    p.add_argument("--dataleit-fim", dest="dataleit_fim", default=None, help="Data final inclusiva (YYYY-MM-DD)")
    p.add_argument("--limit", type=int, default=100, help="Limite (1–500)")
    p.add_argument("--offset", type=int, default=0, help="Offset/paginação")

    p.add_argument("--auth", choices=["bearer", "x-api-key", "none"], default="bearer", help="Como enviar API_TOKEN (padrão bearer). Use none para desativar mesmo com API_TOKEN no .env")
    return p


def main(argv: Optional[list[str]] = None) -> int:
    if load_dotenv is not None:
        load_dotenv()

    parser = build_parser()
    args = parser.parse_args(argv)

    base_url = (args.base_url or _default_base_url()).rstrip("/")
    if (argv is None or len(argv) == 0) and not args.codplantacao and not args.dataleit_inicio and not args.dataleit_fim:
        _print_suggested_help(base_url)
        _print("Ajuda completa:\n")
        parser.print_help()
        return 2

    params = {
        "codplantacao": args.codplantacao,
        "dataleit_inicio": args.dataleit_inicio,
        "dataleit_fim": args.dataleit_fim,
        "limit": args.limit,
        "offset": args.offset,
    }
    # remove None
    params = {k: v for k, v in params.items() if v is not None}
    qs = urlencode(params)

    endpoint = urljoin(base_url + "/", "leituras")
    url = endpoint + ("?" + qs if qs else "")

    headers: Dict[str, str] = {"Accept": "application/json"}
    token = _token_from_env()
    if token and args.auth != "none":
        if args.auth == "x-api-key":
            headers["X-API-Key"] = token
        else:
            headers["Authorization"] = f"Bearer {token}"

    _print(f"GET: {url}")
    if token:
        _print(f"Auth: {args.auth} (API_TOKEN do .env {'enviado' if args.auth != 'none' else 'NÃO enviado'})")

    status, resp_headers, body = _read_url(url, headers=headers, timeout_s=args.timeout, insecure_tls=args.insecure_tls)
    _print(f"HTTP {status}")
    _print(f"Content-Type: {resp_headers.get('content-type', '')}")

    text = _pretty_body(body)
    _print("\n--- BODY ---")
    _print(_truncate(text, args.max_chars))

    # tenta realçar campos de erro no stderr também
    if status >= 400:
        try:
            obj = json.loads(body.decode("utf-8", errors="replace"))
            err = obj.get("error")
            detail = obj.get("detail")
            missing = obj.get("missing")
            if err:
                _eprint(f"[erro] {err}")
            if detail:
                _eprint(f"[detail] {detail}")
            if missing:
                _eprint(f"[missing] {missing}")
        except Exception:
            pass
        return 2

    return 0


if __name__ == "__main__":
    raise SystemExit(main())

