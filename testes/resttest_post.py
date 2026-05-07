#!/usr/bin/env python3
"""
Teste simples de REST (POST /leituras) para depurar no console.

- Lê `.env` automaticamente (python-dotenv), e usa API_TOKEN se estiver definido.
- Envia JSON e imprime HTTP status + corpo (incluindo `error`/`detail`/`missing`).

Uso:
  python3 testes/resttest_post.py --help

Exemplos:
  # Exemplo mínimo (ajuste lat/lon e códigos)
  python3 testes/resttest_post.py --base-url http://127.0.0.1:8001 \
    --codplantacao PLANTDEMO --codleitura LEIT001 --lat -22.9 --lon -43.17

  # Enviar JSON completo a partir de arquivo
  python3 testes/resttest_post.py --json-file leitura.json
"""

from __future__ import annotations

import argparse
import json
import os
import ssl
import sys
from datetime import datetime, timezone
from typing import Dict, Optional, Tuple
from urllib.error import HTTPError, URLError
from urllib.parse import urljoin
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
    try:
        obj = json.loads(txt)
        return json.dumps(obj, ensure_ascii=False, indent=2, sort_keys=False)
    except Exception:
        return txt


def _now_dataleit_horaleit() -> Tuple[str, str]:
    now = datetime.now(timezone.utc).astimezone()
    return now.date().isoformat(), now.time().replace(microsecond=0).isoformat()


def _print_suggested_help(base_url: str) -> None:
    token = _token_from_env()
    endpoint = urljoin(base_url.rstrip("/") + "/", "leituras")
    _print("Exemplos (ajuste lat/lon e códigos):")
    _print(f"  python3 resttest_post.py --base-url {base_url} --codplantacao PLANTDEMO --codleitura LEIT001 --lat -22.9 --lon -43.17")
    _print(f"  python3 resttest_post.py --base-url {base_url} --json-file leitura.json")
    _print("")
    _print("O que conferir:")
    _print(f"  - Endpoint: {endpoint}")
    if token:
        _print(f"  - API_TOKEN está definido no .env ({token!r}). O servidor exige o MESMO valor em POST /leituras.")
        _print("    Formas aceitas:")
        _print("      - Authorization: Bearer <API_TOKEN>")
        _print("      - X-API-Key: <API_TOKEN>")
        _print("")
        _print("    Exemplos enviando token (recomendado):")
        _print(f"      python3 resttest_post.py --base-url {base_url} --codplantacao PLANTDEMO --codleitura LEIT001 --lat -22.9 --lon -43.17 --auth bearer")
        _print(f"      python3 resttest_post.py --base-url {base_url} --codplantacao PLANTDEMO --codleitura LEIT001 --lat -22.9 --lon -43.17 --auth x-api-key")
        _print("")
        _print("    Se você estiver recebendo 401 e quer confirmar, rode SEM token:")
        _print(f"      python3 resttest_post.py --base-url {base_url} --codplantacao PLANTDEMO --codleitura LEIT001 --lat -22.9 --lon -43.17 --auth none")
        _print("")
        _print("    Dica: o token NÃO é 'gerado' automaticamente; é um segredo configurado no servidor via API_TOKEN.")
    else:
        _print("  - API_TOKEN está vazio/ausente; POST /leituras não exige token.")
    _print("")
    _print("Exemplo completo de JSON (A = alfanumérico, 0 = numérico):")
    _print(
        json.dumps(
            {
                "codplantacao": "A",
                "codleitura": "A",
                "codsensor": "A",
                "lat": 0,
                "lon": 0,
                "dataleit": "2026-05-07",
                "horaleit": "12:00:00",
                "temp_solo": 0,
                "temp_ar": 0,
                "umid_solo": 0,
                "umid_ar": 0,
                "luz": 0,
                "chuva": 0,
                "umid_folha": 0,
                "scomunicacao": 0,
                "stensao": 0,
                "scorrente": 0,
                "spotencia": 0,
                "ref_rssi_dbm": 0,
                "rec_rssi_dbm": 0,
                "fator_n": 0,
                "distcalc_app": 0,
                "status_blockchain": "PENDENTE",
                "hash_blockchain": "A",
                "tx_hash": "A",
                "criadoem": "2026-05-07T12:00:00",
            },
            ensure_ascii=False,
            indent=2,
            sort_keys=False,
        )
    )
    _print("")
    _print("Você pode enviar esse JSON completo assim (recomendado via arquivo):")
    _print("  1) Salve como leitura.json")
    _print(f"  2) Rode: python3 resttest_post.py --base-url {base_url} --json-file leitura.json")
    _print("")


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="REST debug: POST /leituras")
    p.add_argument("--base-url", default=None, help="Base da API (ex.: http://127.0.0.1:8001). Default: SOAP_PUBLIC_URL (se apontar para /soap) ou http://127.0.0.1:8001")
    p.add_argument("--timeout", type=int, default=20, help="Timeout (segundos)")
    p.add_argument("--insecure-tls", action="store_true", help="Ignora validação TLS (apenas HTTPS)")
    p.add_argument("--max-chars", type=int, default=8000, help="Máximo de caracteres no console (0 = sem truncar)")
    p.add_argument("--auth", choices=["bearer", "x-api-key", "none"], default="bearer", help="Como enviar API_TOKEN (padrão bearer). Use none para desativar mesmo com API_TOKEN no .env")

    src = p.add_mutually_exclusive_group()
    src.add_argument("--json-file", default=None, help="Arquivo JSON com o corpo completo (ex.: leitura.json)")
    src.add_argument("--json", default=None, help="JSON inline (string) com o corpo completo")

    p.add_argument("--codplantacao", default=None, help="Obrigatório (se não usar --json/--json-file)")
    p.add_argument("--codleitura", default=None, help="Obrigatório (se não usar --json/--json-file)")
    p.add_argument("--codsensor", default=None, help="Opcional (texto; até 20 chars)")
    p.add_argument("--lat", type=float, default=None, help="Obrigatório (se não usar --json/--json-file)")
    p.add_argument("--lon", type=float, default=None, help="Obrigatório (se não usar --json/--json-file)")
    p.add_argument("--dataleit", default=None, help="Data (YYYY-MM-DD). Se omitido, usa hoje")
    p.add_argument("--horaleit", default=None, help="Hora (HH:MM ou HH:MM:SS). Se omitido, usa agora")
    p.add_argument("--ref-rssi-dbm", dest="ref_rssi_dbm", type=float, default=None, help="Opcional (número; dBm)")
    p.add_argument("--rec-rssi-dbm", dest="rec_rssi_dbm", type=float, default=None, help="Opcional (número; dBm)")
    p.add_argument("--fator-n", dest="fator_n", type=float, default=None, help="Opcional (número)")
    p.add_argument("--distcalc-app", dest="distcalc_app", type=float, default=None, help="Opcional (número)")
    return p


def main(argv: Optional[list[str]] = None) -> int:
    if load_dotenv is not None:
        load_dotenv()

    parser = build_parser()
    args = parser.parse_args(argv)

    base_url = (args.base_url or _default_base_url()).rstrip("/")
    if argv is None or len(argv) == 0:
        _print_suggested_help(base_url)
        _print("Ajuda completa:\n")
        parser.print_help()
        return 2

    body_obj: Dict[str, object]
    if args.json_file:
        with open(args.json_file, "rb") as f:
            raw = f.read().decode("utf-8", errors="replace")
        body_obj = json.loads(raw)
    elif args.json:
        body_obj = json.loads(args.json)
    else:
        missing = []
        for k in ("codplantacao", "codleitura", "lat", "lon"):
            if getattr(args, k) is None:
                missing.append(k)
        if missing:
            _eprint(f"Faltando parâmetros: {missing}. Use --help para ver exemplos.")
            return 2

        dataleit, horaleit = _now_dataleit_horaleit()
        body_obj = {
            "codplantacao": args.codplantacao,
            "codleitura": args.codleitura,
            "codsensor": args.codsensor,
            "lat": args.lat,
            "lon": args.lon,
            "dataleit": args.dataleit or dataleit,
            "horaleit": args.horaleit or horaleit,
        }
        if args.ref_rssi_dbm is not None:
            body_obj["ref_rssi_dbm"] = args.ref_rssi_dbm
        if args.rec_rssi_dbm is not None:
            body_obj["rec_rssi_dbm"] = args.rec_rssi_dbm
        if args.fator_n is not None:
            body_obj["fator_n"] = args.fator_n
        if args.distcalc_app is not None:
            body_obj["distcalc_app"] = args.distcalc_app

    endpoint = urljoin(base_url + "/", "leituras")
    url = endpoint

    headers: Dict[str, str] = {
        "Content-Type": "application/json; charset=utf-8",
        "Accept": "application/json",
    }
    token = _token_from_env()
    if token and args.auth != "none":
        if args.auth == "x-api-key":
            headers["X-API-Key"] = token
        else:
            headers["Authorization"] = f"Bearer {token}"

    payload = json.dumps(body_obj, ensure_ascii=False).encode("utf-8")

    _print(f"POST: {url}")
    if token:
        _print(f"Auth: {args.auth} (API_TOKEN do .env {'enviado' if args.auth != 'none' else 'NÃO enviado'})")
    _print("\n--- REQUEST JSON ---")
    _print(json.dumps(body_obj, ensure_ascii=False, indent=2, sort_keys=False))

    status, resp_headers, body = _read_url(
        url,
        method="POST",
        headers=headers,
        data=payload,
        timeout_s=args.timeout,
        insecure_tls=args.insecure_tls,
    )

    _print(f"\nHTTP {status}")
    _print(f"Content-Type: {resp_headers.get('content-type', '')}")
    _print("\n--- BODY ---")
    _print(_truncate(_pretty_body(body), args.max_chars))

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

