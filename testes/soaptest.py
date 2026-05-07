#!/usr/bin/env python3
"""
Teste simples de cliente SOAP (Spyne) para o endpoint /soap deste projeto.

Uso (exemplos):
  python testes/soaptest.py wsdl --base-url http://127.0.0.1:8001
  python testes/soaptest.py call --codplantacao PLANTDEMO
  python testes/soaptest.py call --dataleit-inicio 2026-05-01 --dataleit-fim 2026-05-31
  python testes/soaptest.py get --format json --codplantacao PLANTDEMO

Dicas:
  - Se você estiver usando SOAP_PUBLIC_URL no .env, o script tenta usar como base.
  - Para depurar erro "Client" (SOAP Fault), rode com --show-request e/ou --full.
"""

from __future__ import annotations

import argparse
import os
import re
import ssl
import sys
from dataclasses import dataclass
from typing import Dict, Optional, Tuple
from urllib.error import HTTPError, URLError
from urllib.parse import urljoin
from urllib.request import Request, urlopen
from xml.etree import ElementTree as ET

try:
    from dotenv import load_dotenv
except Exception:  # pragma: no cover
    load_dotenv = None


SOAP11_ENV = "http://schemas.xmlsoap.org/soap/envelope/"


def _print(s: str = "") -> None:
    sys.stdout.write(s + "\n")


def _eprint(s: str = "") -> None:
    sys.stderr.write(s + "\n")


def _default_base_url() -> str:
    public = (os.getenv("SOAP_PUBLIC_URL") or "").strip()
    if public:
        return public.rstrip("/")
    return "http://127.0.0.1:8001"


def _soap_endpoint_from_base(base_url: str) -> str:
    base = base_url.rstrip("/") + "/"
    return urljoin(base, "soap")


def _wsdl_url_from_base(base_url: str) -> str:
    return _soap_endpoint_from_base(base_url) + "?wsdl"


def _read_url(url: str, method: str = "GET", headers: Optional[Dict[str, str]] = None, data: Optional[bytes] = None, timeout_s: int = 20, insecure_tls: bool = False) -> Tuple[int, Dict[str, str], bytes]:
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


@dataclass
class WsdlInfo:
    target_namespace: Optional[str]
    soap_address: Optional[str]


def _parse_wsdl(wsdl_xml: bytes) -> WsdlInfo:
    try:
        root = ET.fromstring(wsdl_xml)
    except ET.ParseError:
        return WsdlInfo(target_namespace=None, soap_address=None)

    # tenta achar targetNamespace no <definitions>
    tns = root.attrib.get("targetNamespace")

    # procura <soap:address location="..."/>
    soap_addr = None
    for el in root.iter():
        tag = el.tag
        if isinstance(tag, str) and tag.endswith("address"):
            loc = el.attrib.get("location")
            if loc:
                soap_addr = loc
                break

    return WsdlInfo(target_namespace=tns, soap_address=soap_addr)


def _xml_pretty_or_raw(xml_bytes: bytes) -> str:
    try:
        root = ET.fromstring(xml_bytes)
        ET.indent(root, space="  ")  # py>=3.9
        return ET.tostring(root, encoding="unicode")
    except Exception:
        try:
            return xml_bytes.decode("utf-8", errors="replace")
        except Exception:
            return repr(xml_bytes)


def _extract_soap_fault(xml_bytes: bytes) -> Optional[Dict[str, str]]:
    try:
        root = ET.fromstring(xml_bytes)
    except ET.ParseError:
        return None

    ns = {"soap": SOAP11_ENV}
    fault = root.find(".//soap:Fault", ns)
    if fault is None:
        # às vezes vem sem namespace corretamente; fallback simples
        for el in root.iter():
            if isinstance(el.tag, str) and el.tag.endswith("Fault"):
                fault = el
                break
    if fault is None:
        return None

    def _find_text(name: str) -> str:
        el = fault.find(name)
        if el is not None and el.text:
            return el.text.strip()
        # fallback por sufixo do tag
        for child in list(fault):
            if isinstance(child.tag, str) and child.tag.endswith(name) and child.text:
                return child.text.strip()
        return ""

    faultcode = _find_text("faultcode")
    faultstring = _find_text("faultstring")
    detail = ""
    d = fault.find("detail")
    if d is not None:
        detail = "".join(ET.tostring(x, encoding="unicode") for x in list(d)).strip()
        if not detail and (d.text or "").strip():
            detail = (d.text or "").strip()
    return {"faultcode": faultcode, "faultstring": faultstring, "detail": detail}


def _build_envelope(tns: str, codplantacao: Optional[str], dataleit_inicio: Optional[str], dataleit_fim: Optional[str], limit: int, offset: int) -> bytes:
    env = ET.Element(f"{{{SOAP11_ENV}}}Envelope")
    body = ET.SubElement(env, f"{{{SOAP11_ENV}}}Body")

    op = ET.SubElement(body, f"{{{tns}}}listarLeituras")
    filtro = ET.SubElement(op, f"{{{tns}}}filtro")

    def add(tag: str, value: Optional[str]) -> None:
        if value is None or value == "":
            return
        el = ET.SubElement(filtro, f"{{{tns}}}{tag}")
        el.text = value

    add("codplantacao", codplantacao)
    add("dataleit_inicio", dataleit_inicio)
    add("dataleit_fim", dataleit_fim)
    add("limit", str(limit))
    add("offset", str(offset))

    return ET.tostring(env, encoding="utf-8", xml_declaration=True)


def _truncate(s: str, max_chars: int) -> str:
    if max_chars <= 0 or len(s) <= max_chars:
        return s
    return s[: max_chars - 20] + "\n... (truncado) ...\n"


def cmd_wsdl(args: argparse.Namespace) -> int:
    wsdl_url = _wsdl_url_from_base(args.base_url)
    _print(f"WSDL: {wsdl_url}")

    status, headers, body = _read_url(wsdl_url, timeout_s=args.timeout, insecure_tls=args.insecure_tls)
    _print(f"HTTP {status}")
    _print(f"Content-Type: {headers.get('content-type', '')}")

    info = _parse_wsdl(body)
    _print(f"targetNamespace: {info.target_namespace or '(não encontrado)'}")
    _print(f"soap:address location: {info.soap_address or '(não encontrado)'}")

    if args.full:
        _print("\n--- WSDL ---")
        _print(_xml_pretty_or_raw(body))
    return 0 if 200 <= status < 400 else 2


def cmd_get(args: argparse.Namespace) -> int:
    # GET /soap?format=json|xml&...
    endpoint = _soap_endpoint_from_base(args.base_url)
    qs = []
    fmt = (args.format or "json").strip().lower()
    qs.append(f"format={fmt}")
    if args.codplantacao:
        qs.append(f"codplantacao={args.codplantacao}")
    if args.dataleit_inicio:
        qs.append(f"dataleit_inicio={args.dataleit_inicio}")
    if args.dataleit_fim:
        qs.append(f"dataleit_fim={args.dataleit_fim}")
    qs.append(f"limit={args.limit}")
    qs.append(f"offset={args.offset}")
    url = endpoint + "?" + "&".join(qs)

    _print(f"GET: {url}")
    status, headers, body = _read_url(url, timeout_s=args.timeout, insecure_tls=args.insecure_tls)
    _print(f"HTTP {status}")
    _print(f"Content-Type: {headers.get('content-type', '')}")

    text = body.decode("utf-8", errors="replace")
    _print("\n--- BODY ---")
    _print(_truncate(text, args.max_chars if not args.full else 0))
    return 0 if 200 <= status < 400 else 2


def _guess_tns_from_wsdl(wsdl_body: bytes) -> Optional[str]:
    info = _parse_wsdl(wsdl_body)
    if info.target_namespace:
        return info.target_namespace
    # fallback simples por regex
    m = re.search(r'targetNamespace="([^"]+)"', wsdl_body.decode("utf-8", errors="ignore"))
    return m.group(1) if m else None


def cmd_call(args: argparse.Namespace) -> int:
    # 1) baixa WSDL para descobrir namespace e/ou endereço
    wsdl_url = _wsdl_url_from_base(args.base_url)
    _print(f"WSDL: {wsdl_url}")
    wsdl_status, _wsdl_headers, wsdl_body = _read_url(wsdl_url, timeout_s=args.timeout, insecure_tls=args.insecure_tls)
    _print(f"WSDL HTTP {wsdl_status}")

    tns = args.tns or _guess_tns_from_wsdl(wsdl_body) or "http://utfpr.edu.br/bluesensores/leituras"
    _print(f"Usando targetNamespace (tns): {tns}")

    endpoint = _soap_endpoint_from_base(args.base_url)
    envelope = _build_envelope(
        tns=tns,
        codplantacao=args.codplantacao,
        dataleit_inicio=args.dataleit_inicio,
        dataleit_fim=args.dataleit_fim,
        limit=args.limit,
        offset=args.offset,
    )

    if args.show_request:
        _print("\n--- REQUEST (SOAP 1.1) ---")
        _print(_xml_pretty_or_raw(envelope))

    headers = {
        "Content-Type": "text/xml; charset=utf-8",
        "SOAPAction": "listarLeituras",
        "Accept": "text/xml, application/xml, */*",
    }
    _print(f"\nPOST: {endpoint}")
    status, resp_headers, resp_body = _read_url(
        endpoint,
        method="POST",
        headers=headers,
        data=envelope,
        timeout_s=args.timeout,
        insecure_tls=args.insecure_tls,
    )
    _print(f"HTTP {status}")
    _print(f"Content-Type: {resp_headers.get('content-type', '')}")

    # Sempre tentar extrair/imprimir SOAP Fault (muito útil para debugar erro "Client").
    # Se não for XML, imprime o corpo bruto mesmo assim.
    fault = _extract_soap_fault(resp_body)
    if fault is not None:
        _print("\n--- SOAP FAULT ---")
        _print(f"faultcode: {fault.get('faultcode','')}")
        _print(f"faultstring: {fault.get('faultstring','')}")
        if (fault.get("detail") or "").strip():
            _print(f"detail: {fault.get('detail','')}")
    elif status >= 400:
        _print("\n--- ERRO (corpo bruto) ---")
        try:
            _print(_truncate(resp_body.decode('utf-8', errors='replace'), args.max_chars if not args.full else 0))
        except Exception:
            _print(repr(resp_body))

    _print("\n--- RESPONSE ---")
    resp_text = _xml_pretty_or_raw(resp_body)
    _print(_truncate(resp_text, args.max_chars if not args.full else 0))

    return 0 if 200 <= status < 400 and fault is None else 2


def build_parser() -> argparse.ArgumentParser:
    common = argparse.ArgumentParser(add_help=False)
    common.add_argument(
        "--base-url",
        default=None,
        help=(
            "Base da API (ex.: http://127.0.0.1:8001). "
            "Default: SOAP_PUBLIC_URL ou http://127.0.0.1:8001"
        ),
    )
    common.add_argument("--timeout", type=int, default=20, help="Timeout (segundos)")
    common.add_argument("--insecure-tls", action="store_true", help="Ignora validação TLS (apenas HTTPS)")

    p = argparse.ArgumentParser(description="Cliente SOAP simples para depurar /soap", parents=[common])

    # Não deixamos required=True para poder imprimir uma ajuda mais útil
    # (com comandos sugeridos) quando o usuário roda sem "cmd".
    sub = p.add_subparsers(dest="cmd")

    # Repetimos as opções comuns também nos subcomandos para aceitar:
    #   python3 soaptest.py wsdl --base-url ...
    # além de:
    #   python3 soaptest.py --base-url ... wsdl
    p_wsdl = sub.add_parser("wsdl", help="Baixa e mostra infos do WSDL", parents=[common])
    p_wsdl.add_argument("--full", action="store_true", help="Imprime o WSDL completo")
    p_wsdl.set_defaults(func=cmd_wsdl)

    p_get = sub.add_parser("get", help="GET /soap?format=json|xml (atalho do gateway)", parents=[common])
    p_get.add_argument("--format", default="json", choices=["json", "xml"], help="Formato de saída do GET")
    p_get.add_argument("--codplantacao", default=None)
    p_get.add_argument("--dataleit-inicio", dest="dataleit_inicio", default=None)
    p_get.add_argument("--dataleit-fim", dest="dataleit_fim", default=None)
    p_get.add_argument("--limit", type=int, default=100)
    p_get.add_argument("--offset", type=int, default=0)
    p_get.add_argument("--full", action="store_true", help="Não truncar a resposta")
    p_get.add_argument("--max-chars", type=int, default=4000, help="Máximo de caracteres no console (quando não usar --full)")
    p_get.set_defaults(func=cmd_get)

    p_call = sub.add_parser("call", help="POST SOAP 1.1 (listarLeituras)", parents=[common])
    p_call.add_argument("--tns", default=None, help="Sobrescreve o targetNamespace (se o WSDL não for parseado)")
    p_call.add_argument("--codplantacao", default=None)
    p_call.add_argument("--dataleit-inicio", dest="dataleit_inicio", default=None)
    p_call.add_argument("--dataleit-fim", dest="dataleit_fim", default=None)
    p_call.add_argument("--limit", type=int, default=100)
    p_call.add_argument("--offset", type=int, default=0)
    p_call.add_argument("--show-request", action="store_true", help="Imprime o XML enviado")
    p_call.add_argument("--full", action="store_true", help="Não truncar a resposta")
    p_call.add_argument("--max-chars", type=int, default=8000, help="Máximo de caracteres no console (quando não usar --full)")
    p_call.set_defaults(func=cmd_call)

    return p


def _print_suggested_help(base_url: str) -> None:
    endpoint = _soap_endpoint_from_base(base_url)
    wsdl = _wsdl_url_from_base(base_url)
    api_token = (os.getenv("API_TOKEN") or "").strip()

    _print("Comandos disponíveis: wsdl | get | call\n")
    _print("Exemplos (ajuste a URL se necessário):")
    _print(f"  python3 soaptest.py wsdl --base-url {base_url}")
    _print(f"  python3 soaptest.py get  --base-url {base_url} --format json --codplantacao PLANTDEMO")
    _print(f"  python3 soaptest.py call --base-url {base_url} --codplantacao PLANTDEMO --show-request")
    _print(f"  python3 soaptest.py call --base-url {base_url} --dataleit-inicio 2026-05-01 --dataleit-fim 2026-05-31 --show-request")
    _print("")
    _print("O que conferir primeiro:")
    _print(f"  - WSDL: {wsdl}")
    _print(f"  - Endpoint SOAP (POST): {endpoint}")
    _print("")
    _print("Observações do seu .env:")
    if api_token:
        _print(f"  - API_TOKEN está definido ({api_token!r}), mas **/soap NÃO usa token** (só /leituras).")
    else:
        _print("  - API_TOKEN está vazio/ausente; isso não afeta /soap.")
    _print("")
    _print("Campos retornados (inclui os novos quando existirem no banco):")
    _print("  - codsensor, ref_rssi_dbm, rec_rssi_dbm, fator_n, distcalc_app")
    _print("")
    _print("Dica para debugar Fault 'Client': use --show-request e compare o namespace/tns com o WSDL.\n")


def main(argv: Optional[list[str]] = None) -> int:
    if load_dotenv is not None:
        load_dotenv()

    parser = build_parser()
    args = parser.parse_args(argv)
    if not args.base_url:
        args.base_url = _default_base_url()

    try:
        if not getattr(args, "cmd", None):
            _print_suggested_help(args.base_url)
            _print("Ajuda completa do argparse:\n")
            parser.print_help()
            return 2
        return int(args.func(args))
    except KeyboardInterrupt:
        _eprint("\nInterrompido.")
        return 130
    except Exception as e:
        _eprint(f"Erro inesperado: {e}")
        return 2


if __name__ == "__main__":
    raise SystemExit(main())

