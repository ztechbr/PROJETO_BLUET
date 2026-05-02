"""
Roteamento WSGI para /soap:
- GET com query iniciando em wsdl → Spyne (WSDL)
- POST → Spyne (SOAP XML)
- GET com format=json|xml e filtros → mesma consulta que GET /leituras (sem API_TOKEN)
"""
import json
from urllib.parse import parse_qs
from xml.etree import ElementTree as ET

from leituras_query import ConsultaLeiturasError, consulta_leituras_desde_strings


def _query_is_wsdl(query_string):
    if not query_string:
        return False
    return query_string.split("&")[0].split("=")[0].strip().lower() == "wsdl"


def _payload_to_xml(payload):
    root = ET.Element("listagem")
    ET.SubElement(root, "total").text = str(payload["total"])
    ET.SubElement(root, "limit").text = str(payload["limit"])
    ET.SubElement(root, "offset").text = str(payload["offset"])
    items_el = ET.SubElement(root, "items")
    for row in payload["items"]:
        item = ET.SubElement(items_el, "item")
        for key, val in row.items():
            el = ET.SubElement(item, key)
            if val is not None:
                el.text = str(val)
    return ET.tostring(root, encoding="utf-8", xml_declaration=True)


def _xml_error(message, status_detail=None):
    err = ET.Element("error")
    m = ET.SubElement(err, "message")
    m.text = message
    if status_detail:
        d = ET.SubElement(err, "detail")
        d.text = status_detail
    return ET.tostring(err, encoding="utf-8", xml_declaration=True)


def _is_soap_path(path):
    p = path.rstrip("/") or "/"
    return p == "/soap"


class SoapHttpGateway:
    """Envolve o Flask WSGI e o Spyne SOAP; interceta GET /soap para JSON/XML."""

    def __init__(self, flask_wsgi_app, soap_wsgi_app):
        self.flask_wsgi_app = flask_wsgi_app
        self.soap_wsgi_app = soap_wsgi_app

    def __call__(self, environ, start_response):
        path = environ.get("PATH_INFO", "")
        method = environ.get("REQUEST_METHOD", "GET")
        if not _is_soap_path(path):
            return self.flask_wsgi_app(environ, start_response)

        qs_raw = environ.get("QUERY_STRING", "")

        if method == "GET" and _query_is_wsdl(qs_raw):
            return self.soap_wsgi_app(environ, start_response)

        if method == "POST":
            return self.soap_wsgi_app(environ, start_response)

        if method == "GET":
            return self._handle_get_simple(environ, start_response, qs_raw)

        start_response("405 Method Not Allowed", [("Content-Type", "text/plain")])
        return [b"Method Not Allowed"]

    def _handle_get_simple(self, environ, start_response, qs_raw):
        q = parse_qs(qs_raw, keep_blank_values=False)

        def g(key):
            v = q.get(key)
            return v[0] if v else None

        fmt = (g("format") or "json").strip().lower()
        if fmt not in ("json", "xml"):
            fmt = "json"

        cod = g("codplantacao")
        d_ini = g("dataleit_inicio")
        d_fim = g("dataleit_fim")
        limit_raw = g("limit")
        offset_raw = g("offset")

        try:
            limit = int(limit_raw) if limit_raw not in (None, "") else 100
            offset = int(offset_raw) if offset_raw not in (None, "") else 0
        except ValueError:
            return self._send(
                start_response,
                fmt,
                {"error": "limit e offset devem ser inteiros"},
                400,
            )

        if not cod and not d_ini and not d_fim:
            help_body = {
                "descricao": (
                    "Consulta via GET no mesmo endpoint SOAP; mesmo filtros do GET /leituras."
                ),
                "wsdl": "GET /soap?wsdl",
                "exemplo_json": (
                    "/soap?format=json&codplantacao=PLANTDEMO&limit=50&offset=0"
                ),
                "exemplo_xml": (
                    "/soap?format=xml&dataleit_inicio=2026-05-01&dataleit_fim=2026-05-31"
                ),
                "parametros": [
                    "codplantacao",
                    "dataleit_inicio",
                    "dataleit_fim",
                    "limit",
                    "offset",
                    "format (json ou xml; padrão json)",
                ],
                "observacao": (
                    "É obrigatório pelo menos um filtro entre codplantacao, "
                    "dataleit_inicio e dataleit_fim. Esta rota não usa API_TOKEN."
                ),
            }
            return self._send(start_response, "json", help_body, 200)

        try:
            payload = consulta_leituras_desde_strings(
                codplantacao_raw=cod,
                dataleit_inicio_raw=d_ini,
                dataleit_fim_raw=d_fim,
                limit=limit,
                offset=offset,
            )
        except ConsultaLeiturasError as e:
            err_obj = {"error": e.message}
            if e.http_status >= 500:
                err_obj["detail"] = e.detail
            return self._send(start_response, fmt, err_obj, e.http_status)

        if fmt == "json":
            body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
            start_response(
                "200 OK",
                [
                    ("Content-Type", "application/json; charset=utf-8"),
                    ("Content-Length", str(len(body))),
                ],
            )
            return [body]

        body = _payload_to_xml(payload)
        start_response(
            "200 OK",
            [
                ("Content-Type", "application/xml; charset=utf-8"),
                ("Content-Length", str(len(body))),
            ],
        )
        return [body]

    def _send(self, start_response, fmt, obj, status_code):
        status_lines = {
            200: "200 OK",
            400: "400 Bad Request",
            401: "401 Unauthorized",
            404: "404 Not Found",
            500: "500 Internal Server Error",
        }
        status = status_lines.get(status_code, f"{status_code} Error")

        if fmt == "xml":
            if isinstance(obj, dict) and "error" in obj:
                body = _xml_error(obj["error"], obj.get("detail"))
            elif isinstance(obj, dict) and "total" in obj:
                body = _payload_to_xml(obj)
            else:
                body = _xml_error(str(obj))
            start_response(
                status,
                [
                    ("Content-Type", "application/xml; charset=utf-8"),
                    ("Content-Length", str(len(body))),
                ],
            )
            return [body]

        body = json.dumps(obj, ensure_ascii=False).encode("utf-8")
        start_response(
            status,
            [
                ("Content-Type", "application/json; charset=utf-8"),
                ("Content-Length", str(len(body))),
            ],
        )
        return [body]
