from __future__ import annotations

from xml.etree import ElementTree as ET


class SoapFaultHttpMiddleware:
    """
    Melhora a clareza de erros SOAP no HTTP.

    - Spyne costuma devolver SOAP Fault com HTTP 500 mesmo para falhas do cliente (faultcode Client).
    - Aqui, se detectarmos Fault 'Client' no corpo, devolvemos HTTP 400 e adicionamos headers de diagnóstico.
    - Não altera o envelope SOAP (somente status/headers).
    """

    _SOAP11_ENV = "http://schemas.xmlsoap.org/soap/envelope/"

    def __init__(self, wsgi_app):
        self.wsgi_app = wsgi_app

    def __call__(self, environ, start_response):
        path = (environ.get("PATH_INFO") or "").rstrip("/") or "/"
        method = (environ.get("REQUEST_METHOD") or "GET").upper()
        if path != "/soap" or method != "POST":
            return self.wsgi_app(environ, start_response)

        captured: dict = {"status": None, "headers": None, "exc_info": None}

        def _capture_start_response(status, headers, exc_info=None):
            captured["status"] = status
            captured["headers"] = list(headers)
            captured["exc_info"] = exc_info
            return None

        body_iter = self.wsgi_app(environ, _capture_start_response)
        chunks = []
        try:
            for c in body_iter:
                chunks.append(c)
        finally:
            close = getattr(body_iter, "close", None)
            if callable(close):
                close()

        body = b"".join(chunks)
        status = captured["status"] or "500 Internal Server Error"
        headers = captured["headers"] or []

        def _get_header(name):
            n = name.lower()
            for k, v in headers:
                if k.lower() == n:
                    return v
            return None

        content_type = (_get_header("Content-Type") or "").lower()
        is_xml = ("xml" in content_type) or body.lstrip().startswith(b"<")

        faultcode = None
        faultstring = None
        if is_xml and body:
            try:
                root = ET.fromstring(body)
                ns = {"soap": self._SOAP11_ENV}
                fault = root.find(".//soap:Fault", ns)
                if fault is None:
                    for el in root.iter():
                        if isinstance(el.tag, str) and el.tag.endswith("Fault"):
                            fault = el
                            break
                if fault is not None:
                    fc = fault.findtext("faultcode") or ""
                    fs = fault.findtext("faultstring") or ""
                    faultcode = fc.strip() or None
                    faultstring = fs.strip() or None
            except ET.ParseError:
                pass

        if (faultcode or "").lower().endswith(":client") or (faultcode or "").lower() == "client":
            status = "400 Bad Request"
            headers = [(k, v) for (k, v) in headers if k.lower() != "content-length"]
            headers.append(("X-SOAP-FaultCode", faultcode or "Client"))
            if faultstring:
                headers.append(("X-SOAP-FaultString", faultstring[:200]))
            headers.append(
                (
                    "X-SOAP-Debug-Help",
                    "É obrigatório informar pelo menos um filtro: codplantacao e/ou dataleit_inicio e/ou dataleit_fim",
                )
            )

        start_response(status, headers, captured.get("exc_info"))
        return [body]

