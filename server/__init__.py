from __future__ import annotations

import os

from dotenv import load_dotenv
from flasgger import Swagger
from flask import Flask

from server.controllers.mcp import mcp_bp
from server.controllers.rest import rest_bp, swagger_template
from server.controllers.web import web_bp
from server.middleware.auth import require_api_token_for_mcp, require_rest_api_token_for_leituras
from server.middleware.soap_fault import SoapFaultHttpMiddleware
from soap_gateway import SoapHttpGateway
from soap_service import soap_wsgi_app
from server.swagger_ui_head import swagger_auth_expiry_head_text


def _swagger_ui_auth_ttl_ms() -> int:
    raw = (os.getenv("SWAGGER_UI_AUTH_TTL_HOURS") or "24").strip()
    try:
        h = float(raw.replace(",", "."))
    except ValueError:
        h = 24.0
    if h <= 0:
        return 0
    return int(round(h * 3600 * 1000))


def create_app() -> Flask:
    load_dotenv()

    app = Flask(__name__, template_folder="templates", static_folder="static")
    app.config["JSON_SORT_KEYS"] = False

    # Swagger UI: operações expandidas, label de autorização ao lado do botão, tempo da requisição
    swagger_ui_ttl_ms = _swagger_ui_auth_ttl_ms()
    app.config["SWAGGER"] = {
        "doc_expansion": "full",
        # Flasgger emite `let auth_config = {{ config.auth | safe }}`; sem isso vira None (JS inválido) e o Swagger trava no loading.
        "auth": {},
        "ui_params": {
            "deepLinking": False,
            "displayRequestDuration": True,
            "persistAuthorization": True,
            "tryItOutEnabled": True,
        },
        "head_text": swagger_auth_expiry_head_text(swagger_ui_ttl_ms),
    }

    @app.template_filter("fmt6")
    def _fmt_dashboard_num(v):  # noqa: ANN001
        """Dashboard: sempre 6 casas decimais (ou traço)."""
        if v is None:
            return "—"
        try:
            return f"{float(v):.6f}"
        except (TypeError, ValueError):
            return "—"

    Swagger(app, template=swagger_template)

    # Blueprints
    app.register_blueprint(rest_bp)
    app.register_blueprint(web_bp)
    app.register_blueprint(mcp_bp)

    # Auth middlewares (before_request)
    app.before_request(require_rest_api_token_for_leituras)
    app.before_request(require_api_token_for_mcp)

    # WSGI chain: SOAP Fault middleware -> SOAP gateway (flask + spyne)
    app.wsgi_app = SoapFaultHttpMiddleware(SoapHttpGateway(app.wsgi_app, soap_wsgi_app))

    return app

