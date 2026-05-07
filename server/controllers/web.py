from __future__ import annotations

from flask import Blueprint, jsonify, render_template, request

from server.services.dashboard import build_dashboard_context
from server.services.geo import top_cities_context
from server.services.graphs import grafcontroles_context


web_bp = Blueprint("web", __name__)


@web_bp.get("/")
def root():
    ctx, status = build_dashboard_context()
    return render_template("dashboard.html", **ctx), status


@web_bp.get("/grafcontroles")
def grafcontroles():
    field = (request.args.get("field") or "").strip()
    cod = (request.args.get("codplantacao") or "").strip() or None
    try:
        ctx = grafcontroles_context(field=field, codplantacao=cod)
    except ValueError as e:
        return jsonify({"error": "Medida inválida", "detail": str(e)}), 400
    return render_template("grafcontroles.html", **ctx)


@web_bp.get("/geocidade")
def geocidade():
    ctx = top_cities_context(top_n=4)
    return render_template("geocidade.html", **ctx)

