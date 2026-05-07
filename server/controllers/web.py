from __future__ import annotations

from flask import Blueprint, jsonify, render_template, request

from leituras_query import fetch_leitura_completa_por_hash
from server.services.correlogram import correlogram_sensor_context
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


@web_bp.get("/matrizcorrelacao")
def matriz_correlacao():
    ctx = correlogram_sensor_context()
    return render_template("matriz_correlacao.html", **ctx)


@web_bp.get("/leitura/<hash_pk>")
def leitura_detalhe(hash_pk: str):
    row = fetch_leitura_completa_por_hash((hash_pk or "").strip())
    if row is None:
        return (
            render_template(
                "leitura_detalhe.html",
                not_found=True,
                hash_pk=hash_pk,
                row=None,
                field_items=[],
            ),
            404,
        )
    field_items = sorted(row.items(), key=lambda kv: kv[0])
    return render_template(
        "leitura_detalhe.html",
        not_found=False,
        hash_pk=row.get("hash_pk"),
        row=row,
        field_items=field_items,
    )

