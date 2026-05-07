from __future__ import annotations

from typing import Any, Dict

import numpy as np
from bokeh.embed import components
from bokeh.layouts import column as bokeh_column
from bokeh.models import BoxAnnotation, ColumnDataSource, Div, TeX, Whisker
from bokeh.plotting import figure
from bokeh.resources import CDN
from bokeh.transform import factor_cmap, jitter
from psycopg2.extras import RealDictCursor

from database import get_connection
from server.services.measures import allowed_numeric_cols


def grafcontroles_context(field: str, codplantacao: str | None) -> dict[str, Any]:
    allowed = {name: has_sentinel for (name, has_sentinel) in allowed_numeric_cols()}
    if field not in allowed:
        raise ValueError(f"Use uma destas medidas: {', '.join(sorted(allowed.keys()))}")
    has_sentinel = allowed[field]

    sql = f"""
        SELECT
            (dataleit::timestamp + horaleit) AS ts,
            codplantacao,
            {field} AS v
        FROM public.leituras
        WHERE dataleit >= (CURRENT_DATE - INTERVAL '30 days')
          AND {field} IS NOT NULL
          {"AND " + field + " <> -9999" if has_sentinel else ""}
        ORDER BY ts ASC
    """

    conn = get_connection()
    try:
        with conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(sql)
                rows = cur.fetchall()
    finally:
        conn.close()

    points_all = []
    for r in rows:
        ts = r.get("ts")
        cod = (r.get("codplantacao") or "").strip()
        v = r.get("v")
        if ts is None or v is None:
            continue
        try:
            fv = float(v)
        except Exception:
            continue
        points_all.append((ts, cod, fv))

    if codplantacao:
        points_filtered = [(t, c, v) for (t, c, v) in points_all if c == codplantacao]
    else:
        points_filtered = list(points_all)

    xs = [t for (t, _c, _v) in points_filtered]
    ys = [_v for (_t, _c, _v) in points_filtered]

    # Whisker (top) por codplantacao
    by_cod: Dict[str, list[float]] = {}
    for _ts, cod, v in points_all:
        by_cod.setdefault(cod or "(vazio)", []).append(v)
    classes = sorted(by_cod.keys())

    def _quantile(vals, q: float) -> float:
        vals = sorted(vals)
        if len(vals) == 1:
            return vals[0]
        pos = (len(vals) - 1) * q
        lo = int(pos)
        hi = min(lo + 1, len(vals) - 1)
        frac = pos - lo
        return vals[lo] * (1 - frac) + vals[hi] * frac

    upper = [_quantile(by_cod[c], 0.80) for c in classes] if classes else []
    lower = [_quantile(by_cod[c], 0.20) for c in classes] if classes else []

    p_whisk = figure(
        height=420,
        x_range=classes,
        sizing_mode="stretch_width",
        tools="pan,wheel_zoom,box_zoom,reset,save",
        title=f"{field} por codplantacao — últimos 30 dias (quantis 20%–80%)",
        background_fill_color="#f8fafc",
    )
    p_whisk.xgrid.grid_line_color = None
    p_whisk.xaxis.major_label_orientation = 1.0
    if classes:
        src = ColumnDataSource(data=dict(base=classes, upper=upper, lower=lower))
        whisk = Whisker(base="base", upper="upper", lower="lower", source=src, level="annotation", line_width=2)
        whisk.upper_head.size = 14
        whisk.lower_head.size = 14
        p_whisk.add_layout(whisk)

        scatter_src = ColumnDataSource(
            data=dict(
                cod=[c if c else "(vazio)" for (_t, c, _v) in points_all],
                v=[_v for (_t, _c, _v) in points_all],
            )
        )
        palette = ["#10b981", "#7c3aed", "#0ea5e9", "#f59e0b", "#ef4444", "#22c55e", "#6366f1"]
        p_whisk.scatter(
            jitter("cod", 0.35, range=p_whisk.x_range),
            "v",
            source=scatter_src,
            alpha=0.45,
            size=8,
            line_color="white",
            color=factor_cmap("cod", palette, classes),
        )
    p_whisk.yaxis.axis_label = field
    p_whisk.ygrid.grid_line_alpha = 0.35

    # Temporal + hist (filtráveis)
    p_time = figure(
        x_axis_type="datetime",
        height=420,
        sizing_mode="stretch_width",
        tools="pan,wheel_zoom,box_zoom,reset,save",
        title=f"{field} — últimos 30 dias",
    )
    if xs and ys and len(xs) == len(ys):
        p_time.line(xs, ys, line_width=2, color="#334155", legend_label=field)
        p_time.scatter(xs, ys, size=4, color="#10b981", alpha=0.8, legend_label="pontos")
        n = len(ys)
        avg = sum(ys) / n
        var = sum((v - avg) ** 2 for v in ys) / n
        std = var**0.5
        low_top = avg - std
        mid_top = avg + std
        p_time.add_layout(BoxAnnotation(top=low_top, fill_alpha=0.12, fill_color="#7c3aed"))
        p_time.add_layout(BoxAnnotation(bottom=low_top, top=mid_top, fill_alpha=0.10, fill_color="#10b981"))
        p_time.add_layout(BoxAnnotation(bottom=mid_top, fill_alpha=0.12, fill_color="#7c3aed"))
    p_time.legend.location = "top_left"
    p_time.legend.click_policy = "hide"
    p_time.xaxis.axis_label = "Tempo"
    p_time.yaxis.axis_label = field
    p_time.xgrid.grid_line_color = None
    p_time.ygrid.grid_line_alpha = 0.35

    p_hist = figure(height=420, sizing_mode="stretch_width", toolbar_location=None, title=f"Distribuição de {field}")
    div_math = Div(text=f"<div style='font-size:12px;color:#475569;'>Sem dados para histograma.</div>")
    if ys:
        arr = np.asarray(ys, dtype=float)
        xbar = float(arr.mean())
        sigma = float(arr.std()) if float(arr.std()) != 0.0 else 1.0
        scaled = (arr - xbar) / sigma
        bins = np.linspace(-3, 3, 40)
        hist, edges = np.histogram(scaled, density=True, bins=bins)
        p_hist.quad(top=hist, bottom=0, left=edges[:-1], right=edges[1:], fill_color="skyblue", line_color="white")
        x = np.linspace(-3.0, 3.0, 200)
        pdf = np.exp(-0.5 * x**2) / np.sqrt(2.0 * np.pi)
        p_hist.line(x, pdf, line_width=2, line_color="navy")
        p_hist.y_range.start = 0
        p_hist.xaxis.ticker = [-3, -2, -1, 0, 1, 2, 3]
        p_hist.xaxis.major_label_overrides = {
            -3: TeX(r"\overline{x} - 3\sigma"),
            -2: TeX(r"\overline{x} - 2\sigma"),
            -1: TeX(r"\overline{x} - \sigma"),
            0: TeX(r"\overline{x}"),
            1: TeX(r"\overline{x} + \sigma"),
            2: TeX(r"\overline{x} + 2\sigma"),
            3: TeX(r"\overline{x} + 3\sigma"),
        }
        div_math = Div(text=r"$$\qquad PDF(x) = \frac{1}{\sigma\sqrt{2\pi}} \exp\left[-\frac{1}{2}\left(\frac{x-\overline{x}}{\sigma}\right)^2 \right]$$")
    p_hist.xgrid.grid_line_color = None
    p_hist.ygrid.grid_line_alpha = 0.35

    layout = bokeh_column(p_whisk, bokeh_column(p_time, p_hist, div_math, sizing_mode="stretch_width"), sizing_mode="stretch_width")
    script, div = components(layout)

    return {
        "field": field,
        "cod_filter": codplantacao,
        "classes": classes,
        "has_data": bool(xs and ys and len(xs) == len(ys)),
        "bokeh_resources": CDN.render(),
        "bokeh_script": script,
        "bokeh_div": div,
    }

