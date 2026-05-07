"""Correlograma Bokeh para medições do bloco 'Medições Sensores' (estilo categorical correlogram)."""

from __future__ import annotations

from itertools import combinations
from typing import Any

import numpy as np
from bokeh.embed import components
from bokeh.models import ColumnDataSource, Div as BokehDiv, FixedTicker
from bokeh.plotting import figure
from bokeh.resources import CDN
from bokeh.transform import linear_cmap
from psycopg2.extras import RealDictCursor

from database import get_connection

# Mesmo conjunto do dashboard (Medições Sensores)
SENSOR_MEASURES: tuple[str, ...] = (
    "temp_solo",
    "temp_ar",
    "umid_solo",
    "umid_ar",
    "luz",
    "chuva",
    "umid_folha",
)


def correlogram_sensor_context() -> dict[str, Any]:
    cols_sql = ", ".join(SENSOR_MEASURES)
    sql = f"""
        SELECT {cols_sql}
        FROM public.leituras
    """

    conn = get_connection()
    try:
        with conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(sql)
                rows = cur.fetchall()
    finally:
        conn.close()

    n_rows = len(rows)
    if n_rows == 0:
        d = BokehDiv(
            text="<p>Não há linhas na tabela <code>leituras</code>.</p>",
            width=640,
            height=80,
        )
        script, div = components(d)
        return {
            "bokeh_resources": CDN.render(),
            "bokeh_script": script,
            "bokeh_div": div,
            "n_rows": 0,
            "n_pairs": 0,
        }

    arrays: dict[str, np.ndarray] = {}
    for name in SENSOR_MEASURES:
        raw = [row.get(name) for row in rows]
        arr = np.array(raw, dtype=float)
        arr[np.isclose(arr, -9999.0)] = np.nan
        arrays[name] = arr

    med_1: list[str] = []
    med_2: list[str] = []
    correlations: list[float] = []
    dot_sizes: list[float] = []

    for a, b in combinations(SENSOR_MEASURES, 2):
        xa, xb = arrays[a], arrays[b]
        mask = np.isfinite(xa) & np.isfinite(xb)
        if int(mask.sum()) < 2:
            continue
        xa_v = xa[mask]
        xb_v = xb[mask]
        if np.std(xa_v) == 0.0 or np.std(xb_v) == 0.0:
            continue
        corr = float(np.corrcoef(xa_v, xb_v)[0, 1])
        if np.isnan(corr):
            continue
        med_1.append(a)
        med_2.append(b)
        correlations.append(corr)
        dot_sizes.append((1.0 + 10.0 * abs(corr)) * 10.0)

    if not med_1:
        msg = (
            "Não foi possível calcular correlações (menos de 2 pontos válidos por par ou "
            "variância zero). Inclua leituras com valores numéricos distintos do sentinela -9999."
        )
        d = BokehDiv(text=f"<p style='max-width:640px'>{msg}</p>", width=640, height=120)
        script, div = components(d)
        return {
            "bokeh_resources": CDN.render(),
            "bokeh_script": script,
            "bokeh_div": div,
            "n_rows": n_rows,
            "n_pairs": 0,
        }

    source = ColumnDataSource(
        data=dict(
            med_1=med_1,
            med_2=med_2,
            correlation=correlations,
            dot_size=dot_sizes,
        )
    )

    x_range = list(SENSOR_MEASURES)
    y_range = list(SENSOR_MEASURES)

    p = figure(
        x_axis_location="above",
        toolbar_location=None,
        x_range=x_range,
        y_range=y_range,
        width=720,
        height=680,
        sizing_mode="stretch_width",
        title="Matriz de correlação — Medições Sensores (Pearson; sentinela -9999 ignorado)",
        background_fill_color="#fafafa",
    )

    renderer = p.scatter(
        x="med_1",
        y="med_2",
        size="dot_size",
        source=source,
        fill_color=linear_cmap("correlation", "RdYlGn9", -1.0, 1.0),
        line_color="#202020",
        alpha=0.92,
    )

    color_bar = renderer.construct_color_bar(
        location=(0, 0),
        ticker=FixedTicker(ticks=[-1.0, -0.5, 0.0, 0.5, 1.0]),
        title="correlação",
        major_tick_line_color=None,
        width=180,
        height=22,
    )
    p.add_layout(color_bar, "below")

    p.axis.major_tick_line_color = None
    p.axis.major_tick_out = 0
    p.axis.axis_line_color = None
    p.grid.grid_line_color = None
    p.outline_line_color = None
    p.xaxis.major_label_orientation = 0.9
    p.yaxis.major_label_orientation = 0

    script, div = components(p)

    return {
        "bokeh_resources": CDN.render(),
        "bokeh_script": script,
        "bokeh_div": div,
        "n_rows": n_rows,
        "n_pairs": len(med_1),
    }
