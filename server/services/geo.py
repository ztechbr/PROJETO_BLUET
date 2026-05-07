from __future__ import annotations

from typing import Any

from psycopg2.extras import RealDictCursor

import reverse_geocode

from database import get_connection


def top_cities_context(top_n: int = 4) -> dict[str, Any]:
    sql = """
        SELECT
            ROUND(lat::numeric, 6) AS lat_r,
            ROUND(lon::numeric, 6) AS lon_r,
            COUNT(*)::int AS c
        FROM public.leituras
        WHERE lat IS NOT NULL AND lon IS NOT NULL
        GROUP BY lat_r, lon_r
        ORDER BY c DESC
        LIMIT 200
    """

    conn = get_connection()
    try:
        with conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(sql)
                rows = cur.fetchall()
    finally:
        conn.close()

    if not rows:
        return {"top_rows": [], "empty": True}

    coords = [(float(r["lat_r"]), float(r["lon_r"])) for r in rows]
    places = reverse_geocode.search(coords)

    agg: dict[tuple[str, str, str, str], dict[str, Any]] = {}
    for r, p in zip(rows, places):
        key = (
            (p.get("city") or "").strip() or "—",
            (p.get("state") or "").strip() or "",
            (p.get("country") or "").strip() or "",
            (p.get("country_code") or "").strip() or "",
        )
        a = agg.get(key)
        if a is None:
            a = {"count": 0, "examples": []}
            agg[key] = a
        a["count"] += int(r.get("c") or 0)
        if len(a["examples"]) < 3:
            a["examples"].append(
                {"lat": float(r["lat_r"]), "lon": float(r["lon_r"]), "n": int(r.get("c") or 0)}
            )

    top = sorted(agg.items(), key=lambda kv: kv[1]["count"], reverse=True)[:top_n]
    top_rows = []
    for (city, state, country, cc), info in top:
        top_rows.append(
            {
                "city": city,
                "state": state,
                "country": country,
                "cc": cc,
                "count": int(info["count"]),
                "examples": info["examples"],
            }
        )

    return {"top_rows": top_rows, "empty": False}

