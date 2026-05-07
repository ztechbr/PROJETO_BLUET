from __future__ import annotations

from collections import OrderedDict
from typing import Any

from psycopg2.extras import RealDictCursor
from flask import request

from database import get_connection
from leituras_query import fetch_recent_collects_preview
from server.services.measures import allowed_numeric_cols


def _client_ip() -> str | None:
    xff = (request.headers.get("X-Forwarded-For") or "").strip()
    if xff:
        return xff.split(",")[0].strip()
    return (request.remote_addr or "").strip() or None


def _to_float(v):
    if v is None:
        return None
    try:
        return float(v)
    except Exception:
        return None


def _round6(v) -> float | None:
    """Arredonda valores exibidos no dashboard para 6 casas decimais."""
    f = _to_float(v)
    if f is None:
        return None
    return round(f, 6)


def build_dashboard_context() -> tuple[dict[str, Any], int]:
    # DB health
    db_ok = False
    db_detail = None
    total_rows = 0
    numeric_stats: list[dict[str, Any]] = []

    try:
        conn = get_connection()
        try:
            with conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT 1")
                    cur.fetchone()
            db_ok = True
        finally:
            conn.close()
    except Exception as e:
        db_ok = False
        db_detail = str(e)

    if db_ok:
        numeric_cols = allowed_numeric_cols()

        def filt(col, has_sentinel):
            if has_sentinel:
                return f"{col} IS NOT NULL AND {col} <> -9999"
            return f"{col} IS NOT NULL"

        select_parts = ["COUNT(*)::int AS total_rows"]
        for col, has_sentinel in numeric_cols:
            w = filt(col, has_sentinel)
            select_parts.extend(
                [
                    f"MIN({col}) FILTER (WHERE {w}) AS {col}__min",
                    f"AVG({col}) FILTER (WHERE {w}) AS {col}__avg",
                    f"mode() WITHIN GROUP (ORDER BY {col}) FILTER (WHERE {w}) AS {col}__mode",
                    f"STDDEV_POP({col}) FILTER (WHERE {w}) AS {col}__stddev",
                    f"MAX({col}) FILTER (WHERE {w}) AS {col}__max",
                ]
            )
        sql = "SELECT " + ",\n       ".join(select_parts) + "\nFROM public.leituras"

        try:
            conn = get_connection()
            try:
                with conn:
                    with conn.cursor(cursor_factory=RealDictCursor) as cur:
                        cur.execute(sql)
                        row = cur.fetchone() or {}
            finally:
                conn.close()
            total_rows = int(row.get("total_rows") or 0)
            for col, _sent in numeric_cols:
                numeric_stats.append(
                    {
                        "name": col,
                        "min": _round6(row.get(f"{col}__min")),
                        "avg": _round6(row.get(f"{col}__avg")),
                        "mode": _round6(row.get(f"{col}__mode")),
                        "stddev": _round6(row.get(f"{col}__stddev")),
                        "max": _round6(row.get(f"{col}__max")),
                    }
                )
        except Exception as e:
            db_ok = False
            db_detail = str(e)

    # Agrupamentos de UI (mantendo as decisões do app.py anterior)
    latlon = {"lat", "lon"}
    sensor = {"temp_solo", "temp_ar", "umid_solo", "umid_ar", "luz", "chuva", "umid_folha"}
    comm = {"scomunicacao", "stensao", "scorrente", "spotencia"}
    rssi_list = {"rec_rssi_dbm", "distcalc_app"}

    latlon_stats = [s for s in numeric_stats if s["name"] in latlon]
    sensor_stats = [s for s in numeric_stats if s["name"] in sensor]
    rssi_stats = [s for s in numeric_stats if s["name"] in rssi_list]
    other_stats = [
        s
        for s in numeric_stats
        if s["name"] not in latlon and s["name"] not in sensor and s["name"] not in comm and s["name"] not in rssi_list
    ]

    # Comparativos: Bluetooth + categorias conjuntas (mantém SQL do app.py)
    comm_compare = []
    rssi_joint_compare = []
    if db_ok:
        try:
            conn = get_connection()
            try:
                with conn:
                    with conn.cursor(cursor_factory=RealDictCursor) as cur:
                        cur.execute(
                            """
                            SELECT
                                scomunicacao::int AS scom,
                                COUNT(*)::int AS n,
                                MIN(stensao) FILTER (WHERE stensao IS NOT NULL AND stensao <> -9999) AS stensao_min,
                                AVG(stensao) FILTER (WHERE stensao IS NOT NULL AND stensao <> -9999) AS stensao_avg,
                                STDDEV_POP(stensao) FILTER (WHERE stensao IS NOT NULL AND stensao <> -9999) AS stensao_stddev,
                                MAX(stensao) FILTER (WHERE stensao IS NOT NULL AND stensao <> -9999) AS stensao_max,
                                MIN(scorrente) FILTER (WHERE scorrente IS NOT NULL AND scorrente <> -9999) AS scorrente_min,
                                AVG(scorrente) FILTER (WHERE scorrente IS NOT NULL AND scorrente <> -9999) AS scorrente_avg,
                                STDDEV_POP(scorrente) FILTER (WHERE scorrente IS NOT NULL AND scorrente <> -9999) AS scorrente_stddev,
                                MAX(scorrente) FILTER (WHERE scorrente IS NOT NULL AND scorrente <> -9999) AS scorrente_max,
                                MIN(spotencia) FILTER (WHERE spotencia IS NOT NULL AND spotencia <> -9999) AS spotencia_min,
                                AVG(spotencia) FILTER (WHERE spotencia IS NOT NULL AND spotencia <> -9999) AS spotencia_avg,
                                STDDEV_POP(spotencia) FILTER (WHERE spotencia IS NOT NULL AND spotencia <> -9999) AS spotencia_stddev,
                                MAX(spotencia) FILTER (WHERE spotencia IS NOT NULL AND spotencia <> -9999) AS spotencia_max
                            FROM public.leituras
                            WHERE scomunicacao IS NOT NULL
                              AND scomunicacao <> -9999
                              AND scomunicacao IN (0, 1, 2)
                            GROUP BY scom
                            ORDER BY scom ASC
                            """
                        )
                        rows = cur.fetchall()
            finally:
                conn.close()

            label = {0: "ND", 1: "BTLowPower", 2: "BTNormal"}
            present = set()
            for r in rows:
                sc = int(r["scom"])
                present.add(sc)
                comm_compare.append(
                    {
                        "scom": sc,
                        "classe": label.get(sc, str(sc)),
                        "n": int(r["n"]),
                        "stensao": {
                            "min": _round6(r.get("stensao_min")),
                            "avg": _round6(r.get("stensao_avg")),
                            "stddev": _round6(r.get("stensao_stddev")),
                            "max": _round6(r.get("stensao_max")),
                        },
                        "scorrente": {
                            "min": _round6(r.get("scorrente_min")),
                            "avg": _round6(r.get("scorrente_avg")),
                            "stddev": _round6(r.get("scorrente_stddev")),
                            "max": _round6(r.get("scorrente_max")),
                        },
                        "spotencia": {
                            "min": _round6(r.get("spotencia_min")),
                            "avg": _round6(r.get("spotencia_avg")),
                            "stddev": _round6(r.get("spotencia_stddev")),
                            "max": _round6(r.get("spotencia_max")),
                        },
                    }
                )
            for sc in (0, 1, 2):
                if sc not in present:
                    comm_compare.append(
                        {
                            "scom": sc,
                            "classe": label.get(sc, str(sc)),
                            "n": 0,
                            "stensao": {"min": None, "avg": None, "stddev": None, "max": None},
                            "scorrente": {"min": None, "avg": None, "stddev": None, "max": None},
                            "spotencia": {"min": None, "avg": None, "stddev": None, "max": None},
                        }
                    )
            comm_compare.sort(key=lambda x: x["scom"])

            conn = get_connection()
            try:
                with conn:
                    with conn.cursor(cursor_factory=RealDictCursor) as cur:
                        cur.execute(
                            """
                            SELECT
                                ROUND(fator_n::numeric, 3) AS fator_n_r,
                                ROUND(ref_rssi_dbm::numeric, 1) AS ref_rssi_dbm_r,
                                COUNT(*)::int AS n,
                                AVG(rec_rssi_dbm) FILTER (WHERE rec_rssi_dbm IS NOT NULL AND rec_rssi_dbm <> -9999) AS rec_avg,
                                STDDEV_POP(rec_rssi_dbm) FILTER (WHERE rec_rssi_dbm IS NOT NULL AND rec_rssi_dbm <> -9999) AS rec_stddev,
                                AVG(distcalc_app) FILTER (WHERE distcalc_app IS NOT NULL AND distcalc_app <> -9999) AS dist_avg,
                                STDDEV_POP(distcalc_app) FILTER (WHERE distcalc_app IS NOT NULL AND distcalc_app <> -9999) AS dist_stddev
                            FROM public.leituras
                            WHERE fator_n IS NOT NULL AND fator_n <> -9999
                              AND ref_rssi_dbm IS NOT NULL AND ref_rssi_dbm <> -9999
                            GROUP BY fator_n_r, ref_rssi_dbm_r
                            ORDER BY n DESC
                            LIMIT 30
                            """
                        )
                        rows = cur.fetchall()
            finally:
                conn.close()

            for r in rows:
                rssi_joint_compare.append(
                    {
                        "fator_n": _round6(r.get("fator_n_r")),
                        "ref_rssi_dbm": _round6(r.get("ref_rssi_dbm_r")),
                        "n": int(r.get("n") or 0),
                        "rec_avg": _round6(r.get("rec_avg")),
                        "rec_stddev": _round6(r.get("rec_stddev")),
                        "dist_avg": _round6(r.get("dist_avg")),
                        "dist_stddev": _round6(r.get("dist_stddev")),
                    }
                )
        except Exception:
            # não derruba a página
            comm_compare = []
            rssi_joint_compare = []

    recent_collects_sections: list[dict[str, Any]] = []
    if db_ok:
        try:
            recent_collects = fetch_recent_collects_preview(5)
            groups: OrderedDict[str, list[dict[str, Any]]] = OrderedDict()
            for r in recent_collects:
                dk = r.get("dataleit")
                key = str(dk) if dk is not None else "—"
                if key not in groups:
                    groups[key] = []
                groups[key].append(r)
            recent_collects_sections = [
                {"dataleit": k, "rows": v} for k, v in groups.items()
            ]
        except Exception:
            recent_collects_sections = []

    status = 200 if db_ok else 503
    return (
        {
            "db_ok": db_ok,
            "db_detail": db_detail,
            "status_code": status,
            "client_ip": _client_ip(),
            "total_rows": total_rows,
            "recent_collects_sections": recent_collects_sections,
            "latlon_stats": latlon_stats,
            "sensor_stats": sensor_stats,
            "rssi_stats": rssi_stats,
            "rssi_joint_compare": rssi_joint_compare,
            "comm_compare": comm_compare,
            "other_stats": other_stats,
        },
        status,
    )

