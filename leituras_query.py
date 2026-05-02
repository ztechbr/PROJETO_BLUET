from datetime import date, datetime, time
from decimal import Decimal

import psycopg2
from psycopg2.extras import RealDictCursor

from database import get_connection


class ConsultaLeiturasError(Exception):
    def __init__(self, message, http_status=400, detail=None):
        super().__init__(message)
        self.message = message
        self.http_status = http_status
        self.detail = detail if detail is not None else message


def _parse_date(value):
    if isinstance(value, date) and not isinstance(value, datetime):
        return value
    if isinstance(value, str):
        return date.fromisoformat(value)
    raise ValueError("dataleit inválida")


def _serialize_value(v):
    if v is None:
        return None
    if isinstance(v, datetime):
        return v.isoformat()
    if isinstance(v, date):
        return v.isoformat()
    if isinstance(v, time):
        return v.isoformat()
    if isinstance(v, Decimal):
        return float(v)
    return v


def _serialize_row(row):
    return {k: _serialize_value(v) for k, v in row.items()}


def consulta_leituras(
    codplantacao=None,
    dataleit_inicio=None,
    dataleit_fim=None,
    limit=100,
    offset=0,
):
    """
    codplantacao: str ou None (já normalizado).
    dataleit_inicio / dataleit_fim: date ou None.
    Mesmas regras do GET /leituras: pelo menos um filtro; limit 1–500; offset >= 0.
    Retorna dict com total, limit, offset, items (lista de dict).
    """
    if (
        codplantacao is None
        and dataleit_inicio is None
        and dataleit_fim is None
    ):
        raise ConsultaLeiturasError(
            "Informe pelo menos um filtro: codplantacao e/ou "
            "dataleit_inicio e/ou dataleit_fim"
        )

    if dataleit_inicio and dataleit_fim and dataleit_inicio > dataleit_fim:
        raise ConsultaLeiturasError(
            "dataleit_inicio não pode ser posterior a dataleit_fim"
        )

    if limit < 1 or limit > 500:
        raise ConsultaLeiturasError("limit deve ser entre 1 e 500")
    if offset < 0:
        raise ConsultaLeiturasError("offset deve ser >= 0")

    conds = []
    params = []
    if codplantacao is not None:
        conds.append("codplantacao = %s")
        params.append(codplantacao)
    if dataleit_inicio is not None:
        conds.append("dataleit >= %s")
        params.append(dataleit_inicio)
    if dataleit_fim is not None:
        conds.append("dataleit <= %s")
        params.append(dataleit_fim)

    where_sql = " AND ".join(conds)

    count_sql = f"SELECT COUNT(*) AS c FROM public.leituras WHERE {where_sql}"
    select_sql = f"""
        SELECT
            codplantacao,
            codleitura,
            lat,
            lon,
            dataleit,
            horaleit,
            temp_solo,
            temp_ar,
            umid_solo,
            umid_ar,
            luz,
            chuva,
            umid_folha,
            scomunicacao,
            stensao,
            scorrente,
            spotencia,
            hash_pk,
            status_blockchain,
            hash_blockchain,
            tx_hash,
            criadoem
        FROM public.leituras
        WHERE {where_sql}
        ORDER BY dataleit DESC, horaleit DESC
        LIMIT %s OFFSET %s
    """

    try:
        conn = get_connection()
    except Exception as e:
        raise ConsultaLeiturasError(
            "Falha ao conectar ao banco", http_status=500, detail=str(e)
        ) from e

    try:
        with conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(count_sql, params)
                total = cur.fetchone()["c"]
                cur.execute(select_sql, params + [limit, offset])
                rows = cur.fetchall()
    except psycopg2.Error as e:
        raise ConsultaLeiturasError(
            "Erro ao consultar", http_status=500, detail=str(e)
        ) from e
    finally:
        conn.close()

    items = [_serialize_row(r) for r in rows]
    return {
        "total": total,
        "limit": limit,
        "offset": offset,
        "items": items,
    }


def consulta_leituras_desde_strings(
    codplantacao_raw,
    dataleit_inicio_raw,
    dataleit_fim_raw,
    limit,
    offset,
):
    """
    Parâmetros como no GET: strings opcionais para cod e datas (YYYY-MM-DD).
    """
    cod = codplantacao_raw
    if cod is not None:
        cod = cod.strip()
        if cod == "":
            cod = None

    dataleit_inicio = None
    dataleit_fim = None
    if dataleit_inicio_raw:
        try:
            dataleit_inicio = _parse_date(dataleit_inicio_raw.strip())
        except (ValueError, TypeError):
            raise ConsultaLeiturasError(
                "dataleit_inicio inválida (use YYYY-MM-DD)"
            ) from None
    if dataleit_fim_raw:
        try:
            dataleit_fim = _parse_date(dataleit_fim_raw.strip())
        except (ValueError, TypeError):
            raise ConsultaLeiturasError(
                "dataleit_fim inválida (use YYYY-MM-DD)"
            ) from None

    return consulta_leituras(
        codplantacao=cod,
        dataleit_inicio=dataleit_inicio,
        dataleit_fim=dataleit_fim,
        limit=limit,
        offset=offset,
    )
