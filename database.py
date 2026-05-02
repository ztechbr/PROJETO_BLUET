import os

import psycopg2


def get_connection():
    url = os.getenv("DATABASE_URL")
    if url:
        return psycopg2.connect(url)
    host = os.getenv("DB_HOST")
    name = os.getenv("DB_NAME")
    user = os.getenv("DB_USER")
    password = os.getenv("DB_PASSWORD", "")
    port = os.getenv("DB_PORT", "5432")
    if not all([host, name, user is not None]):
        raise RuntimeError(
            "Configure DATABASE_URL ou DB_HOST, DB_NAME, DB_USER (e opcionalmente DB_PASSWORD, DB_PORT) no .env"
        )
    return psycopg2.connect(
        host=host,
        port=port,
        dbname=name,
        user=user,
        password=password,
    )
