from __future__ import annotations

import os

import psycopg


def connect(
    *,
    database_url_env: str = "DATABASE_URL",
    connect_timeout_s: int = 10,
    application_name: str = "foodb",
) -> psycopg.Connection:
    database_url = os.environ.get(database_url_env)
    if not database_url:
        raise SystemExit(f"{database_url_env} is not set")
    return psycopg.connect(
        database_url,
        connect_timeout=connect_timeout_s,
        application_name=application_name,
    )
