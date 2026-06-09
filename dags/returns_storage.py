from __future__ import annotations

import io
import json
import os
from datetime import datetime
from typing import Any, List, Optional

import pandas as pd
import psycopg2

try:
    from returns_config import env
except ImportError:  # pragma: no cover - package import path for tests
    from dags.returns_config import env


def get_db_connection() -> psycopg2.extensions.connection:
    return psycopg2.connect(
        host=env("DB_HOST"),
        port=env("DB_PORT"),
        dbname=env("DB_NAME"),
        user=env("DB_USER"),
        password=env("DB_PASSWORD"),
    )


def ensure_schema(conn: psycopg2.extensions.connection, schema: str) -> None:
    cur = conn.cursor()
    cur.execute(f'CREATE SCHEMA IF NOT EXISTS "{schema}"')
    conn.commit()
    cur.close()


def df_to_postgres(
    df: pd.DataFrame,
    table_name: str,
    conn: psycopg2.extensions.connection,
    schema: str,
    *,
    replace: bool = True,
    unique_keys: Optional[List[str]] = None,
) -> None:
    cur = conn.cursor()
    cur.execute(f'CREATE SCHEMA IF NOT EXISTS "{schema}"')

    column_defs = []
    for col in df.columns:
        dtype = df[col].dtype
        if pd.api.types.is_integer_dtype(dtype):
            col_type = "BIGINT"
        elif pd.api.types.is_float_dtype(dtype):
            col_type = "DOUBLE PRECISION"
        else:
            col_type = "TEXT"
        column_defs.append(f'"{col}" {col_type}')

    if replace:
        cur.execute(f'DROP TABLE IF EXISTS "{schema}"."{table_name}"')
        cur.execute(f'CREATE TABLE "{schema}"."{table_name}" ({", ".join(column_defs)})')
        buffer = io.StringIO()
        df.to_csv(buffer, index=False)
        buffer.seek(0)
        cur.copy_expert(f'COPY "{schema}"."{table_name}" FROM STDIN WITH CSV HEADER', buffer)
        conn.commit()
        cur.close()
        return

    temp_table = f"{table_name}__staging"
    cur.execute(f'DROP TABLE IF EXISTS "{schema}"."{temp_table}"')
    cur.execute(f'CREATE TABLE "{schema}"."{temp_table}" ({", ".join(column_defs)})')

    buffer = io.StringIO()
    df.to_csv(buffer, index=False)
    buffer.seek(0)
    cur.copy_expert(f'COPY "{schema}"."{temp_table}" FROM STDIN WITH CSV HEADER', buffer)

    cur.execute(f'CREATE TABLE IF NOT EXISTS "{schema}"."{table_name}" ({", ".join(column_defs)})')
    cur.execute(
        """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = %s AND table_name = %s
        ORDER BY ordinal_position
        """,
        (schema, table_name),
    )
    existing_cols = [row[0] for row in cur.fetchall()]
    if existing_cols != list(df.columns):
        cur.execute(f'DROP TABLE IF EXISTS "{schema}"."{table_name}"')
        cur.execute(f'CREATE TABLE "{schema}"."{table_name}" ({", ".join(column_defs)})')

    if not unique_keys:
        raise ValueError("unique_keys required for incremental upsert")

    key_match = " AND ".join([f't."{k}" = s."{k}"' for k in unique_keys])
    cur.execute(f'DELETE FROM "{schema}"."{table_name}" t USING "{schema}"."{temp_table}" s WHERE {key_match}')
    cur.execute(f'INSERT INTO "{schema}"."{table_name}" SELECT * FROM "{schema}"."{temp_table}"')
    cur.execute(f'DROP TABLE IF EXISTS "{schema}"."{temp_table}"')
    conn.commit()
    cur.close()


def append_df_to_postgres(
    df: pd.DataFrame,
    table_name: str,
    conn: psycopg2.extensions.connection,
    schema: str,
) -> None:
    cur = conn.cursor()
    cur.execute(f'CREATE SCHEMA IF NOT EXISTS "{schema}"')

    column_defs = []
    for col in df.columns:
        dtype = df[col].dtype
        if pd.api.types.is_integer_dtype(dtype):
            col_type = "BIGINT"
        elif pd.api.types.is_float_dtype(dtype):
            col_type = "DOUBLE PRECISION"
        else:
            col_type = "TEXT"
        column_defs.append((col, col_type))

    create_columns_sql = ", ".join(f'"{col}" {col_type}' for col, col_type in column_defs)
    cur.execute(f'CREATE TABLE IF NOT EXISTS "{schema}"."{table_name}" ({create_columns_sql})')
    cur.execute(
        """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = %s AND table_name = %s
        """,
        (schema, table_name),
    )
    existing_cols = {row[0] for row in cur.fetchall()}
    for col, col_type in column_defs:
        if col not in existing_cols:
            cur.execute(f'ALTER TABLE "{schema}"."{table_name}" ADD COLUMN "{col}" {col_type}')

    buffer = io.StringIO()
    df.to_csv(buffer, index=False)
    buffer.seek(0)
    columns_sql = ", ".join(f'"{col}"' for col in df.columns)
    cur.copy_expert(f'COPY "{schema}"."{table_name}" ({columns_sql}) FROM STDIN WITH CSV HEADER', buffer)
    conn.commit()
    cur.close()


def table_exists(conn: psycopg2.extensions.connection, schema: str, table_name: str) -> bool:
    cur = conn.cursor()
    cur.execute(
        """
        SELECT 1
        FROM information_schema.tables
        WHERE table_schema = %s AND table_name = %s
        """,
        (schema, table_name),
    )
    exists = cur.fetchone() is not None
    cur.close()
    return exists


def write_raw_payload(
    table_name: str,
    payload_obj: Any,
    raw_schema: str,
    *,
    fetch_start_date: Optional[str] = None,
    fetch_end_date: Optional[str] = None,
) -> None:
    conn = get_db_connection()
    ensure_schema(conn, raw_schema)
    payload_df = pd.DataFrame(
        [
            {
                "run_ts": datetime.utcnow().isoformat(),
                "fetch_start_date": fetch_start_date or "",
                "fetch_end_date": fetch_end_date or "",
                "payload": json.dumps(payload_obj, ensure_ascii=False),
            }
        ]
    )
    append_df_to_postgres(payload_df, table_name, conn, raw_schema)
    conn.close()


def read_raw_payload(table_name: str, raw_schema: str) -> List[dict[str, Any]]:
    conn = get_db_connection()
    if not table_exists(conn, raw_schema, table_name):
        conn.close()
        return []
    cur = conn.cursor()
    cur.execute(f'SELECT payload FROM "{raw_schema}"."{table_name}" ORDER BY run_ts DESC LIMIT 1')
    row = cur.fetchone()
    cur.close()
    conn.close()
    if not row:
        return []
    payload_obj = json.loads(row[0])
    return payload_obj if isinstance(payload_obj, list) else []
