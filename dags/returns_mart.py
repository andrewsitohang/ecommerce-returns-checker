from __future__ import annotations

from typing import List

import psycopg2


def refresh_returns_marts_sql(
    conn: psycopg2.extensions.connection,
    *,
    staging_schema: str,
    mart_schema: str,
    staging_table: str,
    weekly_table: str,
    reason_table: str,
    driver_table: str,
    affected_weeks: List[tuple[int, int]],
) -> None:
    if not affected_weeks:
        return

    _create_empty_mart_tables(
        conn,
        mart_schema=mart_schema,
        weekly_table=weekly_table,
        reason_table=reason_table,
        driver_table=driver_table,
    )
    cur = conn.cursor()
    cur.execute('CREATE TEMP TABLE affected_return_weeks ("year" BIGINT, "week_of_year" BIGINT) ON COMMIT DROP')
    cur.executemany(
        'INSERT INTO affected_return_weeks ("year", "week_of_year") VALUES (%s, %s)',
        affected_weeks,
    )

    for table_name in [weekly_table, reason_table, driver_table]:
        cur.execute(
            f"""
            DELETE FROM "{mart_schema}"."{table_name}" m
            USING affected_return_weeks a
            WHERE m."year" = a."year"
              AND m."week_of_year" = a."week_of_year"
            """
        )

    base_sql = f"""
        FROM "{staging_schema}"."{staging_table}" s
        JOIN affected_return_weeks a
          ON EXTRACT(YEAR FROM NULLIF(s."event_date"::text, '')::date)::bigint = a."year"
         AND EXTRACT(WEEK FROM NULLIF(s."event_date"::text, '')::date)::bigint = a."week_of_year"
        WHERE COALESCE(NULLIF(s."eligible_shipment_flag"::text, ''), '0')::bigint = 1
          AND s."event_date" IS NOT NULL
          AND NULLIF(s."event_date"::text, '') IS NOT NULL
    """

    cur.execute(
        f"""
        INSERT INTO "{mart_schema}"."{weekly_table}"
        SELECT
            EXTRACT(YEAR FROM NULLIF(s."event_date"::text, '')::date)::bigint AS "year",
            EXTRACT(WEEK FROM NULLIF(s."event_date"::text, '')::date)::bigint AS "week_of_year",
            COALESCE(NULLIF(s."province"::text, ''), 'No Value') AS "province",
            COALESCE(NULLIF(s."city"::text, ''), 'No Value') AS "city",
            COALESCE(NULLIF(s."expedition"::text, ''), 'No Value') AS "expedition",
            COALESCE(NULLIF(s."service_type"::text, ''), 'No Value') AS "service_type",
            COALESCE(NULLIF(s."payment_method"::text, ''), 'No Value') AS "payment_method",
            COALESCE(NULLIF(s."cod_type"::text, ''), 'NON-COD') AS "cod_type",
            COUNT(*)::bigint AS "total_shipments",
            SUM(COALESCE(NULLIF(s."return_flag"::text, ''), '0')::bigint)::bigint AS "total_returns",
            SUM(COALESCE(NULLIF(s."order_value"::text, ''), '0')::double precision) AS "total_order_value",
            SUM(COALESCE(NULLIF(s."cod_value"::text, ''), '0')::double precision) AS "total_cod_value",
            SUM(COALESCE(NULLIF(s."shipping_fee"::text, ''), '0')::double precision) AS "total_shipping_fee",
            SUM(COALESCE(NULLIF(s."return_flag"::text, ''), '0')::bigint)::double precision / NULLIF(COUNT(*), 0) AS "return_rate"
        {base_sql}
        GROUP BY 1,2,3,4,5,6,7,8
        """
    )

    cur.execute(
        f"""
        INSERT INTO "{mart_schema}"."{reason_table}"
        WITH grouped AS (
            SELECT
                EXTRACT(YEAR FROM NULLIF(s."event_date"::text, '')::date)::bigint AS "year",
                EXTRACT(WEEK FROM NULLIF(s."event_date"::text, '')::date)::bigint AS "week_of_year",
                COALESCE(NULLIF(s."province"::text, ''), 'No Value') AS "province",
                COALESCE(NULLIF(s."city"::text, ''), 'No Value') AS "city",
                COALESCE(NULLIF(s."expedition"::text, ''), 'No Value') AS "expedition",
                COALESCE(NULLIF(s."service_type"::text, ''), 'No Value') AS "service_type",
                COALESCE(NULLIF(s."return_reason"::text, ''), 'No Reason Provided') AS "return_reason",
                COUNT(*)::bigint AS "total_shipments",
                SUM(COALESCE(NULLIF(s."return_flag"::text, ''), '0')::bigint)::bigint AS "total_returns"
            {base_sql}
            GROUP BY 1,2,3,4,5,6,7
        )
        SELECT
            "year",
            "week_of_year",
            "province",
            "city",
            "expedition",
            "service_type",
            "return_reason",
            "total_shipments",
            "total_returns",
            "total_returns"::double precision / NULLIF(
                SUM("total_returns") OVER (
                    PARTITION BY "year", "week_of_year", "province", "city", "expedition", "service_type"
                ),
                0
            ) AS "reason_share",
            "total_returns"::double precision / NULLIF(
                SUM("total_returns") OVER (
                    PARTITION BY "year", "week_of_year", "province", "city", "expedition", "service_type"
                ),
                0
            ) AS "return_rate"
        FROM grouped
        """
    )

    cur.execute(
        f"""
        INSERT INTO "{mart_schema}"."{driver_table}"
        WITH grouped AS (
            SELECT
                EXTRACT(YEAR FROM NULLIF(s."event_date"::text, '')::date)::bigint AS "year",
                EXTRACT(WEEK FROM NULLIF(s."event_date"::text, '')::date)::bigint AS "week_of_year",
                COALESCE(NULLIF(s."province"::text, ''), 'No Value') AS "province",
                COALESCE(NULLIF(s."city"::text, ''), 'No Value') AS "city",
                COALESCE(NULLIF(s."expedition"::text, ''), 'No Value') AS "expedition",
                'service_type'::text AS "driver_type",
                COALESCE(NULLIF(s."service_type"::text, ''), 'No Value') AS "driver_value",
                COUNT(*)::bigint AS "total_shipments",
                SUM(COALESCE(NULLIF(s."return_flag"::text, ''), '0')::bigint)::bigint AS "total_returns",
                SUM(COALESCE(NULLIF(s."order_value"::text, ''), '0')::double precision) AS "total_order_value"
            {base_sql}
            GROUP BY 1,2,3,4,5,6,7

            UNION ALL

            SELECT
                EXTRACT(YEAR FROM NULLIF(s."event_date"::text, '')::date)::bigint AS "year",
                EXTRACT(WEEK FROM NULLIF(s."event_date"::text, '')::date)::bigint AS "week_of_year",
                COALESCE(NULLIF(s."province"::text, ''), 'No Value') AS "province",
                COALESCE(NULLIF(s."city"::text, ''), 'No Value') AS "city",
                COALESCE(NULLIF(s."expedition"::text, ''), 'No Value') AS "expedition",
                'source_system'::text AS "driver_type",
                COALESCE(NULLIF(s."source_system"::text, ''), 'No Value') AS "driver_value",
                COUNT(*)::bigint AS "total_shipments",
                SUM(COALESCE(NULLIF(s."return_flag"::text, ''), '0')::bigint)::bigint AS "total_returns",
                SUM(COALESCE(NULLIF(s."order_value"::text, ''), '0')::double precision) AS "total_order_value"
            {base_sql}
            GROUP BY 1,2,3,4,5,6,7
        ),
        scored AS (
            SELECT
                grouped.*,
                SUM("total_shipments") OVER (
                    PARTITION BY "year", "week_of_year", "province", "city", "expedition", "driver_type"
                ) AS "group_total_shipments"
            FROM grouped
        )
        SELECT
            "year",
            "week_of_year",
            "province",
            "city",
            "expedition",
            "driver_type",
            "driver_value",
            "total_shipments",
            "total_returns",
            "total_order_value",
            "total_shipments"::double precision / NULLIF("group_total_shipments", 0) AS "shipments_share",
            "total_returns"::double precision / NULLIF("total_shipments", 0) AS "return_rate",
            DENSE_RANK() OVER (
                PARTITION BY "year", "week_of_year", "province", "city", "expedition", "driver_type"
                ORDER BY "total_returns"::double precision / NULLIF("total_shipments", 0) DESC
            )::bigint AS "rank_in_group"
        FROM scored
        """
    )

    conn.commit()
    cur.close()
    _create_reporting_indexes(
        conn,
        staging_schema=staging_schema,
        mart_schema=mart_schema,
        staging_table=staging_table,
        weekly_table=weekly_table,
        reason_table=reason_table,
        driver_table=driver_table,
    )


def _create_reporting_indexes(
    conn: psycopg2.extensions.connection,
    *,
    staging_schema: str,
    mart_schema: str,
    staging_table: str,
    weekly_table: str,
    reason_table: str,
    driver_table: str,
) -> None:
    cur = conn.cursor()
    cur.execute(
        f'CREATE INDEX IF NOT EXISTS idx_{staging_table}_source_order '
        f'ON "{staging_schema}"."{staging_table}" ("source_system", "order_id")'
    )
    cur.execute(
        f'CREATE INDEX IF NOT EXISTS idx_{staging_table}_event_date '
        f'ON "{staging_schema}"."{staging_table}" ("event_date")'
    )
    cur.execute(
        f'CREATE INDEX IF NOT EXISTS idx_{staging_table}_dims '
        f'ON "{staging_schema}"."{staging_table}" ("province", "city", "expedition", "service_type")'
    )
    cur.execute(
        f'CREATE INDEX IF NOT EXISTS idx_{weekly_table}_week_dims '
        f'ON "{mart_schema}"."{weekly_table}" ("year", "week_of_year", "province", "city", "expedition")'
    )
    cur.execute(
        f'CREATE INDEX IF NOT EXISTS idx_{reason_table}_week_dims '
        f'ON "{mart_schema}"."{reason_table}" ("year", "week_of_year", "province", "city", "expedition")'
    )
    cur.execute(
        f'CREATE INDEX IF NOT EXISTS idx_{driver_table}_week_dims '
        f'ON "{mart_schema}"."{driver_table}" ("year", "week_of_year", "province", "city", "expedition")'
    )
    conn.commit()
    cur.close()


def _create_empty_mart_tables(
    conn: psycopg2.extensions.connection,
    *,
    mart_schema: str,
    weekly_table: str,
    reason_table: str,
    driver_table: str,
) -> None:
    cur = conn.cursor()
    cur.execute(f'CREATE SCHEMA IF NOT EXISTS "{mart_schema}"')
    cur.execute(
        f"""
        CREATE TABLE IF NOT EXISTS "{mart_schema}"."{weekly_table}" (
            "year" BIGINT,
            "week_of_year" BIGINT,
            "province" TEXT,
            "city" TEXT,
            "expedition" TEXT,
            "service_type" TEXT,
            "payment_method" TEXT,
            "cod_type" TEXT,
            "total_shipments" BIGINT,
            "total_returns" BIGINT,
            "total_order_value" DOUBLE PRECISION,
            "total_cod_value" DOUBLE PRECISION,
            "total_shipping_fee" DOUBLE PRECISION,
            "return_rate" DOUBLE PRECISION
        )
        """
    )
    cur.execute(
        f"""
        CREATE TABLE IF NOT EXISTS "{mart_schema}"."{reason_table}" (
            "year" BIGINT,
            "week_of_year" BIGINT,
            "province" TEXT,
            "city" TEXT,
            "expedition" TEXT,
            "service_type" TEXT,
            "return_reason" TEXT,
            "total_shipments" BIGINT,
            "total_returns" BIGINT,
            "reason_share" DOUBLE PRECISION,
            "return_rate" DOUBLE PRECISION
        )
        """
    )
    cur.execute(f'ALTER TABLE "{mart_schema}"."{reason_table}" ADD COLUMN IF NOT EXISTS "reason_share" DOUBLE PRECISION')
    cur.execute(
        f"""
        CREATE TABLE IF NOT EXISTS "{mart_schema}"."{driver_table}" (
            "year" BIGINT,
            "week_of_year" BIGINT,
            "province" TEXT,
            "city" TEXT,
            "expedition" TEXT,
            "driver_type" TEXT,
            "driver_value" TEXT,
            "total_shipments" BIGINT,
            "total_returns" BIGINT,
            "total_order_value" DOUBLE PRECISION,
            "shipments_share" DOUBLE PRECISION,
            "return_rate" DOUBLE PRECISION,
            "rank_in_group" BIGINT
        )
        """
    )
    conn.commit()
    cur.close()
