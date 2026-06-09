from __future__ import annotations

import uuid
import unittest

import psycopg2

from dags.returns_mart import refresh_returns_marts_sql
from dags.returns_storage import ensure_schema, get_db_connection


class TestReturnsMart(unittest.TestCase):
    def setUp(self) -> None:
        self.conn = get_db_connection()
        self.schema_suffix = uuid.uuid4().hex[:8]
        self.staging_schema = f"test_staging_{self.schema_suffix}"
        self.mart_schema = f"test_mart_{self.schema_suffix}"
        self.staging_table = "stg_return_shipments"
        self.weekly_table = "fact_returns_weekly"
        self.reason_table = "fact_return_reason_weekly"
        self.driver_table = "fact_return_driver_weekly"

        ensure_schema(self.conn, self.staging_schema)
        ensure_schema(self.conn, self.mart_schema)

        cur = self.conn.cursor()
        cur.execute(
            f"""
            CREATE TABLE "{self.staging_schema}"."{self.staging_table}" (
                source_system TEXT,
                order_id TEXT,
                event_date TEXT,
                province TEXT,
                city TEXT,
                expedition TEXT,
                service_type TEXT,
                payment_method TEXT,
                cod_type TEXT,
                order_value TEXT,
                cod_value TEXT,
                shipping_fee TEXT,
                return_flag TEXT,
                return_reason TEXT,
                eligible_shipment_flag TEXT
            )
            """
        )
        rows = [
            ("spx_api", "A1", "2026-05-12", "JAWA BARAT", "BANDUNG", "SPX", "Reguler", "COD", "COD", "100000", "100000", "10000", "1", "Pesanan ditolak pembeli", "1"),
            ("spx_api", "A2", "2026-05-13", "JAWA BARAT", "BANDUNG", "SPX", "Reguler", "COD", "COD", "100000", "100000", "10000", "1", "Pesanan ditolak pembeli", "1"),
            ("spx_api", "A3", "2026-05-14", "JAWA BARAT", "BANDUNG", "SPX", "Reguler", "NON-COD", "NON-COD", "80000", "0", "8000", "1", "Alamat tidak lengkap", "1"),
            ("spx_api", "A4", "2026-05-15", "JAWA BARAT", "BANDUNG", "SPX", "Eco", "NON-COD", "NON-COD", "70000", "0", "7000", "0", "No Reason Provided", "1"),
            ("spx_api", "A5", "2026-05-16", "JAWA BARAT", "BANDUNG", "SPX", "Eco", "NON-COD", "NON-COD", "70000", "0", "7000", "1", "Kendala operasional jasa kirim", "0")
        ]
        cur.executemany(
            f'INSERT INTO "{self.staging_schema}"."{self.staging_table}" VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)',
            rows,
        )
        self.conn.commit()
        cur.close()

    def tearDown(self) -> None:
        cur = self.conn.cursor()
        cur.execute(f'DROP SCHEMA IF EXISTS "{self.staging_schema}" CASCADE')
        cur.execute(f'DROP SCHEMA IF EXISTS "{self.mart_schema}" CASCADE')
        self.conn.commit()
        cur.close()
        self.conn.close()

    def test_refresh_returns_marts_sql_builds_weekly_reason_and_driver_metrics(self) -> None:
        refresh_returns_marts_sql(
            self.conn,
            staging_schema=self.staging_schema,
            mart_schema=self.mart_schema,
            staging_table=self.staging_table,
            weekly_table=self.weekly_table,
            reason_table=self.reason_table,
            driver_table=self.driver_table,
            affected_weeks=[(2026, 20)],
        )

        cur = self.conn.cursor()

        cur.execute(
            f"""
            SELECT SUM(total_shipments), SUM(total_returns),
                   round((SUM(total_returns)::numeric / NULLIF(SUM(total_shipments), 0)), 4)
            FROM "{self.mart_schema}"."{self.weekly_table}"
            WHERE service_type = 'Reguler'
            """
        )
        weekly_reguler = cur.fetchone()
        self.assertEqual(weekly_reguler, (3, 3, 1.0000))

        cur.execute(
            f"""
            SELECT return_reason, total_returns, round(reason_share::numeric, 4)
            FROM "{self.mart_schema}"."{self.reason_table}"
            WHERE service_type = 'Reguler'
            ORDER BY total_returns DESC, return_reason
            """
        )
        reason_rows = cur.fetchall()
        self.assertEqual(reason_rows[0][0], "Pesanan ditolak pembeli")
        self.assertEqual(reason_rows[0][1], 2)
        self.assertEqual(float(reason_rows[0][2]), 0.6667)
        self.assertEqual(reason_rows[1][0], "Alamat tidak lengkap")
        self.assertEqual(reason_rows[1][1], 1)
        self.assertEqual(float(reason_rows[1][2]), 0.3333)

        cur.execute(
            f"""
            SELECT driver_value, total_shipments, total_returns,
                   round(shipments_share::numeric, 4), round(return_rate::numeric, 4)
            FROM "{self.mart_schema}"."{self.driver_table}"
            ORDER BY driver_value
            """
        )
        driver_rows = cur.fetchall()
        self.assertIn(("Eco", 1, 0, 0.2500, 0.0000), driver_rows)
        self.assertIn(("Reguler", 3, 3, 0.7500, 1.0000), driver_rows)

        cur.close()


if __name__ == "__main__":
    unittest.main()
