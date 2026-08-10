from __future__ import annotations

import unittest
from unittest.mock import MagicMock, patch

from dags import returns_pipeline


class TestExtractRaw(unittest.TestCase):
    def setUp(self) -> None:
        patcher = patch.dict(
            "os.environ",
            {
                "SPX_API_SOURCE_ENABLED": "true",
                "EVERPRO_API_SOURCE_ENABLED": "true",
                "MENGANTAR_API_SOURCE_ENABLED": "true",
                "RETURNS_FETCH_START_DATE": "2026-01-01",
                "RETURNS_FETCH_END_DATE": "2026-01-31",
            },
        )
        patcher.start()
        self.addCleanup(patcher.stop)

    @patch("dags.returns_pipeline.write_raw_payload")
    @patch("dags.returns_pipeline.fetch_spx_api_records")
    def test_extract_spx_api_raw_fetches_and_writes_when_enabled(self, mock_fetch, mock_write):
        mock_fetch.return_value = [{"order_id": "A1"}]

        returns_pipeline.extract_spx_api_raw()

        mock_fetch.assert_called_once_with("2026-01-01", "2026-01-31")
        mock_write.assert_called_once_with(
            returns_pipeline.RAW_SPX_API_TABLE,
            [{"order_id": "A1"}],
            returns_pipeline.RAW_SCHEMA,
            fetch_start_date="2026-01-01",
            fetch_end_date="2026-01-31",
        )

    @patch("dags.returns_pipeline.write_raw_payload")
    @patch("dags.returns_pipeline.fetch_spx_api_records")
    def test_extract_spx_api_raw_skips_fetch_when_source_disabled(self, mock_fetch, mock_write):
        with patch.dict("os.environ", {"SPX_API_SOURCE_ENABLED": "false"}):
            returns_pipeline.extract_spx_api_raw()

        mock_fetch.assert_not_called()
        mock_write.assert_called_once_with(
            returns_pipeline.RAW_SPX_API_TABLE,
            [],
            returns_pipeline.RAW_SCHEMA,
            fetch_start_date="2026-01-01",
            fetch_end_date="2026-01-31",
        )

    @patch("dags.returns_pipeline.write_raw_payload")
    @patch("dags.returns_pipeline.fetch_everpro_api_payloads")
    def test_extract_everpro_api_raw_fetches_and_writes_when_enabled(self, mock_fetch, mock_write):
        mock_fetch.return_value = [{"data": {"list": []}}]

        returns_pipeline.extract_everpro_api_raw()

        mock_fetch.assert_called_once_with("2026-01-01", "2026-01-31")
        mock_write.assert_called_once_with(
            returns_pipeline.RAW_EVERPRO_API_TABLE,
            [{"data": {"list": []}}],
            returns_pipeline.RAW_SCHEMA,
            fetch_start_date="2026-01-01",
            fetch_end_date="2026-01-31",
        )

    @patch("dags.returns_pipeline.write_raw_payload")
    @patch("dags.returns_pipeline.fetch_mengantar_api_records")
    def test_extract_mengantar_api_raw_skips_fetch_when_source_disabled(
        self, mock_fetch, mock_write
    ):
        with patch.dict("os.environ", {"MENGANTAR_API_SOURCE_ENABLED": "false"}):
            returns_pipeline.extract_mengantar_api_raw()

        mock_fetch.assert_not_called()
        mock_write.assert_called_once_with(
            returns_pipeline.RAW_MENGANTAR_API_TABLE,
            [],
            returns_pipeline.RAW_SCHEMA,
            fetch_start_date="2026-01-01",
            fetch_end_date="2026-01-31",
        )


class TestBuildReturnsReportingTables(unittest.TestCase):
    @patch("dags.returns_pipeline.refresh_returns_marts_sql")
    @patch("dags.returns_pipeline.df_to_postgres")
    @patch("dags.returns_pipeline.read_raw_payload")
    @patch("dags.returns_pipeline.ensure_schema")
    @patch("dags.returns_pipeline.get_db_connection")
    def test_dedupes_by_order_and_computes_eligibility(
        self,
        mock_get_conn,
        mock_ensure_schema,
        mock_read_raw,
        mock_df_to_postgres,
        mock_refresh_marts,
    ):
        mock_conn = MagicMock()
        mock_get_conn.return_value = mock_conn

        # Two payloads for the same order_id: build_returns_reporting_tables
        # must keep only the last one (staging upsert semantics).
        spx_payloads = [
            {
                "source_system": "spx_api",
                "order_id": "A1",
                "event_date": "2026-05-12",
                "province": "JAWA BARAT",
                "city": "BANDUNG",
                "expedition": "SPX",
                "service_type": "Reguler",
                "payment_method": "COD",
                "cod_type": "COD",
                "order_value": "100000",
                "cod_value": "100000",
                "shipping_fee": "10000",
                "return_flag": 0,
                "return_reason": "",
                "delivery_status": "On Delivery",
            },
            {
                "source_system": "spx_api",
                "order_id": "A1",
                "event_date": "2026-05-12",
                "province": "JAWA BARAT",
                "city": "BANDUNG",
                "expedition": "SPX",
                "service_type": "Reguler",
                "payment_method": "COD",
                "cod_type": "COD",
                "order_value": "100000",
                "cod_value": "100000",
                "shipping_fee": "10000",
                "return_flag": 1,
                "return_reason": "Returned to sender",
                "delivery_status": "Returned",
            },
        ]

        def fake_read_raw_payload(table, schema):
            if table == returns_pipeline.RAW_SPX_API_TABLE:
                return spx_payloads
            return []

        mock_read_raw.side_effect = fake_read_raw_payload

        returns_pipeline.build_returns_reporting_tables()

        mock_ensure_schema.assert_any_call(mock_conn, returns_pipeline.RAW_SCHEMA)
        mock_ensure_schema.assert_any_call(mock_conn, returns_pipeline.STAGING_SCHEMA)
        mock_ensure_schema.assert_any_call(mock_conn, returns_pipeline.MART_SCHEMA)

        mock_df_to_postgres.assert_called_once()
        written_df = mock_df_to_postgres.call_args[0][0]
        self.assertEqual(len(written_df), 1)
        self.assertEqual(written_df.iloc[0]["return_flag"], 1)
        self.assertEqual(written_df.iloc[0]["delivery_status"], "Returned")
        # "Returned" is a final, non-cancelled status for a non-Everpro
        # source, so the surviving row must be marked eligible.
        self.assertEqual(written_df.iloc[0]["eligible_shipment_flag"], 1)

        mock_refresh_marts.assert_called_once()
        _, kwargs = mock_refresh_marts.call_args
        self.assertEqual(kwargs["affected_weeks"], [(2026, 20)])
        self.assertEqual(kwargs["staging_schema"], returns_pipeline.STAGING_SCHEMA)
        self.assertEqual(kwargs["mart_schema"], returns_pipeline.MART_SCHEMA)

        mock_conn.close.assert_called_once()

    @patch("dags.returns_pipeline.refresh_returns_marts_sql")
    @patch("dags.returns_pipeline.df_to_postgres")
    @patch("dags.returns_pipeline.read_raw_payload")
    @patch("dags.returns_pipeline.ensure_schema")
    @patch("dags.returns_pipeline.get_db_connection")
    def test_cancelled_shipment_is_not_eligible(
        self,
        mock_get_conn,
        mock_ensure_schema,
        mock_read_raw,
        mock_df_to_postgres,
        mock_refresh_marts,
    ):
        mock_get_conn.return_value = MagicMock()

        spx_payloads = [
            {
                "source_system": "spx_api",
                "order_id": "B1",
                "event_date": "2026-05-12",
                "province": "JAWA BARAT",
                "city": "BANDUNG",
                "expedition": "SPX",
                "service_type": "Reguler",
                "payment_method": "COD",
                "cod_type": "COD",
                "order_value": "100000",
                "cod_value": "100000",
                "shipping_fee": "10000",
                "return_flag": 0,
                "return_reason": "Dibatalkan oleh pembeli",
                "delivery_status": "Delivered",
            },
        ]
        mock_read_raw.side_effect = lambda table, schema: (
            spx_payloads if table == returns_pipeline.RAW_SPX_API_TABLE else []
        )

        returns_pipeline.build_returns_reporting_tables()

        written_df = mock_df_to_postgres.call_args[0][0]
        self.assertEqual(written_df.iloc[0]["is_cancelled"], 1)
        self.assertEqual(written_df.iloc[0]["eligible_shipment_flag"], 0)


class TestValidateReturnsOutputs(unittest.TestCase):
    def setUp(self) -> None:
        patcher = patch.dict(
            "os.environ",
            {
                "SPX_API_SOURCE_ENABLED": "true",
                "EVERPRO_API_SOURCE_ENABLED": "true",
                "MENGANTAR_API_SOURCE_ENABLED": "true",
                "VALIDATION_MIN_STAGING_ROWS": "100",
                "VALIDATION_MIN_ELIGIBLE_ROWS": "10",
                "VALIDATION_MAX_SPX_NO_VALUE_RATIO": "0.05",
            },
        )
        patcher.start()
        self.addCleanup(patcher.stop)

    def _make_conn(self, fetchone_results):
        mock_cur = MagicMock()
        mock_cur.fetchone.side_effect = fetchone_results
        mock_conn = MagicMock()
        mock_conn.cursor.return_value = mock_cur
        return mock_conn

    @patch("dags.returns_pipeline.get_db_connection")
    def test_passes_when_all_checks_are_within_thresholds(self, mock_get_conn):
        mock_get_conn.return_value = self._make_conn(
            [
                (150, 20),  # total_rows, eligible_rows
                (50,),  # spx_api row count
                (50,),  # everpro_api row count
                (50,),  # mengantar_api row count
                (150.0, 0.01, 0.01, 0.0),  # spx No Value ratios
                (10,),  # fact_returns_weekly row count
                (10,),  # fact_return_reason_weekly row count
                (10,),  # fact_return_driver_weekly row count
            ]
        )

        returns_pipeline.validate_returns_outputs()  # must not raise

    @patch("dags.returns_pipeline.get_db_connection")
    def test_raises_when_staging_rows_below_minimum(self, mock_get_conn):
        mock_get_conn.return_value = self._make_conn([(10, 5)])

        with self.assertRaisesRegex(ValueError, "staging rows too low"):
            returns_pipeline.validate_returns_outputs()

    @patch("dags.returns_pipeline.get_db_connection")
    def test_raises_when_eligible_rows_below_minimum(self, mock_get_conn):
        mock_get_conn.return_value = self._make_conn([(150, 2)])

        with self.assertRaisesRegex(ValueError, "eligible shipment rows too low"):
            returns_pipeline.validate_returns_outputs()

    @patch("dags.returns_pipeline.get_db_connection")
    def test_raises_when_enabled_source_has_no_rows(self, mock_get_conn):
        mock_get_conn.return_value = self._make_conn(
            [
                (150, 20),
                (0,),  # spx_api has zero rows despite being enabled
            ]
        )

        with self.assertRaisesRegex(ValueError, "no rows loaded for enabled source 'spx_api'"):
            returns_pipeline.validate_returns_outputs()

    @patch("dags.returns_pipeline.get_db_connection")
    def test_raises_when_spx_no_value_ratio_too_high(self, mock_get_conn):
        mock_get_conn.return_value = self._make_conn(
            [
                (150, 20),
                (50,),
                (50,),
                (50,),
                (150.0, 0.20, 0.01, 0.0),  # province No Value ratio 20% > 5% max
            ]
        )

        with self.assertRaisesRegex(ValueError, "No Value ratio too high"):
            returns_pipeline.validate_returns_outputs()

    @patch("dags.returns_pipeline.get_db_connection")
    def test_raises_when_a_mart_table_is_empty(self, mock_get_conn):
        mock_get_conn.return_value = self._make_conn(
            [
                (150, 20),
                (50,),
                (50,),
                (50,),
                (150.0, 0.01, 0.01, 0.0),
                (0,),  # fact_returns_weekly is empty
            ]
        )

        with self.assertRaisesRegex(ValueError, "mart table 'fact_returns_weekly' is empty"):
            returns_pipeline.validate_returns_outputs()


if __name__ == "__main__":
    unittest.main()
