from __future__ import annotations

import unittest

from dags import mengantar_api_source


class TestMengantarApiSource(unittest.TestCase):
    def test_normalize_order_maps_location_service_and_cod(self) -> None:
        item = {
            "sttNumber": "JNE0012345678",
            "courier": "JNE",
            "plan": "Standart JNE",
            "createdAt": "2026-06-01T08:00:00.000Z",
            "status": "Delivered",
            "receiver_province": "DKI JAKARTA",
            "receiver_city": "JAKARTA SELATAN",
            "cod_value": 50000,
        }

        normalized = mengantar_api_source._normalize_order(item)

        self.assertEqual(normalized["source_system"], "mengantar_api")
        self.assertEqual(normalized["order_id"], "JNE0012345678")
        self.assertEqual(normalized["province"], "DKI JAKARTA")
        self.assertEqual(normalized["city"], "JAKARTA SELATAN")
        self.assertEqual(normalized["expedition"], "JNE")
        self.assertEqual(normalized["payment_method"], "COD")
        self.assertEqual(normalized["cod_type"], "COD")
        self.assertEqual(normalized["return_flag"], 0)
        self.assertEqual(normalized["return_reason"], "No Reason Provided")

    def test_normalize_order_flags_return_status(self) -> None:
        item = {
            "sttNumber": "JNE0099999999",
            "courier": "JNE",
            "status": "Return To Sender",
            "receiver_province": "JAWA BARAT",
            "receiver_city": "KOTA BANDUNG",
            "cod_value": 0,
        }

        normalized = mengantar_api_source._normalize_order(item)

        self.assertEqual(normalized["payment_method"], "NON-COD")
        self.assertEqual(normalized["return_flag"], 1)
        self.assertEqual(normalized["return_reason"], "Return To Sender")


if __name__ == "__main__":
    unittest.main()
