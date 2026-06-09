from __future__ import annotations

import unittest

from dags import spx_api_source


class TestSpxApiSource(unittest.TestCase):
    def test_normalize_order_maps_location_service_payment_and_reason(self) -> None:
        item = {
            "base_info": {
                "product_name": "Economy Service",
            },
            "fulfillment_info": {
                "payment_role": 1,
                "cod_collection": 1,
                "cod_amount": 100000,
            },
            "deliver_info": {
                "deliver_state": "SULAWESI UTARA",
                "deliver_city": "KOTA KOTAMOBAGU",
            },
            "order_info": {
                "order_id": 11353120183,
                "tracking_code_group_name": "Return",
                "tracking_code_subgroup_name": "Returned",
                "estimated_shipping_fee": 38050,
                "ctime": 1779178283,
            },
            "tracking_info": {
                "returned_time": 1779267600,
                "latest_tracking_reason": "Pesanan ditolak pembeli",
            },
        }

        normalized = spx_api_source._normalize_order(item)

        self.assertEqual(normalized["source_system"], "spx_api")
        self.assertEqual(normalized["order_id"], "11353120183")
        self.assertEqual(normalized["province"], "SULAWESI UTARA")
        self.assertEqual(normalized["city"], "KOTA KOTAMOBAGU")
        self.assertEqual(normalized["service_type"], "Economy")
        self.assertEqual(normalized["payment_method"], "COD")
        self.assertEqual(normalized["raw_payment_method"], "Sender Paid")
        self.assertEqual(normalized["cod_type"], "COD")
        self.assertEqual(normalized["return_flag"], 1)
        self.assertEqual(normalized["return_reason"], "Pesanan ditolak pembeli")
        self.assertEqual(normalized["delivery_status"], "Return / Returned")

    def test_normalize_order_falls_back_to_non_cod(self) -> None:
        item = {
            "base_info": {
                "product_name": "Regular Service",
            },
            "fulfillment_info": {
                "payment_role": 1,
                "cod_collection": 0,
                "cod_amount": 0,
            },
            "deliver_info": {
                "deliver_state": "JAWA BARAT",
                "deliver_city": "KOTA BANDUNG",
            },
            "order_info": {
                "order_id": 11354470183,
                "tracking_code_group_name": "Delivered",
                "tracking_code_subgroup_name": "Delivered",
                "estimated_shipping_fee": 26975,
                "ctime": 1779178283,
            },
            "tracking_info": {
                "latest_tracking_reason": "",
            },
        }

        normalized = spx_api_source._normalize_order(item)

        self.assertEqual(normalized["service_type"], "Reguler")
        self.assertEqual(normalized["payment_method"], "NON-COD")
        self.assertEqual(normalized["cod_type"], "NON-COD")
        self.assertEqual(normalized["return_flag"], 0)
        self.assertEqual(normalized["return_reason"], "No Reason Provided")


if __name__ == "__main__":
    unittest.main()
