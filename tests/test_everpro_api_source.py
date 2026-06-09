from __future__ import annotations

import unittest

from dags.everpro_api_source import normalize_everpro_api_orders


class TestEverproApiSource(unittest.TestCase):
    def test_normalize_everpro_orders_maps_returned_order(self) -> None:
        payloads = [
            {
                "data": {
                    "statuses": [
                        {"id": "6", "name": "RETURN"},
                    ],
                    "orders": [
                        {
                            "awb_number": "JP123456789",
                            "created_at": "2026-05-01T10:00:00",
                            "rts_reasons": ["Penerima menolak paket"],
                            "rts_status": "REJECTED",
                            "is_cod": True,
                            "shipment": {
                                "status": "6",
                                "total_price": 18000,
                                "type": "regular",
                            },
                            "logistic": {
                                "name": "JNE",
                                "rate_type_name": "regular",
                            },
                            "receiver": {
                                "address_detail": {
                                    "province": "JAWA TIMUR",
                                    "city": "SURABAYA",
                                }
                            },
                            "shipper": {
                                "address_detail": {
                                    "province": "DKI JAKARTA",
                                }
                            },
                            "cod": {
                                "total": 125000,
                            },
                            "package": {
                                "price": 125000,
                            },
                        }
                    ]
                }
            }
        ]

        normalized = normalize_everpro_api_orders(payloads)
        self.assertEqual(len(normalized), 1)
        row = normalized[0]

        self.assertEqual(row["source_system"], "everpro_api")
        self.assertEqual(row["order_id"], "JP123456789")
        self.assertEqual(row["province"], "JAWA TIMUR")
        self.assertEqual(row["city"], "SURABAYA")
        self.assertEqual(row["expedition"], "JNE")
        self.assertEqual(row["service_type"], "Standard")
        self.assertEqual(row["payment_method"], "COD")
        self.assertEqual(row["cod_type"], "COD")
        self.assertEqual(row["return_flag"], 1)
        self.assertEqual(row["delivery_status"], "Returned")
        self.assertEqual(row["return_reason"], "Penerima menolak paket")

    def test_normalize_everpro_orders_maps_completed_order(self) -> None:
        payloads = [
            {
                "data": {
                    "statuses": [
                        {"id": "3", "name": "COMPLETED"},
                    ],
                    "orders": [
                        {
                            "shipment_order_no": "EVP-001",
                            "created_at": "2026-05-02T10:00:00",
                            "rts_reasons": [],
                            "rts_status": "",
                            "is_cod": False,
                            "shipment": {
                                "status": "3",
                                "price": 12000,
                                "type": "eco",
                            },
                            "logistic": {
                                "name": "SiCepat",
                                "rate_name": "eco",
                            },
                            "receiver": {
                                "address_detail": {
                                    "province": "BANTEN",
                                    "city": "TANGERANG",
                                }
                            },
                            "shipper": {
                                "address_detail": {
                                    "province": "DKI JAKARTA",
                                }
                            },
                            "cod": {
                                "total": 0,
                            },
                            "package": {
                                "price": 99000,
                            },
                        }
                    ]
                }
            }
        ]

        normalized = normalize_everpro_api_orders(payloads)
        self.assertEqual(len(normalized), 1)
        row = normalized[0]

        self.assertEqual(row["order_id"], "EVP-001")
        self.assertEqual(row["service_type"], "Eco")
        self.assertEqual(row["payment_method"], "NON-COD")
        self.assertEqual(row["return_flag"], 0)
        self.assertEqual(row["delivery_status"], "Delivered")
        self.assertEqual(row["return_reason"], "No Reason Provided")


if __name__ == "__main__":
    unittest.main()
