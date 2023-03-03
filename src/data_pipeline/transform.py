from __future__ import annotations

from datetime import datetime
from decimal import Decimal, ROUND_HALF_UP
from typing import Dict

from .models import OrderRecord


class OrderTransformer:
    """Cleanses and normalizes extracted payloads."""

    def normalize(self, record: Dict[str, str]) -> OrderRecord:
        quantity = int(record["quantity"])
        unit_price = Decimal(record["unit_price_usd"]).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
        total_price = (unit_price * quantity).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
        order_ts = self._parse_ts(record["order_ts"])

        return OrderRecord(
            order_id=int(record["order_id"]),
            customer_id=record["customer_id"].strip(),
            customer_name=record["customer_name"].strip(),
            customer_email=record["customer_email"].lower().strip(),
            product_name=record["product_name"].strip(),
            category=record["category"].strip(),
            quantity=quantity,
            unit_price_usd=unit_price,
            total_price_usd=total_price,
            order_ts=order_ts,
            status=record["status"].upper().strip(),
        )

    @staticmethod
    def _parse_ts(value: str) -> datetime:
        value = value.strip()
        if value.endswith("Z"):
            value = value.replace("Z", "+00:00")
        return datetime.fromisoformat(value)
