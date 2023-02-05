from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal


@dataclass(frozen=True)
class OrderRecord:
    order_id: int
    customer_id: str
    customer_name: str
    customer_email: str
    product_name: str
    category: str
    quantity: int
    unit_price_usd: Decimal
    total_price_usd: Decimal
    order_ts: datetime
    status: str
