from __future__ import annotations

from typing import Iterable

from sqlalchemy import Column, DateTime, Integer, MetaData, Numeric, String, Table, create_engine, delete

from .models import OrderRecord


class OrderLoader:
    """Persists curated data into the relational store."""

    def __init__(self, dsn: str, echo_sql: bool = False) -> None:
        self._engine = create_engine(dsn, echo=echo_sql, future=True)
        self._metadata = MetaData()
        self._orders = Table(
            "fact_orders",
            self._metadata,
            Column("order_id", Integer, primary_key=True),
            Column("customer_id", String(32), nullable=False),
            Column("customer_name", String(255), nullable=False),
            Column("customer_email", String(255), nullable=False),
            Column("product_name", String(255), nullable=False),
            Column("category", String(128), nullable=False),
            Column("quantity", Integer, nullable=False),
            Column("unit_price_usd", Numeric(12, 2), nullable=False),
            Column("total_price_usd", Numeric(12, 2), nullable=False),
            Column("order_ts", DateTime(timezone=True), nullable=False),
            Column("status", String(32), nullable=False),
        )
        self._metadata.create_all(self._engine)

    def load(self, records: Iterable[OrderRecord]) -> int:
        payload = [self._serialize(record) for record in records]
        if not payload:
            return 0
        ids = [row["order_id"] for row in payload]
        with self._engine.begin() as connection:
            connection.execute(delete(self._orders).where(self._orders.c.order_id.in_(ids)))
            connection.execute(self._orders.insert(), payload)
        return len(payload)

    @staticmethod
    def _serialize(record: OrderRecord) -> dict:
        return {
            "order_id": record.order_id,
            "customer_id": record.customer_id,
            "customer_name": record.customer_name,
            "customer_email": record.customer_email,
            "product_name": record.product_name,
            "category": record.category,
            "quantity": record.quantity,
            "unit_price_usd": float(record.unit_price_usd),
            "total_price_usd": float(record.total_price_usd),
            "order_ts": record.order_ts,
            "status": record.status,
        }
