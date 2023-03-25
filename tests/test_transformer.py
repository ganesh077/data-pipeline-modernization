from decimal import Decimal

from data_pipeline.transform import OrderTransformer


def test_normalize_casts_types_and_enforces_casing():
    transformer = OrderTransformer()
    record = transformer.normalize(
        {
            "order_id": "101",
            "customer_id": "C900",
            "customer_name": "  Test User  ",
            "customer_email": "USER@EXAMPLE.COM ",
            "product_name": "Widget",
            "category": "Tools",
            "quantity": "2",
            "unit_price_usd": "10.155",
            "order_ts": "2024-01-01T12:00:00Z",
            "status": "delivered",
        }
    )

    assert record.order_id == 101
    assert record.customer_email == "user@example.com"
    assert record.unit_price_usd == Decimal("10.16")
    assert record.total_price_usd == Decimal("20.32")
    assert record.status == "DELIVERED"
