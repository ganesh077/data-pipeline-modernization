from pathlib import Path

from sqlalchemy import create_engine, text

from data_pipeline.config import PipelineConfig
from data_pipeline.pipeline import ModernizedPipeline


def test_pipeline_loads_data_into_relational_store(tmp_path):
    raw_path = tmp_path / "orders.csv"
    raw_path.write_text(
        """order_id,customer_id,customer_name,customer_email,product_name,category,quantity,unit_price_usd,order_ts,status
1,C100,Jane Doe,jane@example.com,Course,Books,1,50.00,2024-01-01T10:00:00Z,SHIPPED
2,C200,John Doe,john@example.com,Course,Books,2,25.00,2024-01-02T11:00:00Z,DELIVERED
""",
        encoding="utf-8",
    )

    sqlite_path = tmp_path / "warehouse.db"
    sqlite_url = f"sqlite:///{sqlite_path}"
    config_path = tmp_path / "settings.yaml"
    config_path.write_text(
        f"""db:
  dsn: \"{sqlite_url}\"
  echo_sql: false
paths:
  raw_orders: \"{raw_path}\"
batch_size: 1
""",
        encoding="utf-8",
    )

    config = PipelineConfig.from_yaml(config_path)
    pipeline = ModernizedPipeline(config)

    assert pipeline.run() == 2

    engine = create_engine(sqlite_url, future=True)
    with engine.connect() as conn:
        count = conn.execute(text("SELECT COUNT(*) FROM fact_orders")).scalar_one()
        assert count == 2
