# Data Pipeline Modernization (Academic Capstone)

Modernization of a legacy CSV-based pipeline into a maintainable Python + SQL solution that feeds a centralized PostgreSQL-ready relational store. The project spotlights relational modeling, object-oriented design, Agile collaboration practices, and comprehensive automated tests that ensure downstream analytics receive trustworthy data.

## Architecture Highlights

- **Relational modeling**: `db/schema.sql` defines the `fact_orders` table optimized for analytic workloads with documented indexes and constraints.
- **Object-oriented pipeline**: `data_pipeline` package splits the flow into extraction (`CSVExtractor`), transformation (`OrderTransformer`), and loading (`OrderLoader`) stages that can be independently tested.
- **Configurability**: YAML-driven settings make it easy to target PostgreSQL, SQLite, or any SQLAlchemy-supported backend without code changes.
- **Quality gates**: Pytest suite covers the transformation logic and an end-to-end run against SQLite to guarantee regression-safe refactors.

## Getting Started

1. **Create a virtual environment**
   ```bash
   python3 -m venv .venv
   source .venv/bin/activate
   pip install -r requirements.txt
   ```
2. **Configure the pipeline**
   ```bash
   cp config/settings.example.yaml config/settings.yaml
   # Update db.dsn to your PostgreSQL DSN, e.g.
   # postgresql+psycopg://user:pass@localhost:5432/analytics
   ```
3. **Run the pipeline**
   ```bash
   python run_pipeline.py --config config/settings.yaml --log-level INFO
   ```

The sample dataset located at `data/raw/orders.csv` demonstrates the improved flow and can be replaced with real-world exports as needed.

## Testing

```bash
pytest
```

Tests include:
- Pure transformation validation (`tests/test_transformer.py`).
- Full-stack pipeline test that writes into a temporary SQLite warehouse, mirroring the production contract (`tests/test_pipeline.py`).

## Data Warehouse Schema

The schema in `db/schema.sql` was redesigned to separate analytical responsibilities and provide performant query paths. Apply it to your target database before running the loader:

```bash
psql $DSN -f db/schema.sql
```

## Agile + Collaboration Notes

- Work was broken into small, demonstrable increments (config scaffolding, ingestion logic, loader, docs).
- Each increment is covered by automated tests for rapid feedback.
- Documentation captures operational runbooks, enabling hand-offs across teammates.

## Next Steps

- Integrate orchestration (e.g., Airflow, Dagster) for production scheduling.
- Extend the loader with slowly changing dimensions or CDC capture for larger domains.
