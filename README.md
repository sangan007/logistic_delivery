# Project 2 — Logistics & Delivery Performance Analytics (Databricks)

## Business scenario
A logistics company wants to understand why deliveries are late and where shipping costs are high. This pipeline ingests shipment, warehouse, and carrier data into Databricks Unity Catalog, enforces data quality checks, materializes Delta Lake tables, and exposes Databricks SQL KPI views for dashboards and operational recommendations.

## Databricks Architecture
```text
CSV Files (warehouses.csv, shipments.csv, carriers.csv)
   │
   ▼
Unity Catalog Volume: /Volumes/workspace/logistics/raw_data/
   │
   ▼  Task 1: src/prepare_shipments.py (Python + PySpark Delta write)
Unity Catalog Delta Tables (overwrite mode):
   ├── workspace.logistics.shipments_clean
   ├── workspace.logistics.warehouses
   └── workspace.logistics.carriers
   │
   ▼  Task 2: sql/logistics_kpis.sql (Databricks SQL)
Unity Catalog KPI Views (workspace.logistics.kpi_*):
   ├── kpi_executive_summary
   ├── kpi_carrier_scorecard
   ├── kpi_warehouse_performance
   ├── kpi_destination_performance
   ├── kpi_top10_delayed_destinations
   ├── kpi_most_expensive_carriers
   └── kpi_monthly_trend
```

## Unity Catalog Setup
Run the following once in a Databricks SQL Editor or notebook to provision the schema and managed Volume:

```sql
CREATE CATALOG IF NOT EXISTS workspace;
CREATE SCHEMA IF NOT EXISTS workspace.logistics;
CREATE VOLUME IF NOT EXISTS workspace.logistics.raw_data;
```

Upload the source CSV files (`data/shipments.csv`, `data/warehouses.csv`, `data/carriers.csv`) into:

```text
/Volumes/workspace/logistics/raw_data/
```

You can also upload via the Databricks CLI:

```bash
databricks fs cp data/shipments.csv dbfs:/Volumes/workspace/logistics/raw_data/shipments.csv --overwrite
databricks fs cp data/warehouses.csv dbfs:/Volumes/workspace/logistics/raw_data/warehouses.csv --overwrite
databricks fs cp data/carriers.csv dbfs:/Volumes/workspace/logistics/raw_data/carriers.csv --overwrite
```

## Business & Data Quality Rules
- **Shipment grain**: One row per `shipment_id` (unique primary key).
- **Dimensions**: `warehouse_id` must be unique in `warehouses`; `carrier` must be unique in `carriers` with non-negative `base_cost` and positive `contact_sla_hours`.
- **Measure checks**: `distance_km > 0`, `weight_kg > 0`, `shipping_cost >= 0`, `promised_days > 0`, and `actual_days >= 0`.
- **Referential integrity**: Every shipment `warehouse_id` and `carrier` must exist in the corresponding dimension table.
- **In-transit handling**: Shipments with `status = 'In Transit'` and a missing `delivery_date` are preserved (not deleted); all other statuses require a valid `delivery_date`.
- **Derived metrics**:
  - `delay_days = max(actual_days - promised_days, 0)`
  - `is_late = actual_days > promised_days`
  - `cost_per_km = shipping_cost / distance_km`

## Databricks Workflow Job Tasks
Create a multi-task Databricks Workflow (e.g., `logistics_delivery_pipeline`) backed by this Git repository:

1. **Task 1 — `prepare_shipments` (Python script task)**
   - **Source**: Git repository (`src/prepare_shipments.py`)
   - **Parameters / Environment**: Optional `RAW_DATA_PATH` environment variable or `--raw-path /Volumes/workspace/logistics/raw_data` (default).
   - **Output**: Overwrites Delta tables `workspace.logistics.shipments_clean`, `workspace.logistics.warehouses`, and `workspace.logistics.carriers`.

2. **Task 2 — `build_kpi_views` (SQL file task, depends on `prepare_shipments`)**
   - **Source**: Git repository (`sql/logistics_kpis.sql`)
   - **Warehouse**: Databricks SQL Warehouse
   - **Output**: Creates/updates all `workspace.logistics.kpi_*` views.

## GitHub Actions CI/CD & Required Secrets
The workflow in `.github/workflows/ci.yml`:
1. Runs `pytest` on every push and pull request to `main` and `develop`.
2. On pushes to `main`, installs the Databricks CLI via `databricks/setup-cli@main` and triggers the Databricks Workflow using `databricks jobs run-now`.

Configure these repository secrets in **GitHub Settings → Secrets and variables → Actions**:
- `DATABRICKS_HOST`: Workspace URL (e.g. `https://adb-1234567890123456.7.azuredatabricks.net` or `https://dbc-xxxxxxxx-xxxx.cloud.databricks.com`)
- `DATABRICKS_TOKEN`: Databricks Personal Access Token (PAT) or Service Principal OAuth token with permission to run the job
- `DATABRICKS_JOB_ID`: Numeric ID of the Databricks Workflow job

## Local Development & Testing
Install dependencies and run the local test suite without requiring a live Databricks cluster:

```bash
pip install -r requirements.txt pytest
pytest -v
```

Validate and preview the transformation locally against `data/` without writing Delta tables:

```bash
python src/prepare_shipments.py --raw-path data --dry-run
```

## Project Structure
```text
capstone_2_logistics_delivery/
├── .github/workflows/ci.yml   # CI tests + Databricks job trigger on main
├── data/                      # Local sample/source CSVs (500+ rows each)
│   ├── carriers.csv
│   ├── shipments.csv
│   └── warehouses.csv
├── sql/
│   └── logistics_kpis.sql     # Databricks SQL KPI views in workspace.logistics
├── src/
│   └── prepare_shipments.py   # Validation, cleaning, and Delta table writer
├── tests/
│   └── test_sources.py        # Unit & data-contract tests
├── README.md
└── requirements.txt
```
