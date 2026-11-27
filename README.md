# Project 2 — Logistics & Delivery Performance Analytics (Databricks)

## Business scenario
A logistics company wants to understand why deliveries are late and where shipping costs are high. This pipeline ingests shipment, warehouse, and carrier data into Databricks Unity Catalog, enforces data quality checks, materializes Delta Lake tables, and exposes Databricks SQL KPI views across isolated `dev` and `prod` environments using Databricks Asset Bundles (DABs).

## Databricks Architecture & Environments
```text
CSV Files (warehouses.csv, shipments.csv, carriers.csv)
   │
   ▼
Unity Catalog Volume:
   ├── dev:  /Volumes/workspace/logistics_dev/raw_data/
   └── prod: /Volumes/workspace/logistics/raw_data/
   │
   ▼  Task 1: src/prepare_shipments.py (Serverless Python + Delta overwrite)
Unity Catalog Delta Tables:
   ├── <catalog>.<schema>.shipments_clean   (Fact table)
   ├── <catalog>.<schema>.warehouses        (Dimension table)
   └── <catalog>.<schema>.carriers          (Dimension table)
   │
   ▼  Task 2: sql/logistics_kpis.sql (Databricks SQL Warehouse)
Unity Catalog KPI Views (<catalog>.<schema>.kpi_*):
   ├── kpi_executive_summary
   ├── kpi_carrier_scorecard
   ├── kpi_warehouse_performance
   ├── kpi_destination_performance
   ├── kpi_top10_delayed_destinations
   ├── kpi_most_expensive_carriers
   └── kpi_monthly_trend
```

| Environment | Branch | Bundle Mode | Target Schema | Raw Volume Path |
|---|---|---|---|---|
| `dev` (default) | `develop` | `development` | `workspace.logistics_dev` | `/Volumes/workspace/logistics_dev/raw_data` |
| `prod` | `main` | `production` | `workspace.logistics` | `/Volumes/workspace/logistics/raw_data` |

## Unity Catalog Setup
Run once in a Databricks SQL Editor or notebook to provision both `dev` and `prod` schemas and managed Volumes:

```sql
CREATE CATALOG IF NOT EXISTS workspace;

-- Dev environment
CREATE SCHEMA IF NOT EXISTS workspace.logistics_dev;
CREATE VOLUME IF NOT EXISTS workspace.logistics_dev.raw_data;

-- Prod environment
CREATE SCHEMA IF NOT EXISTS workspace.logistics;
CREATE VOLUME IF NOT EXISTS workspace.logistics.raw_data;
```

Upload `data/shipments.csv`, `data/warehouses.csv`, and `data/carriers.csv` into each Volume via the Databricks CLI:

```bash
for vol in logistics_dev logistics; do
  databricks fs cp data/shipments.csv  dbfs:/Volumes/workspace/$vol/raw_data/shipments.csv  --overwrite
  databricks fs cp data/warehouses.csv dbfs:/Volumes/workspace/$vol/raw_data/warehouses.csv --overwrite
  databricks fs cp data/carriers.csv   dbfs:/Volumes/workspace/$vol/raw_data/carriers.csv   --overwrite
done
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

## Databricks Asset Bundle Deployment (DABs)
Bundle configuration lives in [databricks.yml](databricks.yml) and [resources/logistics_pipeline.job.yml](resources/logistics_pipeline.job.yml).

Validate, deploy, and run `dev`:
```bash
databricks bundle validate --strict -t dev --profile capstone_logistic_delivery
databricks bundle deploy -t dev --profile capstone_logistic_delivery
databricks bundle run logistics_delivery_pipeline -t dev --profile capstone_logistic_delivery
```

Validate, deploy, and run `prod`:
```bash
databricks bundle validate --strict -t prod --profile capstone_logistic_delivery
databricks bundle deploy -t prod --profile capstone_logistic_delivery
databricks bundle run logistics_delivery_pipeline -t prod --profile capstone_logistic_delivery
```

## GitHub Actions CI/CD & Required Secrets
The workflow in [.github/workflows/ci.yml](.github/workflows/ci.yml):
1. Runs `pytest` on every push and pull request to `main` and `develop`.
2. On pushes to `develop`, validates, deploys, and runs the **`dev`** DABs target (`workspace.logistics_dev`).
3. On pushes to `main`, validates, deploys, and runs the **`prod`** DABs target (`workspace.logistics`).

Configure these repository secrets in **GitHub Settings → Secrets and variables → Actions**:
- `DATABRICKS_HOST`: Workspace URL (e.g. `https://dbc-e8436b8e-0330.cloud.databricks.com`)
- `DATABRICKS_TOKEN`: Databricks Service Principal token (recommended for CI/CD) or workspace access token

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
├── .github/workflows/ci.yml               # CI tests + dev/prod DABs deployment
├── databricks.yml                         # Databricks Asset Bundle targets (dev/prod)
├── resources/
│   └── logistics_pipeline.job.yml         # Parameterized serverless multi-task job
├── data/                                  # Local sample/source CSVs (500+ rows each)
│   ├── carriers.csv
│   ├── shipments.csv
│   └── warehouses.csv
├── sql/
│   └── logistics_kpis.sql                 # Parameterized Databricks SQL KPI views
├── src/
│   └── prepare_shipments.py               # Validation, cleaning, and Delta writer
├── tests/
│   └── test_sources.py                    # Unit & data-contract tests
├── README.md
└── requirements.txt
```
