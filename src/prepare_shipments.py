"""Databricks-native ETL pipeline for Logistics & Delivery Performance Analytics.

Reads raw CSV inputs (`warehouses.csv`, `shipments.csv`, `carriers.csv`) from a
configurable Unity Catalog Volume path (or local directory for testing),
applies pure-pandas validation and cleaning rules, and writes curated Delta
tables into Unity Catalog (`workspace.logistics`).
"""

from __future__ import annotations

import argparse
import os
from pathlib import Path
from typing import Any

import pandas as pd

DEFAULT_VOLUME_PATH = "/Volumes/workspace/logistics/raw_data"
DEFAULT_CATALOG = "workspace"
DEFAULT_SCHEMA = "logistics"

REQUIRED_WAREHOUSE_COLUMNS = {"warehouse_id", "warehouse_name", "region", "manager"}
REQUIRED_CARRIER_COLUMNS = {"carrier", "service_type", "base_cost", "contact_sla_hours"}
REQUIRED_SHIPMENT_COLUMNS = {
    "shipment_id",
    "order_date",
    "warehouse_id",
    "carrier",
    "destination_city",
    "distance_km",
    "weight_kg",
    "shipping_cost",
    "status",
    "promised_days",
    "actual_days",
    "delivery_date",
}


def resolve_raw_data_path(raw_dir: str | Path | None = None) -> str | Path:
    """Resolve the source directory from argument, env var, or UC Volume default."""
    if raw_dir is not None:
        return raw_dir
    env_path = os.getenv("RAW_DATA_PATH", "").strip()
    if env_path:
        return env_path
    return DEFAULT_VOLUME_PATH


def _validate_required_columns(df: pd.DataFrame, required: set[str], name: str) -> None:
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"{name} is missing required columns: {sorted(missing)}")


def _read_csv_source(
    raw_dir: str | Path,
    filename: str,
    parse_dates: list[str] | None = None,
    spark: Any | None = None,
) -> pd.DataFrame:
    """Read a CSV from a local directory or Unity Catalog Volume path."""
    if isinstance(raw_dir, Path):
        return pd.read_csv(raw_dir / filename, parse_dates=parse_dates)

    raw_str = str(raw_dir).rstrip("/\\")
    if raw_str.startswith("/Volumes/"):
        volume_file = f"{raw_str}/{filename}"
        try:
            return pd.read_csv(volume_file, parse_dates=parse_dates)
        except FileNotFoundError:
            if spark is None:
                raise
            pdf = (
                spark.read.option("header", True)
                .option("inferSchema", True)
                .csv(volume_file)
                .toPandas()
            )
            for col in parse_dates or []:
                if col in pdf.columns:
                    pdf[col] = pd.to_datetime(pdf[col], errors="coerce")
            return pdf

    return pd.read_csv(Path(raw_str) / filename, parse_dates=parse_dates)


def load_raw_data(
    raw_dir: str | Path | None = None,
    spark: Any | None = None,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Load raw shipments, warehouses, and carriers CSVs from Volume or local path."""
    resolved = resolve_raw_data_path(raw_dir)
    shipments = _read_csv_source(
        resolved,
        "shipments.csv",
        parse_dates=["order_date", "delivery_date"],
        spark=spark,
    )
    warehouses = _read_csv_source(resolved, "warehouses.csv", spark=spark)
    carriers = _read_csv_source(resolved, "carriers.csv", spark=spark)
    return shipments, warehouses, carriers


def validate_warehouses(warehouses_df: pd.DataFrame) -> pd.DataFrame:
    """Validate warehouse dimension integrity and return a clean copy."""
    _validate_required_columns(warehouses_df, REQUIRED_WAREHOUSE_COLUMNS, "warehouses")
    df = warehouses_df.copy()
    if df.empty:
        raise ValueError("warehouses dataset must not be empty")
    if df["warehouse_id"].isna().any():
        raise ValueError("warehouses contains null warehouse_id values")
    if not df["warehouse_id"].is_unique:
        raise ValueError("warehouse_id must be unique in warehouses")
    return df


def validate_carriers(carriers_df: pd.DataFrame) -> pd.DataFrame:
    """Validate carrier dimension integrity and return a clean copy."""
    _validate_required_columns(carriers_df, REQUIRED_CARRIER_COLUMNS, "carriers")
    df = carriers_df.copy()
    if df.empty:
        raise ValueError("carriers dataset must not be empty")
    if df["carrier"].isna().any():
        raise ValueError("carriers contains null carrier values")
    if not df["carrier"].is_unique:
        raise ValueError("carrier must be unique in carriers")
    if (df["base_cost"] < 0).any():
        raise ValueError("carriers base_cost must be non-negative")
    if (df["contact_sla_hours"] <= 0).any():
        raise ValueError("carriers contact_sla_hours must be positive")
    return df


def transform_shipments(
    shipments_df: pd.DataFrame,
    warehouses_df: pd.DataFrame,
    carriers_df: pd.DataFrame,
) -> pd.DataFrame:
    """Validate shipments against business rules and derive analytical metrics.

    Preserves 'In Transit' records with missing delivery_date values while
    enforcing uniqueness, positive measures, referential integrity, and
    non-negative delay calculations.
    """
    _validate_required_columns(shipments_df, REQUIRED_SHIPMENT_COLUMNS, "shipments")
    wh = validate_warehouses(warehouses_df)
    carriers = validate_carriers(carriers_df)

    df = shipments_df.copy()
    if df.empty:
        raise ValueError("shipments dataset must not be empty")

    df["order_date"] = pd.to_datetime(df["order_date"], errors="coerce")
    df["delivery_date"] = pd.to_datetime(df["delivery_date"], errors="coerce")

    if df["shipment_id"].isna().any():
        raise ValueError("shipments contains null shipment_id values")
    if not df["shipment_id"].is_unique:
        raise ValueError("shipment_id must be unique")
    if df["order_date"].isna().any():
        raise ValueError("shipments contains invalid or null order_date values")
    if df["status"].isna().any():
        raise ValueError("shipments contains null status values")

    non_transit_missing_delivery = (df["status"] != "In Transit") & df["delivery_date"].isna()
    if non_transit_missing_delivery.any():
        raise ValueError("Non-'In Transit' shipments must have a valid delivery_date")

    if (df["distance_km"] <= 0).any() or df["distance_km"].isna().any():
        raise ValueError("distance_km must be positive")
    if (df["weight_kg"] <= 0).any() or df["weight_kg"].isna().any():
        raise ValueError("weight_kg must be positive")
    if (df["shipping_cost"] < 0).any() or df["shipping_cost"].isna().any():
        raise ValueError("shipping_cost must be non-negative")
    if (df["promised_days"] <= 0).any() or df["promised_days"].isna().any():
        raise ValueError("promised_days must be positive")
    if (df["actual_days"] < 0).any() or df["actual_days"].isna().any():
        raise ValueError("actual_days must be non-negative")

    if not set(df["warehouse_id"]).issubset(set(wh["warehouse_id"])):
        raise ValueError("shipments contains unknown warehouse_id references")
    if not set(df["carrier"]).issubset(set(carriers["carrier"])):
        raise ValueError("shipments contains unknown carrier references")

    df["delay_days"] = (df["actual_days"] - df["promised_days"]).clip(lower=0)
    df["is_late"] = df["actual_days"] > df["promised_days"]
    df["cost_per_km"] = df["shipping_cost"] / df["distance_km"]
    return df


def build_quality_report(
    shipments_clean: pd.DataFrame,
    warehouses_df: pd.DataFrame,
    carriers_df: pd.DataFrame,
) -> dict[str, int | float]:
    """Compute a summary data quality report for the curated datasets."""
    completed = shipments_clean[shipments_clean["status"] != "In Transit"]
    on_time_count = int((~completed["is_late"]).sum()) if len(completed) else 0
    on_time_pct = round(100.0 * on_time_count / len(completed), 2) if len(completed) else 0.0
    return {
        "total_shipments": int(len(shipments_clean)),
        "completed_shipments": int(len(completed)),
        "in_transit_shipments": int((shipments_clean["status"] == "In Transit").sum()),
        "late_shipments": int(shipments_clean["is_late"].sum()),
        "returned_shipments": int((shipments_clean["status"] == "Returned").sum()),
        "warehouse_count": int(len(warehouses_df)),
        "carrier_count": int(len(carriers_df)),
        "on_time_pct": on_time_pct,
    }


def prepare_shipments(
    raw_dir: str | Path | None = None,
    spark: Any | None = None,
) -> pd.DataFrame:
    """Load raw CSVs from the configured path and return cleaned shipments."""
    shipments_df, warehouses_df, carriers_df = load_raw_data(raw_dir=raw_dir, spark=spark)
    return transform_shipments(shipments_df, warehouses_df, carriers_df)


def _prepare_pandas_for_spark(df: pd.DataFrame) -> pd.DataFrame:
    """Convert pandas datetime columns to Python date/None for clean Spark ingestion."""
    out = df.copy()
    for col in out.columns:
        if pd.api.types.is_datetime64_any_dtype(out[col]):
            out[col] = out[col].dt.date.where(out[col].notna(), None)
    return out


def write_delta_tables(
    spark: Any,
    shipments_clean: pd.DataFrame,
    warehouses_df: pd.DataFrame,
    carriers_df: pd.DataFrame,
    catalog: str = DEFAULT_CATALOG,
    schema: str = DEFAULT_SCHEMA,
) -> dict[str, str]:
    """Write curated datasets to Unity Catalog Delta tables using overwrite mode."""
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")

    targets = {
        "shipments_clean": (f"{catalog}.{schema}.shipments_clean", _prepare_pandas_for_spark(shipments_clean)),
        "warehouses": (f"{catalog}.{schema}.warehouses", _prepare_pandas_for_spark(warehouses_df)),
        "carriers": (f"{catalog}.{schema}.carriers", _prepare_pandas_for_spark(carriers_df)),
    }

    written: dict[str, str] = {}
    for key, (table_name, pdf) in targets.items():
        sdf = spark.createDataFrame(pdf)
        (
            sdf.write.format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .saveAsTable(table_name)
        )
        written[key] = table_name

    return written


def run_pipeline(
    raw_dir: str | Path | None = None,
    catalog: str = DEFAULT_CATALOG,
    schema: str = DEFAULT_SCHEMA,
    write_tables: bool = True,
    spark: Any | None = None,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, dict[str, int | float]]:
    """Execute the end-to-end load, validation, transformation, and Delta write."""
    if write_tables and spark is None:
        from pyspark.sql import SparkSession

        spark = SparkSession.builder.appName("logistics-prepare-shipments").getOrCreate()

    shipments_raw, warehouses_raw, carriers_raw = load_raw_data(raw_dir=raw_dir, spark=spark)
    warehouses_clean = validate_warehouses(warehouses_raw)
    carriers_clean = validate_carriers(carriers_raw)
    shipments_clean = transform_shipments(shipments_raw, warehouses_clean, carriers_clean)
    report = build_quality_report(shipments_clean, warehouses_clean, carriers_clean)

    if write_tables:
        write_delta_tables(
            spark=spark,
            shipments_clean=shipments_clean,
            warehouses_df=warehouses_clean,
            carriers_df=carriers_clean,
            catalog=catalog,
            schema=schema,
        )

    return shipments_clean, warehouses_clean, carriers_clean, report


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Prepare logistics data for Databricks Unity Catalog.")
    parser.add_argument(
        "--raw-path",
        default=None,
        help=f"Source directory or UC Volume path (defaults to RAW_DATA_PATH env var or {DEFAULT_VOLUME_PATH}).",
    )
    parser.add_argument("--catalog", default=os.getenv("DATABRICKS_CATALOG", DEFAULT_CATALOG))
    parser.add_argument("--schema", default=os.getenv("DATABRICKS_SCHEMA", DEFAULT_SCHEMA))
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Validate and transform data without writing Unity Catalog Delta tables.",
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = _parse_args()
    shipments_df, _, _, quality_report = run_pipeline(
        raw_dir=args.raw_path,
        catalog=args.catalog,
        schema=args.schema,
        write_tables=not args.dry_run,
    )
    print("Quality Report:", quality_report)
    print(shipments_df.groupby("carrier")["is_late"].mean().sort_values())
