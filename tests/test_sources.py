import sys
from pathlib import Path
from unittest.mock import MagicMock

import pandas as pd
import pytest

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.prepare_shipments import (
    DEFAULT_VOLUME_PATH,
    build_quality_report,
    prepare_shipments,
    resolve_raw_data_path,
    transform_shipments,
    validate_carriers,
    validate_warehouses,
    write_delta_tables,
)

DATA = Path(__file__).resolve().parents[1] / "data"


def _sample_frames() -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    warehouses = pd.DataFrame(
        {
            "warehouse_id": ["WH0001", "WH0002"],
            "warehouse_name": ["Warehouse 1", "Warehouse 2"],
            "region": ["North", "South"],
            "manager": ["Manager 1", "Manager 2"],
        }
    )
    carriers = pd.DataFrame(
        {
            "carrier": ["Carrier001", "Carrier002"],
            "service_type": ["Express", "Standard"],
            "base_cost": [100.0, 50.0],
            "contact_sla_hours": [24, 48],
        }
    )
    shipments = pd.DataFrame(
        {
            "shipment_id": ["SH001", "SH002", "SH003"],
            "order_date": ["2025-05-01", "2025-05-02", "2025-05-03"],
            "warehouse_id": ["WH0001", "WH0002", "WH0001"],
            "carrier": ["Carrier001", "Carrier002", "Carrier001"],
            "destination_city": ["Mumbai", "Pune", "Vizag"],
            "distance_km": [100.0, 200.0, 50.0],
            "weight_kg": [10.0, 20.0, 5.0],
            "shipping_cost": [300.0, 400.0, 150.0],
            "status": ["Delivered", "Delayed", "In Transit"],
            "promised_days": [3, 2, 4],
            "actual_days": [2, 5, 4],
            "delivery_date": ["2025-05-03", "2025-05-07", None],
        }
    )
    return shipments, warehouses, carriers


def test_source_files_have_at_least_500_rows():
    for path in DATA.glob("*.csv"):
        df = pd.read_csv(path)
        assert len(df) >= 500, f"{path.name} has only {len(df)} rows"


def test_source_files_are_readable():
    assert list(DATA.glob("*.csv")), "No CSV files found"


def test_resolve_raw_data_path_priority(monkeypatch):
    monkeypatch.delenv("RAW_DATA_PATH", raising=False)
    assert resolve_raw_data_path() == DEFAULT_VOLUME_PATH

    monkeypatch.setenv("RAW_DATA_PATH", "/Volumes/custom/schema/vol")
    assert resolve_raw_data_path() == "/Volumes/custom/schema/vol"
    assert resolve_raw_data_path(DATA) == DATA


def test_prepare_shipments_on_local_csv_data():
    df = prepare_shipments(raw_dir=DATA)

    assert len(df) == 1100
    assert df["shipment_id"].is_unique
    assert (df["delay_days"] >= 0).all()
    assert (df["is_late"] == (df["actual_days"] > df["promised_days"])).all()
    assert (df["cost_per_km"] > 0).all()

    in_transit = df[df["status"] == "In Transit"]
    assert len(in_transit) == 70
    assert in_transit["delivery_date"].isna().all()


def test_transform_shipments_preserves_in_transit_and_derives_columns():
    shipments, warehouses, carriers = _sample_frames()
    cleaned = transform_shipments(shipments, warehouses, carriers)

    assert len(cleaned) == 3
    assert cleaned["delay_days"].tolist() == [0, 3, 0]
    assert cleaned["is_late"].tolist() == [False, True, False]
    assert cleaned["cost_per_km"].tolist() == pytest.approx([3.0, 2.0, 3.0])
    assert pd.isna(cleaned.loc[2, "delivery_date"])


def test_transform_shipments_rejects_missing_delivery_for_completed_shipment():
    shipments, warehouses, carriers = _sample_frames()
    shipments.loc[0, "delivery_date"] = None

    with pytest.raises(ValueError, match="In Transit"):
        transform_shipments(shipments, warehouses, carriers)


def test_transform_shipments_rejects_duplicate_shipment_ids():
    shipments, warehouses, carriers = _sample_frames()
    shipments.loc[1, "shipment_id"] = "SH001"

    with pytest.raises(ValueError, match="unique"):
        transform_shipments(shipments, warehouses, carriers)


@pytest.mark.parametrize(
    "column,bad_value,pattern",
    [
        ("distance_km", 0, "distance_km"),
        ("weight_kg", -1.5, "weight_kg"),
        ("shipping_cost", -10.0, "shipping_cost"),
    ],
)
def test_transform_shipments_rejects_invalid_measures(column, bad_value, pattern):
    shipments, warehouses, carriers = _sample_frames()
    shipments.loc[0, column] = bad_value

    with pytest.raises(ValueError, match=pattern):
        transform_shipments(shipments, warehouses, carriers)


def test_transform_shipments_rejects_invalid_foreign_keys():
    shipments, warehouses, carriers = _sample_frames()
    shipments_bad_wh = shipments.copy()
    shipments_bad_wh.loc[0, "warehouse_id"] = "WH9999"
    with pytest.raises(ValueError, match="warehouse_id"):
        transform_shipments(shipments_bad_wh, warehouses, carriers)

    shipments_bad_carrier = shipments.copy()
    shipments_bad_carrier.loc[0, "carrier"] = "UnknownCarrier"
    with pytest.raises(ValueError, match="carrier"):
        transform_shipments(shipments_bad_carrier, warehouses, carriers)


def test_validate_dimensions_reject_duplicates_and_invalid_values():
    _, warehouses, carriers = _sample_frames()
    assert len(validate_warehouses(warehouses)) == 2
    assert len(validate_carriers(carriers)) == 2

    dup_wh = warehouses.copy()
    dup_wh.loc[1, "warehouse_id"] = "WH0001"
    with pytest.raises(ValueError, match="unique"):
        validate_warehouses(dup_wh)

    bad_carrier = carriers.copy()
    bad_carrier.loc[0, "contact_sla_hours"] = 0
    with pytest.raises(ValueError, match="contact_sla_hours"):
        validate_carriers(bad_carrier)


def test_build_quality_report():
    shipments, warehouses, carriers = _sample_frames()
    cleaned = transform_shipments(shipments, warehouses, carriers)
    report = build_quality_report(cleaned, warehouses, carriers)

    assert report == {
        "total_shipments": 3,
        "completed_shipments": 2,
        "in_transit_shipments": 1,
        "late_shipments": 1,
        "returned_shipments": 0,
        "warehouse_count": 2,
        "carrier_count": 2,
        "on_time_pct": 50.0,
    }


def test_write_delta_tables_targets_unity_catalog():
    shipments, warehouses, carriers = _sample_frames()
    cleaned = transform_shipments(shipments, warehouses, carriers)

    spark = MagicMock()
    written_tables = []

    def make_sdf(_pdf):
        sdf = MagicMock()
        writer = sdf.write
        fmt = writer.format.return_value
        mode = fmt.mode.return_value
        opt = mode.option.return_value
        opt.saveAsTable.side_effect = lambda name: written_tables.append(name)
        return sdf

    spark.createDataFrame.side_effect = make_sdf

    result = write_delta_tables(spark, cleaned, warehouses, carriers)

    spark.sql.assert_called_once_with("CREATE SCHEMA IF NOT EXISTS workspace.logistics")
    assert result == {
        "shipments_clean": "workspace.logistics.shipments_clean",
        "warehouses": "workspace.logistics.warehouses",
        "carriers": "workspace.logistics.carriers",
    }
    assert written_tables == [
        "workspace.logistics.shipments_clean",
        "workspace.logistics.warehouses",
        "workspace.logistics.carriers",
    ]
