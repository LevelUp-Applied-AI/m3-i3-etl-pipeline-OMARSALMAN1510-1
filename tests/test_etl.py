"""Tests for the ETL pipeline.

Write at least 3 tests:
1. test_transform_filters_cancelled — cancelled orders excluded after transform
2. test_transform_filters_suspicious_quantity — quantities > 100 excluded
3. test_validate_catches_nulls — validate() raises ValueError on null customer_id
"""

import pandas as pd
import pytest

from etl_pipeline import transform, validate


def test_transform_filters_cancelled():
    """Create test DataFrames with a cancelled order. Confirm it's excluded."""
    customers = pd.DataFrame({
        "customer_id": [1],
        "customer_name": ["Omar"],
        "city": ["Amman"]
    })

    products = pd.DataFrame({
        "product_id": [101, 102],
        "product_name": ["Phone", "Laptop"],
        "category": ["Electronics", "Electronics"],
        "unit_price": [200.0, 500.0]
    })

    orders = pd.DataFrame({
        "order_id": [1001, 1002],
        "customer_id": [1, 1],
        "order_date": ["2026-01-01", "2026-01-02"],
        "status": ["completed", "cancelled"]
    })

    order_items = pd.DataFrame({
        "order_item_id": [1, 2],
        "order_id": [1001, 1002],
        "product_id": [101, 102],
        "quantity": [1, 1],
        "unit_price": [200.0, 500.0]
    })

    data_dict = {
        "customers": customers,
        "products": products,
        "orders": orders,
        "order_items": order_items,
    }

    result = transform(data_dict)

    assert len(result) == 1
    assert result.loc[0, "customer_id"] == 1
    assert result.loc[0, "total_orders"] == 1
    assert result.loc[0, "total_revenue"] == 200.0
    assert result.loc[0, "avg_order_value"] == 200.0


def test_transform_filters_suspicious_quantity():
    customers = pd.DataFrame({
        "customer_id": [1],
        "customer_name": ["Omar"],
        "city": ["Amman"]
    })

    products = pd.DataFrame({
        "product_id": [101, 102],
        "product_name": ["Phone", "Laptop"],
        "category": ["Electronics", "Electronics"],
        "unit_price": [200.0, 500.0]
    })

    orders = pd.DataFrame({
        "order_id": [1001, 1002],
        "customer_id": [1, 1],
        "order_date": ["2026-01-01", "2026-01-02"],
        "status": ["completed", "completed"]
    })

    order_items = pd.DataFrame({
        "order_item_id": [1, 2],
        "order_id": [1001, 1002],
        "product_id": [101, 102],
        "quantity": [1, 150],  
        "unit_price": [200.0, 500.0]
    })

    data_dict = {
        "customers": customers,
        "products": products,
        "orders": orders,
        "order_items": order_items,
    }

    result = transform(data_dict)

    assert len(result) == 1
    assert result.loc[0, "customer_id"] == 1
    assert result.loc[0, "total_orders"] == 1
    assert result.loc[0, "total_revenue"] == 200.0
    assert result.loc[0, "avg_order_value"] == 200.0


def test_validate_catches_nulls():
    bad_df = pd.DataFrame({
        "customer_id": [1, None],
        "customer_name": ["Omar", "Ali"],
        "city": ["Amman", "Zarqa"],
        "total_orders": [2, 1],
        "total_revenue": [300.0, 150.0],
        "avg_order_value": [150.0, 150.0],
        "top_category": ["Electronics", "Accessories"],
    })

    with pytest.raises(ValueError):
        validate(bad_df)