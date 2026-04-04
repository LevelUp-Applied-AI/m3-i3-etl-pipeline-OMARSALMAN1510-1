"""ETL Pipeline — Amman Digital Market Customer Analytics

Extracts data from PostgreSQL, transforms it into customer-level summaries,
validates data quality, and loads results to a database table and CSV file.
"""

from sqlalchemy import create_engine
import pandas as pd
import os


def extract(engine):
    """Extract all source tables from PostgreSQL into DataFrames.

    Args:
        engine: SQLAlchemy engine connected to the amman_market database

    Returns:
        dict: {"customers": df, "products": df, "orders": df, "order_items": df}
    """
    customers = pd.read_sql("SELECT * FROM customers", engine)
    products = pd.read_sql("SELECT * FROM products", engine)
    orders = pd.read_sql("SELECT * FROM orders", engine)
    order_items = pd.read_sql("SELECT * FROM order_items", engine)

    return {
        "customers": customers,
        "products": products,
        "orders": orders,
        "order_items": order_items,
    }


def transform(data_dict):
    """Transform raw data into customer-level analytics summary.

    Steps:
    1. Join orders with order_items and products
    2. Compute line_total (quantity * unit_price)
    3. Filter out cancelled orders (status = 'cancelled')
    4. Filter out suspicious quantities (quantity > 100)
    5. Aggregate to customer level: total_orders, total_revenue,
       avg_order_value, top_category

    Args:
        data_dict: dict of DataFrames from extract()

    Returns:
        DataFrame: customer-level summary with columns:
            customer_id, customer_name, city, total_orders,
            total_revenue, avg_order_value, top_category
    """
    customers = data_dict["customers"].copy()
    products = data_dict["products"].copy()
    orders = data_dict["orders"].copy()
    order_items = data_dict["order_items"].copy()

    if "name" in customers.columns and "customer_name" not in customers.columns:
        customers = customers.rename(columns={"name": "customer_name"})

    if "name" in products.columns and "product_name" not in products.columns:
        products = products.rename(columns={"name": "product_name"})

    merged = orders.merge(order_items, on="order_id", how="inner")
    merged = merged.merge(products, on="product_id", how="left")

    merged = merged[merged["status"].str.lower() != "cancelled"].copy()

    merged = merged[merged["quantity"] <= 100].copy()

    merged["line_total"] = merged["quantity"] * merged["unit_price_x"]

    order_totals = (
        merged.groupby(["customer_id", "order_id"], as_index=False)["line_total"]
        .sum()
        .rename(columns={"line_total": "order_total"})
    )

    customer_order_stats = (
        order_totals.groupby("customer_id", as_index=False)
        .agg(
            total_orders=("order_id", "nunique"),
            total_revenue=("order_total", "sum"),
            avg_order_value=("order_total", "mean"),
        )
    )

    category_revenue = (
        merged.groupby(["customer_id", "category"], as_index=False)["line_total"]
        .sum()
        .rename(columns={"line_total": "category_revenue"})
    )

    category_revenue = category_revenue.sort_values(
        by=["customer_id", "category_revenue", "category"],
        ascending=[True, False, True]
    )

    top_category = (
        category_revenue.drop_duplicates(subset=["customer_id"], keep="first")
        [["customer_id", "category"]]
        .rename(columns={"category": "top_category"})
    )

    customer_cols = ["customer_id"]
    if "customer_name" in customers.columns:
        customer_cols.append("customer_name")
    if "city" in customers.columns:
        customer_cols.append("city")

    customer_base = customers[customer_cols].drop_duplicates(subset=["customer_id"])

    summary = customer_base.merge(customer_order_stats, on="customer_id", how="inner")
    summary = summary.merge(top_category, on="customer_id", how="left")

    if "customer_name" not in summary.columns:
        summary["customer_name"] = ""

    if "city" not in summary.columns:
        summary["city"] = ""

    summary["total_revenue"] = summary["total_revenue"].round(2)
    summary["avg_order_value"] = summary["avg_order_value"].round(2)

    summary = summary[
        [
            "customer_id",
            "customer_name",
            "city",
            "total_orders",
            "total_revenue",
            "avg_order_value",
            "top_category",
        ]
    ].sort_values(by="customer_id").reset_index(drop=True)

    return summary


def validate(df):
    """Run data quality checks on the transformed DataFrame.

    Checks:
    - No nulls in customer_id or customer_name
    - total_revenue > 0 for all customers
    - No duplicate customer_ids
    - total_orders > 0 for all customers

    Args:
        df: transformed customer summary DataFrame

    Returns:
        dict: {check_name: bool} for each check

    Raises:
        ValueError: if any critical check fails
    """
    checks = {
        "no_null_customer_id": df["customer_id"].notna().all(),
        "no_null_customer_name": df["customer_name"].notna().all(),
        "positive_total_revenue": (df["total_revenue"] > 0).all(),
        "unique_customer_id": not df["customer_id"].duplicated().any(),
        "positive_total_orders": (df["total_orders"] > 0).all(),
    }

    print("\nValidation Results:")
    for check_name, passed in checks.items():
        status = "PASS" if passed else "FAIL"
        print(f"- {check_name}: {status}")

    failed_checks = [name for name, passed in checks.items() if not passed]
    if failed_checks:
        raise ValueError(f"Validation failed for checks: {', '.join(failed_checks)}")

    return checks


def load(df, engine, csv_path):
    """Load customer summary to PostgreSQL table and CSV file.

    Args:
        df: validated customer summary DataFrame
        engine: SQLAlchemy engine
        csv_path: path for CSV output
    """
    output_dir = os.path.dirname(csv_path)
    if output_dir:
        os.makedirs(output_dir, exist_ok=True)

    df.to_sql("customer_summary", engine, if_exists="replace", index=False)

    df.to_csv(csv_path, index=False)

    print(f"\nLoaded {len(df)} rows to PostgreSQL table: customer_summary")
    print(f"Loaded CSV to: {csv_path}")


def main():
    """Orchestrate the ETL pipeline: extract -> transform -> validate -> load."""
    database_url = os.getenv(
        "DATABASE_URL",
        "postgresql+psycopg2://postgres:postgres@localhost:5432/amman_market"
    )

    print("Creating database engine...")
    engine = create_engine(database_url)

    print("Extracting source tables...")
    data_dict = extract(engine)
    for table_name, df in data_dict.items():
        print(f"- {table_name}: {df.shape[0]} rows, {df.shape[1]} columns")

    print("\nTransforming data...")
    customer_summary = transform(data_dict)
    print(f"Transformed customer summary: {customer_summary.shape[0]} rows, {customer_summary.shape[1]} columns")

    print("\nValidating data...")
    validate(customer_summary)

    print("\nLoading outputs...")
    csv_path = os.path.join("output", "customer_analytics.csv")
    load(customer_summary, engine, csv_path)

    print("\nETL pipeline completed successfully.")


if __name__ == "__main__":
    main()