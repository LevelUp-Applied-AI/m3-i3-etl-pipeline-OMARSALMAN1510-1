[![Review Assignment Due Date](https://classroom.github.com/assets/deadline-readme-button-22041afd0340ce965d47ae6ef1cefeee28c7c493a6346c4f15d667ab976d596c.svg)](https://classroom.github.com/a/Nvxy3054)

# ETL Pipeline — Amman Digital Market

## Overview

This project implements an end-to-end ETL (Extract, Transform, Load) pipeline using Python and PostgreSQL.

The pipeline:
- Extracts raw e-commerce data from a PostgreSQL database
- Transforms it into a clean, customer-level analytics dataset
- Validates data quality using business rules
- Loads the final result into both a database table and a CSV file

The goal is to generate meaningful customer insights such as:
- Total orders per customer
- Total revenue per customer
- Average order value
- Top product category per customer

---

## Setup

1. Start PostgreSQL container:
   ```bash
   docker start postgres-m3-int

Load schema and data:

docker exec -i postgres-m3-int psql -U postgres -d amman_market < schema.sql
docker exec -i postgres-m3-int psql -U postgres -d amman_market < seed_data.sql

Install dependencies:

pip install pandas sqlalchemy psycopg2-binary pytest
How to Run
python etl_pipeline.py

## Output

# The pipeline generates a customer-level analytics dataset stored in:

PostgreSQL table: customer_summary
CSV file: output/customer_analytics.csv
The dataset includes:
customer_id
customer_name
city
total_orders
total_revenue
avg_order_value
top_category

## Quality Checks

The pipeline performs the following validation checks:

No null values in customer_id
No null values in customer_name
All customers have total_revenue > 0
No duplicate customer_id
All customers have total_orders > 0

If any critical check fails, the pipeline raises a ValueError to prevent loading invalid data.

## Notes
Revenue is calculated using order_items.unit_price (price at purchase time)
Product table prices are not used for revenue calculation
Cancelled orders are excluded from analysis
Suspicious records with quantity > 100 are removed

## License

This repository is provided for educational use only. See LICENSE
 for terms.

You may clone and modify this repository for personal learning and practice, and reference code you wrote here in your professional portfolio. Redistribution outside this course is not permitted.