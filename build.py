#!/usr/bin/env python3
"""Promotion Impact Pipeline build script (Raw -> Staging -> Curated + tests + reconciliation).
This script keeps the full pipeline in one file:
- Raw load from CSV (idempotent)
- Staging + Curated build (embedded SQL)
- Embedded checks SQL (no separate checks.sql file)
- Fail-fast gate when any check fails
- Reproducibility: curated_checksum (MD5 of ordered fact rows)

Run:
  python build.py
  python build.py --db promotion.duckdb
  python build.py --show-all-checks
"""
from __future__ import annotations

import argparse
from datetime import datetime
from pathlib import Path

import duckdb

# -----------------------------------------------------------------------------
# Paths
# -----------------------------------------------------------------------------
HERE = Path(__file__).resolve().parent
DATA_DIR = HERE / "data" / "raw"

# -----------------------------------------------------------------------------
# Schema
# -----------------------------------------------------------------------------
SCHEMA_SQL = r"""
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS curated;
"""

# -----------------------------------------------------------------------------
# Staging
# -----------------------------------------------------------------------------
STAGING_SQL = r"""
CREATE OR REPLACE TABLE staging.orders AS
SELECT
    TRIM(order_id) AS order_id,
    TRIM(customer_id) AS customer_id,
    LOWER(TRIM(order_status)) AS order_status,
    CAST(order_purchase_timestamp AS TIMESTAMP) AS order_purchase_timestamp,
    CAST(order_approved_at AS TIMESTAMP) AS order_approved_at,
    CAST(order_delivered_customer_date AS TIMESTAMP) AS order_delivered_customer_date,
    CAST(order_estimated_delivery_date AS TIMESTAMP) AS order_estimated_delivery_date
FROM raw_orders
WHERE order_id IS NOT NULL
  AND order_purchase_timestamp IS NOT NULL
  AND LOWER(TRIM(order_status)) IN ('delivered', 'shipped', 'in transit', 'processing', 'approved');

CREATE OR REPLACE TABLE staging.order_items AS
SELECT
    TRIM(order_id) AS order_id,
    order_item_id,
    TRIM(product_id) AS product_id,
    TRIM(seller_id) AS seller_id,
    CAST(price AS DOUBLE) AS price,
    CAST(freight_value AS DOUBLE) AS freight_value,
    CAST(price AS DOUBLE) + CAST(freight_value AS DOUBLE) AS revenue_per_item
FROM raw_order_items
WHERE order_id IS NOT NULL AND order_item_id IS NOT NULL;

CREATE OR REPLACE TABLE staging.campaigns AS
SELECT campaign_id, campaign_name, window_start, window_end, discount_pct, coupon_code
FROM (
    SELECT
        TRIM(campaign_id) AS campaign_id,
        LOWER(TRIM(campaign_name)) AS campaign_name,
        CAST(window_start AS DATE) AS window_start,
        CAST(window_end AS DATE) AS window_end,
        discount_pct,
        LOWER(TRIM(COALESCE(coupon_code, ''))) AS coupon_code,
        ROW_NUMBER() OVER (PARTITION BY TRIM(campaign_id) ORDER BY channel) AS rn
    FROM raw_campaigns
    WHERE campaign_id IS NOT NULL
) t
WHERE rn = 1;
"""

# -----------------------------------------------------------------------------
# Curated
# -----------------------------------------------------------------------------
CURATED_SQL = r"""
CREATE OR REPLACE TABLE curated.campaigns_dim AS
SELECT
    DENSE_RANK() OVER (ORDER BY campaign_id) AS campaign_sk,
    campaign_id,
    campaign_name,
    window_start,
    window_end,
    discount_pct,
    coupon_code
FROM staging.campaigns;

CREATE OR REPLACE TABLE curated.customers_dim AS
SELECT
    DENSE_RANK() OVER (ORDER BY customer_id) AS customer_sk,
    customer_id
FROM (SELECT DISTINCT customer_id FROM staging.orders WHERE customer_id IS NOT NULL) o;

CREATE OR REPLACE TABLE curated.curated_revenue_fact AS
WITH order_revenue AS (
    SELECT
        order_id,
        SUM(price + freight_value) AS order_revenue,
        SUM(price) AS order_price_sum,
        SUM(freight_value) AS order_freight_sum
    FROM staging.order_items
    WHERE order_id IS NOT NULL
    GROUP BY order_id
)
SELECT
    o.order_id,
    o.order_purchase_timestamp,
    cd.customer_sk,
    camp.campaign_sk,
    camp.campaign_id,
    camp.campaign_name,
    camp.discount_pct AS campaign_discount_pct,
    ord.order_revenue,
    ord.order_price_sum,
    ord.order_freight_sum
FROM staging.orders o
INNER JOIN order_revenue ord ON o.order_id = ord.order_id
LEFT JOIN curated.campaigns_dim camp
    ON CAST(o.order_purchase_timestamp AS DATE) BETWEEN camp.window_start AND camp.window_end
LEFT JOIN curated.customers_dim cd ON o.customer_id = cd.customer_id;
"""

# -----------------------------------------------------------------------------
# Checks (INSERT into audit_results with run_id; fail_count > 0 -> fail)
# -----------------------------------------------------------------------------
CHECKS_SQL = r"""
CREATE TABLE IF NOT EXISTS audit_results (
    run_id VARCHAR,
    check_name VARCHAR,
    fail_count BIGINT,
    sample VARCHAR,
    created_at TIMESTAMP
);

-- C1) No orphans: campaign_sk when campaign_id is not null
INSERT INTO audit_results
SELECT
    '{{RUN_ID}}',
    'C1_no_orphans_campaign_sk',
    COUNT(*),
    NULL,
    CURRENT_TIMESTAMP
FROM curated.curated_revenue_fact f
LEFT JOIN curated.campaigns_dim d ON f.campaign_sk = d.campaign_sk
WHERE f.campaign_id IS NOT NULL AND d.campaign_sk IS NULL;

-- C2) No orphans: customer_sk
INSERT INTO audit_results
SELECT
    '{{RUN_ID}}',
    'C2_no_orphans_customer_sk',
    COUNT(*),
    NULL,
    CURRENT_TIMESTAMP
FROM curated.curated_revenue_fact f
LEFT JOIN curated.customers_dim d ON f.customer_sk = d.customer_sk
WHERE d.customer_sk IS NULL AND f.customer_sk IS NOT NULL;

-- C3) campaigns_dim unique (no duplicate campaign_id)
INSERT INTO audit_results
SELECT
    '{{RUN_ID}}',
    'C3_campaigns_dim_unique',
    COALESCE((SELECT COUNT(*) FROM (SELECT campaign_id FROM curated.campaigns_dim GROUP BY campaign_id HAVING COUNT(*) > 1) x), 0),
    NULL,
    CURRENT_TIMESTAMP;

-- C4) customers_dim unique
INSERT INTO audit_results
SELECT
    '{{RUN_ID}}',
    'C4_customers_dim_unique',
    COALESCE((SELECT COUNT(*) FROM (SELECT customer_id FROM curated.customers_dim GROUP BY customer_id HAVING COUNT(*) > 1) x), 0),
    NULL,
    CURRENT_TIMESTAMP;

-- C5) Fact order_id unique (no duplicate order_id in fact)
INSERT INTO audit_results
SELECT
    '{{RUN_ID}}',
    'C5_fact_order_id_unique',
    COALESCE((SELECT COUNT(*) FROM (SELECT order_id FROM curated.curated_revenue_fact GROUP BY order_id HAVING COUNT(*) > 1) x), 0),
    NULL,
    CURRENT_TIMESTAMP;

-- R1) Lost events (expected in staging, missing in curated)
INSERT INTO audit_results
SELECT
    '{{RUN_ID}}',
    'R1_reconciliation_expected_not_in_curated',
    (SELECT COUNT(*) FROM (
        SELECT order_id FROM staging.orders WHERE order_id IN (SELECT order_id FROM staging.order_items)
        EXCEPT
        SELECT order_id FROM curated.curated_revenue_fact
    ) x),
    NULL,
    CURRENT_TIMESTAMP;

-- R2) Extra events (in curated, not in expected)
INSERT INTO audit_results
SELECT
    '{{RUN_ID}}',
    'R2_reconciliation_curated_not_in_expected',
    (SELECT COUNT(*) FROM (
        SELECT order_id FROM curated.curated_revenue_fact
        EXCEPT
        SELECT order_id FROM staging.orders WHERE order_id IN (SELECT order_id FROM staging.order_items)
    ) x),
    NULL,
    CURRENT_TIMESTAMP;
"""


def run(con: duckdb.DuckDBPyConnection, sql: str) -> None:
    con.execute(sql)


def run_sql_template(con: duckdb.DuckDBPyConnection, sql: str, *, run_id: str) -> None:
    escaped_run_id = run_id.replace("'", "''")
    rendered = sql.replace("{{RUN_ID}}", escaped_run_id)
    con.execute(rendered)


def load_raw(con: duckdb.DuckDBPyConnection) -> None:
    raw_specs = [
        ("raw_orders", "olist_orders_dataset.csv"),
        ("raw_order_items", "olist_order_items_dataset.csv"),
        ("raw_campaigns", "campaign_metadata.csv"),
    ]
    for name, fname in raw_specs:
        path = DATA_DIR / fname
        if not path.exists():
            raise FileNotFoundError(f"Raw data not found: {path}")
        path_str = path.as_posix().replace("'", "''")
        con.execute(f"""
            CREATE OR REPLACE TABLE {name} AS
            SELECT * FROM read_csv_auto('{path_str}')
        """)


def curated_checksum(con: duckdb.DuckDBPyConnection) -> str:
    return con.execute(
        r"""
        SELECT md5(string_agg(row_str, chr(10))) AS curated_checksum
        FROM (
            SELECT
                order_id || '|' || CAST(order_purchase_timestamp AS VARCHAR) || '|' ||
                COALESCE(CAST(customer_sk AS VARCHAR), '') || '|' ||
                COALESCE(CAST(campaign_sk AS VARCHAR), '') || '|' ||
                COALESCE(campaign_id, '') || '|' ||
                COALESCE(CAST(order_revenue AS VARCHAR), '') AS row_str
            FROM curated.curated_revenue_fact
            ORDER BY order_id
        ) t;
        """
    ).fetchone()[0]


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--db",
        type=Path,
        default=HERE / "promotion.duckdb",
        help="DuckDB file path (default: ./promotion.duckdb next to this script)",
    )
    parser.add_argument(
        "--show-all-checks",
        action="store_true",
        help="Print all check rows for this run_id (including PASS rows).",
    )
    args = parser.parse_args()

    args.db.parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(args.db))

    try:
        run(con, "PRAGMA disable_progress_bar")

        load_raw(con)
        run(con, SCHEMA_SQL)
        run(con, STAGING_SQL)
        run(con, CURATED_SQL)

        run_id = datetime.now().isoformat(timespec="seconds")
        run_sql_template(con, CHECKS_SQL, run_id=run_id)

        curated_rows = con.execute("SELECT COUNT(*) FROM curated.curated_revenue_fact").fetchone()[0]
        checksum = curated_checksum(con)

        print(f"BUILD OK: curated_revenue_fact rows = {curated_rows}")
        print(f"run_id={run_id}")
        print(f"curated_checksum={checksum}")

        if args.show_all_checks:
            all_rows = con.execute(
                "SELECT check_name, fail_count, sample FROM audit_results WHERE run_id = ? ORDER BY check_name",
                [run_id],
            ).fetchall()
            print("CHECK SUMMARY:")
            for check_name, fail_count, sample in all_rows:
                print(f"  - {check_name}: {fail_count} ({sample})")

        failed = con.execute(
            "SELECT check_name, fail_count FROM audit_results WHERE run_id = ? AND fail_count > 0 ORDER BY check_name",
            [run_id],
        ).fetchall()

        if failed:
            print("FAILED CHECKS:")
            for name, n in failed:
                print(f"  - {name}: {n}")
            return 1

        print("ALL CHECKS PASSED")
        return 0

    finally:
        con.close()


if __name__ == "__main__":
    raise SystemExit(main())
