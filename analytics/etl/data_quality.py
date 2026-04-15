import duckdb
import pandas as pd
from datetime import datetime
from pathlib import Path
import os

def run_dq_checks(duck_conn, scope: str = "all")-> dict:
    """
    Runs a Data Quality audit against the provided DuckDB connection.

    Args:
        duck_conn: An active DuckDB connection (managed by the caller).
        scope: "dimensions", "facts", or "all" (default).
    """
    
    report = {
    "title": "🛡️ LabHub Warehouse Audit",
    "timestamp": datetime.now().strftime('%Y-%m-%d %H:%M'),
    "scope": scope,
    "checks": [],
    "passed_all": True
    }

    def add_check(name, query, expected_zero=True, critical=False):
        try:
            val = duck_conn.execute(query).fetchone()[0]
            passed = (val == 0) if expected_zero else (val > 0)
            status = "✅ PASS" if passed else ("❌ FAIL" if critical else "⚠️ WARN")
            if not passed and critical:
                report["passed_all"] = False
            report["checks"].append({
                "check_name": name,
                "value": val,
                "status": status
            })
        except Exception as e:
            report["checks"].append({
                "check_name": name,
                "status": "💥 ERROR",
                "error": str(e)
            })
            report["passed_all"] = False

    # --- Dimension checks ---
    if scope in ("dimensions", "all"):
        add_check(
            "Dim_Product populated",
            "SELECT COUNT(*) FROM dw.Dim_Product",
            expected_zero=False, critical=True
        )
        add_check(
            "Dim_Location populated",
            "SELECT COUNT(*) FROM dw.Dim_Location",
            expected_zero=False, critical=True
        )
        add_check(
            "Dim_User populated",
            "SELECT COUNT(*) FROM dw.Dim_User",
            expected_zero=False, critical=True
        )
        add_check(
            "Dim_Date populated",
            "SELECT COUNT(*) FROM dw.Dim_Date",
            expected_zero=False, critical=True
        )
        add_check(
            "Missing AI Descriptions",
            "SELECT COUNT(*) FROM dw.Dim_Product WHERE Description IS NULL OR length(trim(Description)) < 5",
            critical=False
        )

    # --- Fact checks ---
    if scope in ("facts", "all"):
        add_check(
            "Negative Stock (Absolute)",
            "SELECT COUNT(*) FROM dw.Fact_Inventory_Transactions WHERE AbsoluteQuantity < 0",
            critical=True
        )
        add_check(
            "Orphaned Products",
            """SELECT COUNT(*) FROM dw.Fact_Inventory_Transactions f
               LEFT JOIN dw.Dim_Product p ON f.ProductKey = p.ProductKey
               WHERE p.ProductKey IS NULL""",
            critical=True
        )
        add_check(
            "Orphaned Locations",
            """SELECT COUNT(*) FROM dw.Fact_Inventory_Transactions f
               LEFT JOIN dw.Dim_Location l ON f.LocationKey = l.LocationKey
               WHERE l.LocationKey IS NULL""",
            critical=True
        )
        add_check(
            "Duplicate Transaction IDs",
            "SELECT COUNT(TransactionID) - COUNT(DISTINCT TransactionID) FROM dw.Fact_Inventory_Transactions",
            critical=True
        )
        today_key = int(datetime.now().strftime('%Y%m%d'))
        add_check(
            "Data is from Today",
            f"SELECT COUNT(*) FROM dw.Fact_Inventory_Transactions WHERE DateKey = {today_key}",
            expected_zero=False, critical=False
        )

    return report

def print_dq_report(report: dict):
    print(f"\n{'='*40}")
    print(f"WAREHOUSE AUDIT REPORT [{report.get('scope', 'all').upper()}]: {report['timestamp']}")
    print(f"{'='*40}")
    for check in report["checks"]:
        print(f"  {check['status']} | {check['check_name']}: {check.get('value', check.get('error', 'N/A'))}")
    print(f"{'='*40}")
    print("✅ STATUS: HEALTHY" if report["passed_all"] else "🛑 STATUS: ISSUES DETECTED")
    print()

def inspect_warehouse(duck_conn):
    """
    Prints a shape summary of every table in the dw schema:
    row counts and column names. Useful for quick sanity checks.
    """
    print(f"\n{'='*50}")
    print("🏗️  WAREHOUSE SHAPE INSPECTOR")
    print(f"{'='*50}")

    tables = duck_conn.execute("""
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = 'dw'
        ORDER BY table_name;
    """).fetchall()

    if not tables:
        print("  No tables found in schema 'dw'.")
        return

    for (table_name,) in tables:
        row_count = duck_conn.execute(
            f"SELECT COUNT(*) FROM dw.{table_name}"
        ).fetchone()[0]

        columns = duck_conn.execute(f"""
            SELECT column_name, data_type
            FROM information_schema.columns
            WHERE table_schema = 'dw' AND table_name = '{table_name}'
            ORDER BY ordinal_position;
        """).fetchall()

        col_summary = ", ".join(f"{c[0]} ({c[1]})" for c in columns)
        print(f"\n  📋 dw.{table_name}")
        print(f"     Rows   : {row_count:,}")
        print(f"     Columns: {col_summary}")

    print(f"\n{'='*50}\n")


if __name__ == "__main__":
    from analytics.warehouse.connect_db import get_warehouse_conn
    conn = get_warehouse_conn()
    try:
        inspect_warehouse(conn)
        report = run_dq_checks(conn)
        print_dq_report(report)
    finally:
        conn.close()

