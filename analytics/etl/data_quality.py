import duckdb
import pandas as pd
from datetime import datetime
from pathlib import Path
import os

def run_dq_checks(duck_conn, scope: str = "all")-> dict:
    """
    Runs a Data Quality audit against the provided DuckDB connection.
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
        dim_tables = [
            "Dim_Product", "Dim_Location", "Dim_User", "Dim_Date", 
            "Dim_Status", "Dim_Stock_Event", "Dim_Storage_Conditions", "Dim_Vendor"
        ]
        for table in dim_tables:
            add_check(f"{table} populated", f"SELECT COUNT(*) FROM dw.{table}", expected_zero=False, critical=True)


    # --- Fact checks ---
    if scope in ("facts", "all"):
        inventory_orphans = [
            ("Dim_Product", "ProductKey"), 
            ("Dim_User", "UserKey"), 
            ("Dim_Location", "LocationKey"), 
            ("Dim_Stock_Event", "StockEventKey"), 
            ("Dim_Storage_Conditions", "StorageConditionKey")
        ]

        for dim, key in inventory_orphans:
            add_check(
                f"Orphaned Inventory Transaction {dim}", 
                f"SELECT COUNT(*) FROM dw.Fact_Inventory_Transactions f LEFT JOIN dw.{dim} d ON f.{key} = d.{key} WHERE d.{key} IS NULL", 
                critical=True
            )

        purchase_orphans = [
            ("Dim_Product", "ProductKey"), 
            ("Dim_User", "RequestedByKey"),
            ("Dim_Status", "StatusKey"), 
            ("Dim_Storage_Conditions", "StorageConditionKey")
        ]
        for dim, key in purchase_orphans:
            join_key = "UserKey" if dim == "Dim_User" else key
            add_check(
                f"purchase orphans {dim}", 
                f"SELECT COUNT(*) FROM dw.Fact_Purchase_Orders f LEFT JOIN dw.{dim} d ON f.{key} = d.{join_key} WHERE d.{join_key} IS NULL", 
                critical=True
            )

        add_check("Negative Stock (Absolute)", "SELECT COUNT(*) FROM dw.Fact_Inventory_Transactions WHERE AbsoluteQuantity < 0", critical=True)
        add_check("Zero/Negative purchase_orphans Cost", "SELECT COUNT(*) FROM dw.Fact_Purchase_Orders WHERE TotalCost <= 0", critical=False)

        today_key = int(datetime.now().strftime('%Y%m%d'))

        add_check("Inventory Transactions Today", f"SELECT COUNT(*) FROM dw.Fact_Inventory_Transactions WHERE DeliveryDateKey = {today_key}", expected_zero=False, critical=False)
        add_check("Purchase Orders Today", f"SELECT COUNT(*) FROM dw.Fact_Purchase_Orders WHERE OrderDateKey = {today_key}", expected_zero=False, critical=False)

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

