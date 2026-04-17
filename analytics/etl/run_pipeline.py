import logging
import pandas as pd
from analytics.warehouse.connect_db import get_warehouse_conn, get_oltp_connection
from analytics.etl.dimensions import dim_product, dim_storage_conditions, dim_user, dim_location, dim_date, dim_vendor, dim_status, dim_stock_event
from analytics.etl.facts import fact_inventory, fact_purchase_orders
from analytics.warehouse.create_views import create_analytics_views
from analytics.etl.data_quality import run_dq_checks, print_dq_report, inspect_warehouse
 
# --- Logging Setup ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - [PIPELINE] - %(message)s'
)
logger = logging.getLogger(__name__)

def get_data_effective_date() -> str:
    """
    Returns the earliest event date in the OLTP data.
    Used as EffectiveDate for dimension first versions so that
    SCD2 date range joins in fact tables resolve correctly.
    """
    conn = get_oltp_connection()
    try:
        df = pd.read_sql("SELECT MIN(EventDate) AS min_date FROM inventory.StockEvent", conn)
        min_date = df['min_date'].iloc[0]
        if pd.isna(min_date):
            logger.warning("No EventDate found in OLTP — using fallback effective date 2020-01-01.")
            return "2020-01-01 00:00:00"
        effective = pd.to_datetime(min_date).strftime('%Y-%m-%d %H:%M:%S')
        logger.info(f"Effective date derived from OLTP: {effective}")
        return effective
    except Exception as e:
        logger.error(f"Failed to derive effective date: {e}")
        raise
    finally:
        conn.close()
 
 
def run_inventory_warehouse(inspect: bool = False):
    """
    Run the Inventory Data Warehouse ETL pipeline.
    """
    duck_conn = get_warehouse_conn()
    try:
        logger.info("Warehouse refresh...")
        duck_conn.execute("BEGIN TRANSACTION;")
 
        # Derive effective date from OLTP before any dimension loads
        effective_date = get_data_effective_date()
 
        # --- 1/4: Dimensions ---
        logger.info("1/4: Refreshing Dimensions...")
        dim_date.run_dim_date_etl(duck_conn)
        dim_vendor.run_dim_vendor_etl(duck_conn, effective_date)
        dim_status.run_dim_status_etl(duck_conn, effective_date)
        dim_location.run_dim_location_etl(duck_conn)
        dim_user.run_dim_user_etl(duck_conn, effective_date)
        dim_product.run_dim_product_etl(duck_conn, effective_date)
        dim_storage_conditions.run_dim_conditions_etl(duck_conn)
        dim_stock_event.run_dim_stock_event_etl(duck_conn)
 
        # --- Dimension DQ gate ---
        logger.info("Dimension Data Quality Checks...")
        dim_dq_report = run_dq_checks(duck_conn, scope="dimensions")
        print_dq_report(dim_dq_report)
 
        if not dim_dq_report["passed_all"]:
            logger.error("🛑 Dimension DQ FAILED — rolling back and aborting pipeline.")
            duck_conn.execute("ROLLBACK;")
            return
 
        # --- 2/4: Facts ---
        logger.info("2/4: Incremental Fact Load...")
        fact_inventory.run_fact_inventory_etl(duck_conn)
        fact_purchase_orders.run_fact_purchase_orders_etl(duck_conn)
 
        # --- 3/4: Full DQ audit ---
        logger.info("3/4: Full warehouse quality audit...")
        dq_report = run_dq_checks(duck_conn)
        print_dq_report(dq_report)
 
        if not dq_report["passed_all"]:
            logger.error("🛑 DQ FAILED — rolling back. Warehouse NOT updated.")
            duck_conn.execute("ROLLBACK;")
            logger.error("❌ Review the DQ report above. No data was written to the warehouse.")
            return
 
        logger.info("✅ All checks PASS — committing.")
        duck_conn.execute("COMMIT;")
 
        # --- 4/4: Refresh views (outside transaction — DDL in DuckDB is auto-committed) ---
        logger.info("4/4: Refreshing analytics views...")
        create_analytics_views()
        logger.info("✅ Analytics views refreshed.")
 
        if inspect:
            inspect_warehouse(duck_conn)
 
        logger.info("✅ Warehouse Refresh Completed.")
 
    except Exception as e:
        logger.critical(f"Pipeline failed during execution: {e}")
        try:
            duck_conn.execute("ROLLBACK;")
        except Exception:
            pass
        raise
    finally:
        duck_conn.close()
 
 
if __name__ == "__main__":
    run_inventory_warehouse(inspect=True)