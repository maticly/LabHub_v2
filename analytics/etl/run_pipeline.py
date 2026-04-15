import logging
from analytics.warehouse.connect_db import get_warehouse_conn
from analytics.etl.dimensions import dim_product, dim_user, dim_location, dim_date
from analytics.etl.facts import fact_inventory
from analytics.warehouse.create_views import create_analytics_views
from analytics.etl.data_quality import run_dq_checks, print_dq_report, inspect_warehouse

# --- Logging Setup ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - [PIPELINE] - %(message)s'
)
logger = logging.getLogger(__name__)

def run_inventory_warehouse(inspect: bool = False):
    """
    Run the Inventory Data Warehouse ETL pipeline.

    Coordinates dimension and fact loads:
    - Loads dimensions (date, product, user, location).
    - Loads facts: fact skip rows already present in OLAP

    Args:
        inspect: If True, prints a warehouse shape summary after a successful load.
    """
    duck_conn = get_warehouse_conn()
    try:
        
        duck_conn.execute("BEGIN TRANSACTION;")

        # 1. Load Dimensions
        logger.info("Step 1/3: Refreshing Dimensions...")
        dim_date.run_dim_date_etl(duck_conn)
        dim_product.run_dim_product_etl(duck_conn)
        dim_user.run_dim_user_etl(duck_conn)
        dim_location.run_dim_location_etl(duck_conn)

        # 1B. Dimensions Quality Check
        logger.info("Running Dimension Data Quality Checks...") 
        dim_dq_report = run_dq_checks(duck_conn, scope="dimensions") 
        print_dq_report(dim_dq_report)

        if not dim_dq_report["passed_all"]: 
            logger.error("🛑 Dimension DQ FAILED — rolling back and aborting pipeline.") 
            duck_conn.execute("ROLLBACK;") 
            return
    
        # 2. Load Facts 
        logger.info("Step 2/3: Performing Incremental Fact Load...")
        fact_inventory.run_fact_inventory_etl(duck_conn)

        # 3. Data Quality
        logger.info("Step 3/3: Running Data Quality Audit...")
        dq_report = run_dq_checks(duck_conn)
        print_dq_report(dq_report)

        if dq_report["passed_all"]:
            logger.info("✅ All checks PASS")
            duck_conn.execute("COMMIT;")
        else:
            logger.error("🛑 DQ FAILED — rolling back. Warehouse NOT updated.")
            duck_conn.execute("ROLLBACK;")
            logger.error(
                "❌ Review the DQ report above. "
                "No data was written to the warehouse."
            )
            return

        # 4. Refresh Views
        create_analytics_views()
        logger.info("✅ Analytics views refreshed.")

        if inspect:
            inspect_warehouse(duck_conn)

        logger.info("✅ Warehouse Refresh Completed Successfully.")

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
    run_inventory_warehouse()

