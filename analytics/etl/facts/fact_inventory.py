import logging
import pandas as pd
from analytics.warehouse.connect_db import get_oltp_connection, get_warehouse_conn

# --- Logging Setup ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - [Fact_Inventory] - %(message)s')
logger = logging.getLogger(__name__)

# -------------------------
# 1. Extract
# -------------------------
def extract_fact_source_data():
    """
    Pulls raw events and current item states from OLTP.
    """
    logger.info("Extracting raw inventory events from OLTP...")
    query = """
        WITH Snapshots AS(
            SELECT
                inventory.StockEvent.StockEventID AS TransactionID,
                inventory.InventoryItem.ProductID,
                inventory.StockEvent.LocationID,
                inventory.StockEvent.[UserID],
                inventory.StockEvent.EventDate,
                inventory.InventoryItem.ExpirationDate,
                inventory.InventoryItem.LotNumber,
                inventory.StockEvent.OldQuantity,
                inventory.StockEvent.NewQuantity,
                FIRST_VALUE(inventory.StockEvent.NewQuantity) OVER(PARTITION BY inventory.StockEvent.InventoryItemID
                    ORDER BY inventory.StockEvent.EventDate DESC, inventory.StockEvent.StockEventID DESC) AS CurrentStockSnapshot
            FROM inventory.StockEvent
            LEFT JOIN inventory.InventoryItem ON inventory.StockEvent.InventoryItemID = inventory.InventoryItem.InventoryItemID
            ) SELECT * FROM Snapshots;
    """
    conn = get_oltp_connection()
    try:
        df = pd.read_sql(query, conn)
        logger.info(f"Successfully extracted {len(df)} inventory events.")
        return df
    except Exception as e:
        logger.error(f"Failed to extract fact data: {e}")
        raise
    finally:
        conn.close()

#3. Load
def load_fact_inventory(duck_conn, df_fact_inventory: pd.DataFrame):
    """
    Performs dimension lookups and incrementally inserts only new rows
    into Fact_Inventory_Transactions, skipping any TransactionID that
    already exists. SCD2 aware
    """
    logger.info("load into Fact_Inventory_Transactions...")

    # Count existing rows before insert for accurate delta
    before_count = duck_conn.execute(
        "SELECT COUNT(*) FROM dw.Fact_Inventory_Transactions"
    ).fetchone()[0]

    duck_conn.register("tmp_fact_inventory", df_fact_inventory)

    duck_conn.execute("""
        INSERT INTO dw.Fact_Inventory_Transactions (
                TransactionID, ProductKey, DeliveryDateKey, ExpirationDateKey,
                LocationKey, UserKey, LotNumber, QuantityDelta, 
                AbsoluteQuantity, CurrentStockSnapshot
        )
        SELECT 
            tmp_fact_inventory.TransactionID,
            dw.Dim_Product.ProductKey,
            CAST(strftime(tmp_fact_inventory.EventDate, '%Y%m%d') AS INT) AS DeliveryDateKey,
            COALESCE(CAST(strftime(tmp_fact_inventory.ExpirationDate, '%Y%m%d') AS INT), 19000101) AS ExpirationDateKey,
            dw.Dim_Location.LocationKey,
            dw.Dim_User.UserKey,
            COALESCE(tmp_fact_inventory.LotNumber, 'UNKNOWN'),
            (tmp_fact_inventory.NewQuantity - tmp_fact_inventory.OldQuantity) AS QuantityDelta,
            tmp_fact_inventory.NewQuantity AS AbsoluteQuantity,
            tmp_fact_inventory.CurrentStockSnapshot
        FROM tmp_fact_inventory
        -- SCD2 lookup for product
        JOIN dw.Dim_Product ON tmp_fact_inventory.ProductID = dw.Dim_Product.ProductID 
            AND tmp_fact_inventory.EventDate >= dw.Dim_Product.EffectiveDate 
            AND (tmp_fact_inventory.EventDate < dw.Dim_Product.EndDate OR dw.Dim_Product.EndDate IS NULL)
        -- SCD2 lookup for user
        JOIN dw.Dim_User ON tmp_fact_inventory.UserID = dw.Dim_User.UserID 
            AND tmp_fact_inventory.EventDate >= dw.Dim_User.EffectiveDate 
            AND (tmp_fact_inventory.EventDate < dw.Dim_User.EndDate OR dw.Dim_User.EndDate IS NULL)
        -- SCD1 / Standard Lookups
        JOIN dw.Dim_Location ON tmp_fact_inventory.LocationID = dw.Dim_Location.LocationID
        WHERE NOT EXISTS (
            SELECT 1 FROM dw.Fact_Inventory_Transactions
            WHERE dw.Fact_Inventory_Transactions.TransactionID = tmp_fact_inventory.TransactionID
        );
    """)
    after_count = duck_conn.execute(
        "SELECT COUNT(*) FROM dw.Fact_Inventory_Transactions"
    ).fetchone()[0]

    new_rows = after_count - before_count
    logger.info(
        f"✅ Incremental load complete. "
        f"{new_rows} new rows inserted out of {len(df_fact_inventory)} source events."
    )

# -------------------------
# Orchestration
# -------------------------

def run_fact_inventory_etl(duck_conn):
    try:
        df_source = extract_fact_source_data()
        if not df_source.empty:
            load_fact_inventory(duck_conn, df_source)
        else:
            logger.warning("No source data found to load into Fact table.")
    except Exception as e:
        logger.error(f"Fact Inventory ETL aborted: {e}")
        raise

if __name__ == "__main__":
    duck_conn = get_warehouse_conn()
    try:
        duck_conn.execute("BEGIN TRANSACTION;")

        before_count = duck_conn.execute(
            "SELECT COUNT(*) FROM dw.Fact_Inventory_Transactions"
        ).fetchone()[0]
        
        run_fact_inventory_etl(duck_conn)

        after_count = duck_conn.execute(
            "SELECT COUNT(*) FROM dw.Fact_Inventory_Transactions"
        ).fetchone()[0]

        sample = duck_conn.execute("""
            SELECT * FROM dw.Fact_Inventory_Transactions 
            LIMIT 5
        """).df()

        logger.info(f"Test: New Rows Detected={after_count - before_count}")
        if not sample.empty:
            logger.info(f"Sample Data:\n{sample}")

        duck_conn.execute("ROLLBACK;")
        logger.info("✅ dry run")

    except Exception as e:
        duck_conn.execute("ROLLBACK;")
        logger.error(f"dry run failed: {e}")
    finally:
        duck_conn.close()