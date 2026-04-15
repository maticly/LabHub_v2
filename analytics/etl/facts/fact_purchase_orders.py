import logging
import pandas as pd
from datetime import datetime
from analytics.warehouse.connect_db import get_oltp_connection, get_warehouse_conn

# --- Logging Setup ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - [Fact_Purchase_Orders] - %(message)s')
logger = logging.getLogger(__name__)

# -------------------------
# 1. Extract
# -------------------------
def extract_purchase_order_data():
    """
    extracts order data
    """
    logger.info("Extracting order data from OLTP...")
    query = """
        SELECT
            supply.[Order].OrderID AS PurchaseOrderID,
            supply.[Order].ProductID,
            supply.[Order].VendorID,
            supply.[Order].UserID AS RequesterUserID,
            supply.[Order].OrderStatusID,
            supply.[Order].Quantity AS QuantityOrdered,
            (supply.[Order].Quantity * supply.[Order].UnitPrice) AS TotalCost,
            supply.[Order].CreatedAt AS OrderDate,
            inventory.InventoryItem.AddedAt AS DeliveryDate,
            ABS(DATEDIFF(DAY, supply.[Order].CreatedAt, inventory.InventoryItem.AddedAt)) AS VendorLeadTimeDays
        FROM supply.[Order]
        JOIN core.Product ON supply.[Order].ProductID = core.Product.ProductID
        JOIN inventory.InventoryItem ON inventory.InventoryItem.ProductID = core.Product.ProductID
    """
    conn = get_oltp_connection()
    try:
        df = pd.read_sql(query, conn)
        logger.info(f"Successfully extracted {len(df)} orders.")
        return df
    except Exception as e:
        logger.error(f"Failed to extract order data: {e}")
        raise
    finally:
        conn.close()

#3. Load
def load_fact_purchase_orders(duck_conn, df_po: pd.DataFrame):
    """
    SCD2 lookups and incremental load
    """
    logger.info("load into Fact_Inventory_Transactions...")

    before_count = duck_conn.execute("SELECT COUNT(*) FROM dw.Fact_Purchase_Orders").fetchone()[0]
    duck_conn.register("tmp_fact_po", df_po)

    duck_conn.execute("""
        INSERT INTO dw.Fact_Purchase_Orders (
            PurchaseOrderID, ProductKey, OrderDateKey, DeliveryDateKey,
            VendorKey, StatusKey, RequestedByKey,
            QuantityOrdered, TotalCost, VendorLeadTimeDays
        )
        SELECT
        tmp_fact_po.PurchaseOrderID,
            dw.Dim_Product.ProductKey,
            CAST(strftime(tmp_fact_po.OrderDate, '%Y%m%d') AS INT) AS OrderDateKey,
            COALESCE(CAST(strftime(tmp_fact_po.DeliveryDate, '%Y%m%d') AS INT), 19000101) AS DeliveryDateKey,
            dw.Dim_Vendor.VendorKey,
            dw.Dim_Status.StatusKey,
            dw.Dim_User.UserKey AS RequestedByKey,
            tmp_fact_po.QuantityOrdered,
            tmp_fact_po.TotalCost,
            tmp_fact_po.VendorLeadTimeDays
        FROM tmp_fact_po
        -- Product SCD2 lookup
        JOIN dw.Dim_Product ON tmp_fact_po.ProductID = dw.Dim_Product.ProductID 
            AND tmp_fact_po.OrderDate >= dw.Dim_Product.EffectiveDate 
            AND (tmp_fact_po.OrderDate < dw.Dim_Product.EndDate OR dw.Dim_Product.EndDate IS NULL)
        -- Vendor SCD3 lookup
        JOIN dw.Dim_Vendor ON tmp_fact_po.VendorID = dw.Dim_Vendor.VendorID
        -- Status SCD2 lookup
        JOIN dw.Dim_Status ON tmp_fact_po.OrderStatusID = dw.Dim_Status.StatusID
            AND tmp_fact_po.OrderDate >= dw.Dim_Status.EffectiveDate 
            AND (tmp_fact_po.OrderDate < dw.Dim_Status.EndDate OR dw.Dim_Status.EndDate IS NULL)
        -- User SCD2 lookup for RequestedByKey
        JOIN dw.Dim_User ON tmp_fact_po.RequesterUserID = dw.Dim_User.UserID 
            AND tmp_fact_po.OrderDate >= dw.Dim_User.EffectiveDate 
            AND (tmp_fact_po.OrderDate < dw.Dim_User.EndDate OR dw.Dim_User.EndDate IS NULL)
        WHERE NOT EXISTS (
            SELECT 1 FROM dw.Fact_Purchase_Orders 
            WHERE dw.Fact_Purchase_Orders.PurchaseOrderID = tmp_fact_po.PurchaseOrderID
        );
    """)
    after_count = duck_conn.execute("SELECT COUNT(*) FROM dw.Fact_Purchase_Orders").fetchone()[0]
    logger.info(f"✅ load complete. New POs: {after_count - before_count}")

# -------------------------
# Orchestration
# -------------------------

def run_fact_purchase_orders_etl(duck_conn):
    try:
        df_source = extract_purchase_order_data()
        if not df_source.empty:
            load_fact_purchase_orders(duck_conn, df_source)
        else:
            logger.warning("No PO data found to load.")
    except Exception as e:
        logger.error(f"Fact PO ETL failed: {e}")
        raise

if __name__ == "__main__":
    duck_conn = get_warehouse_conn()
    try:
        logger.info("TEST (Dry Run)...")
        duck_conn.execute("BEGIN TRANSACTION;")
        
        start_count = duck_conn.execute("SELECT COUNT(*) FROM dw.Fact_Purchase_Orders").fetchone()[0]
        run_fact_purchase_orders_etl(duck_conn)
        end_count = duck_conn.execute("SELECT COUNT(*) FROM dw.Fact_Purchase_Orders").fetchone()[0]
        
        logger.info(f"Test Results: New Rows={end_count - start_count}")
        
        sample = duck_conn.execute("""
            SELECT PurchaseOrderID, TotalCost, VendorLeadTimeDays 
            FROM dw.Fact_Purchase_Orders 
            LIMIT 5
        """).df()
        
        if not sample.empty:
            logger.info(f"Sample Data:\n{sample}")

        duck_conn.execute("ROLLBACK;")
        logger.info("✅ Test")
    except Exception as e:
        try: duck_conn.execute("ROLLBACK;")
        except: pass
        logger.error(f"❌ run failed: {e}")
    finally:
        duck_conn.close()