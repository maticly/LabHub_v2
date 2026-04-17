import logging
import pandas as pd
from datetime import datetime
from analytics.warehouse.connect_db import get_oltp_connection, get_warehouse_conn

# --- Logging Setup ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - [Dim_Stock_Event] - %(message)s')
logger = logging.getLogger(__name__)

# -------------------------
# Extract
# -------------------------
def extract_stock_events():
    """
    Pulls data from inventory.EventReason.
    """
    logger.info("Extracting stock event reasons from OLTP...")
    query = """
        SELECT
            inventory.StockEvent.StockEventID,
            inventory.EventReason.Reason AS StockEventReason,
            inventory.StockEvent.EventDescription
        FROM inventory.StockEvent
        JOIN inventory.EventReason ON inventory.StockEvent.EventReasonID = inventory.EventReason.EventReasonID;
    """
    conn = get_oltp_connection()
    try:
        df_events = pd.read_sql(query, conn)
        logger.info(f"Successfully extracted {len(df_events)} event reasons.")
        return df_events
    except Exception as e:
        logger.error(f"Failed to extract stock events: {e}")
        raise
    finally:
        conn.close()

# -------------------------
# Transform
# -------------------------
def transform_dim_stock_event(df_events: pd.DataFrame) -> pd.DataFrame:
    """
    placeholder for EventType to match dw.dim_stock_event.py -- it is an extra feature not used here .
    """
    logger.info("Transforming stock event data...")
    dim_df = df_events.copy()
    
    dim_df['EventDescription'] =dim_df['EventDescription'].str.strip()
    dim_df['StockEventReason'] = dim_df['StockEventReason'].str.strip()
    dim_df['StockEventType'] = 'N/A'

    return dim_df[["StockEventID", "StockEventType", "StockEventReason", "EventDescription"]]
# -------------------------
# Load
# -------------------------
def load_dim_stock_event(duck_conn, dim_df: pd.DataFrame):
    """
    SCD1
    """
    now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    logger.info("SCD2 for dw.Dim_Stock_Event...")
    duck_conn.register("tmp_dim_stock_event", dim_df)

    # update records
    duck_conn.execute(f"""
        UPDATE dw.Dim_Stock_Event
        SET
            StockEventType = tmp_dim_stock_event.StockEventType,
            StockEventReason = tmp_dim_stock_event.StockEventReason,
            EventDescription = tmp_dim_stock_event.EventDescription,
            LastUpdated = '{now}'
        FROM tmp_dim_stock_event
        WHERE dw.Dim_Stock_Event.StockEventID = tmp_dim_stock_event.StockEventID
          AND (
              dw.Dim_Stock_Event.StockEventType IS DISTINCT FROM tmp_dim_stock_event.StockEventType OR
              dw.Dim_Stock_Event.StockEventReason IS DISTINCT FROM tmp_dim_stock_event.StockEventReason OR
              dw.Dim_Stock_Event.EventDescription IS DISTINCT FROM tmp_dim_stock_event.EventDescription
          );
    """)

    # Insert new records
    duck_conn.execute(f"""
        INSERT INTO dw.Dim_Stock_Event (
            StockEventID, StockEventType, StockEventReason, EventDescription, LastUpdated
        )
        SELECT
            tmp_dim_stock_event.StockEventID,
            tmp_dim_stock_event.StockEventType,
            tmp_dim_stock_event.StockEventReason,
            tmp_dim_stock_event.EventDescription,
            '{now}' AS LastUpdated
        FROM tmp_dim_stock_event
        WHERE NOT EXISTS (
            SELECT 1 FROM dw.Dim_Stock_Event
            WHERE dw.Dim_Stock_Event.StockEventID = tmp_dim_stock_event.StockEventID
        );
    """)

    logger.info(f"Source rows: {len(dim_df)}.")

# -------------------------
# 4. Orchestration
# -------------------------
def run_dim_stock_event_etl(duck_conn):
    try:
        raw_data = extract_stock_events()
        transformed_df = transform_dim_stock_event(raw_data)
        load_dim_stock_event(duck_conn, transformed_df)
        logger.info("✅ Dim_Stock_Event ETL completed successfully.")
    except Exception as e:
        logger.error(f"❌ Dim_Stock_Event ETL failed: {e}")
        raise

if __name__ == "__main__":
    duck_conn = get_warehouse_conn()
    try:
        logger.info("🧪 STARTING INTEGRATION TEST...")
        duck_conn.execute("BEGIN TRANSACTION;")
        
        run_dim_stock_event_etl(duck_conn)
        
        # Verify
        count = duck_conn.execute("SELECT COUNT(*) FROM dw.Dim_Stock_Event").fetchone()[0]
        logger.info(f"🧪 Rows in dw.Dim_Stock_Event: {count}")
        
        duck_conn.execute("ROLLBACK;")
        logger.info("✅ Test successful and rolled back.")
    except Exception as e:
        try: duck_conn.execute("ROLLBACK;")
        except: pass
        logger.error(f"❌ Standalone run failed: {e}")
    finally:
        duck_conn.close()