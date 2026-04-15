import logging
import pandas as pd
from datetime import datetime
from analytics.warehouse.connect_db import get_oltp_connection, get_warehouse_conn

# --- Logging Setup ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - [Dim_Status] - %(message)s')
logger = logging.getLogger(__name__)

# -------------------------
# Extract
# -------------------------
def extract_statuses():
    """
    Pulls user data from the supply.OrderStatus OLTP table.
    """
    logger.info("Extracting status data from OLTP...")
    query = """
        SELECT
            supply.OrderStatus.StatusID,
            supply.OrderStatus.StatusName
        FROM supply.OrderStatus;
    """
    conn = get_oltp_connection()
    try:
        df_status = pd.read_sql(query, conn)
        logger.info(f"Successfully extracted {len(df_status)}.")
        return df_status
    except Exception as e:
        logger.error(f"Failed to extract: {e}")
        raise
    finally:
        conn.close()

# -------------------------
# Transform
# -------------------------
def transform_dim_status(df_status: pd.DataFrame) -> pd.DataFrame:
    """
    placeholder for StatusCategory to match dw.Dim_Status -- StatusCategory is an extra feature not used here .
    """
    logger.info("Transforming status data...")
    dim_status_df = df_status.copy()
    
    dim_status_df['StatusName'] = dim_status_df['StatusName'].str.strip()
    dim_status_df['StatusCategory'] = 'General' 

    return dim_status_df[["StatusID", "StatusName", "StatusCategory"]]
# -------------------------
# Load
# -------------------------
def load_dim_status(duck_conn, dim_df: pd.DataFrame):
    """
    SCD2
    """
    now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    logger.info("SCD2 for dw.Dim_Status...")
    duck_conn.register("tmp_dim_status", dim_df)

    # expire old records
    duck_conn.execute(f"""
        UPDATE dw.Dim_Status
        SET
            EndDate = '{now}',
            IsCurrent = 0
        FROM tmp_dim_status
        WHERE dw.Dim_Status.StatusID = tmp_dim_status.StatusID
          AND dw.Dim_Status.IsCurrent = 1
          AND (
              dw.Dim_Status.StatusName     IS DISTINCT FROM tmp_dim_status.StatusName OR
              dw.Dim_Status.StatusCategory IS DISTINCT FROM tmp_dim_status.StatusCategory
          );
    """)

    # Insert new records
    duck_conn.execute(f"""
        INSERT INTO dw.Dim_Status (
            StatusID, StatusName, StatusCategory, 
            EffectiveDate, EndDate, IsCurrent
        )
        SELECT
            tmp_dim_status.StatusID,
            tmp_dim_status.StatusName,
            tmp_dim_status.StatusCategory,
            '{now}' AS EffectiveDate,
            NULL AS EndDate,
            1 AS IsCurrent
        FROM tmp_dim_status
        WHERE NOT EXISTS (
            SELECT 1 FROM dw.Dim_Status
            WHERE dw.Dim_Status.StatusID = tmp_dim_status.StatusID
              AND dw.Dim_Status.IsCurrent = 1
        );
    """)

    logger.info(f"Source rows: {len(dim_df)}.")

# -------------------------
# 4. Orchestration
# -------------------------
def run_dim_status_etl(duck_conn):
    try:
        raw_data = extract_statuses()
        transformed_df = transform_dim_status(raw_data)
        load_dim_status(duck_conn, transformed_df)
        logger.info("✅ Dim_Status ETL completed successfully.")
    except Exception as e:
        logger.error(f"❌ Dim_Status ETL failed: {e}")
        raise

if __name__ == "__main__":
    duck_conn = get_warehouse_conn()
    try:
        logger.info(" STARTING TEST...")
        duck_conn.execute("BEGIN TRANSACTION;")
        
        run_dim_status_etl(duck_conn)
        
        results = duck_conn.execute("""
            SELECT StatusID, StatusName, StatusCategory, IsCurrent 
            FROM dw.Dim_Status
        """).df()
        
        logger.info(f"\n{results}")
        
        duck_conn.execute("ROLLBACK;")
        logger.info("✅ Test successful and rolled back.")
    except Exception as e:
        try: duck_conn.execute("ROLLBACK;")
        except: pass
        logger.error(f"❌ Standalone run failed: {e}")
    finally:
        duck_conn.close()