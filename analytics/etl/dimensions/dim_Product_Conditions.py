import logging
import pandas as pd
from datetime import datetime
from analytics.warehouse.connect_db import get_oltp_connection, get_warehouse_conn

# --- Logging Setup ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - [Dim_Conditions] - %(message)s')
logger = logging.getLogger(__name__)

# -------------------------
# Extract
# -------------------------
def extract_storage_conditions():
    """
    Extracts storage temperature and description data from core.StorageConditions.
    """
    logger.info("Extracting storage conditions from OLTP...")
    query = """
        SELECT
            core.StorageConditions.StorageConditionID,
            core.StorageConditions.MaxTemp,
            core.StorageConditions.MinTemp,
            core.StorageConditions.ConditionDescription,
            core.StorageConditions.ConditionName
        FROM core.StorageConditions;
    """
    conn = get_oltp_connection()
    try:
        df_conditions = pd.read_sql(query, conn)
        logger.info(f"Successfully extracted {len(df_conditions)} storage conditions.")
        return df_conditions
    except Exception as e:
        logger.error(f"Failed to extract storage conditions: {e}")
        raise
    finally:
        conn.close()

# -------------------------
# Transform
# -------------------------
def transform_dim_conditions(df_conditions: pd.DataFrame) -> pd.DataFrame:
    logger.info("Transforming storage condition data...")
    dim_df = df_conditions.copy()
    
    dim_df['ConditionDescription'] = dim_df['ConditionDescription'].str.strip()
    dim_df['ConditionName'] = dim_df['ConditionName'].str.strip()
    
    return dim_df[[
        "StorageConditionID", 
        "MaxTemp", 
        "MinTemp", 
        "ConditionDescription", 
        "ConditionName"
    ]]
# -------------------------
# Load
# -------------------------
def load_dim_conditions(duck_conn, dim_df: pd.DataFrame):
    """
    SCD1
    """
    now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    logger.info("SCD1 for dw.Dim_Product_Conditions...")
    duck_conn.register("tmp_dim_conditions", dim_df)

    # Update existing records
    duck_conn.execute(f"""
        UPDATE dw.Dim_Product_Conditions
        SET
            MaxTemp = tmp_dim_conditions.MaxTemp,
            MinTemp = tmp_dim_conditions.MinTemp,
            ConditionDescription = tmp_dim_conditions.ConditionDescription,
            LastUpdated = '{now}',
            ConditionName = tmp_dim_conditions.ConditionName
        FROM tmp_dim_conditions
        WHERE dw.Dim_Product_Conditions.StorageConditionID = tmp_dim_conditions.StorageConditionID
          AND (
              dw.Dim_Product_Conditions.MaxTemp IS DISTINCT FROM tmp_dim_conditions.MaxTemp OR
              dw.Dim_Product_Conditions.MinTemp IS DISTINCT FROM tmp_dim_conditions.MinTemp OR
              dw.Dim_Product_Conditions.ConditionDescription IS DISTINCT FROM tmp_dim_conditions.ConditionDescription OR
              dw.Dim_Product_Conditions.ConditionName IS DISTINCT FROM tmp_dim_conditions.ConditionName
          );
    """)

    # Insert new records
    duck_conn.execute(f"""
        INSERT INTO dw.Dim_Product_Conditions (
            StorageConditionID, MaxTemp, MinTemp, ConditionDescription, LastUpdated, ConditionName
        )
        SELECT
            tmp_dim_conditions.StorageConditionID,
            tmp_dim_conditions.MaxTemp,
            tmp_dim_conditions.MinTemp,
            tmp_dim_conditions.ConditionDescription,
            '{now}' AS LastUpdated,
            tmp_dim_conditions.ConditionName
        FROM tmp_dim_conditions
        WHERE NOT EXISTS (
            SELECT 1 FROM dw.Dim_Product_Conditions
            WHERE dw.Dim_Product_Conditions.StorageConditionID = tmp_dim_conditions.StorageConditionID
        );
    """)

    logger.info(f"Source rows: {len(dim_df)}.")

# -------------------------
# 4. Orchestration
# -------------------------
def run_dim_conditions_etl(duck_conn):
    try:
        raw_data = extract_storage_conditions()
        transformed_df = transform_dim_conditions(raw_data)
        load_dim_conditions(duck_conn, transformed_df)
        logger.info("✅ Dim_Product_Conditions ETL completed successfully.")
    except Exception as e:
        logger.error(f"❌ Dim_Product_Conditions ETL failed: {e}")
        raise

if __name__ == "__main__":
    duck_conn = get_warehouse_conn()
    try:
        logger.info("🧪 STARTING INTEGRATION TEST...")
        duck_conn.execute("BEGIN TRANSACTION;")
        
        run_dim_conditions_etl(duck_conn)
        
        # Quick validation check
        count = duck_conn.execute("SELECT COUNT(*) FROM dw.Dim_Product_Conditions").fetchone()[0]
        logger.info(f"🧪 Rows in Warehouse: {count}")
        
        duck_conn.execute("ROLLBACK;")
        logger.info("✅ Test successful and rolled back.")
    except Exception as e:
        try: duck_conn.execute("ROLLBACK;")
        except: pass
        logger.error(f"❌ Standalone run failed: {e}")
    finally:
        duck_conn.close()