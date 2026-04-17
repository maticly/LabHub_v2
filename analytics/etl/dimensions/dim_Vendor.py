import logging
import pandas as pd
from datetime import datetime
from analytics.warehouse.connect_db import get_oltp_connection, get_warehouse_conn

# --- Logging Setup ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - [Dim_Vendor] - %(message)s')
logger = logging.getLogger(__name__)

# -------------------------
# Extract
# -------------------------
def extract_vendors():
    """
    Pulls vendor data from core.Vendor.
    """
    logger.info("Extracting vendor data from OLTP...")
    query = """
        SELECT
            VendorID,
            VendorName,
            VendorStatus
        FROM core.Vendor;
    """
    conn = get_oltp_connection()
    try:
        df_vendors = pd.read_sql(query, conn)
        logger.info(f"Successfully extracted {len(df_vendors)} vendors.")
        return df_vendors
    except Exception as e:
        logger.error(f"Failed to extract vendors: {e}")
        raise
    finally:
        conn.close()

# -------------------------
# Transform
# -------------------------
def transform_dim_vendor(df_vendors: pd.DataFrame) -> pd.DataFrame:
    """
    Basic cleaning for vendor data.
    """
    logger.info("Transforming vendor data...")
    dim_df = df_vendors.copy()
    dim_df['VendorName'] = dim_df['VendorName'].str.strip()
    dim_df['VendorStatus'] = dim_df['VendorStatus'].str.strip()
    
    return dim_df

# -------------------------
# Load
# -------------------------
def load_dim_vendor(duck_conn, dim_df: pd.DataFrame, effective_date: str):
    """
    SCD3
    """
    logger.info("SCD3 load for dw.Dim_Vendor...")
    duck_conn.register("tmp_dim_vendor", dim_df)

    # Update existing records
    duck_conn.execute(f"""
        UPDATE dw.Dim_Vendor
        SET VendorPreviousName = CASE 
                WHEN dw.Dim_Vendor.VendorName IS DISTINCT FROM tmp_dim_vendor.VendorName 
                THEN dw.Dim_Vendor.VendorName 
                ELSE dw.Dim_Vendor.VendorPreviousName 
            END,
            VendorPreviousStatus = CASE 
                WHEN dw.Dim_Vendor.VendorStatus IS DISTINCT FROM tmp_dim_vendor.VendorStatus 
                THEN dw.Dim_Vendor.VendorStatus 
                ELSE dw.Dim_Vendor.VendorPreviousStatus 
            END,
            VendorName = tmp_dim_vendor.VendorName,
            VendorStatus = tmp_dim_vendor.VendorStatus,
            EffectiveDate = '{effective_date}'
        FROM tmp_dim_vendor
        WHERE dw.Dim_Vendor.VendorID = tmp_dim_vendor.VendorID
            AND (dw.Dim_Vendor.VendorName IS DISTINCT FROM tmp_dim_vendor.VendorName
            OR dw.Dim_Vendor.VendorStatus IS DISTINCT FROM tmp_dim_vendor.VendorStatus
        );

    """)

    # Insert new records
    duck_conn.execute(f"""
        INSERT INTO dw.Dim_Vendor (
            VendorID, VendorName, VendorPreviousName, 
            VendorStatus, VendorPreviousStatus, EffectiveDate
        )
        SELECT
            tmp_dim_vendor.VendorID,
            tmp_dim_vendor.VendorName,
            '' AS VendorPreviousName,
            tmp_dim_vendor.VendorStatus,
            'N/A' AS VendorPreviousStatus,
            '{effective_date}' AS EffectiveDate
        FROM tmp_dim_vendor
        WHERE NOT EXISTS (
            SELECT 1 FROM dw.Dim_Vendor
            WHERE dw.Dim_Vendor.VendorID = tmp_dim_vendor.VendorID
        );
    """)

    logger.info(f"Source rows: {len(dim_df)}.")

# -------------------------
# 4. Orchestration
# -------------------------
def run_dim_vendor_etl(duck_conn, effective_date: str):
    try:
        raw_data = extract_vendors()
        transformed_df = transform_dim_vendor(raw_data)
        load_dim_vendor(duck_conn, transformed_df, effective_date)
        logger.info("✅ Dim_Vendor ETL completed successfully.")
    except Exception as e:
        logger.error(f"❌ Dim_Vendor ETL failed: {e}")
        raise

if __name__ == "__main__":
    duck_conn = get_warehouse_conn()
    try:
        logger.info("🧪 STARTING INTEGRATION TEST...")
        duck_conn.execute("BEGIN TRANSACTION;")
        
        run_dim_vendor_etl(duck_conn)
        
        # Test output: Find a vendor to see if it loaded
        test_view = duck_conn.execute("SELECT * FROM dw.Dim_Vendor LIMIT 5").df()
        logger.info(f"\n{test_view}")
        
        duck_conn.execute("ROLLBACK;")
        logger.info("✅ Test successful and rolled back.")
    except Exception as e:
        try: duck_conn.execute("ROLLBACK;")
        except: pass
        logger.error(f"❌ Standalone run failed: {e}")
    finally:
        duck_conn.close()