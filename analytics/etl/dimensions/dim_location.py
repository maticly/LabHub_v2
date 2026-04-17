import logging
import pandas as pd
from datetime import datetime
from analytics.warehouse.connect_db import get_oltp_connection, get_warehouse_conn

# --- Logging Setup ---
logger = logging.getLogger(__name__)

# -------------------------
# Extract
# -------------------------
def extract_locations():
    """
    Pulls location data from the OLTP database.
    """
    query = """
        SELECT
            Location.LocationID,
            Location.SiteName,
            Location.Building,
            Location.RoomNumber,
            Location.StorageType
        FROM inventory.Location
    """
    conn = get_oltp_connection()
    try:
        df_locations = pd.read_sql(query, conn)
        logger.info(f"Successfully extracted {len(df_locations)} locations.")
        return df_locations
    except Exception as e:
        logger.error(f"Failed to extract locations: {e}")
        raise
    finally:
        conn.close()

# -------------------------
# Transform
# -------------------------
def transform_dim_location(df_locations: pd.DataFrame) -> pd.DataFrame:
    """
    Shapes OLTP location data into Dim_Location rows.
    """
    logger.info("Transforming location data...")
    dim_location_df = df_locations.copy()

    #Data cleaning: Trim and Title Case
    for col in ['SiteName', 'Building', 'StorageType']:
        if col in dim_location_df.columns:
            dim_location_df[col] = dim_location_df[col].fillna("Unknown").str.strip().str.title()

    return dim_location_df[["LocationID", "SiteName", "Building", "RoomNumber", "StorageType"]]

# -------------------------
# Load
# -------------------------
def load_dim_location(duck_conn, dim_df: pd.DataFrame):
    """
    SCD2
    """
    now_fixed = datetime(1900, 1, 1, 0, 0, 0)
    now = now_fixed.strftime('%Y-%m-%d %H:%M:%S') # for SCD replace wth datetime.now() 
    logger.info(f"Upserting {len(dim_df)} rows into dw.Dim_Location...")
    duck_conn.register("tmp_dim_location", dim_df)

    #SCD2
    duck_conn.execute("""
        UPDATE dw.Dim_Location
        SET
            SiteName = tmp.SiteName,
            Building = tmp.Building,
            RoomNumber = tmp.RoomNumber,
            StorageType = tmp.StorageType,
            EndDate = '{now}',
            IsCurrent = 0
        FROM tmp_dim_location AS tmp
        WHERE dw.Dim_Location.LocationID = tmp.LocationID
          AND (
              dw.Dim_Location.SiteName IS DISTINCT FROM tmp.SiteName OR
              dw.Dim_Location.Building IS DISTINCT FROM tmp.Building OR
              dw.Dim_Location.RoomNumber IS DISTINCT FROM tmp.RoomNumber OR
              dw.Dim_Location.StorageType IS DISTINCT FROM tmp.StorageType
          );
    """)

    #Insert new locations
    duck_conn.execute(f"""
        INSERT INTO dw.Dim_Location (LocationID, SiteName, Building, RoomNumber, StorageType,
            EffectiveDate, EndDate, IsCurrent)
        SELECT tmp.LocationID, tmp.SiteName, tmp.Building, tmp.RoomNumber, tmp.StorageType,
            '{now}' AS EffectiveDate,
            NULL AS EndDate,
            1 AS IsCurrent
        FROM tmp_dim_location AS tmp
        WHERE NOT EXISTS (
            SELECT 1 FROM dw.Dim_Location
            WHERE dw.Dim_Location.LocationID = tmp.LocationID
        );
    """)

    logger.info(f"Dim_Location upsert complete. Source rows: {len(dim_df)}.")

# -------------------------
# Orchestration
# -------------------------
def run_dim_location_etl(duck_conn):
    """Orchestrates the Location Dimension ETL.
    Called by run_pipeline.py."""
    try:
        raw_locations = extract_locations()
        dim_location_df = transform_dim_location(raw_locations)
        load_dim_location(duck_conn, dim_location_df)
        logger.info("✅ Dim_Location ETL completed successfully.")
    except Exception as e:
        logger.error(f"❌ Dim_Location ETL failed: {e}")
        raise


# -------------------------
# TEST
# -------------------------
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - [Dim_Location_Test] - %(message)s')

    duck_conn = get_warehouse_conn()
    try:
        logger.info("STARTING TEST...")
        duck_conn.execute("BEGIN TRANSACTION;")
        run_dim_location_etl(duck_conn)

        count = duck_conn.execute("SELECT COUNT(*) FROM dw.Dim_Location").fetchone()[0]
        logger.info(f"✅ Works great!")
        duck_conn.execute("ROLLBACK;")
        logger.info("✅ Transaction ROLLED BACK")

    except Exception as e:
        try:
            duck_conn.execute("ROLLBACK;")
        except Exception as rollback_e:
            pass
        logger.error(f"❌ Test failed: {e}")
    finally:
        duck_conn.close()