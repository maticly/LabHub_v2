import logging
import pandas as pd
from analytics.warehouse.connect_db import get_oltp_connection, get_warehouse_conn

# --- Logging Setup ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - [Dim_Date] - %(message)s'
)
logger = logging.getLogger(__name__)


# 1.Extract
# -------------------------
def extract_date_range():
    """
    Queries OLTP to find the the absolute date boundaries across both 
    Inventory (StockEvents) and Supply (Orders).
    """
    query = """
        SELECT 
            MIN(AllDates) AS MinDate,
            MAX(AllDates) AS MaxDate 
        FROM (
            SELECT EventDate AS AllDates FROM inventory.StockEvent
            UNION ALL
            SELECT CreatedAt AS AllDates FROM supply.[Order]
            UNION ALL 
            SELECT UpdatedAt AS AllDates FROM supply.[Order]
            UNION ALL
            SELECT AddedAt AS AllDates FROM inventory.InventoryItem
            UNION ALL
            SELECT ExpirationDate AS AllDates FROM inventory.InventoryItem
        ) AS DateSource;
    """
    conn = get_oltp_connection()
    min_date = None
    max_date = None

    try:
        df_dates = pd.read_sql(query, conn)
        min_date = df_dates['MinDate'].iloc[0]
        max_date = df_dates['MaxDate'].iloc[0]
    except Exception as e:
        logger.error(f"Error querying date range: {e}")
    finally:
        conn.close()

    # Fallback logic if table is empty or query fails
    if pd.isna(min_date) or pd.isna(max_date):
        logger.warning("No dates found in OLTP. Using default range (2026-01-31 to 2026-04-10).")
        return pd.Timestamp("2026-01-31"), pd.Timestamp("2026-04-10")
    
    logger.info(f"Date range discovered: {min_date} to {max_date}")
    return pd.to_datetime(min_date), pd.to_datetime(max_date)

# -------------------------
# 2.Transform
# -------------------------
def transform_dim_date(min_date, max_date) -> pd.DataFrame:
    """
    Generates a continuous sequence of dates and calculates attributes.
    """

    # Normalize to midnight to ensure clean day increments
    start_dt = min_date.normalize()
    end_dt = max_date.normalize()
    
    logger.info(f"Generating date attributes for range: {start_dt.date()} to {end_dt.date()}")

    #generate date range
    date_range = pd.date_range(start=start_dt, end=end_dt, freq='D')
    dim_date_df = pd.DataFrame({"FullDate": date_range})

    #transformt to Dim_Date structure
    dim_date_df['DateKey'] = dim_date_df['FullDate'].dt.strftime('%Y%m%d').astype(int)
    dim_date_df['Day'] = dim_date_df['FullDate'].dt.day
    dim_date_df['Month'] = dim_date_df['FullDate'].dt.month
    dim_date_df['MonthName'] = dim_date_df['FullDate'].dt.month_name()
    dim_date_df['Quarter'] = dim_date_df['FullDate'].dt.quarter
    dim_date_df['Year'] = dim_date_df['FullDate'].dt.year
    dim_date_df['DayOfWeek'] = dim_date_df['FullDate'].dt.day_name()
    dim_date_df['IsDayOff'] = (dim_date_df['FullDate'].dt.weekday >= 5).astype(int)
    dim_date_df['IsAfterHours'] = dim_date_df['FullDate'].dt.hour >= 17

    return dim_date_df
# -------------------------
# 3.Load
# -------------------------

def load_dim_date(duck_conn, dim_date_df: pd.DataFrame):
    """
    Incremental insert into dw.Dim_Date — only adds dates not already present.
    """
    logger.info(f"Syncing {len(dim_date_df)} dates to warehouse...")
    duck_conn.register("tmp_dim_date", dim_date_df)
    duck_conn.execute("""
        INSERT INTO dw.Dim_Date (DateKey, FullDate, Day, Month, MonthName, Quarter, Year, DayOfWeek, IsDayOff, IsAfterHours)
        SELECT
            tmp_dim_date.DateKey,
            tmp_dim_date.FullDate,
            tmp_dim_date.Day,
            tmp_dim_date.Month,
            tmp_dim_date.MonthName,
            tmp_dim_date.Quarter,
            tmp_dim_date.Year,
            tmp_dim_date.DayOfWeek,
            tmp_dim_date.IsDayOff,
            tmp_dim_date.IsAfterHours
        FROM tmp_dim_date
        WHERE NOT EXISTS (
            SELECT 1 FROM dw.Dim_Date
            WHERE dw.Dim_Date.DateKey = tmp_dim_date.DateKey
        );
    """)
    logger.info("Dim_Date incremental load completed successfully.")

# -------------------------
# Orchestration
# -------------------------

def run_dim_date_etl(duck_conn):
    try:
        # extract global boundries from OLTP 
        min_d, max_d = extract_date_range()

        #transform into calandar 
        dim_date_df = transform_dim_date(min_d, max_d)
        load_dim_date(duck_conn, dim_date_df)
        logger.info("✅ Dim_Date ETL completed successfully.")
    except Exception as e:
        logger.error(f"Dim_Date ETL process failed: {e}")
        raise



# -------------------------
# TEST
# -------------------------
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    duck_conn = get_warehouse_conn()

    try:
        logger.info("STARTING TEST")
        duck_conn.execute("BEGIN TRANSACTION;")
        run_dim_date_etl(duck_conn)
        count = duck_conn.execute("SELECT COUNT(*) FROM dw.Dim_Date").fetchone()[0]
        logger.info(f"✅ Success! {count} rows would have been inserted.")

        duck_conn.execute("ROLLBACK;")
        logger.info("✅ Transaction ROLLED BACK")

    except Exception as e:
        duck_conn.execute("ROLLBACK;")
        logger.error(f"❌Standalone run failed: {e}")
    finally:
        duck_conn.close()