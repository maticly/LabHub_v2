import duckdb
from pathlib import Path

"""
THIS IS THE WAREHOUSE INITIALIZATION FILE. 
IT CREATES THE DUCKDB FILE AND RUNS THE DDL TO CREATE THE SCHEMA.
IT DOES NOT LOAD ANY DATA. THAT HAPPENS IN THE ETL STEP.
"""

# Paths
PROJECT_ROOT = Path(__file__).resolve().parent
DB_PATH = PROJECT_ROOT / "warehouse.duckdb"
SCHEMA_SQL_PATH = PROJECT_ROOT / "warehouse_schema.sql"

def init_warehouse():

    if DB_PATH.exists():
        try:
            DB_PATH.unlink()
            print(f"⚠️ Existing warehouse found and deleted: {DB_PATH}")
        except PermissionError:
            print(f"❌ Permission denied when trying to delete existing warehouse at {DB_PATH}.")
            print("Please close any applications that might be using the file and try again.")
            return
        
    # Ensure data folder exists
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)

    # Connect to DuckDB
    conn = duckdb.connect(str(DB_PATH))

    try:
        # Read DDL
        with open(SCHEMA_SQL_PATH, "r", encoding="utf-8") as f:
            full_sql = f.read()

        # Split the file by semicolon to run statements one by one
        statements = full_sql.split(';')
        for statement in statements:
            clean_sql = statement.strip()
            if clean_sql:
                conn.execute(clean_sql)
        
        print(f"✅ Warehouse initialized at: {DB_PATH}")

        # VERIFICATION
        print("\n--- Verifying Tables ---")
        check_sql = "SELECT table_name FROM information_schema.tables WHERE table_schema = 'dw'"
        tables = conn.execute(check_sql).fetchdf()

        if tables.empty:
            print("❌ Connection successful, but no tables found in the warehouse.")
        else:
            print(tables)

    except Exception as e:
        print(f"❌ Failed to initialize: {e}")
    finally:
        conn.close()

if __name__ == "__main__":
    init_warehouse()