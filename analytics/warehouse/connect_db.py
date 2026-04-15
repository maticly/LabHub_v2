"""
THIS IS THE OLTP CONNECTION FILE.
IT CONTAINS FUNCTIONS TO CONNECT TO THE SQL SERVER OLTP DATABASE AND THE DUCKDB WAREHOUSE.
IT ALSO HAS SOME TEST FUNCTIONS TO CHECK THE CONNECTIONS AND RUN SAMPLE QUERIES.
"""

try:
    import pyodbc
except ImportError:
    pyodbc = None
import pandas as pd
import duckdb
from pathlib import Path

# -------------------------
# Config
# -------------------------
PROJECT_ROOT = Path(__file__).resolve().parents[0]
WAREHOUSE_DB = PROJECT_ROOT / "warehouse.duckdb"


Driver = 'ODBC Driver 18 for SQL Server'
Server = r'TABLET-LTM0C509\SQLEXPRESS01'
Database = 'LabHub_v2'

def get_oltp_connection(): 
    """Returns a connection to the SQL Server OLTP."""
    conn_str = ( 
        f"DRIVER={{{Driver}}};"
        f"SERVER={Server};"
        f"DATABASE={Database};"
        f"Trusted_Connection=yes;"
        f"TrustServerCertificate=yes;"
        f"Encrypt=yes;"
    ) 
    return pyodbc.connect(conn_str)

def get_warehouse_conn():
    """Returns a connection to the DuckDB Warehouse."""
    return duckdb.connect(str(WAREHOUSE_DB))

def test_query():
    conn = get_oltp_connection()
    query = "SELECT TOP 5 * FROM inventory.StockEvent;"  # change to a table that exists
    df = pd.read_sql(query, conn)
    print(df)
    conn.close()

if __name__ == "__main__":
    try:
        test_query()
        print("✅ Connection successful and query executed.")
    except Exception as e:
        print("❌ Connection failed.")
        print(e)

