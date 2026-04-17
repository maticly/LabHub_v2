import logging
import pandas as pd
from datetime import datetime
from analytics.warehouse.connect_db import get_oltp_connection, get_warehouse_conn

# --- Logging Setup ---
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# -------------------------
# Extract
# -------------------------
def extract_users():
    """
    Pulls user data from the OLTP database, joining Role and Department.
    """
    logger.info("Extracting user data from OLTP...")
    query = """
        SELECT
            [User].UserID,
            [User].FirstName + ' ' + [User].LastName AS UserName,
            UserRole.UserRoleName AS UserRole,
            Department.DepartmentName
        FROM core.[User]
        JOIN core.UserRole ON [User].UserRoleID = UserRole.UserRoleID
        JOIN core.Department ON [User].DepartmentID = Department.DepartmentID;
    """
    conn = get_oltp_connection()
    try:
        df_users = pd.read_sql(query, conn)
        logger.info(f"Successfully extracted {len(df_users)} users.")
        return df_users
    except Exception as e:
        logger.error(f"Failed to extract users: {e}")
        raise
    finally:
        conn.close()

# -------------------------
# Transform
# -------------------------
def transform_dim_user(df_users: pd.DataFrame) -> pd.DataFrame:
    """
    Shapes OLTP user data into Dim_User rows.
    """
    logger.info("Transforming user data...")
    dim_user_df = df_users.copy()
    
    dim_user_df['UserName'] = dim_user_df['UserName'].str.strip()
    dim_user_df['UserRole'] = dim_user_df['UserRole'].str.strip()
    dim_user_df['DepartmentName'] = dim_user_df['DepartmentName'].str.strip()
    
    # Enforces schema column order
    return dim_user_df[["UserID", "UserName", "UserRole", "DepartmentName"]]
# -------------------------
# Load
# -------------------------
def load_dim_user(duck_conn, dim_df: pd.DataFrame, effective_date: str):
    """
    SCD2
    """
    logger.info("SCD2 for dw.Dim_User...")
    duck_conn.register("tmp_dim_user", dim_df)

    # expire old records
    duck_conn.execute(f"""
        UPDATE dw.Dim_User
        SET EndDate = '{effective_date}', IsCurrent = 0
        FROM tmp_dim_user
        WHERE dw.Dim_User.UserID = tmp_dim_user.UserID
        AND dw.Dim_User.IsCurrent = 1
          AND (
              dw.Dim_User.UserName IS DISTINCT FROM tmp_dim_user.UserName OR
              dw.Dim_User.UserRole IS DISTINCT FROM tmp_dim_user.UserRole OR
              dw.Dim_User.DepartmentName IS DISTINCT FROM tmp_dim_user.DepartmentName
          );
    """)

    # Insert new users
    duck_conn.execute(f"""
        INSERT INTO dw.Dim_User (UserID, UserName, UserRole, DepartmentName, EffectiveDate, EndDate, IsCurrent)
        SELECT
            tmp_dim_user.UserID,
            tmp_dim_user.UserName,
            tmp_dim_user.UserRole,
            tmp_dim_user.DepartmentName,
            '{effective_date}' AS EffectiveDate,
            NULL AS EndDate,
            1 AS IsCurrent
        FROM tmp_dim_user
        WHERE NOT EXISTS (
            SELECT 1 FROM dw.Dim_User
            WHERE dw.Dim_User.UserID = tmp_dim_user.UserID 
            AND dw.Dim_User.IsCurrent = 1
        );
    """)

    logger.info(f"Dim_User SCD2. Source rows: {len(dim_df)}.")

# -------------------------
# 4. Orchestration
# -------------------------
def run_dim_user_etl(duck_conn, effective_date: str):
    try:
        raw_users = extract_users()
        dim_user_df = transform_dim_user(raw_users)
        load_dim_user(duck_conn, dim_user_df, effective_date)
        logger.info("✅ Dim_User ETL completed successfully")
    except Exception as e:
        logger.error(f"❌ Dim_User ETL failed: {e}")
        raise


if __name__ == "__main__":
    duck_conn = get_warehouse_conn()
    try:
        duck_conn.execute("BEGIN TRANSACTION;")
        run_dim_user_etl(duck_conn)

        count = duck_conn.execute("SELECT COUNT(*) FROM dw.Dim_User").fetchone()[0]
        currents = duck_conn.execute("SELECT COUNT(*) FROM dw.Dim_User WHERE IsCurrent = 1").fetchone()[0]
        logger.info(f"Test Results: Total Rows={count}, Active Versions={currents}")
    except Exception as e:
        try:
            duck_conn.execute("ROLLBACK;")
        except:
            pass
        logger.error(f"❌ Standalone run failed: {e}")
    finally:
        duck_conn.close()