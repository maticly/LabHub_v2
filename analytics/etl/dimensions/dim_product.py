import logging
import pandas as pd
from datetime import datetime
from analytics.warehouse.connect_db import get_oltp_connection, get_warehouse_conn

# --- Logging Setup ---
logger = logging.getLogger(__name__)

# -------------------------
# 1. Extract
# -------------------------
def extract_products():
    """
    Extracts raw product data from OLTP, tracking columns for SCD2.
    """
    query = """
        SELECT
            core.Product.ProductID,
            core.Product.ProductName,
            core.ProductCategory.CategoryName,
            core.UnitOfMeasure.UnitName AS UnitOfMeasure,
            CAST(core.Product.Description AS NVARCHAR(MAX)) AS Description,
            core.Product.IsHazardous,
            AVG(supply.[Order].UnitPrice) AS UnitCost,
            core.Product.StorageConditionID
        FROM core.Product
        JOIN core.ProductCategory ON core.Product.ProductCategoryID = core.ProductCategory.CategoryID
        JOIN core.UnitOfMeasure ON core.Product.UnitID = core.UnitOfMeasure.UnitID
        LEFT JOIN supply.[Order] 
            ON supply.[Order].ProductID = core.Product.ProductID
        GROUP BY
            core.Product.ProductID,
            core.Product.ProductName,
            core.ProductCategory.CategoryName,
            core.UnitOfMeasure.UnitName,
            CAST(core.Product.Description AS NVARCHAR(MAX)),
            core.Product.IsHazardous,
            core.Product.StorageConditionID
    """
    conn = get_oltp_connection()
    try:
        df_products = pd.read_sql(query, conn)
        df_products = df_products.rename(columns={'UnitCost': 'unit_cost'})
        logger.info(f"Successfully extracted {len(df_products)} products.")
        return df_products
    except Exception as e:
        logger.error(f"Failed to extract products: {e}")
        raise
    finally:
        conn.close()

# -------------------------
# 2. Transform
# -------------------------
def transform_dim_product(df_products: pd.DataFrame) -> pd.DataFrame:
    """
    Shapes OLTP product data into Dim_Product rows.
    """
    logger.info("Transforming product data...")
    dim_product_df = df_products.copy()

    dim_product_df['ProductName'] = dim_product_df['ProductName'].str.strip()
    dim_product_df['Description'] = dim_product_df['Description'].fillna("").str.strip()
    dim_product_df['IsHazardous'] = dim_product_df['IsHazardous'].astype(int)

    before = len(dim_product_df)
    dim_product_df = dim_product_df.drop_duplicates(subset=['ProductID'], keep='first')
    after = len(dim_product_df)
    if before != after:
        logger.warning(f"Dropped {before - after} duplicate ProductIDs in transform — check extract query.")

    return dim_product_df


# -------------------------
# 3. Load (SCD2)
# -------------------------
def load_dim_product(duck_conn, dim_df: pd.DataFrame, effective_date: str):
    
    duck_conn.register("tmp_dim_product", dim_df)

    # expire old records
    logger.info("Expiring changed product versions...")
    duck_conn.execute(f"""
        UPDATE dw.Dim_Product
        SET EndDate = '{effective_date}', IsCurrent = 0
        FROM tmp_dim_product
        WHERE dw.Dim_Product.ProductID = tmp_dim_product.ProductID
            AND dw.Dim_Product.IsCurrent = 1
            AND (
                dw.Dim_Product.ProductName IS DISTINCT FROM tmp_dim_product.ProductName OR
                dw.Dim_Product.CategoryName IS DISTINCT FROM tmp_dim_product.CategoryName OR
                dw.Dim_Product.UnitOfMeasure IS DISTINCT FROM tmp_dim_product.UnitOfMeasure OR
                dw.Dim_Product.Description IS DISTINCT FROM tmp_dim_product.Description OR
                dw.Dim_Product.unit_cost IS DISTINCT FROM tmp_dim_product.unit_cost OR
                dw.Dim_Product.IsHazardous IS DISTINCT FROM tmp_dim_product.IsHazardous OR
                dw.Dim_Product.StorageConditionID IS DISTINCT FROM tmp_dim_product.StorageConditionID
            );   
    """)

    # new products
    logger.info("Inserting new versions only where no current row exists...")
    duck_conn.execute(f"""
        INSERT INTO dw.Dim_Product (ProductID, ProductName, CategoryName, UnitOfMeasure, 
                      Description, IsHazardous, unit_cost, StorageConditionID, 
                      EffectiveDate, EndDate, IsCurrent)
        SELECT                      
            tmp_dim_product.ProductID,
            tmp_dim_product.ProductName,
            tmp_dim_product.CategoryName,
            tmp_dim_product.UnitOfMeasure,
            tmp_dim_product.Description,
            tmp_dim_product.IsHazardous,
            tmp_dim_product.unit_cost,
            tmp_dim_product.StorageConditionID,
            '{effective_date}' AS EffectiveDate,
            NULL AS EndDate,
            1 AS IsCurrent
        FROM tmp_dim_product
        WHERE NOT EXISTS (
            SELECT 1 FROM dw.Dim_Product
            WHERE dw.Dim_Product.ProductID = tmp_dim_product.ProductID
              AND dw.Dim_Product.IsCurrent = 1
        );
    """)

    logger.info(f"Dim_Product ✅ - source rows: {len(dim_df)}.")


# -------------------------
# 4. Orchestration
# -------------------------
def run_dim_product_etl(duck_conn, effective_date: str):
    """
    run_pipeline.py will call this function to execute the full ETL for Dim_Product.
    """
    try:
        raw_products = extract_products()
        dim_product_df = transform_dim_product(raw_products)
        load_dim_product(duck_conn, dim_product_df, effective_date)
        logger.info("✅ Dim_Product SCD2 ETL completed successfully.")
    except Exception as e:
        logger.error(f"❌ Dim_Product SCD2 ETL failed: {e}")
        raise


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - [Dim_Product_Test] - %(message)s')
    from analytics.warehouse.connect_db import get_oltp_connection
    import pandas as pd

    def get_effective_date():
        conn = get_oltp_connection()
        try:
            df = pd.read_sql("SELECT MIN(EventDate) AS min_date FROM inventory.StockEvent", conn)
            return pd.to_datetime(df['min_date'].iloc[0]).strftime('%Y-%m-%d %H:%M:%S')
        finally:
            conn.close()

    duck_conn = get_warehouse_conn()
    try:
        duck_conn.execute("BEGIN TRANSACTION;")
        effective_date = get_effective_date()
        run_dim_product_etl(duck_conn, effective_date)

        count = duck_conn.execute("SELECT COUNT(*) FROM dw.Dim_Product").fetchone()[0]
        currents = duck_conn.execute("SELECT COUNT(*) FROM dw.Dim_Product WHERE IsCurrent = 1").fetchone()[0]
        logger.info(f"Test Results: Total Rows in DW={count}, Active Versions={currents}")

        duck_conn.execute("ROLLBACK;")
        logger.info("Transaction ROLLED BACK.")
    except Exception as e:
        duck_conn.execute("ROLLBACK;")
        logger.error(f"❌ Standalone run failed: {e}")
    finally:
        duck_conn.close()