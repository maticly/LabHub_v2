import logging
import pandas as pd
from pathlib import Path
from analytics.data.connect_db import get_oltp_connection, get_warehouse_conn

# --- Logging Setup ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - [Dim_Product] - %(message)s'
)
logger = logging.getLogger(__name__)

PROJECT_ROOT = Path(__file__).resolve().parents[2] 
CSV_DESCRIPTION_PATH = PROJECT_ROOT / "data" / "generated_data_OLTP" / "core.Product_with_Descriptions.csv"

# 1. EXTRACT
def extract_products():
    """
    Extracts raw product data from SQL Server.
    """
    query = """
        SELECT
            Product.ProductID,
            Product.ProductName,
            ProductCategory.CategoryName,
            UnitOfMeasure.UnitName AS UnitOfMeasure
        FROM core.Product
        JOIN core.ProductCategory ON Product.ProductCategoryID = ProductCategory.CategoryID
        JOIN core.UnitOfMeasure ON Product.UnitID = UnitOfMeasure.UnitID;
    """
    conn = get_oltp_connection()
    try:
        df_products = pd.read_sql(query, conn)
        logger.info(f"Successfully extracted {len(df_products)} products.")
        return df_products
    except Exception as e:
        logger.error(f"Failed to extract products: {e}")
        raise
    finally:
        conn.close()

# 2. TRANSFORM
def transform_dim_product(df_products: pd.DataFrame) -> pd.DataFrame:
    """
    Shapes OLTP product data into Dim_Product rows.
    Merges AI-generated descriptions from CSV if available —
    description is not an OLTP attribute so it is handled here in the ETL layer.
    """
    logger.info("Transforming product data...")
    dim_product_df = df_products.copy()
    dim_product_df['ProductName'] = dim_product_df['ProductName'].str.strip()

    if CSV_DESCRIPTION_PATH.exists():
        try:
            df_descriptions = pd.read_csv(CSV_DESCRIPTION_PATH, usecols=["ProductID", "Description"])
            dim_product_df = dim_product_df.merge(df_descriptions, on="ProductID", how="left")
            logger.info(f"Descriptions merged from CSV ({len(df_descriptions)} rows).")
        except Exception as e:
            logger.warning(f"Failed to load descriptions CSV: {e}. Description column will be NULL.")
            dim_product_df['Description'] = None
    else:
        logger.warning(f"Descriptions CSV not found at {CSV_DESCRIPTION_PATH}. Description column will be NULL.")
        dim_product_df['Description'] = None

    return dim_product_df[["ProductID", "ProductName", "CategoryName", "UnitOfMeasure", "Description"]]


# 3. LOAD
def load_dim_product(duck_conn, dim_df: pd.DataFrame):
    
    logger.info("Upserting data into dw.Dim_Product...")
    duck_conn.register("tmp_dim_product", dim_df)

    # Step 1: Update changed attributes on existing rows
    duck_conn.execute("""
        UPDATE dw.Dim_Product
        SET
            ProductName   = tmp_dim_product.ProductName,
            CategoryName  = tmp_dim_product.CategoryName,
            UnitOfMeasure = tmp_dim_product.UnitOfMeasure,
            Description   = tmp_dim_product.Description
        FROM tmp_dim_product
        WHERE dw.Dim_Product.ProductID = tmp_dim_product.ProductID
          AND (
              dw.Dim_Product.ProductName   IS DISTINCT FROM tmp_dim_product.ProductName   OR
              dw.Dim_Product.CategoryName  IS DISTINCT FROM tmp_dim_product.CategoryName  OR
              dw.Dim_Product.UnitOfMeasure IS DISTINCT FROM tmp_dim_product.UnitOfMeasure OR
              dw.Dim_Product.Description   IS DISTINCT FROM tmp_dim_product.Description
          );
    """)

    # Step 2: Insert new products
    duck_conn.execute("""
        INSERT INTO dw.Dim_Product (ProductID, ProductName, CategoryName, UnitOfMeasure, Description)
        SELECT
            tmp_dim_product.ProductID,
            tmp_dim_product.ProductName,
            tmp_dim_product.CategoryName,
            tmp_dim_product.UnitOfMeasure,
            tmp_dim_product.Description
        FROM tmp_dim_product
        WHERE NOT EXISTS (
            SELECT 1 FROM dw.Dim_Product
            WHERE dw.Dim_Product.ProductID = tmp_dim_product.ProductID
        );
    """)

    logger.info(f"Dim_Product upsert complete. Source rows: {len(dim_df)}.")


# 4. ORCHESTRATION
def run_dim_product_etl(duck_conn):
    """
    Orchestrates the Product Dimension ETL.
    Accepts a shared connection — does NOT open its own or manage transactions.
    """
    try:
        raw_products = extract_products()
        dim_product_df = transform_dim_product(raw_products)
        load_dim_product(duck_conn, dim_product_df)
        logger.info("✅ Dim_Product ETL completed successfully.")
    except Exception as e:
        logger.error(f"Dim_Product ETL failed: {e}")
        raise


if __name__ == "__main__":
    duck_conn = get_warehouse_conn()
    try:
        duck_conn.execute("BEGIN TRANSACTION;")
        run_dim_product_etl(duck_conn)
        duck_conn.execute("COMMIT;")

        count = duck_conn.execute("SELECT COUNT(*) FROM dw.Dim_Product").fetchone()[0]
        desc_count = duck_conn.execute(
            "SELECT COUNT(*) FROM dw.Dim_Product WHERE Description IS NOT NULL"
        ).fetchone()[0]
        logger.info(f"🧪 Post-load check: {count} records in Dim_Product, {desc_count} with descriptions.")
    except Exception as e:
        duck_conn.execute("ROLLBACK;")
        logger.error(f"Standalone run failed: {e}")
    finally:
        duck_conn.close()