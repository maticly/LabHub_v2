# --Current inventory--
from multiprocessing.dummy import connection
import duckdb
from analytics.warehouse.connect_db import get_warehouse_conn

def create_analytics_views():
    conn = get_warehouse_conn()

    try:
        #-- Base metrics view
        conn.execute("""
            CREATE OR REPLACE VIEW dw.v_inventory_metrics_base AS
            SELECT 
                ProductKey,
                LocationKey,
                UserKey,
                DateKey,
                -- central logic for metrics
                ABS(SUM(CASE WHEN QuantityDelta < 0 THEN QuantityDelta ELSE 0 END)) AS TotalQuantityConsumed,
                COUNT(CASE WHEN QuantityDelta < 0 THEN 1 END) AS TransactionCount,
                COUNT(CASE WHEN QuantityDelta > 0 THEN 1 END) AS ReplenishmentCount,
                SUM(QuantityDelta) AS NetStockChange
            FROM dw.Fact_Inventory_Transactions
            GROUP BY ProductKey, LocationKey, UserKey, DateKey;
            """)
        
        # --- 1. Current Stock on Hand ---
        conn.execute("""
        CREATE OR REPLACE VIEW dw.v_current_inventory AS
        SELECT 
            Dim_Product.ProductName,
            Dim_Product.CategoryName,
            Dim_Location.SiteName,
            Dim_Location.Building,
            Dim_Location.RoomNumber,
            SUM(Fact_Inventory_Transactions.QuantityDelta) AS StockOnHand,
            Dim_Product.UnitOfMeasure
        FROM dw.Fact_Inventory_Transactions
        JOIN dw.Dim_Product ON Fact_Inventory_Transactions.ProductKey = Dim_Product.ProductKey
        JOIN dw.Dim_Location ON Fact_Inventory_Transactions.LocationKey = Dim_Location.LocationKey
        GROUP BY
            Dim_Product.ProductName, 
            Dim_Product.CategoryName, 
            Dim_Location.SiteName, 
            Dim_Location.Building, 
            Dim_Location.RoomNumber, 
            Dim_Product.UnitOfMeasure
        HAVING SUM(Fact_Inventory_Transactions.QuantityDelta) > 0;
        """)

        # --- 1A. Current Stock of each item ---
        conn.execute("""
        CREATE OR REPLACE VIEW dw.v_kpi_stock_risk AS
        WITH ProductMax AS (
            -- Calculates 20% of the peak historical stock as the threshold
            SELECT 
                ProductKey,
                MAX(AbsoluteQuantity) * 0.20 AS LowStockThreshold
            FROM dw.Fact_Inventory_Transactions
            GROUP BY ProductKey
        ),
        CurrentStock AS (
            SELECT ProductKey, SUM(QuantityDelta) AS StockOnHand
            FROM dw.Fact_Inventory_Transactions
            GROUP BY ProductKey
        )
        SELECT 
            COUNT(*) AS LowStockCount
        FROM CurrentStock
        JOIN ProductMax ON CurrentStock.ProductKey = ProductMax.ProductKey
        WHERE CurrentStock.StockOnHand < ProductMax.LowStockThreshold AND CurrentStock.StockOnHand > 0;
        """)

        # --- 2. Monthly Usage Trends ---
        conn.execute("""
        CREATE OR REPLACE VIEW dw.v_monthly_usage AS
        SELECT 
            Dim_Date.Year,
            Dim_Date.Month,
            Dim_Date.MonthName,
            Dim_Product.CategoryName,
            SUM(v_inventory_metrics_base.TotalQuantityConsumed) AS TotalQuantityConsumed,
        FROM dw.v_inventory_metrics_base
        JOIN dw.Dim_Date ON v_inventory_metrics_base.DateKey = Dim_Date.DateKey
        JOIN dw.Dim_Product ON v_inventory_metrics_base.ProductKey = Dim_Product.ProductKey
        GROUP BY 
            Dim_Date.Year, 
            Dim_Date.Month, 
            Dim_Date.MonthName, 
            Dim_Product.CategoryName
        ORDER BY Dim_Date.Year DESC, Dim_Date.Month DESC;
        """)

        # --- 2A. Monthly-over-month events ---
        conn.execute("""
        CREATE OR REPLACE VIEW dw.v_kpi_monthly_events AS
        SELECT 
            Dim_Date.Year,
            Dim_Date.Month,
            COUNT(*) AS EventCount,
            LAG(COUNT(*)) OVER (ORDER BY Dim_Date.Year, Dim_Date.Month) AS PreviousMonthCount
        FROM dw.Fact_Inventory_Transactions
        JOIN dw.Dim_Date ON Fact_Inventory_Transactions.DateKey = Dim_Date.DateKey
        GROUP BY 
            Dim_Date.Year, 
            Dim_Date.Month
        ORDER BY Dim_Date.Year DESC, Dim_Date.Month DESC;
        """)

        # --- 2B. products with zero consumption ---
        conn.execute("""
        CREATE OR REPLACE VIEW dw.v_kpi_zero_usage AS
        SELECT 
            COUNT(DISTINCT Dim_Product.ProductKey) AS ZeroUsageCount,
        FROM dw.Dim_Product
        LEFT JOIN dw.Fact_Inventory_Transactions ON Dim_Product.ProductKey = Fact_Inventory_Transactions.ProductKey
            AND Fact_Inventory_Transactions.QuantityDelta < 0
            AND Fact_Inventory_Transactions.DateKey >= (SELECT MIN(DateKey) FROM dw.Dim_Date WHERE Month = extract(month from current_date))
        WHERE Fact_Inventory_Transactions.TransactionID IS NULL;
        """)

        # --- 3. User Activity Audit ---
        # Useful for a "Recent Activity" table in your UI
        conn.execute("""
        CREATE OR REPLACE VIEW dw.v_recent_activity AS
        SELECT 
            Dim_Date.FullDate,
            Dim_User.UserName,
            Dim_Product.ProductName,
            Fact_Inventory_Transactions.EventType,
            Fact_Inventory_Transactions.QuantityDelta,
            Dim_Location.SiteName
        FROM dw.Fact_Inventory_Transactions
        JOIN dw.Dim_Date ON Fact_Inventory_Transactions.DateKey = Dim_Date.DateKey
        JOIN dw.Dim_User ON Fact_Inventory_Transactions.UserKey = Dim_User.UserKey
        JOIN dw.Dim_Product ON Fact_Inventory_Transactions.ProductKey = Dim_Product.ProductKey
        JOIN dw.Dim_Location ON Fact_Inventory_Transactions.LocationKey = Dim_Location.LocationKey
        ORDER BY Dim_Date.FullDate DESC;
        """)

        # --- 4. Product Consumption Summary
        conn.execute("""
        CREATE OR REPLACE VIEW dw.v_consumption_summary AS
        SELECT 
            Dim_Product.ProductName,
            Dim_Product.CategoryName,
            Dim_Product.UnitOfMeasure,
            SUM(v_inventory_metrics_base.TotalQuantityConsumed) AS TotalQuantityConsumed,
            SUM(v_inventory_metrics_base.TransactionCount) AS TransactionCount,
        FROM dw.v_inventory_metrics_base
        JOIN dw.Dim_Product ON v_inventory_metrics_base.ProductKey = Dim_Product.ProductKey
        GROUP BY 
            Dim_Product.ProductName, 
            Dim_Product.CategoryName, 
            Dim_Product.UnitOfMeasure
        ORDER BY TotalQuantityConsumed DESC;
        """)

        # --- 5. Location level Hotspots ---
        conn.execute("""
        CREATE OR REPLACE VIEW dw.v_location_hotspots AS
        WITH LatestProductStock AS (
            -- latest snapshot for every product in every location as latest_rank = 1
            SELECT 
                LocationKey, --for each location and product key
                ProductKey,
                AbsoluteQuantity,
                ROW_NUMBER() OVER ( -- assign a row number based in the location, product, and latest transaction ID
                    PARTITION BY LocationKey, ProductKey 
                    ORDER BY DateKey DESC, TransactionID DESC
                ) as latest_rank
            FROM dw.Fact_Inventory_Transactions
            ),
            LatestDate AS (
            SELECT 
                LocationKey, 
                MAX(DateKey) as LastDateKey
            FROM dw.Fact_Inventory_Transactions
            GROUP BY LocationKey
            ),
            RoomStockBalance AS (
            -- Sum only the most recent snapshots for each room
                SELECT 
                    LocationKey,
                    SUM(AbsoluteQuantity) as TrueCurrentStock
                FROM LatestProductStock
                WHERE latest_rank = 1 -- the letest known stock snapshot of this product at this location
                GROUP BY LocationKey
            ),
            LabGlobalUsage AS (
                SELECT SUM(ABS(QuantityDelta)) as GlobalTotal
                FROM dw.Fact_Inventory_Transactions
                WHERE QuantityDelta < 0 --to show only gross number, not inventory change
            ),
            CampusUsage AS (
                SELECT
                    dw.Dim_Location.SiteName,
                    SUM(ABS(dw.Fact_Inventory_Transactions.QuantityDelta)) AS CampusTotal
                    FROM dw.Fact_Inventory_Transactions
                    JOIN dw.Dim_Location ON dw.Fact_Inventory_Transactions.LocationKey = dw.Dim_Location.LocationKey
                    WHERE dw.Fact_Inventory_Transactions.QuantityDelta < 0 --to show only gross number, not inventory change
                    GROUP BY dw.Dim_Location.SiteName
            )
        SELECT
            dw.Dim_Location.SiteName || ' › ' || dw.Dim_Location.Building as LocationPath,
            dw.Dim_Location.RoomNumber,
            -- -- USAGE: The sum of what was taken out (displayed as a positive number)
            SUM(CASE WHEN dw.Fact_Inventory_Transactions.QuantityDelta < 0 
                    THEN ABS(dw.Fact_Inventory_Transactions.QuantityDelta) ELSE 0 END) AS TotalUsage,
            -- Stock: The actual shelf balance from the CTE
            MAX(RoomStockBalance.TrueCurrentStock) AS CurrentLocalStock,
            MAX(DATE(STRPTIME(CAST(LatestDate.LastDateKey AS VARCHAR), '%Y%m%d'))) AS LastUpdatedKey,
            -- Percentage: Usage vs Lab Global
            ROUND((SUM(CASE WHEN dw.Fact_Inventory_Transactions.QuantityDelta < 0 
                            THEN ABS(dw.Fact_Inventory_Transactions.QuantityDelta) ELSE 0 END) * 100.0) 
                  / (SELECT GlobalTotal FROM LabGlobalUsage), 2) as PercentOfLabUsage,
            -- percentage of campus usage
            ROUND((SUM(CASE WHEN dw.Fact_Inventory_Transactions.QuantityDelta < 0 
                            THEN ABS(dw.Fact_Inventory_Transactions.QuantityDelta) ELSE 0 END) * 100.0) 
                  / ANY_VALUE(CampusUsage.CampusTotal), 2) as PercentOfCampusUsage
        FROM dw.Fact_Inventory_Transactions
        JOIN dw.Dim_Location ON dw.Fact_Inventory_Transactions.LocationKey = dw.Dim_Location.LocationKey
        LEFT JOIN RoomStockBalance ON dw.Dim_Location.LocationKey = RoomStockBalance.LocationKey
        LEFT JOIN LatestDate ON dw.Dim_Location.LocationKey = LatestDate.LocationKey
        LEFT JOIN CampusUsage ON dw.Dim_Location.SiteName = CampusUsage.SiteName
        GROUP BY 
            dw.Dim_Location.RoomNumber,
            dw.Dim_Location.SiteName || ' › ' || dw.Dim_Location.Building
        ORDER BY TotalUsage DESC;
        """)

        # --- Global Product Performance (6-Month usage) ---
        conn.execute("""
        CREATE OR REPLACE VIEW dw.v_product_performance_global AS
        WITH DateThresholds AS (
            SELECT 
                CAST(strftime(CURRENT_DATE - INTERVAL '30 days', '%Y%m%d') AS INTEGER) as key_30d,
                CAST(strftime(CURRENT_DATE - INTERVAL '6 months', '%Y%m%d') AS INTEGER) as key_6m,
                CAST(strftime(CURRENT_DATE - INTERVAL '12 months', '%Y%m%d') AS INTEGER) as key_12m
            ),
            LatestGlobalProductStock AS (
                -- Find the latest snapshot for each product per location
                SELECT 
                    ProductKey,
                    LocationKey,
                    AbsoluteQuantity,
                    ROW_NUMBER() OVER (
                        PARTITION BY ProductKey, LocationKey 
                        ORDER BY DateKey DESC, TransactionID DESC
                    ) as latest_rank
                FROM dw.Fact_Inventory_Transactions
            ),
            GlobalStockLevels AS (
                -- Sum the latest counts across ALL locations
                SELECT 
                    ProductKey,
                    SUM(AbsoluteQuantity) as TotalGlobalStock
                FROM LatestGlobalProductStock
                WHERE latest_rank = 1
                GROUP BY ProductKey
            )
        SELECT 
            dw.Dim_Product.ProductID,
            dw.Dim_Product.ProductName,
            dw.Dim_Product.CategoryName,
            dw.Dim_Product.UnitOfMeasure,
            dw.Dim_Product.Description,
            SUM(CASE WHEN dw.Fact_Inventory_Transactions.QuantityDelta < 0 
                    AND dw.Fact_Inventory_Transactions.DateKey >= (SELECT key_30d FROM DateThresholds) 
                    THEN ABS(dw.Fact_Inventory_Transactions.QuantityDelta) ELSE 0 END) AS Usage30d,
            SUM(CASE WHEN dw.Fact_Inventory_Transactions.QuantityDelta < 0 
                    AND dw.Fact_Inventory_Transactions.DateKey >= (SELECT key_6m FROM DateThresholds) 
                    THEN ABS(dw.Fact_Inventory_Transactions.QuantityDelta) ELSE 0 END) AS Usage6m,
            SUM(CASE WHEN dw.Fact_Inventory_Transactions.QuantityDelta < 0 
                    AND dw.Fact_Inventory_Transactions.DateKey >= (SELECT key_12m FROM DateThresholds) 
                    THEN ABS(dw.Fact_Inventory_Transactions.QuantityDelta) ELSE 0 END) AS Usage12m,
            -- Global Balance: Sum of all transactions. 
            MAX(GlobalStockLevels.TotalGlobalStock) AS GlobalStockBalance
        FROM dw.Dim_Product
        JOIN dw.Fact_Inventory_Transactions ON dw.Dim_Product.ProductKey = dw.Fact_Inventory_Transactions.ProductKey
        LEFT JOIN GlobalStockLevels ON dw.Dim_Product.ProductKey = GlobalStockLevels.ProductKey
        GROUP BY 1, 2, 3, 4, 5
        ORDER BY Usage30d DESC;
        """)

        # --- 6. Product x Location Matrix ---
        conn.execute("""
        CREATE OR REPLACE VIEW dw.v_product_location_matrix AS
        SELECT 
            Dim_Product.ProductName,
            Dim_Product.CategoryName,
            Dim_Location.SiteName,
            Dim_Location.Building,
            ABS(SUM(CASE WHEN Fact_Inventory_Transactions.QuantityDelta < 0 
                        THEN Fact_Inventory_Transactions.QuantityDelta 
                        ELSE 0 END)) AS QuantityConsumed,
            SUM(Fact_Inventory_Transactions.QuantityDelta) AS CurrentLocalStock
        FROM dw.Fact_Inventory_Transactions
        JOIN dw.Dim_Product  ON Fact_Inventory_Transactions.ProductKey = dw.Dim_Product.ProductKey
        JOIN dw.Dim_Location ON Fact_Inventory_Transactions.LocationKey = dw.Dim_Location.LocationKey
        GROUP BY 
            Dim_Product.ProductName, 
            Dim_Product.CategoryName, 
            Dim_Location.SiteName, 
            Dim_Location.Building;
        """)

        # --- 6A.  Global Product Distribution ---
        conn.execute("""
        CREATE OR REPLACE VIEW dw.v_product_distribution_detailed AS
        WITH ProductThresholds AS (
            SELECT ProductKey, MAX(AbsoluteQuantity) * 0.20 as LowThreshold
            FROM dw.Fact_Inventory_Transactions GROUP BY ProductKey
        ),
        LocalUsage AS (
            SELECT ProductKey, LocationKey,
                SUM(CASE WHEN QuantityDelta < 0 AND DateKey >= CAST(strftime(CURRENT_DATE - INTERVAL '30 days', '%Y%m%d') AS INTEGER) 
                            THEN ABS(QuantityDelta) ELSE 0 END) as LocalUsage1M,
                SUM(CASE WHEN QuantityDelta < 0 AND DateKey >= CAST(strftime(CURRENT_DATE - INTERVAL '6 months', '%Y%m%d') AS INTEGER) 
                            THEN ABS(QuantityDelta) ELSE 0 END) as LocalUsage6M,
                SUM(CASE WHEN QuantityDelta < 0 AND DateKey >= CAST(strftime(CURRENT_DATE - INTERVAL '1 year', '%Y%m%d') AS INTEGER) 
                            THEN ABS(QuantityDelta) ELSE 0 END) as LocalUsage1Y
            FROM dw.Fact_Inventory_Transactions GROUP BY 1, 2
        ),
        LatestState AS ( --inventory = flow (how much moved) and state (how much left)
            SELECT ProductKey, LocationKey, AbsoluteQuantity,
                   ROW_NUMBER() OVER (PARTITION BY ProductKey, LocationKey ORDER BY DateKey DESC, TransactionID DESC) as r
            FROM dw.Fact_Inventory_Transactions --ledger of every move 
        )
        SELECT 
            dw.Dim_Product.ProductName,
            dw.Dim_Product.CategoryName,
            dw.Dim_Location.SiteName || ' › ' || dw.Dim_Location.Building as LocationPath,
            dw.Dim_Location.RoomNumber,
            MAX(LatestState.AbsoluteQuantity) as CurrentStock,
            MAX(LocalUsage.LocalUsage1Y) as LocalUsage1Y,
            CEIL(MAX(ProductThresholds.LowThreshold)) as Threshold,
            (MAX(LatestState.AbsoluteQuantity) - CEIL(MAX(ProductThresholds.LowThreshold))) as StockBuffer
        FROM dw.Fact_Inventory_Transactions
        JOIN dw.Dim_Product ON dw.Fact_Inventory_Transactions.ProductKey = dw.Dim_Product.ProductKey
        JOIN dw.Dim_Location ON dw.Fact_Inventory_Transactions.LocationKey = dw.Dim_Location.LocationKey
        JOIN LatestState ON dw.Fact_Inventory_Transactions.ProductKey = LatestState.ProductKey 
             AND dw.Fact_Inventory_Transactions.LocationKey = LatestState.LocationKey AND LatestState.r = 1
        LEFT JOIN LocalUsage ON dw.Fact_Inventory_Transactions.ProductKey = LocalUsage.ProductKey 
             AND dw.Fact_Inventory_Transactions.LocationKey = LocalUsage.LocationKey
        LEFT JOIN ProductThresholds ON dw.Fact_Inventory_Transactions.ProductKey = ProductThresholds.ProductKey
        GROUP BY 1, 2, 3, 4;
        """)
        

        # --- 7. User x Product Consumption (Accountability)   
        conn.execute("""
        CREATE OR REPLACE VIEW dw.v_user_product_consumption AS
        SELECT 
            Dim_User.UserName,
            Dim_User.UserRole,
            Dim_Product.ProductName,
            Dim_Product.CategoryName,
            ABS(SUM(CASE WHEN Fact_Inventory_Transactions.QuantityDelta < 0 
                        THEN Fact_Inventory_Transactions.QuantityDelta 
                        ELSE 0 END)) AS TotalQuantityConsumed,
            COUNT(Fact_Inventory_Transactions.TransactionID) AS TotalActions
        FROM dw.Fact_Inventory_Transactions
        JOIN dw.Dim_User ON Fact_Inventory_Transactions.UserKey = dw.Dim_User.UserKey
        JOIN dw.Dim_Product ON Fact_Inventory_Transactions.ProductKey = dw.Dim_Product.ProductKey
        GROUP BY 
            Dim_User.UserName, 
            Dim_User.UserRole, 
            Dim_Product.ProductName, 
            Dim_Product.CategoryName
        ORDER BY TotalQuantityConsumed DESC;
        """)
        # --- 8. Daily Inventory Movement Log (The Audit Trail) 
        conn.execute("""
        CREATE OR REPLACE VIEW dw.v_movement_log AS
        SELECT 
            Dim_Date.FullDate,
            Dim_Product.ProductName,
            Dim_Location.SiteName,
            Dim_Location.Building,
            Dim_User.UserName,
            Fact_Inventory_Transactions.EventType,
            Fact_Inventory_Transactions.QuantityDelta,
            Fact_Inventory_Transactions.AbsoluteQuantity AS NewQuantity,
            Fact_Inventory_Transactions.CurrentStockSnapshot
        FROM dw.Fact_Inventory_Transactions
        JOIN dw.Dim_Date ON Fact_Inventory_Transactions.DateKey = dw.Dim_Date.DateKey
        JOIN dw.Dim_Product ON Fact_Inventory_Transactions.ProductKey = dw.Dim_Product.ProductKey
        JOIN dw.Dim_Location ON Fact_Inventory_Transactions.LocationKey = dw.Dim_Location.LocationKey
        JOIN dw.Dim_User ON Fact_Inventory_Transactions.UserKey = dw.Dim_User.UserKey
        ORDER BY Dim_Date.FullDate DESC;
        """)
        
        print("✅ Analytics Views created in dw schema.")

    except Exception as e:
        print(f"❌ Error creating views: {e}")
        raise
    finally:
        conn.close()

if __name__ == "__main__":
    create_analytics_views()