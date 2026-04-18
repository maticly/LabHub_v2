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
                DeliveryDateKey,
                -- central logic for metrics
                ABS(SUM(CASE WHEN QuantityDelta < 0 THEN QuantityDelta ELSE 0 END)) AS TotalQuantityConsumed,
                COUNT(CASE WHEN QuantityDelta < 0 THEN 1 END) AS TransactionCount,
                COUNT(CASE WHEN QuantityDelta > 0 THEN 1 END) AS ReplenishmentCount,
                SUM(QuantityDelta) AS NetStockChange
            FROM dw.Fact_Inventory_Transactions
            GROUP BY ProductKey, LocationKey, UserKey, DeliveryDateKey;
            """)
        
        conn.execute("""
        CREATE OR REPLACE VIEW dw.v_po_summary AS
        SELECT 
            COUNT(CASE WHEN DeliveryDateKey = 19000101 THEN 1 END) as OpenOrdersCount,
            AVG(CASE WHEN DeliveryDateKey > 19000101 THEN VendorLeadTimeDays END) as AvgLeadTime
        FROM dw.Fact_Purchase_Orders;
        """)
        
        #----------used in last diagram
        # --- 2. Monthly Usage Trends ---
        conn.execute("""
        CREATE OR REPLACE VIEW dw.v_monthly_usage AS
        SELECT 
            Dim_Date.Year,
            Dim_Date.Month,
            Dim_Date.MonthName,
            Dim_Product.CategoryName,
            SUM(v_inventory_metrics_base.TotalQuantityConsumed) AS TotalQuantityConsumed
        FROM dw.v_inventory_metrics_base
        JOIN dw.Dim_Date ON v_inventory_metrics_base.DeliveryDateKey = Dim_Date.DateKey
        JOIN dw.Dim_Product ON v_inventory_metrics_base.ProductKey = Dim_Product.ProductKey
        GROUP BY 
            Dim_Date.Year, 
            Dim_Date.Month, 
            Dim_Date.MonthName, 
            Dim_Product.CategoryName
        ORDER BY Dim_Date.Year DESC, Dim_Date.Month DESC;
        """)
        
        #----------used in kpi
        # --- 2A. Monthly-over-month events ---
        conn.execute("""
        CREATE OR REPLACE VIEW dw.v_kpi_monthly_events AS
        SELECT 
            Dim_Date.Year,
            Dim_Date.Month,
            COUNT(*) AS EventCount,
            LAG(COUNT(*)) OVER (ORDER BY Dim_Date.Year, Dim_Date.Month) AS PreviousMonthCount
        FROM dw.Fact_Inventory_Transactions
        JOIN dw.Dim_Date ON Fact_Inventory_Transactions.DeliveryDateKey = Dim_Date.DateKey
        GROUP BY 
            Dim_Date.Year, 
            Dim_Date.Month
        ORDER BY Dim_Date.Year DESC, Dim_Date.Month DESC;
        """)

        #----------used in kpi
        # --- 2B. products with zero consumption ---
        conn.execute("""
        CREATE OR REPLACE VIEW dw.v_kpi_zero_usage AS
        SELECT 
            COUNT(DISTINCT Dim_Product.ProductKey) AS ZeroUsageCount
        FROM dw.Dim_Product
        LEFT JOIN dw.Fact_Inventory_Transactions ON Dim_Product.ProductKey = Fact_Inventory_Transactions.ProductKey
            AND Fact_Inventory_Transactions.QuantityDelta < 0
            AND Fact_Inventory_Transactions.DeliveryDateKey >= CAST(strftime(CURRENT_DATE - INTERVAL '30 days', '%Y%m%d') AS INT)
        WHERE Fact_Inventory_Transactions.TransactionID IS NULL;
        """)


        #----------used in kpi
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
                    ORDER BY DeliveryDateKey DESC, TransactionID DESC
                ) as latest_rank
            FROM dw.Fact_Inventory_Transactions
            ),
            LatestDate AS (
            SELECT 
                LocationKey, 
                MAX(DeliveryDateKey) as LastDateKey
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

        #----------used in first tab
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
                        ORDER BY DeliveryDateKey DESC, TransactionID DESC
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
                    AND dw.Fact_Inventory_Transactions.DeliveryDateKey >= (SELECT key_30d FROM DateThresholds) 
                    THEN ABS(dw.Fact_Inventory_Transactions.QuantityDelta) ELSE 0 END) AS Usage30d,
            SUM(CASE WHEN dw.Fact_Inventory_Transactions.QuantityDelta < 0 
                    AND dw.Fact_Inventory_Transactions.DeliveryDateKey >= (SELECT key_6m FROM DateThresholds) 
                    THEN ABS(dw.Fact_Inventory_Transactions.QuantityDelta) ELSE 0 END) AS Usage6m,
            SUM(CASE WHEN dw.Fact_Inventory_Transactions.QuantityDelta < 0 
                    AND dw.Fact_Inventory_Transactions.DeliveryDateKey >= (SELECT key_12m FROM DateThresholds) 
                    THEN ABS(dw.Fact_Inventory_Transactions.QuantityDelta) ELSE 0 END) AS Usage12m,
            -- Global Balance: Sum of all transactions. 
            MAX(GlobalStockLevels.TotalGlobalStock) AS GlobalStockBalance
        FROM dw.Dim_Product
        JOIN dw.Fact_Inventory_Transactions ON dw.Dim_Product.ProductKey = dw.Fact_Inventory_Transactions.ProductKey
        LEFT JOIN GlobalStockLevels ON dw.Dim_Product.ProductKey = GlobalStockLevels.ProductKey
        GROUP BY 1, 2, 3, 4, 5
        ORDER BY Usage30d DESC;
        """)

        #----------used in first tab
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


        #----------used in kpi
        # --- 6A.  Global Product Distribution ---
        conn.execute("""
        CREATE OR REPLACE VIEW dw.v_product_distribution_detailed AS
        WITH ProductThresholds AS (
            SELECT ProductKey, MAX(AbsoluteQuantity) * 0.20 as LowThreshold
            FROM dw.Fact_Inventory_Transactions GROUP BY ProductKey
        ),
        LocalUsage AS (
            SELECT ProductKey, LocationKey,
                SUM(CASE WHEN QuantityDelta < 0 AND DeliveryDateKey >= CAST(strftime(CURRENT_DATE - INTERVAL '30 days', '%Y%m%d') AS INTEGER) 
                            THEN ABS(QuantityDelta) ELSE 0 END) as LocalUsage1M,
                SUM(CASE WHEN QuantityDelta < 0 AND DeliveryDateKey >= CAST(strftime(CURRENT_DATE - INTERVAL '6 months', '%Y%m%d') AS INTEGER) 
                            THEN ABS(QuantityDelta) ELSE 0 END) as LocalUsage6M,
                SUM(CASE WHEN QuantityDelta < 0 AND DeliveryDateKey >= CAST(strftime(CURRENT_DATE - INTERVAL '1 year', '%Y%m%d') AS INTEGER) 
                            THEN ABS(QuantityDelta) ELSE 0 END) as LocalUsage1Y
            FROM dw.Fact_Inventory_Transactions GROUP BY 1, 2
        ),
        LatestState AS ( --inventory = flow (how much moved) and state (how much left)
            SELECT ProductKey, LocationKey, AbsoluteQuantity,
                   ROW_NUMBER() OVER (PARTITION BY ProductKey, LocationKey ORDER BY DeliveryDateKey DESC, TransactionID DESC) as r
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
        


        #----------this stays 
        # --- 8. Daily Inventory Movement Log (The Audit Trail) 
        conn.execute("""
        CREATE OR REPLACE VIEW dw.v_movement_log AS
        SELECT 
            Dim_Date.FullDate,
            Dim_Product.ProductName,
            Dim_Location.SiteName,
            Dim_Location.Building,
            Dim_User.UserName,
            dw.Dim_Stock_Event.StockEventReason AS EventReason,
            Fact_Inventory_Transactions.QuantityDelta,
            Fact_Inventory_Transactions.AbsoluteQuantity AS NewQuantity,
            Fact_Inventory_Transactions.CurrentStockSnapshot
        FROM dw.Fact_Inventory_Transactions
        JOIN dw.Dim_Date ON Fact_Inventory_Transactions.EventDateKey = dw.Dim_Date.DateKey
        JOIN dw.Dim_Product ON Fact_Inventory_Transactions.ProductKey = dw.Dim_Product.ProductKey
        JOIN dw.Dim_Location ON Fact_Inventory_Transactions.LocationKey = dw.Dim_Location.LocationKey
        JOIN dw.Dim_User ON Fact_Inventory_Transactions.UserKey = dw.Dim_User.UserKey
        JOIN dw.Dim_Stock_Event ON Fact_Inventory_Transactions.StockEventKey = Dim_Stock_Event.StockEventKey
        ORDER BY Dim_Date.FullDate DESC;
        """)

        # --- SUPPLY CHAIN & RISK ANALYTICS ---
        # 9 Expiration Exposure (30, 60, 90 Days) ---
        conn.execute("""
        CREATE OR REPLACE VIEW dw.v_expiration_exposure AS
        WITH DateThresholds AS (
            SELECT 
                CAST(strftime(CURRENT_DATE + INTERVAL '30 days', '%Y%m%d') AS INTEGER) as key_30d,
                CAST(strftime(CURRENT_DATE + INTERVAL '60 days', '%Y%m%d') AS INTEGER) as key_60d,
                CAST(strftime(CURRENT_DATE + INTERVAL '90 days', '%Y%m%d') AS INTEGER) as key_90d
            ),
        LatestItemStock AS (
            SELECT 
                ProductKey,
                LocationKey,
                ExpirationDateKey,
                CurrentStockSnapshot,
                ROW_NUMBER() OVER (PARTITION BY ProductKey, LocationKey, LotNumber ORDER BY ExpirationDateKey DESC, TransactionID DESC) as latest_rank
            FROM dw.Fact_Inventory_Transactions
            WHERE ExpirationDateKey > 19000101)
        SELECT 
            dw.Dim_Product.ProductName,
            dw.Dim_Product.CategoryName,
            SUM(CASE WHEN LatestItemStock.ExpirationDateKey <= (SELECT key_30d FROM DateThresholds) THEN LatestItemStock.CurrentStockSnapshot ELSE 0 END) AS Expiring_30d,
            SUM(CASE WHEN LatestItemStock.ExpirationDateKey <= (SELECT key_60d FROM DateThresholds) THEN LatestItemStock.CurrentStockSnapshot ELSE 0 END) AS Expiring_60d,
            SUM(CASE WHEN LatestItemStock.ExpirationDateKey <= (SELECT key_90d FROM DateThresholds) THEN LatestItemStock.CurrentStockSnapshot ELSE 0 END) AS Expiring_90d
        FROM LatestItemStock
        JOIN dw.Dim_Product ON LatestItemStock.ProductKey = dw.Dim_Product.ProductKey
        WHERE LatestItemStock.latest_rank = 1 AND LatestItemStock.CurrentStockSnapshot > 0
        GROUP BY dw.Dim_Product.ProductName, dw.Dim_Product.CategoryName;
        """)

        # 10 Waste-to-Usage Ratio by Category ---
        conn.execute("""
        CREATE OR REPLACE VIEW dw.v_waste_usage_ratio AS
        SELECT 
            Dim_Product.CategoryName,
            SUM(CASE WHEN Dim_Stock_Event.StockEventReason IN ('Damage', 'Disposal') THEN ABS(Fact_Inventory_Transactions.QuantityDelta) ELSE 0 END) AS TotalWaste,
            SUM(CASE WHEN Dim_Stock_Event.StockEventReason = 'Use' THEN ABS(Fact_Inventory_Transactions.QuantityDelta) ELSE 0 END) AS TotalConsumption,
            ROUND(
                SUM(CASE WHEN Dim_Stock_Event.StockEventReason IN ('Damage', 'Disposal') THEN ABS(Fact_Inventory_Transactions.QuantityDelta) ELSE 0 END) / 
                NULLIF(SUM(CASE WHEN Dim_Stock_Event.StockEventReason = 'Use' THEN ABS(Fact_Inventory_Transactions.QuantityDelta) ELSE 0 END), 0), 
            4) AS WasteUsageRatio
        FROM dw.Fact_Inventory_Transactions
        JOIN dw.Dim_Product ON Fact_Inventory_Transactions.ProductKey = Dim_Product.ProductKey
        JOIN dw.Dim_Stock_Event ON Fact_Inventory_Transactions.StockEventKey = Dim_Stock_Event.StockEventKey
        GROUP BY Dim_Product.CategoryName;
        """)

        # 11 Expiration Risk
        conn.execute("""
        CREATE OR REPLACE VIEW dw.v_location_expiration_risk AS
        WITH ItemRisk AS (
            SELECT 
                dw.Fact_Inventory_Transactions.LocationKey,
                dw.Fact_Inventory_Transactions.ProductKey,
                dw.Fact_Inventory_Transactions.LotNumber,
                dw.Fact_Inventory_Transactions.ExpirationDateKey,
                dw.Fact_Inventory_Transactions.CurrentStockSnapshot,
                ROW_NUMBER() OVER (
                    PARTITION BY dw.Fact_Inventory_Transactions.ProductKey,
                                dw.Fact_Inventory_Transactions.LocationKey,
                                dw.Fact_Inventory_Transactions.LotNumber
                    ORDER BY dw.Fact_Inventory_Transactions.ExpirationDateKey DESC,
                            dw.Fact_Inventory_Transactions.TransactionID DESC
                ) AS latest_rank
            FROM dw.Fact_Inventory_Transactions
        )
        SELECT
            dw.Dim_Location.SiteName,
            dw.Dim_Location.Building,
            dw.Dim_Location.RoomNumber,
            ItemRisk.ProductKey,
            ItemRisk.LotNumber,
            ItemRisk.ExpirationDateKey,
            ItemRisk.CurrentStockSnapshot AS CurrentStockSnapshot,
            DATEDIFF(
                'day',
                CURRENT_DATE,
                CAST(STRPTIME(CAST(ItemRisk.ExpirationDateKey AS VARCHAR), '%Y%m%d') AS DATE)
            ) AS DaysUntilExpiration
        FROM ItemRisk
        JOIN dw.Dim_Location
            ON ItemRisk.LocationKey = dw.Dim_Location.LocationKey
        WHERE ItemRisk.latest_rank = 1
        AND ItemRisk.CurrentStockSnapshot > 0
        AND ItemRisk.ExpirationDateKey > CAST(strftime(CURRENT_DATE, '%Y%m%d') AS INT);
        """)
                     
        # Room‑level Expiration summary view
        conn.execute("""
        CREATE OR REPLACE VIEW dw.v_location_expiration_summary AS
        SELECT
            SiteName,
            Building,
            RoomNumber,
            SUM(CASE WHEN DaysUntilExpiration < 30 THEN CurrentStockSnapshot ELSE 0 END) AS AtRiskStock30,
            SUM(CurrentStockSnapshot) AS TotalStock,
            SUM(CASE WHEN DaysUntilExpiration < 30 THEN ItemRisk.ProductKey ELSE 0 END) AS AtRiskProducts30,
        FROM dw.v_location_expiration_risk
        GROUP BY SiteName, Building, RoomNumber;
        """)
                    

        # 12 Shelf Life Efficiency ---
        conn.execute("""
        CREATE OR REPLACE VIEW dw.v_shelf_life_efficiency AS
        WITH ProductTimelines AS (
            SELECT 
                ProductKey,
                LotNumber,
                MIN(CASE WHEN QuantityDelta > 0 
                    THEN CAST(STRPTIME(CAST(DeliveryDateKey AS VARCHAR), '%Y%m%d') AS DATE) END) AS ReceivedDate,
                MIN(CASE WHEN QuantityDelta < 0 THEN CAST(STRPTIME(CAST(EventDateKey AS VARCHAR), '%Y%m%d') AS DATE) END) AS FirstUseDate,
                MAX(CAST(STRPTIME(CAST(ExpirationDateKey AS VARCHAR), '%Y%m%d') AS DATE)) AS ExpirationDate
            FROM dw.Fact_Inventory_Transactions
            GROUP BY ProductKey, LotNumber
        )
        SELECT 
            dw.Dim_Product.ProductName,
            ProductTimelines.LotNumber,
            ProductTimelines.ReceivedDate,
            DATEDIFF('day', ProductTimelines.ReceivedDate, ProductTimelines.FirstUseDate) AS DaysToFirstUse,
            DATEDIFF('day', ProductTimelines.ReceivedDate, ProductTimelines.ExpirationDate)
                AS TotalShelfLifeDays,

            ROUND(CAST(DATEDIFF('day', ProductTimelines.ReceivedDate, ProductTimelines.FirstUseDate) AS FLOAT)
                /NULLIF(DATEDIFF('day', ProductTimelines.ReceivedDate, ProductTimelines.ExpirationDate),
                    0),4) AS ShelfLifeConsumptionIndex
        FROM ProductTimelines
        JOIN dw.Dim_Product ON ProductTimelines.ProductKey = dw.Dim_Product.ProductKey
        WHERE ProductTimelines.FirstUseDate IS NOT NULL AND ProductTimelines.ReceivedDate IS NOT NULL;
        """)

        # 13 Product Dwell Time by Location ---
        # bottlenecks and exposure risks
        conn.execute("""
        CREATE OR REPLACE VIEW dw.v_product_dwell_time AS
        WITH ProductReceipts AS (
            SELECT 
                dw.Fact_Inventory_Transactions.ProductKey,
                dw.Fact_Inventory_Transactions.LocationKey,
                dw.Fact_Inventory_Transactions.LotNumber,
                MIN(CAST(strptime(CAST(dw.Fact_Inventory_Transactions.DeliveryDateKey AS VARCHAR), '%Y%m%d') AS DATE)) AS FirstReceivedDate
            FROM dw.Fact_Inventory_Transactions
            WHERE dw.Fact_Inventory_Transactions.QuantityDelta > 0
            GROUP BY 
                dw.Fact_Inventory_Transactions.ProductKey, 
                dw.Fact_Inventory_Transactions.LocationKey, 
                dw.Fact_Inventory_Transactions.LotNumber
        ),
        CurrentStock AS (
            SELECT 
                dw.Fact_Inventory_Transactions.ProductKey,
                dw.Fact_Inventory_Transactions.LocationKey,
                dw.Fact_Inventory_Transactions.LotNumber,
                SUM(dw.Fact_Inventory_Transactions.QuantityDelta) AS OnHand
            FROM dw.Fact_Inventory_Transactions
            GROUP BY 
                dw.Fact_Inventory_Transactions.ProductKey, 
                dw.Fact_Inventory_Transactions.LocationKey, 
                dw.Fact_Inventory_Transactions.LotNumber
            HAVING SUM(dw.Fact_Inventory_Transactions.QuantityDelta) > 0
        )
        SELECT 
            dw.Dim_Product.ProductName,
            dw.Dim_Product.CategoryName,
            dw.Dim_Location.SiteName,
            dw.Dim_Location.Building,
            dw.Dim_Location.RoomNumber,
            CurrentStock.LotNumber,
            ProductReceipts.FirstReceivedDate,
            DATEDIFF('day', ProductReceipts.FirstReceivedDate, CURRENT_DATE) AS DwellDays
        FROM CurrentStock
        JOIN ProductReceipts ON CurrentStock.ProductKey = ProductReceipts.ProductKey 
            AND CurrentStock.LocationKey = ProductReceipts.LocationKey
            AND CurrentStock.LotNumber = ProductReceipts.LotNumber
        JOIN dw.Dim_Product ON CurrentStock.ProductKey = dw.Dim_Product.ProductKey
        JOIN dw.Dim_Location ON CurrentStock.LocationKey = dw.Dim_Location.LocationKey
        ORDER BY DwellDays DESC;
        """)

        # 14 Inventory Leakage Patterns ---
        # Detects loss, misplacement, or improper handling
        conn.execute("""
        CREATE OR REPLACE VIEW dw.v_inventory_leakage_report AS
        SELECT 
            dw.Dim_Product.ProductName,
            dw.Dim_Product.CategoryName,
            dw.Dim_Location.SiteName,
            SUM(CASE WHEN dw.Dim_Stock_Event.StockEventReason = 'Consume' 
                     THEN ABS(dw.Fact_Inventory_Transactions.QuantityDelta) ELSE 0 END) AS RecordedConsumption,
            SUM(CASE WHEN dw.Dim_Stock_Event.StockEventReason IN ('Waste', 'Correction') AND dw.Fact_Inventory_Transactions.QuantityDelta < 0 
                     THEN ABS(dw.Fact_Inventory_Transactions.QuantityDelta) ELSE 0 END) AS LeakageQuantity,
            ROUND(
                SUM(CASE WHEN dw.Dim_Stock_Event.StockEventReason IN ('Waste', 'Correction') AND dw.Fact_Inventory_Transactions.QuantityDelta < 0 
                         THEN ABS(dw.Fact_Inventory_Transactions.QuantityDelta) ELSE 0 END) / 
                NULLIF(SUM(CASE WHEN dw.Dim_Stock_Event.StockEventReason = 'Consume' 
                                THEN ABS(dw.Fact_Inventory_Transactions.QuantityDelta) ELSE 0 END), 0), 
            4) AS LeakageUsageRatio
        FROM dw.Fact_Inventory_Transactions
        JOIN dw.Dim_Product ON dw.Fact_Inventory_Transactions.ProductKey = dw.Dim_Product.ProductKey
        JOIN dw.Dim_Location ON dw.Fact_Inventory_Transactions.LocationKey = dw.Dim_Location.LocationKey
        JOIN dw.Dim_Stock_Event ON dw.Fact_Inventory_Transactions.StockEventKey = dw.Dim_Stock_Event.StockEventKey
        GROUP BY 
            dw.Dim_Product.ProductName, 
            dw.Dim_Product.CategoryName, 
            dw.Dim_Location.SiteName
        HAVING LeakageQuantity > 0
        ORDER BY LeakageUsageRatio DESC;
        """)


        # 16: User with highest product consumption per category
        conn.execute("""
        CREATE OR REPLACE VIEW dw.v_top_consumers_by_category AS 
        SELECT 
            dw.Dim_Product.CategoryName, 
            dw.Dim_User.UserName, 
            dw.Dim_User.DepartmentName, 
            SUM(ABS(dw.Fact_Inventory_Transactions.QuantityDelta)) AS TotalConsumedQuantity, 
            RANK() OVER (
                PARTITION BY dw.Dim_Product.CategoryName 
                ORDER BY SUM(ABS(dw.Fact_Inventory_Transactions.QuantityDelta)) DESC
            ) AS ConsumptionRank 
        FROM dw.Fact_Inventory_Transactions 
        JOIN dw.Dim_Product ON dw.Fact_Inventory_Transactions.ProductKey = dw.Dim_Product.ProductKey 
        JOIN dw.Dim_User ON dw.Fact_Inventory_Transactions.UserKey = dw.Dim_User.UserKey 
        WHERE dw.Fact_Inventory_Transactions.QuantityDelta < 0 
        GROUP BY 
            dw.Dim_Product.CategoryName, 
            dw.Dim_User.UserName, 
            dw.Dim_User.DepartmentName;
        """)


        # 17: After Hours movment audit 
        #Inventory movements during non-standard operating hours
        conn.execute("""
        CREATE OR REPLACE VIEW dw.v_after_hours_movement_audit AS 
        SELECT 
            dw.Dim_Date.FullDate, 
            dw.Dim_Date.DayOfWeek, 
            dw.Dim_User.UserName, 
            dw.Dim_Location.SiteName, 
            dw.Dim_Location.Building, 
            dw.Dim_Product.ProductName, 
            dw.Fact_Inventory_Transactions.QuantityDelta, 
            dw.Fact_Inventory_Transactions.LotNumber 
        FROM dw.Fact_Inventory_Transactions 
        JOIN dw.Dim_Date ON dw.Fact_Inventory_Transactions.EventDateKey = dw.Dim_Date.DateKey 
        JOIN dw.Dim_User ON dw.Fact_Inventory_Transactions.UserKey = dw.Dim_User.UserKey 
        JOIN dw.Dim_Location ON dw.Fact_Inventory_Transactions.LocationKey = dw.Dim_Location.LocationKey 
        JOIN dw.Dim_Product ON dw.Fact_Inventory_Transactions.ProductKey = dw.Dim_Product.ProductKey 
        WHERE dw.Dim_Date.IsAfterHours = TRUE;
        """)

        # 18: users generating orphaned or incomplete transactions
        #uncorrected negative stock and ledger imbalances
        conn.execute("""
        CREATE OR REPLACE VIEW dw.v_process_integrity_gaps AS
        SELECT 
            dw.Dim_User.UserName,
            dw.Dim_User.DepartmentName,
            COUNT(dw.Fact_Inventory_Transactions.TransactionID) AS TotalSuspectTransactions,
            SUM(CASE WHEN dw.Fact_Inventory_Transactions.CurrentStockSnapshot < 0 THEN 1 ELSE 0 END) AS NegativeBalanceEvents,
            SUM(CASE WHEN dw.Fact_Inventory_Transactions.AbsoluteQuantity < 0 THEN 1 ELSE 0 END) AS LogicIntegrityErrors
        FROM dw.Fact_Inventory_Transactions         
            JOIN dw.Dim_User ON dw.Fact_Inventory_Transactions.UserKey = dw.Dim_User.UserKey
        GROUP BY dw.Dim_User.UserName, dw.Dim_User.DepartmentName
            HAVING SUM(CASE WHEN dw.Fact_Inventory_Transactions.CurrentStockSnapshot < 0 OR
                dw.Fact_Inventory_Transactions.AbsoluteQuantity < 0 THEN 1 ELSE 0 END) > 0;       
        """)

        print("✅ Analytics Views created in dw schema.")

    except Exception as e:
        print(f"❌ Error creating views: {e}")
        raise
    finally:
        conn.close()

if __name__ == "__main__":
    create_analytics_views()