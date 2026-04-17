"""Reusable UI elements (cards, charts)"""

$ python -m analytics.warehouse.create_views

python -m streamlit run app/main_dashboard.py

python -m analytics.warehouse.data_quality

python -m vector.search

python -m analytics.etl.run_pipeline

python -m streamlit run app/main_dashboard.py





"""
# ---------- KPI COLOR GRADIENT ----------
def gradient_color(value, min_val, max_val):
    """
    Returns smooth red → yellow → green gradient.
    value: metric value
    min_val: worst value
    max_val: best value
    """

    BAD  = (237, 185, 185)
    OK   = (237, 218, 185)
    GOOD = (208, 237, 185)

    if max_val == min_val:
        ratio = 0.5
    else:
        ratio = (value - min_val) / (max_val - min_val)

    ratio = max(0, min(1, ratio))

    def lerp(c1, c2, t):
        return tuple(int(c1[i] + (c2[i] - c1[i]) * t) for i in range(3))

    if ratio < 0.5:
        rgb = lerp(BAD, OK, ratio * 2)
    else:
        rgb = lerp(OK, GOOD, (ratio - 0.5) * 2)

    return f"rgb({rgb[0]}, {rgb[1]}, {rgb[2]})"

def get_kpi_metrics():
    # 1. Low Stock Risk
    low_stock_data = get_data("v_product_distribution_detailed")
    low_stock = len(low_stock_data[low_stock_data['StockBuffer'] < 0])


    # 2. Stock Events & MoM
    events_df = get_data("v_kpi_monthly_events")
    if not events_df.empty and len(events_df) >= 1:
        current_month = events_df.iloc[-1]
        curr_events = current_month["EventCount"]
        
        # Check if there is a previous month to compare to
        prev_val = current_month["PreviousMonthCount"]
        if pd.isna(prev_val) or prev_val == 0:
            mom_change = 0.0  # No data for previous month
        else:
            mom_change = ((curr_events - prev_val) / prev_val) * 100
    else:
        curr_events, mom_change = 0, 0.0
    
    # 3. Zero Usage
    zero_usage = get_data("v_kpi_zero_usage")["ZeroUsageCount"].iloc[0]
    
    # 4. Most Active Location
    hotspot_df = get_data("v_location_hotspots")
    if not hotspot_df.empty:
        top_row = hotspot_df.iloc[0]
        full_path = top_row['LocationPath'] 
        site = full_path.split(' › ')[0]
        bldg = full_path.split(' › ')[1] if ' › ' in full_path else "Main"
    else:
        site, bldg = "N/A", ""
        
    return low_stock, curr_events, mom_change, zero_usage, site, bldg


with tab_warehouse:
    low_stock, curr_events, mom_change, zero_usage, site, bldg = get_kpi_metrics()

    low_stock_color = gradient_color(
    value=max(0, 100 - low_stock),
    min_val=0,
    max_val=100)

    events_color = gradient_color(
        curr_events, 0, 500
    )

    zero_usage_color = gradient_color(
        max(0, 200 - zero_usage), 0, 200
    )

    location_color = "#DEE7F1"

    # --- ROW 1: KPI CARDS ---
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        kpi_card("Low Stock", low_stock, "Critical Items", low_stock_color)

    with col2:
        kpi_card("Events This Month", f"{int(curr_events):,}",
                f"{mom_change:+.1f}% MoM", events_color)

    with col3:
        kpi_card("30 Days No Use", zero_usage, "Stock Audit", zero_usage_color)

    with col4:
        kpi_card("Top Location", site, bldg, location_color)

    st.divider()
"""



# 15: Vendor Freshness Audit ---
        #Identifies vendors delivering products near expiration.
        conn.execute("""
        CREATE OR REPLACE VIEW dw.v_vendor_freshness_audit AS
        WITH DeliveryStats AS (
            SELECT 
                dw.Fact_Purchase_Orders.VendorKey,
                dw.Fact_Purchase_Orders.ProductKey,
                dw.Fact_Purchase_Orders.PurchaseOrderID,
                CAST(strptime(CAST(dw.Fact_Purchase_Orders.DeliveryDateKey AS VARCHAR), '%Y%m%d') AS DATE) AS DeliveryDate,
                CAST(strptime(CAST(dw.Fact_Inventory_Transactions.ExpirationDateKey AS VARCHAR), '%Y%m%d') AS DATE) AS ExpirationDate
            FROM dw.Fact_Purchase_Orders
            JOIN dw.Fact_Inventory_Transactions ON dw.Fact_Purchase_Orders.PurchaseOrderID = dw.Fact_Inventory_Transactions.PurchaseOrderID
            WHERE dw.Fact_Purchase_Orders.DeliveryDateKey > 19000101 
              AND dw.Fact_Inventory_Transactions.ExpirationDateKey > 19000101
        )
        SELECT 
            dw.Dim_Vendor.VendorName,
            dw.Dim_Product.ProductName,
            AVG(DATEDIFF('day', DeliveryStats.DeliveryDate, DeliveryStats.ExpirationDate)) AS AvgShelfLifeDaysAtDelivery,
            COUNT(DeliveryStats.PurchaseOrderID) AS TotalDeliveriesAudited
        FROM DeliveryStats
        JOIN dw.Dim_Vendor ON DeliveryStats.VendorKey = dw.Dim_Vendor.VendorKey
        JOIN dw.Dim_Product ON DeliveryStats.ProductKey = dw.Dim_Product.ProductKey
        GROUP BY 
            dw.Dim_Vendor.VendorName, 
            dw.Dim_Product.ProductName
        ORDER BY AvgShelfLifeDaysAtDelivery ASC;
        """)


##---------------Search tab
from vector.search import semantic_search
from inventory_helpers import show_stock_detail


def render_tab_search_logic(db_path):
    st.title("Semantic Lab Inventory Search")

    # Load warehouse view
    stock_data = get_data("v_product_performance_global")
    
    stock_data["ProductID"] = stock_data["ProductID"].astype(str)

    # --- Sidebar Filters ---
    st.sidebar.header("Search Filters")
    category_filter = st.sidebar.multiselect(
        "Filter by Category",
        options=sorted(stock_data["CategoryName"].unique()),
        default=sorted(stock_data["CategoryName"].unique()),
        key="search_cat_filter"
    )
    
    # --- Search Input ---
    query = st.text_input("Search for items by use-case (e.g., 'things for cleaning glass' or 'PCR reagents')",
                          placeholder="🔍 Type e.g., Durable glass beaker for heating",
                          key="semantic_search_input")
    
    if not query:
        st.info("Enter a query above to begin searching.")
        return
    
    # get matches from ChromaDB
    matches = semantic_search(query, n_results=15)
        
    if not matches:
        st.warning("No matches found in the vector store.")
        return

    # query DuckDB
    try:
        match_map = {m['id']: m['distance'] for m in matches}
        product_ids = list(match_map.keys())
        
        df_results = stock_data[stock_data["ProductID"].isin(product_ids)]
        df_results = df_results[df_results["CategoryName"].isin(category_filter)]

        # Display Results
        if df_results.empty:
            st.info("Matches found, but they don't match your category filters.")
        else:
            # Map distances back to the dataframe for sorting
            df_results['score'] = df_results['ProductID'].astype(str).map(match_map)
            df_results = df_results.sort_values('score')

            # Fetch location paths for matched products
            conn = duckdb.connect(db_path)
            placeholders = ", ".join(["?"] * len(product_ids))

            location_df = conn.execute(f"""
                SELECT
                    dw.Dim_Product.ProductID,
                    dw.v_product_distribution_detailed.LocationPath
                FROM dw.v_product_distribution_detailed
                JOIN dw.Dim_Product ON dw.v_product_distribution_detailed.ProductName = dw.Dim_Product.ProductName
                WHERE dw.Dim_Product.ProductID IN ({placeholders}) 
                AND dw.v_product_distribution_detailed.CurrentStock > 0
            """, product_ids).df()

            conn.close()

            # Aggregates locations
            location_df = (
                location_df.groupby("ProductID")["LocationPath"]
                .apply(lambda x: " | ".join(sorted(set(x))))
                .to_dict()
            )

            for _, row in df_results.iterrows():
                with st.container():
                    col1, col2 = st.columns([4, 1])
                    with col1:
                        st.subheader(row['ProductName'])
                        st.caption(f"**Category:** {row['CategoryName']}")

                        stock = int(row["GlobalStockBalance"])
                        st.markdown(f"📦 **Global Stock:** {stock}")
                        st.write(row['Description'] if row['Description'] else "No description available.")

                    with col2:
                        # Converts distance to a 'match percentage' for the UI
                        match_pct = max(0, int((1 - row['score']) * 100))
                        st.metric("Match", f"{match_pct}%")
                        
                        if st.button("View Details", key=f"btn_{row['ProductID']}"):
                            show_stock_detail(row['ProductID'], db_path)
        conn.close()
    except Exception as e:
        st.error(f"Error querying the database: {e}")

st.divider()

with tab_search:
    render_tab_search_logic(str(WAREHOUSE_DB))

##-----------------

with tab_compliance:
    st.subheader("📜 System Audit Trail")
    st.markdown("Full history of every inventory transaction for regulatory compliance.")

    # 1. Load Data
    log_data = get_data("v_movement_log")

    # 2. Layout & Search (Single Column Definition)
    #search_col, export_col = st.columns([1, 2])
    toolbar = st.container()
    col1, col2 = toolbar.columns([1,1])

    with toolbar:
        # label_visibility="collapsed" is the secret to vertical alignment
        log_filter = st.text_input(
            "Filter Search", 
            placeholder="🔍 Search by user, product, or event...", 
            label_visibility="collapsed"
        )

    # 3. Filter Logic
    if log_filter:
        mask = log_data.apply(lambda row: row.astype(str).str.contains(log_filter, case=False).any(), axis=1)
        display_df = log_data[mask]
    else:
        display_df = log_data

    with toolbar:
        # Export the CURRENT filtered view
        csv = display_df.to_csv(index=False).encode('utf-8')
        st.download_button(
            label="📥 Export CSV",
            data=csv,
            file_name='labhub_audit_log.csv',
            mime='text/csv'
        )

    # 4. Display Table
    st.dataframe(
        display_df, 
        use_container_width=True, 
        hide_index=True,
        column_config={
            "QuantityDelta": st.column_config.NumberColumn("Change", format="%+d"),
            "NewQuantity": "Total After"
        }
    )