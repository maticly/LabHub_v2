import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import streamlit as st

# Page Configuration
st.set_page_config(
    page_title="LabHub | Lab Intelligence",
    page_icon="🔬",
    layout="wide"
)

import pandas as pd
import duckdb
from analytics.data.connect_db import PROJECT_ROOT
from analytics.data.connect_db import WAREHOUSE_DB
from app.styles import apply_custom_style
import plotly.express as px
from vector.search import semantic_search
from inventory_helpers import show_stock_detail
from app.ui.kpi import kpi_card

apply_custom_style()

# Database Connection 
@st.cache_data
def get_data(view_name):
    with duckdb.connect(str(WAREHOUSE_DB), read_only=True) as conn:
        return conn.execute(f"SELECT * FROM dw.{view_name}").df()

# --- HEADER ---
st.title("🔬 LabHub Intelligence")
st.markdown("<p style='font-size: 1.2rem;'>Advanced Inventory Analytics & Audit Trail</p>", unsafe_allow_html=True)


# --- TABS ---
tab_warehouse, tab_search, tab_compliance = st.tabs([
    "🚀 Overview", 
    "🔍 Search", 
    "📜 Compliance"
])

# kpi logic for warehouse tab
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

    # --- ROW 2: DEMAND INTELLIGENCE ---

    
    st.subheader("Lab-Level Hotspots")
    st.caption("Usage and Local Stock by Site, Building, and Room")

    hotspot_detailed = get_data("v_location_hotspots")

    st.dataframe(
        hotspot_detailed,
        column_config={
            "LocationPath": "Campus/Building",
            "RoomNumber": "Room",
            "TotalUsage": st.column_config.NumberColumn("Total Usage", format="%d 📉"),
            "CurrentLocalStock": st.column_config.NumberColumn("Current Room Stock", format="%d 📦"),
            "LastUpdatedKey": st.column_config.DateColumn("Last Updated"),
            "PercentOfCampusUsage": st.column_config.ProgressColumn(
                "% of Campus Total",
                help="Usage in this room compared to the entire campus",
                format="%.1f%%",
                min_value=0,
                max_value=100
            ),
                "PercentOfLabUsage": st.column_config.ProgressColumn(
                    "% of Lab Total",
                    help="Usage in this room compared to the entire lab",
                    format="%.1f%%",
                    min_value=0,
                    max_value=100
                )
            },
        hide_index=True,
        use_container_width=True
        )
        

    st.divider()

    st.subheader("Global Product Demand")
    st.caption("Product usage over time & Stock Balance for whole organization")
        
    performance_df = get_data("v_product_performance_global")
    if not performance_df.empty:
        cols = [c for c in performance_df.columns if c != 'Description']
        performance_df = performance_df[cols + ['Description']]

    max_stock = int(performance_df["GlobalStockBalance"].max()) if not performance_df.empty else 100
        
    # search filter just for this table
    search_term = st.text_input("🔍 Search Products", placeholder="🔍 Type e.g., DNA, Gloves...")
    if search_term:
        mask = performance_df.astype(str).apply(lambda x: x.str.contains(search_term, case=False)).any(axis=1)
        performance_df = performance_df[mask]

    st.dataframe(
        performance_df,
        column_config={
                "ProductID": None,
                "GlobalStockBalance": st.column_config.ProgressColumn(
                    "Global Stock Balance", 
                    format="%d", 
                    min_value=0, 
                    max_value=max(max_stock, 1)
                ),
                "Usage30d": st.column_config.NumberColumn("1 Month Usage", format="%d"),
                "Usage6m": st.column_config.NumberColumn("6 Month Usage", format="%d"),
                "Usage12m": st.column_config.NumberColumn("12 Month Usage", format="%d"),
            },
        hide_index=True,
        use_container_width=True
        )



    # --- ROW 3: Local Product Distribution ---
    st.subheader("🏢 Local Product Distribution")
    matrix_data = get_data("v_product_distribution_detailed")
    
    def color_stock_logic(val):
        # Logic for Buffer and Stock text colors: Red if negative, White if safe
        try:
            return 'color: #ff4b4b' if float(val) < 0 else 'color: white'
        except:
            return 'color: white'
        
    # all numeric columns to integers to remove decimals
    cols_to_fix = ['CurrentStock', 'LocalUsage1Y', 'Threshold', 'StockBuffer']
    for col in cols_to_fix:
        if col in matrix_data.columns:
            matrix_data[col] = matrix_data[col].fillna(0).astype(int)

    # search filter just for this table
    search_term = st.text_input("🔍 Search Products or Locations", placeholder="🔍 Type e.g., DNA, Gloves...")
    if search_term:
        mask = matrix_data.astype(str).apply(lambda x: x.str.contains(search_term, case=False)).any(axis=1)
        matrix_data = matrix_data[mask]

    styled_df = matrix_data.style.map(color_stock_logic, subset=['CurrentStock', 'StockBuffer'])
    st.dataframe(styled_df, 
                column_config={
                    "LocalUsage1M": "1 Month Local Usage",
                    "LocalUsage6M": "6 Months Local Usage",
                    "LocalUsage1Y": "12 Months Local Usage",
                    "Threshold": "Recommended MIN Stock",
                    "StockBuffer": st.column_config.ProgressColumn(
                        "Threshold",
                        help="Distance from Low Stock Threshold",
                        format="%d",
                        min_value=int(matrix_data["Threshold"].min()),
                        max_value=int(matrix_data["StockBuffer"].max())
                    )
                },
                use_container_width=True, hide_index=True)

    st.divider()

    # --- ROW 4: USAGE TRENDS HISTOGRAM ---
    st.subheader("📊 12-Month Usage Trends")
    usage_data = get_data("v_monthly_usage") 

    if not usage_data.empty:
        import plotly.express as px

        # 1. Manual sort order for months
        month_order = ['January', 'February', 'March', 'April', 'May', 'June', 
                       'July', 'August', 'September', 'October', 'November', 'December']
        
        usage_data['MonthName'] = pd.Categorical(
            usage_data['MonthName'], 
            categories=month_order, 
            ordered=True
        )
        
        # Sort data and take top 36 (12 months * 3 categories)
        plot_df = usage_data.sort_values('MonthName').head(36)

        # 2. Create Plotly Figure for granular styling control
        fig = px.bar(
            plot_df, 
            x="MonthName", 
            y="TotalQuantityConsumed", 
            color="CategoryName",
            barmode="group", # This replicates stack=False (side-by-side bars)
            color_discrete_sequence=px.colors.qualitative.Pastel # Optional: softer colors
        )

        # 3. Update Axis Styling: Bold and Light Gray
        fig.update_layout(
            xaxis=dict(
                title=dict(text="Month", font=dict(family="Arial Black", color="#D3D3D3")),
                tickfont=dict(family="Arial Black", color="#D3D3D3", size=12)
            ),
            yaxis=dict(
                title=dict(text="Quantity", font=dict(family="Arial Black", color="#D3D3D3")),
                tickfont=dict(family="Arial Black", color="#C7C2C2", size=12),
                gridcolor="#43444B" # Subtle grid line to match dark theme
            ),
            legend=dict(
                font=dict(color="#D3D3D3"),
                orientation="h", 
                yanchor="bottom", 
                y=1.02, 
                xanchor="right", 
                x=1
            ),
            paper_bgcolor='#0E1117', 
            plot_bgcolor='#0E1117',
            margin=dict(l=0, r=0, t=30, b=0)
        )

        st.plotly_chart(fig, use_container_width=True, config={'displayModeBar': False})

##---------------Search tab

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