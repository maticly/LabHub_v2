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
import plotly.express as px

from analytics.warehouse.connect_db import WAREHOUSE_DB
from app.styles import apply_custom_style
from app.ui.kpi import kpi_card

apply_custom_style()

# Database Connection 
@st.cache_data
def get_data(view_name):
    with duckdb.connect(str(WAREHOUSE_DB), read_only=True) as conn:
        return conn.execute(f"SELECT * FROM dw.{view_name}").df()

# kpi logic for warehouse tab
def get_header_kpis():
    # 1. Low Stock Risk
    low_stock_data = get_data("v_product_distribution_detailed")
    low_stock = len(low_stock_data[low_stock_data['StockBuffer'] < 0])

    # 2. Active Risk Index (% expiring < 30 days)
    risk_df = get_data("v_location_expiration_summary")
    risk_count = int(
        risk_df[risk_df['DaysUntilExpiration'] < 30]['CurrentStockSnapshot'].sum()
    )

    # 3. Stock Events & MoM
    events_df = get_data("v_kpi_monthly_events")
    curr_month = ["Dim_Date.Month"]
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

    # 4. Open Orders & Lead Time
    po_df = get_data("v_po_summary")
    open_pos = po_df['OpenOrdersCount'].iloc[0]
    avg_lead = po_df['AvgLeadTime'].iloc[0]

    return low_stock, curr_events, curr_month, mom_change, risk_count, open_pos, avg_lead

# --- UI LAYOUT ---

# Title & Subheader
st.title("🔬 LabHub Intelligence")
st.markdown("Advanced Inventory Analytics & Audit Trail")

# Fetch Data
low_stock, curr_events, curr_month, mom_change, risk_count, open_pos, avg_lead = get_header_kpis()

# --- ROW 1: HEADER KPI CARDS ---
col1, col2, col3, col4 = st.columns(4)

with col1:
    kpi_card("Low Stock Items", f"{low_stock}", "Below Safety Threshold", "#FFD6D6" if low_stock > 100 else "#D6FFD6")

with col2:
    risk_color = "#FFD6D6" if risk_count > 50 else "#FFF4D6" if risk_count > 10 else "#D6FFD6"
    kpi_card("Expiration Risk",
        f"{risk_count}",
        "Units expiring < 30 days",
        risk_color
    )

with col3:
    mom_color = "#D6FFD6" if mom_change >= 0 else "#FFD6D6"
    kpi_card(
        f"Stock Events in {curr_month}",
        f"{curr_events}",
        f"MoM Change: {mom_change:.1f}%",
        mom_color
    )

with col4:
    kpi_card("Open Orders", f"{open_pos} POs", f"Avg Lead: {avg_lead:.1f} Days", "#DEE7F1")

st.divider()

# --- FIVE TABS STRUCTURE ---
tab1, tab2, tab3, tab4, tab5 = st.tabs([
    "📦 Product Visibility", 
    "⛓️ Lineage & Traceability", 
    "👤 User Accountability", 
    "🧠 Semantic Search",
    "⚙️ System Health"
])

with tab1:
    st.subheader("Product Visibility & Expiration Risk")
    st.subheader("📦 Inventory Distribution & Expiry")
    col_a, col_b = st.columns([2, 1])
    with col_a:
        st.dataframe(get_data("v_product_distribution_detailed"), use_container_width=True)
    with col_b:
        st.dataframe(get_data("v_location_expiration_risk"), use_container_width=True)
    
with tab2:
    st.subheader("Lineage, Traceability, & Loss Prevention")
    # Insert View 10 logic here (v_inventory_movement_lineage)

with tab3:
    st.subheader("User Accountability & Behavioral Patterns")
    # Insert View 18 logic here (v_user_waste_behavior)

with tab4:
    st.subheader("Semantic Search Engine")
    query = st.text_input("Search for materials, chemical properties, or storage protocols...", placeholder="e.g. 'Flammable acids in Building B'")
    if query:
        st.info(f"Searching for: {query}")
        # Semantic search implementation goes here

with tab5:
    st.subheader("System Health & Data Quality")
    # Placeholder for Pipeline Status and Data Quality checks



########################################################################
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

