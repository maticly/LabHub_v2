import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import streamlit as st
import pandas as pd
import duckdb
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots

from analytics.warehouse.connect_db import WAREHOUSE_DB

# ── Page config ────────────────────────────────────────────────
st.set_page_config(
    page_title="LabHub Intelligence",
    page_icon="🔬",
    layout="wide",
    initial_sidebar_state="collapsed",
)
 
# ── Theme ─────────────────────────────────────────────────────────────────────
DARK   = "#0E1117"
CARD   = "#2D323B"
BORDER = "#21262D"
TEXT1  = "#E6EDF3"
TEXT2  = "#8B949E"
ACCENT = "#58A6FF"
GREEN  = "#3FB950"
AMBER  = "#D29922"
RED    = "#F85149"
PURPLE = "#BC8CFF"
 
st.markdown(f"""
<style>
@import url('https://fonts.googleapis.com/css2?family=DM+Mono:wght@400;500&family=Syne:wght@400;600;700;800&display=swap');
 
html, body, [class*="css"] {{
    font-family: 'Syne', sans-serif;
    background: {DARK};
    color: {TEXT1};
}}
.block-container {{ padding: 2.5rem 2rem 3rem; max-width: 100%; }}
 
/* KPI cards */
.kpi-grid {{ display: grid; grid-template-columns: repeat(4, 1fr); gap: 12px; margin-bottom: 1.5rem; }}
.kpi-card {{
    background: {CARD};
    border: 1px solid {BORDER};
    border-radius: 10px;
    padding: 18px 20px;
    position: relative;
    overflow: hidden;
}}
.kpi-card::before {{
    content: '';
    position: absolute;
    top: 0; left: 0; right: 0;
    height: 2px;
}}
.kpi-card.ok::before   {{ background: {GREEN}; }}
.kpi-card.warn::before {{ background: {AMBER}; }}
.kpi-card.bad::before  {{ background: {RED}; }}
.kpi-card.info::before {{ background: {ACCENT}; }}
.kpi-label {{ font-size: 11px; font-weight: 600; letter-spacing: .08em; text-transform: uppercase; color: {TEXT2}; margin-bottom: 8px; }}
.kpi-value {{ font-size: 32px; font-weight: 800; line-height: 1; margin-bottom: 6px; }}
.kpi-sub   {{ font-size: 12px; color: {TEXT2}; font-family: 'DM Mono', monospace; }}
 
/* Section headers */
.section-head {{ font-size: 13px; font-weight: 700; letter-spacing: .06em; text-transform: uppercase;
    color: {TEXT2}; border-bottom: 1px solid {BORDER}; padding-bottom: 6px; margin: 1.5rem 0 .8rem; }}
 
/* Tab styling */
.stTabs [data-baseweb="tab-list"] {{ gap: 4px; background: transparent; border-bottom: 1px solid {BORDER}; }}
.stTabs [data-baseweb="tab"] {{
    background: transparent; border: none; color: {TEXT2};
    font-family: 'Syne', sans-serif; font-weight: 600; font-size: 13px;
    padding: 8px 16px; border-radius: 6px 6px 0 0;
}}
.stTabs [aria-selected="true"] {{ background: {CARD} !important; color: {TEXT1} !important; border-bottom: 2px solid {ACCENT} !important; }}
 
/* Search bar */
.stTextInput input {{
    background: {CARD}; border: 1px solid {BORDER}; color: {TEXT1};
    font-family: 'DM Mono', monospace; font-size: 13px; border-radius: 6px;
}}
.stTextInput input:focus {{ border-color: {ACCENT}; box-shadow: 0 0 0 2px rgba(88,166,255,.15); }}
 
/* Dataframe */
.stDataFrame {{ border: 1px solid {BORDER}; border-radius: 8px; overflow: hidden; }}
 
/* Divider */
hr {{ border-color: {BORDER}; margin: 1.5rem 0; }}
</style>
""", unsafe_allow_html=True)
 
# ── Data layer ────────────────────────────────────────────────────────────────
@st.cache_data(ttl=300)
def q(view: str) -> pd.DataFrame:
    with duckdb.connect(str(WAREHOUSE_DB), read_only=True) as conn:
        return conn.execute(f"SELECT * FROM dw.{view}").df()
 
@st.cache_data(ttl=300)
def sql(query: str) -> pd.DataFrame:
    with duckdb.connect(str(WAREHOUSE_DB), read_only=True) as conn:
        return conn.execute(query).df()
 
# ── Plotly theme helper ───────────────────────────────────────────────────────
def theme(fig, height=340):
    fig.update_layout(
        height=height,
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        font=dict(family="Syne, sans-serif", color=TEXT2, size=12),
        margin=dict(l=0, r=0, t=28, b=0),
        legend=dict(font=dict(size=11), bgcolor="rgba(0,0,0,0)", bordercolor=BORDER),
        xaxis=dict(gridcolor=BORDER, zeroline=False, tickfont=dict(size=11)),
        yaxis=dict(gridcolor=BORDER, zeroline=False, tickfont=dict(size=11)),
    )
    return fig
 
PALETTE = [ACCENT, GREEN, PURPLE, AMBER, RED, "#79C0FF", "#56D364", "#D2A8FF"]
import calendar
# ── KPI computation ───────────────────────────────────────────────────────────
def header_kpis():
    dist   = q("v_product_distribution_detailed")
    low_stock = int(len(dist[dist["StockBuffer"] < 0]))
 
    exp = sql("""
        SELECT COALESCE(SUM(AtRiskStock30),0) AS r
        FROM dw.v_location_expiration_summary
    """)
    risk_count = int(exp["r"].iloc[0])

    ev = q("v_kpi_monthly_events")
    if not ev.empty:
        row = ev.iloc[0]          # ordered DESC so first row = most recent
        curr_events = int(row["EventCount"])
        month_name = calendar.month_name[int(row["Month"])]
        curr_month_label = f"{month_name} {int(row['Year'])}"
        prev = row["PreviousMonthCount"]
        mom = 0.0 if pd.isna(prev) or prev == 0 else ((curr_events - prev) / prev) * 100

        
    else:
        curr_events, curr_month_label, mom = 0, "N/A", 0.0
 
    po = q("v_po_summary")
    open_pos  = int(po["OpenOrdersCount"].iloc[0])
    avg_lead  = float(po["AvgLeadTime"].iloc[0] or 0)
 
    return low_stock, risk_count, curr_events, curr_month_label, mom, open_pos, avg_lead
 
low_stock, risk_count, curr_events, curr_month_label, mom, open_pos, avg_lead = header_kpis()
 
# ── Header ────────────────────────────────────────────────────────────────────
st.markdown("""
<div style="display:flex;align-items:baseline;gap:12px;margin-bottom:1.5rem">
  <span style="font-size:28px;font-weight:800;letter-spacing:-.5px">🔬 LabHub</span>
  <span style="font-size:13px;color:#8B949E;font-family:'DM Mono',monospace">Laboratory Intelligence & Compliance</span>
</div>
""", unsafe_allow_html=True)
 
# ── KPI tiles ─────────────────────────────────────────────────────────────────
ls_cls   = "bad"  if low_stock  > 20 else "warn" if low_stock  > 5  else "ok"
ri_cls   = "bad"  if risk_count > 50 else "warn" if risk_count > 10 else "ok"
mom_cls  = "ok"   if mom >= 0         else "warn"
po_cls   = "warn" if open_pos   > 30  else "ok"
 
c1, c2, c3, c4 = st.columns(4)
for col, label, value, sub, cls in [
    (c1, "Low Stock Items",     f"{low_stock}","Products below recommended stock",       ls_cls),
    (c2, "Expiration Risk",     f"{risk_count} units",       "Expiring within 30 days",               ri_cls),
    (c3, curr_month_label,      f"{curr_events}",            f"MoM {mom:+.1f}% vs prior month",       mom_cls),
    (c4, "Open Purchase Orders",f"{open_pos} POs",           f"Avg lead time {avg_lead:.1f} days",    po_cls),
]:
    col.markdown(f"""
    <div class="kpi-card {cls}">
      <div class="kpi-label">{label}</div>
      <div class="kpi-value">{value}</div>
      <div class="kpi-sub">{sub}</div>
    </div>""", unsafe_allow_html=True)
 
st.markdown("<div style='margin-bottom:1rem'></div>", unsafe_allow_html=True)
 
# ── Recent large adjustments (main page deep-dive) ───────────────────────────
st.markdown('<div class="section-head">Recent Large Adjustments — Non-Standard Movements</div>', unsafe_allow_html=True)
adj_df = sql("""
    SELECT
        d.FullDate          AS Date,
        p.ProductName       AS Product,
        l.SiteName || ' › ' || l.Building || ' · ' || l.RoomNumber AS Location,
        u.UserName          AS User,
        e.StockEventReason  AS Reason,
        f.QuantityDelta     AS Delta,
        f.CurrentStockSnapshot AS StockAfter
    FROM dw.Fact_Inventory_Transactions f
    JOIN dw.Dim_Date         d ON f.EventDateKey  = d.DateKey
    JOIN dw.Dim_Product      p ON f.ProductKey    = p.ProductKey
    JOIN dw.Dim_Location     l ON f.LocationKey   = l.LocationKey
    JOIN dw.Dim_User         u ON f.UserKey       = u.UserKey
    JOIN dw.Dim_Stock_Event  e ON f.StockEventKey = e.StockEventKey
    WHERE e.StockEventReason NOT IN ('Use','Replenishment','Receive','Order')
    ORDER BY ABS(f.QuantityDelta) DESC
    LIMIT 10
""")
 
if not adj_df.empty:
    def color_delta(val):
        try:
            return f"color: {RED}" if float(val) < 0 else f"color: {GREEN}"
        except:
            return ""
    styled = adj_df.style.map(color_delta, subset=["Delta"])
    st.dataframe(styled, use_container_width=True, hide_index=True)
else:
    st.info("No non-standard movements found.")
 
st.divider()
 
# ── TABS ──────────────────────────────────────────────────────────────────────
tab1, tab2, tab3, tab4, tab5 = st.tabs([
    "📦  Product Visibility",
    "⛓️  Lineage & Traceability",
    "👤  User Accountability",
    "🧠  Semantic Search",
    "⚙️  System Health",
])
 
# ═══════════════════════════════════════════════════════════════
# TAB 1 — Product Visibility & Expiration Risk
# ═══════════════════════════════════════════════════════════════
with tab1:
    # ── Expiration stacked bar ─────────────────────────────────
    st.markdown('<div class="section-head">Expiration Exposure by Category</div>', unsafe_allow_html=True)
    exp_df = q("v_expiration_exposure")
    if not exp_df.empty:
        melt = exp_df.melt(
            id_vars=["ProductName", "CategoryName"],
            value_vars=["Expiring_30d", "Expiring_60d", "Expiring_90d"],
            var_name="Window", value_name="Units"
        )
        cat_agg = melt.groupby(["CategoryName", "Window"])["Units"].sum().reset_index()
        cat_agg["Window"] = cat_agg["Window"].map({
            "Expiring_30d": "≤ 30 days",
            "Expiring_60d": "≤ 60 days",
            "Expiring_90d": "≤ 90 days",
        })
        fig_exp = px.bar(
            cat_agg, x="CategoryName", y="Units", color="Window",
            barmode="group",
            color_discrete_map={"≤ 30 days": RED, "≤ 60 days": AMBER, "≤ 90 days": ACCENT},
            labels={"CategoryName": "Category", "Units": "Units at Risk"},
        )
        theme(fig_exp)
        st.plotly_chart(fig_exp, use_container_width=True, config={"displayModeBar": False})
    else:
        st.info("No expiration data available.")
 
    col_a, col_b = st.columns([1, 1])
 
    # ── Location Risk Ranking ──────────────────────────────────
    with col_a:
        st.markdown('<div class="section-head">Location Risk Ranking</div>', unsafe_allow_html=True)
        loc_sum = q("v_location_expiration_summary")
        if not loc_sum.empty:
            loc_sum["Location"] = loc_sum["SiteName"] + " › " + loc_sum["Building"] + " · " + loc_sum["RoomNumber"]
            loc_sorted = loc_sum.sort_values("AtRiskStock30", ascending=False).head(15)
            fig_loc = px.bar(
                loc_sorted, x="AtRiskStock30", y="Location",
                orientation="h", color="AtRiskStock30",
                color_continuous_scale=[[0, ACCENT], [0.5, AMBER], [1, RED]],
                labels={"AtRiskStock30": "Units expiring ≤30d", "Location": ""},
            )
            fig_loc.update_layout(coloraxis_showscale=False, yaxis=dict(autorange="reversed"))
            theme(fig_loc, height=380)
            st.plotly_chart(fig_loc, use_container_width=True, config={"displayModeBar": False})
        else:
            st.info("No location risk data.")
 
    # ── Shelf-Life Efficiency gauge ────────────────────────────
    with col_b:
        st.markdown('<div class="section-head">Shelf-Life Efficiency — Time to First Use</div>', unsafe_allow_html=True)
        sle = q("v_shelf_life_efficiency")
        if not sle.empty:
            avg_index = float(sle["ShelfLifeConsumptionIndex"].dropna().mean())
            pct = round(avg_index * 100, 1)
            gauge_color = RED if pct > 70 else AMBER if pct > 40 else GREEN
            fig_g = go.Figure(go.Indicator(
                mode="gauge+number+delta",
                value=pct,
                delta={"reference": 40, "valueformat": ".1f", "suffix": "%"},
                number={"suffix": "%", "font": {"size": 40, "color": TEXT1}},
                gauge={
                    "axis": {"range": [0, 100], "tickcolor": TEXT2, "tickfont": {"color": TEXT2}},
                    "bar": {"color": gauge_color},
                    "bgcolor": CARD,
                    "bordercolor": BORDER,
                    "steps": [
                        {"range": [0, 40],  "color": "rgba(63,185,80,.12)"},
                        {"range": [40, 70], "color": "rgba(210,153,34,.12)"},
                        {"range": [70, 100],"color": "rgba(248,81,73,.12)"},
                    ],
                    "threshold": {"line": {"color": AMBER, "width": 2}, "thickness": 0.75, "value": 40},
                },
                title={"text": "Avg shelf life consumed before first use", "font": {"color": TEXT2, "size": 12}},
            ))
            fig_g.update_layout(paper_bgcolor="rgba(0,0,0,0)", height=300, margin=dict(l=20, r=20, t=20, b=10))
            st.plotly_chart(fig_g, use_container_width=True, config={"displayModeBar": False})
            st.caption("Lower = product used early in its shelf life (good). Above 70% = procurement too early or overstocking.")
        else:
            st.info("No shelf-life data available.")
 
    st.divider()
 
    # ── Hotspots ───────────────────────────────────────────────
    st.markdown('<div class="section-head">Lab-Level Usage Hotspots</div>', unsafe_allow_html=True)
    hotspot = q("v_location_hotspots")
    if not hotspot.empty:
        st.dataframe(hotspot, use_container_width=True, hide_index=True, column_config={
            "LocationPath": "Campus › Building",
            "RoomNumber": "Room",
            "TotalUsage": st.column_config.NumberColumn("Total Usage", format="%d"),
            "CurrentLocalStock": st.column_config.NumberColumn("Current Stock", format="%d"),
            "LastUpdatedKey": st.column_config.DateColumn("Last Updated"),
            "PercentOfCampusUsage": st.column_config.ProgressColumn("% Campus", format="%.1f%%", min_value=0, max_value=100),
            "PercentOfLabUsage":    st.column_config.ProgressColumn("% Lab",    format="%.1f%%", min_value=0, max_value=100),
        })
 
    st.divider()
 
    # ── Global product performance ─────────────────────────────
    st.markdown('<div class="section-head">Global Product Demand</div>', unsafe_allow_html=True)
    perf = q("v_product_performance_global")
    if not perf.empty:
        cols = [c for c in perf.columns if c != "Description"]
        perf = perf[cols + ["Description"]]
        s1 = st.text_input("🔍 Search products", placeholder="e.g. DNA, Ethanol, Gloves…", key="search_perf")
        if s1:
            mask = perf.astype(str).apply(lambda x: x.str.contains(s1, case=False)).any(axis=1)
            perf = perf[mask]
        max_stock = max(int(perf["GlobalStockBalance"].max()), 1)
        st.dataframe(perf, use_container_width=True, hide_index=True, column_config={
            "ProductID": None,
            "GlobalStockBalance": st.column_config.ProgressColumn("Global Stock", format="%d", min_value=0, max_value=max_stock),
            "Usage30d":  st.column_config.NumberColumn("1M Usage",  format="%d"),
            "Usage6m":   st.column_config.NumberColumn("6M Usage",  format="%d"),
            "Usage12m":  st.column_config.NumberColumn("12M Usage", format="%d"),
        })
 
    st.divider()
 
    # ── Local distribution ─────────────────────────────────────
    st.markdown('<div class="section-head">Local Product Distribution & Stock Buffers</div>', unsafe_allow_html=True)
    dist = q("v_product_distribution_detailed")
    for col in ["CurrentStock", "LocalUsage1Y", "Threshold", "StockBuffer"]:
        if col in dist.columns:
            dist[col] = dist[col].fillna(0).astype(int)
 
    s2 = st.text_input("🔍 Search products or locations", placeholder="e.g. Building B, PBS…", key="search_dist")
    if s2:
        mask = dist.astype(str).apply(lambda x: x.str.contains(s2, case=False)).any(axis=1)
        dist = dist[mask]
 
    def color_neg(val):
        try: return f"color: {RED}" if float(val) < 0 else f"color: {GREEN}"
        except: return ""
 
    styled_dist = dist.style.map(color_neg, subset=["CurrentStock", "StockBuffer"])
    st.dataframe(styled_dist, use_container_width=True, hide_index=True, column_config={
        "LocalUsage1Y": "12M Local Usage",
        "Threshold":    "Min Stock Threshold",
        "StockBuffer": st.column_config.ProgressColumn(
            "Buffer vs Threshold", format="%d",
            min_value=int(dist["StockBuffer"].min()) if not dist.empty else 0,
            max_value=max(int(dist["StockBuffer"].max()), 1) if not dist.empty else 1,
        ),
    })
 
    st.divider()
 
    # ── 12-month usage trend ───────────────────────────────────
    st.markdown('<div class="section-head">12-Month Usage Trends by Category</div>', unsafe_allow_html=True)
    usage = q("v_monthly_usage")
    if not usage.empty:
        month_order = ["January","February","March","April","May","June",
                       "July","August","September","October","November","December"]
        usage["MonthName"] = pd.Categorical(usage["MonthName"], categories=month_order, ordered=True)
        plot_u = usage.sort_values(["Year","Month"]).tail(48)
        fig_u = px.bar(
            plot_u, x="MonthName", y="TotalQuantityConsumed", color="CategoryName",
            barmode="group", color_discrete_sequence=PALETTE,
            labels={"MonthName": "Month", "TotalQuantityConsumed": "Quantity", "CategoryName": "Category"},
        )
        theme(fig_u, height=320)
        st.plotly_chart(fig_u, use_container_width=True, config={"displayModeBar": False})
 
 
# ═══════════════════════════════════════════════════════════════
# TAB 2 — Lineage, Traceability & Loss Prevention
# ═══════════════════════════════════════════════════════════════
with tab2:
    st.markdown('<div class="section-head">Product Dwell Time by Location — Bottleneck Detection</div>', unsafe_allow_html=True)
    dwell = q("v_product_dwell_time")
    if not dwell.empty:
        # Box plot: dwell days per storage type / site
        fig_dwell = px.box(
            dwell, x="SiteName", y="DwellDays", color="SiteName",
            points="outliers", color_discrete_sequence=PALETTE,
            labels={"SiteName": "Site", "DwellDays": "Days in Storage"},
        )
        theme(fig_dwell)
        fig_dwell.update_layout(showlegend=False)
        st.plotly_chart(fig_dwell, use_container_width=True, config={"displayModeBar": False})
 
        st.caption("Outlier points (dots above the whiskers) are lots sitting significantly longer than peers — investigate for forgotten stock or incorrect location assignment.")
 
        s_dwell = st.text_input("🔍 Filter dwell table", placeholder="e.g. Cold Storage, Lot-001…", key="search_dwell")
        df_show = dwell.copy()
        if s_dwell:
            mask = df_show.astype(str).apply(lambda x: x.str.contains(s_dwell, case=False)).any(axis=1)
            df_show = df_show[mask]
        st.dataframe(df_show, use_container_width=True, hide_index=True, column_config={
            "DwellDays": st.column_config.NumberColumn("Days Stored", format="%d 📦"),
            "FirstReceivedDate": st.column_config.DateColumn("Received"),
        })
    else:
        st.info("No dwell time data available.")
 
    st.divider()
 
    # ── Leakage / ghost consumption ────────────────────────────
    st.markdown('<div class="section-head">Inventory Leakage — Ghost Consumption & Unexplained Loss</div>', unsafe_allow_html=True)
    leak = q("v_inventory_leakage_report")
    if not leak.empty:
        col_la, col_lb = st.columns([1, 1])
        with col_la:
            top_leak = leak.sort_values("LeakageUsageRatio", ascending=False).head(12)
            fig_leak = px.bar(
                top_leak, x="LeakageUsageRatio", y="ProductName",
                orientation="h", color="LeakageUsageRatio",
                color_continuous_scale=[[0, ACCENT], [0.5, AMBER], [1, RED]],
                labels={"LeakageUsageRatio": "Leakage Ratio", "ProductName": ""},
            )
            fig_leak.update_layout(coloraxis_showscale=False, yaxis=dict(autorange="reversed"))
            theme(fig_leak, height=380)
            st.plotly_chart(fig_leak, use_container_width=True, config={"displayModeBar": False})
        with col_lb:
            s_leak = st.text_input("🔍 Filter leakage table", placeholder="e.g. Ethanol, Building A…", key="search_leak")
            df_leak = leak.copy()
            if s_leak:
                mask = df_leak.astype(str).apply(lambda x: x.str.contains(s_leak, case=False)).any(axis=1)
                df_leak = df_leak[mask]
            st.dataframe(df_leak, use_container_width=True, hide_index=True, column_config={
                "RecordedConsumption": st.column_config.NumberColumn("Recorded Use", format="%d"),
                "LeakageQuantity":     st.column_config.NumberColumn("Unexplained Loss", format="%d"),
                "LeakageUsageRatio":   st.column_config.NumberColumn("Leakage Ratio", format="%.4f"),
            })
    else:
        st.info("No leakage patterns detected.")
 
    st.divider()
 
    # ── Vendor freshness ───────────────────────────────────────
    st.markdown('<div class="section-head">Vendor Freshness Scorecard — Remaining Shelf Life at Delivery</div>', unsafe_allow_html=True)
    try:
        fresh = q("v_vendor_freshness_audit")
        if not fresh.empty:
            s_vend = st.text_input("🔍 Filter vendors", placeholder="e.g. VendorName, Product…", key="search_vend")
            df_fresh = fresh.copy()
            if s_vend:
                mask = df_fresh.astype(str).apply(lambda x: x.str.contains(s_vend, case=False)).any(axis=1)
                df_fresh = df_fresh[mask]
 
            def color_freshness(val):
                try:
                    v = float(val)
                    return f"color: {RED}" if v < 30 else f"color: {AMBER}" if v < 90 else f"color: {GREEN}"
                except: return ""
 
            styled_fresh = df_fresh.style.map(color_freshness, subset=["AvgShelfLifeDaysAtDelivery"])
            st.dataframe(styled_fresh, use_container_width=True, hide_index=True, column_config={
                "AvgShelfLifeDaysAtDelivery": st.column_config.NumberColumn("Avg Days Remaining at Delivery", format="%d days"),
                "TotalDeliveriesAudited":     st.column_config.NumberColumn("Deliveries Audited", format="%d"),
            })
            st.caption("Red = vendor consistently delivering near-expiry stock. Use this as leverage for renegotiation or vendor review.")
        else:
            st.info("Vendor freshness data requires linked PurchaseOrderID in inventory transactions.")
    except Exception:
        st.info("Vendor freshness view not yet available — requires PurchaseOrderID linkage in Fact_Inventory_Transactions.")
 
    st.divider()
 
    # ── Movement audit trail ───────────────────────────────────
    st.markdown('<div class="section-head">Movement Audit Trail</div>', unsafe_allow_html=True)
    s_mov = st.text_input("🔍 Filter movements", placeholder="e.g. product name, user, reason…", key="search_mov")
    mov = sql("SELECT * FROM dw.v_movement_log LIMIT 500")
    if s_mov:
        mask = mov.astype(str).apply(lambda x: x.str.contains(s_mov, case=False)).any(axis=1)
        mov = mov[mask]
 
    def color_qty(val):
        try: return f"color: {RED}" if float(val) < 0 else f"color: {GREEN}"
        except: return ""
 
    styled_mov = mov.style.map(color_qty, subset=["QuantityDelta"])
    st.dataframe(styled_mov, use_container_width=True, hide_index=True, column_config={
        "FullDate":              st.column_config.DateColumn("Date"),
        "QuantityDelta":         st.column_config.NumberColumn("Δ Quantity", format="%+.0f"),
        "NewQuantity":           st.column_config.NumberColumn("Qty After", format="%d"),
        "CurrentStockSnapshot":  st.column_config.NumberColumn("Snapshot", format="%d"),
    })
 
 
# ═══════════════════════════════════════════════════════════════
# TAB 3 — User Accountability & Behavioral Patterns
# ═══════════════════════════════════════════════════════════════
with tab3:
    col3a, col3b = st.columns([1, 1])
 
    # ── Consumption leaderboard ────────────────────────────────
    with col3a:
        st.markdown('<div class="section-head">Consumption Leaderboard by Category</div>', unsafe_allow_html=True)
        leaders = q("v_top_consumers_by_category")
        if not leaders.empty:
            top = leaders[leaders["ConsumptionRank"] <= 5].sort_values("TotalConsumedQuantity", ascending=True)
            fig_lead = px.bar(
                top, x="TotalConsumedQuantity", y="UserName",
                orientation="h", color="CategoryName",
                color_discrete_sequence=PALETTE,
                labels={"TotalConsumedQuantity": "Units Consumed", "UserName": "", "CategoryName": "Category"},
                facet_col=None,
            )
            theme(fig_lead, height=400)
            st.plotly_chart(fig_lead, use_container_width=True, config={"displayModeBar": False})
            st.caption("Top 5 consumers per category. Use for grant-fund allocation and training planning.")
 
            s_lead = st.text_input("🔍 Filter leaderboard", placeholder="e.g. user name, department…", key="search_lead")
            df_lead = leaders.copy()
            if s_lead:
                mask = df_lead.astype(str).apply(lambda x: x.str.contains(s_lead, case=False)).any(axis=1)
                df_lead = df_lead[mask]
            st.dataframe(df_lead, use_container_width=True, hide_index=True, column_config={
                "TotalConsumedQuantity": st.column_config.NumberColumn("Units Consumed", format="%d"),
                "ConsumptionRank": st.column_config.NumberColumn("Rank", format="#%d"),
            })
 
    # ── After-hours audit ──────────────────────────────────────
    with col3b:
        st.markdown('<div class="section-head">After-Hours Movement Audit</div>', unsafe_allow_html=True)
        ah = q("v_after_hours_movement_audit")
        if not ah.empty:
            ah["FullDate"] = pd.to_datetime(ah["FullDate"])
            daily_ah = ah.groupby(ah["FullDate"].dt.date).size().reset_index(name="Movements")
            daily_ah.columns = ["Date", "Movements"]
            fig_ah = px.area(
                daily_ah, x="Date", y="Movements",
                color_discrete_sequence=[RED],
                labels={"Date": "", "Movements": "After-Hours Events"},
            )
            fig_ah.update_traces(fill="tozeroy", fillcolor=f"rgba(248,81,73,.15)", line=dict(color=RED, width=1.5))
            theme(fig_ah, height=280)
            st.plotly_chart(fig_ah, use_container_width=True, config={"displayModeBar": False})
            st.caption("Spikes indicate unusual activity. Cross-reference with user table below for investigation.")
 
            s_ah = st.text_input("🔍 Filter after-hours events", placeholder="e.g. user, building…", key="search_ah")
            df_ah = ah.copy()
            if s_ah:
                mask = df_ah.astype(str).apply(lambda x: x.str.contains(s_ah, case=False)).any(axis=1)
                df_ah = df_ah[mask]
            st.dataframe(df_ah, use_container_width=True, hide_index=True, column_config={
                "FullDate": st.column_config.DateColumn("Date"),
                "QuantityDelta": st.column_config.NumberColumn("Δ Quantity", format="%+.0f"),
            })
        else:
            st.info("No after-hours movements recorded.")
 
    st.divider()
 
    # ── Process compliance / training matrix ────────────────────
    st.markdown('<div class="section-head">Process Compliance Matrix — Training Requirement Tracker</div>', unsafe_allow_html=True)
    gaps = q("v_process_integrity_gaps")
    if not gaps.empty:
        s_gaps = st.text_input("🔍 Filter compliance table", placeholder="e.g. department, user…", key="search_gaps")
        df_gaps = gaps.copy()
        if s_gaps:
            mask = df_gaps.astype(str).apply(lambda x: x.str.contains(s_gaps, case=False)).any(axis=1)
            df_gaps = df_gaps[mask]
 
        def color_issues(val):
            try: return f"color: {RED}" if int(val) > 5 else f"color: {AMBER}" if int(val) > 0 else ""
            except: return ""
 
        styled_gaps = df_gaps.style\
            .map(color_issues, subset=["NegativeBalanceEvents"])\
            .map(color_issues, subset=["LogicIntegrityErrors"])
 
        st.dataframe(styled_gaps, use_container_width=True, hide_index=True, column_config={
            "TotalSuspectTransactions": st.column_config.NumberColumn("Suspect Transactions", format="%d"),
            "NegativeBalanceEvents":    st.column_config.NumberColumn("Negative Balance Events", format="%d"),
            "LogicIntegrityErrors":     st.column_config.NumberColumn("Logic Errors", format="%d"),
        })
        st.caption("Users here have transactions with impossible stock states. Flag for retraining on data-entry procedures, not disciplinary action.")
    else:
        st.success("✅ No process integrity gaps detected.")
 
    st.divider()
 
    # ── Waste/usage ratio ──────────────────────────────────────
    st.markdown('<div class="section-head">Waste-to-Usage Ratio by Category</div>', unsafe_allow_html=True)
    waste = q("v_waste_usage_ratio")
    if not waste.empty:
        fig_waste = px.bar(
            waste.sort_values("WasteUsageRatio", ascending=False),
            x="CategoryName", y="WasteUsageRatio",
            color="WasteUsageRatio",
            color_continuous_scale=[[0, GREEN], [0.5, AMBER], [1, RED]],
            labels={"CategoryName": "Category", "WasteUsageRatio": "Waste / Consumption Ratio"},
        )
        fig_waste.update_layout(coloraxis_showscale=False)
        theme(fig_waste, height=280)
        st.plotly_chart(fig_waste, use_container_width=True, config={"displayModeBar": False})
 
 
# ═══════════════════════════════════════════════════════════════
# TAB 4 — Semantic Search (placeholder)
# ═══════════════════════════════════════════════════════════════
with tab4:
    st.markdown('<div class="section-head">Semantic Search Engine</div>', unsafe_allow_html=True)
    query_text = st.text_input(
        "Search across materials, chemical properties, storage protocols, or locations…",
        placeholder="e.g. 'Flammable acids in Building B' or 'cold chain reagents expiring soon'",
        key="semantic_search",
    )
    if query_text:
        st.info(f"Semantic search for: **{query_text}** — connect an embedding model to enable this feature.")
    else:
        st.markdown(f"""
        <div style="background:{CARD};border:1px solid {BORDER};border-radius:10px;padding:24px;text-align:center;color:{TEXT2};margin-top:1rem">
          <div style="font-size:32px;margin-bottom:8px">🧠</div>
          <div style="font-size:14px;font-weight:600;color:{TEXT1};margin-bottom:6px">Natural Language Inventory Search</div>
          <div style="font-size:12px;font-family:'DM Mono',monospace">
            Connect a vector embedding model to search by chemical properties,<br>
            safety classifications, storage requirements, or plain English queries.
          </div>
        </div>
        """, unsafe_allow_html=True)
 
 
# ═══════════════════════════════════════════════════════════════
# TAB 5 — System Health & Data Quality
# ═══════════════════════════════════════════════════════════════
with tab5:
    st.markdown('<div class="section-head">Warehouse Row Counts</div>', unsafe_allow_html=True)
 
    tables = [
        ("dw.Dim_Product", "Dim_Product"),
        ("dw.Dim_User", "Dim_User"),
        ("dw.Dim_Location", "Dim_Location"),
        ("dw.Dim_Date", "Dim_Date"),
        ("dw.Dim_Status", "Dim_Status"),
        ("dw.Dim_Stock_Event", "Dim_Stock_Event"),
        ("dw.Dim_Storage_Conditions", "Dim_Storage_Conditions"),
        ("dw.Dim_Vendor", "Dim_Vendor"),
        ("dw.Fact_Inventory_Transactions", "Fact_Inventory"),
        ("dw.Fact_Purchase_Orders", "Fact_Purchase_Orders"),
    ]
    health_rows = []
    with duckdb.connect(str(WAREHOUSE_DB), read_only=True) as conn:
        for tbl, label in tables:
            try:
                cnt = conn.execute(f"SELECT COUNT(*) FROM {tbl}").fetchone()[0]
                health_rows.append({"Table": label, "Rows": cnt, "Status": "✅"})
            except Exception as e:
                health_rows.append({"Table": label, "Rows": 0, "Status": f"❌ {e}"})
 
    health_df = pd.DataFrame(health_rows)
    st.dataframe(health_df, use_container_width=True, hide_index=True, column_config={
        "Rows": st.column_config.NumberColumn("Row Count", format="%d"),
    })
 
    st.divider()
 
    st.markdown('<div class="section-head">SCD2 Version Audit — Duplicate Current Rows</div>', unsafe_allow_html=True)
    scd_checks = {
        "Dim_Product": "SELECT ProductID, COUNT(*) AS versions FROM dw.Dim_Product WHERE IsCurrent=1 GROUP BY ProductID HAVING COUNT(*)>1",
        "Dim_User":    "SELECT UserID,    COUNT(*) AS versions FROM dw.Dim_User    WHERE IsCurrent=1 GROUP BY UserID    HAVING COUNT(*)>1",
        "Dim_Location":"SELECT LocationID,COUNT(*) AS versions FROM dw.Dim_Location WHERE IsCurrent=1 GROUP BY LocationID HAVING COUNT(*)>1",
        "Dim_Status":  "SELECT StatusID,  COUNT(*) AS versions FROM dw.Dim_Status  WHERE IsCurrent=1 GROUP BY StatusID  HAVING COUNT(*)>1",
    }
    any_issues = False
    with duckdb.connect(str(WAREHOUSE_DB), read_only=True) as conn:
        for dim, chk_sql in scd_checks.items():
            try:
                dupes = conn.execute(chk_sql).df()
                if dupes.empty:
                    st.success(f"✅ {dim} — no duplicate current rows")
                else:
                    st.error(f"❌ {dim} — {len(dupes)} ProductIDs with multiple IsCurrent=1 rows")
                    st.dataframe(dupes, use_container_width=True, hide_index=True)
                    any_issues = True
            except Exception as e:
                st.warning(f"⚠️ Could not check {dim}: {e}")
 
    if not any_issues:
        st.success("All SCD2 dimensions are clean.")