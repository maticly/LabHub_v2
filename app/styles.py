import streamlit as st

def apply_custom_style():
        # ── Theme ─────────────────────────────────────────────────────────────────────
    DARK   = "#0E1117"
    CARD   = "#21262DB7"
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