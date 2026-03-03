"""CSS styling"""
import streamlit as st
from pathlib import Path

def apply_custom_style():
    
    st.markdown("""
    <style>
/* =============FORCE LIGHT THEME */

:root {
    color-scheme: light !important;
}

/* Streamlit theme variables override */
html, body, .stApp {
    background-color: #FBFBFD !important;
    color: #1D1D1F !important;
}

/* kill dark auto theme */
@media (prefers-color-scheme: dark) {

    html, body, .stApp {
        background-color: #FBFBFD !important;
        color: #1D1D1F !important;
    }
}

/* main block container */
[data-testid="stAppViewContainer"] {
    background-color: #FBFBFD !important;
}


/* ================= KPI CARDS ================= */


.kpi-title {
    font-size:18px;
    color:#1D1D1F;
    font-weight:400;
    margin-bottom:6px;
}

.kpi-value {
    font-size:32px;
    font-weight:700;
    color:#1D1D1F;
}

.kpi-sub {
    font-size:15px;
    color:#8E8E93;
}
    


/* markdown + normal text inside tabs */
[data-testid="stTabs"] div {
    color: #4A4A4A !important;   /* dark gray */
}

                
/* ================= DARK TABLES ================= */

[data-testid="stDataFrame"] {
    background-color:#2C2C2E;
    border-radius:12px;
}

[data-testid="stDataFrame"] div[role="gridcell"],
[data-testid="stDataFrame"] div[role="columnheader"] {
    color:white !important;
}
                
/* ================= DIVIDERS ================= */

hr, [data-testid="stDivider"] {
    background-color: #D0D0D5 !important;
    height: 1px !important;
    border: none !important;
}

/* ================= DATAFRAME DARK STYLE ================= */

[data-testid="stDataFrame"] {
    background-color: #1C1C1E;
    border-radius: 12px;
}

[data-testid="stDataFrame"] div[role="gridcell"],
[data-testid="stDataFrame"] div[role="columnheader"] {
    color: white !important;
}

/* ================= EXPORT BUTTON ================= */

.stDownloadButton button {
    background-color: #3A3A3F !important;
    color: white !important;
}

/* ================= VIEW DETAILS BUTTON ================= */

.stButton > button {
    background-color: #E5E5EA !important;
    color: #1D1D1F !important;
    border: none !important;
    border-radius: 8px !important;
    font-weight: 600 !important;
}

.stButton > button:hover {
    background-color: #D1D1D6 !important;
    color: #000000 !important;
}
                                
/* ================= SEARCH INPUT TEXT ================= */

div[data-testid="stTextInput"] input {
    color: #e3e1e1 !important;
}

/* ================= METRIC TEXT ================= */

[data-testid="stMetricValue"] {
    color: #000000 !important;
    font-weight: 700 !important;
}

[data-testid="stMetricLabel"] {
    color: #1D1D1F !important;
}
                                
/* ================= TABS TEXT ================= */

button[role="tab"] {
    color: #1D1D1F !important;   /* black text */
    font-weight: 700 !important;
}

button[role="tab"][aria-selected="true"] {
    color: #000000 !important;
}

button[role="tab"]:hover {
    color: #000000 !important;
}
                                
/* ================= FULL WIDTH SEARCH ================= */

[data-testid="stTextInput"] {
    width: 100%;
}

    </style>
    """, unsafe_allow_html=True)
