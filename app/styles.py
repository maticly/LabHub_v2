import streamlit as st

def apply_custom_style():
    st.markdown("""
    <style>
    /* ============= FORCE LIGHT THEME ============= */
    :root {
        color-scheme: light !important;
    }

    /* Streamlit App Background */
    html, body, .stApp {
        background-color: #FBFBFD !important;
        color: #1D1D1F !important;
        font-family: "Inter", "SF Pro Display", -apple-system, sans-serif;
    }

    /* Main Container Padding */
    [data-testid="stAppViewContainer"] {
        background-color: #FBFBFD !important;
        padding-top: 2rem;
    }

    /* ============= CLEAN TABLES (NOTION STYLE) ============= */
    /* Remove the dark background for a clean white/gray look */
    [data-testid="stDataFrame"] {
        background-color: #FFFFFF !important;
        border: 1px solid #E5E5EA !important;
        border-radius: 12px !important;
        padding: 10px;
    }

    /* ============= BUTTONS ============= */
    /* Secondary/View Details Button */
    .stButton > button {
        background-color: #FFFFFF !important;
        color: #1D1D1F !important;
        border: 1px solid #D1D1D6 !important;
        border-radius: 8px !important;
        padding: 0.5rem 1rem !important;
        font-weight: 500 !important;
        transition: all 0.2s ease;
    }

    .stButton > button:hover {
        background-color: #F2F2F7 !important;
        border-color: #AEAEB2 !important;
    }

    /* ============= INPUTS ============= */
    div[data-testid="stTextInput"] input {
        background-color: #FFFFFF !important;
        color: #1D1D1F !important;
        border-radius: 8px !important;
        border: 1px solid #D1D1D6 !important;
    }

    /* ============= TABS ============= */
    button[role="tab"] {
        font-size: 16px !important;
        color: #8E8E93 !important; /* Inactive tab gray */
    }

    button[role="tab"][aria-selected="true"] {
        color: #1D1D1F !important;
        border-bottom: 2px solid #1D1D1F !important;
    }

    /* ============= METRICS ============= */
    [data-testid="stMetricValue"] {
        color: #1D1D1F !important;
        font-size: 28px !important;
    }
    
    [data-testid="stMetricLabel"] {
        color: #8E8E93 !important;
        font-weight: 500 !important;
    }

    </style>
    """, unsafe_allow_html=True)