import streamlit as st

def kpi_card(title, value, subtitle="", bg_color="#F2F2F7"):
    """
    Standard KPI Card for Warehouse Analytics.
    Default bg_color is Apple/Google light-gray.
    """
    st.markdown(f"""
    <div style="
        background-color: {bg_color};
        padding: 20px;
        border-radius: 12px;
        border: 1px solid #E5E5EA;
        color: #1D1D1F;
        min-height: 100px;
        display: flex;
        flex-direction: column;
        justify-content: center;
        margin-bottom: 1rem;
    ">
        <div style="font-size: 14px; font-weight: 500; color: #8E8E93; margin-bottom: 4px;">
            {title.upper()}
        </div>
        <div style="font-size: 28px; font-weight: 700; color: #1D1D1F;">
            {value}
        </div>
        {f'<div style="font-size: 13px; color: #48484A; margin-top: 4px;">{subtitle}</div>' if subtitle else ""}
    </div>
    """, unsafe_allow_html=True)