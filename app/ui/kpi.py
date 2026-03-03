import streamlit as st

def kpi_card(title, value, subtitle="", bg_color="#DEE7F1"):
    st.markdown(f"""
    <div style="
        background:{bg_color};
        padding: 24px 22px 20px 22px;
        border-radius:18px;
        color:white;
        border:1px solid #c2c2c2;
        box-shadow:0 12px 20px rgba(0,0,0,0.10);
        transition: box-shadow 0.2s ease, transform 0.2s ease;
        min-height: 110px;
        display: flex;
        flex-direction: column;
        justify-content: space-between;)
    ">
    
    <div style="font-size:18px;font-weight:600">{title}</div>
    <div style="font-size:32px;font-weight:700">{value}</div>
    <div style="font-size:15px">{subtitle}</div>

        
    </div>
    """, unsafe_allow_html=True)
