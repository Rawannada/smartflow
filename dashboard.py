import streamlit as st
import pandas as pd
from sqlalchemy import create_engine
import plotly.express as px

st.set_page_config(page_title="SmartFlow", layout="wide", page_icon="🚚")

st.markdown("""
    <style>
    .block-container {
        padding-top: 0.5rem !important;
        padding-bottom: 0rem !important;
        padding-left: 1rem !important;
        padding-right: 1rem !important;
    }
    h1 {
        font-size: 1.5rem !important;
        margin-bottom: 0.5rem !important;
        padding-top: 0px !important;
    }
    .stDeployButton {display:none;}
    [data-testid="stMetricValue"] { font-size: 1.2rem !important; }
    [data-testid="stMetricLabel"] { font-size: 0.8rem !important; }
    iframe { height: 200px !important; }
    </style>
    """, unsafe_content_path=True)

try:
    engine = create_engine('postgresql://admin:password123@localhost:5432/logistics_db')
    df = pd.read_sql("SELECT * FROM public_analytics.courier_performance", engine)
    
    cols = st.columns([3, 1])
    cols[0].title("🚚 SmartFlow Logistics")
    if not df.empty:
        cols[1].success("Connected", icon="✅")

    m1, m2, m3, m4 = st.columns(4)
    m1.metric("Total", len(df))
    m2.metric("Avg Speed", f"{df['avg_speed'].mean():.1f}")
    m3.metric("High Risk", len(df[df['risk_level'].str.contains('Risk')]))
    m4.metric("Top Perf.", len(df[df['performance_status'] == 'Top Performer']))

    c1, c2 = st.columns([1, 1])

    with c1:
        fig = px.pie(df, names='risk_level', hole=0.7, height=180,
                     color='risk_level',
                     color_discrete_map={'Safe': '#2ecc71', 'Low Risk': '#a2d149', 'Moderate': '#f1c40f', 'High Risk': '#e67e22', 'Extreme Risk': '#e74c3c'})
        fig.update_layout(margin=dict(t=10, b=10, l=0, r=0), showlegend=False)
        st.plotly_chart(fig, use_container_width=True, config={'displayModeBar': False})

    with c2:
        st.bar_chart(df.set_index('courier_id')['speed_deviation'], height=180)

    st.dataframe(
        df.style.applymap(lambda x: f"color: {'#ff4b4b' if 'Extreme' in str(x) else '#2ecc71' if 'Safe' in str(x) else 'none'}; font-weight: bold", subset=['risk_level']), 
        use_container_width=True, 
        height=200
    )

except Exception as e:
    st.error(f"Waiting for Data: {e}")