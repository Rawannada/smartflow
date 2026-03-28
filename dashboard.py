import streamlit as st
import pandas as pd
from sqlalchemy import create_engine
import plotly.express as px

# 1. Page Config
st.set_page_config(page_title="SmartFlow Logistics", layout="wide", page_icon="🚚")

# Clean Header
st.title("🚚 SmartFlow: Logistics Performance")

# 2. Database Connection
try:
    engine = create_engine('postgresql://admin:password123@postgres:5432/logistics_db')
    df = pd.read_sql("SELECT * FROM public_analytics.courier_performance", engine)
    
    st.sidebar.success("Database Connected")

    # 3. Key Metrics (Compact Row)
    m1, m2, m3, m4 = st.columns(4)
    m1.metric("Total Couriers", len(df))
    m2.metric("Avg Speed", f"{df['avg_speed'].mean():.1f} km/h")
    m3.metric("High Risk", len(df[df['risk_level'].str.contains('Risk')]), delta="Danger", delta_color="inverse")
    m4.metric("Top Performers", len(df[df['performance_status'] == 'Top Performer']))

    st.divider()

    # 4. Charts Row (Reduced Height)
    c1, c2 = st.columns([1, 1])

    with c1:
        st.caption("🎯 Risk Distribution")
        fig = px.pie(df, names='risk_level', hole=0.5, height=300,
                     color='risk_level',
                     color_discrete_map={'Safe': '#2ecc71', 'Moderate': '#f1c40f', 'High Risk': '#e67e22', 'Extreme Risk': '#e74c3c'})
        fig.update_layout(margin=dict(t=0, b=0, l=0, r=0))
        st.plotly_chart(fig, use_container_width=True)

    with c2:
        st.caption("📊 Speed Deviation")
        st.bar_chart(df.set_index('courier_id')['speed_deviation'], height=300)

    # 5. Analysis Table
    st.caption("📋 Analysis Detail")
    
    def style_risk(val):
        color = '#ff4b4b' if 'Extreme' in val else '#ffa500' if 'High' in val else '#2ecc71' if 'Safe' in val else 'none'
        return f'color: {color}; font-weight: bold'

    st.dataframe(df.style.applymap(style_risk, subset=['risk_level']), use_container_width=True)

except Exception as e:
    st.sidebar.error("Connection Failed")
    st.error(f"Error: {e}")