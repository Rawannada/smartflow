import streamlit as st
import pandas as pd
from sqlalchemy import create_engine

# إعداد الصفحة
st.set_page_config(page_title="Logistics Analytics", layout="wide")

# الربط بالداتابيز (نفس بيانات Postgres بتاعتك)
engine = create_engine('postgresql://admin:admin@localhost:5432/logistics_db')

st.title("📊 Logistics Performance Analytics (dbt Powered)")

# سحب البيانات اللي dbt عملها
df = pd.read_sql("SELECT * FROM public_analytics.courier_performance", engine)

# عرض المؤشرات (Metrics)
col1, col2, col3 = st.columns(3)
col1.metric("Total Couriers", len(df))
col2.metric("Total Violations", df['violation_count'].sum())
col3.metric("Avg Fleet Speed", f"{df['avg_speed'].mean():.2f} km/h")

# رسم بياني تفاعلي باستخدام Plotly
import plotly.express as px
fig = px.bar(df, x='courier_id', y='avg_speed', color='violation_count', 
             title="Courier Speed vs Violations")
st.plotly_chart(fig, use_container_width=True)

# عرض الجدول النهائي
st.subheader("Detailed Performance Ledger")
st.dataframe(df)