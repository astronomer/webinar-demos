import streamlit as st
import pandas as pd
from datetime import datetime
from snowflake.snowpark.context import get_active_session

session = get_active_session()

enriched_sales_df = session.table("ETL_DEMO.DEV.ENRICHED_SALES").to_pandas()
revenue_by_tea_type_df = session.table("ETL_DEMO.DEV.REVENUE_BY_TEA_TYPE").to_pandas()
sales_funnel_analysis_df = session.table("ETL_DEMO.DEV.SALES_FUNNEL_ANALYSIS").to_pandas()
top_users_by_spending_df = session.table("ETL_DEMO.DEV.TOP_USERS_BY_SPENDING").to_pandas()

enriched_sales_df['SALE_DATE'] = pd.to_datetime(enriched_sales_df['SALE_DATE'])

today = datetime.now().date() 
start_of_month = today.replace(day=1)  
start_of_year = today.replace(month=1, day=1)  

sales_today = enriched_sales_df[enriched_sales_df['SALE_DATE'].dt.date == today]['TOTAL_REVENUE'].sum()
sales_this_month = enriched_sales_df[enriched_sales_df['SALE_DATE'].dt.date >= start_of_month]['TOTAL_REVENUE'].sum()
sales_this_year = enriched_sales_df[enriched_sales_df['SALE_DATE'].dt.date >= start_of_year]['TOTAL_REVENUE'].sum()


latest_sales = enriched_sales_df.sort_values(by='SALE_DATE', ascending=False).head(5)[[
    'SALE_DATE', 'USER_NAME', 'TEA_NAME', 'QUANTITY', 'TOTAL_REVENUE'
]]

st.set_page_config(page_title="Tea Sales Dashboard", layout="wide")
st.title("🫖 Tea Sales Dashboard")

st.markdown("## Key Metrics")
col1, col2, col3 = st.columns(3)
with col1:
    st.metric(label="Sales So Far Today", value=f"${sales_today:,.2f}")
with col2:
    st.metric(label="Total Sales This Month", value=f"${sales_this_month:,.2f}")
with col3:
    st.metric(label="Total Sales This Year", value=f"${sales_this_year:,.2f}")

st.markdown("---")
st.markdown("## 🛒 5 Latest Sales")
st.table(latest_sales)

st.markdown("---")
st.markdown("## 📊 Revenue by Tea Type")
st.bar_chart(revenue_by_tea_type_df.set_index('TEA_TYPE')['TOTAL_REVENUE'])

st.markdown("---")
st.markdown("## 🔍 Sales Funnel Analysis")
col1, col2 = st.columns([2, 1])
with col1:
    st.bar_chart(sales_funnel_analysis_df.set_index('UTM_SOURCE')['TOTAL_REVENUE'])
with col2:
    st.dataframe(sales_funnel_analysis_df[['UTM_SOURCE', 'UTM_MEDIUM', 'UTM_CAMPAIGN', 'TOTAL_REVENUE']])

st.markdown("---")
st.markdown("## 🏆 Top Users by Spending")
st.dataframe(top_users_by_spending_df)

st.markdown("---")
st.markdown("## 🎯 Detailed Sales Funnel Analysis")
utm_campaign_filter = st.multiselect(
    'Filter by Campaign',
    sales_funnel_analysis_df['UTM_CAMPAIGN'].unique(),
    default=sales_funnel_analysis_df['UTM_CAMPAIGN'].unique()
)

filtered_sales_funnel = sales_funnel_analysis_df[sales_funnel_analysis_df['UTM_CAMPAIGN'].isin(utm_campaign_filter)]
st.bar_chart(filtered_sales_funnel.set_index('UTM_SOURCE')['TOTAL_REVENUE'])

st.markdown("---")
st.markdown("© 2024 Tea Makes Everything Better Inc.")
