import streamlit as st
import pandas as pd
import plotly.express as px
from sqlalchemy import create_engine

# =====================================================

# POSTGRES CONNECTION

# =====================================================

DB_URL = "postgresql://postgres:UnigapPostgres@123@postgres:5432/postgres"

engine = create_engine(DB_URL)

def run_query(query):
    return pd.read_sql(query, engine)

# =====================================================

# STREAMLIT CONFIG

# =====================================================

st.set_page_config(
page_title="User Behavior Dashboard",
layout="wide"
)

st.title("User Behavior Streaming Dashboard")

# =====================================================

# TOP 10 PRODUCTS

# =====================================================

st.header("Top 10 Products")

df_products = run_query("""

SELECT *
FROM vw_top_10_products;

""")

fig_products = px.bar(
df_products,
x="product_id",
y="total_views"
)

st.plotly_chart(
fig_products,
use_container_width=True
)

st.dataframe(df_products)

# =====================================================

# TOP 10 COUNTRIES

# =====================================================

st.header("Top 10 Countries")

df_countries = run_query("""

SELECT *
FROM vw_top_10_countries;

""")

fig_countries = px.bar(
df_countries,
x="country_name_long",
y="total_views"
)

st.plotly_chart(
fig_countries,
use_container_width=True
)

st.dataframe(df_countries)

# =====================================================

# TOP REFERRER URLS

# =====================================================

st.header("Top 5 Referrer URLs")

df_ref = run_query("""

SELECT *
FROM vw_top_5_referrer_urls;

""")

fig_ref = px.bar(
df_ref,
x="referrer_url",
y="total_views"
)

st.plotly_chart(
fig_ref,
use_container_width=True
)

st.dataframe(df_ref)

# =====================================================

# STORE VIEWS BY COUNTRY

# =====================================================

st.header("Store Views By Country")

countries = run_query("""

SELECT DISTINCT country_name_long
FROM dim_location
ORDER BY country_name_long;

""")

selected_country = st.selectbox(
"Select Country",
countries["country_name_long"]
)

df_store = run_query(f"""

SELECT *
FROM vw_store_views_by_country
WHERE country_name_long = '{selected_country}';

""")

fig_store = px.bar(
df_store,
x="store_name",
y="total_views"
)

st.plotly_chart(
fig_store,
use_container_width=True
)

st.dataframe(df_store)

# =====================================================

# PRODUCT HOURLY VIEWS

# =====================================================

st.header("Product Hourly Views")

products = run_query("""

SELECT DISTINCT product_id
FROM dim_product
ORDER BY product_id;

""")

selected_product = st.selectbox(
"Select Product",
products["product_id"]
)

df_product_hourly = run_query(f"""

SELECT *
FROM vw_product_hourly_views
WHERE product_id = '{selected_product}';

""")

fig_product_hourly = px.line(
df_product_hourly,
x="hour",
y="total_views",
markers=True
)

st.plotly_chart(
fig_product_hourly,
use_container_width=True
)

st.dataframe(df_product_hourly)

# =====================================================

# BROWSER + OS HOURLY VIEWS

# =====================================================

st.header("Browser & OS Hourly Views")

df_browser = run_query("""

SELECT *
FROM vw_browser_os_hourly_views;

""")

fig_browser = px.line(
df_browser,
x="hour",
y="total_views",
color="browser",
line_group="os",
markers=True
)

st.plotly_chart(
fig_browser,
use_container_width=True
)

st.dataframe(df_browser)
