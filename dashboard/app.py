import streamlit as st
import pandas as pd
import psycopg2
import plotly.express as px
from streamlit_autorefresh import st_autorefresh


# =====================================================
# AUTO REFRESH
# =====================================================

st_autorefresh(interval=5000, key="refresh")

st.set_page_config(
    page_title="Realtime Product Dashboard",
    layout="wide"
)

st.title("📊 Realtime Product View Dashboard")


# =====================================================
# POSTGRES CONNECTION
# =====================================================
conn = psycopg2.connect(
    host="localhost",
    port="5432",
    database="postgres",
    user="postgres",
    password="Uni1234"
)

# =====================================================
# READ SQL
# =====================================================

def read_sql(query):

    return pd.read_sql(query, conn)


# =====================================================
# REPORT 1
# TOP 10 PRODUCTS
# =====================================================

st.header("🔥 Top 10 Products Today")

query_1 = """

SELECT
    product_id,
    COUNT(*) AS total_views
FROM fact_product_view
WHERE DATE(local_time) =
(
    SELECT MAX(DATE(local_time))
    FROM fact_product_view
)
GROUP BY product_id
ORDER BY total_views DESC
LIMIT 10

"""

df1 = read_sql(query_1)

col1, col2 = st.columns([1, 2])

with col1:

    st.dataframe(df1)

with col2:

    fig1 = px.bar(
        df1,
        x="product_id",
        y="total_views",
        title="Top Products"
    )

    st.plotly_chart(fig1, use_container_width=True)


# =====================================================
# REPORT 2
# TOP COUNTRIES
# =====================================================

st.header("🌍 Top 10 Countries Today")

query_2 = """

SELECT
    l.country_name_long,
    COUNT(*) AS total_views
FROM fact_product_view f
JOIN dim_location l
ON f.sk_location = l.sk_location
WHERE DATE(f.local_time) =
(
    SELECT MAX(DATE(local_time))
    FROM fact_product_view
)
GROUP BY l.country_name_long
ORDER BY total_views DESC
LIMIT 10

"""

df2 = read_sql(query_2)

col1, col2 = st.columns([1, 2])

with col1:

    st.dataframe(df2)

with col2:

    fig2 = px.bar(
        df2,
        x="country_name_long",
        y="total_views",
        title="Top Countries"
    )

    st.plotly_chart(fig2, use_container_width=True)


# =====================================================
# REPORT 3
# TOP REFERRER URL
# =====================================================

st.header("🔗 Top 5 Referrer URLs")

query_3 = """

SELECT
    referrer_url,
    COUNT(*) AS total_views
FROM fact_product_view
WHERE
    DATE(local_time) =
    (
        SELECT MAX(DATE(local_time))
        FROM fact_product_view
    )
    AND referrer_url IS NOT NULL
    AND TRIM(referrer_url) <> ''
GROUP BY referrer_url
ORDER BY total_views DESC
LIMIT 5

"""

df3 = read_sql(query_3)

st.dataframe(df3)


# =====================================================
# REPORT 4
# STORE BY COUNTRY
# =====================================================

st.header("🏪 Store Views By Country")

country_df = read_sql("""

SELECT DISTINCT country_name_long
FROM dim_location
ORDER BY country_name_long

""")

country = st.selectbox(
    "Choose Country",
    country_df["country_name_long"]
)

query_4 = f"""

SELECT
    s.store_id,
    COUNT(*) AS total_views
FROM fact_product_view f
JOIN dim_store s
ON f.store_id = s.store_id
JOIN dim_location l
ON f.sk_location = l.sk_location
WHERE l.country_name_long = '{country}'
GROUP BY s.store_id
ORDER BY total_views DESC

"""

df4 = read_sql(query_4)

col1, col2 = st.columns([1, 2])

with col1:

    st.dataframe(df4)

with col2:

    fig4 = px.bar(
        df4,
        x="store_id",
        y="total_views",
        title=f"Store Views - {country}"
    )

    st.plotly_chart(fig4, use_container_width=True)


# =====================================================
# REPORT 5
# PRODUCT HOURLY ANALYTICS
# =====================================================

st.header("⏰ Product Hourly Analytics")

# -----------------------------------------------------
# TOP PRODUCTS ONLY
# -----------------------------------------------------

product_df = read_sql("""

SELECT
    product_id,
    COUNT(*) AS total_views
FROM fact_product_view
WHERE DATE(local_time) =
(
    SELECT MAX(DATE(local_time))
    FROM fact_product_view
)
GROUP BY product_id
ORDER BY total_views DESC
LIMIT 20

""")

product_id = st.selectbox(
    "Choose Product",
    product_df["product_id"]
)

# -----------------------------------------------------
# HOURLY DATA
# -----------------------------------------------------

query_5 = f"""

SELECT
    EXTRACT(HOUR FROM local_time) AS hour,
    COUNT(*) AS total_views
FROM fact_product_view
WHERE
    product_id = '{product_id}'
    AND DATE(local_time) =
    (
        SELECT MAX(DATE(local_time))
        FROM fact_product_view
    )
GROUP BY hour
ORDER BY hour

"""

df5 = read_sql(query_5)

# -----------------------------------------------------
# KPI
# -----------------------------------------------------

if not df5.empty:

    total_views = int(df5["total_views"].sum())

    peak_hour = int(
        df5.loc[
            df5["total_views"].idxmax(),
            "hour"
        ]
    )

    peak_views = int(
        df5["total_views"].max()
    )

    avg_views = round(
        df5["total_views"].mean(),
        2
    )

    k1, k2, k3, k4 = st.columns(4)

    k1.metric(
        "Total Views",
        total_views
    )

    k2.metric(
        "Peak Hour",
        f"{peak_hour}:00"
    )

    k3.metric(
        "Peak Views",
        peak_views
    )

    k4.metric(
        "Avg Views/Hour",
        avg_views
    )

# -----------------------------------------------------
# CHARTS
# -----------------------------------------------------

col1, col2 = st.columns(2)

with col1:

    fig_line = px.line(
        df5,
        x="hour",
        y="total_views",
        markers=True,
        title=f"Hourly Trend - Product {product_id}"
    )

    st.plotly_chart(
        fig_line,
        use_container_width=True
    )

with col2:

    fig_bar = px.bar(
        df5,
        x="hour",
        y="total_views",
        title=f"Hourly Distribution - Product {product_id}"
    )

    st.plotly_chart(
        fig_bar,
        use_container_width=True
    )

# -----------------------------------------------------
# RAW DATA
# -----------------------------------------------------

with st.expander("View Raw Hourly Data"):
    st.dataframe(df5)
# =====================================================
# REPORT 6
# BROWSER + OS
# =====================================================

st.header("💻 Browser & OS Hourly Views")

query_6 = """

SELECT
    d.hour,
    a.browser,
    a.os,
    COUNT(*) AS total_views
FROM fact_product_view f
JOIN dim_agent a
ON f.sk_agent = a.sk_agent
JOIN dim_date d
ON f.sk_date = d.sk_date
WHERE DATE(f.local_time) =
(
    SELECT MAX(DATE(local_time))
    FROM fact_product_view
)
GROUP BY
    d.hour,
    a.browser,
    a.os
ORDER BY d.hour

"""

df6 = read_sql(query_6)

fig6 = px.bar(
    df6,
    x="hour",
    y="total_views",
    color="browser",
    hover_data=["os"],
    title="Browser & OS Hourly Views"
)

st.plotly_chart(fig6, use_container_width=True)