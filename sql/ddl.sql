CREATE TABLE IF NOT EXISTS dim_product (

    sk_product SERIAL PRIMARY KEY,

    product_id VARCHAR(255) NOT NULL,

    option VARCHAR(255)
);



CREATE TABLE IF NOT EXISTS dim_customer (

    sk_customer SERIAL PRIMARY KEY,

    email VARCHAR(255) NOT NULL,

    ip VARCHAR(255) NOT NULL
);



CREATE TABLE IF NOT EXISTS dim_store (

    sk_store SERIAL PRIMARY KEY,

    store_id VARCHAR(255) NOT NULL,

    store_name VARCHAR(255)
);



CREATE TABLE IF NOT EXISTS dim_agent (

    sk_agent SERIAL PRIMARY KEY,

    browser VARCHAR(255),

    os VARCHAR(255)

);



CREATE TABLE IF NOT EXISTS dim_location (

    sk_location SERIAL PRIMARY KEY,

    country_name_short VARCHAR(255) NOT NULL,
 
    country_name_long VARCHAR(255) NOT NULL,

    city_name VARCHAR(255) NOT NULL,

    region_name VARCHAR(255) NOT NULL

);



CREATE TABLE IF NOT EXISTS dim_date (

    sk_date SERIAL PRIMARY KEY,

    full_date TIMESTAMP,

    day_of_week INT,

    day_of_month INT,

    day_of_year INT,

    year_month VARCHAR(255),

    month INT,

    week_of_year INT,

    quarter_number INT,

    year INT,

    year_number INT,

    is_weekend BOOLEAN,

    hour INT,

    minute INT
);



CREATE TABLE IF NOT EXISTS fact_product_view (

    view_id SERIAL PRIMARY KEY,

    sk_product INT,

    sk_store INT,

    sk_customer INT,

    sk_agent INT,

    sk_location INT,

    sk_date INT,

    api_version VARCHAR(255),

    collection VARCHAR(255),

    current_url TEXT,

    referrer_url TEXT,

    local_time TIMESTAMP,

    time_stamp BIGINT,

    FOREIGN KEY (sk_product)
        REFERENCES dim_product(sk_product),

    FOREIGN KEY (sk_store)
        REFERENCES dim_store(sk_store),

    FOREIGN KEY (sk_customer)
        REFERENCES dim_customer(sk_customer),

    FOREIGN KEY (sk_agent)
        REFERENCES dim_agent(sk_agent),

    FOREIGN KEY (sk_location)
        REFERENCES dim_location(sk_location),

    FOREIGN KEY (sk_date)
        REFERENCES dim_date(sk_date)
);

-- =====================================================
-- REPORT 1
-- Top 10 product_id có lượt view cao nhất trong ngày
-- =====================================================

CREATE OR REPLACE VIEW vw_top_10_products AS

SELECT
    p.product_id,
    COUNT(*) AS total_views
FROM fact_product_view f
JOIN dim_product p
ON f.sk_product = p.sk_product
WHERE DATE(f.local_time) = CURRENT_DATE
GROUP BY p.product_id
ORDER BY total_views DESC
LIMIT 10;



-- =====================================================
-- REPORT 2
-- Top 10 quốc gia có lượt view cao nhất trong ngày
-- =====================================================

CREATE OR REPLACE VIEW vw_top_10_countries AS

SELECT
    l.country_name_long,
    COUNT(*) AS total_views
FROM fact_product_view f
JOIN dim_location l
ON f.sk_location = l.sk_location
WHERE DATE(f.local_time) = CURRENT_DATE
GROUP BY l.country_name_long
ORDER BY total_views DESC
LIMIT 10;



-- =====================================================
-- REPORT 3
-- Top 5 referrer_url có lượt view cao nhất trong ngày
-- =====================================================

CREATE OR REPLACE VIEW vw_top_5_referrer_urls AS

SELECT
    referrer_url,
    COUNT(*) AS total_views
FROM fact_product_view
WHERE
    DATE(local_time) = CURRENT_DATE
    AND referrer_url IS NOT NULL
    AND referrer_url != ''
GROUP BY referrer_url
ORDER BY total_views DESC
LIMIT 5;



-- =====================================================
-- REPORT 4
-- Store views by country
-- =====================================================

CREATE OR REPLACE VIEW vw_store_views_by_country AS

SELECT
    l.country_name_long,
    s.store_id,
    s.store_name,
    COUNT(*) AS total_views
FROM fact_product_view f
JOIN dim_location l
ON f.sk_location = l.sk_location
JOIN dim_store s
ON f.sk_store = s.sk_store
GROUP BY
    l.country_name_long,
    s.store_id,
    s.store_name
ORDER BY total_views DESC;



-- =====================================================
-- REPORT 5
-- Product hourly views
-- =====================================================

CREATE OR REPLACE VIEW vw_product_hourly_views AS

SELECT
    p.product_id,
    d.hour,
    COUNT(*) AS total_views
FROM fact_product_view f
JOIN dim_product p
ON f.sk_product = p.sk_product
JOIN dim_date d
ON f.sk_date = d.sk_date
WHERE DATE(f.local_time) = CURRENT_DATE
GROUP BY
    p.product_id,
    d.hour
ORDER BY
    p.product_id,
    d.hour;



-- =====================================================
-- REPORT 6
-- Browser + OS hourly views
-- =====================================================

CREATE OR REPLACE VIEW vw_browser_os_hourly_views AS

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
WHERE DATE(f.local_time) = CURRENT_DATE
GROUP BY
    d.hour,
    a.browser,
    a.os
ORDER BY
    d.hour,
    total_views DESC;

