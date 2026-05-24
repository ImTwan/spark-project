CREATE TABLE IF NOT EXISTS dim_product (
    product_id VARCHAR(255) PRIMARY KEY,
    option TEXT
);


CREATE TABLE IF NOT EXISTS dim_customer (
    sk_customer INT PRIMARY KEY,
    email VARCHAR(255),
    ip VARCHAR(255)
);



CREATE TABLE IF NOT EXISTS dim_store (
    store_id VARCHAR(255) PRIMARY KEY,
    store_name VARCHAR(255) NOT NULL 
);


CREATE TABLE IF NOT EXISTS dim_agent (
    sk_agent INT PRIMARY KEY,
    browser VARCHAR(255),
    os VARCHAR(255)
);



CREATE TABLE IF NOT EXISTS dim_location (
    sk_location INT PRIMARY KEY,
    country_name_long VARCHAR(255),
    city_name VARCHAR(255),
    region_name VARCHAR(255)
);


CREATE TABLE IF NOT EXISTS dim_date (
    sk_date BIGINT PRIMARY KEY,
    full_date DATE,
    day_of_week INT,
    day_of_month INT,
    day_of_year INT,
    year_month VARCHAR(20),
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

    id VARCHAR(255) PRIMARY KEY,

    product_id VARCHAR(255),
    store_id VARCHAR(255),

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

    FOREIGN KEY (product_id)
        REFERENCES dim_product(product_id),

    FOREIGN KEY (store_id)
        REFERENCES dim_store(store_id),

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
-- UNKNOWN DIMENSION ROWS
-- =====================================================

INSERT INTO dim_product (product_id, option)
VALUES (...)
ON CONFLICT (product_id)
DO UPDATE SET option = EXCLUDED.option;


INSERT INTO dim_customer
(sk_customer, email, ip)
VALUES
(0, 'UNKNOWN', '0.0.0.0')
ON CONFLICT DO NOTHING;


INSERT INTO dim_agent
(sk_agent, browser, os)
VALUES
(0, 'UNKNOWN', 'UNKNOWN')
ON CONFLICT DO NOTHING;


INSERT INTO dim_location
(
    sk_location,
    country_name_long,
    city_name,
    region_name
)
VALUES
(
    0,
    'UNKNOWN',
    'UNKNOWN',
    'UNKNOWN'
)
ON CONFLICT DO NOTHING;


INSERT INTO dim_date
(
    sk_date,
    full_date,
    day_of_week,
    day_of_month,
    day_of_year,
    year_month,
    month,
    week_of_year,
    quarter_number,
    year,
    year_number,
    is_weekend,
    hour,
    minute
)
VALUES
(
    0,
    '1970-01-01',
    0,
    0,
    0,
    '1970-01',
    0,
    0,
    0,
    1970,
    1970,
    false,
    0,
    0
)
ON CONFLICT DO NOTHING;



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
ON f.product_id = p.product_id
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
    l.country_name_long AS country,
    COUNT(*) AS views
FROM fact_product_view f
JOIN dim_location l ON f.sk_location = l.sk_location
WHERE DATE(f.local_time) = CURRENT_DATE
GROUP BY l.country_name_long
ORDER BY views DESC
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
    ON f.store_id = s.store_id
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
ON f.product_id = p.product_id
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

