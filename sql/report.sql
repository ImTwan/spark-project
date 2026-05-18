-- =====================================================
-- name: top_10_products
-- Top 10 product_id có lượt view cao nhất trong ngày hiện tại
-- =====================================================

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
-- name: top_10_countries
-- Top 10 quốc gia có lượt view cao nhất trong ngày hiện tại
-- =====================================================

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
-- name: top_5_referrer_urls
-- Top 5 referrer_url có lượt view cao nhất trong ngày hiện tại
-- =====================================================

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
-- name: store_views_by_country
-- Với 1 quốc gia bất kỳ:
-- lấy danh sách store_id và lượt view tương ứng
-- sắp xếp giảm dần
--
-- CHANGE:
-- 'Chile'
-- =====================================================

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
WHERE l.country_name_long = 'Chile'
GROUP BY
    l.country_name_long,
    s.store_id,
    s.store_name
ORDER BY total_views DESC;



-- =====================================================
-- name: product_hourly_views
-- Dữ liệu view phân bổ theo giờ
-- của một product_id bất kỳ trong ngày
--
-- CHANGE:
-- '96672'
-- =====================================================

SELECT
    d.hour,
    COUNT(*) AS total_views
FROM fact_product_view f
JOIN dim_product p
    ON f.sk_product = p.sk_product
JOIN dim_date d
    ON f.sk_date = d.sk_date
WHERE
    p.product_id = '96672'
    AND DATE(f.local_time) = CURRENT_DATE
GROUP BY d.hour
ORDER BY d.hour;



-- =====================================================
-- name: browser_os_hourly_views
-- Dữ liệu view theo giờ của từng browser, os
-- =====================================================

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