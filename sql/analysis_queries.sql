-- Coffee Market Analysis
-- SQL queries used for exploratory analysis

-- 1. Count coffee shops by city
SELECT
    city,
    COUNT(*) AS coffee_shop_count
FROM business
GROUP BY city
ORDER BY coffee_shop_count DESC;


-- 2. Average rating by city
SELECT
    city,
    ROUND(AVG(stars), 2) AS average_rating
FROM business
GROUP BY city
ORDER BY average_rating DESC;


-- 3. Top coffee shops by review count
SELECT
    name,
    city,
    stars,
    review_count
FROM business
ORDER BY review_count DESC
LIMIT 10;


-- 4. Rating distribution
SELECT
    stars,
    COUNT(*) AS business_count
FROM business
GROUP BY stars
ORDER BY stars;


-- 5. Highest-rated cities with at least 5 coffee shops
SELECT
    city,
    COUNT(*) AS coffee_shop_count,
    ROUND(AVG(stars), 2) AS average_rating
FROM business
GROUP BY city
HAVING COUNT(*) >= 5
ORDER BY average_rating DESC;


-- 6. Businesses grouped by rating band
SELECT
    CASE
        WHEN stars < 2 THEN 'Below 2.0'
        WHEN stars >= 2 AND stars < 3 THEN '2.0 - 2.9'
        WHEN stars >= 3 AND stars < 4 THEN '3.0 - 3.9'
        WHEN stars >= 4 AND stars < 5 THEN '4.0 - 4.9'
        ELSE '5.0'
    END AS rating_band,
    COUNT(*) AS business_count
FROM business
GROUP BY rating_band
ORDER BY business_count DESC;
