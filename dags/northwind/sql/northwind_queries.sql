-- 1) Revenue by country
SELECT
    shipCountry AS country,
    COUNT(DISTINCT orderID) AS total_orders,
    SUM(quantity * unitPrice * (1 - COALESCE(discount, 0))) AS total_revenue
FROM fact_orders
GROUP BY shipCountry
ORDER BY total_revenue DESC;

-- 2) Revenue by category
SELECT
    categoryName AS category,
    COUNT(DISTINCT orderID) AS total_orders,
    SUM(quantity) AS total_quantity,
    SUM(quantity * unitPrice * (1 - COALESCE(discount, 0))) AS total_revenue
FROM fact_orders
GROUP BY categoryName
ORDER BY total_revenue DESC;

-- 3) Audit rows rejected during cleaning
SELECT table_name, reason, COUNT(*) AS rejected_rows
FROM audit_invalid_rows
GROUP BY table_name, reason
ORDER BY table_name, rejected_rows DESC;

-- 4) Top 10 customers by revenue
SELECT
    customerID,
    COUNT(DISTINCT orderID) AS orders_count,
    SUM(quantity * unitPrice * (1 - COALESCE(discount, 0))) AS total_revenue
FROM fact_orders
GROUP BY customerID
ORDER BY total_revenue DESC
LIMIT 10;
