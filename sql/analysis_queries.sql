-- Preview each Bronze table (10 rows)
SELECT * FROM ECOMMERCE_AI.BRONZE.ORDERS LIMIT 10;
SELECT * FROM ECOMMERCE_AI.BRONZE.ORDER_ITEMS LIMIT 10;
SELECT * FROM ECOMMERCE_AI.BRONZE.CUSTOMERS LIMIT 10;
SELECT * FROM ECOMMERCE_AI.BRONZE.PRODUCTS LIMIT 10;
SELECT * FROM ECOMMERCE_AI.BRONZE.ORDER_PAYMENTS LIMIT 10;
SELECT * FROM ECOMMERCE_AI.BRONZE.ORDER_REVIEWS LIMIT 10;
SELECT * FROM ECOMMERCE_AI.BRONZE.SELLERS LIMIT 10;
SELECT * FROM ECOMMERCE_AI.BRONZE.CATEGORY_TRANSLATION LIMIT 10;


SELECT * FROM ECOMMERCE_AI.BRONZE.ORDER_REVIEWS LIMIT 10;


CREATE OR REPLACE TABLE ECOMMERCE_AI.GOLD.REVENUE_METRICS AS
SELECT
    year_month,
    product_category_name_english,
    customer_state,
    payment_type,
    COUNT(DISTINCT order_id)    AS total_orders,
    SUM(total_item_value)       AS total_revenue,
    AVG(total_item_value)       AS avg_order_value,
    AVG(days_to_deliver)        AS avg_delivery_days
FROM ECOMMERCE_AI.SILVER.ORDERS_ENRICHED
WHERE product_category_name_english IS NOT NULL
AND year_month IS NOT NULL
GROUP BY
    year_month,
    product_category_name_english,
    customer_state,
    payment_type
ORDER BY year_month, total_revenue DESC;

SELECT * FROM ECOMMERCE_AI.GOLD.REVENUE_METRICS LIMIT 10;


CREATE OR REPLACE TABLE ECOMMERCE_AI.GOLD.CUSTOMER_METRICS AS
SELECT
    customer_id,
    customer_state,
    customer_city,
    COUNT(DISTINCT order_id)    AS total_orders,
    SUM(total_item_value)       AS total_spent,
    AVG(total_item_value)       AS avg_order_value,
    MIN(purchase_date)          AS first_order_date,
    MAX(purchase_date)          AS last_order_date
FROM ECOMMERCE_AI.SILVER.ORDERS_ENRICHED
WHERE customer_id IS NOT NULL
GROUP BY
    customer_id,
    customer_state,
    customer_city
ORDER BY total_spent DESC;

CREATE OR REPLACE TABLE ECOMMERCE_AI.GOLD.SELLER_METRICS AS
SELECT
    seller_id,
    seller_city,
    seller_state,
    COUNT(DISTINCT order_id)    AS total_orders,
    SUM(total_item_value)       AS total_revenue,
    AVG(total_item_value)       AS avg_order_value,
    AVG(days_to_deliver)        AS avg_delivery_days
FROM ECOMMERCE_AI.SILVER.ORDERS_ENRICHED
WHERE seller_id IS NOT NULL
GROUP BY
    seller_id,
    seller_city,
    seller_state
ORDER BY total_revenue DESC;


WITH monthly_category AS (
    SELECT
        product_category_name_english,
        year_month,
        SUM(total_revenue) AS monthly_revenue
    FROM ECOMMERCE_AI.GOLD.REVENUE_METRICS
    GROUP BY product_category_name_english, year_month
),
with_previous AS (
    SELECT
        product_category_name_english,
        year_month,
        monthly_revenue,
        LAG(monthly_revenue) OVER (
            PARTITION BY product_category_name_english
            ORDER BY year_month
        ) AS prev_month_revenue
    FROM monthly_category
)
SELECT
    product_category_name_english,
    year_month,
    monthly_revenue,
    prev_month_revenue,
    monthly_revenue - prev_month_revenue AS revenue_change
FROM with_previous
WHERE monthly_revenue < prev_month_revenue
ORDER BY revenue_change ASC;


WITH customer_orders AS (
    SELECT
        customer_id,
        total_orders
    FROM ECOMMERCE_AI.GOLD.CUSTOMER_METRICS
),
repeat_customers AS (
    SELECT
        COUNT(*) AS repeat_count
    FROM customer_orders
    WHERE total_orders > 1
),
total_customers AS (
    SELECT
        COUNT(*) AS total_count
    FROM customer_orders
)
SELECT
    repeat_count,
    total_count,
    ROUND(repeat_count * 100.0 / total_count, 2) AS repeat_customer_pct
FROM repeat_customers, total_customers;


SELECT
    customer_state,
    COUNT(customer_id)      AS total_customers,
    SUM(total_spent)        AS total_revenue,
    AVG(total_spent)        AS avg_spent_per_customer
FROM ECOMMERCE_AI.GOLD.CUSTOMER_METRICS
GROUP BY customer_state
ORDER BY total_customers DESC;


SELECT
    seller_id,
    seller_state,
    total_revenue,
    avg_delivery_days,
    total_orders
FROM ECOMMERCE_AI.GOLD.SELLER_METRICS
WHERE total_revenue > (
    SELECT AVG(total_revenue) 
    FROM ECOMMERCE_AI.GOLD.SELLER_METRICS
)
AND avg_delivery_days > (
    SELECT AVG(avg_delivery_days) 
    FROM ECOMMERCE_AI.GOLD.SELLER_METRICS
)
ORDER BY total_revenue DESC;



WITH category_revenue AS (
    SELECT
        product_category_name_english,
        SUM(total_revenue) AS category_revenue
    FROM ECOMMERCE_AI.GOLD.REVENUE_METRICS
    GROUP BY product_category_name_english
),
with_total AS (
    SELECT
        product_category_name_english,
        category_revenue,
        SUM(category_revenue) OVER () AS grand_total
    FROM category_revenue
)


WITH seller_state_metrics AS (
    SELECT
        seller_state,
        SUM(total_orders)                               AS total_orders,
        SUM(total_revenue)                              AS total_revenue,
        AVG(avg_delivery_days)                          AS avg_delivery_days
    FROM ECOMMERCE_AI.GOLD.SELLER_METRICS
    GROUP BY seller_state
)
SELECT
    seller_state,
    total_orders,
    total_revenue,
    ROUND(avg_delivery_days, 2)                         AS avg_delivery_days
FROM seller_state_metrics
ORDER BY avg_delivery_days DESC;
SELECT
    product_category_name_english,
    category_revenue,
    grand_total,
    ROUND(category_revenue * 100.0 / grand_total, 2) AS revenue_pct
FROM with_total
ORDER BY category_revenue DESC
LIMIT 10;