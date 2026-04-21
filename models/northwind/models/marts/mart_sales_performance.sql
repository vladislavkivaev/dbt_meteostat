with sales AS (
    SELECT *
    FROM {{ref ('prep_sales')}}
)
SELECT 
order_year,
order_month,
category_name,
ROUND (SUM(revenue), 2) as total_revenue,
COUNT(DISTINCT order_id) as total_number_of_orders,
ROUND (AVG(revenue), 2) as avg_revenue_per_order
FROM sales
GROUP BY 1, 2, 3
ORDER BY category_name, order_year, order_month
