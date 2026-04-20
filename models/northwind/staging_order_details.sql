WITH source_data AS (
    SELECT *
    FROM {{source ('northwind', 'order_details') }}
)
SELECT 
orderid AS order_id,
productid AS product_id,
unitprice::NUMERIC as unit_price,
quantity::INT as quantity,
discount::NUMERIC as discount
FROM source_data