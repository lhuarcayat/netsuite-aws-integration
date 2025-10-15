-- Validation Query 1: Ensure there is only one active record for each dimension ID
SELECT customer_id
FROM sales.dim_customer
WHERE active_flag = 1
GROUP BY customer_id
HAVING COUNT(*) > 1;

SELECT product_id
FROM sales_uk.dim_product
WHERE active_flag = 1
GROUP BY product_id
HAVING COUNT(*) > 1;


-- Validation Query 2: Ensure All Foreign Keys in Fact Tables Exist in Dimension Tables
SELECT order_customer_id, order_id_
FROM sales_uk.fact_orders
WHERE order_customer_id NOT IN (
    SELECT customer_id
    FROM sales_uk.dim_customer
);
 
SELECT product_id, order_details_id_
FROM sales_uk.fact_order_details
WHERE product_id NOT IN (
    SELECT product_id
    FROM sales_uk.dim_product
);

