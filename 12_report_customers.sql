/*
===============================================================================
Customer Report
===============================================================================
Purpose:
    - This report consolidates key customer metrics and behaviors

Highlights:
    1. Gathers essential fields such as names, ages, and transaction details.
	2. Segments customers into categories (VIP, Regular, New) and age groups.
    3. Aggregates customer-level metrics:
	   - total orders
	   - total sales
	   - total quantity purchased
	   - total products
	   - lifespan (in months)
    4. Calculates valuable KPIs:
	    - recency (months since last order)
		- average order value
		- average monthly spend
===============================================================================
*/

-- =============================================================================
-- Create Report: gold.report_customers
-- =============================================================================

/*---------------------------------------------------------------------------
1) Base Query: Retrieves core columns from tables
---------------------------------------------------------------------------*/
WITH base_query As(
SELECT
f.order_number,
f.product_key,
f.order_date,
f.sales_amount,
f.quantity,
c.customer_key,
c.customer_number,
CONCAT(c.first_name, ' ', c.last_name) As customer_name,
DATE_PART('year', AGE(current_date, c.birthdate)) AS age

FROM gold.fact_sales f
LEFT JOIN gold.dim_customers c
ON c.customer_key = f.customer_key
WHERE f.order_date IS NOT NULL)
, customer_aggregation AS (
/*---------------------------------------------------------------------------
2) Customer Aggregations: Summarizes key metrics at the customer level
---------------------------------------------------------------------------*/
SELECT 
	customer_key,
	customer_number,
	customer_name,
	age,
	COUNT(DISTINCT order_number) AS total_orders,
	SUM(sales_amount) AS total_sales, 
	SUM(quantity) AS total_quantity,
	COUNT( DISTINCT product_key) AS total_products,
	MAX (order_date) AS last_order_date,
	(DATE_PART('year', AGE(MAX(order_date), MIN(order_date))) * 12 +
	DATE_PART('month', AGE(MAX(order_date), MIN(order_date)))) AS lifespan_months
FROM base_query
GROUP BY 
	customer_key,
	customer_number,
	customer_name,
	age
) 
SELECT
	customer_key,
	customer_number,
	customer_name,
	age,
	CASE WHEN age < 20 THEN 'Under 20'
		 WHEN age between 20 and 29 THEN '20-29'
		 WHEN age between 30 and 39 THEN '30-39'
		 WHEN age between 40 and 49 THEN '20-29'
		 ELSE '50 and above'
	END AS age_group,
	CASE WHEN lifespan_months > 12 AND total_sales > 5000 THEN 'VIP'
		 WHEN lifespan_months >= 12 AND total_sales <= 5000 THEN 'REGULAR CUSTOMER'
		 ELSE 'New'
	END as customer_segment,
	total_orders,
	total_sales, 
	total_quantity,
	total_products,
	last_order_date,
	DATE_PART('year', AGE(current_date, last_order_date)) * 12 +
	DATE_PART('month', AGE(current_date, last_order_date)) AS recency,
	lifespan_months,
	--compute average order value
	CASE WHEN total_orders = 0 THEN 0
		ELSE total_sales/total_orders 
	END AS  avg_order_value,
    -- Compute month spending = total Sales/ nr.of months
	CASE WHEN lifespan_months = 0 THEN total_sales
		ELSE total_sales/lifespan_months
	END AS avg_monthly_spend
from customer_aggregation


