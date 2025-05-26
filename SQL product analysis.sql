CREATE SCHEMA IF NOT EXISTS gold;

-- Create tables
CREATE TABLE gold.dim_customers (
	customer_key INTEGER,
	customer_id INTEGER,
	customer_number VARCHAR(50),
	first_name VARCHAR(50),
	last_name VARCHAR(50),
	country VARCHAR(50),
	marital_status VARCHAR(50),
	gender VARCHAR(50),
	birthdate DATE,
	create_date DATE
);

CREATE TABLE gold.dim_products (
	product_key INTEGER,
	product_id INTEGER,
	product_number VARCHAR(50),
	product_name VARCHAR(50),
	category_id VARCHAR(50),
	category VARCHAR(50),
	subcategory VARCHAR(50),
	maintenance VARCHAR(50),
	cost INTEGER,
	product_line VARCHAR(50),
	start_date DATE
);

CREATE TABLE gold.fact_sales (
	order_number VARCHAR(50),
	product_key INTEGER,
	customer_key INTEGER,
	order_date DATE,
	shipping_date DATE,
	due_date DATE,
	sales_amount INTEGER,
	quantity SMALLINT,
	price INTEGER
);


Select * 
FROM gold.dim_customers

Select * 
FROM gold.dim_products;

Select * 
FROM gold.fact_sales;

--Grouping total_sales by order date
-- Grouping total_sales by order year
SELECT 
     
    SUM(sales_amount) AS total_sales,
	COUNT( DISTINCT(customer_key)) as total_customers,
	SUM (quantity) as total_quantity
FROM gold.fact_sales
WHERE order_date IS NOT NULL
GROUP BY EXTRACT(YEAR FROM order_date)
ORDER BY EXTRACT(YEAR FROM order_date);
-- Grouping total_sales by order MONTH
SELECT 
	EXTRACT(YEAR FROM order_date) AS order_year,
    EXTRACT(MONTH FROM order_date) AS order_month, 
    SUM(sales_amount) AS total_sales,
	COUNT( DISTINCT(customer_key)) as total_customers,
	SUM (quantity) as total_quantity
FROM gold.fact_sales
WHERE order_date IS NOT NULL
GROUP BY EXTRACT(YEAR FROM order_date) , EXTRACT(MONTH FROM order_date)
ORDER BY EXTRACT(YEAR FROM order_date) , EXTRACT(MONTH FROM order_date);


SELECT 
  TO_CHAR(DATE_TRUNC('month', order_date), 'YYYY-MM-DD') AS order_month,
  SUM(sales_amount) AS total_sales,
  COUNT(DISTINCT customer_key) AS total_customers,
  SUM(quantity) AS total_quantity
FROM gold.fact_sales
WHERE order_date IS NOT NULL
GROUP BY DATE_TRUNC('month', order_date)
ORDER BY DATE_TRUNC('month', order_date);


--Calculate the total sales per month and the running total of sales over time--

SELECT 
  TO_CHAR(DATE_TRUNC('month', order_date), 'YYYY-Mon') AS order_month,
  SUM(sales_amount) AS total_sales,
  COUNT(DISTINCT customer_key) AS total_customers,
  SUM(quantity) AS total_quantity
FROM gold.fact_sales
WHERE order_date IS NOT NULL
GROUP BY DATE_TRUNC('month', order_date)
ORDER BY DATE_TRUNC('month', order_date);

--CUMULATIVE ANALYSIS

SELECT 
	order_month,
	total_sales,
	SUM(total_sales) OVER (ORDER BY order_date) AS running_total_sales
FROM 
(

	SELECT 
		TO_CHAR(DATE_TRUNC('month', order_date), 'YYYY-MM-DD') AS order_month,
		SUM(sales_amount) AS total_sales
	FROM gold.fact_sales
	WHERE order_date IS NOT NULL
	GROUP BY DATE_TRUNC('month', order_date)

) t;


SELECT
  order_date,
  total_sales,
  SUM(total_sales) OVER (PARTITION BY order_date ORDER BY order_date) AS running_total
FROM (
  SELECT 
    TO_CHAR(DATE_TRUNC('month', order_date), 'YYYY-MM-DD') AS order_date,
    SUM(sales_amount) AS total_sales
  FROM gold.fact_sales
  WHERE order_date IS NOT NULL
  GROUP BY DATE_TRUNC('month', order_date)
) t;

--running total per year
SELECT
  order_date,
  total_sales,
  SUM(total_sales) OVER (ORDER BY order_date) AS running_total
FROM (
  SELECT 
    TO_CHAR(DATE_TRUNC('year', order_date), 'YYYY-MM-DD') AS order_date,
    SUM(sales_amount) AS total_sales
  FROM gold.fact_sales
  WHERE order_date IS NOT NULL
  GROUP BY DATE_TRUNC('year', order_date)
) t;

SELECT
  order_date,
  total_sales,
  SUM(total_sales) OVER (ORDER BY order_date) AS running_total,
  ROUND(AVG(avg_price) OVER (ORDER BY order_date), 2) AS avg_running_total

FROM (
  SELECT 
    TO_CHAR(DATE_TRUNC('year', order_date), 'YYYY-MM-DD') AS order_date,
	SUM(sales_amount) AS total_sales,
    AVG(price) AS avg_price
  FROM gold.fact_sales
  WHERE order_date IS NOT NULL
  GROUP BY DATE_TRUNC('year', order_date)
) t;

-- Analyze the yearly perfomance of products by comparing each product's sales to both its average sales perfomance and the previous year's sales.


SELECT TO_CHAR(DATE_TRUNC('year', order_date), 'YYYY') AS order_date,
	p.product_name,
	SUM(f.sales_amount) AS total_sales
FROM gold.fact_sales f 
LEFT JOIN gold.dim_products p
ON f.product_key = p.product_key
WHERE f.order_date IS NOT NULL
GROUP BY DATE_TRUNC('year', order_date), p.product_name
ORDER BY DATE_TRUNC('year', order_date)



WITH yearly_product_sales As(
SELECT TO_CHAR(DATE_TRUNC('year', order_date), 'YYYY') AS order_year,
	p.product_name,
	SUM(f.sales_amount) AS total_sales
FROM gold.fact_sales f 
LEFT JOIN gold.dim_products p 
ON f.product_key = p.product_key
WHERE f.order_date IS NOT NULL
GROUP BY DATE_TRUNC('year', order_date), p.product_name
ORDER BY DATE_TRUNC('year', order_date)
)
SELECT order_year, product_name, total_sales, 
	AVG(total_sales) OVER (PARTITION BY product_name) avg_sales,
	total_sales - AVG(total_sales) OVER (PARTITION BY product_name) AS diff_avg,
CASE WHEN total_sales - AVG(total_sales) OVER (PARTITION BY product_name) > 0 THEN ' above AVG'
	 WHEN total_sales - AVG(total_sales) OVER (PARTITION BY product_name) < 0 THEN ' below AVG'
	 ELSE 'AVG'
END avg_change,
--Year- over - year analysis
LAG(total_sales) OVER (PARTITION BY product_name ORDER BY order_year) py_sales,
total_sales - LAG(total_sales) OVER (PARTITION BY product_name ORDER BY order_year) as diff_py_sales,
CASE WHEN total_sales - LAG(total_sales) OVER (PARTITION BY product_name ORDER BY order_year) > 0 THEN ' increase'
	 WHEN total_sales - LAG(total_sales) OVER (PARTITION BY product_name ORDER BY order_year) < 0 THEN ' decrease'
	 ELSE 'AVG'
END py_change
FROM yearly_product_sales
ORDER by product_name, order_year

--Which categories contribute the most to overall sales?
WITH category_sales As (
SELECT Category, SUM (sales_amount) AS total_sales
FROM gold.fact_sales f 
LEFT JOIN gold.dim_products p
ON f.product_key = p.product_key
GROUP BY category ) 

SELECT Category, total_sales, SUM(total_sales) OVER () overall_sales,
	CONCAT(ROUND(CAST(total_sales AS NUMERIC) / SUM(total_sales) OVER () * 100, 2), '%') AS percentage_of_total
FROM category_sales

-- 	GROUP the data based on a specifit 
-- Segment products into cost ranges and count how many products fall into each segment
WITH product_segment As (
SELECT product_key, Product_name, cost,
CASE WHEN cost < 100 THEN 'Below 100'
	 WHEN cost BETWEEN 100 AND 500 THEN '100-500'
	 WHEN cost BETWEEN 500 AND 1000 THEN '500-1000'
	 ELSE 'ABOVE 1000'
END cost_range
FROM gold.dim_products ) 

SELECT cost_range, COUNT(product_key) AS total_products
FROM product_segment
GROUP BY cost_range
ORDER BY total_products desc

/*GROUP customers into three segments based on their spending behavior:
--- VIP: at least 12 months of history and spending more than 5000
--- Regular: at least 12 months of history but spending 5000 or less
--- New: lifespand less than 12 months
And find the total number of customers by each group*/ 

SELECT c.customer_key, 
	SUM(f.sales_amount) AS total_spending, 
	MIN(order_date) AS first_order,
	MAX(order_date) AS last_order
	DATEDIFF
FROM gold.fact_sales f 
LEFT JOIN gold.dim_customers c
ON f.customer_key = c.customer_key
GROUP BY c.customer_key

WITH customer_spending as (
SELECT
  c.customer_key,
  SUM(f.sales_amount) AS total_spending,
  MIN(order_date) AS first_order,
  MAX(order_date) AS last_order,
  (DATE_PART('year', AGE(MAX(order_date), MIN(order_date))) * 12 +
   DATE_PART('month', AGE(MAX(order_date), MIN(order_date)))) AS lifespan_months
FROM gold.fact_sales f
LEFT JOIN gold.dim_customers c ON f.customer_key = c.customer_key
GROUP BY c.customer_key)

SELECT customer_key, total_spending, lifespan_months,
CASE WHEN lifespan_months > 12 AND total_spending > 5000 THEN 'VIP'
	 WHEN lifespan_months >= 12 AND total_spending <= 5000 THEN 'REGULAR CUSTOMER'
	 ELSE 'New'
END customer_segment 
FROM customer_spending


WITH customer_spending as (
SELECT
  c.customer_key,
  SUM(f.sales_amount) AS total_spending,
  MIN(order_date) AS first_order,
  MAX(order_date) AS last_order,
  (DATE_PART('year', AGE(MAX(order_date), MIN(order_date))) * 12 +
   DATE_PART('month', AGE(MAX(order_date), MIN(order_date)))) AS lifespan_months
FROM gold.fact_sales f
LEFT JOIN gold.dim_customers c ON f.customer_key = c.customer_key
GROUP BY c.customer_key)

Select customer_segment, COUNT(customer_key)
FROM (
SELECT customer_key,
CASE WHEN lifespan_months > 12 AND total_spending > 5000 THEN 'VIP'
	 WHEN lifespan_months >= 12 AND total_spending <= 5000 THEN 'REGULAR CUSTOMER'
	 ELSE 'New'
END customer_segment 
FROM customer_spending) t
GROUP BY customer_segment

