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


-----------------------------------------------------------------------------------
-- INITIAL EXPLORATION
-----------------------------------------------------------------------------------

-- Explore data to understand columns and sample rows.
SELECT * FROM gold.dim_customers;
SELECT * FROM gold.dim_products;
SELECT * FROM gold.fact_sales;

-----------------------------------------------------------------------------------
-- AGGREGATING SALES BY YEAR
-----------------------------------------------------------------------------------

-- Group sales by year to see annual trends.
-- Metrics:
-- total_sales: how much revenue was made that year,
-- total_customers: unique customers who bought something,
-- total_quantity: total items sold.
SELECT 
    EXTRACT(YEAR FROM order_date) AS order_year,
    SUM(sales_amount) AS total_sales,
	COUNT(DISTINCT customer_key) AS total_customers,
	SUM(quantity) AS total_quantity
FROM gold.fact_sales
WHERE order_date IS NOT NULL
GROUP BY EXTRACT(YEAR FROM order_date)
ORDER BY EXTRACT(YEAR FROM order_date);

-----------------------------------------------------------------------------------
-- AGGREGATING SALES BY YEAR AND MONTH
-----------------------------------------------------------------------------------

-- Drill down further into year and month to analyze seasonality and patterns over time.
SELECT 
	EXTRACT(YEAR FROM order_date) AS order_year,
    EXTRACT(MONTH FROM order_date) AS order_month,
    SUM(sales_amount) AS total_sales,
	COUNT(DISTINCT customer_key) AS total_customers,
	SUM(quantity) AS total_quantity
FROM gold.fact_sales
WHERE order_date IS NOT NULL
GROUP BY EXTRACT(YEAR FROM order_date), EXTRACT(MONTH FROM order_date)
ORDER BY EXTRACT(YEAR FROM order_date), EXTRACT(MONTH FROM order_date);

-----------------------------------------------------------------------------------
-- AGGREGATING SALES BY MONTH (FORMATTED)
-----------------------------------------------------------------------------------

-- Use DATE_TRUNC to group by month regardless of day.
-- Use TO_CHAR to output a clear label like 'YYYY-MM-DD'.
SELECT 
  TO_CHAR(DATE_TRUNC('month', order_date), 'YYYY-MM-DD') AS order_month,
  SUM(sales_amount) AS total_sales,
  COUNT(DISTINCT customer_key) AS total_customers,
  SUM(quantity) AS total_quantity
FROM gold.fact_sales
WHERE order_date IS NOT NULL
GROUP BY DATE_TRUNC('month', order_date)
ORDER BY DATE_TRUNC('month', order_date);

-----------------------------------------------------------------------------------
-- MONTHLY TOTALS FOR CUMULATIVE ANALYSIS
-----------------------------------------------------------------------------------

-- Another grouping for month-level data, with a slightly different format (YYYY-Mon)
-- Useful for feeding into a running total or visualization.
SELECT 
  TO_CHAR(DATE_TRUNC('month', order_date), 'YYYY-Mon') AS order_month,
  SUM(sales_amount) AS total_sales,
  COUNT(DISTINCT customer_key) AS total_customers,
  SUM(quantity) AS total_quantity
FROM gold.fact_sales
WHERE order_date IS NOT NULL
GROUP BY DATE_TRUNC('month', order_date)
ORDER BY DATE_TRUNC('month', order_date);

-----------------------------------------------------------------------------------
-- CUMULATIVE ANALYSIS: RUNNING TOTALS
-----------------------------------------------------------------------------------

-- First, aggregate sales per month in a subquery.
-- Then use a window function SUM() OVER to calculate the running total over time.
SELECT 
	order_month,
	total_sales,
	SUM(total_sales) OVER (ORDER BY order_date) AS running_total_sales
FROM (
	SELECT 
		TO_CHAR(DATE_TRUNC('month', order_date), 'YYYY-MM-DD') AS order_month,
		SUM(sales_amount) AS total_sales,
		DATE_TRUNC('month', order_date) AS order_date
	FROM gold.fact_sales
	WHERE order_date IS NOT NULL
	GROUP BY DATE_TRUNC('month', order_date)
) t;

-----------------------------------------------------------------------------------
-- RUNNING TOTALS PER YEAR
-----------------------------------------------------------------------------------

-- Similar approach but aggregate by year.
-- Gives you cumulative yearly performance across multiple years.
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

-----------------------------------------------------------------------------------
-- RUNNING TOTALS PLUS RUNNING AVERAGE OF PRICE
-----------------------------------------------------------------------------------

-- Combines running total of sales with a running average of price.
-- Shows not only growth but also pricing trends over time.
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

-----------------------------------------------------------------------------------
-- YEARLY PRODUCT PERFORMANCE (COMPARE TO AVERAGE AND PREVIOUS YEAR)
-----------------------------------------------------------------------------------

-- CTE: Summarize each product’s sales per year
WITH yearly_product_sales AS (
  SELECT 
    TO_CHAR(DATE_TRUNC('year', order_date), 'YYYY') AS order_year,
	p.product_name,
	SUM(f.sales_amount) AS total_sales
  FROM gold.fact_sales f
  LEFT JOIN gold.dim_products p ON f.product_key = p.product_key
  WHERE f.order_date IS NOT NULL
  GROUP BY DATE_TRUNC('year', order_date), p.product_name
)

-- Final query: analyze performance
SELECT 
  order_year,
  product_name,
  total_sales,
  -- historical average for each product
  AVG(total_sales) OVER (PARTITION BY product_name) AS avg_sales,
  -- difference from the average
  total_sales - AVG(total_sales) OVER (PARTITION BY product_name) AS diff_avg,
  -- label above or below average
  CASE 
    WHEN total_sales - AVG(total_sales) OVER (PARTITION BY product_name) > 0 THEN 'above AVG'
    WHEN total_sales - AVG(total_sales) OVER (PARTITION BY product_name) < 0 THEN 'below AVG'
    ELSE 'AVG'
  END AS avg_change,
  -- previous year’s sales
  LAG(total_sales) OVER (PARTITION BY product_name ORDER BY order_year) AS py_sales,
  -- difference from previous year
  total_sales - LAG(total_sales) OVER (PARTITION BY product_name ORDER BY order_year) AS diff_py_sales,
  -- label growth or decline
  CASE 
    WHEN total_sales - LAG(total_sales) OVER (PARTITION BY product_name ORDER BY order_year) > 0 THEN 'increase'
    WHEN total_sales - LAG(total_sales) OVER (PARTITION BY product_name ORDER BY order_year) < 0 THEN 'decrease'
    ELSE 'same as last year'
  END AS py_change
FROM yearly_product_sales
ORDER BY product_name, order_year;

-----------------------------------------------------------------------------------
-- CATEGORY CONTRIBUTION TO TOTAL SALES
-----------------------------------------------------------------------------------

-- First, calculate total sales by category in a CTE.
WITH category_sales AS (
  SELECT 
    p.category,
    SUM(f.sales_amount) AS total_sales
  FROM gold.fact_sales f
  LEFT JOIN gold.dim_products p ON f.product_key = p.product_key
  GROUP BY category
)

-- Then calculate each category’s contribution as a percentage of overall sales.
SELECT 
  category,
  total_sales,
  SUM(total_sales) OVER () AS overall_sales,
  CONCAT(
    ROUND(CAST(total_sales AS NUMERIC) / SUM(total_sales) OVER () * 100, 2),
    '%'
  ) AS percentage_of_total
FROM category_sales;

-----------------------------------------------------------------------------------
-- SEGMENT PRODUCTS BY COST RANGES
-----------------------------------------------------------------------------------

-- Bucket products into ranges based on cost.
-- This lets us quickly see product mix across price segments.
WITH product_segment AS (
  SELECT 
    product_key,
    product_name,
    cost,
    CASE 
      WHEN cost < 100 THEN 'Below 100'
      WHEN cost BETWEEN 100 AND 500 THEN '100-500'
      WHEN cost BETWEEN 500 AND 1000 THEN '500-1000'
      ELSE 'Above 1000'
    END AS cost_range
  FROM gold.dim_products
)

-- Count how many products are in each range.
SELECT 
  cost_range,
  COUNT(product_key) AS total_products
FROM product_segment
GROUP BY cost_range
ORDER BY total_products DESC;

-----------------------------------------------------------------------------------
-- CUSTOMER SEGMENTATION BASED ON LIFESPAN AND SPENDING
-----------------------------------------------------------------------------------

-- CTE: For each customer, calculate total spending, first order, last order, and lifespan in months.
WITH customer_spending AS (
  SELECT
    c.customer_key,
    SUM(f.sales_amount) AS total_spending,
    MIN(order_date) AS first_order,
    MAX(order_date) AS last_order,
    (DATE_PART('year', AGE(MAX(order_date), MIN(order_date))) * 12 +
     DATE_PART('month', AGE(MAX(order_date), MIN(order_date)))) AS lifespan_months
  FROM gold.fact_sales f
  LEFT JOIN gold.dim_customers c ON f.customer_key = c.customer_key
  GROUP BY c.customer_key
)

-- Final classification:
-- VIP: active >12 months and spending >5000
-- Regular: active >=12 months and spending <=5000
-- New: active <12 months
SELECT 
  customer_key,
  CASE 
    WHEN lifespan_months > 12 AND total_spending > 5000 THEN 'VIP'
    WHEN lifespan_months >= 12 AND total_spending <= 5000 THEN 'Regular Customer'
    ELSE 'New'
  END AS customer_segment
FROM customer_spending;

-----------------------------------------------------------------------------------
-- COUNT CUSTOMERS IN EACH SEGMENT
-----------------------------------------------------------------------------------

WITH customer_spending AS (
  SELECT
    c.customer_key,
    SUM(f.sales_amount) AS total_spending,
    MIN(order_date) AS first_order,
    MAX(order_date) AS last_order,
    (DATE_PART('year', AGE(MAX(order_date), MIN(order_date))) * 12 +
     DATE_PART('month', AGE(MAX(order_date), MIN(order_date)))) AS lifespan_months
  FROM gold.fact_sales f
  LEFT JOIN gold.dim_customers c ON f.customer_key = c.customer_key
  GROUP BY c.customer_key
)

-- Count how many customers fall into each segment for a quick summary.
SELECT 
  customer_segment,
  COUNT(customer_key)
FROM (
  SELECT 
    customer_key,
    CASE 
      WHEN lifespan_months > 12 AND total_spending > 5000 THEN 'VIP'
      WHEN lifespan_months >= 12 AND total_spending <= 5000 THEN 'Regular Customer'
      ELSE 'New'
    END AS customer_segment
  FROM customer_spending
) t
GROUP BY customer_segment;
