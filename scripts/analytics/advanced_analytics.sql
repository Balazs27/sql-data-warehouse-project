--Advanced Analytics Project

--Step 1: Changes Over Time Analysis
--We analyze how a measure evolves over time
--It helps us track trends and seasonality in our data
--Formula: AGG[Measure] By [Date Dimension] - for example: Total Sales by Year
--Let's look at some key metrics over the years
SELECT
    EXTRACT(year FROM order_date) AS order_year,
    SUM(sales_amount) AS total_sales,
    COUNT(DISTINCT customer_key) AS total_customers,
    SUM(quantity) AS total_quantity
FROM fact_sales
WHERE order_date IS NOT NULL
GROUP BY EXTRACT(year FROM order_date)
ORDER BY EXTRACT(year FROM order_date);

--Now we can look at these metrics over the years but on a monthly split
SELECT
    EXTRACT(year FROM order_date) AS order_year,
    EXTRACT(month FROM order_date) AS order_month,
    SUM(sales_amount) AS total_sales,
    COUNT(DISTINCT customer_key) AS total_customers,
    SUM(quantity) AS total_quantity
FROM fact_sales
WHERE order_date IS NOT NULL
GROUP BY EXTRACT(year FROM order_date), EXTRACT(month FROM order_date)
ORDER BY EXTRACT(year FROM order_date), EXTRACT(month FROM order_date);

--We could check only 1 year(2012):
SELECT
    EXTRACT(year FROM order_date) AS order_year,
    EXTRACT(month FROM order_date) AS order_month,
    SUM(sales_amount) AS total_sales,
    COUNT(DISTINCT customer_key) AS total_customers,
    SUM(quantity) AS total_quantity
FROM fact_sales
WHERE order_date IS NOT NULL AND EXTRACT(year FROM order_date) = 2012
GROUP BY EXTRACT(year FROM order_date), EXTRACT(month FROM order_date)
ORDER BY EXTRACT(year FROM order_date), EXTRACT(month FROM order_date);


--Step 2: Cumulative Analysis
--We aggregate the data progressively over time
--It helps us understand whether our business is growing or declining
--Formula: AGG[Cumulative Measure] By [Date Dimension]
--For example: Running Total Sales By Year or Moving Average of Sales By Month
--Task: Calculate the total sales per month and the running total of sales over time
SELECT
    CAST(DATE_TRUNC('month', order_date) AS DATE) AS order_month,
    SUM(sales_amount) AS total_sales
FROM fact_sales
WHERE order_date IS NOT NULL
GROUP BY DATE_TRUNC('month', order_date)
ORDER BY DATE_TRUNC('month', order_date);
--Now we are going to calculate the running total of sales over time (With Subquery):
SELECT
    order_date,
    total_sales,
    SUM(total_sales) OVER (ORDER BY order_date) AS running_total_month
FROM (
    SELECT
        CAST(DATE_TRUNC('month', order_date) AS DATE) AS order_date,
        SUM(sales_amount) AS total_sales
    FROM fact_sales
    WHERE order_date IS NOT NULL
    GROUP BY DATE_TRUNC('month', order_date)
    ORDER BY DATE_TRUNC('month', order_date)) t;
--Now we are going to calculate the running total of sales over time (With CTE):
WITH total_monthly_sales AS (
    SELECT
        CAST(DATE_TRUNC('month', order_date) AS DATE) AS order_date,
        SUM(sales_amount) AS total_sales
    FROM fact_sales
    WHERE order_date IS NOT NULL
    GROUP BY DATE_TRUNC('month', order_date)
    ORDER BY DATE_TRUNC('month', order_date)
)
SELECT
    order_date,
    total_sales,
    SUM(total_sales) OVER (ORDER BY order_date) AS running_total_month
FROM total_monthly_sales;

--We can now look at the cumulative sales over a year, so we partition our data by year (With CTE):
WITH total_monthly_sales AS (
    SELECT
        CAST(DATE_TRUNC('month', order_date) AS DATE) AS order_date,
        SUM(sales_amount) AS total_sales
    FROM fact_sales
    WHERE order_date IS NOT NULL
    GROUP BY DATE_TRUNC('month', order_date)
    ORDER BY DATE_TRUNC('month', order_date)
)
SELECT
    order_date,
    total_sales,
    SUM(total_sales) OVER (PARTITION BY EXTRACT(year FROM order_date) ORDER BY order_date) AS running_total_month
FROM total_monthly_sales;

--We can also include the moving average of the price in our analysis (With CTE):
WITH total_monthly_sales AS (
    SELECT
        CAST(DATE_TRUNC('month', order_date) AS DATE) AS order_date,
        SUM(sales_amount) AS total_sales,
        ROUND(AVG(price), 2) AS average_price
    FROM fact_sales
    WHERE order_date IS NOT NULL
    GROUP BY DATE_TRUNC('month', order_date)
    ORDER BY DATE_TRUNC('month', order_date)
)
SELECT
    order_date,
    total_sales,
    ROUND(average_price, 2),
    SUM(total_sales) OVER (PARTITION BY EXTRACT(year FROM order_date) ORDER BY order_date) AS running_total_month,
    AVG(ROUND(average_price, 2)) OVER (PARTITION BY EXTRACT(year FROM order_date) ORDER BY order_date) AS moving_average_price
FROM total_monthly_sales;


--Step 3: Performance Analysis
--Comparing the current value to a target value
--Helps measure success and compare performance
--Formula: Current[Measure] - Target[Measure]
--For Example: Current Sales - Average Sales or Current Year Sales - Previous Year Sales
/* Task: Analyze the yearly performance of products by comparing their sales to
   both the average sales performance of the product and the previous year's sales. */
--Step 1: Calculating the Yearly Performance of Products
SELECT
    EXTRACT(year FROM s.order_date) AS order_year,
    p.product_name,
    SUM(s.sales_amount) AS total_revenue
FROM fact_sales s
LEFT JOIN dim_products p
ON s.product_key = p.product_key
WHERE EXTRACT(year FROM s.order_date) IS NOT NULL
GROUP BY p.product_name, EXTRACT(year FROM order_date);

--Step 2: Comparing the yearly performance of the products to their average average sales performance
WITH yearly_product_sales AS (
    SELECT
        EXTRACT(year FROM s.order_date) AS order_year,
        p.product_name,
        SUM(s.sales_amount) AS total_revenue
    FROM fact_sales s
    LEFT JOIN dim_products p
    ON s.product_key = p.product_key
    WHERE EXTRACT(year FROM s.order_date) IS NOT NULL
    GROUP BY p.product_name, EXTRACT(year FROM order_date)
)
SELECT
    order_year,
    product_name,
    total_revenue,
    AVG(ROUND(total_revenue, 2)) OVER (PARTITION BY product_name) AS avg_sales,
    total_revenue - AVG(ROUND(total_revenue, 2)) OVER (PARTITION BY product_name) AS diff_avg,
    CASE
        WHEN total_revenue - AVG(ROUND(total_revenue, 2)) OVER (PARTITION BY product_name) > 0 THEN 'Above Avg'
        WHEN total_revenue - AVG(ROUND(total_revenue, 2)) OVER (PARTITION BY product_name) < 0 THEN 'Below Avg'
        ELSE 'Avg'
    END AS avg_change
FROM yearly_product_sales;

--Step 3: Comparing the yearly performance of the products to the previous year's sales performance
WITH yearly_product_sales AS (
    SELECT
        EXTRACT(year FROM s.order_date) AS order_year,
        p.product_name,
        SUM(s.sales_amount) AS total_revenue
    FROM fact_sales s
    LEFT JOIN dim_products p
    ON s.product_key = p.product_key
    WHERE EXTRACT(year FROM s.order_date) IS NOT NULL
    GROUP BY p.product_name, EXTRACT(year FROM order_date)
)
SELECT
    order_year,
    product_name,
    total_revenue,
    AVG(ROUND(total_revenue, 2)) OVER (PARTITION BY product_name) AS avg_sales,
    total_revenue - AVG(ROUND(total_revenue, 2)) OVER (PARTITION BY product_name) AS diff_avg,
    CASE
        WHEN total_revenue - AVG(ROUND(total_revenue, 2)) OVER (PARTITION BY product_name) > 0 THEN 'Above Avg'
        WHEN total_revenue - AVG(ROUND(total_revenue, 2)) OVER (PARTITION BY product_name) < 0 THEN 'Below Avg'
        ELSE 'Avg'
    END AS avg_change,
    --Year-Over-Year Analysis
    LAG(total_revenue) OVER(PARTITION BY product_name ORDER BY order_year) AS previous_year_sales,
    total_revenue - LAG(total_revenue) OVER(PARTITION BY product_name ORDER BY order_year) AS difference_previous_year,
    CASE
        WHEN total_revenue - LAG(total_revenue) OVER(PARTITION BY product_name ORDER BY order_year) > 0 THEN 'Increase'
        WHEN total_revenue - LAG(total_revenue) OVER(PARTITION BY product_name ORDER BY order_year) < 0 THEN 'Decrease'
        ELSE 'No change'
    END AS avg_change
FROM yearly_product_sales;


--Step 4: Part to Whole Analysis
/* Analyze how an individual part is performing compared to the overall,
   allowing us to understand which category has the greatest impact on the business.
   Formula: ([Measure] / Total[Measure]) * 100 By [Dimension]
   For example: (Sales / Total Sales) * 100 By Category
   A: 200, B: 300, C: 100
   A: 33% B: 50%, C: 17% of the total sales which is 600
 */
--Which Categories contribute the most to overall sales:
WITH total_revenue_by_category AS (
SELECT
    p.category,
    SUM(s.sales_amount) AS total_sales_by_category
FROM fact_sales s
LEFT JOIN dim_products p
ON s.product_key = p.product_key
GROUP BY p.category
)
SELECT
    category,
    total_sales_by_category,
    SUM(total_sales_by_category) OVER () AS total_sales,
    ROUND(
        (total_sales_by_category / SUM(total_sales_by_category) OVER ()) * 100,
        2
    ) || '%' AS total_contribution
FROM total_revenue_by_category
ORDER BY total_sales_by_category DESC;


--Step 5: Data Segmentation
--Group the data based on a specific range
--Helps us understand the correlation between two measures
--Formula: [Measure] By [Measure]
--For example: Total Products By Sales Range or Total Customers By Age
--Task: Segment products into cost ranges and count how many products fall into each segment
SELECT
    product_key,
    product_name,
    product_cost,
    CASE
        WHEN product_cost < 100 THEN 'Below 100'
        WHEN product_cost BETWEEN 100 AND 500 THEN '100-500'
        WHEN product_cost BETWEEN 501 AND 1000 THEN '501-1000'
        ELSE 'Above 1000'
    END AS cost_range
FROM dim_products;

--Now let's calculate the product count in each cost range
WITH product_segments AS (
    SELECT
    product_key,
    product_name,
    product_cost,
    CASE
        WHEN product_cost < 100 THEN 'Below 100'
        WHEN product_cost BETWEEN 100 AND 500 THEN '100-500'
        WHEN product_cost BETWEEN 501 AND 1000 THEN '501-1000'
        ELSE 'Above 1000'
    END AS cost_range
    FROM dim_products
    )
SELECT
    cost_range,
    COUNT(product_key) AS total_products
FROM product_segments
GROUP BY cost_range
ORDER BY total_products DESC;

/* Group Customers into 3 segments based on their spending behavior:
   VIP: Customers with at least 12 months of history and spending more than €5.000
   Regular: Customers with at least 12 months of history but spending €5.000 or less
   New: Customers with a lifespan less than 12 months
   And find the total number of customers by each group
 */
SELECT
    c.customer_id,
    SUM(s.sales_amount) AS total_spending,
    MIN(s.order_date) AS first_order,
    MAX(s.order_date) AS last_order
FROM fact_sales s
LEFT JOIN dim_customers c
ON s.customer_key = c.customer_key
GROUP BY c.customer_id;

--Now we can calculate the lifespan of the customer by subtracting the first order from the last order
WITH total_customer_sepnding AS (
    SELECT
        c.customer_id,
        SUM(s.sales_amount) AS total_spending,
        MIN(s.order_date) AS first_order,
        MAX(s.order_date) AS last_order
    FROM fact_sales s
    LEFT JOIN dim_customers c
    ON s.customer_key = c.customer_key
    GROUP BY c.customer_id
)
SELECT
    customer_id,
    total_spending,
    first_order,
    last_order,
    (EXTRACT(YEAR FROM last_order) - EXTRACT(YEAR FROM first_order)) * 12 +
    (EXTRACT(MONTH FROM last_order) - EXTRACT(MONTH FROM first_order)) AS customer_lifespan_months
FROM total_customer_sepnding;

--And now we can also create our customer segments
WITH total_customer_sepnding AS (
    SELECT
        c.customer_id,
        SUM(s.sales_amount) AS total_spending,
        MIN(s.order_date) AS first_order,
        MAX(s.order_date) AS last_order
    FROM fact_sales s
    LEFT JOIN dim_customers c
    ON s.customer_key = c.customer_key
    GROUP BY c.customer_id
), customer_lifespan AS (
    SELECT
        customer_id,
        total_spending,
        first_order,
        last_order,
        (EXTRACT(YEAR FROM last_order) - EXTRACT(YEAR FROM first_order)) * 12 +
        (EXTRACT(MONTH FROM last_order) - EXTRACT(MONTH FROM first_order)) AS customer_lifespan_months
    FROM total_customer_sepnding
)
SELECT
    customer_id,
    total_spending,
    customer_lifespan_months,
    CASE
        WHEN customer_lifespan_months >= 12 AND total_spending > 5000 THEN 'VIP'
        WHEN customer_lifespan_months >= 12 AND total_spending <= 5000 THEN 'Regular'
        ELSE'New'
    END AS customer_segment
FROM customer_lifespan;

--And now we can find the total number of customers by each segment
WITH total_customer_sepnding AS (
    SELECT
        c.customer_id,
        SUM(s.sales_amount) AS total_spending,
        MIN(s.order_date) AS first_order,
        MAX(s.order_date) AS last_order
    FROM fact_sales s
    LEFT JOIN dim_customers c
    ON s.customer_key = c.customer_key
    GROUP BY c.customer_id
), customer_lifespan AS (
    SELECT
        customer_id,
        total_spending,
        first_order,
        last_order,
        (EXTRACT(YEAR FROM last_order) - EXTRACT(YEAR FROM first_order)) * 12 +
        (EXTRACT(MONTH FROM last_order) - EXTRACT(MONTH FROM first_order)) AS customer_lifespan_months
    FROM total_customer_sepnding
), customer_segments AS (
    SELECT
        customer_id,
        total_spending,
        customer_lifespan_months,
        CASE
            WHEN customer_lifespan_months >= 12 AND total_spending > 5000 THEN 'VIP'
            WHEN customer_lifespan_months >= 12 AND total_spending <= 5000 THEN 'Regular'
            ELSE'New'
        END AS customer_segment
    FROM customer_lifespan
)
SELECT
    customer_segment,
    COUNT(customer_id) AS total_customers
FROM customer_segments
GROUP BY customer_segment
ORDER BY total_customers DESC;


--Step 6: Build Customer Report
/*

============================================================================
 Customer Report
============================================================================
Purpose:
    -This report consolidates key customer metrics and behaviors

Highlights:
    1) Gathers essential fields such as names, ages, and transaction details
    2) Segments customers into categories (VIP, Regular, New) and age groups
    3) Aggregates customer-level metrics:
        -total orders
        -total sales
        -total quantity purchased
        -total products
        -lifespan (in months)
    4) Calculates Valuable KPIs:
        -recency (months since last order)
        -average order value --> total sales / total nr. of orders
        -average monthly spend --> total sales / nr. of months
============================================================================
 */
--Step 1: Base Query: Retrieves core columns from table
SELECT
    f.order_number,
    f.product_key,
    f.order_date,
    f.sales_amount,
    f.quantity,
    c.customer_key,
    c.customer_number,
    CONCAT(c.first_name, ' ', c.last_name) AS customer_name,
    EXTRACT(YEAR FROM CURRENT_DATE) - EXTRACT(YEAR FROM birthdate) AS customer_age
FROM fact_sales f
LEFT JOIN dim_customers c
ON f.customer_key = c.customer_key
WHERE f.order_date IS NOT NULL;

--Step 2: Aggregating customer-level metrics
    WITH base_query AS (
        SELECT
        f.order_number,
        f.product_key,
        f.order_date,
        f.sales_amount,
        f.quantity,
        c.customer_key,
        c.customer_number,
        CONCAT(c.first_name, ' ', c.last_name) AS customer_name,
        EXTRACT(YEAR FROM CURRENT_DATE) - EXTRACT(YEAR FROM birthdate) AS customer_age
    FROM fact_sales f
    LEFT JOIN dim_customers c
    ON f.customer_key = c.customer_key
    WHERE f.order_date IS NOT NULL
)
    SELECT
        customer_key,
        customer_number,
        customer_name,
        customer_age,
        COUNT(DISTINCT order_number) AS total_orders,
        SUM(sales_amount) AS total_sales,
        SUM(quantity) AS total_quantity,
        COUNT(DISTINCT product_key) AS total_products,
        MAX(order_date) AS last_order_date,
        (EXTRACT(YEAR FROM MAX(order_date)) - EXTRACT(YEAR FROM MIN(order_date))) * 12 +
        (EXTRACT(MONTH FROM MAX(order_date)) - EXTRACT(MONTH FROM MIN(order_date))) AS customer_lifespan_months
    FROM base_query
    GROUP BY customer_key, customer_number, customer_name, customer_age;

--Step 3: Segmenting our customers
 WITH base_query AS (
        SELECT
        f.order_number,
        f.product_key,
        f.order_date,
        f.sales_amount,
        f.quantity,
        c.customer_key,
        c.customer_number,
        CONCAT(c.first_name, ' ', c.last_name) AS customer_name,
        EXTRACT(YEAR FROM CURRENT_DATE) - EXTRACT(YEAR FROM birthdate) AS customer_age
    FROM fact_sales f
    LEFT JOIN dim_customers c
    ON f.customer_key = c.customer_key
    WHERE f.order_date IS NOT NULL
), aggregated_metrics AS (
    SELECT
        customer_key,
        customer_number,
        customer_name,
        customer_age,
        COUNT(DISTINCT order_number) AS total_orders,
        SUM(sales_amount) AS total_sales,
        SUM(quantity) AS total_quantity,
        COUNT(DISTINCT product_key) AS total_products,
        MAX(order_date) AS last_order_date,
        (EXTRACT(YEAR FROM MAX(order_date)) - EXTRACT(YEAR FROM MIN(order_date))) * 12 +
        (EXTRACT(MONTH FROM MAX(order_date)) - EXTRACT(MONTH FROM MIN(order_date))) AS customer_lifespan_months
    FROM base_query
    GROUP BY customer_key, customer_number, customer_name, customer_age
 )
 SELECT
     customer_key,
     customer_number,
     customer_name,
     customer_age,
     CASE
         WHEN customer_age < 20 THEN 'Under 20'
         WHEN customer_age BETWEEN 20 AND 30 THEN '20-30'
         WHEN customer_age BETWEEN 31 AND 40 THEN '31-40'
         WHEN customer_age BETWEEN 41 AND 50 THEN '41-50'
         ELSE '51 and above'
    END AS age_group,
    CASE
        WHEN customer_lifespan_months >= 12 AND total_sales > 5000 THEN 'VIP'
        WHEN customer_lifespan_months >= 12 AND total_sales <= 5000 THEN 'Regular'
        ELSE'New'
    END AS customer_segment,
    total_orders,
    total_sales,
    total_quantity,
    total_products,
    last_order_date,
    customer_lifespan_months
 FROM aggregated_metrics;

--Step 4: Calculating the Valuable KPIs:
WITH base_query AS (
        SELECT
        f.order_number,
        f.product_key,
        f.order_date,
        f.sales_amount,
        f.quantity,
        c.customer_key,
        c.customer_number,
        CONCAT(c.first_name, ' ', c.last_name) AS customer_name,
        EXTRACT(YEAR FROM CURRENT_DATE) - EXTRACT(YEAR FROM birthdate) AS customer_age
    FROM fact_sales f
    LEFT JOIN dim_customers c
    ON f.customer_key = c.customer_key
    WHERE f.order_date IS NOT NULL
), aggregated_metrics AS (
    SELECT
        customer_key,
        customer_number,
        customer_name,
        customer_age,
        COUNT(DISTINCT order_number) AS total_orders,
        SUM(sales_amount) AS total_sales,
        SUM(quantity) AS total_quantity,
        COUNT(DISTINCT product_key) AS total_products,
        MAX(order_date) AS last_order_date,
        (EXTRACT(YEAR FROM MAX(order_date)) - EXTRACT(YEAR FROM MIN(order_date))) * 12 +
        (EXTRACT(MONTH FROM MAX(order_date)) - EXTRACT(MONTH FROM MIN(order_date))) AS customer_lifespan_months
    FROM base_query
    GROUP BY customer_key, customer_number, customer_name, customer_age
 ), customer_segments AS (
 SELECT
     customer_key,
     customer_number,
     customer_name,
     customer_age,
     CASE
         WHEN customer_age < 20 THEN 'Under 20'
         WHEN customer_age BETWEEN 20 AND 30 THEN '20-30'
         WHEN customer_age BETWEEN 31 AND 40 THEN '31-40'
         WHEN customer_age BETWEEN 41 AND 50 THEN '41-50'
         ELSE '51 and above'
    END AS age_group,
    CASE
        WHEN customer_lifespan_months >= 12 AND total_sales > 5000 THEN 'VIP'
        WHEN customer_lifespan_months >= 12 AND total_sales <= 5000 THEN 'Regular'
        ELSE'New'
    END AS customer_segment,
    total_orders,
    total_sales,
    total_quantity,
    total_products,
    last_order_date,
    customer_lifespan_months
 FROM aggregated_metrics
)
SELECT
    customer_key,
    customer_number,
    customer_name,
    customer_age,
    age_group,
    customer_segment,
    last_order_date,
    (EXTRACT(YEAR FROM CURRENT_DATE) - EXTRACT(YEAR FROM last_order_date)) * 12 +
    EXTRACT(MONTH FROM CURRENT_DATE) - EXTRACT(MONTH FROM last_order_date) AS recency,
    total_orders,
    total_sales,
    total_quantity,
    total_products,
    customer_lifespan_months,
    CASE
        WHEN total_sales = 0 THEN 0
        ELSE total_sales / total_orders
    END AS average_order_value,
    CASE
        WHEN customer_lifespan_months = 0 THEN total_sales
        ELSE ROUND(total_sales / customer_lifespan_months, 2)
    END AS average_monthly_spend
FROM customer_segments;

--Step 5: We create a View out of our Query so then we can share it with other Analysts and can be used in a dashboard
CREATE VIEW report_customers AS
    WITH base_query AS (
        SELECT
        f.order_number,
        f.product_key,
        f.order_date,
        f.sales_amount,
        f.quantity,
        c.customer_key,
        c.customer_number,
        CONCAT(c.first_name, ' ', c.last_name) AS customer_name,
        EXTRACT(YEAR FROM CURRENT_DATE) - EXTRACT(YEAR FROM birthdate) AS customer_age
    FROM fact_sales f
    LEFT JOIN dim_customers c
    ON f.customer_key = c.customer_key
    WHERE f.order_date IS NOT NULL
), aggregated_metrics AS (
    SELECT
        customer_key,
        customer_number,
        customer_name,
        customer_age,
        COUNT(DISTINCT order_number) AS total_orders,
        SUM(sales_amount) AS total_sales,
        SUM(quantity) AS total_quantity,
        COUNT(DISTINCT product_key) AS total_products,
        MAX(order_date) AS last_order_date,
        (EXTRACT(YEAR FROM MAX(order_date)) - EXTRACT(YEAR FROM MIN(order_date))) * 12 +
        (EXTRACT(MONTH FROM MAX(order_date)) - EXTRACT(MONTH FROM MIN(order_date))) AS customer_lifespan_months
    FROM base_query
    GROUP BY customer_key, customer_number, customer_name, customer_age
 ), customer_segments AS (
 SELECT
     customer_key,
     customer_number,
     customer_name,
     customer_age,
     CASE
         WHEN customer_age < 20 THEN 'Under 20'
         WHEN customer_age BETWEEN 20 AND 30 THEN '20-30'
         WHEN customer_age BETWEEN 31 AND 40 THEN '31-40'
         WHEN customer_age BETWEEN 41 AND 50 THEN '41-50'
         ELSE '51 and above'
    END AS age_group,
    CASE
        WHEN customer_lifespan_months >= 12 AND total_sales > 5000 THEN 'VIP'
        WHEN customer_lifespan_months >= 12 AND total_sales <= 5000 THEN 'Regular'
        ELSE'New'
    END AS customer_segment,
    total_orders,
    total_sales,
    total_quantity,
    total_products,
    last_order_date,
    customer_lifespan_months
 FROM aggregated_metrics
)
SELECT
    customer_key,
    customer_number,
    customer_name,
    customer_age,
    age_group,
    customer_segment,
    last_order_date,
    (EXTRACT(YEAR FROM CURRENT_DATE) - EXTRACT(YEAR FROM last_order_date)) * 12 +
    EXTRACT(MONTH FROM CURRENT_DATE) - EXTRACT(MONTH FROM last_order_date) AS recency,
    total_orders,
    total_sales,
    total_quantity,
    total_products,
    customer_lifespan_months,
    CASE
        WHEN total_sales = 0 THEN 0
        ELSE total_sales / total_orders
    END AS average_order_value,
    CASE
        WHEN customer_lifespan_months = 0 THEN total_sales
        ELSE ROUND(total_sales / customer_lifespan_months, 2)
    END AS average_monthly_spend
FROM customer_segments;

--Checking our View
SELECT * FROM report_customers;

--Doing a quick analysis on top of the view
--Total sales and customers by age group
SELECT
    age_group,
    COUNT(customer_number) AS total_customers,
    SUM(total_sales) AS total_sales
FROM report_customers
GROUP BY age_group;
--Total sales and customers by customer segment
SELECT
    customer_segment,
    COUNT(customer_number) AS total_customers,
    SUM(total_sales) AS total_sales
FROM report_customers
GROUP BY customer_segment;
