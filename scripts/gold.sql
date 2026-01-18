-- Grain definition:
-- One row represents one product sold in a single order (order line level)

-- Dimension identification:
-- Customer, Product, Date, Location, and Ship Mode
-- provide descriptive context for sales transactions.


/* ============================================================
   Gold Layer - Dimension Table
   Name: dim_date
   Purpose:
   - Provide time-based attributes for analysis
   - One row per calendar date
   ============================================================ */


IF OBJECT_ID('gold.dim_date', 'U') IS NOT NULL
	DROP TABLE gold.dim_date;
GO

CREATE TABLE gold.dim_date (
	Date_key INT PRIMARY KEY,
	Full_date DATE ,
	Year INT,
	Month INT,
	Month_name VARCHAR(20),
	Quarter INT
);
GO


INSERT INTO gold.dim_date 
SELECT DISTINCT 
	CONVERT (INT ,FORMAT(Order_date,'yyyyMMdd')) AS Date_key,
	Order_date AS Full_date,
	YEAR(Order_date) AS Year,
	MONTH(Order_date) AS Month,
	DATENAME(MONTH ,Order_date) AS Month_name,
	DATEPART(QUARTER , Order_date) AS Quarter
FROM silver.Superstore_sales
WHERE Order_date IS NOT NULL ;
GO

SELECT * FROM gold.dim_date;
SELECT COUNT(*) FROM gold.dim_date;

/* ============================================================
   Gold Layer - Dimension Table
   Name: dim_customer
   Purpose:
   - Store descriptive customer attributes
   - Used to analyze sales by customer and segment
   ============================================================ */


IF OBJECT_ID ('gold.dim_customer' , 'U') IS NOT NULL
	DROP TABLE gold.dim_customer;
GO

CREATE TABLE gold.dim_customer (
	Customer_key INT IDENTITY(1,1) PRIMARY KEY,
	Customer_name VARCHAR(100),
	Segment VARCHAR(50),
	Country VARCHAR(50),
	Market VARCHAR(50)
);
GO


INSERT INTO gold.dim_customer (
	Customer_name,
	Segment,
	Country,
	Market
)
SELECT DISTINCT 
	Customer_name,
	Segment,
	Country,
	Market
FROM silver.Superstore_sales
WHERE Customer_name IS NOT NULL ;
GO

SELECT * FROM gold.dim_customer;
SELECT COUNT(*) FROM gold.dim_customer;

/* ============================================================
   Gold Layer - Dimension Table
   Name: dim_product
   Purpose:
   - Store descriptive product attributes
   - Enable analysis by product, category, and sub-category
   ============================================================ */


IF OBJECT_ID('gold.dim_product' , 'U') IS NOT NULL 
	DROP TABLE gold.dim_product;

CREATE TABLE gold.dim_product(
	Product_key INT IDENTITY(1,1) PRIMARY KEY,
	Product_id VARCHAR(50),
	Product_name VARCHAR(200),
	Category VARCHAR(50),
	Sub_category VARCHAR(50)
);
GO

INSERT INTO gold.dim_product(
	Product_id,
	Product_name,
	Category,
	Sub_category
)
SELECT DISTINCT 
	Product_id,
	Product_name,
	Category,
	Sub_category
FROM silver.Superstore_sales
WHERE Product_id IS NOT NULL ;
GO



SELECT TOP 10 * FROM gold.dim_product;
SELECT COUNT(*) FROM gold.dim_product;

/* ============================================================
   Gold Layer - Dimension Table
   Name: dim_location
   Purpose:
   - Store geographic attributes for sales analysis
   - Enable reporting by country, state, and region
   ============================================================ */


IF OBJECT_ID('gold.dim_location', 'U') IS NOT NULL
    DROP TABLE gold.dim_location;
GO

CREATE TABLE gold.dim_location (
    Location_key INT IDENTITY(1,1) PRIMARY KEY,  -- Surrogate key
    Country VARCHAR(50),
    State VARCHAR(50),
    Region VARCHAR(50)
);
GO

INSERT INTO gold.dim_location (
    Country,
    State,
    Region
)
SELECT DISTINCT
    Country,
    State,
    Region
FROM silver.Superstore_sales
WHERE Country IS NOT NULL;
GO


SELECT TOP 10 * FROM gold.dim_location;
SELECT COUNT(*) FROM gold.dim_location;



/* ============================================================
   Gold Layer - Dimension Table
   Name: dim_ship_mode
   Purpose:
   - Store shipping method attributes
   - Enable logistics and delivery performance analysis
   ============================================================ */



IF OBJECT_ID('gold.dim_ship_mode', 'U') IS NOT NULL
    DROP TABLE gold.dim_ship_mode;
GO

CREATE TABLE gold.dim_ship_mode (
    Ship_mode_key INT IDENTITY(1,1) PRIMARY KEY,  -- Surrogate key
    Ship_mode VARCHAR(50)
);
GO

INSERT INTO gold.dim_ship_mode (Ship_mode)
SELECT DISTINCT
    Ship_mode
FROM silver.Superstore_sales
WHERE Ship_mode IS NOT NULL;
GO

SELECT * FROM gold.dim_ship_mode;


/* ============================================================
   Gold Layer - Fact Table
   Name: fact_sales
   Grain:
   - One row represents one product sold in a single order
   Purpose:
   - Store measurable sales metrics
   - Join point for all dimension tables
   ============================================================ */

IF OBJECT_ID('gold.fact_sales' , 'U') IS NOT NULL
	DROP TABLE gold.fact_sales;
GO

CREATE TABLE gold.fact_sales(
	Sales_key INT IDENTITY(1,1) PRIMARY KEY ,

	--Business identifier
	Order_id VARCHAR(50),

	 -- Foreign Keys
    Date_key INT,
    Customer_key INT,
    Product_key INT,
    Location_key INT,
    Ship_mode_key INT,

	-- Measures
    Sales DECIMAL(12,2),
    Quantity INT,
    Discount DECIMAL(5,2),
    Profit DECIMAL(12,2),
    Shipping_cost DECIMAL(12,2),

	-- Data quality / business flags
    is_valid_sale BIT,
    is_valid_shipping_date BIT
);
GO

/* ============================================================
   Fact Load:
   - Replace natural keys with surrogate keys
   - Join Silver data to all dimensions
   - Apply business validation rules
   ============================================================ */


INSERT INTO gold.fact_sales(
	Order_id,
	Date_key,
	Customer_key,
	Product_key,
	Location_key,
	Ship_mode_key,
	Sales,
	Quantity,
	Discount,
	Profit,
	Shipping_cost,
	is_valid_sale,
	is_valid_shipping_date
)
SELECT
    s.Order_id,          -- 1
    d.Date_key,          -- 2
    c.Customer_key,      -- 3
    p.Product_key,       -- 4
    l.Location_key,      -- 5
    sm.Ship_mode_key,    -- 6
    s.Sales,             -- 7
    s.Quantity,          -- 8
    s.Discount,          -- 9
    s.Profit,            --10
    s.Shipping_cost,     --11
    CASE WHEN s.Sales > 0 THEN 1 ELSE 0 END,      --12
    CASE WHEN s.Ship_date >= s.Order_date THEN 1 ELSE 0 END --13
FROM silver.Superstore_sales s
LEFT JOIN gold.dim_date d
	ON s.Order_date = d.Full_date
LEFT JOIN gold.dim_customer c
	ON s.Customer_name = c.Customer_name
	AND s.Segment = c.Segment
	AND s.Country = c.Country
	AND s.Market = c.Market
LEFT JOIN gold.dim_product p
	ON s.Product_id = p.Product_id
LEFT JOIN gold.dim_location l
    ON s.Country = l.Country
   AND s.State = l.State
   AND s.Region = l.Region
LEFT JOIN gold.dim_ship_mode sm
    ON s.Ship_mode = sm.Ship_mode;
GO


-- Ensure no data loss between Silver and Gold
SELECT COUNT(*) FROM silver.Superstore_sales;
SELECT COUNT(*) FROM gold.fact_sales;

-- Check for missing foreign keys
SELECT *
FROM gold.fact_sales
WHERE date_key IS NULL
   OR customer_key IS NULL
   OR product_key IS NULL;

-- Aggregate validation
SELECT SUM(Sales) FROM silver.Superstore_sales;
SELECT SUM(sales) FROM gold.fact_sales;

-- Detect missing dimension relationships
SELECT *
FROM gold.fact_sales
WHERE date_key IS NULL
   OR customer_key IS NULL
   OR product_key IS NULL
   OR location_key IS NULL
   OR ship_mode_key IS NULL;


-- Customers duplicated
SELECT customer_name, segment, COUNT(*)
FROM gold.dim_customer
GROUP BY customer_name, segment
HAVING COUNT(*) > 1;


-- Products duplicated
SELECT product_id, COUNT(*)
FROM gold.dim_product
GROUP BY product_id
HAVING COUNT(*) > 1;


