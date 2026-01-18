/* ============================================================
   Layer: Silver (Cleaned & Standardized Data)
   Table: silver.Superstore_sales
   Purpose:
   - Convert raw Bronze data into strongly-typed, trusted data
   - Standardize date and numeric formats
   - Remove exact duplicate records
   - Preserve data quality issues for downstream (Gold) handling
   Platform: SQL Server
   ============================================================ */

-- ============================================================
-- Step 1: Drop & Recreate Silver Table (Idempotent Load)
-- Ensures consistent structure when rerunning the pipeline
-- ============================================================
IF OBJECT_ID('silver.Superstore_sales', 'U') IS NOT NULL
    DROP TABLE silver.Superstore_sales;
GO

CREATE TABLE silver.Superstore_sales (
    Order_id        VARCHAR(50),
    Order_date      DATE,
    Ship_date       DATE,
    Ship_mode       VARCHAR(50),
    Customer_name   VARCHAR(100),
    Segment         VARCHAR(50),
    State           VARCHAR(50),
    Country         VARCHAR(50),
    Market          VARCHAR(50),
    Region          VARCHAR(50),
    Product_id      VARCHAR(50),
    Category        VARCHAR(50),
    Sub_category    VARCHAR(50),
    Product_name    VARCHAR(200),
    Sales           DECIMAL(12,2),
    Quantity        INT,
    Discount        DECIMAL(5,2),
    Profit          DECIMAL(12,2),
    Shipping_cost   DECIMAL(12,2),
    Order_priority  VARCHAR(50),
    Order_year      INT
);
GO

/* ============================================================
   Step 2: Transform & Load Data from Bronze → Silver
   Applied Rules:
   - Safely parse multiple date formats
   - Convert numeric fields using TRY_CAST
   - Remove exact duplicate records
   - Filter records with invalid numeric Sales values
   ============================================================ */

INSERT INTO silver.Superstore_sales
SELECT DISTINCT
    Order_id,

    /* Order Date: safely handle mixed date formats */
    CASE
        WHEN TRY_CONVERT(DATE, Order_date, 101) IS NOT NULL
            THEN TRY_CONVERT(DATE, Order_date, 101)  -- MM/DD/YYYY
        WHEN TRY_CONVERT(DATE, Order_date, 103) IS NOT NULL
            THEN TRY_CONVERT(DATE, Order_date, 103)  -- DD/MM/YYYY
        WHEN TRY_CONVERT(DATE, Order_date, 120) IS NOT NULL
            THEN TRY_CONVERT(DATE, Order_date, 120)  -- YYYY-MM-DD
        ELSE NULL
    END AS Order_date,

    /* Ship Date: safely handle mixed date formats */
    CASE
        WHEN TRY_CONVERT(DATE, Ship_date, 101) IS NOT NULL
            THEN TRY_CONVERT(DATE, Ship_date, 101)
        WHEN TRY_CONVERT(DATE, Ship_date, 103) IS NOT NULL
            THEN TRY_CONVERT(DATE, Ship_date, 103)
        WHEN TRY_CONVERT(DATE, Ship_date, 120) IS NOT NULL
            THEN TRY_CONVERT(DATE, Ship_date, 120)
        ELSE NULL
    END AS Ship_date,

    Ship_mode,
    Customer_name,
    Segment,
    State,
    Country,
    Market,
    Region,
    Product_id,
    Category,
    Sub_category,
    Product_name,

    /* Numeric conversions */
    TRY_CAST(Sales AS DECIMAL(12,2))          AS Sales,
    TRY_CAST(Quantity AS INT)                 AS Quantity,
    TRY_CAST(Discount AS DECIMAL(5,2))        AS Discount,
    TRY_CAST(Profit AS DECIMAL(12,2))         AS Profit,
    TRY_CAST(Shipping_cost AS DECIMAL(12,2))  AS Shipping_cost,

    Order_priority,
    TRY_CAST(Year AS INT) AS Order_year
FROM bronze.Superstore_sales

-- Filter rows where Sales cannot be converted to a numeric value
-- (Invalid numeric data, not business logic)
WHERE TRY_CAST(Sales AS DECIMAL(12,2)) IS NOT NULL;
GO

/* ============================================================
   Step 3: Data Quality Validation (Read-Only Checks)
   Purpose:
   - Identify data anomalies
   - Do NOT fix or delete business-logic issues here
   ============================================================ */

-- Orders shipped before order date (business logic issue)
-- These records are intentionally kept for Gold-layer handling
SELECT *
FROM silver.Superstore_sales
WHERE Ship_date < Order_date;

-- Zero or negative sales (returns, refunds, promotions)
SELECT *
FROM silver.Superstore_sales
WHERE Sales <= 0;

-- Missing critical identifiers
SELECT *
FROM silver.Superstore_sales
WHERE Order_id IS NULL
   OR Product_id IS NULL;






