/* ============================================================
   Project: Superstore Data Warehouse
   Layer: Bronze (Raw Data)
   Table: bronze.superstore_sales
   Purpose:
   - Store raw Superstore sales data exactly as received
   - No transformations or data cleaning applied
   - Acts as a source-of-truth for reprocessing and auditing
   ============================================================ */

-- ============================================================
-- Create Bronze Table (Raw Ingestion)
-- All columns are stored as VARCHAR to preserve original format
-- ============================================================
CREATE TABLE bronze.superstore_sales (
    order_id        VARCHAR(MAX),
    order_date      VARCHAR(MAX),
    ship_date       VARCHAR(MAX),
    ship_mode       VARCHAR(MAX),
    customer_name   VARCHAR(MAX),
    segment         VARCHAR(MAX),
    state           VARCHAR(MAX),
    country         VARCHAR(MAX),
    market          VARCHAR(MAX),
    region          VARCHAR(MAX),
    product_id      VARCHAR(MAX),
    category        VARCHAR(MAX),
    sub_category    VARCHAR(MAX),
    product_name    VARCHAR(MAX),
    sales           VARCHAR(MAX),
    quantity        VARCHAR(MAX),
    discount        VARCHAR(MAX),
    profit          VARCHAR(MAX),
    shipping_cost   VARCHAR(MAX),
    order_priority  VARCHAR(MAX),
    year            VARCHAR(MAX)
);

-- ============================================================
-- Load CSV Data into Bronze Table using BULK INSERT
-- Notes:
-- - FIRSTROW = 2 skips header row
-- - FIELDQUOTE handles quoted fields in CSV
-- - CODEPAGE 65001 supports UTF-8 encoding
-- ============================================================
BULK INSERT bronze.superstore_sales
FROM 'C:\Users\Mo\Documents\SQL Server Management Studio\SuperStoreOrders.csv'
WITH (
    FIRSTROW = 2,
    FIELDTERMINATOR = ',',
    ROWTERMINATOR = '\n',
    FIELDQUOTE = '"',
    CODEPAGE = '65001'
);

-- ============================================================
-- Validation Queries
-- Purpose: Ensure data was loaded successfully
-- ============================================================

-- Total number of rows loaded
SELECT COUNT(*) 
FROM bronze.superstore_sales;

-- Preview sample records
SELECT TOP 10 * 
FROM bronze.superstore_sales;
