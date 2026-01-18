# SQL-Project-superstore-sales
“I built a data warehouse using SQL Server with Medallion Architecture. Bronze layer stores raw data, Silver layer cleans and validates it, and Gold layer implements a star schema optimized for analytics.”

# Superstore Data Warehouse (SQL Server)

## Overview
This project builds a **Data Warehouse** using **SQL Server** and the **Medallion Architecture**:
**Bronze → Silver → Gold**.

The goal is to clean raw sales data and create an **analytics-ready Star Schema**.

---

## Architecture
- **Bronze**: Raw data (no changes)
- **Silver**: Cleaned and standardized data
- **Gold**: Star Schema (Fact & Dimension tables)

---

## Gold Layer Model
- **Fact Table**: `fact_sales`
- **Dimensions**:
  - `dim_date`
  - `dim_customer`
  - `dim_product`
  - `dim_location`
  - `dim_ship_mode`

---

## Data Quality
- Valid sales only
- Shipping date ≥ order date
- No duplicate dimension records
- Foreign key validation

---

## Tools
- SQL Server
- T-SQL
- Star Schema Modeling

---

## Author
Mohammed – Data Engineer
