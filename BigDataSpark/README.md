# Distributed ETL Pipeline with Apache Spark and Analytical Data Warehousing

## Project Overview

This project implements a distributed ETL pipeline using Apache Spark to transform transactional data into a dimensional star schema and build analytical data marts for business reporting.

The pipeline demonstrates:

- Data modeling (fact & dimension design)
- Distributed data processing with Spark
- Analytical warehouse construction in PostgreSQL
- OLAP-style reporting in ClickHouse
- Containerized big data infrastructure using Docker

## Architecture

**Data Flow:**

- Raw CSV Files (10,000 records)
- PostgreSQL (staging layer)
- Spark ETL
- Star Schema (PostgreSQL Data Warehouse)
- Spark Aggregations
- Analytical Reports (ClickHouse)

Infrastructure is fully containerized using Docker Compose.

## 1. Source Data

- 10 CSV files
- 1,000 rows each
- **Total: 10,000 transactional records**

Data includes:

- Sales transactions
- Customers
- Products
- Stores
- Suppliers
- Ratings

## 2. Data Warehouse Design

A dimensional model (**Star Schema**) was designed:

**Fact Table:**
- `fact_sales`

**Dimension Tables:**
- `dim_customer`
- `dim_product`
- `dim_store`
- `dim_supplier`
- `dim_time`

The schema enables efficient aggregation and analytical queries.

## 3. Spark ETL Implementation

Implemented using **PySpark**:

- Data extraction from PostgreSQL
- Data cleansing and normalization
- Surrogate key generation
- Fact-dimension separation
- Aggregations for reporting
- Parallel transformations

## 4. Analytical Reports (ClickHouse)

Six analytical data marts were built in ClickHouse:

### 1. Product Sales Mart
- Top-10 best-selling products
- Revenue by product category
- Average rating per product
- Review count

### 2. Customer Sales Mart
- Top-10 customers by total spending
- Customer distribution by country
- Average order value per customer

### 3. Time-based Sales Mart
- Monthly and yearly sales trends
- Revenue comparison across periods
- Average order value by month

### 4. Store Performance Mart
- Top-5 stores by revenue
- Sales distribution by geography
- Average order size per store

### 5. Supplier Performance Mart
- Top-5 suppliers by revenue
- Average product price per supplier
- Supplier country distribution

### 6. Product Quality Mart
- Highest and lowest rated products
- Correlation between rating and sales volume
- Most reviewed products

Each report is materialized as a separate table in ClickHouse.

## 5. Infrastructure

**Containerized services:**
- PostgreSQL (Data Warehouse)
- Apache Spark
- ClickHouse (Analytical DB)

Docker Compose orchestrates networking and service connectivity.

**Run:**
```bash
./run.sh
```

**Verify**
```bash
docker exec -it bigdataspark-clickhouse-1 clickhouse-client --query "SELECT * FROM <table_name> LIMIT 10"
```

## 6. Technologies
- Python
- PySpark
- PostgreSQL
- ClickHouse
- Docker