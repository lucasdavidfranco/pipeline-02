# Pipeline-02 - Sales batch processing with PySpark, Docker and Airflow

## Overview 

This project simulates a batch data pipeline for sales processing using PySpark

The pipeline:
1. Generate synthetic sales data
2. Applies data validations and transformations
3. Builds monthly and quarterly aggregations
4. Calculates top 3 products by revenue per month
5. Stores outputs as partitioned Parquet datasets
6. Can be executed standalone with Docker or orchestrated with Airflow

---

## Tech Stack
- Python 3.10
- PySpark
- Pandas
- Docker
- Airflow
- Parquet

---

## Project Structure

project/
├── airflow/
│   └── dags/
│       └── pipeline_02_dag.py
├── data/
│   ├── raw
│   └── processed
├── spark_job/
│   ├── __init__.py
│   ├── config.py
│   ├── generate_data.py
│   ├── logger.py
│   ├── main.py
│   ├── transformations.py
│   └── validations.py
├── Dockerfile
├── Dockerfile.airflow
├── docker-compose.yml
├── requirements.txt
└── README.md

---

## Pipeline Logic

### 1. Data generation
Synthetic sales data is generated with:
- order_id
- user_id
- product
- price
- quantity
- sales_datetime

Prices are generated using realistic ranges by product category.

### 2. Validations
The pipeline validates:
- price > 0
- quantity > 0
- sales_datetime is not null
- order_id is not null
- not duplicated orders

### 3. Transformations
The pipeline enriches the data with:
- sales_amount
- year
- month
- day
- quarter

### 4. Aggregations
The pipeline creates:
- monthly sales, orders amount, units sold and average selling price by product
- quarterly sales, orders amount, units sold and average selling price
- top 3 products by month based on sales with their total sales amount, orders amount, units sold and average selling price

### 5. Storage
Outputs are stored in Parquet format with partitioning by:
- year, month on sales detail, sales aggregated by month and top 3 products by month 
- year, quarter for quarterly sales

### 6. Outputs:
- sales (transformed data)
- sales_agg (monthly aggregations)
- sales_quarterly (quarterly aggregations)
- top_products (top products per month)

---

## Run

### Docker (standalone) + Airflow (orchestrated)

```bash
# Build standalone image
docker build -f Dockerfile -t pipeline-02-spark .

# Run standalone (local test)
docker run --rm -v $(pwd)/data:/app/data pipeline-02-spark

# Run Airflow (orchestrated pipeline)
export AIRFLOW_UID=$(id -u)
docker compose up --build