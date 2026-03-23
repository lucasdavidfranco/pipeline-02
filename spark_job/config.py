import os

APP_NAME = "sales-batch-job"
RAW_PATH = "data/raw/sales.csv"
BASE_OUTPUT = os.getenv("BASE_OUTPUT", "data/processed")
PROCESSED_SALES_PATH = f"{BASE_OUTPUT}/sales"
PROCESSED_AGG_PATH = f"{BASE_OUTPUT}/sales_agg"
PROCESSED_QUARTERLY_PATH = f"{BASE_OUTPUT}/sales_quarterly"
TOP_PATH = f"{BASE_OUTPUT}/top_monthly"