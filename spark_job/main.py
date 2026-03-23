
from pyspark.sql import SparkSession
from spark_job.transformations import (
    add_sales_amount, 
    add_date_columns, 
    add_quarter, 
    aggregate_sales, 
    quarterly_sales,
    top_products
)
from spark_job.logger import get_logger
from spark_job.validations import validate_sales_data, drop_duplicate_orders
from spark_job.config import (
    APP_NAME,
    RAW_PATH,
    PROCESSED_SALES_PATH,
    PROCESSED_AGG_PATH,
    PROCESSED_QUARTERLY_PATH,
    TOP_PATH
)
import shutil
import os

logger = get_logger()

def safe_delete(path):
    if os.path.exists(path):
        shutil.rmtree(path)

def main():

    spark = None

    try: 
        logger.info("Starting Spark Job...")
        
        spark = SparkSession.builder \
            .appName(APP_NAME) \
            .getOrCreate()
        
        print("Spark session created")

        raw_df = spark.read.csv(RAW_PATH, header=True, inferSchema=True)
        logger.info("Raw csv loaded")
        logger.info(f"Raw base rows scanned: {raw_df.count()}")

        raw_validated, raw_invalidated = validate_sales_data(raw_df)
        logger.info(f"Invalid rows detected: {raw_invalidated.count()}")

        raw_deduplicated = drop_duplicate_orders(raw_validated)
        logger.info(f"Rows after removing duplicates: {raw_deduplicated.count()}")
        logger.info(raw_deduplicated.show(5))

        logger.info("Start transformation...")

        base_df = add_sales_amount(raw_deduplicated)
        base_df = add_date_columns(base_df)
        base_df = add_quarter(base_df, spark)
        base_df = base_df.repartition("year", "month")

        logger.info("Applied transformation on base table")
        logger.info(f"Rows transformed: {base_df.count()}")
        logger.info(base_df.show(5))
        base_df.printSchema()

        logger.info("Start aggregations...")
        
        df_agg = aggregate_sales(base_df)
        df_agg = df_agg.repartition("year", "month")

        df_quarterly = quarterly_sales(df_agg, spark)
        df_quarterly = df_quarterly.repartition("year", "quarter")

        df_top = top_products(df_agg)
        df_top = df_top.repartition("year", "month")

        logger.info("Created aggregations")

        logger.info(f"Agg base rows calculated: {df_agg.count()}")
        logger.info(df_agg.show(5))

        logger.info(f"Quarterly base rows calculated: {df_quarterly.count()}")
        logger.info(df_quarterly.show(5))

        logger.info(f"Top 3 base rows calculated: {df_top.count()}")
        logger.info(df_top.show(5))

        logger.info("Start data storage...")

        safe_delete(PROCESSED_SALES_PATH)

        base_df.write \
            .partitionBy("year", "month") \
            .mode("overwrite") \
            .parquet(PROCESSED_SALES_PATH)

        logger.info(f"Detail data stored in {PROCESSED_SALES_PATH}")

        safe_delete(PROCESSED_AGG_PATH)

        df_agg.write \
            .partitionBy("year", "month") \
            .mode("overwrite") \
            .parquet(PROCESSED_AGG_PATH)

        logger.info(f"Aggregated data stored in {PROCESSED_AGG_PATH}")

        safe_delete(PROCESSED_QUARTERLY_PATH)

        df_quarterly.write \
            .partitionBy("year", "quarter") \
            .mode("overwrite") \
            .parquet(PROCESSED_QUARTERLY_PATH)

        logger.info(f"Quarterly data stored in {PROCESSED_QUARTERLY_PATH}")

        safe_delete(TOP_PATH)

        df_top.write \
            .partitionBy("year", "month") \
            .mode("overwrite") \
            .parquet(TOP_PATH)

        logger.info(f"Top 3 products data stored in {TOP_PATH}")
    
        logger.info("All parquet outputs saved successfully")

    except Exception as e:
        logger.exception(f"Spark job failed: {e}")
        raise

    finally:
        if spark:
            spark.stop()
            logger.info("Spark session stopped")

if __name__ == "__main__":
    main()