
from pyspark.sql import SparkSession
from transformations import add_sales_amount, add_date_columns, add_quarter, aggregate_sales, quarterly_sales, top_products

def main():

    spark = SparkSession.builder \
        .appName("sales-batch-job") \
        .getOrCreate()
    
    print("Spark session created")

    df = spark.read.csv("data/raw/sales.csv", header=True, inferSchema=True)

    print("Read csv data")
    df.show(5)

    print("Start transformations...")

    df = add_sales_amount(df)
    df = add_date_columns(df)
    df = add_quarter(df, spark)

    print("Applied transformation on base table")

    df.show(5)

    print("Start aggregations...")
    
    df_agg = aggregate_sales(df)
    df_quarterly = quarterly_sales(df_agg, spark)
    df_top = top_products(df_agg) 

    print("Created aggregations")

    df_agg.show(5)
    df_quarterly.show(5)
    df_top.show(5)

    print("Start data storage...")

    df.write \
        .partitionBy("year", "month") \
        .mode("overwrite") \
        .parquet("data/processed/sales")

    print("Detail data stored in data/procesed/sales.parquet")

    df_agg.write \
        .partitionBy("year", "month") \
        .mode("overwrite") \
        .parquet("data/processed/sales_agg")

    print("Aggregated data stored in data/procesed/sales_agg.parquet")

    df_quarterly.write \
        .partitionBy("year", "quarter") \
        .mode("overwrite") \
        .parquet("data/processed/sales_quarterly")

    print("Quarterly data stored in data/procesed/sales_quarterly.parquet")

    df_top.write \
        .partitionBy("year", "month") \
        .mode("overwrite") \
        .parquet("data/processed/top_monthly")

    print("Top 3 products data stored in data/procesed/top_monthly.parquet")

    spark.stop()

if __name__ == "__main__":
    main()