
from pyspark.sql import SparkSession
from transformations import add_sales_amount, add_date_columns, add_quarter, aggregate_sales, quarterly_sales, top_products

def main():

    spark = SparkSession.builder \
        .appName("sales-batch-job") \
        .getOrCreate()
    
    print("Spark session created")

    raw_df = spark.read.csv("data/raw/sales.csv", header=True, inferSchema=True)
    print("Read csv data")
    print(f"Raw base rows scanned: {raw_df.count()}")
    raw_df.show(5)

    print("Start transformations...")

    base_df = add_sales_amount(raw_df)
    base_df = add_date_columns(base_df)
    base_df = add_quarter(base_df, spark)
    base_df = base_df.repartition("year", "month")

    print("Applied transformation on base table")
    print(f"Raw base rows transformed: {base_df.count()}")
    base_df.show(5)

    print("Start aggregations...")
    
    df_agg = aggregate_sales(base_df)
    df_agg = df_agg.repartition("year", "month")

    df_quarterly = quarterly_sales(df_agg, spark)
    df_quarterly = df_quarterly.repartition("year", "quarter")

    df_top = top_products(df_agg)
    df_top = df_top.repartition("year", "month")

    print("Created aggregations")

    print(f"Agg base rows calculated: {df_agg.count()}")
    df_agg.show(5)
    print(f"Quarterly base rows calculated: {df_quarterly.count()}")
    df_quarterly.show(5)
    print(f"Top 3 base rows calculated: {df_top.count()}")
    df_top.show(5)

    raw_df.printSchema()
    base_df.printSchema()
    df_agg.printSchema()
    df_quarterly.printSchema()
    df_top.printSchema()

    print("Start data storage...")

    base_df.write \
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