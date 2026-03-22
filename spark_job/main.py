
from pyspark.sql import SparkSession
from transformations import add_sales_amount, add_date_columns, add_quarter

def main():

    spark = SparkSession.builder \
        .appName("sales-batch-job") \
        .getOrCreate()
    
    print("Spark session created")

    df = spark.read.csv("data/raw/sales.csv", header=True, inferSchema=True)

    print("Read csv data")
    df.show(5)

    df = add_sales_amount(df)
    df = add_date_columns(df)
    df = add_quarter(df, spark)

    print("Applied transformations")
    df.show(5)

    df.write \
        .partitionBy("year", "month") \
        .mode("overwrite") \
        .parquet("data/processed/sales")

    print("Data stored in data/procesed/sales.parquet")

    spark.stop()

if __name__ == "__main__":
    main()