from pyspark.sql.functions import col, year, month, day, round, sum as _sum, count, when, rank
from pyspark.sql.window import Window

def add_sales_amount(df):
    return df.withColumn("sales_amount", round( col("price") * col("quantity"), 2))

def add_date_columns(df):
    return(
        df.withColumn("year", year(col("sales_datetime")))
            .withColumn("month", month(col("sales_datetime")))
            .withColumn("day", day(col("sales_datetime")))
    )

def add_quarter(df, spark):

    quarter = [
        {"month": 1, "quarter": "q1"},
        {"month": 2, "quarter": "q1"},
        {"month": 3, "quarter": "q1"},
        {"month": 4, "quarter": "q2"},
        {"month": 5, "quarter": "q2"},
        {"month": 6, "quarter": "q2"},
        {"month": 7, "quarter": "q3"},
        {"month": 8, "quarter": "q3"},
        {"month": 9, "quarter": "q3"},
        {"month": 10, "quarter": "q4"},
        {"month": 11, "quarter": "q4"},
        {"month": 12, "quarter": "q4"}
    ]

    quarter_df = spark.createDataFrame(quarter)

    return df.join(quarter_df, on="month", how="left")

def aggregate_sales(df):
    return (
        df.groupBy("year", "month", "product")
            .agg(
                _sum("sales_amount").alias("total_sales"),
                count("order_id").alias("total_orders"),
                _sum("quantity").alias("total_units"),
            )
            .withColumn(
                "asp", 
                when(
                    col("total_units") != 0, 
                    col("total_sales") / col("total_units")
                )
            )
            .select(
                "year",
                "month",
                "product",
                round(col("total_sales"),2).alias("total_sales"),
                "total_orders",
                "total_units",
                round(col("asp"), 2).alias("asp")
            )
            .orderBy("year", "month", "product")
    )

def quarterly_sales(df, spark):
        
        df_quarterly = add_quarter(df, spark)

        return (
        df_quarterly.groupBy("year", "quarter")
            .agg(
                _sum("total_sales").alias("total_sales"),
                count("total_orders").alias("total_orders"),
                _sum("total_units").alias("total_units"),
            )
            .withColumn(
                "asp", 
                when(
                    col("total_units") != 0, 
                    col("total_sales") / col("total_units")
                )
            )
            .select(
                "year",
                "quarter",
                round(col("total_sales"),2).alias("total_sales"),
                "total_orders",
                "total_units",
                round(col("asp"), 2).alias("asp")
            )
            .orderBy("year", "quarter")
    )

def top_products(df):

    window_spec = (
        Window
            .partitionBy("year", "month")
            .orderBy(col("total_sales").desc())
    )

    df_ranked = df.withColumn("rank", rank().over(window_spec))
    
    return df_ranked.filter(col("rank") <= 3)