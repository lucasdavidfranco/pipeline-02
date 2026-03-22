from pyspark.sql.functions import col, year, month, day, round

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

