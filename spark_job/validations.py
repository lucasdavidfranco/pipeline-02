from pyspark.sql.functions import col 

def validate_sales_data(df):

    invalid_df = df.filter(
        (col("price") <= 0) |
        (col("quantity") <= 0) |
        (col("sales_datetime").isNull()) |
        (col("order_id").isNull()) 
    )

    valid_df = df.filter(
        (col("price") > 0) |
        (col("quantity") > 0) |
        (col("sales_datetime").isNotNull()) |
        (col("order_id").isNotNull()) 
    )

    return valid_df, invalid_df

def drop_duplicate_orders(df):
    return df.dropDuplicates(["order_id"])

    