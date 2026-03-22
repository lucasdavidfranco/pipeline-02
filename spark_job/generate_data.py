import os
import random
from datetime import datetime, timedelta

import pandas as pd

def generate_sales_data(num_rows: int = 100000):

    products = [
        {"product": "Laptop", "product_price_min": 500, "product_price_max": 1500 },
        {"product": "Mouse", "product_price_min": 10, "product_price_max": 20 },
        {"product": "Keyboard", "product_price_min": 25, "product_price_max": 50 },
        {"product": "Monitor", "product_price_min": 200, "product_price_max": 400 },
        {"product": "Headphones", "product_price_min": 50, "product_price_max": 200 },
        {"product": "Webcam", "product_price_min": 100, "product_price_max": 250 },
        {"product": "Chair", "product_price_min": 60, "product_price_max": 150 },
        {"product": "Desk", "product_price_min": 150, "product_price_max": 300 }
    ]

    start_date = datetime(2024, 1, 1)
    end_date = datetime(2024, 12, 31)
    date_range_days = (end_date - start_date).days

    rows = []

    for order_id in range(1, num_rows + 1):
        user_id = random.randint(1000, 5000)
        
        product_info = random.choice(products)
        product = product_info["product"]
        price = round(random.uniform(product_info["product_price_min"], product_info["product_price_max"]), 2)
        quantity = random.randint(1, 3)
        random_days = random.randint(0, date_range_days)
        random_seconds = random.randint(0, 86399)
        sales_datetime = start_date + timedelta(days=random_days, seconds=random_seconds)

        rows.append({
            "order_id": order_id,
            "user_id": user_id,
            "product": product,
            "price": price,
            "quantity": quantity,
            "sales_datetime": sales_datetime
        })
    
    df = pd.DataFrame(rows)
    print(df.head(10))
    print(df.dtypes)

    df.to_csv("data/raw/sales.csv", index=False)    
    print(f"CSV generado correctamente con {num_rows} filas en data/raw/sales.csv")

if __name__ == "__main__":
    generate_sales_data()