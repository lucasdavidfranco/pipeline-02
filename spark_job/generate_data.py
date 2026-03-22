import os
import random
from datetime import datetime, timedelta

import pandas as pd

def generate_sales_data(num_rows: int = 100000):

    products = [
        "Laptop",
        "Mouse",
        "Keyboard",
        "Monitor",
        "Headphones",
        "Webcam",
        "Chair",
        "Desk"
    ]

    start_date = datetime(2024, 1, 1)
    end_date = datetime(2024, 12, 31)
    date_range_days = (end_date - start_date).days

    rows = []
    