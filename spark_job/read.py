
import pandas as pd

df = pd.read_parquet("data/processed/sales")

print(df.head(5))