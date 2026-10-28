
from pathlib import Path

import pandas as pd

input_path = Path("data/raw/nyc_311.csv")

df = pd.read_csv(input_path)

print("\n--- Shape ---")
print(df.shape)

print("\n--- Column names ---")
print(df.columns.tolist())

print("\n--- First 5 records ---")
print(df.head())

print("\n--- Data types ---")
print(df.dtypes)

print("\n--- Missing values ---")
print(df.isna().sum().sort_values(ascending=False))
