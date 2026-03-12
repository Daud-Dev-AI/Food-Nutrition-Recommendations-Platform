from pathlib import Path
import pandas as pd

raw_dir = Path("data/raw")
files = list(raw_dir.glob("*.csv"))

if not files:
    raise FileNotFoundError("No CSV found in data/raw")

file_path = files[0]
print(f"Reading file: {file_path}")

df = pd.read_csv(file_path)

print("\nShape:")
print(df.shape)

print("\nColumns:")
print(df.columns.tolist())

print("\nDtypes:")
print(df.dtypes)

print("\nFirst 5 rows:")
print(df.head())

print("\nNull counts:")
print(df.isnull().sum())