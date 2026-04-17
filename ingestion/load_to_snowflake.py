import os
import pandas as pd
import snowflake.connector
from dotenv import load_dotenv

load_dotenv()

conn = snowflake.connector.connect(
    user=os.getenv("SNOWFLAKE_USER"),
    password=os.getenv("SNOWFLAKE_PASSWORD"),
    account=os.getenv("SNOWFLAKE_ACCOUNT"),
    warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
    database=os.getenv("SNOWFLAKE_DATABASE"),
    schema="BRONZE"
)

cursor = conn.cursor()

csv_files = {
    "ORDERS": "data/raw/olist_orders_dataset.csv",
    "ORDER_ITEMS": "data/raw/olist_order_items_dataset.csv",
    "ORDER_PAYMENTS": "data/raw/olist_order_payments_dataset.csv",
    "ORDER_REVIEWS": "data/raw/olist_order_reviews_dataset.csv",
    "CUSTOMERS": "data/raw/olist_customers_dataset.csv",
    "PRODUCTS": "data/raw/olist_products_dataset.csv",
    "SELLERS": "data/raw/olist_sellers_dataset.csv",
    "CATEGORY_TRANSLATION": "data/raw/product_category_name_translation.csv"
}

def get_snowflake_type(dtype):
    if "int" in str(dtype):
        return "NUMBER"
    elif "float" in str(dtype):
        return "FLOAT"
    else:
        return "VARCHAR"

def load_csv_to_bronze(table_name, file_path):
    print(f"Loading {table_name}...")
    df = pd.read_csv(file_path)
    df.columns = [col.upper() for col in df.columns]
    cols = ", ".join([
        f"{col} {get_snowflake_type(dtype)}"
        for col, dtype in zip(df.columns, df.dtypes)
    ])
    cursor.execute(f"DROP TABLE IF EXISTS ECOMMERCE_AI.BRONZE.{table_name}")
    cursor.execute(f"CREATE TABLE ECOMMERCE_AI.BRONZE.{table_name} ({cols})")
    batch_size = 1000
    total = 0
    for i in range(0, len(df), batch_size):
        batch = df.iloc[i:i+batch_size]
        rows = [tuple(
            None if pd.isna(v) else v
            for v in row
        ) for row in batch.itertuples(index=False)]
        placeholders = ", ".join(["%s"] * len(df.columns))
        cursor.executemany(
            f"INSERT INTO ECOMMERCE_AI.BRONZE.{table_name} VALUES ({placeholders})",
            rows
        )
        total += len(rows)
    print(f"Done! {table_name} - {total} rows loaded")

print("Starting Bronze ingestion...")
for table_name, file_path in csv_files.items():
    load_csv_to_bronze(table_name, file_path)

cursor.close()
conn.close()
print("All tables loaded into ECOMMERCE_AI.BRONZE!")