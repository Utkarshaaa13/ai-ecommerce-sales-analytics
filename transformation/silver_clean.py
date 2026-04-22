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

print("Connected to Snowflake!")


# ============================================
# BLOCK 2 — READ BRONZE TABLES INTO PANDAS
# As per Client A Business Requirements
# ============================================

print("Reading Bronze tables...")

orders = pd.read_sql("SELECT * FROM ECOMMERCE_AI.BRONZE.ORDERS", conn)
items = pd.read_sql("SELECT * FROM ECOMMERCE_AI.BRONZE.ORDER_ITEMS", conn)
customers = pd.read_sql("SELECT * FROM ECOMMERCE_AI.BRONZE.CUSTOMERS", conn)
products = pd.read_sql("SELECT * FROM ECOMMERCE_AI.BRONZE.PRODUCTS", conn)
sellers = pd.read_sql("SELECT * FROM ECOMMERCE_AI.BRONZE.SELLERS", conn)
payments = pd.read_sql("SELECT * FROM ECOMMERCE_AI.BRONZE.ORDER_PAYMENTS", conn)
reviews = pd.read_sql("SELECT * FROM ECOMMERCE_AI.BRONZE.ORDER_REVIEWS", conn)
category = pd.read_sql("SELECT * FROM ECOMMERCE_AI.BRONZE.CATEGORY_TRANSLATION", conn)

print("All Bronze tables loaded!")

