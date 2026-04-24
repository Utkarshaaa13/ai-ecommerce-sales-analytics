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

# ============================================
# CLIENT A — BUSINESS REQUIREMENTS
# ============================================
# Client: Olist Brazilian E-Commerce Platform
# Goal: Understand business performance across
# 4 areas — Revenue, Delivery, Customers, Sellers
# ============================================

# ============================================
# BLOCK 3 — CLEAN ORDERS TABLE
# ============================================
# Serving: Revenue Analysis + Delivery Performance
# What we do:
# - Convert timestamps to proper dates
# - Derive days_to_deliver (delivered - purchased)
# - Derive year_month for monthly trend analysis
# - Derive is_late (delivered > estimated date)
# - Keep only delivered orders
# ============================================
orders.columns = orders.columns.str.lower()

orders['order_purchase_timestamp'] = pd.to_datetime(orders['order_purchase_timestamp'])
orders['order_delivered_customer_date'] = pd.to_datetime(orders['order_delivered_customer_date'])
orders['order_estimated_delivery_date'] = pd.to_datetime(orders['order_estimated_delivery_date'])

orders['purchase_date'] = orders['order_purchase_timestamp'].dt.date
orders['year_month'] = orders['order_purchase_timestamp'].dt.to_period('M').astype(str)
orders['days_to_deliver'] = (orders['order_delivered_customer_date'] - orders['order_purchase_timestamp']).dt.days
orders['is_late'] = (orders['order_delivered_customer_date'] > orders['order_estimated_delivery_date'])

orders_clean = orders[orders['order_status'] == 'delivered'][[
    'order_id', 'customer_id', 'order_status',
    'purchase_date', 'year_month',
    'days_to_deliver', 'is_late'
]]

print(f"ORDERS cleaned: {len(orders_clean)} rows")
# ============================================
# BLOCK 4 — CLEAN CUSTOMERS TABLE
# ============================================
# Serving: Customer Behavior
# What we do:
# - Keep only customer_id, city, state
# - Drop zip code (not needed)
# ============================================

customers.columns = customers.columns.str.lower()

customers_clean = customers[[
    'customer_id',
    'customer_city',
    'customer_state'
]].drop_duplicates()

print(f"CUSTOMERS cleaned: {len(customers_clean)} rows")

# ============================================
# BLOCK 5 — CLEAN ORDER ITEMS TABLE
# ============================================
# Serving: Revenue Analysis + Seller Performance
# What we do:
# - Derive total_item_value = price + freight
# - Keep order_id, product_id, seller_id, price
# ============================================

items.columns = items.columns.str.lower()

items['total_item_value'] = items['price'] + items['freight_value']

items_clean = items[[
    'order_id', 'product_id', 'seller_id',
    'price', 'freight_value', 'total_item_value'
]]

print(f"ORDER_ITEMS cleaned: {len(items_clean)} rows")

# ============================================
# BLOCK 6 — CLEAN PRODUCTS + CATEGORY TABLE
# ============================================
# Serving: Revenue Analysis
# What we do:
# - Join Products with Category Translation
# - Convert Portuguese → English category names
# ============================================
products.columns = products.columns.str.lower()
category.columns = category.columns.str.lower()

products_clean = products.merge(
    category,
    on='product_category_name',
    how='left'
)[[
    'product_id',
    'product_category_name_english'
]]

print(f"PRODUCTS cleaned: {len(products_clean)} rows")
# ============================================
# BLOCK 7 — CLEAN SELLERS TABLE
# ============================================
# Serving: Seller Performance
# What we do:
# - Keep seller_id, city, state
# - Drop zip code (not needed)
# ============================================
sellers.columns = sellers.columns.str.lower()

sellers_clean = sellers[[
    'seller_id',
    'seller_city',
    'seller_state'
]]

print(f"SELLERS cleaned: {len(sellers_clean)} rows")
# ============================================
# BLOCK 8 — CLEAN PAYMENTS TABLE
# ============================================
# Serving: Customer Behavior
# What we do:
# - Group multiple payments per order into one row
# - Sum payment values per order
# ============================================
payments.columns = payments.columns.str.lower()

payments_clean = payments.groupby('order_id').agg(
    payment_type=('payment_type', 'first'),
    payment_installments=('payment_installments', 'max'),
    payment_value=('payment_value', 'sum')
).reset_index()

print(f"PAYMENTS cleaned: {len(payments_clean)} rows")
# ============================================
# BLOCK 9 — BUILD ORDERS_ENRICHED
# ============================================
# Serving: All 4 requirements
# What we do:
# - Join all cleaned tables into one wide table
# ============================================
print("Building ORDERS_ENRICHED...")

enriched = orders_clean \
    .merge(customers_clean, on='customer_id', how='left') \
    .merge(items_clean, on='order_id', how='left') \
    .merge(products_clean, on='product_id', how='left') \
    .merge(sellers_clean, on='seller_id', how='left') \
    .merge(payments_clean, on='order_id', how='left')

print(f"ORDERS_ENRICHED shape: {enriched.shape}")
# ============================================
# BLOCK 10 — BUILD REVIEWS_CLEAN
# ============================================
# Serving: AI Layer (Claude API)
# What we do:
# - Keep only reviews WITH comments
# - Ready for sentiment analysis
# ============================================
reviews.columns = reviews.columns.str.lower()

reviews['review_creation_date'] = pd.to_datetime(reviews['review_creation_date'])

reviews_clean = reviews[[
    'review_id', 'order_id', 'review_score',
    'review_comment_message', 'review_creation_date'
]].dropna(subset=['review_comment_message'])

print(f"REVIEWS_CLEAN: {len(reviews_clean)} rows")
# ============================================
# BLOCK 11 — WRITE TO SNOWFLAKE SILVER
# ============================================
# Serving: Gold layer + Tableau dashboard
# What we do:
# - Write ORDERS_ENRICHED to Silver
# - Write REVIEWS_CLEAN to Silver
# ============================================
def write_to_silver(df, table_name):
    print(f"Writing {table_name} to Silver...")
    cursor.execute(f"DROP TABLE IF EXISTS ECOMMERCE_AI.SILVER.{table_name}")
    cols = ", ".join([f"{col} VARCHAR" for col in df.columns])
    cursor.execute(f"CREATE TABLE ECOMMERCE_AI.SILVER.{table_name} ({cols})")
    for i in range(0, len(df), 1000):
        batch = df.iloc[i:i+1000]
        rows = [tuple(row) for row in batch.itertuples(index=False)]
        placeholders = ", ".join(["%s"] * len(df.columns))
        cursor.executemany(
            f"INSERT INTO ECOMMERCE_AI.SILVER.{table_name} VALUES ({placeholders})",
            rows
        )
    print(f"Done! {table_name} written!")

enriched = enriched.fillna('').astype(str)
reviews_clean = reviews_clean.fillna('').astype(str)

write_to_silver(enriched, "ORDERS_ENRICHED")
write_to_silver(reviews_clean, "REVIEWS_CLEAN")

cursor.close()
conn.close()
print("Silver layer complete!")