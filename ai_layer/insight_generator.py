# ============================================
# AI LAYER — INSIGHT GENERATOR
# ============================================
# Uses Claude API to generate executive insights
# from 5 key business questions
# ============================================

import os
import anthropic
import snowflake.connector
import pandas as pd
from dotenv import load_dotenv

load_dotenv()

# ============================================
# BLOCK 1 — CONNECT TO SNOWFLAKE
# ============================================
conn = snowflake.connector.connect(
    user=os.getenv("SNOWFLAKE_USER"),
    password=os.getenv("SNOWFLAKE_PASSWORD"),
    account=os.getenv("SNOWFLAKE_ACCOUNT"),
    warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
    database=os.getenv("SNOWFLAKE_DATABASE"),
    schema="GOLD"
)

print("Connected to Snowflake!")

# ============================================
# BLOCK 2 — READ 5 BUSINESS QUESTIONS
# ============================================
print("Running 5 business queries...")

# Question 1: Declining revenue categories
declining_categories = pd.read_sql("""
    WITH monthly_category AS (
        SELECT
            product_category_name_english,
            year_month,
            SUM(total_revenue) AS monthly_revenue
        FROM ECOMMERCE_AI.GOLD.REVENUE_METRICS
        GROUP BY product_category_name_english, year_month
    ),
    with_previous AS (
        SELECT
            product_category_name_english,
            year_month,
            monthly_revenue,
            LAG(monthly_revenue) OVER (
                PARTITION BY product_category_name_english
                ORDER BY year_month
            ) AS prev_month_revenue
        FROM monthly_category
    )
    SELECT
        product_category_name_english,
        year_month,
        monthly_revenue,
        prev_month_revenue,
        monthly_revenue - prev_month_revenue AS revenue_change
    FROM with_previous
    WHERE monthly_revenue < prev_month_revenue
    ORDER BY revenue_change ASC
    LIMIT 10
""", conn)

# Question 2: Customer distribution by state
repeat_customers = pd.read_sql("""
    SELECT
        customer_state,
        COUNT(customer_id)      AS total_customers,
        SUM(total_spent)        AS total_revenue,
        AVG(total_spent)        AS avg_spent_per_customer
    FROM ECOMMERCE_AI.GOLD.CUSTOMER_METRICS
    GROUP BY customer_state
    ORDER BY total_customers DESC
    LIMIT 10
""", conn)

# Question 3: High revenue poor delivery sellers
poor_delivery_sellers = pd.read_sql("""
    SELECT
        seller_id,
        seller_state,
        total_revenue,
        avg_delivery_days,
        total_orders
    FROM ECOMMERCE_AI.GOLD.SELLER_METRICS
    WHERE total_revenue > (
        SELECT AVG(total_revenue)
        FROM ECOMMERCE_AI.GOLD.SELLER_METRICS
    )
    AND avg_delivery_days > (
        SELECT AVG(avg_delivery_days)
        FROM ECOMMERCE_AI.GOLD.SELLER_METRICS
    )
    ORDER BY total_revenue DESC
    LIMIT 10
""", conn)

# Question 4: % revenue from top 10 categories
top_category_share = pd.read_sql("""
    WITH category_revenue AS (
        SELECT
            product_category_name_english,
            SUM(total_revenue) AS category_revenue
        FROM ECOMMERCE_AI.GOLD.REVENUE_METRICS
        GROUP BY product_category_name_english
    ),
    with_total AS (
        SELECT
            product_category_name_english,
            category_revenue,
            SUM(category_revenue) OVER () AS grand_total
        FROM category_revenue
    )
    SELECT
        product_category_name_english,
        category_revenue,
        ROUND(category_revenue * 100.0 / grand_total, 2) AS revenue_pct
    FROM with_total
    ORDER BY category_revenue DESC
    LIMIT 10
""", conn)

# Question 5: Late delivery rate by seller state
late_delivery_states = pd.read_sql("""
    SELECT
        seller_state,
        SUM(total_orders)               AS total_orders,
        ROUND(AVG(avg_delivery_days), 2) AS avg_delivery_days
    FROM ECOMMERCE_AI.GOLD.SELLER_METRICS
    GROUP BY seller_state
    ORDER BY avg_delivery_days DESC
    LIMIT 10
""", conn)

print("All 5 business queries done!")

# ============================================
# BLOCK 3 — BUILD PROMPT FOR CLAUDE
# ============================================
prompt = f"""
You are a senior data analyst presenting to
the board of Olist Brazilian E-Commerce Platform.

Here are results from 5 key business questions:

QUESTION 1: Categories with Declining Revenue
{declining_categories.to_string(index=False)}

QUESTION 2: Customer Distribution by State
{repeat_customers.to_string(index=False)}

QUESTION 3: High Revenue but Poor Delivery Sellers
{poor_delivery_sellers.to_string(index=False)}

QUESTION 4: Top 10 Categories Revenue Share
{top_category_share.to_string(index=False)}

QUESTION 5: Late Delivery Rate by Seller State
{late_delivery_states.to_string(index=False)}

Based on these 5 business questions and their results,
generate 5 executive-level insights with recommendations.
Format each as:

INSIGHT [N]: [title]
Finding: [what data shows]
Action: [recommended business action]
"""

# ============================================
# BLOCK 4 — CALL CLAUDE API
# ============================================
print("Calling Claude API...")

client = anthropic.Anthropic(
    api_key=os.getenv("ANTHROPIC_API_KEY")
)

message = client.messages.create(
    model="claude-opus-4-5",
    max_tokens=1000,
    messages=[
        {"role": "user", "content": prompt}
    ]
)

insights = message.content[0].text
print("Claude API response received!")

# ============================================
# BLOCK 5 — SAVE INSIGHTS TO FILE
# ============================================
print("Saving insights...")

os.makedirs("outputs", exist_ok=True)

with open("outputs/ai_insights.md", "w") as f:
    f.write("# AI-Generated Executive Insights\n")
    f.write("## Client: Olist Brazilian E-Commerce\n")
    f.write("## Generated by: Claude AI (Anthropic)\n\n")
    f.write("---\n\n")
    f.write(insights)

print("Insights saved to outputs/ai_insights.md!")

conn.close()
print("\nAI Layer complete!")