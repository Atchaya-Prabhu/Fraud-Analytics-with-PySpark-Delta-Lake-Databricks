# Databricks notebook source
# ============================================================================
# Project      : Credit Card Fraud Analytics Pipeline
# Notebook     : 05_Analytics_Dashboard
# Author       : Atchaya P
# ============================================================================

from pyspark.sql.functions import *
from pyspark.sql.window import Window

# =============================================================================
# Load Dataset
# =============================================================================

DATA_PATH = "/Workspace/Users/atchaya.analyst@gmail.com/Fraud Analytics/fraudTrain.csv"

transactions_df = (
    spark.read
         .format("csv")
         .option("header", True)
         .option("inferSchema", True)
         .load(DATA_PATH)
)

# =============================================================================
# Feature Engineering
# =============================================================================

transactions_df = (
    transactions_df
    .withColumn("Customer_Name", concat_ws(" ", col("first"), col("last")))
    .withColumn("Transaction_Date", to_date(col("trans_date_trans_time")))
    .withColumn("Transaction_Hour", hour(col("trans_date_trans_time")))
    .withColumn("Month_Name", date_format(col("Transaction_Date"), "MMMM"))
    .withColumn(
        "Weekend_Flag",
        when(dayofweek(col("Transaction_Date")).isin(1,7),"Weekend")
        .otherwise("Weekday")
    )
)

print("="*80)
print("Credit Card Fraud Analytics Dashboard")
print("="*80)

# =============================================================================
# Executive KPI
# =============================================================================

kpi_df = transactions_df.agg(
    count("*").alias("Total_Transactions"),
    round(sum("amt"),2).alias("Total_Revenue"),
    round(avg("amt"),2).alias("Average_Transaction"),
    sum("is_fraud").alias("Fraud_Transactions"),
    round(avg("is_fraud")*100,2).alias("Fraud_Rate (%)")
)

display(kpi_df)

# =============================================================================
# Revenue by Category
# =============================================================================

category_summary = (
    transactions_df
    .groupBy("category")
    .agg(
        round(sum("amt"),2).alias("Revenue"),
        count("*").alias("Transactions")
    )
    .orderBy(desc("Revenue"))
)

display(category_summary)

# Visualization:
# Chart Type : Horizontal Bar
# X Axis     : Revenue
# Y Axis     : Category

# =============================================================================
# Fraud by Category
# =============================================================================

fraud_category = (
    transactions_df
    .groupBy("category")
    .agg(
        sum("is_fraud").alias("Fraud_Transactions")
    )
    .orderBy(desc("Fraud_Transactions"))
)

display(fraud_category)

# Visualization:
# Chart Type : Column Chart
# X Axis     : Category
# Y Axis     : Fraud_Transactions

# =============================================================================
# Revenue by State
# =============================================================================

state_summary = (
    transactions_df
    .groupBy("state")
    .agg(
        round(sum("amt"),2).alias("Revenue")
    )
    .orderBy(desc("Revenue"))
)

display(state_summary)

# Visualization:
# Chart Type : Map (Preferred) or Bar Chart

# =============================================================================
# Top 10 Merchants
# =============================================================================

merchant_summary = (
    transactions_df
    .groupBy("merchant")
    .agg(
        round(sum("amt"),2).alias("Revenue")
    )
    .orderBy(desc("Revenue"))
    .limit(10)
)

display(merchant_summary)

# Visualization:
# Chart Type : Horizontal Bar Chart

# =============================================================================
# Top 10 Customers
# =============================================================================

customer_summary = (
    transactions_df
    .groupBy("Customer_Name")
    .agg(
        round(sum("amt"),2).alias("Total_Spend")
    )
    .orderBy(desc("Total_Spend"))
    .limit(10)
)

display(customer_summary)

# Visualization:
# Chart Type : Horizontal Bar Chart
# X Axis     : Total_Spend
# Y Axis     : Customer_Name

# =============================================================================
# Weekend vs Weekday Transactions
# =============================================================================

weekend_summary = (
    transactions_df
    .groupBy("Weekend_Flag")
    .agg(
        count("*").alias("Transactions"),
        round(sum("amt"),2).alias("Revenue")
    )
)

display(weekend_summary)

# Visualization:
# Chart Type : Donut Chart
# Label      : Weekend_Flag
# Value       : Transactions

# =============================================================================
# Monthly Revenue
# =============================================================================

monthly_revenue = (
    transactions_df
    .groupBy("Month_Name")
    .agg(
        round(sum("amt"),2).alias("Revenue")
    )
)

display(monthly_revenue)

# Visualization:
# Chart Type : Line Chart
# X Axis     : Month_Name
# Y Axis     : Revenue

# =============================================================================
# Monthly Fraud Trend
# =============================================================================

monthly_fraud = (
    transactions_df
    .groupBy("Month_Name")
    .agg(
        sum("is_fraud").alias("Fraud_Transactions")
    )
)

display(monthly_fraud)

# Visualization:
# Chart Type : Line Chart
# X Axis     : Month_Name
# Y Axis     : Fraud_Transactions

# =============================================================================
# Fraud Percentage by Category
# =============================================================================

fraud_percentage = (
    transactions_df
    .groupBy("category")
    .agg(
        count("*").alias("Total_Transactions"),
        sum("is_fraud").alias("Fraud_Transactions")
    )
    .withColumn(
        "Fraud_Percentage",
        round(
            col("Fraud_Transactions") * 100 /
            col("Total_Transactions"),
            2
        )
    )
    .orderBy(desc("Fraud_Percentage"))
)

display(fraud_percentage)

# Visualization:
# Chart Type : Column Chart
# X Axis     : Category
# Y Axis     : Fraud_Percentage

# =============================================================================
# Running Revenue (Window Function)
# =============================================================================

window_spec = (
    Window
    .orderBy("Transaction_Date")
    .rowsBetween(
        Window.unboundedPreceding,
        Window.currentRow
    )
)

running_revenue = (
    transactions_df
    .withColumn(
        "Running_Revenue",
        sum("amt").over(window_spec)
    )
)

display(

running_revenue.select(
    "Transaction_Date",
    "amt",
    "Running_Revenue"
)

)

# Visualization:
# Chart Type : Line Chart
# X Axis     : Transaction_Date
# Y Axis     : Running_Revenue

# =============================================================================
# Highest Transaction by Category (Window Function)
# =============================================================================

category_window = (
    Window
    .partitionBy("category")
    .orderBy(desc("amt"))
)

highest_transaction = (
    transactions_df
    .withColumn(
        "Rank",
        row_number().over(category_window)
    )
    .filter(col("Rank") == 1)
)

display(

highest_transaction.select(
    "category",
    "merchant",
    "amt",
    "Customer_Name"
)

)

# =============================================================================
# Spark SQL Analytics
# =============================================================================

transactions_df.createOrReplaceTempView("fraud_transactions")

print("="*80)
print("Spark SQL Analytics")
print("="*80)

# =============================================================================
# SQL Query 1 - Revenue by Category
# =============================================================================

display(
spark.sql("""

SELECT
    category,
    COUNT(*) AS Total_Transactions,
    ROUND(SUM(amt),2) AS Revenue,
    ROUND(AVG(amt),2) AS Average_Transaction
FROM fraud_transactions
GROUP BY category
ORDER BY Revenue DESC

""")
)

# =============================================================================
# SQL Query 2 - Top 10 Fraud States
# =============================================================================

display(
spark.sql("""

SELECT
    state,
    SUM(is_fraud) AS Fraud_Transactions
FROM fraud_transactions
GROUP BY state
ORDER BY Fraud_Transactions DESC
LIMIT 10

""")
)

# Visualization:
# Chart Type : Bar Chart

# =============================================================================
# SQL Query 3 - Top 10 Merchants
# =============================================================================

display(
spark.sql("""

SELECT
    merchant,
    COUNT(*) AS Transactions,
    ROUND(SUM(amt),2) AS Revenue
FROM fraud_transactions
GROUP BY merchant
ORDER BY Revenue DESC
LIMIT 10

""")
)

# Visualization:
# Chart Type : Horizontal Bar Chart

# =============================================================================
# SQL Query 4 - Average Transaction by Gender
# =============================================================================

display(
spark.sql("""

SELECT
    gender,
    ROUND(AVG(amt),2) AS Average_Transaction
FROM fraud_transactions
GROUP BY gender

""")
)

# Visualization:
# Chart Type : Column Chart

# =============================================================================
# SQL Query 5 - Weekend vs Weekday Revenue
# =============================================================================

display(
spark.sql("""

SELECT
    Weekend_Flag,
    COUNT(*) AS Transactions,
    ROUND(SUM(amt),2) AS Revenue
FROM fraud_transactions
GROUP BY Weekend_Flag

""")
)

# Visualization:
# Chart Type : Donut Chart

# =============================================================================
# Dashboard Visualizations
# =============================================================================

print("="*80)
print("Recommended Dashboard")
print("="*80)

print("1. Executive KPI")
print("   • Total Transactions")
print("   • Total Revenue")
print("   • Fraud Transactions")
print("   • Fraud Rate")

print("\n2. Revenue by Category (Horizontal Bar)")

print("\n3. Fraud by Category (Column Chart)")

print("\n4. Revenue by State (Map or Bar Chart)")

print("\n5. Top 10 Merchants (Horizontal Bar)")

print("\n6. Top 10 Customers (Horizontal Bar)")

print("\n7. Monthly Revenue (Line Chart)")

print("\n8. Monthly Fraud Trend (Line Chart)")

print("\n9. Weekend vs Weekday (Donut Chart)")

print("\n10. Fraud Percentage by Category (Column Chart)")

# =============================================================================
# Business Insights
# =============================================================================

print("="*80)
print("BUSINESS INSIGHTS")
print("="*80)

print("• Analyze which merchant categories generate the highest revenue.")
print("• Identify categories with the highest fraud transactions.")
print("• Compare transaction activity on weekends versus weekdays.")
print("• Identify the highest revenue-generating merchants.")
print("• Analyze customer spending patterns.")
print("• Identify states with the highest revenue.")
print("• Monitor monthly transaction trends.")
print("• Track monthly fraud trends.")
print("• Compare fraud percentage across merchant categories.")
print("• Use these outputs to build an interactive Power BI dashboard.")

print("="*80)
print("Analytics Dashboard Completed Successfully")
print("="*80)

# =============================================================================
# VISUAL DASHBOARD
# =============================================================================

import matplotlib.pyplot as plt

# -----------------------------
# Revenue by Category
# -----------------------------

category_pd = (
    category_summary
    .orderBy(desc("Revenue"))
    .limit(10)
    .toPandas()
)

plt.figure(figsize=(10,5))
plt.bar(category_pd["category"], category_pd["Revenue"])
plt.title("Revenue by Category")
plt.xlabel("Category")
plt.ylabel("Revenue")
plt.xticks(rotation=45)
plt.tight_layout()
plt.show()

# -----------------------------
# Fraud by Category
# -----------------------------

fraud_pd = fraud_category.toPandas()

plt.figure(figsize=(10,5))
plt.bar(fraud_pd["category"], fraud_pd["Fraud_Transactions"])
plt.title("Fraud Transactions by Category")
plt.xlabel("Category")
plt.ylabel("Fraud Transactions")
plt.xticks(rotation=45)
plt.tight_layout()
plt.show()

# -----------------------------
# Top Merchants
# -----------------------------

merchant_pd = merchant_summary.toPandas()

plt.figure(figsize=(10,5))
plt.barh(merchant_pd["merchant"], merchant_pd["Revenue"])
plt.title("Top 10 Merchants by Revenue")
plt.xlabel("Revenue")
plt.tight_layout()
plt.show()

# -----------------------------
# Monthly Revenue
# -----------------------------

monthly_pd = monthly_revenue.toPandas()

plt.figure(figsize=(10,5))
plt.plot(monthly_pd["Month_Name"], monthly_pd["Revenue"], marker="o")
plt.title("Monthly Revenue")
plt.xlabel("Month")
plt.ylabel("Revenue")
plt.xticks(rotation=45)
plt.tight_layout()
plt.show()

# -----------------------------
# Weekend vs Weekday
# -----------------------------

weekend_pd = weekend_summary.toPandas()

plt.figure(figsize=(6,6))
plt.pie(
    weekend_pd["Transactions"],
    labels=weekend_pd["Weekend_Flag"],
    autopct="%1.1f%%",
    startangle=90
)
plt.title("Weekend vs Weekday Transactions")
plt.show()

# =============================================================================
# End of Project
# =============================================================================

# COMMAND ----------

