# Databricks notebook source

"""
Project      : Credit Card Fraud Analytics Pipeline
Notebook     : 04_Data_Analysis.py
Author       : Atchaya P
Environment  : Databricks Serverless
Description  : Business analytics using PySpark aggregations, window functions and Spark SQL.
"""

from pyspark.sql.functions import *
from pyspark.sql.window import Window

# -------------------------------------------------------------------
# Read Engineered Dataset
# -------------------------------------------------------------------

DATA_PATH = "/Workspace/Users/atchaya.analyst@gmail.com/Fraud Analytics/fraudTrain.csv"

transactions_df = (
    spark.read.format("csv")
    .option("header", True)
    .option("inferSchema", True)
    .load(DATA_PATH)
)

# -------------------------------------------------------------------
# Basic Feature Engineering (if Notebook 03 not executed)
# -------------------------------------------------------------------

transactions_df = (
    transactions_df
    .withColumn("Customer_Name", concat_ws(" ", col("first"), col("last")))
    .withColumn("Transaction_Date", to_date("trans_date_trans_time"))
    .withColumn("Month_Name", date_format("Transaction_Date","MMMM"))
    .withColumn("Weekend_Flag",
                when(dayofweek("Transaction_Date").isin(1,7),"Weekend")
                .otherwise("Weekday"))
)

print("="*80)
print("Credit Card Fraud Analytics")
print("="*80)

# -------------------------------------------------------------------
# KPI Summary
# -------------------------------------------------------------------

kpi_df = transactions_df.agg(
    count("*").alias("Total_Transactions"),
    round(sum("amt"),2).alias("Total_Revenue"),
    round(avg("amt"),2).alias("Average_Transaction"),
    sum("is_fraud").alias("Fraud_Transactions")
)

display(kpi_df)

# -------------------------------------------------------------------
# Revenue by Category
# -------------------------------------------------------------------

category_df = (
    transactions_df
    .groupBy("category")
    .agg(
        round(sum("amt"),2).alias("Revenue"),
        count("*").alias("Transactions"),
        round(avg("amt"),2).alias("Average_Amount")
    )
    .orderBy(desc("Revenue"))
)

display(category_df)
# Visualization: Bar Chart

# -------------------------------------------------------------------
# Fraud by Category
# -------------------------------------------------------------------

fraud_category = (
    transactions_df
    .groupBy("category")
    .agg(sum("is_fraud").alias("Fraud_Count"))
    .orderBy(desc("Fraud_Count"))
)

display(fraud_category)
# Visualization: Horizontal Bar

# -------------------------------------------------------------------
# Top Merchants
# -------------------------------------------------------------------

merchant_df = (
    transactions_df
    .groupBy("merchant")
    .agg(round(sum("amt"),2).alias("Revenue"))
    .orderBy(desc("Revenue"))
    .limit(10)
)

display(merchant_df)
# Visualization: Horizontal Bar

# -------------------------------------------------------------------
# Revenue by State
# -------------------------------------------------------------------

state_df = (
    transactions_df
    .groupBy("state")
    .agg(round(sum("amt"),2).alias("Revenue"))
    .orderBy(desc("Revenue"))
)

display(state_df)
# Visualization: Map or Bar

# -------------------------------------------------------------------
# Top Customers
# -------------------------------------------------------------------

customer_df = (
    transactions_df
    .groupBy("cc_num","Customer_Name")
    .agg(round(sum("amt"),2).alias("Total_Spend"))
)

window_spec = Window.orderBy(desc("Total_Spend"))

customer_df = customer_df.withColumn(
    "Rank",
    dense_rank().over(window_spec)
)

display(customer_df.filter(col("Rank") <= 10))
# Visualization: Bar

# -------------------------------------------------------------------
# Highest Transaction Per Category
# -------------------------------------------------------------------

category_window = Window.partitionBy("category").orderBy(desc("amt"))

highest_txn = (
    transactions_df
    .withColumn("Row_Num", row_number().over(category_window))
    .filter(col("Row_Num") == 1)
)

display(highest_txn)

# -------------------------------------------------------------------
# Running Revenue
# -------------------------------------------------------------------

running_window = (
    Window.orderBy("Transaction_Date")
    .rowsBetween(Window.unboundedPreceding, Window.currentRow)
)

running_df = transactions_df.withColumn(
    "Running_Revenue",
    sum("amt").over(running_window)
)

display(running_df.select("Transaction_Date","amt","Running_Revenue"))

# -------------------------------------------------------------------
# Fraud Percentage by Category
# -------------------------------------------------------------------

fraud_percent = (
    transactions_df
    .groupBy("category")
    .agg(
        count("*").alias("Total"),
        sum("is_fraud").alias("Fraud")
    )
    .withColumn(
        "Fraud_Percentage",
        round(col("Fraud")*100/col("Total"),2)
    )
    .orderBy(desc("Fraud_Percentage"))
)

display(fraud_percent)
# Visualization: Bar

# -------------------------------------------------------------------
# Spark SQL
# -------------------------------------------------------------------

transactions_df.createOrReplaceTempView("fraud_transactions")

display(spark.sql("""
SELECT
    category,
    COUNT(*) AS Transactions,
    ROUND(SUM(amt),2) AS Revenue,
    ROUND(AVG(amt),2) AS Average_Amount
FROM fraud_transactions
GROUP BY category
ORDER BY Revenue DESC
"""))

display(spark.sql("""
SELECT
    state,
    SUM(is_fraud) AS Fraud_Transactions
FROM fraud_transactions
GROUP BY state
ORDER BY Fraud_Transactions DESC
LIMIT 10
"""))

display(spark.sql("""
SELECT
    Month_Name,
    ROUND(SUM(amt),2) AS Revenue
FROM fraud_transactions
GROUP BY Month_Name
ORDER BY Revenue DESC
"""))

display(spark.sql("""
SELECT
    Weekend_Flag,
    COUNT(*) AS Transactions,
    SUM(is_fraud) AS Fraud_Transactions
FROM fraud_transactions
GROUP BY Weekend_Flag
"""))

print("="*80)
print("Notebook 04 Completed Successfully")
print("="*80)

