# Databricks notebook source
# ============================================================================
# Project      : Credit Card Fraud Analytics Pipeline using PySpark
# Notebook     : 02_Data_Cleaning_Silver
# Layer        : Silver
# Author       : Atchaya P
# ============================================================================

from pyspark.sql.functions import *
from pyspark.sql.types import *

# -----------------------------------------------------------------------------
# Read Dataset
# -----------------------------------------------------------------------------

TRAIN_DATA_PATH = "/Workspace/Users/atchaya.analyst@gmail.com/Fraud Analytics/fraudTrain.csv"

transactions_df = (
    spark.read
         .format("csv")
         .option("header", True)
         .option("inferSchema", True)
         .load(TRAIN_DATA_PATH)
)

print("Dataset Loaded Successfully")

display(transactions_df)

# -----------------------------------------------------------------------------
# Total Records Before Cleaning
# -----------------------------------------------------------------------------

print("Total Records :", transactions_df.count())

# -----------------------------------------------------------------------------
# Remove Duplicate Records
# -----------------------------------------------------------------------------

transactions_df = transactions_df.dropDuplicates()

print("Records After Removing Duplicates :", transactions_df.count())

# -----------------------------------------------------------------------------
# Remove Leading and Trailing Spaces
# -----------------------------------------------------------------------------

string_columns = ["merchant","category","first","last","city","state","job"]

for column in string_columns:
    transactions_df = transactions_df.withColumn(column, trim(col(column)))

# -----------------------------------------------------------------------------
# Standardize Merchant Names
# Remove 'fraud_' prefix
# -----------------------------------------------------------------------------

transactions_df = transactions_df.withColumn(
    "merchant",
    regexp_replace(col("merchant"), "^fraud_", "")
)

# -----------------------------------------------------------------------------
# Standardize Category Names
# -----------------------------------------------------------------------------

transactions_df = transactions_df.withColumn(
    "category",
    initcap(col("category"))
)

# -----------------------------------------------------------------------------
# Standardize Gender
# -----------------------------------------------------------------------------

transactions_df = transactions_df.withColumn(
    "gender",
    upper(col("gender"))
)

# -----------------------------------------------------------------------------
# Handle Null Values
# -----------------------------------------------------------------------------

transactions_df = transactions_df.fillna({
    "merchant": "Unknown Merchant",
    "category": "Unknown",
    "city": "Unknown",
    "state": "Unknown",
    "job": "Unknown"
})

# -----------------------------------------------------------------------------
# Remove Invalid Transaction Amounts
# -----------------------------------------------------------------------------

transactions_df = transactions_df.filter(col("amt") > 0)

# -----------------------------------------------------------------------------
# Remove Invalid Population
# -----------------------------------------------------------------------------

transactions_df = transactions_df.filter(col("city_pop") > 0)

# -----------------------------------------------------------------------------
# Convert Transaction Timestamp
# -----------------------------------------------------------------------------

transactions_df = transactions_df.withColumn(
    "Transaction_Date",
    to_date(col("trans_date_trans_time"))
)

transactions_df = transactions_df.withColumn(
    "Transaction_Time",
    date_format(col("trans_date_trans_time"), "HH:mm:ss")
)

# -----------------------------------------------------------------------------
# Add Audit Columns
# -----------------------------------------------------------------------------

transactions_df = transactions_df.withColumn(
    "Load_Date",
    current_date()
)

transactions_df = transactions_df.withColumn(
    "Load_Timestamp",
    current_timestamp()
)

# -----------------------------------------------------------------------------
# Data Quality Check - Missing Values
# -----------------------------------------------------------------------------

missing_df = transactions_df.select([
    count(when(col(c).isNull(), c)).alias(c)
    for c in transactions_df.columns
])

display(missing_df)

# -----------------------------------------------------------------------------
# Verify Fraud Distribution
# -----------------------------------------------------------------------------

display(
    transactions_df.groupBy("is_fraud")
                   .count()
                   .orderBy("is_fraud")
)

# -----------------------------------------------------------------------------
# Category Distribution
# -----------------------------------------------------------------------------

display(
    transactions_df.groupBy("category")
                   .count()
                   .orderBy(desc("count"))
)

# -----------------------------------------------------------------------------
# Merchant Distribution
# -----------------------------------------------------------------------------

display(
    transactions_df.groupBy("merchant")
                   .count()
                   .orderBy(desc("count"))
                   .limit(20)
)

# -----------------------------------------------------------------------------
# Transaction Amount Statistics
# -----------------------------------------------------------------------------

display(
    transactions_df.select("amt").summary()
)

# -----------------------------------------------------------------------------
# Save Cleaned DataFrame as Temporary View
# -----------------------------------------------------------------------------

transactions_df.createOrReplaceTempView("silver_transactions")

# -----------------------------------------------------------------------------
# SQL Validation
# -----------------------------------------------------------------------------

display(
    spark.sql("""
        SELECT
            category,
            COUNT(*) AS Total_Transactions,
            ROUND(AVG(amt),2) AS Avg_Amount
        FROM silver_transactions
        GROUP BY category
        ORDER BY Total_Transactions DESC
    """)
)

# -----------------------------------------------------------------------------
# Display Final Dataset
# -----------------------------------------------------------------------------

display(transactions_df)

