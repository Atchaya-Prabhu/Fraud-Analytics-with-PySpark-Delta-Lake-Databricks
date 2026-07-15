# Databricks notebook source
# ============================================================================
# Project      : Credit Card Fraud Analytics Pipeline using PySpark
# Notebook     : 01_Data_Ingestion_Bronze
# Layer        : Bronze
# Author       : Atchaya P
# Environment  : Databricks Serverless
# ============================================================================

from pyspark.sql.functions import *
from pyspark.sql.types import *

# -----------------------------------------------------------------------------
# Configuration
# -----------------------------------------------------------------------------

TRAIN_DATA_PATH = "/Workspace/Users/atchaya.analyst@gmail.com/Fraud Analytics/fraudTrain.csv"

TEST_DATA_PATH = "/Workspace/Users/atchaya.analyst@gmail.com/Fraud Analytics/fraudTest.csv"

print("="*80)
print("Credit Card Fraud Analytics Pipeline")
print("Notebook 01 - Data Ingestion (Bronze Layer)")
print("="*80)

# -----------------------------------------------------------------------------
# Read Training Dataset
# -----------------------------------------------------------------------------

train_df = (
    spark.read
         .format("csv")
         .option("header", True)
         .option("inferSchema", True)
         .load(TRAIN_DATA_PATH)
)

print("Training Dataset Loaded Successfully")

display(train_df)

# -----------------------------------------------------------------------------
# Read Testing Dataset
# -----------------------------------------------------------------------------

test_df = (
    spark.read
         .format("csv")
         .option("header", True)
         .option("inferSchema", True)
         .load(TEST_DATA_PATH)
)

print("Testing Dataset Loaded Successfully")

display(test_df)

# -----------------------------------------------------------------------------
# Schema
# -----------------------------------------------------------------------------

print("Training Schema")

train_df.printSchema()

print("Testing Schema")

test_df.printSchema()

# -----------------------------------------------------------------------------
# Record Counts
# -----------------------------------------------------------------------------

train_count = train_df.count()

test_count = test_df.count()

print(f"Training Records : {train_count:,}")

print(f"Testing Records  : {test_count:,}")

# -----------------------------------------------------------------------------
# Number of Columns
# -----------------------------------------------------------------------------

print("Training Columns :", len(train_df.columns))

print("Testing Columns  :", len(test_df.columns))

# -----------------------------------------------------------------------------
# Column Names
# -----------------------------------------------------------------------------

print("="*80)

print("Training Dataset Columns")

print("="*80)

for c in train_df.columns:

    print(c)

# -----------------------------------------------------------------------------
# Display Sample Records
# -----------------------------------------------------------------------------

display(train_df.limit(10))

# -----------------------------------------------------------------------------
# Missing Values
# -----------------------------------------------------------------------------

missing_values = train_df.select([

    count(

        when(col(c).isNull(), c)

    ).alias(c)

    for c in train_df.columns

])

display(missing_values)

# -----------------------------------------------------------------------------
# Duplicate Check
# -----------------------------------------------------------------------------

total_records = train_df.count()

distinct_records = train_df.distinct().count()

duplicate_records = total_records - distinct_records

print("Total Records      :", total_records)

print("Distinct Records   :", distinct_records)

print("Duplicate Records  :", duplicate_records)

# -----------------------------------------------------------------------------
# Summary Statistics
# -----------------------------------------------------------------------------

display(train_df.describe())

# -----------------------------------------------------------------------------
# Fraud Distribution
# -----------------------------------------------------------------------------

display(

    train_df.groupBy("is_fraud")
            .count()
            .orderBy("is_fraud")

)

# -----------------------------------------------------------------------------
# Gender Distribution
# -----------------------------------------------------------------------------

display(

    train_df.groupBy("gender")
            .count()

)

# -----------------------------------------------------------------------------
# Category Distribution
# -----------------------------------------------------------------------------

display(

    train_df.groupBy("category")
            .count()
            .orderBy(desc("count"))

)

# -----------------------------------------------------------------------------
# Top 20 Merchants
# -----------------------------------------------------------------------------

display(

    train_df.groupBy("merchant")
            .count()
            .orderBy(desc("count"))
            .limit(20)

)

# -----------------------------------------------------------------------------
# Top 20 Cities
# -----------------------------------------------------------------------------

display(

    train_df.groupBy("city")
            .count()
            .orderBy(desc("count"))
            .limit(20)

)

# -----------------------------------------------------------------------------
# Top States
# -----------------------------------------------------------------------------

display(

    train_df.groupBy("state")
            .count()
            .orderBy(desc("count"))

)

# -----------------------------------------------------------------------------
# Amount Statistics
# -----------------------------------------------------------------------------

display(

    train_df.select("amt")
            .summary()

)

# -----------------------------------------------------------------------------
# Transaction Category Revenue
# -----------------------------------------------------------------------------

display(

    train_df.groupBy("category")
            .agg(

                round(sum("amt"),2).alias("Total_Revenue"),

                round(avg("amt"),2).alias("Average_Amount"),

                count("*").alias("Transactions")

            )
            .orderBy(desc("Total_Revenue"))

)

# -----------------------------------------------------------------------------
# Create SQL View
# -----------------------------------------------------------------------------

train_df.createOrReplaceTempView("fraud_transactions")

# -----------------------------------------------------------------------------
# SQL Query
# -----------------------------------------------------------------------------

display(

spark.sql("""

SELECT
category,
COUNT(*) AS Transactions,
ROUND(AVG(amt),2) AS Average_Amount,
ROUND(SUM(amt),2) AS Revenue

FROM fraud_transactions

GROUP BY category

ORDER BY Revenue DESC

""")

)

# -----------------------------------------------------------------------------
# Explain Execution Plan
# -----------------------------------------------------------------------------

train_df.explain()
