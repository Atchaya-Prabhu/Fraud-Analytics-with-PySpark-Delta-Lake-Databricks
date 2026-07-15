# Databricks notebook source
# ============================================================================
# Project      : Credit Card Fraud Analytics Pipeline using PySpark
# Notebook     : 03_Feature_Engineering
# Layer        : Silver → Gold
# Author       : Atchaya P
# Environment  : Databricks Serverless
# ============================================================================

from pyspark.sql.functions import *
from pyspark.sql.types import *

# -----------------------------------------------------------------------------
# Read Dataset
# -----------------------------------------------------------------------------

TRAIN_DATA_PATH="/Workspace/Users/atchaya.analyst@gmail.com/Fraud Analytics/fraudTrain.csv"

transactions_df=(
    spark.read
         .format("csv")
         .option("header",True)
         .option("inferSchema",True)
         .load(TRAIN_DATA_PATH)
)

display(transactions_df)

# =============================================================================
# Customer Full Name
# =============================================================================

transactions_df=transactions_df.withColumn(
    "Customer_Name",
    concat_ws(" ",col("first"),col("last"))
)

# =============================================================================
# Customer Age
# =============================================================================

transactions_df=transactions_df.withColumn(
    "Customer_Age",
    floor(months_between(current_date(),col("dob"))/12)
)

# =============================================================================
# Transaction Date
# =============================================================================

transactions_df=transactions_df.withColumn(
    "Transaction_Date",
    to_date(col("trans_date_trans_time"))
)

# =============================================================================
# Transaction Year
# =============================================================================

transactions_df=transactions_df.withColumn(
    "Transaction_Year",
    year("Transaction_Date")
)

# =============================================================================
# Transaction Month
# =============================================================================

transactions_df=transactions_df.withColumn(
    "Transaction_Month",
    month("Transaction_Date")
)

# =============================================================================
# Month Name
# =============================================================================

transactions_df=transactions_df.withColumn(
    "Month_Name",
    date_format("Transaction_Date","MMMM")
)

# =============================================================================
# Day Name
# =============================================================================

transactions_df=transactions_df.withColumn(
    "Day_Name",
    date_format("Transaction_Date","EEEE")
)

# =============================================================================
# Transaction Hour
# =============================================================================

transactions_df=transactions_df.withColumn(
    "Transaction_Hour",
    hour("trans_date_trans_time")
)

# =============================================================================
# Weekend Flag
# =============================================================================

transactions_df=transactions_df.withColumn(
    "Weekend_Flag",
    when(dayofweek("Transaction_Date").isin(1,7),"Yes")
    .otherwise("No")
)

# =============================================================================
# Peak Hour Flag
# =============================================================================

transactions_df=transactions_df.withColumn(
    "Peak_Hour",
    when(
        (col("Transaction_Hour")>=18) &
        (col("Transaction_Hour")<=22),
        "Yes"
    ).otherwise("No")
)

# =============================================================================
# High Value Transaction
# =============================================================================

transactions_df=transactions_df.withColumn(
    "High_Value_Flag",
    when(col("amt")>=1000,"Yes")
    .otherwise("No")
)

# =============================================================================
# Amount Bucket
# =============================================================================

transactions_df=transactions_df.withColumn(
    "Amount_Bucket",
    when(col("amt")<100,"Small")
    .when(col("amt")<500,"Medium")
    .when(col("amt")<1000,"Large")
    .otherwise("Very Large")
)

# =============================================================================
# Fraud Risk Category
# =============================================================================

transactions_df=transactions_df.withColumn(
    "Fraud_Risk",
    when(col("amt")>=2000,"High")
    .when(col("amt")>=1000,"Medium")
    .otherwise("Low")
)

# =============================================================================
# Customer Segment
# =============================================================================

transactions_df=transactions_df.withColumn(
    "Customer_Segment",
    when(col("city_pop")<50000,"Rural")
    .when(col("city_pop")<250000,"Urban")
    .otherwise("Metro")
)

# =============================================================================
# Merchant State
# =============================================================================

transactions_df=transactions_df.withColumn(
    "Merchant_State",
    upper(col("state"))
)

# =============================================================================
# Age Group
# =============================================================================

transactions_df=transactions_df.withColumn(
    "Age_Group",
    when(col("Customer_Age")<25,"18-24")
    .when(col("Customer_Age")<35,"25-34")
    .when(col("Customer_Age")<45,"35-44")
    .when(col("Customer_Age")<55,"45-54")
    .otherwise("55+")
)

# =============================================================================
# Transaction Status
# =============================================================================

transactions_df=transactions_df.withColumn(
    "Transaction_Status",
    when(col("is_fraud")==1,"Fraud")
    .otherwise("Genuine")
)

# =============================================================================
# Transaction ID Length
# =============================================================================

transactions_df=transactions_df.withColumn(
    "Transaction_ID_Length",
    length("trans_num")
)

# =============================================================================
# Merchant Name Length
# =============================================================================

transactions_df=transactions_df.withColumn(
    "Merchant_Name_Length",
    length("merchant")
)

# =============================================================================
# Display Engineered Dataset
# =============================================================================

display(transactions_df)

# =============================================================================
# Validate Features
# =============================================================================

display(
    transactions_df.groupBy("Amount_Bucket")
                   .count()
)

display(
    transactions_df.groupBy("Age_Group")
                   .count()
)

display(
    transactions_df.groupBy("Fraud_Risk")
                   .count()
)

display(
    transactions_df.groupBy("Weekend_Flag")
                   .count()
)

display(
    transactions_df.groupBy("Peak_Hour")
                   .count()
)

# =============================================================================
# Register SQL View
# =============================================================================

transactions_df.createOrReplaceTempView("fraud_features")

# =============================================================================
# SQL Validation
# =============================================================================

display(
spark.sql("""

SELECT
Amount_Bucket,
COUNT(*) AS Transactions,
ROUND(AVG(amt),2) Average_Amount
FROM fraud_features
GROUP BY Amount_Bucket
ORDER BY Average_Amount DESC

""")
)

print("="*80)
print("Feature Engineering Completed Successfully")
print("="*80)
