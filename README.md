# Credit Card Fraud Analytics with PySpark & Databricks

## Project Overview

This project demonstrates an end-to-end fraud analytics pipeline built using **PySpark** on **Databricks**. The goal is to transform raw credit card transaction data into business-ready insights by performing data ingestion, cleaning, feature engineering, analytical processing, and dashboard-ready reporting.

The project follows a typical data engineering workflow and showcases how PySpark can efficiently process large-scale transaction datasets while generating insights for fraud monitoring and business decision-making.

---

# Project Architecture

Raw CSV Dataset
        ↓
Data Ingestion
        ↓
Data Cleaning
        ↓
Feature Engineering
        ↓
Business Analytics
        ↓
Dashboard Ready Tables

---

# Technologies Used

- PySpark
- Databricks
- Spark SQL
- Python
- Window Functions
- DataFrame API
- Kaggle Dataset
- GitHub

---

# Dataset

**Source**

Credit Card Fraud Detection Dataset (Kaggle)

The dataset contains historical credit card transactions, customer information, merchant details, locations, transaction timestamps, and fraud labels.

---

# Project Structure

```
Fraud-Analytics-with-PySpark-Databricks/

│── 01_Data_Ingestion.py
│── 02_Data_Cleaning.py
│── 03_Feature_Engineering.py
│── 04_Data_Analysis.py
│── 05_Analytics_Dashboard.py
│
│── README.md
│── requirements.txt
│
└── images/
```

---

# Notebook Description

## 01 – Data Ingestion

Purpose

- Load CSV data into PySpark DataFrames
- Infer schema
- Inspect dataset
- Validate data loading

Why?

Data ingestion is the first step of any ETL pipeline. It enables distributed processing of raw datasets using Spark.

---

## 02 – Data Cleaning

Purpose

- Handle null values
- Remove duplicates
- Standardize data
- Validate data quality

Why?

Clean data improves the reliability of downstream analytics and minimizes processing errors.

---

## 03 – Feature Engineering

Purpose

New analytical features were created including:

- Customer Name
- Transaction Date
- Transaction Hour
- Month Name
- Weekend Flag

Why?

Feature engineering transforms raw attributes into business-friendly fields that simplify reporting and analysis.

---

## 04 – Data Analysis

Purpose

Perform exploratory data analysis using PySpark DataFrame operations.

Examples include:

- Revenue Analysis
- Fraud Analysis
- State-wise Analysis
- Category Analysis
- Transaction Distribution

Why?

EDA helps identify trends, anomalies, and business patterns before dashboard development.

---

## 05 – Analytics Dashboard

Purpose

Generate dashboard-ready analytical tables using PySpark.

Includes

- Executive KPIs
- Revenue by Category
- Fraud Rate by Category
- Revenue by State
- Monthly Revenue Trend
- Monthly Fraud Trend
- Weekend vs Weekday Analysis
- Peak Transaction Hour
- Spark SQL Analytics
- Window Functions

Why?

Business users require summarized datasets instead of raw transactional data. This notebook prepares data for reporting tools such as Power BI or Databricks dashboards.

---

# PySpark Concepts Demonstrated

- DataFrame API
- Aggregations
- Filtering
- Column Transformations
- Window Functions
- Spark SQL
- GroupBy Operations
- Date Functions
- Conditional Logic
- Business KPI Generation

---

# Business Use Cases

This project can help financial organizations:

- Monitor fraudulent transaction patterns
- Identify high-risk merchant categories
- Analyze customer transaction behavior
- Track monthly revenue trends
- Compare weekday and weekend spending
- Monitor regional revenue distribution
- Support fraud investigation teams
- Build executive dashboards for stakeholders

---

# Dashboard Visualizations

## Revenue by Category

![Revenue by Category](images/revenue_by_category.png)

---

## Fraud Transactions by Category

![Fraud by Category](images/fraud_by_category.png)

---

## Monthly Revenue Trend

![Monthly Revenue](images/monthly_revenue.png)

---

## Weekend vs Weekday Transactions

![Weekend vs Weekday](images/weekend_weekday_transaction.png)

---

# Key Insights

- Grocery and shopping categories generate the highest transaction revenue.
- Fraud activity varies across transaction categories.
- Transaction volumes differ between weekdays and weekends.
- Monthly revenue trends reveal seasonal changes in customer spending.
- State-level analysis helps identify regional business performance.

---

# Conclusion

This project demonstrates how PySpark can be used to build scalable analytics pipelines on Databricks. By combining data ingestion, cleaning, feature engineering, Spark SQL, window functions, and business analytics, the solution transforms raw transaction data into meaningful insights that support fraud detection and business decision-making.

The project also illustrates common data engineering practices for preparing analytical datasets that can be consumed by visualization tools such as Power BI or Databricks dashboards.

---

# Future Enhancements

- Delta Lake implementation
- Incremental data loading
- Streaming fraud detection
- Machine Learning fraud prediction
- Automated ETL pipelines
- Azure Data Factory orchestration

---

# Author

**Atchaya P**

Data Analyst | Business Intelligence | Aspiring Data Engineer
