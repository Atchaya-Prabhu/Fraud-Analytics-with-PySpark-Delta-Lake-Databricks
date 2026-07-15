# 🚀 Credit Card Fraud Analytics with PySpark & Databricks

![PySpark](https://img.shields.io/badge/PySpark-3.5-orange?style=for-the-badge)
![Databricks](https://img.shields.io/badge/Databricks-Serverless-red?style=for-the-badge)
![Spark SQL](https://img.shields.io/badge/Spark-SQL-yellow?style=for-the-badge)
![Python](https://img.shields.io/badge/Python-3.12-blue?style=for-the-badge)

---

# 📌 Project Overview

This project demonstrates an end-to-end fraud analytics pipeline built using **PySpark** and **Databricks**. It transforms raw credit card transaction data into dashboard-ready datasets through scalable data processing, feature engineering, Spark SQL analytics, and business reporting.

---

# 📊 Dashboard Preview

## 💰 Revenue by Category

![Revenue by Category](image/Revenue%20by%20Category.png)

---

## 🚨 Fraud Transactions by Category

![Fraud by Category](image/Fraud%20Transactions%20by%20Category.png)

---

## 📈 Monthly Revenue Trend

![Monthly Revenue](image/Monthly%20Revenue.png)

---

## 📅 Weekend vs Weekday Transactions

![Weekend vs Weekday](image/Weekend%20vs%20Weekday%20Transactions.png)


---

# 🏗️ Project Architecture

```text
                 Kaggle Fraud Dataset
                         │
                         ▼
              01_Data_Ingestion.py
                         │
                         ▼
              02_Data_Cleaning.py
                         │
                         ▼
           03_Feature_Engineering.py
                         │
                         ▼
              04_Data_Analysis.py
                         │
                         ▼
         05_Analytics_Dashboard.py
                         │
                         ▼
        Dashboard Ready Business Tables
```

---

# 🛠️ Technologies

- PySpark
- Databricks
- Spark SQL
- Python
- Window Functions
- DataFrame API
- GitHub

---

# 📂 Project Structure

```text
Fraud-Analytics-with-PySpark-Databricks

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
    ├── revenue_by_category.png
    ├── fraud_by_category.png
    ├── monthly_revenue.png
    └── weekend_vs_weekday_transactions.png
```

---

# 💼 Business Use Cases

- Detect fraud patterns across transaction categories.
- Monitor monthly revenue trends.
- Compare weekday and weekend customer behaviour.
- Analyze revenue performance by state.
- Generate executive KPIs.
- Prepare dashboard-ready datasets for BI tools.

---

# 📌 Key Insights

- Grocery and shopping categories generated the highest transaction revenue.
- Fraud activity varied across transaction categories.
- Monthly revenue revealed seasonal spending trends.
- Weekday transactions exceeded weekend transactions.
- State-level analysis highlighted regional business performance.

---

# 📝 Conclusion

This project demonstrates how PySpark can be used to build a scalable fraud analytics pipeline on Databricks. The workflow includes data ingestion, cleaning, feature engineering, Spark SQL analysis, and dashboard-ready reporting, providing practical experience with data engineering concepts used in analytics projects.

---

# 👩‍💻 Author

**Atchaya P**

Data Analyst | Business Intelligence Developer | Aspiring Data Engineer
