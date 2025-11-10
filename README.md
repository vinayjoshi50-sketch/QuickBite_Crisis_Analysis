# QuickBite_Crisis_Analysis

Welcome to QuickBite_Crisis_Analysis repository! 🚀

This project demonstrates an end to end data warehousing and analytics model, from building a data warehouse to generating actionable insights from the problem statement. This Project involves data-ingestion, data-cleaning, data-modelling, insight generation and recommendations into the crisis that happened at QuickBite food-delivery chain.


## 📖 Project Overview

This project involves:

1. **Data Architecture**: Designing a Modern Data Warehouse Using Medallion Architecture **Bronze**, **Silver**, and **Gold** layers.
2. **ETL Pipelines**: Extracting, transforming, and loading data from source systems into the warehouse.
3. **Data Modeling**: Developing fact and dimension tables optimized for analytical queries.
4. **Analytics & Reporting**: Creating SQL-based reports and dashboards for actionable insights.

## 🏗️ Data Architecture (Data Engineering)

The data architecture for this project follows Medallion Architecture **Bronze**, **Silver**, and **Gold** layers:

![Data Architecture](Database/data_architecture.png)

1. **Bronze Layer**: Stores raw data as-is from the source systems. Data is ingested from CSV Files into SQL Server Database.
2. **Silver Layer**: This layer includes data cleansing, standardization, and normalization processes to prepare data for analysis.
3. **Gold Layer**: Houses business-ready required for reporting and analytics.

### BI: Analytics & Reporting (Data Analysis)


![Recovery Dashboard](BI%20&%20Reporting/Recovery%20Dashboard.png) 

#### Objective
Develop SQL-based analytics to deliver detailed insights into:
- **Customer Behavior & ratings**
- **Order & Sales Trends**
- **Citywise Analysis**

These insights empower stakeholders with key business metrics, enabling strategic decision-making. 







   
