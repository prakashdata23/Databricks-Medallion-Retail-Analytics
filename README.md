# 🛒 Ecommerce Analytics: Medallion Architecture on Databricks

## 🚀 Project Overview
This project demonstrates a full-cycle **Medallion Architecture** (Bronze → Silver → Gold) implemented in Databricks. I engineered pipelines to transform raw transactional data into a denormalized Gold Layer (OBT) to power an AI-augmented dashboard via **Databricks Genie**.

## 📂 Repository Structure
* **1_Setup/**: Environment and cluster configuration.
* **2_Medallion_processing_dim/**: Bronze, Silver, and Gold notebooks for Dimensions.
* **3_Medallion_processing_fact/**: Pipelines for Fact tables and the final SQL Gold View.
* **4_Dashboard/**: Visualizations and insights.

## 📊 Dashboard Insights
### 1. Executive Sales Performance
* **Total Revenue:** ₹3.5B
* **KPIs:** Monitoring daily revenue trends against discount impact to maintain margins.

### 2. Product & Brand Intelligence
* **Brand Equity Matrix:** Identifying high-performing brands using Revenue vs. Rating analysis.

### 3. Operational & Channel Insights
* **Peak Performance:** Hourly sales heatmap to identify high-traffic windows.
* **Channel Efficiency:** Comparing promo usage between Mobile and Website users.

## 🛠️ Tech Stack
* **Platform:** Databricks (Lakehouse)
* **Architecture:** Medallion (Delta Lake)
* **Language:** Spark SQL / Python
* **BI Tool:** Databricks Genie
