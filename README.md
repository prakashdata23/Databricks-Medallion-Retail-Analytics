# 🛒 E-Commerce Medallion Architecture 
### End-to-End Data Engineering Pipeline on Databricks 

---

## 1. Executive Summary 
This project demonstrates the implementation of a scalable, production-ready data pipeline using **Databricks, PySpark, and Delta Lake**. By transitioning raw, fragmented E-Commerce CSV data through a structured **Medallion Architecture (Bronze, Silver, and Gold)**, the system provides a "Single Version of Truth" for executive and operational decision-making. The final output supports a multi-page strategic dashboard covering revenue performance, brand intelligence, and operational efficiency. 

## 2. System Setup & Data Strategy 
* **Catalog & Governance:** The project utilizes **Unity Catalog** with a dedicated catalog named `ecommerce`. 
* **Layered Storage:** Data is organized into three distinct schemas: `bronze`, `silver`, and `gold` to maintain data quality and lineage. 
* **Source Management:** Raw data is managed via **Databricks Volumes**, specifically utilizing the path `/Volumes/ecommerce/source_data/raw/` for landing zone ingestion. 

## 3. Project Structure & Repository Organization 
The repository is organized into core processing modules: 
* **`2_medallion_processing_dim/`**: Manages master data for Brands, Categories, Customers, Date, and Products. 
* **`3_Medallion_processing_fact/`**: Manages transactional order data and financial logic. 
    * `1_fact_bronze.py`: Raw data ingestion with explicit schema enforcement. 
    * `2_fact_silver.py`: Data cleaning, RegEx standardization, and anomaly handling. 
    * `3_fact_gold.py`: Calculation of Gross/Discount/Net amounts and FX conversion to INR. 
    * `fact_transactions_denorm`: A SQL view joining facts and dimensions for high-performance consumption. 

## 4. Technical Implementation Highlights 

### A. Bronze Layer: Reliability & Ingestion 
* **Schema Enforcement:** Used `StructType` to define data types upfront to prevent corrupt data entry. 
* **Lineage Tracking:** Added metadata columns like `file_name` and `ingest_timestamp` to ensure full auditability. 

### B. Silver Layer: Data Quality & Transformation 
* **Regex Cleaning:** Stripped special characters and standardized `discount_pct` for numeric consistency. 
* **Anomaly Correction:** Standardized categorical data (e.g., converting "Two" to `2`). 
* **Deduplication:** Ensured integrity by removing duplicates based on composite keys (`order_id` + `item_seq`). 

### C. Gold Layer: Analytics & Business Logic 
* **Financial Engine:** Developed multi-currency conversion logic to unify sales from 7 currencies into **INR**. 
* **Calculated Measures:** Engineered BI-ready columns for Gross Amount, Discount Amount, and Net Sales. 
* **Date Intelligence:** Built a date dimension featuring `is_weekend` flags and `yyyyMMdd` surrogate keys. 

## 5. Strategic Insights & Visualization 
The pipeline serves an interactive dashboard delivering: 
* **Executive Performance:** Monitors total revenue of **₹3.5B** and tracks the impact of an **8.67%** average discount. 
* **Product & Brand Health:** Analyzes Brand Equity by mapping revenue against customer ratings. 
* **Operational Dynamics:** Pinpoints peak sales hours via heatmaps and analyzes coupon usage (~34% efficiency). 

---
**Technologies Used:** `PySpark`, `Databricks SQL`, `Delta Lake`, `Unity Catalog`, `Python`, `RegEx`.
