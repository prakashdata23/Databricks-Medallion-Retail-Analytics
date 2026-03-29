# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE CATALOG IF NOT EXISTS ecommerce;

# COMMAND ----------

# MAGIC %sql
# MAGIC USE CATALOG ecommerce

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS ecommerce.bronze;
# MAGIC CREATE SCHEMA IF NOT EXISTS ecommerce.silver;
# MAGIC CREATE SCHEMA IF NOT EXISTS ecommerce.gold;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SHOW DATABASES FROM ecommerce

# COMMAND ----------

# if u want to drop a catalog
#%sql
#DROP CATALOG IF EXISTS ecommerce CASCADE; 

# COMMAND ----------

# GENERALLY DATA DOURCE FROM OLTP BUT HERE WE ARE USING CSV

# COMMAND ----------

# ORDER ITEM IS FACT TABLE ALL OTHER DIMENSION TABLE

# COMMAND ----------

