# Databricks notebook source
import pyspark.sql.functions as F
from pyspark.sql.types import StringType , IntegerType ,DateType ,TimestampType , FloatType
from pyspark.sql import Row

# COMMAND ----------

catalog_name='ecommerce'


# COMMAND ----------

df_products=(f"{catalog_name}.silver.slv_products")
df_brands=spark.table(f"{catalog_name}.silver.slv_brands")
df_category=(f"{catalog_name}.silver.slv_category")
df_customer=(f"{catalog_name}.silver.slv_customer")
df_date=(f"{catalog_name}.silver.slv_date")
df_order_items=(f"{catalog_name}.silver.slv_order_items")

# COMMAND ----------

# DBTITLE 1,Views
# for joins using views
df_products = spark.table(df_products)
df_products.createOrReplaceTempView("v_products")
df_brands.createOrReplaceTempView("v_brands")
df_category = spark.table(df_category)
df_category.createOrReplaceTempView("v_category")

# COMMAND ----------

display(spark.sql("""select * from v_products limit 5 """))

# COMMAND ----------

display(spark.sql("""select * from v_category limit 5 """))

# COMMAND ----------

display(spark.sql("""select * from v_brands limit 5 """))

# COMMAND ----------

#make sure correct catalog
spark.sql(f"USE CATALOG {catalog_name}")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- creating new table gld_dim_products in gold layer
# MAGIC CREATE OR REPLACE TABLE gold.gld_dim_products AS
# MAGIC WITH brand_categories AS (
# MAGIC     SELECT
# MAGIC         b.brand_name,
# MAGIC         b.brand_code,
# MAGIC         c.category_name,
# MAGIC         c.category_code
# MAGIC     FROM  v_brands b
# MAGIC     INNER JOIN v_category c
# MAGIC     ON
# MAGIC         b.category_code=c.category_code
# MAGIC )
# MAGIC select 
# MAGIC     p.product_id,
# MAGIC     p.sku,
# MAGIC     COALESCE(bc.category_name,'Not_Available') AS category_name,
# MAGIC     p.category_code,
# MAGIC     p.brand_code,
# MAGIC     COALESCE(bc.brand_name,'Not_Available') AS brand_name,
# MAGIC     p.color,
# MAGIC     p.size,
# MAGIC     p.material,
# MAGIC     p.weight_grams,
# MAGIC     p.height_cm,
# MAGIC     p.rating_count,
# MAGIC     p._source_file,
# MAGIC     p.ignored_at
# MAGIC from v_products p
# MAGIC LEFT join brand_categories bc
# MAGIC on  
# MAGIC     p.brand_code = bc.brand_code;

# COMMAND ----------

# India states
india_region = {
    "MH": "West", "GJ": "West", "RJ": "West",
    "KA": "South", "TN": "South", "TS": "South", "AP": "South", "KL": "South",
    "UP": "North", "WB": "North", "DL": "North"
}

# Australia states
australia_region = {
    "VIC": "SouthEast", "WA": "West", "NSW": "East", "QLD": "NorthEast"
}

# United Kingdom states
uk_region = {
    "ENG": "England", "WLS": "Wales", "NIR": "Northern Ireland", "SCT": "Scotland"
}

# United States states
us_region = {
    "MA": "NorthEast", "FL": "South", "NJ": "NorthEast", "CA": "West",
    "NY": "NorthEast", "TX": "South"
}

# UAE states
uae_region = {
    "AUH": "Abu Dhabi", "DU": "Dubai", "SHJ": "Sharjah"
}

# Singapore states
singapore_region = {
    "SG": "Singapore"
}

# Canada states
canada_region = {
    "BC": "West", "AB": "West", "ON": "East", "QC": "East", "NS": "East", "IL": "Other"
}

# Combine into a master dictionary
country_state_map = {
    "India": india_region,
    "Australia": australia_region,
    "United Kingdom": uk_region,
    "United States": us_region,
    "United Arab Emirates": uae_region,
    "Singapore": singapore_region,
    "Canada": canada_region
}

# COMMAND ----------

country_state_map

# COMMAND ----------

# DBTITLE 1,Cell 12
# flatten country_state_map into a list of rows
row=[]
for country,states in country_state_map.items():
    for state_code ,region in states.items():
        row.append(Row(country=country,state=state_code,region=region))

row[:10]

# COMMAND ----------

#create mapping data frame 
df_region_mapping =spark.createDataFrame(row)

df_region_mapping.show(truncate=False)


# COMMAND ----------

df_silver=spark.table(f'{catalog_name}.silver.slv_customer')
display(df_silver.limit(5))

# COMMAND ----------

df_gold=df_silver.join(df_region_mapping,on=['country','state'],how="left")

df_gold = df_gold.fillna({'region':'other'})

display(df_gold.limit(5))

# COMMAND ----------

# Write raw data to the gold layer (catalog: ecommerce, schema: gold, table: gld_dim_customers)
df_gold.write.format("delta") \
    .mode("overwrite") \
    .option("mergeSchema", "true") \
    .saveAsTable(f"{catalog_name}.gold.gld_dim_customers")

# COMMAND ----------

# MAGIC %md
# MAGIC # date

# COMMAND ----------

df_silver = spark.table(f'{catalog_name}.silver.slv_date')
display(df_silver.limit(5))

# COMMAND ----------

df_gold = df_silver.withColumn("date_id", F.date_format(F.col("date"), "yyyyMMdd").cast("int"))

# Add month name (e.g., 'January', 'February', etc.)
df_gold = df_gold.withColumn("month_name", F.date_format(F.col("date"), "MMMM"))

# Add is_weekend column
df_gold = df_gold.withColumn(
    "is_weekend",
    F.when(F.col("day_name").isin("SATURDAY", "SUNDAY"), 1).otherwise(0)
)

display(df_gold.limit(10))

# COMMAND ----------

df_gold = df_gold.withColumn("day_name", F.upper(F.col("day_name")))

display(df_gold.limit(10))

# COMMAND ----------

desired_columns_order = ["date_id", "date", "year", "month_name", "day_name", "is_weekend", "quarter", "ignored_at", "_source_file"]

df_gold = df_gold.select(desired_columns_order)

display(df_gold.limit(5))

# COMMAND ----------

df_gold = df_gold.withColumn("year", F.col("year").cast("int"))

df_gold = df_gold.withColumn("quarter", F.col("quarter").cast("int"))
display(df_gold.limit(5))

# COMMAND ----------

df_gold = df_gold.withColumn("week", F.weekofyear(F.col("date")))

display(df_gold.limit(5))

# COMMAND ----------

# DBTITLE 1,Cell 23
# write table to gold layer

df_gold.write.format("delta") \
    .mode("overwrite") \
    .option("mergeSchema", "true") \
    .saveAsTable(f"{catalog_name}.gold.gld_dim_date")

# COMMAND ----------

