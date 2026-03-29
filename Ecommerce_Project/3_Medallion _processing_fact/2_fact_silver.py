# Databricks notebook source
# MAGIC %md
# MAGIC # CLEANING ORDER_ITEMS 

# COMMAND ----------

import pyspark.sql.functions as F
from pyspark.sql.types import StringType , DateType , TimestampType,FloatType
catalog_name='ecommerce'

# COMMAND ----------

df_silver = spark.table(f"{catalog_name}.bronze.brz_orderitems")
display(df_silver)

# COMMAND ----------

df_silver = df_silver.dropDuplicates(["order_id", "item_seq"])
display(df_silver)

# COMMAND ----------

df_silver.printSchema()

# COMMAND ----------

# Transformation : Remove '%' from discount_pct and cast to double
df_silver = df_silver.withColumn(
    "discount_pct",
    F.regexp_replace("discount_pct", "%", "").cast("double")
)

# COMMAND ----------

from pyspark.sql.functions import to_timestamp, col

# Transformation: datatype conversions
# 1) Convert dt (string → date)
df_silver = df_silver.withColumn(
    "dt",
    F.to_date("dt", "yyyy-MM-dd")     
)

# 2) Convert order_ts (string → timestamp)
df_silver = df_silver.withColumn(
    "order_ts",
    F.coalesce(
        F.to_timestamp("order_ts", "yyyy-MM-dd HH:mm:ss"),  # matches 2025-08-01 22:53:52
        F.to_timestamp("order_ts", "dd-MM-yyyy HH:mm")      # fallback for 01-08-2025 22:53
    )
)
display(df_silver)

# COMMAND ----------

# 4) Convert tax_amount (string → double, strip non-numeric characters)
df_silver = df_silver.withColumn(
    "tax_amount",
    F.regexp_replace("tax_amount", r"[^0-9.\-]", "").cast("double")
)


#Transformation : Add processed time 
df_silver = df_silver.withColumn(
    "processed_time", F.current_timestamp()
)

# COMMAND ----------

from pyspark.sql.functions import regexp_extract, col

# Extract only numbers from 'unit_price' column
df_silver = df_silver.withColumn("unit_price", regexp_extract(col("unit_price"), r"\d+", 0))

display(df_silver)

# COMMAND ----------

# DBTITLE 1,Cell 12
# Transformation : coupon code processing (convert to lower)
df_silver = df_silver.withColumn(
    "coupon_code", F.lower(F.trim(F.col("coupon_code")))
)

# COMMAND ----------

# Transformation : channel processing 
df_silver = df_silver.withColumn(
    "channel",
    F.when(F.col("channel") == "web", "Website")
    .when(F.col("channel") == "app", "Mobile")
    .otherwise(F.col("channel")),
)

# COMMAND ----------

# DBTITLE 1,Cell 12
# Transformation : Remove '%' from discount_pct and cast to double
df_silver = df_silver.withColumn(
    "discount_pct",
    F.regexp_replace("discount_pct", "%", "").cast("double")
)

# COMMAND ----------

#detect anamoly
df_silver.select("quantity").distinct().show()

# COMMAND ----------



#anamolies dictionary
anomalies =  {
    "Two" : "2"
}

df_silver=df_silver.replace(anomalies,subset="quantity")
df_silver.select("quantity").distinct().show()

# COMMAND ----------


df_silver = df_silver.withColumn("order_id", F.col("order_id").cast("int"))

df_silver = df_silver.withColumn("item_seq", F.col("item_seq").cast("int"))

df_silver = df_silver.withColumn("tax_amount", F.col("tax_amount").cast("int"))

df_silver = df_silver.withColumn("unit_price", F.col("unit_price").cast("int"))

df_silver = df_silver.withColumn("quantity", F.col("quantity").cast("int"))

# COMMAND ----------

# DBTITLE 1,Cell 16
# from spark data frame to delta table
df_silver.write.format("delta")\
    .mode("overwrite")\
    .option("overwriteSchema","true")\
    .saveAsTable(f"{catalog_name}.silver.slv_order_items")