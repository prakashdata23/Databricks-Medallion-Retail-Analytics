# Databricks notebook source
from pyspark.sql.types import StructType , StructField , StringType , IntegerType , DateType , TimestampType , FloatType

import pyspark.sql.functions as F

# COMMAND ----------

# DBTITLE 1,Brands schema
catalog_name='ecommerce'

# brands schema definition
brand_schema=StructType([
    StructField("brand_code",StringType(),False),
    StructField("brand_name",StringType(),True),
    StructField("category_code",StringType(),True)
])

# getting path
raw_data_path = "/Volumes/ecommerce/source_data/raw/brands/*.csv"
#making as DF
df=spark.read.option('header','true').option("delimiter",",").schema(brand_schema).csv(raw_data_path)

#add meta columns

df=df.withColumn("_source_file",F.col("_metadata.file_path"))\
    .withColumn("ignored_at",F.current_timestamp())
display(df.limit(5))

# from spark data frame to delta table
df.write.format("delta")\
    .mode("overwrite")\
        .option("mergeSchema","true")\
        .saveAsTable(f"{catalog_name}.bronze.brz_brands")



# COMMAND ----------

# DBTITLE 1,Category schema
catalog_name='ecommerce'

# category schema definition
category_schema=StructType([
    StructField("category_code", StringType(), False),
    StructField("category_name", StringType(), True)
])

# getting path
raw_data_path = "/Volumes/ecommerce/source_data/raw/category/*.csv"
#making as DF
df=spark.read.option('header','true').option("delimiter",",").schema(category_schema).csv(raw_data_path)

#add meta columns

df=df.withColumn("_source_file",F.col("_metadata.file_path"))\
    .withColumn("ignored_at",F.current_timestamp())
display(df.limit(5))

# from spark data frame to delta table
df.write.format("delta")\
    .mode("overwrite")\
        .option("mergeSchema","true")\
        .saveAsTable(f"{catalog_name}.bronze.brz_category")

# COMMAND ----------

# DBTITLE 1,Customer Schema
catalog_name='ecommerce'

# customers schema definition
customers_schema=StructType([
    StructField("customer_id", StringType(), False),
    StructField("phone", StringType(), True),
    StructField("country_code", StringType(), True),
    StructField("country", StringType(), True),
    StructField("state", StringType(), True),
])

# getting path
raw_data_path = "/Volumes/ecommerce/source_data/raw/customers/*.csv"

#making as DF
df=spark.read.option('header','true').option("delimiter",",").schema(customers_schema).csv(raw_data_path)

#add meta columns
df=df.withColumn("_source_file",F.col("_metadata.file_path"))\
    .withColumn("ignored_at",F.current_timestamp())
display(df.limit(5))

# from spark data frame to delta table
df.write.format("delta")\
    .mode("overwrite")\
        .option("mergeSchema","true")\
        .saveAsTable(f"{catalog_name}.bronze.brz_customer")

# COMMAND ----------

# DBTITLE 1,Date Schema
catalog_name='ecommerce'

# date schema definition
date_schema=StructType([
    StructField("date", StringType(), True),
    StructField("year", StringType(), False),
    StructField("day_name", StringType(), True),
    StructField("quarter", StringType(), False),
    StructField("week_of_year", StringType(), False),
])

# getting path
raw_data_path = "/Volumes/ecommerce/source_data/raw/date/*.csv"

#making as DF
df=spark.read.option('header','true').option("delimiter",",").schema(date_schema).csv(raw_data_path)

#add meta columns
df=df.withColumn("_source_file",F.col("_metadata.file_path"))\
    .withColumn("ignored_at",F.current_timestamp())
display(df.limit(5))

# from spark data frame to delta table
df.write.format("delta")\
    .mode("overwrite")\
        .saveAsTable(f"{catalog_name}.bronze.brz_date")

# COMMAND ----------

# DBTITLE 1,Order_Items schema
catalog_name='ecommerce'

# order_items schema definition

order_items_schema=StructType([
    StructField("dt", StringType(), True),
    StructField("order_ts", StringType(), True),
    StructField("customer_id", StringType(), True),
    StructField("order_id", StringType(), True),
    StructField("item_seq", StringType(), False),
    StructField("product_id", StringType(), True),
    StructField("quantity", StringType(), False),
    StructField("unit_price_currency", StringType(), True),
    StructField("unit_price", StringType(), False),
    StructField("discount_pct", StringType(), False),
    StructField("tax_amount", StringType(), False),
    StructField("channel", StringType(), True),
    StructField("coupon_code", StringType(), True),
])

# getting path
raw_data_path = "/Volumes/ecommerce/source_data/raw/order_items/landing/*.csv"

#making as DF
df=spark.read.option('header','true').option("delimiter",",").schema(order_items_schema).csv(raw_data_path)

#add meta columns
df=df.withColumn("_source_file",F.col("_metadata.file_path"))\
    .withColumn("ignored_at",F.current_timestamp())
display(df.limit(5))

# from spark data frame to delta table
df.write.format("delta")\
    .mode("overwrite")\
        .option("mergeSchema","true")\
        .saveAsTable(f"{catalog_name}.bronze.brz_orderitems")

# COMMAND ----------

# DBTITLE 1,Products  Schema
catalog_name='ecommerce'

# products schema definition
products_schema = StructType([
    StructField("product_id", StringType(), False),
    StructField("sku", StringType(), True),
    StructField("category_code", StringType(), True),
    StructField("brand_code", StringType(), True),
    StructField("color", StringType(), True),
    StructField("size", StringType(), True),
    StructField("material", StringType(), True),
    StructField("weight_grams", StringType(), True),  #datatype is string due to incoming data contain anamolies
    StructField("length_cm", StringType(), True),    #datatype is string due to incoming data contain anamolies
    StructField("width_cm", FloatType(), True),
    StructField("height_cm", FloatType(), True),
    StructField("rating_count", IntegerType(), True),
    StructField("file_name", StringType(), False),
    StructField("ingest_timestamp", TimestampType(), False)
])

# getting path
raw_data_path = "/Volumes/ecommerce/source_data/raw/products/products.csv"

#making as DF
df=spark.read.option('header','true').option("delimiter",",").schema(products_schema).csv(raw_data_path)

#add meta columns
df=df.withColumn("_source_file",F.col("_metadata.file_path"))\
    .withColumn("ignored_at",F.current_timestamp())
display(df.limit(5))

# from spark data frame to delta table
df.write.format("delta")\
    .mode("overwrite")\
        .option("mergeSchema","true")\
        .saveAsTable(f"{catalog_name}.bronze.brz_products")

# COMMAND ----------

