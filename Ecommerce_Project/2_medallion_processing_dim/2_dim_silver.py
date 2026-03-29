# Databricks notebook source
import pyspark.sql.functions as F
from pyspark.sql.types import StringType , DateType , TimestampType,FloatType
catalog_name='ecommerce'

# COMMAND ----------

# MAGIC %md
# MAGIC # CLEANING BRANDS

# COMMAND ----------

# DBTITLE 1,TABLE TO DF
df_bronze = spark.table(f"{catalog_name}.bronze.brz_brands")
df_bronze.show()

# COMMAND ----------

# DBTITLE 1,Trimming
df_silver=df_bronze.withColumn('brand_name',F.trim(F.col('brand_name')))
df_silver.show()

# COMMAND ----------

# DBTITLE 1,RegEXP
df_silver = df_silver.withColumn(
    'brand_code',
    F.regexp_replace(F.col('brand_code'), '[^a-zA-Z0-9]', '')
)
display(df_silver)

# COMMAND ----------

# DBTITLE 1,Anomalies detect
df_silver.select("category_code").distinct().show()


# COMMAND ----------

# DBTITLE 1,Anomalies correct
#anamolies dictionary
anomalies =  {
    "GROCERY" : "GRCY",
    "BOOKS" : "BKS",
    "TOYS" : "TOY"
}

df_silver=df_silver.replace(anomalies,subset="category_code")
df_silver.select("category_code").distinct().show()

# COMMAND ----------

# DBTITLE 1,remove a column
df_silver = df_silver.drop('category_name')
display(df_silver)

# COMMAND ----------

# DBTITLE 1,Null count
null_counts = df_silver.select([F.count(F.when(F.col(c).isNull(), c)).alias(c) for c in df_silver.columns])
display(null_counts)

# COMMAND ----------

# DBTITLE 1,writing to table
# from spark data frame to delta table
df_silver.write.format("delta")\
    .mode("overwrite")\
    .option("mergeSchema","true")\
    .saveAsTable(f"{catalog_name}.silver.slv_brands")

# COMMAND ----------

# MAGIC %md
# MAGIC # CLEANING CATEGORY

# COMMAND ----------

df_bronze = spark.table(f"{catalog_name}.bronze.brz_category")
display(df_bronze)



# COMMAND ----------

df_silver = df_bronze.withColumn("category_code", F.upper(F.col("category_code")))
display(df_silver)

# COMMAND ----------

df_duplicates = df_bronze.groupBy("category_code").count().filter(F.col("count") > 1)
display(df_duplicates)

# COMMAND ----------

df_silver = df_bronze.dropDuplicates(['category_code'])
display(df_silver)

# COMMAND ----------

df_silver = df_silver.withColumn("category_code", F.upper(F.col("category_code")))
display(df_silver)

# COMMAND ----------

null_counts = df_silver.select([F.count(F.when(F.col(c).isNull(), c)).alias(c) for c in df_silver.columns])
display(null_counts)

# COMMAND ----------

# DBTITLE 1,Writing to delta table
# from spark data frame to delta table
df_silver.write.format("delta")\
    .mode("overwrite")\
    .option("mergeSchema","true")\
    .saveAsTable(f"{catalog_name}.silver.slv_category")

# COMMAND ----------

# MAGIC %md
# MAGIC # CLEANING CUSTOMER 

# COMMAND ----------

import pyspark.sql.functions as F
from pyspark.sql.types import StringType , DateType , TimestampType,FloatType

catalog_name='ecommerce'

df_bronze = spark.table(f"{catalog_name}.bronze.brz_customer")

# Get row and column count
row_count, column_count = df_bronze.count(), len(df_bronze.columns)

# Print the results
print(f"Row count: {row_count}")
print(f"Column count: {column_count}")

df_bronze.show(10)

# COMMAND ----------

# MAGIC %md
# MAGIC Handle NULL values in customer_id column

# COMMAND ----------

null_count = df_bronze.filter(F.col("customer_id").isNull()).count()
null_count

# COMMAND ----------

# DBTITLE 1,Remove null
df_silver = df_bronze.dropna(subset="customer_id")
display(df_silver)

# COMMAND ----------

# MAGIC %md
# MAGIC Handle NULL values in phone column

# COMMAND ----------

null_count = df_silver.filter(F.col("phone").isNull()).count()
print(f"Number of nulls in phone: {null_count}") 

# COMMAND ----------

# DBTITLE 1,UNKNOWN PHONE
from pyspark.sql.functions import when, col

df_silver = df_silver.withColumn("phone", when(col("phone").isNull(), "Not Available").otherwise(col("phone")))

null_count = df_silver.filter(F.col("phone").isNull()).count()
print(f"Number of nulls in phone: {null_count}") 


# COMMAND ----------

# DBTITLE 1,split
from pyspark.sql.functions import split

df_silver = df_silver.withColumn("phone", split(col("phone"), "\.")[0])
display(df_silver)

# COMMAND ----------

# DBTITLE 1,writing to delta table
# from spark data frame to delta table
df_silver.write.format("delta")\
    .mode("overwrite")\
    .option("mergeSchema","true")\
    .saveAsTable(f"{catalog_name}.silver.slv_customer")

# COMMAND ----------

# MAGIC %md
# MAGIC # CLEANING DATE 

# COMMAND ----------

df_bronze = spark.table(f"{catalog_name}.bronze.brz_date")
# Get row and column count
row_count, column_count = df_bronze.count(), len(df_bronze.columns)

# Print the results
print(f"Row count: {row_count}")
print(f"Column count: {column_count}")

df_bronze.show(3)

# COMMAND ----------

print(df_silver.printSchema())

df_silver.show(5)

# COMMAND ----------

# DBTITLE 1,null removal
df_silver = df_bronze.dropna()
display(df_silver)

# COMMAND ----------

null_counts = df_silver.select([F.count(F.when(F.col(c).isNull(), c)).alias(c) for c in df_silver.columns])
display(null_counts)

# COMMAND ----------

# MAGIC %md
# MAGIC Remove Duplicates

# COMMAND ----------

# Find duplicate rows in the DataFrame
duplicates = df_silver.groupBy('date').count().filter("count > 1")

# Show the duplicate rows
print("Total duplicated Rows: ", duplicates.count())
display(duplicates)

# COMMAND ----------

# DBTITLE 1,Standardization(upper)
df_silver = df_silver.withColumn("day_name", F.initcap(F.col("day_name")))
display(df_silver)

# COMMAND ----------

# DBTITLE 1,dtype conversion
df_silver = df_silver.withColumn("year", F.col("year").cast("int"))

df_silver = df_silver.withColumn("quarter", F.col("quarter").cast("int"))

df_silver = df_silver.withColumn("week_of_year", F.col("week_of_year").cast("int"))



display(df_silver)

# COMMAND ----------

# DBTITLE 1,writing to delta table
# MAGIC %md
# MAGIC # from spark data frame to delta table
# MAGIC df_silver.write.format("delta")\
# MAGIC     .mode("overwrite")\
# MAGIC     .option("overwriteSchema","true")\
# MAGIC     .saveAsTable(f"{catalog_name}.silver.slv_date")

# COMMAND ----------

df_silver = df_silver.withColumn("quarter", F.concat_ws("", F.concat(F.lit("Q"), F.col("quarter"), F.lit("-"), F.col("year"))))

df_silver = df_silver.withColumn("week_of_year", F.concat_ws("-", F.concat(F.lit("Week"), F.col("week_of_year"), F.lit("-"), F.col("year"))))

df_silver.show(3)

# COMMAND ----------

from pyspark.sql.functions import to_date

# Convert the string column to a date type
df_silver = df_bronze.withColumn("date", to_date(df_bronze["date"], "dd-MM-yyyy"))

# COMMAND ----------

# DBTITLE 1,Absolute function
from pyspark.sql.functions import abs, col

# Overwriting the existing column with its absolute value
df_silver = df_silver.withColumn("week_of_year", abs(col("week_of_year")))

# Check the results
display(df_silver)

# COMMAND ----------

# Rename a column
df_silver = df_silver.withColumnRenamed("week_of_year", "week")

# COMMAND ----------

# DBTITLE 1,writing to delta table
# from spark data frame to delta table
df_silver.write.format("delta")\
    .mode("overwrite")\
    .option("overwriteSchema","true")\
    .saveAsTable(f"{catalog_name}.silver.slv_date")

# COMMAND ----------

# MAGIC %md
# MAGIC # Cleaning products
# MAGIC

# COMMAND ----------

df_bronze = spark.table(f"{catalog_name}.bronze.brz_products")

# Get row and column count
row_count, column_count = df_bronze.count(), len(df_bronze.columns)

# Print the results
print(f"Row count: {row_count}")
print(f"Column count: {column_count}")


# COMMAND ----------

display(df_bronze.limit(5))

# COMMAND ----------

# DBTITLE 1,dropping multiple columns
columns_to_drop = ["product_name", "price", "currency", "active", "file_name", "ingest_timestamp"]

df_silver = df_bronze.drop(*columns_to_drop)
display(df_silver)

# COMMAND ----------

# MAGIC %md
# MAGIC Check `length_cm` (comma instead of dot)

# COMMAND ----------

df_silver.select("length_cm").show(3)

# COMMAND ----------

from pyspark.sql.functions import regexp_replace, col

df_silver = df_silver.withColumn("length_cm", regexp_replace(col("length_cm"), ",", "."))
display(df_silver)

# COMMAND ----------

df_silver = df_silver.withColumn("length_cm", F.col("length_cm").cast("float"))

# COMMAND ----------

# MAGIC %md
# MAGIC Check `weight_grams` (contains 'g')

# COMMAND ----------

# Check weight_grams column
df_bronze.select("weight_grams").show(5, truncate=False)

# COMMAND ----------

# DBTITLE 1,string to int
df_silver = df_silver.withColumn("weight_grams", F.col("weight_grams").cast("int"))

# COMMAND ----------

# DBTITLE 1,reg exp extract
from pyspark.sql.functions import regexp_extract, col

df_silver = df_silver.withColumn("weight_grams", regexp_extract(col("weight_grams"), r"\d+", 0))
display(df_silver)

# COMMAND ----------

# MAGIC %md
# MAGIC Negative values in `rating_count`

# COMMAND ----------

df_silver.filter(F.col('rating_count')<0).select("rating_count").show(3)


# COMMAND ----------

# Convert negative rating_count to positive
df_silver = df_silver.withColumn(
    "rating_count",
    F.when(F.col("rating_count").isNotNull(), F.abs(F.col("rating_count")))
     .otherwise(F.lit(0))  # if null, replace with 0
)

# COMMAND ----------

# DBTITLE 1,standardization
from pyspark.sql.functions import upper, col

df_silver = df_silver.withColumn("brand_code", upper(col("brand_code")))
df_silver = df_silver.withColumn("category_code", upper(col("category_code")))

display(df_silver)

# COMMAND ----------

# MAGIC %md
# MAGIC Spelling mistakes in `material` column

# COMMAND ----------

df_silver.select("material").distinct().show()

# COMMAND ----------

# Fix spelling mistakes
df_silver = df_silver.withColumn(
    "material",
    F.when(F.col("material") == "Coton", "Cotton")
     .when(F.col("material") == "Alumium", "Aluminum")
     .when(F.col("material") == "Ruber", "Rubber")
     .otherwise(F.col("material"))
)
df_silver.select("material").distinct().show()    

# COMMAND ----------

# DBTITLE 1,type casting
from pyspark.sql.functions import col

# Use decimal(10, 2) -> 10 total digits, 2 after the dot
df_silver = df_silver.withColumn("width_cm", col("width_cm").cast("decimal(10, 2)"))
df_silver = df_silver.withColumn("height_cm", col("height_cm").cast("decimal(10, 2)"))
df_silver = df_silver.withColumn("length_cm", col("length_cm").cast("decimal(10, 2)"))

display(df_silver)

# COMMAND ----------

# DBTITLE 1,writing to delta table
# from spark data frame to delta table
df_silver.write.format("delta")\
    .mode("overwrite")\
    .option("mergeSchema","true")\
    .saveAsTable(f"{catalog_name}.silver.slv_products")