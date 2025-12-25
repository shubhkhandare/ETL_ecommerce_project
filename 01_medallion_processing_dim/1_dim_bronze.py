# Databricks notebook source
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType,FloatType
import pyspark.sql.functions as F

# COMMAND ----------

catalog_name = 'ecommerce'

brand_schema = StructType([
    StructField('brand_id', StringType(),True),
    StructField('brand_name', StringType(),True),
    StructField('category_code', StringType(),True)
])

# COMMAND ----------

# MAGIC %md
# MAGIC ##brands

# COMMAND ----------

raw_data_path = "/Volumes/ecommerce/raw/source_date/brands/*.csv"

df = spark.read.option("header", "true").option("delimiter", ",").schema(brand_schema).csv(raw_data_path)

df = df.withColumn("_source_file",F.col("_metadata.file_path"))\
    .withColumn("ingestion_time", F.current_timestamp())

display(df)

# COMMAND ----------

df.write\
    .mode("overwrite")\
    .format("delta")\
    .option("overwriteSchema", "true")\
    .saveAsTable(f"{catalog_name}.bronze.bronze_brand")

# COMMAND ----------

# MAGIC %sql
# MAGIC use catalog `ecommerce`; select * from `bronze`.`bronze_brand` limit 100;

# COMMAND ----------

# MAGIC %md
# MAGIC ## category

# COMMAND ----------

category_schema = StructType([
    StructField('category_id', StringType(),True),
    StructField('category_name', StringType(),True),
])

# COMMAND ----------

category_path = "/Volumes/ecommerce/raw/source_date/category/*.csv"

df = spark.read.option("header", "true").option("delimiter", ",").schema(category_schema).csv(category_path)


# COMMAND ----------

from pyspark.sql.functions import col, current_timestamp

df = df.withColumn(
    "_source_file",
    col("_metadata.file_path")
).withColumn(
    "ingestion_time",
    current_timestamp()
)

display(df)

# COMMAND ----------

df.write\
    .mode("overwrite")\
    .format("delta")\
    .option("overwriteSchema", "true")\
    .saveAsTable(f"{catalog_name}.bronze.bronze_category")

# COMMAND ----------

# MAGIC %md
# MAGIC ##customers

# COMMAND ----------

path = "/Volumes/ecommerce/raw/source_date/customers/*.csv"

# df = spark.read.option("header", "true").option("delimiter", ",").csv(path)

# df.display()


# COMMAND ----------

customers_schema = StructType([
    StructField('customer_id', StringType(),True),
    StructField('phone', StringType(),True),
    StructField('country_code', StringType(),True),
    StructField('country', StringType(),True),
    StructField('state', StringType(),True),
])

# COMMAND ----------

df = spark.read.option("header", "true").option("delimiter", ",").schema(customers_schema).csv(path)
df = df.withColumn("_source_file",
    col("_metadata.file_path")
).withColumn(
    "ingestion_time",
    current_timestamp())

df.show(5)

# COMMAND ----------

df.write\
    .mode("overwrite")\
    .format("delta")\
    .option("overwriteSchema", "true")\
    .saveAsTable(f"{catalog_name}.bronze.bronze_customers")


# COMMAND ----------

# MAGIC %md
# MAGIC ##date

# COMMAND ----------

path = "/Volumes/ecommerce/raw/source_date/date/*.csv"
# df = spark.read.option("header", True).csv(path)
# df.show(3)

# COMMAND ----------

date_schema = StructType([
    StructField('date', StringType(),True),
    StructField('year', StringType(),True),
    StructField('day_name', StringType(),True),
    StructField('quarter', StringType(),True),
    StructField('week_of_year', StringType(),True),
])
df = spark.read.option("header", True).option("delimiter", ",").schema(date_schema).csv(path)

df = df.withColumn("_source_file",
    col("_metadata.file_path")
).withColumn(
    "ingestion_time",
    current_timestamp())

df.write\
    .mode("overwrite")\
    .format("delta")\
    .option("overwriteSchema", "true")\
    .saveAsTable(f"{catalog_name}.bronze.bronze_date")


# COMMAND ----------

df.show(3)
