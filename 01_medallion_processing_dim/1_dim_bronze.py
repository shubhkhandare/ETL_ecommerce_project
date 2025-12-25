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
