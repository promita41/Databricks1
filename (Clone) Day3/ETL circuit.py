# Databricks notebook source
df=spark.read.csv("dbfs:/mnt/adlsshelldatabricks/raw/jsonfiles/circuits.csv",header=True)


# COMMAND ----------

df.display()

# COMMAND ----------

df=spark.read.csv("dbfs:/mnt/adlsshelldatabricks/raw/jsonfiles/circuits.csv",header=True, inferSchema=True)

# COMMAND ----------

df.show()

# COMMAND ----------

df.display()

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

df.select(col("circuitid").alias("circuit_id"),"*").display()

# COMMAND ----------

df1= df.select(col("circuitid").alias("circuit_id"),"*")

# COMMAND ----------

df1.display()

# COMMAND ----------

df1=df.withColumnRenamed("circuitId","circuit_ID")

# COMMAND ----------

df1.display()


# COMMAND ----------

df.withColumn("ingestiondate",current_timestamp()).display()

# COMMAND ----------

df1=df.withColumn("ingestiondate",current_timestamp()).drop("url")

# COMMAND ----------

df1.display()

# COMMAND ----------

df1.write.parquet("dbfs:/mnt/adlsshelldatabricks/raw/processed/promita/circuit")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from parquet.`dbfs:/mnt/adlsshelldatabricks/raw/processed/promita/circuit`

# COMMAND ----------

# MAGIC %sql
# MAGIC create schema promita

# COMMAND ----------

df1.write.saveAsTable("promita.circuit")

# COMMAND ----------

hive_metastore.promita.circuithive_metastore.promita.circuit

# COMMAND ----------

# MAGIC %sql
# MAGIC use catalog `hive_metastore`; select * from `promita`.`circuit` limit 100;
