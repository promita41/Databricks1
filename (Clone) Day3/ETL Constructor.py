# Databricks notebook source
df=spark.read.json("dbfs:/mnt/adlsshelldatabricks/raw/jsonfiles/constructors.json")

# COMMAND ----------

df.display()

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

df1=df.withColumn("ingestiondate",current_timestamp())

# COMMAND ----------

df1.write.saveAsTable("promita.Constructors")

# COMMAND ----------

# MAGIC %sql
# MAGIC create table promita.drivers as
# MAGIC select * from json.`dbfs:/mnt/adlsshelldatabricks/raw/jsonfiles/drivers.json`

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from hive_metastore.promita.drivers

# COMMAND ----------

# MAGIC %sql
# MAGIC use catalog `hive_metastore`; select * from `promita`.`drivers` limit 100;
