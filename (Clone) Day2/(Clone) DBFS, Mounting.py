# Databricks notebook source
dbutils.fs.help()

# COMMAND ----------

# MAGIC %md
# MAGIC ####Mounting
# MAGIC  - 1. Access Key
# MAGIC  - 2. SAS
# MAGIC  - 3. Service Principal

# COMMAND ----------

# MAGIC %fs ls

# COMMAND ----------

# MAGIC %fs ls dbfs:/mnt/adlsshelldatabricks/raw/jsonfiles/

# COMMAND ----------

df=spark.read.option("header",True).csv("dbfs:/mnt/adlsshelldatabricks/raw/jsonfiles/circuits.csv")


# COMMAND ----------

df.display()

# COMMAND ----------

df=spark.read.option("header",True).option("inferschema",True).csv("dbfs:/mnt/adlsshelldatabricks/raw/jsonfiles/circuits.csv")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from csv.`dbfs:/mnt/adlsshelldatabricks/raw/jsonfiles/circuits.csv`

# COMMAND ----------


