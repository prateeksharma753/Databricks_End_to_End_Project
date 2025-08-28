# Databricks notebook source
dbutils.widgets.text("p_file_name",'')

# COMMAND ----------

dbutils.widgets.get("p_file_name")

# COMMAND ----------

# MAGIC %md
# MAGIC reading

# COMMAND ----------

# MAGIC %sql
# MAGIC select
# MAGIC   *
# MAGIC from
# MAGIC   read_files('abfss://source@dldatabricksproject.dfs.core.windows.net/regions')
# MAGIC limit 50;

# COMMAND ----------

# MAGIC %md
# MAGIC writing

# COMMAND ----------

df= spark.readStream.format("cloudFiles")\
    .option("cloudFiles.format","parquet")\
    .option("cloudFiles.schemaLocation",f"abfss://bronze@dldatabricksproject.dfs.core.windows.net/checkpoint_{p_file_name}")\
        .load(f"abfss://source@dldatabricksproject.dfs.core.windows.net/{p_file_name}")

# COMMAND ----------

df = spark.writeStream.format("parquet")\
    .outputMode("append")\
    .option("checkpointLocation",f"abfss://bronze@dldatabricksproject.dfs.core.windows.net/checkpoint_{p_file_name}")\
        .option("path",f"abfss://bronze@dldatabricksproject.dfs.core.windows.net/{p_file_name}")\
            .trigger(once=True)\
            .start()

# COMMAND ----------

df = spark.read.fromat("parquet")\
    .load(f"abfss://bronze@dldatabricksproject.dfs.core.windows.net/{p_file_name}")
df.display()

# COMMAND ----------

df.count()
