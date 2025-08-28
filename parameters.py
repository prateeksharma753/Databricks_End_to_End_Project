# Databricks notebook source
datasets =[
    {
        "name": "p_file_name",
        "value": "customers"

    },
    {
        "name": "p_file_name",
        "value": "orders"
    },
    {
        "name": "p_file_name",
        "value": "regions"
    },
    {
        "name": "p_file_name",
        "value": "products"
    }
]

# COMMAND ----------

dbutils.jobs.taskValues.set("output_datasets", datasets)

# COMMAND ----------

# MAGIC %md
# MAGIC demo
