# Databricks notebook source
from pyspark.sql.session import SparkSession

# COMMAND ----------

dbutils.fs.unmount("/mnt/movies8")

# COMMAND ----------

dbutils.fs.mount(
  source = "wasbs://movies8@firstzcleonie.blob.core.windows.net",
  mount_point = "/mnt/movies8",
  extra_configs = {"fs.azure.account.key.firstzcleonie.blob.core.windows.net":"s2Q2V5MUGcxczUXrkBREcFcdLiDpzRPgo/0HeMHPlPQJGAfbXajwFcIK9gdmAjr0VT+XcepUfXRpJD2LYkr0uQ=="})
