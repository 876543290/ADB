# Databricks notebook source
commonPath = f"/mnt/movies8/"

rawPath = commonPath + "raw/"
rawData = [file.path for file in dbutils.fs.ls("dbfs:"+commonPath)]
bronzePath = commonPath + "bronze/"
silverPath = commonPath + "silver/"
silverQuarantinePath = commonPath + "silverQuarantine/"
moviePath = commonPath + "movieSilver/"
genresPath = commonPath + "genresSilver/"
originalLanguagePath = commonPath + "originalLanguageSilver/"

# COMMAND ----------

spark.sql(f"CREATE DATABASE IF NOT EXISTS dbacademy_movies8")
spark.sql(f"USE dbacademy_movies8")
