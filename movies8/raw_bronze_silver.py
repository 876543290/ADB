# Databricks notebook source
# MAGIC %run ./mount

# COMMAND ----------

# MAGIC %run ./configuration

# COMMAND ----------

# MAGIC %run ./functions

# COMMAND ----------

#dbutils.fs.rm(f"{silverPath}/movies", recurse=True)
#dbutils.fs.rm(f"{silverPath}/genres", recurse=True)
#dbutils.fs.rm(f"{silverPath}/originalLanguage", recurse=True)

# COMMAND ----------

rawDF= read_batch_raw(rawPath)

# COMMAND ----------

dbutils.fs.rm(bronzePath, recurse=True)

# COMMAND ----------

#Ingestion Metadata

transformed_rawDF =transform_raw(rawDF)

# COMMAND ----------

#WRITE Batch to a Bronze Table
raw_to_bronze = batch_writer(transformed_rawDF)
raw_to_bronze=raw_to_bronze.save(bronzePath)

# COMMAND ----------

#Register the Bronze Table in the Metastore

spark.sql(
    """
DROP TABLE IF EXISTS movies8_bronze
"""
)

spark.sql(
    f"""
CREATE TABLE movies8_bronze
USING DELTA
LOCATION "{bronzePath}"
"""
)

# COMMAND ----------

bronzeDF=spark.read.table("movies8_bronze")

# COMMAND ----------

bronzeDF_new=bronzeDF.filter("status = 'new'")

# COMMAND ----------

transformed_silverDF = transform_bronze_movies(bronzeDF_new)

# COMMAND ----------

transformed_silverDF_clean = transformed_silverDF.filter(("RunTime > 0") and ("Budget >= 1000000"))
transformed_silverDF_quarantine = transformed_silverDF.filter(("Runtime <= 0") or ("Budget < 1000000"))


# COMMAND ----------

#WRITE Clean Batch to a Silver Table

dbutils.fs.rm(silverPath, recurse=True)

# COMMAND ----------

transformed_silverDF_clean = batch_writer(dataframe=transformed_silverDF_clean,
                                   exclude_columns=["movies"])
transformed_silverDF_clean=transformed_silverDF_clean.save(silverPath)

# COMMAND ----------

#create movies8 silver table

spark.sql(
    """
DROP TABLE IF EXISTS movies8_silver
"""
)

spark.sql(
    f"""
CREATE TABLE movies8_silver
USING DELTA
LOCATION "{silverPath}"
"""
)

# COMMAND ----------

#Clean records that have been loaded into the Silver table and should have their Bronze table status updated to "loaded"
#Update Quarantined records status to "quarantine"

update_bronze_table_status(spark, bronzePath, transformed_silverDF_clean, "loaded")
update_bronze_table_status(spark, bronzePath, transformed_silverDF_quarantine, "quarantined")

# COMMAND ----------

# Handle Quarantined Records
#Step 1: Load Quarantined Records from the Bronze Table

bronzeQuarantinedDF = bronzeDF.filter(
    "status = 'quarantined'"
)
display(bronzeQuarantinedDF)

# COMMAND ----------

#Step 2: Transform the Quarantined Records

silverRepairedDF = transform_bronze_movies(bronzeQuarantinedDF, quarantine=True)

# COMMAND ----------

#update repaired data to silver table

silverRepairedDF = batch_writer(
    dataframe=silverRepairedDF, exclude_columns=["Movies"]
)
silverRepairedDF.save(silverPath)


# COMMAND ----------

#update status on bronze table: 'quarantined' status to 'loaded' status

update_bronze_table_status(spark, bronzePath, bronzeQuaTransDF, "loaded")

# COMMAND ----------

silverGenreNotNullDF= get_genre_lookup_table(spark, "movies8_bronze") 

# COMMAND ----------

spark.sql(
    """
DROP TABLE IF EXISTS genre_silver
"""
)

spark.sql(
    f"""
CREATE TABLE genre_silver
USING DELTA
LOCATION "{genresPath}"
"""
)

# COMMAND ----------

silverLanguageIdDF=get_language_lookup_table(spark, "movies8_bronze")

# COMMAND ----------

spark.sql(
    """
DROP TABLE IF EXISTS language_silver
"""
)

spark.sql(
    f"""
CREATE TABLE language_silver
USING DELTA
LOCATION "{originalLanguagePath}"
"""
)
