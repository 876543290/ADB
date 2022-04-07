# Databricks notebook source
from pyspark.sql.session import SparkSession
from urllib.request import urlretrieve
from pyspark.sql.functions import *
from datetime import datetime
import time

from delta.tables import DeltaTable
from pyspark.sql import DataFrame
from typing import List
from pyspark.sql.window import Window


# COMMAND ----------

dbutils.fs.unmount("/mnt/movies8")

# COMMAND ----------

dbutils.fs.mount(
  source = "wasbs://movies8@firstzcleonie.blob.core.windows.net",
  mount_point = "/mnt/movies8",
  extra_configs = {"fs.azure.account.key.firstzcleonie.blob.core.windows.net":"s2Q2V5MUGcxczUXrkBREcFcdLiDpzRPgo/0HeMHPlPQJGAfbXajwFcIK9gdmAjr0VT+XcepUfXRpJD2LYkr0uQ=="})

# COMMAND ----------

commonPath = f"/mnt/movies8/"

rawPath = commonPath + "raw/"
rawData = [file.path for file in dbutils.fs.ls("dbfs:"+commonPath)]
bronzePath = commonPath + "bronze/"
silverPath = commonPath + "silver/"
silverQuarantinePath = commonPath + "silverQuarantine/"
moviePath = commonPath + "movieSilver/"
genresPath = commonPath + "genresSilver/"
originalLanguagePath = commonPath + "originalLanguageSilver/"
movie_genres_Path = commonPath + "movieGenresSilver/"

# COMMAND ----------

spark.sql(f"CREATE DATABASE IF NOT EXISTS dbacademy_movies8")
spark.sql(f"USE dbacademy_movies8")

# COMMAND ----------

rawDF = (spark.read
         .option("multiline", "true")
         .format("json")
         .load(rawData)
         .select(explode("movie").alias("movies")))

# COMMAND ----------

rawDF.display()

# COMMAND ----------

#ingest raw

##Make Notebook Idempotent

#dbutils.fs.rm(commonPath, recurse=True)


# COMMAND ----------

dbutils.fs.rm(bronzePath, recurse=True)

# COMMAND ----------

#Ingestion Metadata

transformed_rawDF = (
  rawDF.select(
    "movies",
    lit("/mnt/movies8").alias("datasource"),
    current_timestamp().alias("ingesttime"),
    lit("new").alias("status"),
    current_timestamp().cast("date").alias("ingestdate"),
  )
)

# COMMAND ----------

display(transformed_rawDF)

# COMMAND ----------

#WRITE Batch to a Bronze Table

(
    transformed_rawDF.select(
  "datasource",
  "ingesttime",
  "movies",
  "status",
  "ingestdate")
.write.format("delta").mode("append").partitionBy("ingestdate").save(bronzePath)
)

# COMMAND ----------

display(dbutils.fs.ls(bronzePath))

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

# MAGIC %sql
# MAGIC 
# MAGIC SELECT * FROM movies8_bronze

# COMMAND ----------

bronzeDF=spark.read.table("movies8_bronze")

# COMMAND ----------

bronzeDF_new=bronzeDF.filter("status = 'new'")

# COMMAND ----------

# Extract the Nested JSON from the Bronze Records

silverDF=bronzeDF_new.select("movies", "movies.BackdropUrl","movies.Budget","movies.OriginalLanguage","movies.CreatedBy","movies.Id","movies.Price","movies.Revenue","movies.RunTime")

# COMMAND ----------

display(silverDF)

# COMMAND ----------

transformed_silverDF = silverDF.select(
    "movies",
    col("Id").cast("integer").alias("movie_id"),
    "Budget",
    "OriginalLanguage",
    "Revenue",
    "Price",
    col("RunTime").cast("integer")
).dropDuplicates()


# COMMAND ----------

transformed_silverDF.count()

# COMMAND ----------

#Split the Silver DataFrame

transformed_silverDF_clean = transformed_silverDF.filter(("RunTime > 0") and ("Budget >= 1000000"))
transformed_silverDF_quarantine = transformed_silverDF.filter(("Runtime <= 0") or ("Budget < 1000000"))



# COMMAND ----------

display(transformed_silverDF_quarantine)

# COMMAND ----------

#WRITE Clean Batch to a Silver Table

dbutils.fs.rm(silverPath, recurse=True)

# COMMAND ----------

(
    transformed_silverDF_clean.select(
       "movie_id", "Budget", "OriginalLanguage", "Revenue", "Price","RunTime"
    )
    .write.format("delta")
    .mode("append")
    .save(silverPath)
)

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

# MAGIC %sql
# MAGIC 
# MAGIC SELECT * FROM movies8_silver

# COMMAND ----------

# Update Bronze table to Reflect the Loads
#Clean records that have been loaded into the Silver table and should have their Bronze table status updated to "loaded".

bronzeTable = DeltaTable.forPath(spark, bronzePath)
silverAugmented = transformed_silverDF_clean.withColumn("status", lit("loaded"))

update_match = "bronze.movies = clean.movies"
update = {"status": "clean.status"}

(
    bronzeTable.alias("bronze")
    .merge(silverAugmented.alias("clean"), update_match)
    .whenMatchedUpdate(set=update)
    .execute()
)


# COMMAND ----------

#Update Quarantined records status to "quarantine"

silverAugmented = transformed_silverDF_quarantine.withColumn(
    "status", lit("quarantined")
)

update_match = "bronze.movies = quarantine.movies"
update = {"status": "quarantine.status"}

(
    bronzeTable.alias("bronze")
    .merge(silverAugmented.alias("quarantine"), update_match)
    .whenMatchedUpdate(set=update)
    .execute()
)

# COMMAND ----------

# Handle Quarantined Records
#Step 1: Load Quarantined Records from the Bronze Table

bronzeQuarantinedDF = bronzeDF.filter(
    "status = 'quarantined'"
)
display(bronzeQuarantinedDF)

# COMMAND ----------

#Step 2: Transform the Quarantined Records
#not changing data type in this step
#change data type in update silver step

bronzQuarTransDF=bronzeQuarantinedDF.select("movies", "movies.BackdropUrl","movies.Budget","movies.OriginalLanguage","movies.CreatedBy","movies.Id","movies.Price","movies.Revenue","movies.RunTime")
bronzQuarTransDF = bronzQuarTransDF.select(
    "movies",
    col("Id").cast("integer").alias("movie_id"), ##this data type changed because there's no fix needed
    "Budget", ##col("Budget").cast("integer") ## which is different from the 'not quarantine' scenario
    "OriginalLanguage",
    "Revenue",
    "Price",
    "RunTime"
    ##col("RunTime").cast("integer") ##which is different from the 'not quarantine' scenario
).dropDuplicates()


# COMMAND ----------

#Step 3: repair the quarantined table

repairDF=bronzQuarTransDF.withColumn("Budget",lit(1000000)).withColumn("RunTime",abs(bronzQuarTransDF.RunTime))

# COMMAND ----------

repairDF.show(10)

# COMMAND ----------

#step 3.1 change datatype that not changed in step2 and select to the silverRepairedDF
silverRepairedDF=repairDF.select(
    "movies",
    "movie_id",
    col("Budget").cast("Double"),
    "OriginalLanguage",
    "Revenue",
    "Price",
    col("RunTime").cast("integer")
)
silverRepairedDF.show(10)

# COMMAND ----------

##step4 update repaired data to silver table
(
    silverRepairedDF.select(
        "movie_id", "Budget", "OriginalLanguage", "Revenue", "Price","RunTime"
    )
    .write.format("delta")
    .mode("append")
    .save(silverPath)
)

# COMMAND ----------

#step4.1 update status on bronze table: 'quarantined' status to 'loaded' status
bronzeTable = DeltaTable.forPath(spark, bronzePath)
silverAugmented = silverRepairedDF.withColumn("status", lit("loaded"))

update_match = "bronze.movies = clean.movies"
update = {"status": "clean.status"}

(
    bronzeTable.alias("bronze")
    .merge(silverAugmented.alias("clean"), update_match)
    .whenMatchedUpdate(set=update)
    .execute()
)


# COMMAND ----------

#Display the Quarantined Records
#If the update was successful, there should be no quarantined records in the Bronze table.

display(bronzeQuarantinedDF)

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from movies8_silver

# COMMAND ----------

# MAGIC %sql
# MAGIC select distinct OriginalLanguage from movies8_silver

# COMMAND ----------

## to build a genre look up table
#1.extract data from movies8_bronze's "movies" column which includes metadata: movies,movie genre id, movie genre name
#2,transform this selected movies8_bronze into genre_silver that eliminate null value of the movie genre name column, and select distinct genre_id
#3,create a genre lookup table and save genre data into it


# COMMAND ----------

##step1 extract data from movies8_bronze table and get only genre info, transform
bronzeDF=spark.read.table("movies8_bronze")
silverGenreDF=bronzeDF.select(explode("movies.genres").alias("gen"))
silverGenreDF=silverGenreDF.select("gen.id","gen.name").na.drop()
silverGenreDF=silverGenreDF.dropDuplicates()
silverGenreNotNullDF = silverGenreDF.withColumn('name', when(col('name') == '', None).otherwise(col('name'))).na.drop()

# COMMAND ----------

##step2 write batch to genresilver  path
(
    silverGenreNotNullDF.select(
        "id", "name"
    )
    .write.format("delta")
    .mode("overwrite")
    .save(genresPath)
)

# COMMAND ----------

##step3 create a silver table for genre_silver
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

# MAGIC %sql
# MAGIC select * from genre_silver

# COMMAND ----------

##same process for language
##step1 extract data from movies8_bronze table and get only language info, transform
bronzeDF=spark.read.table("movies8_bronze")
silverLanguageDF=bronzeDF.select("movies.Originallanguage").na.drop().distinct()
silverLanguageIdDF=silverLanguageDF.withColumn("id",monotonically_increasing_id())

# COMMAND ----------


##step2 batch write to languageSilverPath
(
    silverLanguageIdDF.select(
        "id", "Originallanguage"
    )
    .write.format("delta")
    .mode("append")
    .save(originalLanguagePath)
)

# COMMAND ----------

##step3 create a silver table
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

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from language_silver

# COMMAND ----------


