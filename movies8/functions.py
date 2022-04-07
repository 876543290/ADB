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

def read_batch_raw(rawPath: str) -> DataFrame:
    return spark.read.option("multiline", "true").format("json").load(rawData).select(explode("movie").alias("movies"))

# COMMAND ----------


def transform_raw(rawDF: DataFrame) -> DataFrame:
    return (rawDF.select(
            "movies",
            lit("/mnt/movies8").alias("datasource"),
            current_timestamp().alias("ingesttime"),
            lit("new").alias("status"),
            current_timestamp().cast("date").alias("ingestdate"))
    )
    


# COMMAND ----------


def batch_writer(
    dataframe: DataFrame,
    exclude_columns: List = [],
    mode: str = "append",
) -> DataFrame:
    return (
        dataframe.drop(
            *exclude_columns
        )  # This uses Python argument unpacking (https://docs.python.org/3/tutorial/controlflow.html#unpacking-argument-lists)
        .write.format("delta")
        .mode(mode)
        #.partitionBy("ingestdate")
    )


# COMMAND ----------

def transform_bronze_movies(bronze: DataFrame, quarantine: bool = False) -> DataFrame:

    silverDF= bronzeDF_new.select("movies", "movies.BackdropUrl","movies.Budget","movies.OriginalLanguage","movies.CreatedBy","movies.Id","movies.Price","movies.Revenue","movies.RunTime")
    
    if not quarantine:
        transformed_silverDF = silverDF.select(
    "movies",
    col("Id").cast("integer").alias("movie_id"),
    "Budget",
    "OriginalLanguage",
    "Revenue",
    "Price",
    col("RunTime").cast("integer")
).dropDuplicates()
       
    else:
        repairDF = silverDF.withColumn("Budget", lit(1000000.0))
#         silver_movies = silver_movies.withColumn("Runtime", abs(silver_movies.Runtime))
        repairDF = silverDF.select(
            col("Id").cast("integer").alias("Movie_Id"),
            "Title",
            "Overview",
            "Budget",
            abs(col("RunTime").cast("integer")).alias("Runtime"),
            "Movies"
        ).dropDuplicates()

    return silver_movies


# COMMAND ----------

def update_bronze_table_status(
    spark: SparkSession, bronzeTablePath: str, dataframe: DataFrame, status: str
) -> bool:

    bronzeTable = DeltaTable.forPath(spark, bronzePath)
    silverAugmented = dataframe.withColumn("status", lit(status))

    update_match = "bronze.movies = dataframe.movies"
    update = {"status": "dataframe.status"}

    (
        bronzeTable.alias("bronze")
        .merge(dataframeAugmented.alias("dataframe"), update_match)
        .whenMatchedUpdate(set=update)
        .execute()
    )

    return True


# COMMAND ----------


def get_genre_lookup_table(
    spark: SparkSession, bronzeTable: str
) -> DataFrame:
    bronzeDF=spark.read.table(bronzeTable)
    silverGenreDF=bronzeDF.select(explode("movies.genres").alias("gen"))
    silverGenreDF=silverGenreDF.select("gen.id","gen.name").na.drop()
    silverGenreDF=silverGenreDF.dropDuplicates()
    silverGenreNotNullDF = silverGenreDF.withColumn('name', when(col('name') == '', None).otherwise(col('name'))).na.drop()
    silverGenreNotNullDF = silverGenreNotNullDF.select("id", "name" )
    return silverGenreNotNullDF


# COMMAND ----------

def get_language_lookup_table(
    spark: SparkSession, bronzeTable: str
) -> DataFrame:
    bronzeDF=spark.read.table(bronzeTable)
    silverLanguageDF=bronzeDF.select("movies.Originallanguage").na.drop().distinct()
    silverLanguageIdDF=silverLanguageDF.withColumn("id",monotonically_increasing_id())
    silverLanguageIdDF=silverLanguageIdDF.select("id", "Originallanguage")
    return silverLanguageIdDF

# COMMAND ----------

def repair_quarantined_records(
    spark: SparkSession, bronzeTable: str
) -> DataFrame:
    bronzeQuarantinedDF = spark.read.table(bronzeTable).filter("status = 'quarantined'")
    bronzeQuarTransDF = transform_bronze(bronzeQuarantinedDF, quarantine=True).alias(
        "quarantine"
    )
   
    repairDF = bronzeQuarTransDF.withColumn("Budget",lit(1000000)).withColumn("RunTime",abs(bronzeQuarTransDF.RunTime))
    
    silverCleanedDF = repairDF.select(
        "movies",
        "movie_id",
        col("Budget").cast("Double"),
        "OriginalLanguage",
        "Revenue",
        "Price",
        col("RunTime").cast("integer")
    )
    return silverCleanedDF
