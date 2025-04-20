# Databricks notebook source
# MAGIC %md
# MAGIC # Songs Analysis

# COMMAND ----------

file_type = 'parquet'

songs = spark.read.parquet("s3://full-stack-bigdata-datasets/Big_Data/YOUTUBE/items_selected.parquet")
songs.printSchema()
songs.count(), len(songs.columns)

# COMMAND ----------

# MAGIC %md
# MAGIC 1. Use `.describe()` on the DataFrame

# COMMAND ----------

songs.describe()

# COMMAND ----------

# MAGIC %md
# MAGIC 2. Count the number of missing values for each column
# MAGIC
# MAGIC *NOTE: Print out the results as a pandas DataFrame*

# COMMAND ----------

from pyspark.sql import functions as F 
import pandas as pd 
songs.select(* (F.sum (F.col(c).isNull().cast("int")).alias(c) for c in songs.columns)) \
  .toPandas().set_index(pd.Index(["Missing"]))


# COMMAND ----------

# MAGIC %md
# MAGIC Curiously, we have a few songs with missing `viewCounts`.
# MAGIC
# MAGIC 3. What are the 5 most popular songs? (by view count)

# COMMAND ----------

songs.select("snippet_channelTitle", "snippet_title" , "statistics_viewCount") \
  .orderBy(F.desc("statistics_viewCount")).limit(5).toPandas()

# COMMAND ----------

# MAGIC %md
# MAGIC 4. Compute:
# MAGIC - total_viewCount: the total viewcount per a channelTitle
# MAGIC - mean_viewCount: the average viewcount per a channel
# MAGIC - max_viewCount: the max view count per a channel
# MAGIC - min_viewCount: the min view count per a channel
# MAGIC - std_viewCount: the standard deviation of view counts per channel
# MAGIC - songsCount: number of songs associated per channel on our list

# COMMAND ----------

expressions = (
 F.sum("statistics_viewCount").alias("sum_viewCount"),
 F.mean("statistics_viewCount").alias("mean_viewCount"),
 F.min("statistics_viewCount").alias("min_viewCount"),
 F.max("statistics_viewCount").alias("max_viewCount"),
 F.stddev("statistics_viewCount").alias("stddev_viewCount"),
 F.count("*").alias("songs_Count"), 
)

channels = songs.groupBy("snippet_channelTitle").agg(*expressions)
channels.limit(5).toPandas()

# COMMAND ----------

# MAGIC %md
# MAGIC 5. What are the top 5 channels by `mean_viewCount`?

# COMMAND ----------

channels.orderBy(F.desc("mean_viewCount")).limit(5).toPandas()

# COMMAND ----------

# MAGIC %md
# MAGIC 6. What are the top 5 channels by `max_viewCount`?

# COMMAND ----------

channels.orderBy(F.desc("max_viewCount")).limit(5).toPandas()

# COMMAND ----------

# MAGIC %md
# MAGIC 7. What are the top 5 channels by `total_viewCount`?

# COMMAND ----------

channels.orderBy(F.desc("sum_viewCount")).limit(5).toPandas()

# COMMAND ----------

# MAGIC %md
# MAGIC 8. What are the top 5 channels by number of songs on our service?

# COMMAND ----------

channels.orderBy(F.desc("songs_Count")).limit(5).toPandas()

# COMMAND ----------

# MAGIC %md
# MAGIC 9. Scatter plot log of `meanViewCount` vs log of `trackCount`

# COMMAND ----------

display(channels.select(F.log("mean_viewCount"),F.log("songs_Count")))

# COMMAND ----------

