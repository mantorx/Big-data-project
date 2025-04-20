# Databricks notebook source
# MAGIC %md
# MAGIC # Preprocessing

# COMMAND ----------

# MAGIC %md
# MAGIC ## Loading our data from S3

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

filepath = "s3://full-stack-bigdata-datasets/Big_Data/youtube_playlog.csv"

# COMMAND ----------

playlog = (spark.read.format('csv')\
             .option('header', 'true')\
             .option('inferSchema', 'true')\
             .load(filepath))
playlog.show(5)

# COMMAND ----------

# MAGIC %md
# MAGIC ## First analysis
# MAGIC 1. Print out our DataFrame's schema

# COMMAND ----------

playlog.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC 2. Use `.describe(...)` on your DataFrame

# COMMAND ----------

playlog.describe()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Missing values check
# MAGIC
# MAGIC 3. Count the missing values for each column put the result in a pandas DataFrame and print it out.
# MAGIC *TIP: you may use dictionnary comprehension in order to create the base to build the DataFrame from*
# MAGIC

# COMMAND ----------

import pandas as pd
missing_values = {c: playlog.filter(playlog[c].isNull()).count() for c in playlog.columns} 
pd.DataFrame.from_dict(
  missing_values,
  orient = "index", columns = ["missing_values"]
  ).T

# COMMAND ----------

# MAGIC %md
# MAGIC ### Duplicates check
# MAGIC
# MAGIC 4. Check if playlog without duplicates has the same number of rows as the original.

# COMMAND ----------

playlog.count()==playlog.dropDuplicates().count()

# COMMAND ----------

# MAGIC %md
# MAGIC Seems like we have duplicates, let's count how many.
# MAGIC
# MAGIC 5. Figure out a way to count the number of duplicates.

# COMMAND ----------

playlog.count() - playlog.dropDuplicates().count()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Other checks
# MAGIC 6. Order the dataframe by ascending `timestamp` and show the first 5 rows.

# COMMAND ----------

playlog.orderBy("timestamp").show(5)

# COMMAND ----------

# MAGIC %md
# MAGIC Do you see anything suspicious?
# MAGIC
# MAGIC The first timestamp is negative, and it seems like it's the only one.  
# MAGIC We will make sure there aren't other like this.
# MAGIC
# MAGIC 7. count the number of rows with a negative timestamp

# COMMAND ----------

playlog.where(playlog["timestamp"] < 0).count()

# COMMAND ----------

# MAGIC %md
# MAGIC As expected, only one such negative timestamp. Since we have only one we can actually `.collect(...)` it.
# MAGIC
# MAGIC 8. Collect the problematic rows

# COMMAND ----------

playlog.where(playlog["timestamp"] < 0).collect()

# COMMAND ----------

# MAGIC %md
# MAGIC There's only one problematic value among more than 25M.  This negative timestamp is an error, as such the real value is missing. We could try to reconstruct the real value but that would be a really tedious task, since it's one value over 25M, we will simply remove it.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Removing the row with a negative timestamp

# COMMAND ----------

# MAGIC %md
# MAGIC We will use our new knowledge about the data to perform some preprocessing.  
# MAGIC
# MAGIC Our pipeline will have 2 steps:
# MAGIC * Remove duplicates (123651 rows)
# MAGIC * Remove row with negative timestamps (1 row)
# MAGIC
# MAGIC We will call our new DataFrame `playlog_processed` and save it to S3 in parquet format.
# MAGIC
# MAGIC 9. Filter out:
# MAGIC * duplicated values
# MAGIC * rows with negative timestamp
# MAGIC * save the result to a new DataFrame: `playlog_processed`
# MAGIC * Finally, print out the number of rows in this DataFrame

# COMMAND ----------

playlog_processed = (playlog.dropDuplicates().filter(~((playlog["timestamp"] < 0))))
playlog_processed.count()

# COMMAND ----------

# MAGIC %md
# MAGIC 10. save the processed DataFrame to S3 using the parquet format for this you may use the the method .write.parquet(...)
# MAGIC *You may use this path 's3://full-stack-bigdata-datasets/Big_Data/playlog_processed_student.parquet'*

# COMMAND ----------

output_path = 's3://full-stack-bigdata-datasets/Big_Data/playlog_processed_student.parquet'
playlog_processed.write.parquet(output_path, mode = "overwrite")