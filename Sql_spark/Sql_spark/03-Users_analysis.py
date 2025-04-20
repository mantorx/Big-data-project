# Databricks notebook source
# MAGIC %md
# MAGIC # Users analysis

# COMMAND ----------

playlog = spark.read.format("csv").option("header", "true").option("inferSchema","true").load("s3://full-stack-bigdata-datasets/Big_Data/youtube_playlog.csv")
playlog.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC 1. Compute a new column `datetime` that converts the timestamp to a datetime, drop the `timestamp` column, and order by `datetime`, save this as a new DataFrame `df`, show the first 5 rows of `df`.
# MAGIC
# MAGIC > TIP: use the method `.from_unixtime(...)`, this method converts integers into dates.

# COMMAND ----------

from pyspark.sql.functions import from_unixtime , unix_timestamp
from pyspark.sql import functions as F
playlog = playlog \
  .withColumn("datetime", from_unixtime("timestamp")) \
  .drop("timestamp") \
  .orderBy(F.desc("datetime"))

# COMMAND ----------

playlog.limit(5).toPandas() 

# COMMAND ----------

# MAGIC %md
# MAGIC Now that we have a datetime column, we can compute new columns, namely:
# MAGIC - [year](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.year.html#pyspark.sql.functions.year)
# MAGIC - [month](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.month.html#pyspark.sql.functions.month)
# MAGIC - [dayofmonth](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.dayofmonth.html#pyspark.sql.functions.dayofmonth)
# MAGIC - [dayofweek](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.dayofweek.html#pyspark.sql.functions.dayofweek)
# MAGIC - [dayofyear](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.dayofyear.html#pyspark.sql.functions.dayofyear)
# MAGIC - [weekofyear](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.weekofyear.html#pyspark.sql.functions.weekofyear)
# MAGIC
# MAGIC We will put the resulting DataFrame in a variable called `df_enriched`.
# MAGIC
# MAGIC 2. Follow previous instructions
# MAGIC
# MAGIC *Tip: you use the reduce function from the functools package in order to automatically produce all the columns, otherwise you can just manually create them one by one*

# COMMAND ----------

import datetime 
from pyspark.sql.functions import year, month, dayofmonth,dayofweek ,dayofyear, weekofyear
playlog = playlog \
   .withColumn("year", year("datetime")) \
   .withColumn("month", month("datetime")) \
   .withColumn("dayofmonth", dayofmonth("datetime")) \
   .withColumn("dayofweek", dayofweek("datetime")) \
   .withColumn("dayofyear", dayofyear("datetime")) \
   .withColumn("weekofyear", weekofyear("datetime"))

playlog.printSchema()
playlog.count(), len(playlog.columns)
playlog.limit(5).toPandas()

# COMMAND ----------

from functools import reduce 
funcs = [F.year, F.month, F.dayofmonth, F.dayofweek, F.dayofyear, F.weekofyear]
df_enriched = reduce(
  lambda col_df, f: col_df.withColumn(f.__name__, f("datetime")),
  funcs,playlog)
df_enriched.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Aggregates
# MAGIC
# MAGIC #### `firstPlay`, `lastPlay`, `playCount`, `uniquePlayCount`
# MAGIC For each user, we will compute these metrics:
# MAGIC - `firstPlay`: datetime of the first listening
# MAGIC - `lastPlay`: datetime of the last listening
# MAGIC - `playCount`: total play counts
# MAGIC - `uniquePlayCount`: unique play counts
# MAGIC
# MAGIC We'll save all these in a new DataFrame: `users`.  
# MAGIC When you're done, print out the first 5 rows of `users` ordered by descending `playCount`.
# MAGIC
# MAGIC 3. Compute, for each user
# MAGIC - firstPlay
# MAGIC - lastPlay
# MAGIC - playCount
# MAGIC - uniquePlayCount
# MAGIC Save the results in a DataFrame named `users`

# COMMAND ----------

def compute_aggregate(df):
  agg_exprs = (
    F.min("datetime").alias("firstPlay"),
    F.max("datetime").alias("lastPlay"),
    F.count("song").alias("playCount"),
    F.countDistinct("song").alias("uniquePlayCount")
  )
  return df.groupBy("user").agg(*agg_exprs)

users = playlog.transform(compute_aggregate)

users.orderBy(F.desc("playCount")).limit(5).toPandas()

# COMMAND ----------

# MAGIC %md
# MAGIC 4. Run a sanity check that all firstPlay are anterior to lastPlay

# COMMAND ----------

users.filter(F.col("firstPlay")>F.col("lastPlay")).count()

# COMMAND ----------

# MAGIC %md
# MAGIC 5. Another sanity check, we grouped on the user column, so each user should represent a single row. Make sure all users are unique in the DataFrame

# COMMAND ----------

print(f"Total users:{users.count()}") 
print(f"Distinct users:{users.select('user').distinct().count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### `timespan`
# MAGIC We will compute `timespan`: the overall span of activity from a user in days, rounded to the inferior, for example:
# MAGIC - if a user was active 23 hours on the service, we will say he was active 0 days
# MAGIC - for 53 hours, that would be 2 days of activity
# MAGIC
# MAGIC We **will not** transform the `users` DataFrame in place, but instead save the result as a new DataFrame: `users_with_timespan`.
# MAGIC
# MAGIC 6. Compute timespan and save the result a new DataFrame: `users_with_timespan`

# COMMAND ----------

from pyspark.sql.types import IntegerType
def compute_timespan(df):
  return df.withColumn("timespan", (
    (F.unix_timestamp("lastPlay") - F.unix_timestamp("firstPlay")) / (60**2 * 24)).cast(IntegerType()))
users_with_timespan = users.transform(compute_timespan)
users_with_timespan.limit(5).toPandas()

# COMMAND ----------

# MAGIC %md
# MAGIC Let's check how this looks like, we will be using Databricks' `display` to plot an histogram of `timespan`.
# MAGIC
# MAGIC 7. Plot an histogram of `timespan`

# COMMAND ----------

display(users_with_timespan.select("timespan"))

# COMMAND ----------

# MAGIC %md
# MAGIC Looking like a powerlaw, let's try to log transform.
# MAGIC
# MAGIC 8. Use describe on the `timespan` column

# COMMAND ----------

users_with_timespan.select("timespan").describe().toPandas().set_index("summary")

# COMMAND ----------

# MAGIC %md
# MAGIC 9. Plot a histogram of log transformed `timespan`

# COMMAND ----------

display(users_with_timespan.select(F.log("timespan")))

# COMMAND ----------

# MAGIC %md
# MAGIC 10. Plot a QQ-Plot of log transformed `timespan`

# COMMAND ----------

import numpy as np
import pylab
import scipy.stats as stats
timespan_data = np.array(users_with_timespan.select("timespan").rdd.flatMap(lambda x:x).collect())
stats.probplot(timespan_data, dist="norm", plot=pylab)
pylab.show()

# COMMAND ----------

# MAGIC %md
# MAGIC We'll filter out users who stayed for less than a day and plot an histogram of this filtered data.
# MAGIC
# MAGIC 11. Plot a histogram of log transformed `timespan` of users who stayed more than one day

# COMMAND ----------

display(users_with_timespan.where(F.log("timespan") != 0).select(F.log("timespan")))

# COMMAND ----------

# MAGIC %md
# MAGIC ### `isSingleDayUser`
# MAGIC What percentage of users used the service for less than one day?

# COMMAND ----------

# MAGIC %md
# MAGIC 12. Compute the percentage of users who used the service for less than a day

# COMMAND ----------

users_with_timespan \
  .select(F.sum((F.col("timespan")<1).cast(IntegerType()))) \
  .rdd.map(lambda r:r[0]).first() / users.count() * 100

# COMMAND ----------

# MAGIC %md
# MAGIC Wow, that's a lot! We will flag this as its own column.  
# MAGIC That means we will create a new Boolean column `isSingleDayUser` that is `True` if the user used the service for less than a day and `False` otherwise.
# MAGIC
# MAGIC 13. Create a new column (isSingleDayUser) to flag if a user used the service for less than a day

# COMMAND ----------

users_with_single_day = users_with_timespan.withColumn("IsSingleDayUser", (F.col("timespan")<1))
users_with_single_day.limit(5).toPandas()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Measure of activity: `activeDaysCount` and `meanPlaycountByActiveDay`
# MAGIC This one is a bit harder, we want to compute:
# MAGIC - the number of active days for each user (not the `timespan`)
# MAGIC - the average play count on these active days for each user
# MAGIC
# MAGIC 14. Create 2 new columns
# MAGIC - activeDaysCount: the count of days each user was active
# MAGIC - dailyAvgPlayCount: the daily average playcount per user (active days only)
# MAGIC - activeDay

# COMMAND ----------

def compute_daily_stat(df):
  gb = df.groupBy(*(F.col(c) for c in ("user","year","dayofyear"))).count()
  exprs = (
    F.mean("count").alias("dailyAvgPlayCount"),
    F.count("count").alias("activeDaysCount")
  )
  return gb.groupBy("user").agg(*exprs)

users_with_avg= users_with_single_day.join(
  playlog.transform(compute_daily_stat), "user"
)
users_with_avg.limit(5).toPandas()

# COMMAND ----------

# MAGIC %md
# MAGIC 15. Plot a histogram of log of `activeDaysCount`

# COMMAND ----------

display(users_with_avg.select(F.log("activeDaysCount")))

# COMMAND ----------

# MAGIC %md
# MAGIC 16. Plot a histogram of log of `dailyAvgPlayCount`

# COMMAND ----------

display(users_with_avg.select(F.log("dailyAvgPlayCount")))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Going further
# MAGIC What else do you think would be interesting to compute?
# MAGIC What about the ratio of activity, e.g. the ratio between `timespan` and `activeDaysCount`?