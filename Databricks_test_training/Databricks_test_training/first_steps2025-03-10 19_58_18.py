# Databricks notebook source
spark
sc = spark.sparkContext


# COMMAND ----------

text_rdd = sc.textFile("s3://full-stack-bigdata-datasets/Big_Data/tears_in_rain.txt")
text_rdd.take(3)

# COMMAND ----------

text_rdd.collect()

# COMMAND ----------

print(text_rdd)


# COMMAND ----------

lineLengths = text_rdd.map(lambda s: len(s))



# COMMAND ----------

lineLengths.take(3)

# COMMAND ----------

lineLengths.collect()

# COMMAND ----------

avgLength = lineLengths.mean()
avgLength 

# COMMAND ----------

totalLength = lineLengths.sum()
totalLength

# COMMAND ----------

# MAGIC %md
# MAGIC try to compute the total sum , but this time using .reduce()

# COMMAND ----------

lineLengths.reduce(lambda x,y:x+y)