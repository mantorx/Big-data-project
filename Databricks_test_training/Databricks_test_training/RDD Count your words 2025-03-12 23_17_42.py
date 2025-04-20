# Databricks notebook source
# We start by defining the spark context to play with RDDs
spark
sc = spark.sparkContext

# Import file 

filename = "s3://full-stack-bigdata-datasets/Big_Data/purple_rain.txt"

# COMMAND ----------



# COMMAND ----------


# Fonction pour transformer en tuples (mot, index)
def token_to_tuple(token):
  return (token,1)

partial_count = tokens.map(token_to_tuple)



# COMMAND ----------

partial_count.take(10)

# COMMAND ----------

