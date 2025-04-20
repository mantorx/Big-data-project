# Databricks notebook source
# MAGIC %md
# MAGIC # First steps with DataFrames

# COMMAND ----------

# MAGIC %md
# MAGIC ## Learning objectives
# MAGIC
# MAGIC - Learn basic transformations and actions on PySpark DataFrames
# MAGIC - Learn to define a temporary view and execute SQL statements using the SparkSession

# COMMAND ----------

spark

# COMMAND ----------

# Load the file hosted at `filepath` onto a PySpark DataFrame: user_logs
filepath = "s3://full-stack-bigdata-datasets/Big_Data/youtube_playlog.csv"

user_logs = (spark.read.format('csv')\
             .option('header', 'true')\
             .option('inferSchema', 'true')\
             .load(filepath))

# COMMAND ----------

# MAGIC %md
# MAGIC It's easier to see PySpark DataFrames abstractions as SQL tables rather than to think of them as equivalent to `pandas`.  If you are familiar with data manipulation in `pandas`, it will be tempting to shortcut your thinking into `pandas`, this is the worse you can do.
# MAGIC
# MAGIC The goal of this notebook is to help you counter your intuition on this.
# MAGIC
# MAGIC This is why, for every task in this notebook, we will first implement them using declarative SQL (using `spark.sql(...)`), you will then try to get the same result using PySpark DataFrames imperative programming style.
# MAGIC
# MAGIC Before we get started, we will first start by running a few actions that have no equivalent in SQL: `.show()`, `.printSchema()` and `.describe()`.
# MAGIC
# MAGIC Remember, these are actions, that means they will **actually perform computations**.
# MAGIC
# MAGIC Unlike most actions, `.show()` and `.printSchema()` won't return a result, but just print out to the screen.
# MAGIC
# MAGIC 1. Show the first 10 rows of `user_logs`:

# COMMAND ----------

user_logs.show(10)

# COMMAND ----------

# MAGIC %md
# MAGIC 2. Print out the schema of `user_logs`

# COMMAND ----------

user_logs.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC Another action, `.describe()`, this one returns a value: descriptive statistics about the DataFrame in a Spark DataFrame format.

# COMMAND ----------

# MAGIC %md
# MAGIC 3. Use `.describe()` on `user_logs` and put it inside `user_describe`:

# COMMAND ----------

user_describe = user_logs.describe()

# COMMAND ----------

# MAGIC %md
# MAGIC 4. Show the results with `.toPandas()`:

# COMMAND ----------

user_describe.toPandas()

# COMMAND ----------

# MAGIC %md
# MAGIC 5. Show the results with `display()`:

# COMMAND ----------

display(user_describe)

# COMMAND ----------

# MAGIC %md
# MAGIC 6. Show the results using `.show()`:

# COMMAND ----------

user_describe.show()

# COMMAND ----------

# MAGIC %md
# MAGIC 7. Before we can query using SQL, we need a `TempView`. Create a TempView of `user_logs` in `user_logs_table`.

# COMMAND ----------

user_logs.createOrReplaceTempView("user_logs_table")


# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 1: count the number of records

# COMMAND ----------

# MAGIC %md
# MAGIC `.count(...)` is an action not a transformation (and will perform computation), while using COUNT in a SQL statement will still return a DataFrame (you'll have to force the compute).
# MAGIC
# MAGIC 1. count the number of records using SQL

# COMMAND ----------

display(spark.sql("""SELECT count(*)
                      FROM user_logs_table """))

# COMMAND ----------

# MAGIC %md
# MAGIC 2. count the number of records using PySpark DataFrames transformations and actions

# COMMAND ----------

user_logs.count()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 2: select the column `user`

# COMMAND ----------

# MAGIC %md
# MAGIC 1. Select the column 'user' using SQL

# COMMAND ----------

spark.sql("""SELECT user
                      FROM user_logs_table """).show()

# COMMAND ----------

# MAGIC %md
# MAGIC 2. Select the column 'user' using PySpark SQL

# COMMAND ----------

user_logs.select("user").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 3: select all distinct user

# COMMAND ----------

# MAGIC %md
# MAGIC 1. Select distinct user using SQL

# COMMAND ----------

spark.sql(""" SELECT DISTINCT(user)
          FROM user_logs_table """).show()

# COMMAND ----------

# MAGIC %md
# MAGIC 2. Select distinct user using PySpark DataFrame API

# COMMAND ----------

user_logs.select("user").distinct().show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 4: Select all distinct users and alias the column name to `distinct_user`

# COMMAND ----------

# MAGIC %md
# MAGIC 1. Select distinct user using SQL and alias the name of the new column to `distinct_user`

# COMMAND ----------

spark.sql(""" SELECT DISTINCT(user) as distinct_user
          FROM user_logs_table """).show()

# COMMAND ----------

# MAGIC %md
# MAGIC 2. Select distinct user using SQL and alias the name of the new column to `distinct_user`

# COMMAND ----------

user_logs.select(user_logs["user"].alias("distinct_user")).distinct().show() 

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 5: count the number of distinct user

# COMMAND ----------

# MAGIC %md
# MAGIC 1. Count the number of distinct user using SQL. Alias the resulting column to `total_distinct_user`

# COMMAND ----------

spark.sql(""" SELECT count(DISTINCT(user)) as total_distinct_user
          FROM user_logs_table """).show()

# COMMAND ----------

# MAGIC %md
# MAGIC 2. Count the number of distinct user using PySpark DataFrame API

# COMMAND ----------

user_logs.select("user").distinct().count()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 6: count the number of distinct songs

# COMMAND ----------

# MAGIC %md
# MAGIC 1. Count the number of distinct songs using SQL. Alias the resulting column to `total_distinct_song`

# COMMAND ----------

spark.sql(""" SELECT count(DISTINCT(song)) as total_distinct_song
          FROM user_logs_table """).show()

# COMMAND ----------

# MAGIC %md
# MAGIC 2. Count the number of distinct songs using SQL

# COMMAND ----------

user_logs.select("song").distinct().count()