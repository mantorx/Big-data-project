# Databricks notebook source
# MAGIC %md
# MAGIC # Introduction to PySpark - Part 1 - RDDs 🐍✨
# MAGIC
# MAGIC ## What you will learn in this course 🧐🧐
# MAGIC This course is a demo that will introduce to you one of the main data formats in spark which is spark RDDs (Resilient Distributed Datasets), we will walk you through how to use this low level data format using pyspark.
# MAGIC Here's the outline:
# MAGIC
# MAGIC * Databricks
# MAGIC     * Login Page
# MAGIC     * Homepage
# MAGIC     * Workspace
# MAGIC     * Create Folder
# MAGIC     * Upload Notebook
# MAGIC     * Notebook View
# MAGIC * Spark Session and Spark Context
# MAGIC * RDDs
# MAGIC     * Creating RDDs
# MAGIC         * Parallelizing existing collection
# MAGIC         * Loading from file
# MAGIC     * Playing with RDDs
# MAGIC         * Actions
# MAGIC             * `.take()`
# MAGIC             * `.collect()`
# MAGIC             * `.count()`
# MAGIC             * `.sum()`
# MAGIC             * `.mean()`
# MAGIC             * `.reduce()`
# MAGIC         * Transformations
# MAGIC             * `.map()`
# MAGIC             * Chaining operations
# MAGIC             * `.filter()`
# MAGIC         * Tuple Key-Value
# MAGIC             * `.groupByKey()`
# MAGIC             * `.reduceByKey()`

# COMMAND ----------

# MAGIC %md
# MAGIC ## Databricks 🧱🧱
# MAGIC Databricks is a cloud service provider which makes available clusters of machines with the spark framework already installed on them. Spark can be a real pain to set up, but gets amazing results once it's all up and running. We'll use databricks here so we can all work on a standardized environment!
# MAGIC
# MAGIC In order to set you up with it, visit this page: [Databricks Community](https://community.cloud.databricks.com/).
# MAGIC
# MAGIC We'll use the community edition which is free, but limits the number and performance of the machines in our cluster. However this is not going to change a thing in terms of the code we'll write, whatever we'll learn here can scale up by connecting to a bigger cluster.
# MAGIC
# MAGIC Here's a walkthrough of what you should do once you are logged in ;)
# MAGIC
# MAGIC ### Login Page 🔑
# MAGIC ![](https://full-stack-assets.s3.eu-west-3.amazonaws.com/images/Databricks/databricks_login.PNG)
# MAGIC
# MAGIC ### Homepage 🏠
# MAGIC ![](https://full-stack-assets.s3.eu-west-3.amazonaws.com/images/Databricks/databricks_homepage.PNG)
# MAGIC
# MAGIC ### Workspace 👷
# MAGIC ![](https://full-stack-assets.s3.eu-west-3.amazonaws.com/images/Databricks/databricks_workspace.PNG)
# MAGIC
# MAGIC ### Create Folder 📁
# MAGIC ![](https://full-stack-assets.s3.eu-west-3.amazonaws.com/images/Databricks/databricks_create_folder.PNG)
# MAGIC
# MAGIC ### Upload Notebook 📤
# MAGIC ![](https://full-stack-assets.s3.eu-west-3.amazonaws.com/images/Databricks/databricks_import_notebook.PNG)
# MAGIC
# MAGIC ### Notebook View 📝
# MAGIC ![](https://full-stack-assets.s3.eu-west-3.amazonaws.com/images/Databricks/databricks_notebook_view.PNG)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Spark Session and Spark Context ✨✨
# MAGIC The spark context is the original access point to the spark framework and let's you use RDDs.
# MAGIC The spark session as created later and is a unified access point to the spark framework, it let's you use Spark Dataframes which we'll study later. Normally you'd have to set them up, fortunately, in databricks it is already all set up for you!

# COMMAND ----------

# If you wish to see what's inside the spark object and the sparkContext run these commands
# but your code will work regardless since they have already been set up.
spark
sc = spark.sparkContext

# COMMAND ----------

type (spark)
type(sc)

# COMMAND ----------

# MAGIC %md
# MAGIC ## RDDs 📄📄
# MAGIC
# MAGIC > An immutable distributed collection of objects. Each RDD is split into multiple *partitions*, which maybe computed on different nodes of the cluster.  
# MAGIC - Learning Spark, page 23 (Holden Karau, Andy Konwinski, Patrick Wendell & Matei Zaharia)
# MAGIC
# MAGIC
# MAGIC **R**esilient **D**istributed **D**ataset (aka RDD) are the primary data abstraction in Apache Spark. They are:
# MAGIC - **Resilient**: fault tolerant, they can recompute missing or damaged partitions.
# MAGIC - **Distributed**: data is spread on multiple clusters
# MAGIC - **Dataset**: RDDs are a collection of objects
# MAGIC
# MAGIC RDDs do not have a data schema, which means they do not have properly defined columns. Think of it a list of entries (rows of data) where the information contained may differ from line to line. This gives this data format great flexibility to store any type of data, but also changing data overtime (the fact that there is no predefined schema means that a new entry maybe added to the data even if it contains information that was never stored before, like a new column, which would not be possible otherwise).
# MAGIC
# MAGIC Moreover, RDDs are immutable (you can't change their value inplace, most like tuples) and their operations are lazy; fault-tolerance is achieved by keeping track of the "lineage" of each RDD (the sequence of operations that produced it) so that it can be reconstructed in the case of data loss. RDDs can contain any type of Python, Java, or Scala objects.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Creating RDDs 📃
# MAGIC
# MAGIC Spark provides two ways to create RDDs: loading an external dataset and "parallelizing" a collection in your driver program.  
# MAGIC
# MAGIC NOTE: on the most frequent usage being to load from external dataset, because usually the collection won't fit into the memory of a single machine.

# COMMAND ----------

# MAGIC %md
# MAGIC #### Parallelizing an existing collection 🔀
# MAGIC
# MAGIC Parallelizing an existing collection means taking a collection of objects that is not stored on a distributed file system and converting it into a distributed object by splitting the data across the machines in the cluster.
# MAGIC
# MAGIC To this we use then`sc.parallelize` function from the spark context

# COMMAND ----------

numbers_rdd = sc.parallelize(range(50)) # create a collection of int and splitting it across the cluster
numbers_rdd

# COMMAND ----------

# MAGIC %md
# MAGIC #### Loading from a file 📥
# MAGIC So far we've been building RDDs from existing Python objects. We can also directly load from a file.

# COMMAND ----------

# MAGIC %md
# MAGIC #### `sc.textFile(...)`
# MAGIC This function let's Spark create an RDD from a text file, treating it as a collection of lines (which means any `\n` newline character will define where a new element in the collection begins and ends).
# MAGIC All we need for this is the **URI** from the file, and we are good to go!

# COMMAND ----------

text_rdd = sc.textFile('s3://full-stack-bigdata-datasets/Big_Data/tears_in_rain.txt')
text_rdd

# COMMAND ----------

# MAGIC %md
# MAGIC ### Playing with RDDs 🎮

# COMMAND ----------

# MAGIC %md
# MAGIC Some resources: https://www.analyticsvidhya.com/blog/2016/10/using-pyspark-to-perform-transformations-and-actions-on-rdd/

# COMMAND ----------

# MAGIC %md
# MAGIC We can perform 2 types of operations on RDDs, **actions** and **transformations**. All transformations are lazy: computations are not done until we apply an action.
# MAGIC RDDs are **immutable**, we cannot change them inplace, we need to apply a **transformation** that will return an **uncomputed** RDD.

# COMMAND ----------

# MAGIC %md
# MAGIC #### ACTIONS 🦸
# MAGIC We will start by performing some **actions** on our RDDs.

# COMMAND ----------

# MAGIC %md
# MAGIC ##### `.take(num)`
# MAGIC Returns the first `num` values of the RDD, where `num` is an integer. Like all actions, this will compute immediately.
# MAGIC It is a method associated with RDD objects.

# COMMAND ----------

numbers_rdd.take(3)

# COMMAND ----------

text_rdd.take(3)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### `.collect()`
# MAGIC Like `.take(...)` but will take effect on all values of the RDD.

# COMMAND ----------

numbers_rdd.collect()

# COMMAND ----------

text_rdd.collect()

# COMMAND ----------

# MAGIC %md
# MAGIC If this monologue sounds familiar, that's because **[it is](https://www.youtube.com/watch?v=NoAzpa1x7jU)**.

# COMMAND ----------

# MAGIC %md
# MAGIC ##### `.count()`
# MAGIC Another very useful action that returns the number of elments in a RDD

# COMMAND ----------

numbers_rdd.count()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### `.sum()`
# MAGIC An action to compute the sum of elements in an RDD

# COMMAND ----------

numbers_rdd.sum()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### `.mean()`
# MAGIC Computes the average of the RDD (requires numerical values).

# COMMAND ----------

numbers_rdd.mean()

# COMMAND ----------

text_rdd.mean() # this will fail

# COMMAND ----------

# MAGIC %md
# MAGIC As you may have noticed, error messages are quite hard to read and understand in Spark, what you see here is only the tip of the iceberg, the main error message that you are getting.
# MAGIC
# MAGIC At the end of the message you can read `TypeError: unsupported operand type(s) for -: &#39;str&#39; and &#39;float&#39;&#39;.` this is were the useful information usually resides, at the very end, same as python. If that does not help, feel free to expand the error message and run a more detailed analysis, but for most cases the main message should suffice.

# COMMAND ----------

# MAGIC %md
# MAGIC ##### `.reduce()`
# MAGIC
# MAGIC Spark RDD `reduce()` aggregate action function is used to calculate min, max, and total of elements in a dataset, we will explain RDD reduce function syntax and usage. The <a href="https://spark.apache.org/docs/3.1.1/api/python/reference/api/pyspark.RDD.reduce.html"> documentation may be found here </a>.
# MAGIC
# MAGIC This function works similarly to map, but it's an action an will return a result immediately. It is used to agregate so the function we may use inside reduce take two arguments and return one, let's give a few examples:

# COMMAND ----------

numbers_rdd.reduce(lambda a,b: a+b) # sums all the elements

# COMMAND ----------

numbers_rdd.reduce(lambda a,b: min(a,b)) # returns the min

# COMMAND ----------

# MAGIC %md
# MAGIC #### TRANSFORMATIONS 🧙
# MAGIC And now we will apply some **transformations**.

# COMMAND ----------

# MAGIC %md
# MAGIC ##### `.map(func)`
# MAGIC Applies `func` to every element of the RDD. Won't compute anything until an action is called. It works just like the `.apply(lambda x: func(x))` method in Pandas.

# COMMAND ----------

lower_rdd = text_rdd.map(lambda s: s.lower()) # we apply a function to each row in the RDD
# since the elements of the RDD are character strings, we may use the .lower method
# note that this lower method is a method from the SparkContext it is not a pythn function

# COMMAND ----------

lower_rdd = text_rdd.map(lambda s: s.lower())

# COMMAND ----------

lower_rdd # when returning the mapped object, no computing happens, only the object type will be returned

# COMMAND ----------

# MAGIC %md
# MAGIC How do I get my result? Use an ACTION! -> `.take(...)` or `.collect()`

# COMMAND ----------

lower_rdd.take(3) # This will returned the first 3 elements of the RDD after applying the transformation!
# note that we only compute the transformation on the elements we will display, and not the rest, the last few elements 
# of the RDD have not been touched at all, which saves time!

# COMMAND ----------

# MAGIC %md
# MAGIC Let's try another example, but this time we will attempt a transformation that will fail!

# COMMAND ----------

text_fail_rdd = text_rdd.map(lambda s : abs(s)) # let's try and calculate the inner sum of each element in the RDD
text_fail_rdd

# COMMAND ----------

# MAGIC %md
# MAGIC No error!? This is most suprising since we are attempting to run a numerical operation on character strings, let's use an action to display the results!

# COMMAND ----------

text_fail_rdd.take(3)

# COMMAND ----------

# MAGIC %md
# MAGIC AH, now get the error! This is the main pitfall of lazy execution, since transformations do not compute (they are lazy) they do not get to see the data until an action is run, which is why this type of error is called a runtime error, an error that occurs when operations are run, even though it comes from an operation you scheduled one or ten cells prior.

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Chaining operations ⛓️
# MAGIC It is possible to chain operations if you wish to run several transformations back to back, this is very useful but be careful, the more transformations you run back to back the more likely you will get a run time error later, and the harder it may be to identify exactly which transformation is not working.

# COMMAND ----------

len_rdd = text_rdd.map(lambda s: s.replace(" ","")).map(lambda s: len(s)) # we remove all space characters
# then count the number of characters in each line

# COMMAND ----------

len_rdd

# COMMAND ----------

# MAGIC %md
# MAGIC Here as well, you need to call an action (like `take` or `collect`) for the computation perform.

# COMMAND ----------

len_rdd.collect()

# COMMAND ----------

# MAGIC %md
# MAGIC But don't do it in between two transformations!
# MAGIC
# MAGIC Actions convert your RDDs to lists, which means none of the things you can use on RDDs would work anymore.

# COMMAND ----------

# This will fail
rdd_lower = text_rdd.map(lambda s: s.lower()).take(3)
rdd_lower.filter(lambda x: len(s) > 8)

# COMMAND ----------

# MAGIC %md
# MAGIC **`.filter(Bool)`**
# MAGIC Let's add a new **transformation**, `filter`, it will filter the RDD based on a function returning a boolean value.  
# MAGIC
# MAGIC *Note that when we're chaining operations, we go back to the line using Python syntax to do do, e.g. `\`.*

# COMMAND ----------

result = text_rdd \
    .map(lambda s: s.replace(" ","")) \
    .map(lambda s: len(s)) \
    .filter(lambda c: c > 50) \
    .collect()
# the function inside filter returns true for each element above 50

# COMMAND ----------

result

# COMMAND ----------

# MAGIC %md
# MAGIC Seems like the only line in the text that contains more than 50 non-space characters contains 53 non-space characters.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Key-value tuples 🔑🔢
# MAGIC It's common to use tuple values, as key-value pairs, this comes from the mapReduce algorithm formalism which deals with key-value pairs.
# MAGIC
# MAGIC Let's give an example here:

# COMMAND ----------

tuples_rdd = sc.parallelize([
    ('banana', 4), ('orange', 12), ('apple', 3),
    ('pineapple', 1), ('banana', 3), ('orange', 6)])
tuples_rdd

# COMMAND ----------

# MAGIC %md
# MAGIC Let's now use the `.groupByKey()` method.
# MAGIC This method works on key-value tuples (K,V) and automatically sets the first element of the tuple (K here) to be the key, and will group each element with the same key together to run some aggregations.

# COMMAND ----------

tuples_rdd.groupByKey().map(lambda t: (t[0], sum(t[1]))).collect() # after grouping by keys we use the map function to
# tuples containing the keys and the sum of values for each key

# COMMAND ----------

# MAGIC %md
# MAGIC We could also use `.reduceByKey(...)`, an action that groups and returns an aggregated result all at once!

# COMMAND ----------

tuples_rdd.reduceByKey(lambda a,b: a+b).collect()

# COMMAND ----------

# MAGIC %md
# MAGIC This concludes our demo on manipulating RDD with spark, now let's move on to the exercises to get some practice!