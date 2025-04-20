# Databricks notebook source
# MAGIC %md
# MAGIC # Words count with PySpark RDDs
# MAGIC If you've ever heard of "Hello, world!" for web development, "Word count" is the actual equivalent for distributed computing.
# MAGIC
# MAGIC In this notebook, we will setup a pipeline that will count the words in a document in a distributed manner. For convenience, we will do this on a single small document, but the operation should easily generalize to bigger documents that would not fit into the memory of a single machine. And that's the idea of this whole module, we work with technology that is able to scale according to the task at hand, even if we practice with smaller data.

# COMMAND ----------

# We start by defining the spark context to play with RDDs
sc = spark.sparkContext

# COMMAND ----------

# We need a S3 filepath

FILENAME = 's3://full-stack-bigdata-datasets/Big_Data/purple_rain.txt'

# COMMAND ----------

# MAGIC %md
# MAGIC 1. Load the filepath to a Spark RDD using `.textFile(...)` from a SparkContext into `text_file`:

# COMMAND ----------

text_file = sc.textFile (FILENAME)

# COMMAND ----------

# MAGIC %md
# MAGIC 2. Print out `text_file`:

# COMMAND ----------

text_file


# COMMAND ----------

# MAGIC %md
# MAGIC 3. That doesn't tell us much, how would you do to see the first 3 elements of this RDD? take the first 3 elements of the RDD `text_file`:

# COMMAND ----------

text_file.take(3)


# COMMAND ----------

# MAGIC %md
# MAGIC 4. This is a list of sentences, what we want is a list of tokens. Use the map function and a string method in order to split the charater strings into lists of words:

# COMMAND ----------

# Tokenization du texte en mots (RDD)
tokens = text_file.map(lambda line: line.split(" "))

tokens.take(3)

# COMMAND ----------

# MAGIC %md
# MAGIC That's not exactly what we wanted... We wanted a list of tokens, we got a... **list of list of tokens**!
# MAGIC
# MAGIC That's because, in this case, we need a special version of `.map()` called `flatMap`: it will flatten the list of list of tokens into a list of tokens.
# MAGIC
# MAGIC Let's try it out: we take the same expression as the previous one, but replace `.map()` with `.flatMap()` and call the resulting variable `tokens`.
# MAGIC
# MAGIC ---
# MAGIC 💡 It usually takes time to understand the notion of `.flatMap` and flattening in general, like `.map()`, these are concepts from the functionnal programming world. Unless you come from such a background, it probably **won't be easy to grasp these concepts the first time you encouter them**.
# MAGIC
# MAGIC **Let's keep our eyes on the goal: to develop a broader understanding of how Spark works.**
# MAGIC
# MAGIC ---

# COMMAND ----------

# MAGIC %md
# MAGIC 5. Copy/paste the previous cell, and:
# MAGIC - replace `.map(...)` with `.flatMap(...)`
# MAGIC - rename the variable `tokenized_text` to `tokens`

# COMMAND ----------

# Tokenization du texte en mots (RDD)
tokens = text_file.flatMap(lambda line: line.split(" "))



# COMMAND ----------

# MAGIC %md
# MAGIC 6. Use this cell to play with `tokens`, take different amounts of it, or collect it.

# COMMAND ----------

tokens.take(3)

# COMMAND ----------

# MAGIC %md
# MAGIC Now that we have our list of words (well, **not exactly a list of words, it is still a RDD**), we can start counting things.
# MAGIC
# MAGIC In order to do that, we need to map each word to an initial count, so instead of having:
# MAGIC ```
# MAGIC ['I',
# MAGIC  'never',
# MAGIC  'meant',
# MAGIC  ...,
# MAGIC  'I',
# MAGIC  'never',
# MAGIC  ...]
# MAGIC ```
# MAGIC We would like our list to look like this:
# MAGIC ```
# MAGIC [('I', 1),
# MAGIC  ('never', 1),
# MAGIC  ('meant', 1),
# MAGIC  ...,
# MAGIC  ('I', 1),
# MAGIC  ('never', 1),
# MAGIC  ...]
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC 7. Write a function `token_to_tuple` that takes:
# MAGIC - a token as input (a string)
# MAGIC - and returns (token, 1) (a tuple) 

# COMMAND ----------

# Fonction pour transformer en tuples (mot, index)
def token_to_tuple(token):
  return (token,1)



# COMMAND ----------

# MAGIC %md
# MAGIC 8. map `tokens` to your new function `token_to_tuple` to create a variable called `partial_count`

# COMMAND ----------

partial_count = tokens.map(token_to_tuple)


# COMMAND ----------

# MAGIC %md
# MAGIC 9. Take the first 10 elements of `partial_count`

# COMMAND ----------

partial_count.take(10)

# COMMAND ----------

# MAGIC %md
# MAGIC Good job!

# COMMAND ----------

# MAGIC %md
# MAGIC Beware, now comes the hard-part... We need to reduce this.
# MAGIC
# MAGIC Don't forget RDD are very low level objects, when we start using DataFrames (the other main kind of data format) all of this will become easier because of their higher level of abstraction.
# MAGIC
# MAGIC What we want is to take tuples with the same key, like `('never', 1)` and `('never', 1)` and aggregate them, so in the end we have `('never', 2)` (or more than 2 if there are more occurences of 'never').
# MAGIC
# MAGIC These kind of tuples are called **key-value pairs**, and while most Spark operations work on RDDs containing any type of objects, a few special operations are only available on RDDs of key-value pairs. You can read more about it [in the documentation](https://spark.apache.org/docs/latest/rdd-programming-guide.html#working-with-key-value-pairs).
# MAGIC
# MAGIC Among these operations are `.groupByKey(...)` and `.reduceByKey(...)`: the latter has better performances, but the former is easier to understand so we will start with this one.

# COMMAND ----------

# MAGIC %md
# MAGIC ### groupByKey

# COMMAND ----------

# MAGIC %md
# MAGIC 10. Call `.groupByKey(...)` on `partial_count` and put it inside `grouped_by_key` variable.

# COMMAND ----------

grouped_by_key = partial_count.groupByKey()

# COMMAND ----------

# MAGIC %md
# MAGIC 11.  take the first 3 elements of `grouped_by_key`.

# COMMAND ----------

grouped_by_key.take(3)

# COMMAND ----------

# MAGIC %md
# MAGIC What's this: `<pyspark.resultiterable.ResultIterable at 0x10bc0c2d0>` ?
# MAGIC
# MAGIC You don't have to worry about the details, but one thing has to attract your attention: `Iterable`, this seems to suggest those objects are iterable, an iterable in Python is something that you can iterate on: basically something that you can call `for` on, like a list, or a string, etc.
# MAGIC
# MAGIC ```python
# MAGIC for letter in 'Spark':
# MAGIC     print(e)
# MAGIC > S
# MAGIC > p
# MAGIC > a
# MAGIC > r
# MAGIC > k
# MAGIC ```
# MAGIC
# MAGIC Each element of `grouped_by_key` is a tuple, and inside a tuple there is an iterable we can iterate over.
# MAGIC
# MAGIC We will first try with the first element.

# COMMAND ----------

# MAGIC %md
# MAGIC 12. Take the first element of `grouped_by_key`, put it in `first_item` variable and print out its type.
# MAGIC
# MAGIC _WARNING: the type should be a tuple, not a list._

# COMMAND ----------

first_item = grouped_by_key.take(1)[0]

# COMMAND ----------

first_item 

# COMMAND ----------

# MAGIC %md
# MAGIC We'd like a way to print these items, for example, such that 'never' would look like this:
# MAGIC ```
# MAGIC 'never': [1, 1, 1, 1]
# MAGIC ```
# MAGIC
# MAGIC We will write a function that does this, take an item as a tuple of (`str`, `ResultIterable`), and print out:
# MAGIC ```
# MAGIC ITEM_NAME: OCCURENCES_AS_A_LIST
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC 13. Define a way to print our items like above:

# COMMAND ----------

def print_item(item_tuple):
  token_name, occurences = item_tuple
  occurencies_as_a_list = list(occurences)
  print(f"{token_name}: {occurencies_as_a_list}")



# COMMAND ----------

# MAGIC %md
# MAGIC 14. take the first 10 items from `grouped_by_key` and then iterate over them. Then, inside the loop, use the function `print_item(...)` on each item:

# COMMAND ----------

for item in grouped_by_key.take(10):
  print_item(item)
  


# COMMAND ----------

# MAGIC %md
# MAGIC Next step might be challenging.
# MAGIC
# MAGIC When you take the first 10 elements of `grouped_by_key`, it returns a list of `Tuple[str, ResultIterable]`.  
# MAGIC What we want instead is a list of `Tuple[str, int]` where the second element is the total number of occurence for the fist element.
# MAGIC
# MAGIC NOTE: you might wanna try to first return a list of `Tuple[str, list]`.
# MAGIC
# MAGIC You should be able to do all this using only list comprehensions.
# MAGIC
# MAGIC 15. Follow previous instructions:

# COMMAND ----------

[(token, sum(list(occurences))) for token, occurences in grouped_by_key.take(10)]

# COMMAND ----------

# MAGIC %md
# MAGIC As you've seen this can be done using standard list comprehension.  
# MAGIC If you're curious, even though Python is not a purely functional language, you can write this in a functional fashion and achieve the same result.
# MAGIC
# MAGIC _Please note this would look obviouly more elegant in a purely functional language.  
# MAGIC And I put it in there only to introduce you to another programming paradigm if you've mostly encountered imperative programming before. This little introduction is helpful because Spark is based on Scala, which, although not being a purely functional language, provides support for many functional programming features._

# COMMAND ----------

# Don't try at home ;)

from functools import reduce

list(map(lambda t: (t[0], reduce(lambda a, b: a + b, t[1])),
         grouped_by_key.take(10)))

# COMMAND ----------

# MAGIC %md
# MAGIC That would work, but that's using regular Python, hence we're not profiting from Spark's distributed computing capabilities, which means:
# MAGIC
# MAGIC - the computation would be much slower on big datasets,
# MAGIC - if the datasets is too big to be stored on the memory of our machine our program would crash.
# MAGIC
# MAGIC That's exactly what `.reduceByKey(...)` will help us to solve. It's usage is a bit similar to `.groupByKey(...)` but it takes a function as a parameter, this function should tell Spark how to aggregate 2 items, in our case, that the value of each tuple. For example, let's say we have a group:
# MAGIC
# MAGIC ```python
# MAGIC ('dog', 1), ('dog', 1)
# MAGIC ```
# MAGIC
# MAGIC We want a formula applied on values that will give us the end result, e.g. "how many dogs". In our case, that's a simple sum:
# MAGIC
# MAGIC ```python
# MAGIC def reduce_function(value_1, value_2):
# MAGIC     return value_1 + value_2
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC 16. Write our reduce function: reduce_function which takes 2 values and return their sum:
# MAGIC
# MAGIC _NOTE: name these parameters `a` and `b`._

# COMMAND ----------

def reduce_function(a,b):
  return a+b

# COMMAND ----------

# MAGIC %md
# MAGIC 17. We're now ready to reduce. You will pass your function as parameter to `.reduceByKey(...)`. call `.reduceByKey(...)` on `partial_count`, name it `reduced`.

# COMMAND ----------

reduced = partial_count.reduceByKey(reduce_function)

# COMMAND ----------

# MAGIC %md
# MAGIC 18. Take the 10 first values:

# COMMAND ----------

reduced.take(10)

# COMMAND ----------

# MAGIC %md
# MAGIC We're almost there... 😅
# MAGIC We've got a list of tuples, where the key is the token, and the value is its count within the text, but.. 
# MAGIC **they're not ordered...** which is inconvenient if we want to have the 10 most popular tokens within the text.
# MAGIC
# MAGIC We will use `.sortBy(...)`, but before we do, let's have a refresher on sorting with Python.
# MAGIC
# MAGIC For example, how would you sort this grocery list by the number of items?

# COMMAND ----------

fruits = [('banana', 3), ('orange', 5), ('pineapple', 2)]
fruits

# COMMAND ----------

# MAGIC %md
# MAGIC `sorted(fruits)` won't work because by default sorting on tuple take the first element, in our case, it would sort alphabetically on the name of the fruits.

# COMMAND ----------

sorted(fruits)

# COMMAND ----------

# MAGIC %md
# MAGIC We can force the `key` parameter to sort on the second item of each tuple.

# COMMAND ----------

sorted(fruits, key=lambda x: x[1])

# COMMAND ----------

# MAGIC %md
# MAGIC 19. Now, we will do the same on our rdd. Just like `key` in Python's `sorted`, PySpark's `.sortBy(...)` can take a function as a parameter. Use `.sortBy(...)` on `reduced`, name it `sorted_counts`.

# COMMAND ----------

sorted_counts = reduced.sortBy(lambda t:t[1])

# COMMAND ----------

# MAGIC %md
# MAGIC 20. Take the 10 first values of `sorted_counts`

# COMMAND ----------

sorted_counts.take(500)

# COMMAND ----------

# MAGIC %md
# MAGIC What do you think?

# COMMAND ----------

# MAGIC %md
# MAGIC It seems sorted, but in **ascending order**!
# MAGIC
# MAGIC If we wanted to do this in Python, we could just set the `reverse` argument to `True` when calling `sorted(...)`.

# COMMAND ----------

sorted(fruits, key=lambda x: x[1], reverse=True)

# COMMAND ----------

# MAGIC %md
# MAGIC We can't do this with Spark's RDDs. What we can do instead is **take the opposite value and order by it**.

# COMMAND ----------

# MAGIC %md
# MAGIC 21. Use `.sortBy(...)` on `reduced`, but with a descending sort, name it `desc_sorted_counts`.

# COMMAND ----------

desc_sorted_counts = reduced.sortBy(lambda t:-t[1])


# COMMAND ----------

# MAGIC %md
# MAGIC 22. Take the 10 first values of `desc_sorted_counts`:

# COMMAND ----------

desc_sorted_counts.take(10)

# COMMAND ----------

# MAGIC %md
# MAGIC Finally, what's the most common word in our document?

# COMMAND ----------

# MAGIC %md
# MAGIC ### **Bonus**: putting everything together
# MAGIC
# MAGIC We will create a function `count_words` that will do everything we did previously, but this time in one swell swoop, we won't use intermediary variables.
# MAGIC
# MAGIC The function will:
# MAGIC - take a filepath as argument
# MAGIC - load the content of this filepath into a Spark RDD
# MAGIC - `flatMap(...)` each line of this RDD into tokens by splitting on the ' ' string
# MAGIC - `.map(...)` each token to `(token, 1)` so this can be then reduced
# MAGIC - by calling `.reduceByKey(...)` with a function that sums the values
# MAGIC - and then sort the results with `.sortBy(...)` using the proper function to sort in descending order
# MAGIC - and return an RDD
# MAGIC
# MAGIC ---
# MAGIC ⚠️ Make sure your function returns a RDD
# MAGIC
# MAGIC ---

# COMMAND ----------

def count_words(filepath):
    # TODO: implement the content of the function
    # 
    # NOTE: you can remove `pass`
    # it's just here to avoid the cell crashing while the
    # content of the function is empty
    pass
    ### BEGIN STRIP ###
    return sc.textFile(filepath)\
    .flatMap(lambda line: line.split(' '))\
    .map(lambda word: (word, 1)) \
    .reduceByKey(lambda a, b: a + b) \
    .sortBy(lambda t: -t[1])
    ### END STRIP ###

# COMMAND ----------

# MAGIC %md
# MAGIC 1. Use `count_words` with `FILENAME` and check its type:

# COMMAND ----------

rdd = count_words(FILENAME)
type(rdd)

# COMMAND ----------

# MAGIC %md
# MAGIC It should be a `pyspark.rdd.PipelineRDD`.

# COMMAND ----------

# MAGIC %md
# MAGIC 2. Finally, take the 10 first elements of your RDD

# COMMAND ----------

rdd.take(10)

# COMMAND ----------

# MAGIC %md
# MAGIC That's it, you've done it!
# MAGIC
# MAGIC You've created a Spark job, the next step would be to neatly package this into a Python executable and submit it to a Spark Cluster for batch or stream execution, but this is beyond the content of this course.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Going further
# MAGIC
# MAGIC We used a toy dataset, we suggest you try with a bigger one.