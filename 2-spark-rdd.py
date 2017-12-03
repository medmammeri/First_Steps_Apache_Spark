# Databricks notebook source
# MAGIC %md # Introduction to Spark programming

# COMMAND ----------

# MAGIC %md # Word Count
# MAGIC 
# MAGIC The "Hello World!" of distributed programming is the wordcount. Basically, you want to count easily number of different words contained in an unstructured text. You will write some code to perform this task on the [Complete Works of William Shakespeare](http://www.gutenberg.org/ebooks/100) retrieved from [Project Gutenberg](http://www.gutenberg.org/wiki/Main_Page).
# MAGIC 
# MAGIC [Spark's Python API reference](https://spark.apache.org/docs/latest/api/python/pyspark.html#pyspark.RDD) could provide some help
# MAGIC 
# MAGIC ### ** Part 1: Creating a base RDD and pair RDDs **
# MAGIC 
# MAGIC #### In this part of the lab, we will explore creating a base RDD with `parallelize` and using pair RDDs to count words.
# MAGIC 
# MAGIC #### We'll start by generating a base RDD by using a Python list and the `sc.parallelize` method.  Then we'll print out the type of the base RDD.

# COMMAND ----------

# Please run this cell to load the Test class
from test_helper import Test

# COMMAND ----------

words_list = ['we', 'few', 'we', 'happy', 'few', "we", "band", "of", "brothers"]
words_RDD = sc.parallelize(words_list, 4)

# Print the type of words_RDD
print type(words_RDD)

# COMMAND ----------

# MAGIC %md We want to capitalize each word contained in a RDD. For such transformation, we use a `map`, as we want to transform a RDD of **n** elements into another RDD of **n** using a function that gets and returns one single element.
# MAGIC 
# MAGIC Please implement `capitalize`function in the cell below.

# COMMAND ----------

def capitalize(word):
    """Capitalize lowercase `words`.

    Args:
        word (str): A lowercase string.

    Returns:
        str: A string which first letter is uppercase.
    """
    return word.capitalize()

print(capitalize('we'))

Test.assertEquals(capitalize('we'), 'We', "Capitalize")

# COMMAND ----------

# MAGIC %md Apply `capitalize` to the base RDD, using a [map()](http://spark.apache.org/docs/latest/api/python/pyspark.html#pyspark.RDD.map) transformation that applies the `capitalize()` function to each element. Then call the [collect()](http://spark.apache.org/docs/latest/api/python/pyspark.html#pyspark.RDD.collect) action to retrieve the values of the transformed RDD, and print them.

# COMMAND ----------

capital_RDD = words_RDD.map(capitalize)
local_result = capital_RDD.collect()
print(local_result)

Test.assertEqualsHashed(local_result, 'bd73c54004cc9655159aceb703bc14fe93369fb1',
                        'incorrect value for local_data')

# COMMAND ----------

rdd = sc.parallelize([2, 3, 4])
rdd.map(lambda x: range(1, x)).collect()


# COMMAND ----------

# MAGIC %md Do the same using a lambda function

# COMMAND ----------

capital_lambda_RDD = words_RDD.map(lambda x : x.capitalize())
local_result = capital_lambda_RDD.collect()
print(local_result)

Test.assertEqualsHashed(local_result, 'bd73c54004cc9655159aceb703bc14fe93369fb1',
                        'incorrect value for capital_lambda_RDD')

# COMMAND ----------

# MAGIC %md Now use `map()` and a `lambda` function to return the number of characters in each word, and `collect` this result directly into a variable.

# COMMAND ----------

plural_lengths = (capital_RDD.map(lambda x : len(x)).collect())
print(plural_lengths)

Test.assertEqualsHashed(plural_lengths, '0772853c8e180c1bed8cfe9bde35aae79b277381',
                  'incorrect values for plural_lengths')

# COMMAND ----------

# MAGIC %md To program a wordcount, we will need `pair RDD` objects. A pair RDD is an RDD where each element is a pair tuple `(k, v)` where `k` is the key and `v` is the value. In this example, we will create a pair consisting of `('<word>', 1)` for each word element in the RDD.
# MAGIC 
# MAGIC Create the pair RDD using the `map()` transformation with a `lambda()` on `words_RDD`.

# COMMAND ----------

words_pair_RDD = words_RDD.map(lambda x : (x,1))
print(words_pair_RDD.collect())

Test.assertEqualsHashed(words_pair_RDD.collect(), 'fb67a530034e01395386569ef29bf5565b503ec6',
                        "incorrect value for wrods_pair_RDD")

# COMMAND ----------

# MAGIC %md Now, let's count the number of times a particular word appears in the RDD. There are multiple ways to perform the counting, but some are much less efficient or scalable than others.
# MAGIC 
# MAGIC A naive approach would be to `collect()` all of the elements and count them in the driver program. While this approach could work for small datasets, it is not scalable as the result of `collect()` would have to fit in the driver's memory. When you should use `collect()` with care, always asking yourself what is the size of data you want to retrieve.
# MAGIC 
# MAGIC In order to program a scalable wordcount, you will need to use parallel operations.
# MAGIC 
# MAGIC #### `groupByKey()` approach
# MAGIC 
# MAGIC An approach you might first consider is based on using the [groupByKey()](http://spark.apache.org/docs/latest/api/python/pyspark.html#pyspark.RDD.groupByKey) transformation. This transformation groups all the elements of the RDD with the same key into a single list, stored in one of the partitions. 
# MAGIC  
# MAGIC Use `groupByKey()` on `words_pair_RDD`
# MAGIC  to generate a pair RDD of type `('word', list)`.

# COMMAND ----------

words_grouped = words_pair_RDD.groupByKey()

for key, value in words_grouped.collect():
    print '{0}: {1}'.format(key, list(value))
    
Test.assertEqualsHashed(sorted(words_grouped.mapValues(lambda x: list(x)).collect()),
                  'fdaad77fd81ef2df23d98ff7fd438fa700ca1fcf',
                  'incorrect value for words_grouped')

# COMMAND ----------

# MAGIC %md Using the `groupByKey()` transformation results in an `pairRDD` containing words as keys, and Python iterators as values. Python iterators are a class of objects on which we can iterate, i.e.
# MAGIC 
# MAGIC     a = some_iterator()
# MAGIC     for elem in a:
# MAGIC         # do stuff with elem
# MAGIC 
# MAGIC Python lists and dictionnaries are iterators for example.
# MAGIC 
# MAGIC Now sum the iterator using a `map()` transformation. The result should be a pair RDD consisting of (word, count) pairs.
# MAGIC 
# MAGIC Hint: there exists a `sum` function
# MAGIC Hint 2: you want to perform an operation only on the values of the pairRDD. Take a look at [mapValues()](http://spark.apache.org/docs/latest/api/python/pyspark.html#pyspark.RDD.mapValues).

# COMMAND ----------

word_grouped_counts = words_grouped.map(lambda(x,y) : (x,sum(y)))
print(word_grouped_counts.collect())

Test.assertEqualsHashed(sorted(word_grouped_counts.collect()),
                  'c20f05d36e98ae399b2cbe5b6cb9bf01b675455a',
                  'incorrect value for word_grouped_counts')

# COMMAND ----------

# MAGIC %md There are two problems with using `groupByKey()`:
# MAGIC   + The operation requires a lot of data movement to move all the values into the appropriate partitions (remember the cost of network communications!).
# MAGIC   + The lists can be very large. Consider a word count of English Wikipedia: the lists for common words (e.g., the, a, etc.) would be huge and could exhaust the available memory of a worker.
# MAGIC 
# MAGIC A better approach is to start from the pair RDD and then use the [reduceByKey()](http://spark.apache.org/docs/latest/api/python/pyspark.html#pyspark.RDD.reduceByKey) transformation to create a new pair RDD. The `reduceByKey()` transformation gathers together pairs that have the same key and applies the function provided to two values at a time, iteratively reducing all of the values to a single value. `reduceByKey()` operates by applying the function first within each partition on a per-key basis and then across the partitions, allowing it to scale efficiently to large datasets.
# MAGIC 
# MAGIC Compute the word count using `reduceByKey`

# COMMAND ----------

word_counts = words_pair_RDD.reduceByKey(lambda x, y: x + y)
print(word_counts.collect())

Test.assertEqualsHashed(sorted(word_counts.collect()), 'c20f05d36e98ae399b2cbe5b6cb9bf01b675455a',
                  'incorrect value for word_counts')

# COMMAND ----------

# MAGIC %md You should be able to perform the word count by composing functions, resulting in a smaller code. Use the `map()` on word RDD to create a pair RDD, apply the `reduceByKey()` transformation, and `collect` in one statement.

# COMMAND ----------

word_counts_collected = (words_RDD
                         .map(lambda x : (x,1))
                         .reduceByKey(lambda x, y : x+ y)
                         .collect()
                        )

print(word_counts_collected)

Test.assertEqualsHashed(sorted(word_counts_collected), 'c20f05d36e98ae399b2cbe5b6cb9bf01b675455a',
                  'incorrect value for word_counts_collected')

# COMMAND ----------

# MAGIC %md Compute the number of unique words using one of the RDD you have already created.

# COMMAND ----------

unique_words = words_RDD.distinct().count()
print(unique_words)

Test.assertEquals(unique_words, 6, 'incorrect count of unique_words')

# COMMAND ----------

# MAGIC %md Use a `reduce()` action to sum the counts in `wordCounts` and then divide by the number of unique words to find the mean number of words per unique word in `word_counts`.  First `map()` RDD `word_counts`, which consists of (key, value) pairs, to an RDD of values.

# COMMAND ----------

from operator import add
total_count = (word_counts
              .map(lambda (x, y) : y)
              .reduce(add))

average = total_count / float(unique_words)
print(total_count)
print(round(average, 2))

Test.assertEquals(round(average, 2), 1.5, 'incorrect value of average')

# COMMAND ----------

# MAGIC %md ## Part 2: Apply word count to a file
# MAGIC 
# MAGIC In this section we will finish developing our word count application.  We'll have to build the `word_count` function, deal with real world problems like capitalization and punctuation, load in our data source, and compute the word count on the new data.
# MAGIC 
# MAGIC First, define a function for word counting. You should reuse the techniques that have been covered in earlier parts of this lab.  This function should take in an RDD that is a list of words like `words_RDD` and return a pair RDD that has all of the words and their associated counts.

# COMMAND ----------

def word_count(word_list_RDD):
    """Creates a pair RDD with word counts from an RDD of words.

    Args:
        wordListRDD (RDD of str): An RDD consisting of words.

    Returns:
        RDD of (str, int): An RDD consisting of (word, count) tuples.
    """
    r = word_list_RDD.map(lambda x : (x,1)).reduceByKey(lambda x, y : x+y)
    return(r)

print(word_count(words_RDD).collect())

Test.assertEqualsHashed(sorted(word_count(words_RDD).collect()),
                      'c20f05d36e98ae399b2cbe5b6cb9bf01b675455a',
                      'incorrect definition for word_count function')

# COMMAND ----------

# MAGIC %md Real world data is more complicated than the data we have been using in this lab. Some of the issues we have to address are:
# MAGIC   + Words should be counted independent of their capitialization (e.g., Spark and spark should be counted as the same word).
# MAGIC   + All punctuation should be removed.
# MAGIC   + Any leading or trailing spaces on a line should be removed.
# MAGIC  
# MAGIC Define the function `removePunctuation` that converts all text to lower case, removes any punctuation, and removes leading and trailing spaces.  Use the Python [re](https://docs.python.org/2/library/re.html) module to remove any text that is not a letter, number, or space. Reading `help(re.sub)` might be useful.
# MAGIC 
# MAGIC If you have never used regex (regular expressions) before, you can refer to [Regular-expressions.info](http://www.regular-expressions.info/python.html)
# MAGIC 
# MAGIC In order to test your regular expressions, you can use [Regex Tester](http://www.regexpal.com)
# MAGIC 
# MAGIC Regex can be a bit obscure at beginning, don't hesitate to search in [StackOverflow](http://stackoverflow.com) or to ask me for some help.

# COMMAND ----------

import re
import string

# Hint: string.punctuation contains all the punctuation symbols

def remove_punctuation(text):
    """Removes punctuation, changes to lower case, and strips leading and trailing spaces.

    Note:
        Only spaces, letters, and numbers should be retained.  Other characters should should be
        eliminated (e.g. it's becomes its).  Leading and trailing spaces should be removed after
        punctuation is removed.

    Args:
        text (str): A string.

    Returns:
        str: The cleaned up string.
    """
    r = re.sub('[^a-z| |0-9]', '', text.strip().lower())
    return(r)

print(remove_punctuation('Hello World!'))
print(remove_punctuation(' No under_score!'))

Test.assertEquals(remove_punctuation("  Remove punctuation: there ARE trailing spaces. "),
                  'remove punctuation there are trailing spaces',
                  'incorrect definition for remove_punctuation function')

# COMMAND ----------

# MAGIC %md For the next part of this lab, we will use the [Complete Works of William Shakespeare](http://www.gutenberg.org/ebooks/100) from [Project Gutenberg](http://www.gutenberg.org/wiki/Main_Page). To convert a text file into an RDD, we use the `SparkContext.textFile()` method. We also apply the recently defined `remove_punctuation()` function using a `map()` transformation to strip out the punctuation and change all text to lowercase.  Since the file is large we use `take(15)`, instead of `collect()` so that we only print 15 lines.
# MAGIC 
# MAGIC Take a look at [zipWithIndex()](http://spark.apache.org/docs/latest/api/python/pyspark.html#pyspark.RDD.zipWithIndex) and [take()](http://spark.apache.org/docs/latest/api/python/pyspark.html#pyspark.RDD.take) to understand the print statement

# COMMAND ----------

file_path = '/FileStore/tables/shakespeare.txt'

shakespeare_RDD = (sc.textFile(file_path, 8)
                     .map(remove_punctuation))

print('\n'.join(shakespeare_RDD
                .zipWithIndex()  # to (line, lineNum) pairRDD
                .map(lambda (l, num): '{0}: {1}'.format(num, l))  # to 'lineNum: line'
                .take(15)))

# COMMAND ----------

# MAGIC %md Before we can use the `word_count()` function, we have to address two issues with the format of the RDD:
# MAGIC   + #### The first issue is that  that we need to split each line by its spaces.
# MAGIC   + #### The second issue is we need to filter out empty lines.
# MAGIC  
# MAGIC Apply a transformation that will split each element of the RDD by its spaces. For each element of the RDD, you should apply Python's string [split()](https://docs.python.org/2/library/string.html#string.split) function. You might think that a [map()](http://spark.apache.org/docs/latest/api/python/pyspark.html#pyspark.RDD.map) transformation is the way to do this, but think about what the result of the `split()` function will be: there is a better option.
# MAGIC 
# MAGIC Hint: remember the problem we had with `GroupByKey()`

# COMMAND ----------

shakespeare_words_RDD = shakespeare_RDD.filter(lambda x : x != '')
shakespeare_word_count_elem = shakespeare_words_RDD.count()
print shakespeare_words_RDD.top(5)
print shakespeare_word_count_elem

# This test allows for leading spaces to be removed either before or after
# punctuation is removed.
Test.assertTrue(shakespeare_word_count_elem == 927631 or shakespeare_word_count_elem == 928908,
                'incorrect value for shakespeare_word_count_elem')

Test.assertEqualsHashed(shakespeare_words_RDD.top(5),
                  'f177c26ee0bc3a48368d7a92c08dd754237c3558',
                  'incorrect value for shakespeare_words_RDD')

# COMMAND ----------

# MAGIC %md The next step is to filter out the empty elements.  Remove all entries where the word is `''`.

# COMMAND ----------

shakespeare_nonempty_words_RDD = shakespeare_words_RDD.filter(lambda x : x != '')
shakespeare_nonempty_word_elem_count = shakespeare_nonempty_words_RDD.count()
print(shakespeare_nonempty_word_elem_count)

Test.assertEquals(shakespeare_nonempty_word_elem_count, 882996, 
                  'incorrect value for shakespeare_nonempty_word_elem_count')

# COMMAND ----------

# MAGIC %md You now have an RDD that contains only words.  Next, apply the `word_count()` function to produce a list of word counts. We can view the top 15 words by using the `takeOrdered()` action. However, since the elements of the RDD are pairs, you will need a custom sort function that sorts using the value part of the pair.
# MAGIC 
# MAGIC Use the `wordCount()` function and `takeOrdered()` to obtain the fifteen most common words and their counts.

# COMMAND ----------

top15_words = word_count(shakespeare_nonempty_words_RDD).takeOrdered(15, lambda (x,y) : -y)
print '\n'.join(map(lambda (w, c): '{0}: {1}'.format(w, c), top15_words))

# COMMAND ----------

Test.assertEqualsHashed(top15_words,
                  'fbb4c8c74f98eeef70d021893f276231dfff55cb',
                  'incorrect value for top15WordsAndCounts')

# COMMAND ----------

# MAGIC %md You will notice that many of the words are common English words. These are called stopwords. In practice, when we do natural language processing, we filter these stopwords as they do not contain a lot of information.
