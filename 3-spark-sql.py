# Databricks notebook source
# MAGIC %md # Spark SQL and DataFrames

# COMMAND ----------

# MAGIC %md ### 1. Basic manipulations
# MAGIC First, let's create some users:

# COMMAND ----------

from datetime import date

# Let's first create some users:
col_names = ["first_name", "last_name", "birth_date", "gender", "country"]
users = [
  ("Alice", "Jones", date(1981, 4, 15), "female", "Canada"),
  ("John", "Doe", date(1951, 1, 21), "male", "USA"),
  ("Barbara", "May", date(1951, 9, 1), "female", "Australia"),
  ("James", "Smith", date(1975, 7, 12), "male", "United Kingdom"),
  ("Gerrard", "Dupont", date(1968, 5, 9), "male", "France"),
  ("Amanda", "B.", date(1988, 12, 16), "female", "New Zeland")
]

users_df = spark.createDataFrame(users, col_names)
display(users_df) # Only works in Databricks. Elswehere, use "df.show()" or "df.toPandas()"

# COMMAND ----------

# MAGIC %md Now it's your turn, create more users (at least 3, with different names) and add to the initial users, saving the result in a new variable.

# COMMAND ----------

new_users = [
  ("Toto", "To", date(1994, 3, 3), "female", "Canada"),
  ("Tata", "Ta", date(1998, 6, 15), "male", "USA"),
  ("Titi", "Ti", date(1996, 8, 25), "female", "Australia"),
]
new_users_df = spark.createDataFrame(new_users, col_names)
all_users_df = users_df.union(new_users_df)
display(all_users_df) # or all_users_df.show()

# COMMAND ----------

# MAGIC %md Now, select only two columns and show the resulting DataFrame, without saving it into a variable.

# COMMAND ----------

all_users_df.select('first_name','last_name').show()

# COMMAND ----------

# MAGIC %md Now, register your DataFrame as a table and select the same two columns with a SQL query string

# COMMAND ----------

query_string = "SELECT first_name, last_name FROM new_view"
all_users_df.createOrReplaceTempView("new_view") # creates a local temporary table accessible by a SQL query
spark.sql(query_string).show()

# COMMAND ----------

# MAGIC %md Now we want to add an unique identifier for each user in the table. There are many strategies for that, and for our example we will use the string `{last_name}_{first_name}`
# MAGIC 
# MAGIC You can use `all_users_df` since your latest operation did not override its values. Add a new column called `user_id` to your DataFrame and save to a new variable.
# MAGIC 
# MAGIC **Hint:** The first place to look is in the [functions](http://spark.apache.org/docs/latest/api/python/pyspark.sql.html#module-pyspark.sql.functions) package

# COMMAND ----------

from pyspark.sql import functions as fn

users_with_id = all_users_df.withColumn('identifier', 
                    fn.concat(fn.col('last_name'),fn.lit('_'), fn.col('first_name')))
display(users_with_id)

# COMMAND ----------

# MAGIC %md You can also do the same thing with an User Defined Function by passing a lambda, although it is not recommended when there is already a function in the `functions` package.
# MAGIC 
# MAGIC Add a new column called `user_id_udf` to the DataFrame, using an UDF that receives two parameters and concatenate them.

# COMMAND ----------

from pyspark.sql.types import StringType

def my_udf(col_1, col_2):
  return col_1 + col_2

concat_udf = fn.udf(my_udf, StringType())
users_with_id_udf = users_with_id.withColumn("user_id_udf", concat_udf(users_with_id['last_name'], users_with_id['first_name']))
display(users_with_id_udf)

# COMMAND ----------

# MAGIC %md Now, let's add another column called `age` with the computed age (in years) of each user, based on a given reference date, and save the resulting DataFrame into a new variable.
# MAGIC 
# MAGIC **Hint:** You can first compute the age in months, and then divide by 12. A final operation will probably be needed to get an integer number.

# COMMAND ----------

reference_date = date(2017, 12, 31)

users_with_age = users_with_id_udf.withColumn("age",2017- fn.year('birth_date'))
# we can do this since the reference date is the 31th of december
display(users_with_age)

# COMMAND ----------

# MAGIC %md Now, an analytical question: How many users of each gender who are more than 40 years old exist in this data? The solution must be a DataFrame with two columns: `age` and `count` and two lines, one for each gender.
# MAGIC 
# MAGIC Bonus: Try to do your solution in a single chain (without intermediate variables)
# MAGIC 
# MAGIC **Hint:** You will need to filter and aggregate the data.

# COMMAND ----------

result = users_with_age.select('*').where("age>40").groupBy('gender').agg(fn.count('*').alias('count'))
display(result)

# COMMAND ----------

# MAGIC %md ### 2. Reading files, performing joins, and aggregating
# MAGIC 
# MAGIC For this section you will use some fake data of two datasets: `Users` and `Donations`. The data is provided in two CSV files *with header* and using *comma* as separator.
# MAGIC 
# MAGIC The `Users` dataset contains information about about the users. 
# MAGIC 
# MAGIC The `Donations` dataset contains information about Donations performed by those users.
# MAGIC 
# MAGIC The first task is to read these files into the appropriate DataFrames.
# MAGIC 
# MAGIC **Note:** You need to set the option "inferSchema" to true in order to have the columns in the correct types.
# MAGIC 
# MAGIC _The data for this section has been created using [Mockaroo](https://www.mockaroo.com/)_.

# COMMAND ----------

users_from_file = spark.read\
          .format("csv")\
          .option("header", "true")\
          .option("sep", ",")\
          .option("inferSchema","true")\
          .load("/FileStore/tables/files_users.csv")
          
donations = spark.read\
          .format("csv")\
          .option("header", "true")\
          .option("sep", ",")\
          .option("inferSchema","true")\
          .load("/FileStore/tables/files_donations.csv")

# COMMAND ----------

# MAGIC %md Now investigate the columns, contents and size of both datasets

# COMMAND ----------

# print the column names and types
users_from_file.printSchema()
donations.printSchema() 

# print 5 elements of the datasets in a tabular format
users_from_file.select('*').limit(5).show()
donations.select('*').limit(5).show()

# print the number of lines of each dataset
print users_from_file.count()
print donations.count()


# COMMAND ----------

# MAGIC %md **Note:** If all the column types shown in the previous results are "string", you need to make sure you passed "inferSchema" as true when reading the CSV files before continuing.
# MAGIC 
# MAGIC Before using the data, we may want to add some information about the users. 
# MAGIC 
# MAGIC Add a column containing the age of each user.

# COMMAND ----------

users_from_file = users_from_file.withColumn("age",2017- fn.year('birth_date'))
users_from_file.show()

# COMMAND ----------

# MAGIC %md Another useful information to have is the age **range** of each user. Using the `when` function, create the following 5 age ranges:
# MAGIC - "(0, 25]"
# MAGIC - "(25, 35]"
# MAGIC - "(35, 45]"
# MAGIC - "(45, 55]"
# MAGIC - "(55, ~]"
# MAGIC 
# MAGIC And add a new column to the users DataFrame, containing this information.
# MAGIC 
# MAGIC **Note:** When building logical operations with Spark DataFrames, it's better to be add parantheses. Example:
# MAGIC ```python
# MAGIC df.select("name", "age").where( (df("age") > 20) & (df("age") <= 30) )
# MAGIC ```
# MAGIC 
# MAGIC **Note 2:** If you are having problems with the `when` function, you can make an User Defined Function and do your logic in standard python.

# COMMAND ----------

range = fn.when(users_from_file.age <= 25, "(0, 25]")\
.when((users_from_file.age > 25) & (users_from_file.age <= 35), "(25, 35]")\
.when(( users_from_file.age > 35) & (users_from_file.age <= 45), "(35, 45]")\
.when(( users_from_file.age > 45) & (users_from_file.age <= 55), "(45, 55]")\
.when( users_from_file.age > 55, "(55, ~]")

users_from_file = users_from_file.withColumn("range", range)
users_from_file.show()

# COMMAND ----------

# MAGIC %md Now that we have improved our users' DataFrame, the first analysis we want to make is the average donation performed by each gender. However, the gender information and the donation value are in different tables, so first we need to join them, using the `user_id` as joining key.
# MAGIC 
# MAGIC **Note:** Make sure you are not using the `users` DataFrame from the first part of the lab.
# MAGIC 
# MAGIC **Note 2:** For better performance, you can [broadcast](http://spark.apache.org/docs/latest/api/python/pyspark.sql.html#pyspark.sql.functions.broadcast) the smaller dataset. But you should only do this when you are sure the DataFrame fits in the memory of the nodes.

# COMMAND ----------

joined_df = users_from_file.join(donations, 'user_id')
display(joined_df)

# COMMAND ----------

# MAGIC %md Now, use aggregation to find the the min, max and avg donation by gender.

# COMMAND ----------

donations_by_gender = joined_df.groupBy('gender').agg(fn.min('donation_value').alias('Min'),fn.max('donation_value').alias('Max'),fn.avg('donation_value').alias('Average'))
donations_by_gender.show()

# COMMAND ----------

# MAGIC %md Now, make the necessary transformations and aggregations to answer to the following questions about the data. Note that some questions are only about the users, so make sure to use the smaller possible dataset when looking for your answers!
# MAGIC 
# MAGIC **Question 1:** 
# MAGIC 
# MAGIC a) What's the average, min and max age of the users? 
# MAGIC 
# MAGIC b) What's the average, min and max age of the users, by gender?

# COMMAND ----------

result_1a = joined_df.agg(fn.min('age').alias('age_Min'),fn.max('age').alias('age_Max'),fn.avg('age').alias('age_Average'))
result_1a.show()

result_1b = joined_df.groupBy('gender').agg(fn.min('age').alias('age_Min'),fn.max('age').alias('age_Max'),fn.avg('age').alias('age_Average'))
result_1b.show()

# COMMAND ----------

# MAGIC %md **Question 2:**
# MAGIC 
# MAGIC a) How many distinct country origins exist in the data? Print a DataFrame listing them.
# MAGIC 
# MAGIC b) What are the top 5 countries with the most users? Print a DataFrame containing the name of the countries and the counts.

# COMMAND ----------

result_2a = users_from_file.select('country').distinct()
result_2a.show()
result_2a.count()

result_2b = users_from_file.groupBy('country').agg(fn.count('*').alias('Number_of_users')).orderBy(fn.desc("Number_of_users")).limit(5)
result_2b.show()

# COMMAND ----------

# MAGIC %md **Question 3:**
# MAGIC 
# MAGIC What's the number of donations average, min and max donations values by age range?

# COMMAND ----------

result_3 = joined_df.groupBy('range').agg(fn.avg('donation_value').alias('Average'),fn.min('donation_value').alias('Min'),fn.max('donation_value').alias('Max') )
result_3.show()

# COMMAND ----------

# MAGIC %md **Question 4:**
# MAGIC 
# MAGIC a) What's the number of donations, average, min and max donation values by user location (country)?
# MAGIC 
# MAGIC b) What is the number of donations, average, min and max donation values by gender for each user location (contry)? (the resulting DataFrame must contain 5 columns: the gender of the user, their country and the 3 metrics)

# COMMAND ----------

result_4a = joined_df.groupBy('country').agg(fn.count('*').alias('Number'),fn.avg('donation_value').alias('Average'),fn.min('donation_value').alias('Min'),fn.max('donation_value').alias('Max') )
result_4a.show()

result_4b = joined_df.groupBy('country','gender').agg(fn.count('*').alias('Number'),fn.avg('donation_value').alias('Average'),fn.min('donation_value').alias('Min'),fn.max('donation_value').alias('Max') ).orderBy('country',fn.desc('gender'))
result_4b.show()

# COMMAND ----------

# MAGIC %md **Question 5**
# MAGIC 
# MAGIC Which month of the year has the largest aggregated donation value?
# MAGIC 
# MAGIC **Hint**: you can use a function to extract the month from a date, then you can aggregate to find the total donation value.

# COMMAND ----------

result_5 = joined_df.withColumn("months",fn.month('date')).groupBy('months').agg(fn.round(fn.sum('donation_value'),2).alias('sum_donations')).orderBy(fn.desc('sum_donations'))
display(result_5)

# COMMAND ----------

# MAGIC %md ### 3. Window Functions
# MAGIC 
# MAGIC _This section uses the same data as the last one._
# MAGIC 
# MAGIC Window functions are very useful for gathering aggregated data without actually aggregating the DataFrame. They can also be used to find "previous" and "next" information for entities, such as an user. [This article](https://databricks.com/blog/2015/07/15/introducing-window-functions-in-spark-sql.html) has a very nice explanation about the concept.

# COMMAND ----------

# MAGIC %md We want to find the users who donated less than a threshold and remove them from the donations dataset. Now, there are two ways of doing that:
# MAGIC 
# MAGIC 1) Performing a traditional aggregation to find the users who donated less than the threshold, then filtering these users from the donations dataset, either with `where(not(df("user_id").isin(a_local_list)))` or with a join of type "anti-join".
# MAGIC 
# MAGIC 2) Using window functions to add a new column with the aggregated donations per user, and then using a normal filter such as `where(aggregated_donation < threshold)`
# MAGIC 
# MAGIC Let's implement both and compare the complexity:
# MAGIC 
# MAGIC First, perform the traditional aggregation and find the users who donated less than 500 in total:

# COMMAND ----------

bad_users = joined_df.groupBy('user_id').agg(fn.round(fn.sum('donation_value'),2).alias('Sum_of_donations')).where('Sum_of_donations<500').orderBy(fn.desc('Sum_of_donations'))
bad_users.show()

# COMMAND ----------

# MAGIC %md You should have found around 10 users. Now, perform an "anti-join" to remove those users from `joined_df`.
# MAGIC 
# MAGIC **Hint:** The `join` operation accepts a third argument which is the join type. The accepted values are: 'inner', 'outer', 'full', 'fullouter', 'leftouter', 'left', 'rightouter', 'right', 'leftsemi', 'leftanti', 'cross'.

# COMMAND ----------

good_donations = joined_df.join(bad_users, 'user_id','leftanti')
good_donations.count()

# COMMAND ----------

# MAGIC %md Verify if the count of `good_donations` makes sense by performing a normal join to find the `bad_donations`.

# COMMAND ----------

bad_donations = joined_df.join(bad_users, 'user_id')
bad_donations.count()

# COMMAND ----------

# MAGIC %md If you done everything right, at this point `good_donations.count()` + `bad_donations.count()` = `joined_df.count()`.
# MAGIC 
# MAGIC But using the join approach can be very heavy and it requires multipe operations. For this kind of problems, Window Functions are better.

# COMMAND ----------

good_donations.count() + bad_donations.count() == joined_df.count()

# COMMAND ----------

# MAGIC %md The first step is to create your window specification over `user_id` by using partitionBy

# COMMAND ----------

from pyspark.sql import Window

window_spec = Window.partitionBy('user_id')

# COMMAND ----------

# MAGIC %md Then, you can use one of the window functions of the `pyspark.sql.functions` package, appling to the created window_spec by using the `over` method. 
# MAGIC 
# MAGIC **Hint:** If you are blocked, try looking at the [documentation](http://spark.apache.org/docs/latest/api/python/pyspark.sql.html#pyspark.sql.Column.over) for the `over` method, or searching on StackOverflow.

# COMMAND ----------

new_column = fn.sum('donation_value').over(window_spec)

donations_with_total = joined_df.withColumn('Total_Donations',fn.round(new_column,2))
donations_with_total.show()

# COMMAND ----------

# MAGIC %md And now you can just filter on the `total_donated_by_user` column:

# COMMAND ----------

good_donations_wf = donations_with_total.select('user_id').where('Total_Donations>500')
good_donations_wf.count()

# COMMAND ----------

# MAGIC %md If you done everything right, you should obtain `good_donations_wf.count()` = `good_donations.count()`

# COMMAND ----------

good_donations_wf.count() == good_donations.count()

# COMMAND ----------

# MAGIC %md Window functions also can be useful to find the "next" and "previous" operations by using `functions.lead` and `functions.lag`. In our example, we can use it to find the date interval between two donations by a specific user.
# MAGIC 
# MAGIC For this kind of Window function, the window specification must be ordered by the date. So, create a new window specification partitioned by the `user_id` and ordered by the donation timestamp.

# COMMAND ----------

ordered_window = Window.partitionBy('user_id').orderBy('date')

# COMMAND ----------

# MAGIC %md Now use `functions.lag().over()` to add a column with the timestamp of the previous donation of the same user. Then, inspect the result to see what it looks like.

# COMMAND ----------

new_column = fn.lag('date',1).over(ordered_window)

donations_with_lag = good_donations.withColumn('Lag',new_column)
donations_with_lag.orderBy("user_id", "timestamp").show()

# COMMAND ----------

# MAGIC %md Finally, compute the average time it took for each user between two of their consecutive donations (in days), and print the 5 users with the smallest averages. The result must include at least the users' id, last name and birth date, as well as the computed average.

# COMMAND ----------

fn.when(donations_with_lag.Lag=='null', 0)\
.otherwise(fn.month(donations_with_lag.Lag))

# COMMAND ----------

time_differnce = fn.datediff(donations_with_lag.date, donations_with_lag.Lag)
users_average_between_donations = donations_with_lag.withColumn("days_between", time_differnce).groupBy('user_id').agg(fn.avg('days_between').alias('Average')).join(users_from_file,'user_id')
users_average_between_donations.show(5)

# COMMAND ----------

# MAGIC %md Congratulations, you have finished this notebook!
