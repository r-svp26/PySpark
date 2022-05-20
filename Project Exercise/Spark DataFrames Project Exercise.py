# Databricks notebook source
# MAGIC %md
# MAGIC # Spark DataFrames Project Exercise 

# COMMAND ----------

# MAGIC %md
# MAGIC Let's get some quick practice with your new Spark DataFrame skills, you will be asked some basic questions about some stock market data, in this case Walmart Stock from the years 2012-2017. This exercise will just ask a bunch of questions, unlike the future machine learning exercises, which will be a little looser and be in the form of "Consulting Projects", but more on that later!
# MAGIC 
# MAGIC For now, just answer the questions and complete the tasks below.

# COMMAND ----------

# MAGIC %md
# MAGIC #### Use the walmart_stock.csv file to Answer and complete the  tasks below!

# COMMAND ----------

# MAGIC %md
# MAGIC #### Start a simple Spark Session

# COMMAND ----------

from pyspark.sql import SparkSession

# COMMAND ----------

spark = SparkSession.builder.appName('project').getOrCreate()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Load the Walmart Stock CSV File, have Spark infer the data types.

# COMMAND ----------

df = spark.read.csv('/FileStore/tables/walmart_stock.csv',inferSchema=True,header=True)

# COMMAND ----------

# MAGIC %md
# MAGIC #### What are the column names?

# COMMAND ----------

df.columns

# COMMAND ----------

# MAGIC %md
# MAGIC #### What does the Schema look like?

# COMMAND ----------

df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Print out the first 5 columns.

# COMMAND ----------

'''
    Print the results
'''
for row in df.head(5):
    print(row)
    print('\n')

# COMMAND ----------

# MAGIC %md
# MAGIC #### Use describe() to learn about the DataFrame.

# COMMAND ----------

df.describe().show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bonus Question!
# MAGIC #### There are too many decimal places for mean and stddev in the describe() dataframe. Format the numbers to just show up to two decimal places. Pay careful attention to the datatypes that .describe() returns, we didn't cover how to do this exact formatting, but we covered something very similar. [Check this link for a hint](http://spark.apache.org/docs/latest/api/python/pyspark.sql.html#pyspark.sql.Column.cast)
# MAGIC 
# MAGIC If you get stuck on this, don't worry, just view the solutions.

# COMMAND ----------

df.describe().printSchema()

# COMMAND ----------

result = df.describe()

# COMMAND ----------

# import the format_number from pyspark sql
from pyspark.sql.functions import format_number, mean

# COMMAND ----------

result.select(result['summary'], 
              format_number(result['Open'].cast('float'),2).alias('Open'),
              format_number(result['High'].cast('float'),2).alias('High'),
              format_number(result['Low'].cast('float'),2).alias('Low'),
              format_number(result['Close'].cast('float'),2).alias('Close'),
              format_number(result['Volume'].cast('int'),2).alias('Volume'),
             ).show()

# COMMAND ----------

# Data should be matched 
'''
    Solved!
'''

# COMMAND ----------

# MAGIC %md
# MAGIC #### Create a new dataframe with a column called HV Ratio that is the ratio of the High Price versus volume of stock traded for a day.

# COMMAND ----------

df2 = df.withColumn('HV Ratio',df['High']/df['Volume'])
df2.select('HV Ratio').show()

# COMMAND ----------

# MAGIC %md
# MAGIC #### What day had the Peak High in Price?

# COMMAND ----------

# df.orderBy(df['High'].desc()).head(1)
df.orderBy(df['High'].desc()).head(1)[0][0]

# COMMAND ----------

# MAGIC %md
# MAGIC #### What is the mean of the Close column?

# COMMAND ----------

from pyspark.sql.functions import mean

# COMMAND ----------

df.select(mean("Close")).show()

# COMMAND ----------

# MAGIC %md
# MAGIC #### What is the max and min of the Volume column?

# COMMAND ----------

from pyspark.sql.functions import (max, min)

# COMMAND ----------

df.select(max('Volume'),min('Volume')).show()

# COMMAND ----------

# MAGIC %md
# MAGIC #### How many days was the Close lower than 60 dollars?

# COMMAND ----------

df.show()

# COMMAND ----------

from pyspark.sql.functions import count 

# COMMAND ----------

# Count the total numbers of records whose[Close] values is less than 60$ 
'''
    Using Python
    df.filter(['Close'] < 60).count()
'''
df.filter('Close < 60 ').count()


# COMMAND ----------

# MAGIC %md
# MAGIC #### What percentage of the time was the High greater than 80 dollars ?
# MAGIC #### In other words, (Number of Days High>80)/(Total Days in the dataset)

# COMMAND ----------

rst= df.filter('High > 80 ').count()
rst

# COMMAND ----------

percentage = (df.filter('High > 80 ').count() / df.count()) * 100
percentage

# COMMAND ----------

# MAGIC %md
# MAGIC #### What is the Pearson correlation between High and Volume?
# MAGIC #### [Hint](http://spark.apache.org/docs/latest/api/python/pyspark.sql.html#pyspark.sql.DataFrameStatFunctions.corr)

# COMMAND ----------

from pyspark.sql.functions import corr

# COMMAND ----------

df.select(corr('High','Volume')).show()

# COMMAND ----------

# MAGIC %md
# MAGIC #### What is the max High per year?

# COMMAND ----------

from pyspark.sql.functions import max,year

# COMMAND ----------

yeardf = df.withColumn('Year',year(df['Date']))

# COMMAND ----------

max_df = yeardf.groupBy('Year').max()

# COMMAND ----------

# max_df.select('Year','max(High)').show()
max_df.select('Year','max(High)').orderBy('Year').show()


# COMMAND ----------

# MAGIC %md
# MAGIC #### What is the average Close for each Calendar Month?
# MAGIC #### In other words, across all the years, what is the average Close price for Jan,Feb, Mar, etc... Your result will have a value for each of these months. 

# COMMAND ----------

df.show()

# COMMAND ----------

from pyspark.sql.functions import month, avg

# COMMAND ----------

# df.select(month(df['Date'])).show()

df_month = df.withColumn('Month',month(df['Date']))

# COMMAND ----------

monthsavg = df_month.select(['Month','Close']).groupBy('Month').mean()

# COMMAND ----------

monthsavg.select('Month','avg(Close)').orderBy('Month').show()

# COMMAND ----------

# MAGIC %md
# MAGIC # Great Job!
