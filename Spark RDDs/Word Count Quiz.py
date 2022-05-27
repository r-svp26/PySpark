# Databricks notebook source
from pyspark import SparkConf, SparkContext
conf = SparkConf().setAppName('Word Count')
sc = SparkContext.getOrCreate(conf=conf)

# COMMAND ----------

rdd = sc.textFile('/FileStore/tables/Word_Count_Quiz.txt')

# COMMAND ----------

# filter(lambda x: len(x) != 0) is used to remove empty string
rdd2 = rdd.flatMap(lambda x : x.split(' ')).filter(lambda x: len(x) != 0)

# COMMAND ----------

rdd3 = rdd2.map(lambda x : (x,len(x)))

# COMMAND ----------

rdd3.reduceByKey(lambda x,y : x+y).collect()

# COMMAND ----------


