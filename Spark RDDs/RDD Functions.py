# Databricks notebook source
from pyspark import SparkConf, SparkContext
conf = SparkConf().setAppName('Functions')
sc = SparkContext.getOrCreate(conf=conf)

# COMMAND ----------

rdd = sc.textFile('/FileStore/tables/Word_Count_Quiz.txt')

# COMMAND ----------

rdd.count()

# COMMAND ----------

rdd.flatMap(lambda x : x.split(' ')).count()

# COMMAND ----------

# MAGIC %md CountByValue()

# COMMAND ----------

rdd.countByValue()

# COMMAND ----------

# Count times words are presents
rdd.flatMap(lambda x : x.split(' ')).countByValue()

# COMMAND ----------


