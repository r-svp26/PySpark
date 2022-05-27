# Databricks notebook source
from pyspark import SparkConf, SparkContext
conf = SparkConf().setAppName('Map Quiz')
sc = SparkContext.getOrCreate(conf=conf)

# COMMAND ----------

rdd = sc.textFile('/FileStore/tables/Map_Quiz_First.txt')

# COMMAND ----------

rdd.collect()

# COMMAND ----------

# Fuction to count the words in string.
def quiz(x):
    l = x.split(' ')
    l2 = []
    for s in l:
        l2.append(len(s))
    return l2
result = rdd.map(quiz)

# COMMAND ----------

result.collect()

# COMMAND ----------

# MAGIC %md Using Lambda Expression

# COMMAND ----------

lambda_result = rdd.map(lambda x: [len(s) for s in x.split(' ')])

# COMMAND ----------

lambda_result.collect()

# COMMAND ----------


