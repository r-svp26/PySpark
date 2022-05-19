# Databricks notebook source
from pyspark.sql import SparkSession

# COMMAND ----------

spark = SparkSession.builder.appName("Ops").getOrCreate()

# COMMAND ----------

df = spark.read.csv('/FileStore/tables/appl_stock.csv',inferSchema=True,header=True)

# COMMAND ----------

df.printSchema()

# COMMAND ----------

df.show()

# COMMAND ----------

df.head(2)

# COMMAND ----------

df.head(2)[0]
# Grab the first value

# COMMAND ----------

df.head(2)[1]
# Grab the second value

# COMMAND ----------

# Show all the results whos values are less than 500 using sql.
df.filter('Close < 500').show()

# COMMAND ----------

# Show spefic column value only
df.filter('Close < 500').select('Open').show()

# COMMAND ----------

# Show multiple column value only
df.filter('Close < 500').select('Open','Close').show()

# COMMAND ----------

# Show multiple column value using normal python operator 
'''
    Using SQL
    df.filter('Close < 500').select('Open','Close').show()
'''
df.filter(df['Close'] < 500).show()

# COMMAND ----------

df.filter(df['Close'] < 500).select('Volume').show()

# COMMAND ----------

# Filtering based multiple operation

# COMMAND ----------

df.filter( (df['Close'] < 500) & (df['Open'] > 200) ).show()

# COMMAND ----------

df.filter( (df['Close'] < 500) & ~(df['Open'] > 200) ).show()
'''
    & is for and
    | is for or
    ~ is for not
'''

# COMMAND ----------

df.filter( (df['Open'] <= 500) & (df['Close'] > 200) ).show()
'''
    & is for and
    | is for or
    ~ is for not
'''

# COMMAND ----------

# Collect method
result = df.filter(df['Low'] == 197.16).collect()

# COMMAND ----------

result

# COMMAND ----------

row = result[0]

# COMMAND ----------

# As a dictionary
row.asDict()['Volume']

# COMMAND ----------


