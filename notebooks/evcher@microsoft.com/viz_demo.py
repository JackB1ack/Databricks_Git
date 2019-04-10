# Databricks notebook source
dataPath = "/databricks-datasets/Rdatasets/data-001/csv/ggplot2/diamonds.csv"
diamonds = spark.read.format("com.databricks.spark.csv")\
  .option("header","true")\
  .option("inferSchema", "true")\
  .load(dataPath)

# COMMAND ----------

display(diamonds)


# COMMAND ----------

diamonds.createOrReplaceTempView("diamonds_view")

# COMMAND ----------

# MAGIC %md SQL

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT color, avg(price) as price FROM diamonds_view GROUP BY color ORDER BY price

# COMMAND ----------

# MAGIC %md R

# COMMAND ----------

# MAGIC %r
# MAGIC 
# MAGIC library(ggplot2)
# MAGIC ggplot(diamonds, aes(carat, price, color = color, group = 1)) + geom_point(alpha = 0.3) + stat_smooth()

# COMMAND ----------

# MAGIC %md Join

# COMMAND ----------

dataPath = "/FileStore/tables/country_company-26d1a.csv"
retailers = spark.read.format("com.databricks.spark.csv")\
  .option("header","true")\
  .option("inferSchema", "true")\
  .load(dataPath)\
  .toDF('_c0','company','country')

# COMMAND ----------

display(retailers)

# COMMAND ----------

tmp = diamonds.join(retailers, '_c0')

# COMMAND ----------

display(tmp.head(100))

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT cut, avg(price) as price FROM diamonds_view GROUP BY cut ORDER BY price