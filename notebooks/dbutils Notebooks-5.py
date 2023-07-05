# Databricks notebook source
# MAGIC %python
# MAGIC data=[(1,"maheer","male"),(2,"pradeep","male"),(3,"annu","female"),(4,"shakti","female")]
# MAGIC cols =["id","name","gender"]
# MAGIC df=spark.createDataFrame(data,cols)
# MAGIC display(df)
# MAGIC

# COMMAND ----------

df.createOrReplaceTempView("person_details")


# COMMAND ----------

# MAGIC %sql
# MAGIC select * from person_details;

# COMMAND ----------

# MAGIC %sql
# MAGIC create widget dropdown genderDD default "male" choices select distinct gender from person_details;

# COMMAND ----------

# MAGIC %sql
# MAGIC select* from person_details where gender = getArgument("genderDD")

# COMMAND ----------

