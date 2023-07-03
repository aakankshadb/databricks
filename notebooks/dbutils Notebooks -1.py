# Databricks notebook source
dbutils.help()


# COMMAND ----------

dbutils.fs.help("ls")

# COMMAND ----------

# MAGIC %md 
# MAGIC #### Data Utility in dbutils

# COMMAND ----------

dbutils.data.help()

# COMMAND ----------

data =[(1,"Aakanksha"),(2,"Manigandan"),(3,"Ajay")]
schema = ["id","name"]
df= spark.createDataFrame(data=data,schema=schema)
display(df)
dbutils.data.summarize(df)

# COMMAND ----------

# MAGIC %md 
# MAGIC #### File System Utility in dbutils
# MAGIC

# COMMAND ----------

dbutils.fs.help()

# COMMAND ----------

dbutils.fs.cp("/FileStore/aakanksha/temp1/Empdata.csv","/FileStore/aakanksha/temp2/Outputdata.csv")

# COMMAND ----------

