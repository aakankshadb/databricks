# Databricks notebook source
# DBTITLE 1,Method 1 : Creating table using Pyspark

from delta.tables import *
DeltaTable.create(spark)\
    .tableName("Employee_Details")\
    .addColumn("emp_id","INT")\
    .addColumn("emp_name","STRING")\
    .addColumn("gender","STRING")\
    .addColumn("salary","INT")\
    .addColumn("Department","STRING")\
    .property("description","table created for practice")\
    .location("/FileStore/tables/delta/createtable")\
    .execute()


# COMMAND ----------

# MAGIC %sql
# MAGIC select * from Employee_Details;

# COMMAND ----------

# DBTITLE 1,Method 2 :Creating table using  Sql
# MAGIC %sql 
# MAGIC create table if not exists employee_details(
# MAGIC     emp_id INT,
# MAGIC     emp_name STRING,
# MAGIC     gender STRING,
# MAGIC     salary STRING,
# MAGIC     department STRING
# MAGIC ) USING DELTA 

# COMMAND ----------

# MAGIC %sql
# MAGIC Select * from employee_details;

# COMMAND ----------

