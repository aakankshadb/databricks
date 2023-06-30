# Databricks notebook source
# MAGIC %md 
# MAGIC #### Method1 : Creating delta table using Pyspark
# MAGIC

# COMMAND ----------


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

# MAGIC %md
# MAGIC #### Method2 : Creating delta table using SQL

# COMMAND ----------

# DBTITLE 0,Method 2 :Creating table using  Sql
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

# MAGIC %md 
# MAGIC #### Method3 : Creating table using Dataframe

# COMMAND ----------

# DBTITLE 0,Method 3 : Creating table using dataframes.
emp_data= [(100,"Stephen","M",2000,"IT"),
           (200,"Phillip","M",2000,"HR"),
           (300,"Lara","F",6000,"SALES")]
emp_schema = ["emp_id","emp_name","gender","salary","department"]
emp_df =spark.createDataFrame(data=emp_data,schema=emp_schema)
display(emp_df)   

emp_df.write.format("delta").saveAsTable("employee_details1")


# COMMAND ----------

# MAGIC %sql 
# MAGIC select * from employee_details1

# COMMAND ----------

# MAGIC %md 
# MAGIC #### Method 1:Creating a instance of employee_details1 using path of table
# MAGIC

# COMMAND ----------

employeedetails1_instance1 = DeltaTable.forPath(spark,"/user/hive/warehouse/employee_details1")
display(employeedetails1_instance1.toDF())

# COMMAND ----------

employeedetails1_instance1.delete("emp_id = 200")
display(employeedetails1_instance1.toDF())

# COMMAND ----------

# MAGIC %md
# MAGIC #### Method 2:Creating a table instance of employee_details1 using table name 
# MAGIC

# COMMAND ----------

employeedetails1_instance2 = DeltaTable.forName(spark,"employee_details1")
display(employeedetails1_instance2.toDF())


# COMMAND ----------

# MAGIC %md 
# MAGIC #### Checking the table History.

# COMMAND ----------

display(employeedetails1_instance2.history())

# COMMAND ----------

