# Databricks notebook source
# MAGIC %md 
# MAGIC #### Create Delta Table
# MAGIC

# COMMAND ----------

from delta.tables import * 

DeltaTable.create(spark)\
        .tableName("emp_table2")\
        .addColumn("emp_id","INT")\
        .addColumn("emp_name","STRING")\
        .addColumn("gender","STRING")\
        .addColumn("salary","INT")\
        .addColumn("Dept","STRING")\
        .location("FileStore/aakanksha/delta_practice3")\
        .execute()


# COMMAND ----------

# MAGIC %md
# MAGIC #### Populating the table

# COMMAND ----------

from pyspark.sql.types import IntegerType,StringType
emp_data=[(100,"Stephen","M",2000,"IT"),
          (200,"Phillip","M",8000,"HR"),
          (300,"Lara","F",6000,"SALES"),
          (400,"Mike","M",4000,"IT"),
          (500,"Sarah","F",9000,"HR"),
          (600,"Serena","F",5000,"SALES"),
          (700,"Mark","M",7000,"SALES")
        ]
emp_schema = StructType([
                            StructField("emp_id",IntegerType()),
                            StructField("emp_name",StringType()),
                            StructField("gender",StringType()),
                            StructField("salary",IntegerType()),
                            StructField("Dept",StringType())
])        
emp_df = spark.createDataFrame(data=emp_data,schema=emp_schema)
display(emp_df)


# COMMAND ----------

emp_df.write.format("delta").mode("overwrite").saveAsTable("emp_table2")

# COMMAND ----------

display(spark.sql("select * from emp_table2"))

# COMMAND ----------

# MAGIC %md 
# MAGIC #### Method 1 : Delete data using SQL

# COMMAND ----------

# MAGIC %sql 
# MAGIC delete from emp_table2 where emp_id = 100;

# COMMAND ----------

# MAGIC %md
# MAGIC #### Method 2 : Delete data using Spark SQL 
# MAGIC

# COMMAND ----------

spark.sql("delete from emp_table2 where emp_id = 300")

# COMMAND ----------

# MAGIC %md 
# MAGIC #### Method 3 : Delete data using delta table location

# COMMAND ----------

# MAGIC %sql
# MAGIC Delete from delta.`/user/hive/warehouse/FileStore/aakanksha/delta_practice3` where emp_id = 200

# COMMAND ----------

# MAGIC %md
# MAGIC #### Method 4: Delete data using delta table instance

# COMMAND ----------

from delta.tables import * 
from pyspark.sql.functions import * 

emp_table2_instance = DeltaTable.forName(spark,"emp_table2")
emp_table2_instance.delete("emp_id = 400")

# COMMAND ----------

# MAGIC %md 
# MAGIC #### Delete data based on Multiple conditions-SQL Predicate 

# COMMAND ----------

emp_table2_instance.delete("emp_id = 500 and gender = 'F'")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Delete data using Pyspark Instance-Spark SQL Predicate

# COMMAND ----------

emp_table2_instance.delete(col("emp_id")== 600)

# COMMAND ----------

