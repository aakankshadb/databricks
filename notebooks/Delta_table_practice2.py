# Databricks notebook source
# MAGIC %md 
# MAGIC #### Inserting data into delta table.
# MAGIC

# COMMAND ----------

from delta.tables import * 

DeltaTable.create(spark)\
          .tableName("emp_details")\
          .addColumn("emp_id","INT")\
          .addColumn("emp_name","STRING")\
          .addColumn("gender","STRING")\
          .addColumn("salary","INT")\
          .addColumn("Dept","STRING")\
          .location("FileStore/aakanksha/deltapractice2")\
          .execute()

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from emp_details;

# COMMAND ----------

# MAGIC %md 
# MAGIC #### SQL Style Insert
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into emp_details values(100,"Rajesh","M",2000,"IT");
# MAGIC

# COMMAND ----------


display(spark.sql("select * from emp_details"))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Dataframe Style Insert
# MAGIC

# COMMAND ----------

from pyspark.sql.types import IntegerType,StringType
emp_data = [(200,"Phillip","M",8000,"HR")]
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

emp_df.write.format("delta").mode("append").saveAsTable("emp_details")

# COMMAND ----------

display(spark.sql("select * from emp_details"))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Dataframe Insert Into Method
# MAGIC

# COMMAND ----------

from pyspark.sql.types import IntegerType,StringType
emp_data = [(300,"Nitisha","F",10000,"Sales")]
emp_schema = StructType([
                            StructField("emp_id",IntegerType()),
                            StructField("emp_name",StringType()),
                            StructField("gender",StringType()),
                            StructField("salary",IntegerType()),
                            StructField("Dept",StringType())
])
emp_df1 = spark.createDataFrame(data=emp_data,schema=emp_schema)
display(emp_df1)

# COMMAND ----------

emp_df1.write.insertInto("emp_details",overwrite = False)


# COMMAND ----------

display(spark.sql("select * from emp_details"))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Insert Using Temp View
# MAGIC

# COMMAND ----------

emp_df1.createOrReplaceTempView("emp_data")


# COMMAND ----------

# MAGIC %sql
# MAGIC select * from emp_data;

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into emp_details
# MAGIC select * from emp_data;

# COMMAND ----------

display(spark.sql("select * from emp_details"))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Spark SQL Insert
# MAGIC

# COMMAND ----------

spark.sql("insert into emp_details select * from emp_data")

# COMMAND ----------

display(spark.sql("select * from emp_details"))

# COMMAND ----------

