# Databricks notebook source
dbutils.fs.mount(
    source="wasbs://aakankshacontainer@aakankshastrg.blob.core.windows.net",
    mount_point="/mnt/mountingstorage",
    extra_configs={"fs.azure.account.key.aakankshastrg.blob.core.windows.net":"Vdjhn6nIUvbfvk6xHaIg5VhpjsyzovzyMV2I6QQpHv5W5BXqxglcVdje9gFTMJACOIL8jmtN20kA+AStx38AFg=="}

)

# COMMAND ----------

csv_df=spark.read.format("csv").options(header=True,inferSchema=True).load("/mnt/mountingstorage/Bronze/sample_csv.csv")
display(csv_df)

# COMMAND ----------

json_df=spark.read.format("json").options(multiline=True).load("/mnt/mountingstorage/Bronze/sample_json.json")
display(json_df)
json_df.printSchema()

# COMMAND ----------

from pyspark.sql.functions import explode_outer,col,when
flatten_df=json_df.withColumn("name",explode_outer("Projects.name"))\
                  .withColumn("status",explode_outer("Projects.status"))\
                  .drop("projects")
                  
display(flatten_df)
flatten_df.printSchema()

# COMMAND ----------

distinct_csv_df=csv_df.distinct()
display(distinct_csv_df)

# COMMAND ----------

distinct_flatten_df=flatten_df.distinct()
display(distinct_flatten_df)

# COMMAND ----------

replacenull_csv_df=distinct_csv_df.withColumn("city",when(col('city') == 'Null','0')
                                             .when(col('city').isNull(),'0')\
                                             .otherwise(distinct_csv_df.city))\
                                             .na.fill(0,['age'])
                        #.filter(distinct_csv_df.id.isNull() | distinct_csv_df.name.isNull() | distinct_csv_df.age.isNull() | distinct_csv_df.city.isNull())
display(replacenull_csv_df)



# COMMAND ----------

replacenull_json_df=distinct_flatten_df.na.fill('0',['name'])\
                           .na.fill(0,['salary'])\
                           .na.fill('0',['status'])
        # .filter(distinct_flatten_df.department.isNull() | distinct_flatten_df.id.isNull() | distinct_flatten_df.projects.isNull() | distinct_flatten_df.salary.isNull() | distinct_flatten_df.name.isNull()))
display(replacenull_json_df)

# COMMAND ----------

#writing csv dataframe into silver layer.
replacenull_csv_df.write.format('parquet').mode('overwrite').save("/mnt/mountingstorage/Silver/csv/")

# COMMAND ----------

#writing json dataframe into silver layer.
replacenull_json_df.write.format('parquet').mode('overwrite').save("/mnt/mountingstorage/Silver/json/")

# COMMAND ----------

csv_df1=spark.read.format('parquet').options(header=True,inferSchema=True).load('/mnt/mountingstorage/Silver/csv/')
display(csv_df1)



# COMMAND ----------

json_df1=spark.read.format('parquet').options(multiline=True).load('/mnt/mountingstorage/Silver/json/')\
              .withColumnRenamed('name',"projectname")

display(json_df1)

# COMMAND ----------

join_df=csv_df1.join(json_df1,["id"],"inner")
display(join_df)
join_df.printSchema()

# COMMAND ----------

#writing the joined dataframe to gold layer
join_df.write.format('delta').option('path','/mnt/mountingstorage/Gold').saveAsTable("employee_details")


# COMMAND ----------

# MAGIC %sql
# MAGIC select * from employee_details

# COMMAND ----------

id_list=[31,40,7,15]
filter_df=join_df.filter(~join_df.id.isin(id_list)).sort("id")
display(filter_df)