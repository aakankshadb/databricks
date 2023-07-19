# Databricks notebook source
def create_mount_pt(src,mntpt,config):
    dbutils.fs.mount(
        source=src,
        mount_point=mntpt,
        extra_configs=config
        )

# COMMAND ----------

src="wasbs://containerrestapiprac@strgrestapiprac.blob.core.windows.net"
mntpt="/mnt/mountstrgrestapiprac"
config={"fs.azure.account.key.strgrestapiprac.blob.core.windows.net":"t4eR5GuZnLKfaV4UY7+4IWxXd8TulASqOBDB+kvkuJxRDlSymoWDrplAB4G1XtSAfmsDZcyzL3a/+AStrY83Mw=="}
create_mount_pt(src,mntpt,config)

# COMMAND ----------

import json
import requests
# from datetime import datetime

# currentdate=datetime.today().strftime('%Y-%m-%d')
url='https://api.publicapis.org/entries'

def get_web_data(url):
    response=requests.get(url)
    data=spark.sparkContext.parallelize([response.text])
    return data

outresult= get_web_data(url)
# print(currentdate)


# COMMAND ----------

df=spark.read.option("multiline",True).json(outresult)
display(df)

# COMMAND ----------

df.write.format('json').mode('append').save('/mnt/mountstrgrestapiprac/Bronze/')


# COMMAND ----------

from pyspark.sql.types import StructType,StructField,StringType,IntegerType,ArrayType

schema=StructType([
                    StructField("count",IntegerType()),
                    StructField("entries",ArrayType(StructType([
                    StructField("API",StringType()),
                    StructField("Auth", StringType()),
                    StructField("Category", StringType()),
                    StructField("Cors", StringType()),
                    StructField("Description", StringType()),
                    StructField("HTTPS", StringType()),
                    StructField("Link", StringType())
                    ])))
                    
])

# COMMAND ----------

def json_read(schema,multiline,path):
    df=spark.read.format("json").schema(schema).option(multiline,multiline).load(path)
    return df


# COMMAND ----------

multiline="true"
json_df=json_read(schema,multiline,"/mnt/mountstrgrestapiprac/Bronze/publicapi.json")
json_df.printSchema()
display(json_df)

# COMMAND ----------

from pyspark.sql.functions import explode
exploded_df=json_df.select("count",explode("entries").alias("data"))
display(exploded_df)


# COMMAND ----------

publicapidf=exploded_df.select("data.API",
                               "data.Auth",
                               "data.Category",
                               "data.Cors",
                               "data.Description",
                               "data.HTTPS",
                               "data.Link"
                               )
display(publicapidf)
