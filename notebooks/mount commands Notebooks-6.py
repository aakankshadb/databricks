# Databricks notebook source
dbutils.fs.mount(
    source='wasbs://mountcontainer1@mountstrg1.blob.core.windows.net/',
    mount_point='/mnt/aakankshablobstorage',
    extra_configs={'fs.azure.account.key.mountstrg1.blob.core.windows.net':'iBdOnlMbcmFyGZ5xx5qakGH8k5VO0fspAzRkuAfEVlmq4lwZkxsq726FRlKRyydm6Jg/iEtUobZi+AStt0HElw=='}
    )

# COMMAND ----------

dbutils.fs.ls('/mnt/aakankshablobstorage')

# COMMAND ----------

dbutils.fs.cp('/mnt/aakankshablobstorage/Empdata.csv','/mnt/aakankshablobstorage/data/Empdata.csv')

# COMMAND ----------

dbutils.fs.mount(
    source="wasbs://mountcontainer1@mountstrg1.blob.core.windows.net/",
    mount_point="/mnt/aakankshablobstorage1",
    extra_configs={'fs.azure.sas.mountcontainer1.mountstrg1.blob.core.windows.net':'sv=2022-11-02&ss=bfqt&srt=sco&sp=rwdlacupiytfx&se=2023-07-06T04:19:43Z&st=2023-07-05T20:19:43Z&spr=https&sig=lKGTMeWQwEn01twMSck%2BAvNFuYv3aFXqG0Ny6jE%2Bzf0%3D'}
)



# COMMAND ----------

dbutils.fs.ls("/mnt/aakankshablobstorage1/")

# COMMAND ----------

