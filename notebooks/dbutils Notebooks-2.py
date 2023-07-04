# Databricks notebook source
dbutils.widgets.help()


# COMMAND ----------

dbutils.widgets.combobox(name="fruitsCB",defaultValue="apple",choices=["grapes","kiwi","banana"],label="Fruits ComboBox")

# COMMAND ----------

dbutils.widgets.dropdown(name="fruitsDB",defaultValue="apple",choices=["apple","grapes","kiwi","banana"],label="Fruits DropdownBox")

# COMMAND ----------

dbutils.widgets.multiselect(name="fruitsMS",defaultValue="apple",choices=["apple","grapes","kiwi","banana"],label="Fruits Multiselect")

# COMMAND ----------

dbutils.widgets.text(name="fruitsTB",defaultValue="apple",label="Fruits TextBox")

# COMMAND ----------

dbutils.widgets.get("fruitsCB")

# COMMAND ----------

dbutils.widgets.get("fruitsDB")

# COMMAND ----------

dbutils.widgets.get("fruitsMS")

# COMMAND ----------

dbutils.widgets.getArgument("fruitsMS1","error:This widget is not present in the notebook")

# COMMAND ----------

dbutils.widgets.remove("fruitsMS")

# COMMAND ----------

