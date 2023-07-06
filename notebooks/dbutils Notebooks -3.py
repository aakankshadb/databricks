# Databricks notebook source
dbutils.widgets.combobox(name="Paracombobox",defaultValue="apple",choices=["apple","banana","orange"],label="Fruits ComboBox")
dbutils.widgets.multiselect(name="Paramultiselect",defaultValue="apple",choices=["apple","banana","orange"],label="Fruits MultiSelect")
dbutils.widgets.dropdown(name="Paradropdown",defaultValue="apple",choices=["apple","banana","orange"],label="Fruits DropDown")
dbutils.widgets.text(name="Paratext",defaultValue="apple",label="Fruits TextBox")

# COMMAND ----------

print("combobox value is " + dbutils.widgets.get("Paracombobox"))
print("multi select value is " + dbutils.widgets.get("Paramultiselect"))
print("dropdown value is " + dbutils.widgets.get("Paradropdown"))
print("text box value is " + dbutils.widgets.get("Paratext"))

# COMMAND ----------

