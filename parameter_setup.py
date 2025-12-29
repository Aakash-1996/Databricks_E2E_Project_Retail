# Databricks notebook source
parameter = [
    {"file_name":"Orders"},
    {"file_name":"Customers"},
    {"file_name":"Products"},
    {"file_name":"Region"}
]

# COMMAND ----------

dbutils.jobs.taskValues.set("File_param", parameter)