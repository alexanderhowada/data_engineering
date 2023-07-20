# Databricks notebook source
dbutils.widgets.help()

# COMMAND ----------

class Widgets:

    @classmethod
    def get_or_mock(dbutils, key, mock=None):
        w = 


