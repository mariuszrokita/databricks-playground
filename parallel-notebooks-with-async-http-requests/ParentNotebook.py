# Databricks notebook source
# MAGIC %md
# MAGIC # Run child notebook
# MAGIC 
# MAGIC The purpose of this step is to run child notebook that will perform operations using selected processing method.

# COMMAND ----------

import json

# COMMAND ----------

for mode in ["sequential", "multi-threaded", "asynchronous"]:
  result = dbutils.notebook.run(
    "ChildNotebook",
    3600, # timeout: 1 hour
    {
      "mode": mode,
      "number_of_requests": 300
    }
  )

  print(json.loads(result))
