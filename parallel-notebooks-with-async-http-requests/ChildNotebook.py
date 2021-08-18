# Databricks notebook source
import json

# COMMAND ----------

dbutils.widgets.dropdown("mode", "asynchronous", choices=["sequential", "multi-threaded", "asynchronous"])

selected_mode = dbutils.widgets.get("mode")
print("selected mode:", selected_mode)

# COMMAND ----------

def sequential():
  print("sequential()")
  return "00:00:20.4"


def multi_threaded():
  print("multi-threaded()")
  return "00:00:04.7"


def asynchronous():
  print("asynchronous()")
  return "00:00:00.8"

# COMMAND ----------

switch = {
  'sequential': sequential,
  'multi-threaded': multi_threaded,
  'asynchronous': asynchronous
}

func = switch[selected_mode]
duration = func()

# COMMAND ----------

dbutils.notebook.exit(json.dumps({
  "status": "OK",
  "mode": selected_mode,
  "duration": duration
}))
