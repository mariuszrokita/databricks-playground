# Databricks notebook source
import json
import time
import timeit

# COMMAND ----------

dbutils.widgets.dropdown("mode", "asynchronous", choices=["sequential", "multi-threaded", "asynchronous"])

selected_mode = dbutils.widgets.get("mode")
print("selected mode:", selected_mode)

# COMMAND ----------

def sequential():
  print("sequential()")
  time.sleep(1)


def multi_threaded():
  print("multi-threaded()")
  time.sleep(1)


def asynchronous():
  print("asynchronous()")
  time.sleep(1)


# COMMAND ----------

switch = {
  'sequential': sequential,
  'multi-threaded': multi_threaded,
  'asynchronous': asynchronous
}

func = switch[selected_mode]

no_repeats = 3
# timeit() disables the garbage collection that could skew the results.
# More info on:  https://www.oreilly.com/library/view/python-cookbook/0596001673/ch17.html
duration = timeit.timeit(func, number=no_repeats) / no_repeats

# COMMAND ----------

dbutils.notebook.exit(json.dumps({
  "status": "OK",
  "mode": selected_mode,
  "duration": duration
}))
