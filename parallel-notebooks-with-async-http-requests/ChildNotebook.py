# Databricks notebook source
dbutils.library.installPyPI("aiohttp")

# COMMAND ----------

import aiohttp
import asyncio
import json
import requests
import time
import timeit

import concurrent.futures
from functools import reduce

# COMMAND ----------

dbutils.widgets.dropdown("mode", "asynchronous", choices=["sequential", "multi-threaded", "asynchronous"])
dbutils.widgets.text("number_of_requests", "100")

selected_mode = dbutils.widgets.get("mode")
number_of_requests = int(dbutils.widgets.get("number_of_requests"))

print("selected mode:", selected_mode)
print("number of requests", number_of_requests)

# COMMAND ----------

def get_pokemon_weight(url):
  resp = requests.get(url)
  if resp.status_code == 200:
    pokemon = resp.json()
    return pokemon['weight']
  else:
    # TODO: shouldn't we throw error?
    return 0


async def get_pokemon_weight_async(session, url):
  async with session.get(url) as resp:
    if resp.status == 200:
      pokemon = await resp.json()
      return pokemon['weight']
    else:
      # TODO: shouldn't we throw error?
      return 0


def sum_array(arr):
  # Functional programming approach. More info: https://realpython.com/python-reduce-function/
  total = reduce(lambda x, y: x+y, arr)  # sum(arr) would wefine as well
  return total


def sequential(urls):
  print("sequential()")
  weights = []
  for url in urls:
    weight = get_pokemon_weight(url)
    print(weight)
    weights.append(weight)
  print("Weight total:", sum_array(weights))


def multi_threaded(urls):
  print("multi-threaded()")
  weights = []
  with concurrent.futures.ThreadPoolExecutor() as executor:
    futures = [executor.submit(get_pokemon_weight, url=url) for url in urls]
    for future in concurrent.futures.as_completed(futures):
      weight = future.result()
      print(weight)
      weights.append(weight)
  print("Weight total:", sum_array(weights))


async def asynchronous(urls):
  print("asynchronous()")
  weights = []
  async with aiohttp.ClientSession() as session:
    tasks = [asyncio.ensure_future(get_pokemon_weight_async(session, url)) for url in urls]
    results = await asyncio.gather(*tasks)
    for r in results:
      print(r)
      weights.append(r)

  print("Weight total:", sum_array(weights))


# COMMAND ----------

# Generate list of URL to make requests to.
urls = ["https://pokeapi.co/api/v2/pokemon/" + str(i) for i in range(number_of_requests)]

# Choose function to execute based on chosen mode
switch = {
  'sequential': sequential,
  'multi-threaded': multi_threaded,
  'asynchronous': asynchronous
}
func = switch[selected_mode]

# timeit() disables the garbage collection that could skew the results.
# More info on:  https://www.oreilly.com/library/view/python-cookbook/0596001673/ch17.html
if selected_mode == 'asynchronous':
  func_to_gauge = lambda: asyncio.run(func(urls))
else:
  func_to_gauge = lambda: func(urls)

no_repeats = 1
duration = timeit.timeit(func_to_gauge, number=no_repeats) / no_repeats


# COMMAND ----------

dbutils.notebook.exit(json.dumps({
  "status": "OK",
  "mode": selected_mode,
  "duration": duration
}))
