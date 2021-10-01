# COMMAND ----------

secret_value = dbutils.secrets.get(scope="scope_name", key="key_name")
print('\u200B'.join(secret_value))