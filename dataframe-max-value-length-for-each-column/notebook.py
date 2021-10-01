# COMMAND ----------

from pyspark.sql.functions import col, length
import pandas as pd


# COMMAND ----------

stats = []
for column_name in data_df.columns:
  # Calculate max value length for each column
  df_temp = data_df.select(column_name).distinct()
  df_temp = df_temp.withColumn("value_length", length(column_name))
  max_length = df_temp.select(col("value_length")).groupby().max().collect()[0].asDict()['max(value_length)']
  # Append column stats
  stats.append({"column_name": column_name, "max_value_length": max_length})

stats_pdf = pd.DataFrame.from_dict(stats)

pd.set_option('display.max_rows', None)  # display all rows
stats_pdf
