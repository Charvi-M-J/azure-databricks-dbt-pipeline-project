# Databricks notebook source
# MAGIC %sql
# MAGIC SELECT * FROM workspace.silver.silver_flights

# COMMAND ----------

# MAGIC %md
# MAGIC # Parameters

# COMMAND ----------

#Catalog
catalog = "workspace"

# Key Cols List
key_cols = "['flight_id']"
key_cols_list = eval(key_cols)

# CDC Column
cdc_col = "modifiedDate"

# Backdated Refresh
backdated_refresh = ""

# Source Object
source_object = "silver_flights"

# Source Schema
source_schema = "silver"

#Target Schema
target_schema = "gold"

#Target Object
target_object = "flights"

#Surrogate Key
surrogate_key = "DimFlifgtsKey"

# COMMAND ----------

key_cols_list

# COMMAND ----------

# MAGIC %md
# MAGIC #  Fetching Parameters & creating variables

# COMMAND ----------

# Key Cols List
key_cols_list = eval(dbutils.widgets.get("keycols"))

# CDC Column
cdc_col = dbutils.widgets.get("cdccol")

# Backdated Refresh
backdated_refresh = dbutils.widgets.get("backdated_refresh")

# COMMAND ----------

# MAGIC %md
# MAGIC # Incremental Data Ingestion

# COMMAND ----------

if len(backdated_refresh) == 0:

    if spark.catalog.tableExists(f"workspace.{target_schema}.{target_object}"):

        last_load = spark.sql(
            f"SELECT max({cdc_col}) FROM workspace.{target_schema}.{target_object}"
        ).collect()[0][0]

    else:
        last_load = "1900-01-01 00:00:00"

else:
    last_load = backdated_refresh

# COMMAND ----------

last_load

# COMMAND ----------

df_src = spark.sql(f"SELECT * FROM {source_schema}.{source_object} WHERE {cdc_col} >= '{last_load}'")


# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# Read source table
df = spark.read.table(f"{source_schema}.{source_object}")

# Add modifiedDate column
df = df.withColumn("modifiedDate", current_timestamp())

# Display result
display(df)

# Create a new target table (recommended)
target_table = f"{source_schema}.{source_object}_new"

df.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable(target_table)

# Verify
spark.sql(f"SELECT * FROM {target_table}").display()

# COMMAND ----------

df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC # Old vs New Records

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

# Key Column String
key_cols_string_inc = ", ".join(key_cols_list)

# COMMAND ----------

', '.join(key_cols_list)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT flight_id from workspace.silver.silver_flights

# COMMAND ----------

key_cols_list = ["flight_id"]

join_condition = ' AND '.join([f"src.{i} = trg.{i}" for i in key_cols_list])

join_condition

# COMMAND ----------

key_cols_string = ", ".join(key_cols_list)

# COMMAND ----------

# DBTITLE 1,Cell 19
spark.sql(f"SELECT {key_cols_string} FROM {catalog}.{source_schema}.{source_object}")

# COMMAND ----------

spark.sql(f"SELECT '' AS flight_id,'' As DimFlightsKey, '1900-01-01 00:00:00' AS create_date, '1900-01-01 00:00:00' AS update_date FROM workspace.silver.silver_flights").display()

# COMMAND ----------

key_cols_string_init =  [f"'' AS {i}" for i in key_cols_list]
key_cols_string_init = [", ".join(key_cols_string_init)]
key_cols_string_init

# COMMAND ----------

if spark.catalog.tableExists(f"{catalog}.{target_schema}.{target_object}"):

    # Key Columns String For Incremental

    key_cols_string_inc = ", ".join(key_cols_list)

    df_trg = spark.sql(f"SELECT {key_cols_string_inc}, {surrogate_key}, create_date, update_date FROM {catalog}.{target_schema}.{target_object}")

else:

    # Key Columns String For Initial

    key_cols_string_init = [f"'' AS {i}" for i in key_cols_list]
    key_cols_string_init = ", ".join(key_cols_string_init)

    df_trg = spark.sql(f"SELECT {key_cols_string_init}, '' AS {surrogate_key}, '' AS create_date, '' AS update_date WHERE 1=0")

# COMMAND ----------

df_trg.display()

# COMMAND ----------

[f"src.{i} = trg.{i}" for i in key_cols_list]

# COMMAND ----------

key_cols_list


# COMMAND ----------

join_condition = ' AND '.join([f"src.{i} = trg.{i}" for i in key_cols_list])

# COMMAND ----------

df_src.createOrReplaceTempView("src")
df_trg.createOrReplaceTempView("trg")

df_join = spark.sql(f"""
    SELECT
        src.*,
        trg.{surrogate_key},
        trg.create_date,
        trg.update_date
    FROM src
    LEFT JOIN trg
    ON {join_condition}
""")

# COMMAND ----------

df_join.display()

# COMMAND ----------

df_old = df_join.filter(col(f'{surrogate_key}').isNotNull())
df_new = df_join.filter(col(f'{surrogate_key}').isNull())


# COMMAND ----------

df_old_enr = df_old.withColumn('update_date', current_timestamp())

# COMMAND ----------

df_new.display()

# COMMAND ----------

if spark.catalog.tableExists(f"{catalog}.{target_schema}.{target_object}"):

    max_surrogate_key = spark.sql(f"""
        SELECT max({surrogate_key})
        FROM {catalog}.{target_schema}.{target_object}
    """).collect()[0][0]

    df_new_enr = df_new.withColumn(
        f'{surrogate_key}',
        lit(max_surrogate_key) + lit(1) + monotonically_increasing_id()
    ) \
    .withColumn('create_date', current_timestamp()) \
    .withColumn('update_date', current_timestamp())

else:

    max_surrogate_key = 0

    df_new_enr = df_new.withColumn(
        f'{surrogate_key}',
        lit(max_surrogate_key) + lit(1) + monotonically_increasing_id()
    ) \
    .withColumn('create_date', current_timestamp()) \
    .withColumn('update_date', current_timestamp())

# COMMAND ----------

df_new_enr.display()

# COMMAND ----------

