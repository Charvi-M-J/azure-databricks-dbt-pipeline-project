# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE VOLUME workspace.raw.rawvolume

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA workspace.bronze

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA workspace.silver

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA workspace.gold

# COMMAND ----------

dbutils.fs.mkdirs("/Volumes/workspace/raw/rawvolume/rawdata/airports")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM delta.`/Volumes/workspace/bronze/bronzevolume/flights/data`

# COMMAND ----------

