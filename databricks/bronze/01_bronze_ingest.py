# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze ingest: Synthea CSV → Delta
# MAGIC Loads every CSV in /Volumes/workspace/bronze/raw_synthea/ into workspace.bronze.<name>.
# MAGIC All columns stay strings; two audit columns are added. Re-runnable (overwrite).

# COMMAND ----------

display(dbutils.fs.ls("/Volumes/workspace/bronze/raw_synthea/"))

# COMMAND ----------

path = "/Volumes/workspace/bronze/raw_synthea/patients.csv"
df = spark.read.option("header", "true").csv(path)
df.printSchema()
print(df.count())
display(df.limit(5))

# COMMAND ----------

(df.write
   .format("delta")
   .mode("overwrite")
   .saveAsTable("workspace.bronze.patients"))

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) AS died FROM workspace.bronze.patients WHERE DEATHDATE IS NOT NULL;

# COMMAND ----------

from pyspark.sql import functions as F

src = "/Volumes/workspace/bronze/raw_synthea/"
files = [f.name for f in dbutils.fs.ls(src) if f.name.endswith(".csv")]

for name in files:
    table = name.replace(".csv", "")
    df = (spark.read.option("header", "true").csv(src + name)
            .withColumn("_source_file", F.col("_metadata.file_path"))
            .withColumn("_ingested_at", F.current_timestamp()))
    (df.write.format("delta").mode("overwrite")
       .option("overwriteSchema", "true")
       .saveAsTable(f"workspace.bronze.{table}"))
    print(table, spark.table(f"workspace.bronze.{table}").count())

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT Id, _source_file, _ingested_at FROM workspace.bronze.patients LIMIT 3;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY workspace.bronze.patients;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) AS n FROM workspace.bronze.patients VERSION AS OF 0;

# COMMAND ----------

