# Databricks notebook source
# MAGIC %md
# MAGIC    # Silver build: Bronze → typed, validated Delta
# MAGIC    Config-driven: one entry per table in CONFIG, one function build_silver().
# MAGIC    Guards: unreadable values and row-count changes stop the build.

# COMMAND ----------

from pyspark.sql import functions as F

b = spark.table("workspace.bronze.patients")

silver = (b.drop("SSN", "DRIVERS", "PASSPORT")
            .withColumn("BIRTHDATE", F.col("BIRTHDATE").cast("date"))
            .withColumn("DEATHDATE", F.col("DEATHDATE").cast("date"))
            .withColumn("is_deceased", F.col("DEATHDATE").isNotNull())
            .withColumn("LAT", F.col("LAT").cast("double"))
            .withColumn("LON", F.col("LON").cast("double"))
            .withColumn("HEALTHCARE_EXPENSES", F.col("HEALTHCARE_EXPENSES").cast("decimal(18,2)"))
            .withColumn("HEALTHCARE_COVERAGE", F.col("HEALTHCARE_COVERAGE").cast("decimal(18,2)"))
            .withColumn("INCOME", F.col("INCOME").cast("decimal(18,2)")))

silver = silver.toDF(*[c.lower() for c in silver.columns])

(silver.write.format("delta").mode("overwrite")
       .option("overwriteSchema", "true")
       .saveAsTable("workspace.silver.patients"))

# COMMAND ----------

tables = ["encounters", "conditions", "medications", "procedures",
          "observations", "organizations", "providers", "payers"]

for t in tables:
    first = spark.table(f"workspace.bronze.{t}").limit(1).collect()[0].asDict()
    print(t)
    print(first)
    print()

# COMMAND ----------

from pyspark.sql import functions as F

TYPE_MAP = {"timestamps": "timestamp", "dates": "date", "doubles": "double",
            "decimals": "decimal(18,2)", "ints": "int"}

CONFIG = {
  "patients": {
    "drop": ["SSN", "DRIVERS", "PASSPORT"],
    "dates": ["BIRTHDATE", "DEATHDATE"],
    "doubles": ["LAT", "LON"],
    "decimals": ["HEALTHCARE_EXPENSES", "HEALTHCARE_COVERAGE", "INCOME"],
    "derived": {"is_deceased": "DEATHDATE IS NOT NULL"},
  },
  "encounters": {
    "timestamps": ["START", "STOP"],
    "decimals": ["BASE_ENCOUNTER_COST", "TOTAL_CLAIM_COST", "PAYER_COVERAGE"],
  },
  "conditions": {"dates": ["START", "STOP"]},
  "medications": {
    "timestamps": ["START", "STOP"],
    "decimals": ["BASE_COST", "PAYER_COVERAGE", "TOTALCOST"],
    "ints": ["DISPENSES"],
  },
  "procedures": {"timestamps": ["START", "STOP"], "decimals": ["BASE_COST"]},
  "observations": {"timestamps": ["DATE"]},
  "organizations": {"doubles": ["LAT", "LON"], "decimals": ["REVENUE"], "ints": ["UTILIZATION"]},
  "providers": {"doubles": ["LAT", "LON"], "ints": ["ENCOUNTERS", "PROCEDURES"]},
  "payers": {
    "decimals": ["AMOUNT_COVERED", "AMOUNT_UNCOVERED", "REVENUE"],
    "doubles": ["QOLS_AVG"],
    "ints": ["COVERED_ENCOUNTERS", "UNCOVERED_ENCOUNTERS", "COVERED_MEDICATIONS",
             "UNCOVERED_MEDICATIONS", "COVERED_PROCEDURES", "UNCOVERED_PROCEDURES",
             "COVERED_IMMUNIZATIONS", "UNCOVERED_IMMUNIZATIONS", "UNIQUE_CUSTOMERS",
             "MEMBER_MONTHS"],
  },
}

# COMMAND ----------

def build_silver(table, cfg):
    df = spark.table(f"workspace.bronze.{table}").drop(*cfg.get("drop", []))
    casts = {c: t for kind, t in TYPE_MAP.items() for c in cfg.get(kind, [])}

    # guard: any value that exists but cannot be converted?
    checks = [F.sum(F.when(F.col(c).isNotNull() &
                           F.expr(f"try_cast(`{c}` AS {t})").isNull(), 1).otherwise(0)).alias(c)
              for c, t in casts.items()]
    bad = {k: v for k, v in df.agg(*checks).collect()[0].asDict().items() if v}
    if bad:
        raise ValueError(f"{table}: unreadable values {bad}")

    for c, t in casts.items():
        df = df.withColumn(c, F.expr(f"try_cast(`{c}` AS {t})"))
    for name, sql in cfg.get("derived", {}).items():
        df = df.withColumn(name, F.expr(sql))
    df = df.toDF(*[c.lower() for c in df.columns])

    (df.write.format("delta").mode("overwrite")
       .option("overwriteSchema", "true")
       .saveAsTable(f"workspace.silver.{table}"))

    b = spark.table(f"workspace.bronze.{table}").count()
    s = spark.table(f"workspace.silver.{table}").count()
    if b != s:
        raise ValueError(f"{table}: row count changed {b} -> {s}")
    print(f"{table}: {s} rows OK")

for table, cfg in CONFIG.items():
    build_silver(table, cfg)

# COMMAND ----------

