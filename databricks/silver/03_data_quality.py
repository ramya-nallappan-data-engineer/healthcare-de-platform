# Databricks notebook source
# MAGIC %md
# MAGIC    # Data quality gate for Silver
# MAGIC    Rules are data (CONFIG-style list). Severity: error stops the pipeline, warn is logged.
# MAGIC    Every run appends to workspace.dq.dq_results.

# COMMAND ----------

spark.sql("CREATE SCHEMA IF NOT EXISTS workspace.dq")

# COMMAND ----------

from datetime import datetime
from pyspark.sql import functions as F

S = "workspace.silver"
REPORT_ONLY = False

def unique(t, c):
    return (t, f"{c}_unique", "error",
            f"SELECT COUNT(*) FROM (SELECT {c} FROM {S}.{t} WHERE {c} IS NOT NULL "
            f"GROUP BY {c} HAVING COUNT(*) > 1) AS d")

def not_null(t, c):
    return (t, f"{c}_not_null", "error",
            f"SELECT COUNT(*) FROM {S}.{t} WHERE {c} IS NULL")

def relationship(t, c, parent, pc="id"):
    return (t, f"{c}_in_{parent}", "error",
            f"SELECT COUNT(*) FROM {S}.{t} x LEFT JOIN {S}.{parent} p ON x.{c} = p.{pc} "
            f"WHERE x.{c} IS NOT NULL AND p.{pc} IS NULL")

def check(t, name, bad_condition, severity="warn"):
    return (t, name, severity, f"SELECT COUNT(*) FROM {S}.{t} WHERE {bad_condition}")

RULES = []
for t in ["patients", "encounters", "providers", "organizations", "payers"]:
    RULES += [unique(t, "id"), not_null(t, "id")]

RULES += [
    relationship("encounters", "patient", "patients"),
    relationship("encounters", "provider", "providers"),
    relationship("encounters", "organization", "organizations"),
    relationship("providers", "organization", "organizations"),
    relationship("conditions", "patient", "patients"),
    relationship("conditions", "encounter", "encounters"),
    relationship("medications", "patient", "patients"),
    relationship("medications", "encounter", "encounters"),
    relationship("procedures", "patient", "patients"),
    relationship("procedures", "encounter", "encounters"),
    relationship("observations", "patient", "patients"),
    relationship("observations", "encounter", "encounters"),
    check("encounters", "start_before_stop", "start > stop"),
    check("patients", "death_after_birth", "deathdate IS NOT NULL AND deathdate < birthdate"),
    check("encounters", "no_negative_costs", "base_encounter_cost < 0 OR total_claim_cost < 0"),
    check("medications", "no_negative_cost", "totalcost < 0"),
    check("procedures", "no_negative_cost", "base_cost < 0"),
]
print(len(RULES), "rules defined")

# COMMAND ----------

run_id = datetime.now().strftime("%Y%m%d_%H%M%S")
rows = []
for t, name, sev, sql in RULES:
    bad = spark.sql(sql).collect()[0][0]
    status = "PASS" if bad == 0 else ("FAIL" if sev == "error" else "WARN")
    rows.append((run_id, t, name, sev, bad, status))

res = (spark.createDataFrame(
           rows,
           "run_id string, table_name string, rule_name string, "
           "severity string, bad_rows long, status string")
       .withColumn("run_ts", F.current_timestamp()))

res.write.format("delta").mode("append").saveAsTable("workspace.dq.dq_results")

display(res.groupBy("status").count())

failed = [r for r in rows if r[5] == "FAIL"]
if failed and not REPORT_ONLY:
    raise ValueError(f"{len(failed)} error rules failed: {failed}")

# COMMAND ----------

# MAGIC   %sql
# MAGIC   SELECT run_id, status, COUNT(*) AS rules
# MAGIC   FROM workspace.dq.dq_results
# MAGIC   GROUP BY run_id, status
# MAGIC   ORDER BY run_id;

# COMMAND ----------

