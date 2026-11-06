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

dbutils.widgets.dropdown("stage", "all", ["silver", "gold", "all"], "Rule set to run")
STAGE = dbutils.widgets.get("stage")

def unique(t, c, s=S):
    return (t, f"{c}_unique", "error",
            f"SELECT COUNT(*) FROM (SELECT {c} FROM {s}.{t} WHERE {c} IS NOT NULL "
            f"GROUP BY {c} HAVING COUNT(*) > 1) AS d")

def not_null(t, c, severity="error", s=S):
    return (t, f"{c}_not_null", severity,
            f"SELECT COUNT(*) FROM {s}.{t} WHERE {c} IS NULL")

def relationship(t, c, parent, pc="id", s=S):
    return (t, f"{c}_in_{parent}", "error",
            f"SELECT COUNT(*) FROM {s}.{t} x LEFT JOIN {s}.{parent} p ON x.{c} = p.{pc} "
            f"WHERE x.{c} IS NOT NULL AND p.{pc} IS NULL")

def check(t, name, bad_condition, severity="warn", s=S):
    return (t, name, severity, f"SELECT COUNT(*) FROM {s}.{t} WHERE {bad_condition}")

def reconcile(name, sql_a, sql_b):
    return ("gold", name, "error",
            f"SELECT CASE WHEN ({sql_a}) = ({sql_b}) THEN 0 ELSE 1 END")

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

RULES += [not_null(t, "patient")
          for t in ["encounters", "conditions", "medications", "procedures", "observations"]]
          
print(len(RULES), "rules defined")

G = "workspace.gold"

GOLD_RULES = []
for t, k in [("dim_patient", "patient_id"), ("dim_provider", "provider_id"),
             ("dim_payer", "payer_id"), ("fact_encounter", "encounter_id"),
             ("mart_patient_360", "patient_id"),
             ("mart_provider_utilization", "provider_id"),
             ("mart_payer_costs", "payer_id")]:
    GOLD_RULES += [unique(t, k, G), not_null(t, k, "error", G)]

GOLD_RULES += [
    relationship("fact_encounter", "patient_id", "dim_patient", "patient_id", G),
    relationship("fact_encounter", "provider_id", "dim_provider", "provider_id", G),
    relationship("fact_encounter", "payer_id", "dim_payer", "payer_id", G),
    reconcile("fact_rows_equal_silver_encounters",
              f"SELECT COUNT(*) FROM {G}.fact_encounter",
              f"SELECT COUNT(*) FROM {S}.encounters"),
    reconcile("claim_cost_fact_equals_payer_mart",
              f"SELECT SUM(total_claim_cost) FROM {G}.fact_encounter",
              f"SELECT SUM(total_claim_cost) FROM {G}.mart_payer_costs"),
    reconcile("claim_cost_fact_equals_provider_mart",
              f"SELECT SUM(total_claim_cost) FROM {G}.fact_encounter",
              f"SELECT SUM(total_claim_cost) FROM {G}.mart_provider_utilization"),
    reconcile("claim_cost_fact_equals_patient_mart",
              f"SELECT SUM(total_claim_cost) FROM {G}.fact_encounter",
              f"SELECT SUM(total_claim_cost) FROM {G}.mart_patient_360"),
]
print(len(RULES), "silver rules,", len(GOLD_RULES), "gold rules")

# COMMAND ----------

run_id = datetime.now().strftime("%Y%m%d_%H%M%S")
rows = []

RULES_TO_RUN = {"silver": RULES, "gold": GOLD_RULES, "all": RULES + GOLD_RULES}[STAGE]

for t, name, sev, sql in RULES_TO_RUN:
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

# MAGIC %sql
# MAGIC SELECT run_id, status, COUNT(*) AS rules
# MAGIC FROM workspace.dq.dq_results
# MAGIC GROUP BY run_id, status
# MAGIC ORDER BY run_id DESC
# MAGIC LIMIT 5

# COMMAND ----------

