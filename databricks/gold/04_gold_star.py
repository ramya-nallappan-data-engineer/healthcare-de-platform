# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE workspace.gold.dim_patient AS
# MAGIC SELECT
# MAGIC   id AS patient_id,
# MAGIC   birthdate,
# MAGIC   deathdate,
# MAGIC   is_deceased,
# MAGIC   CAST(floor(months_between(coalesce(deathdate, current_date()), birthdate) / 12) AS INT) AS age_years,
# MAGIC   gender, race, ethnicity, marital,
# MAGIC   city, state, county, zip,
# MAGIC   healthcare_expenses, healthcare_coverage, income
# MAGIC FROM workspace.silver.patients;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE workspace.gold.dim_provider AS
# MAGIC SELECT
# MAGIC   p.id AS provider_id,
# MAGIC   p.name AS provider_name,
# MAGIC   p.speciality,
# MAGIC   p.gender,
# MAGIC   p.organization AS organization_id,
# MAGIC   o.name AS organization_name,
# MAGIC   o.city, o.state, o.zip
# MAGIC FROM workspace.silver.providers p
# MAGIC LEFT JOIN workspace.silver.organizations o ON p.organization = o.id;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE workspace.gold.dim_payer AS
# MAGIC SELECT id AS payer_id, name AS payer_name, ownership
# MAGIC FROM workspace.silver.payers;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE workspace.gold.fact_encounter AS
# MAGIC SELECT
# MAGIC   id AS encounter_id,
# MAGIC   patient AS patient_id,
# MAGIC   provider AS provider_id,
# MAGIC   organization AS organization_id,
# MAGIC   payer AS payer_id,
# MAGIC   start AS start_ts,
# MAGIC   stop AS stop_ts,
# MAGIC   CAST(start AS DATE) AS encounter_date,
# MAGIC   CAST((unix_timestamp(stop) - unix_timestamp(start)) / 60 AS INT) AS duration_minutes,
# MAGIC   encounterclass AS encounter_class,
# MAGIC   code AS encounter_code,
# MAGIC   description,
# MAGIC   base_encounter_cost,
# MAGIC   total_claim_cost,
# MAGIC   payer_coverage,
# MAGIC   total_claim_cost - payer_coverage AS uncovered_cost
# MAGIC FROM workspace.silver.encounters;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE workspace.gold.mart_patient_360 AS
# MAGIC WITH enc AS (
# MAGIC   SELECT patient_id,
# MAGIC          COUNT(*) AS encounter_count,
# MAGIC          SUM(total_claim_cost) AS total_claim_cost,
# MAGIC          SUM(payer_coverage) AS total_payer_coverage,
# MAGIC          MIN(encounter_date) AS first_encounter_date,
# MAGIC          MAX(encounter_date) AS last_encounter_date
# MAGIC   FROM workspace.gold.fact_encounter
# MAGIC   GROUP BY patient_id
# MAGIC ),
# MAGIC cond AS (
# MAGIC   SELECT patient AS patient_id, COUNT(*) AS condition_count
# MAGIC   FROM workspace.silver.conditions
# MAGIC   GROUP BY patient
# MAGIC ),
# MAGIC med AS (
# MAGIC   SELECT patient AS patient_id, COUNT(*) AS medication_count, SUM(totalcost) AS medication_cost
# MAGIC   FROM workspace.silver.medications
# MAGIC   GROUP BY patient
# MAGIC )
# MAGIC SELECT p.patient_id, p.age_years, p.gender, p.is_deceased,
# MAGIC        COALESCE(e.encounter_count, 0) AS encounter_count,
# MAGIC        COALESCE(e.total_claim_cost, 0) AS total_claim_cost,
# MAGIC        COALESCE(e.total_payer_coverage, 0) AS total_payer_coverage,
# MAGIC        e.first_encounter_date, e.last_encounter_date,
# MAGIC        COALESCE(c.condition_count, 0) AS condition_count,
# MAGIC        COALESCE(m.medication_count, 0) AS medication_count,
# MAGIC        COALESCE(m.medication_cost, 0) AS medication_cost
# MAGIC FROM workspace.gold.dim_patient p
# MAGIC LEFT JOIN enc  e ON p.patient_id = e.patient_id
# MAGIC LEFT JOIN cond c ON p.patient_id = c.patient_id
# MAGIC LEFT JOIN med  m ON p.patient_id = m.patient_id;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE workspace.gold.mart_provider_utilization AS
# MAGIC SELECT d.provider_id, d.provider_name, d.speciality,
# MAGIC        d.organization_id, d.organization_name,
# MAGIC        COUNT(f.encounter_id) AS encounter_count,
# MAGIC        COUNT(DISTINCT f.patient_id) AS unique_patients,
# MAGIC        COALESCE(SUM(f.total_claim_cost), 0) AS total_claim_cost,
# MAGIC        ROUND(AVG(f.duration_minutes), 1) AS avg_duration_minutes
# MAGIC FROM workspace.gold.dim_provider d
# MAGIC LEFT JOIN workspace.gold.fact_encounter f ON d.provider_id = f.provider_id
# MAGIC GROUP BY d.provider_id, d.provider_name, d.speciality, d.organization_id, d.organization_name;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE workspace.gold.mart_payer_costs AS
# MAGIC SELECT d.payer_id, d.payer_name, d.ownership,
# MAGIC        COUNT(f.encounter_id) AS encounter_count,
# MAGIC        COALESCE(SUM(f.total_claim_cost), 0) AS total_claim_cost,
# MAGIC        COALESCE(SUM(f.payer_coverage), 0) AS total_covered,
# MAGIC        COALESCE(SUM(f.uncovered_cost), 0) AS total_uncovered,
# MAGIC        ROUND(SUM(f.payer_coverage) / NULLIF(SUM(f.total_claim_cost), 0), 4) AS coverage_rate
# MAGIC FROM workspace.gold.dim_payer d
# MAGIC LEFT JOIN workspace.gold.fact_encounter f ON d.payer_id = f.payer_id
# MAGIC GROUP BY d.payer_id, d.payer_name, d.ownership;

# COMMAND ----------

