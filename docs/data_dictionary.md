# Data dictionary: Gold layer

All tables live in Databricks (`workspace.gold`) and are loaded into Snowflake as `HEALTHCARE_DB.GOLD`. The data is synthetic (Synthea).
Types below are the Snowflake types.

## dim_patient (one row per patient)

| Column | Type | Description |
|---|---|---|
| patient_id | VARCHAR | Unique patient identifier |
| birthdate | DATE | Date of birth |
| deathdate | DATE | Date of death, empty for living patients |
| is_deceased | BOOLEAN | True when a death date exists |
| age_years | NUMBER(38,0) | Age in whole years: age at death for deceased patients, otherwise age on the day the Gold layer was built |
| gender | VARCHAR | Gender |
| race | VARCHAR | Race |
| ethnicity | VARCHAR | Ethnicity |
| marital | VARCHAR | Marital status |
| city, state, county, zip | VARCHAR | Patient address location (no street address is kept) |
| healthcare_expenses | NUMBER(18,2) | Lifetime healthcare expenses of the patient |
| healthcare_coverage | NUMBER(18,2) | Lifetime healthcare coverage of the patient |
| income | NUMBER(18,2) | Annual income of the patient |

## dim_provider (one row per provider)

| Column | Type | Description |
|---|---|---|
| provider_id | VARCHAR | Unique provider identifier |
| provider_name | VARCHAR | Provider name |
| speciality | VARCHAR | Provider speciality |
| gender | VARCHAR | Provider gender |
| organization_id | VARCHAR | Organization the provider works for |
| organization_name | VARCHAR | Organization name |
| city, state, zip | VARCHAR | Organization location |

## dim_payer (one row per payer)

| Column | Type | Description |
|---|---|---|
| payer_id | VARCHAR | Unique payer identifier |
| payer_name | VARCHAR | Payer (insurer) name |
| ownership | VARCHAR | Ownership type of the payer |

## fact_encounter (one row per encounter)

| Column | Type | Description |
|---|---|---|
| encounter_id | VARCHAR | Unique encounter identifier |
| patient_id | VARCHAR | Link to dim_patient |
| provider_id | VARCHAR | Link to dim_provider |
| organization_id | VARCHAR | Organization where the encounter took place |
| payer_id | VARCHAR | Link to dim_payer |
| start_ts | TIMESTAMP_NTZ | Encounter start time |
| stop_ts | TIMESTAMP_NTZ | Encounter end time |
| encounter_date | DATE | Calendar date of the encounter start time |
| duration_minutes | NUMBER(38,0) | Whole minutes between start and stop (fractions are cut off) |
| encounter_class | VARCHAR | Class of encounter (for example ambulatory or inpatient) |
| encounter_code | VARCHAR | Code for the encounter type |
| description | VARCHAR | Text description of the encounter |
| base_encounter_cost | NUMBER(18,2) | Base cost of the encounter, without line items (Synthea definition) |
| total_claim_cost | NUMBER(18,2) | Total cost of the encounter, including all line items (Synthea definition) |
| payer_coverage | NUMBER(18,2) | Amount of the cost covered by the payer (Synthea definition) |
| uncovered_cost | NUMBER(19,2) | `total_claim_cost - payer_coverage`: the part of the cost the payer did not cover |

## mart_patient_360 (one row per patient)

| Column | Type | Description |
|---|---|---|
| patient_id | VARCHAR | Link to dim_patient |
| age_years | NUMBER(38,0) | Age in whole years: age at death for deceased patients, otherwise age on the day the Gold layer was built |
| gender | VARCHAR | Gender |
| is_deceased | BOOLEAN | True when the patient has died |
| encounter_count | NUMBER(38,0) | Number of encounters |
| total_claim_cost | NUMBER(28,2) | Sum of claim cost over all encounters |
| total_payer_coverage | NUMBER(28,2) | Sum of payer coverage over all encounters |
| first_encounter_date | DATE | Date of the first encounter |
| last_encounter_date | DATE | Date of the latest encounter |
| condition_count | NUMBER(38,0) | Number of recorded conditions |
| medication_count | NUMBER(38,0) | Number of recorded medications |
| medication_cost | NUMBER(28,2) | Total cost of medications |

## mart_provider_utilization (one row per provider)

| Column | Type | Description |
|---|---|---|
| provider_id | VARCHAR | Link to dim_provider |
| provider_name | VARCHAR | Provider name |
| speciality | VARCHAR | Provider speciality |
| organization_id | VARCHAR | Organization identifier |
| organization_name | VARCHAR | Organization name |
| encounter_count | NUMBER(38,0) | Number of encounters handled |
| unique_patients | NUMBER(38,0) | Number of distinct patients seen |
| total_claim_cost | NUMBER(28,2) | Sum of claim cost for the provider's encounters |
| avg_duration_minutes | FLOAT | Average encounter duration in minutes, rounded to 1 decimal |

## mart_payer_costs (one row per payer)

| Column | Type | Description |
|---|---|---|
| payer_id | VARCHAR | Link to dim_payer |
| payer_name | VARCHAR | Payer name |
| ownership | VARCHAR | Ownership type of the payer |
| encounter_count | NUMBER(38,0) | Number of encounters billed to the payer |
| total_claim_cost | NUMBER(28,2) | Sum of claim cost (0 when there are no claims) |
| total_covered | NUMBER(28,2) | Sum of payer coverage |
| total_uncovered | NUMBER(29,2) | Sum of uncovered cost |
| coverage_rate | NUMBER(33,4) | Total covered divided by total claim cost, rounded to 4 decimals; empty when claim cost is 0 |

## Notes

- Money columns are exact decimals, never floating point (except the average duration, which is an average and not an amount).
- Spark widens the precision of sums and divisions, which is why the marts use wider decimal types (for example `NUMBER(28,2)`).
- Identifier columns are text, not numbers, so leading zeros and long ids are never lost.
- `age_years` is calculated with the current date at build time, so it changes if the Gold layer is rebuilt on a later day.
