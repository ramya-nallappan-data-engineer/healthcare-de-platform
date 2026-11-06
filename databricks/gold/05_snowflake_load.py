# Databricks notebook source
# MAGIC %pip install -q "snowflake-connector-python[pandas]==5.2.0"

# COMMAND ----------

# MAGIC %restart_python

# COMMAND ----------

import warnings
warnings.filterwarnings("ignore", category=FutureWarning)

import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
from cryptography.hazmat.primitives import serialization

GOLD_TABLES = ["dim_patient", "dim_provider", "dim_payer", "fact_encounter",
               "mart_patient_360", "mart_provider_utilization", "mart_payer_costs"]

# COMMAND ----------

def get_conn():
    pem = dbutils.secrets.get("snowflake_loader", "private_key")
    pp  = dbutils.secrets.get("snowflake_loader", "private_key_passphrase")
    key = serialization.load_pem_private_key(pem.encode(), password=pp.encode())
    key_bytes = key.private_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    )
    return snowflake.connector.connect(
        account=dbutils.secrets.get("snowflake_loader", "account"),
        user="HC_LOADER_SVC", private_key=key_bytes,
        role="HC_LOADER", warehouse="HC_WH",
        database="HEALTHCARE_DB", schema="GOLD",
    )

# COMMAND ----------

def sf_type(spark_type: str) -> str:
    t = spark_type.lower()
    if t.startswith("decimal"):
        return t.replace("decimal", "NUMBER")
    return {
        "string": "VARCHAR",
        "int": "NUMBER(38,0)",
        "bigint": "NUMBER(38,0)",
        "double": "FLOAT",
        "date": "DATE",
        "timestamp": "TIMESTAMP_NTZ",
        "timestamp_ntz": "TIMESTAMP_NTZ",
        "boolean": "BOOLEAN",
    }[t]

def ddl_for(name: str) -> str:
    fields = spark.table(f"workspace.gold.{name}").schema.fields
    cols = [f"    {f.name.upper()} {sf_type(f.dataType.simpleString())}" for f in fields]
    return (f"CREATE OR REPLACE TABLE HEALTHCARE_DB.GOLD.{name.upper()} (\n"
            + ",\n".join(cols) + "\n);")

for t in GOLD_TABLES:
    print(ddl_for(t))
    print()

# COMMAND ----------

conn = get_conn()
try:
    for t in GOLD_TABLES:
        conn.cursor().execute(ddl_for(t))
        print(f"{t}: table created")
finally:
    conn.close()

# COMMAND ----------

def load_table(conn, name):
    sdf = spark.table(f"workspace.gold.{name}")
    pdf = sdf.toPandas()
    pdf.attrs = {}
    pdf.columns = [c.upper() for c in pdf.columns]

    ok, _, _, _ = write_pandas(conn, pdf, name.upper(),
                               auto_create_table=False, overwrite=True,
                               quote_identifiers=False,
                               use_logical_type=True)

    sf_rows = conn.cursor().execute(f"SELECT COUNT(*) FROM {name.upper()}").fetchone()[0]
    expected = sdf.count()
    if not ok or sf_rows != expected:
        raise ValueError(f"{name}: expected {expected} rows, Snowflake has {sf_rows}")
    print(f"{name}: {sf_rows} rows loaded OK")

# COMMAND ----------

conn = get_conn()
try:
    for t in GOLD_TABLES:
        load_table(conn, t)
finally:
    conn.close()