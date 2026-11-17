import importlib.util
from pathlib import Path

DAG_FILE = Path(__file__).resolve().parents[1] / "airflow" / "dags" / "healthcare_pipeline.py"

EXPECTED_UPSTREAM = {
    "bronze_ingest": [],
    "silver_transform": ["bronze_ingest"],
    "dq_silver": ["silver_transform"],
    "gold_build": ["dq_silver"],
    "dq_gold": ["gold_build"],
    "snowflake_load": ["dq_gold"],
}


def load_dag():
    spec = importlib.util.spec_from_file_location("healthcare_pipeline", DAG_FILE)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module.dag


def test_dag_id():
    assert load_dag().dag_id == "healthcare_pipeline"


def test_tasks_and_order():
    dag = load_dag()
    actual = {tid: sorted(t.upstream_task_ids) for tid, t in dag.task_dict.items()}
    assert actual == EXPECTED_UPSTREAM


def test_dq_tasks_never_retry():
    dag = load_dag()
    for tid in ("dq_silver", "dq_gold"):
        assert dag.task_dict[tid].retries == 0
