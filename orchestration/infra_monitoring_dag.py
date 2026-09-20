"""
Airflow DAG: Data Center Infrastructure Monitoring Pipeline
-------------------------------------------------------------
Production orchestration for the ETL pipeline developed and validated in
infra_monitoring_pipeline.ipynb. This DAG is provided as deployable code to
demonstrate orchestration design; it is not executed inside the notebook
environment (Colab/Jupyter cannot host a persistent Airflow scheduler).

Schedule: every 5 minutes, matching the native resolution of the source
CloudWatch metrics -- i.e. the pipeline keeps pace with how fast the data
actually arrives, rather than an arbitrary batch window.
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.exceptions import AirflowFailException

from pipeline.etl import extract, validate, transform, load  # see pipeline/etl.py

DEFAULT_ARGS = {
    "owner": "data-eng",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
    "email_on_failure": True,
}

# Alert threshold: if more than 5% of extracted rows fail validation,
# fail the run loudly rather than silently loading a degraded dataset.
MAX_REJECT_RATE = 0.05


def _extract_and_validate(**context):
    raw = extract()
    clean, issues = validate(raw)
    reject_rate = (len(raw) - len(clean)) / max(len(raw), 1)
    if reject_rate > MAX_REJECT_RATE:
        raise AirflowFailException(
            f"Data quality gate failed: {reject_rate:.1%} of rows rejected "
            f"(threshold {MAX_REJECT_RATE:.0%}). Issues: {issues}"
        )
    context["ti"].xcom_push(key="clean_df_path", value=_stash(clean))
    context["ti"].xcom_push(key="issues", value=issues)


def _transform_and_load(**context):
    clean = _unstash(context["ti"].xcom_pull(key="clean_df_path"))
    transformed, gaps_filled = transform(clean)
    rows_loaded = load(transformed)
    context["ti"].xcom_push(key="rows_loaded", value=rows_loaded)
    context["ti"].xcom_push(key="gaps_filled", value=gaps_filled)


def _stash(df):
    # In production this writes to a shared staging location (S3/GCS/parquet on
    # a mounted volume) since XCom is not meant to carry full dataframes.
    path = f"/tmp/staging_{datetime.utcnow().timestamp()}.parquet"
    df.to_parquet(path)
    return path


def _unstash(path):
    import pandas as pd
    return pd.read_parquet(path)


with DAG(
    dag_id="infra_monitoring_pipeline",
    default_args=DEFAULT_ARGS,
    description="Extract-validate-transform-load for data center infra telemetry",
    schedule_interval="*/5 * * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["data-engineering", "infra-monitoring"],
) as dag:

    extract_validate = PythonOperator(
        task_id="extract_and_validate",
        python_callable=_extract_and_validate,
    )

    transform_load = PythonOperator(
        task_id="transform_and_load",
        python_callable=_transform_and_load,
    )

    extract_validate >> transform_load
