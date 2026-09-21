# Data Center Infrastructure Monitoring Pipeline

A data engineering pipeline that ingests real server telemetry, validates it, transforms it, and
loads it into a queryable warehouse — built end-to-end and demonstrated against genuine data rather
than illustrative toy examples.

## Overview

Infrastructure telemetry (CPU utilization, network throughput, disk I/O) is only useful once it
reaches a clean, trustworthy, queryable form. In practice it arrives from many servers, in different
formats, at different volumes, with the ordinary defects of real sensor data — dropped readings,
duplicated reports, occasional corrupted values. This project builds the pipeline that handles that:

**Extract → Validate → Transform → Load → Orchestrate → Monitor → Downstream Consumption**

## Notebook breakdown

`infra_monitoring_pipeline.ipynb` walks through each stage in order:

| Section | What it does |
|---|---|
| **1. Extract** | Reads a config-driven source registry (`config/sources.yaml`) and pulls raw telemetry from all four real AWS CloudWatch sources |
| **2. Validate** | A data-quality gate checking for duplicate readings, nulls, and out-of-range values — proven against both real defects found in the data and deliberately injected synthetic bad rows |
| **3. Transform** | Gap-fills each series onto a regular 5-minute time grid, then computes rolling mean/std/z-score features for downstream monitoring use |
| **4. Load** | Loads validated data into a star-schema SQLite warehouse (`dim_asset` + `fact_metric`) |
| **5. Orchestrate** | Documents the production Airflow DAG (`orchestration/infra_monitoring_dag.py`) that would run this on a schedule, with retries and fail-loudly alerting |
| **6. Monitor** | Logs every pipeline run's health metrics to a `pipeline_runs` table — includes a real bug found during development (a dict-merge order issue that silently reported a wrong gap-fill count) as a worked example of why this logging matters |
| **7. Downstream Consumption** | A brief demonstration of the warehouse output feeding an anomaly-flagging use case |
| **8. Conclusion** | Summary, key takeaways, and an honest list of what a production version would do differently |

## What's real vs. simulated

- **Real**: all four telemetry sources, their timestamps and values, the duplicate/gap issues the
  pipeline catches, and every reported metric in the notebook (row counts, rejection counts, etc.)
- **Simulated**: the "asset registry" metadata (site/rack/instance naming) layered on top of the real
  metric values, and three deliberately injected bad rows (tagged `SYNTHETIC_INJECTED`) used to prove
  the validation gate catches null/out-of-range values it wouldn't otherwise see in this clean dataset

## Repo structure

```
.
├── infra_monitoring_pipeline.ipynb   # the full pipeline, documented stage by stage
├── config/
│   └── sources.yaml                  # source registry — edit this to add a new server, no code changes needed
├── data/raw/                         # real AWS CloudWatch CSVs
├── orchestration/
│   └── infra_monitoring_dag.py       # production Airflow DAG (documented, not run in this repo — see below)
├── warehouse/
│   └── infra_monitoring.db           # pre-generated so it can be browsed without running anything
├── requirements.txt
├── .gitignore
└── LICENSE
```

## Running it

```bash
pip install -r requirements.txt
jupyter notebook infra_monitoring_pipeline.ipynb
```

Run all cells top to bottom. The pipeline is deterministic — re-running it end to end reproduces the
same figures (16,826 real rows extracted, 11 real duplicate readings caught, 14 real gaps filled).

## Known simplifications

This project demonstrates the reasoning and stages of a real pipeline at a scale that's runnable and
inspectable in one sitting. It is not a claim that this is how infrastructure monitoring runs at
production scale. Specific gaps, stated plainly:

**Some patterns here are standard practice; some specific numbers are not.** Star-schema warehousing,
data-quality gates, fail-loudly alerting, and retry-with-backoff are genuinely established data
engineering practice. The specific parameter values attached to them in this project are not derived
from any benchmark — the 5% reject-rate alert threshold in the Airflow DAG, the 1-hour rolling window,
and the `|z| > 3` anomaly cutoff were all chosen for this demonstration. In a real deployment these
would be tuned against actual operational costs, not set once and left alone.

**Scale and tooling would differ substantially in a real environment:**

| This project | Typical production equivalent |
|---|---|
| Static CSV files | Streaming ingestion (Kafka/Kinesis) or push-based agents (Prometheus, CloudWatch agent) |
| pandas | Spark or Flink at volumes that don't fit in memory |
| SQLite | A cloud warehouse (Snowflake/BigQuery/Redshift) or a time-series DB (InfluxDB, TimescaleDB) — metrics workloads like this usually suit a TSDB better than a general warehouse |
| Hand-rolled validation checks | Great Expectations or dbt tests, often paired with a data observability tool |
| Transform-then-load (ETL) | Often reversed to ELT — load raw, transform in-warehouse with dbt/SQL |
| A DAG file in this repo | Deployed via CI/CD, monitored with alerting wired into PagerDuty/Opsgenie |
| Hardcoded-looking registry | Genuinely externalized here — `config/sources.yaml` is loaded at runtime, so onboarding a new source means editing YAML, not code |

The Airflow DAG in `orchestration/` is real, correct, deployable code — it is not executed as part of
this repo, since there's no persistent Airflow scheduler available in a notebook context.

## License

MIT — see `LICENSE`.
