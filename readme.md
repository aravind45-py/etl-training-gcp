# Enterprise Full-Stack GCP Data Engineering Pipeline (`ETL_PROJECT_GCP`)

![GCP](https://img.shields.io/badge/GCP-Cloud_Composer_%7C_BigQuery_%7C_Dataform_%7C_Cloud_Build-4285F4?style=flat-square&logo=googlecloud)
![Airflow](https://img.shields.io/badge/Apache_Airflow-2.x-017CEE?style=flat-square&logo=apacheairflow)
![Dataform](https://img.shields.io/badge/Dataform-Core-2B6CB0?style=flat-square)
![Build](https://img.shields.io/badge/CI%2FCD-Cloud_Build-34A853?style=flat-square&logo=googlecloud)

An end-to-end, enterprise-grade data pipeline built on Google Cloud Platform (GCP). The pipeline ingests weather forecast data from the Open-Meteo API, stages raw JSON landing files in Google Cloud Storage (GCS), loads them into BigQuery (Bronze), and executes Dataform transformations to power Silver and Gold analytics layers.

---

## 📐 Architecture Overview

```
[ Open-Meteo API ]
        │
        ▼
[ GCS Landing Bucket ]  --->  gs://<bucket>/landing/*.json
        │
        ▼
[ GCS Raw Bucket ]      --->  gs://<bucket>/raw/*.ndjson
        │
        ▼  (GCSToBigQueryOperator)
[ BigQuery Bronze Layer ] --->  <dataset>.bronze_weather_raw
        │
        ▼  (Dataform Workflow)
[ BigQuery Silver Layer ] --->  <dataset>.silver_weather_cleansed
        │
        ▼  (Dataform Workflow)
[ BigQuery Gold Layer ]   --->  <dataset>.gold_weather_analytics
```

---

## 🗂 Project Topology & Infrastructure

The pipeline follows strict environment isolation between **Development (DEV)** and **Production (PROD)**.

| Component | DEV Environment (`develop` branch) | PROD Environment (`main` branch) |
| :--- | :--- | :--- |
| **GCP Project ID** | `sample-training-001` | `sample-training-001` |
| **Cloud Composer Env** | `etl-training-cc-dev` | `etl-training-cc-prod` |
| **GCS Storage Bucket** | `gs://etl-training-bk-dev` | `gs://etl-training-bk-prod` |
| **BigQuery Dataset** | `etl-training-bq-dev` | `etl-training-bq` |
| **Dataform Target** | `dev` workspace / targets | `prod` workspace / targets |

---

## 🗄️ Medallion Data Architecture

1. **Landing & Raw (GCS)**
   - Extracts weather forecast data from the **Open-Meteo API**.
   - Raw responses are landed in `/landing/`, preprocessed into Newline-Delimited JSON (NDJSON), and saved in `/raw/`.
2. **Bronze (BigQuery)**
   - Managed via Airflow `GCSToBigQueryOperator`.
   - Ingests raw NDJSON files into `bronze_weather_raw` with schema auto-detection or explicit schema enforcement.
3. **Silver & Gold (Dataform)**
   - **Silver**: Cleanses, deduplicates, enforces type casting, and standardizes timestamp attributes.
   - **Gold**: Aggregates metrics (e.g., daily min/max/average temperatures, precipitation trends) optimized for BI reporting and downstream consumption.

---

## ⏱️ Airflow Scheduling & Time Zone Management

> ⚠️ **Important Note on Airflow Schedules:**
> Airflow evaluates cron schedules in **UTC**, while the UI converts timestamps to local display time (e.g., IST / UTC+5:30).

- **Target Execution Time:** `10:00 AM IST`
- **Corresponding UTC Time:** `04:30 AM UTC`
- **Cron Expression:** `30 4 * * *`


## 🚀 CI/CD Pipeline & Deployment

Automated dynamic routing and deployment are managed via **Cloud Build** triggered on repository branch events:

- **Develop Branch (`develop`)**:
  - Automatically targets `etl-training-bk-dev` and `etl-training-bq-dev`.
  - Syncs Airflow DAGs to `etl-training-cc-dev` GCS DAGs folder.
  - Compiles Dataform assertions and dev tags.
- **Main Branch (`main`)**:
  - Targets production bucket `etl-training-bk-prod` and `etl-training-bq`.
  - Syncs Airflow DAGs to `etl-training-cc-prod` GCS DAGs folder.
  - Deploys production Dataform compilation targets.

---

## 🛡️ Enterprise Safeguards & Data Quality

- **Dataform Assertions:** Built-in quality checks for unique keys, non-null constraint checks on critical columns, and range validations before promoting data from Silver to Gold.
- **IAM & Security:** Multi-project/multi-environment service account isolation with Least Privilege access.
- **Secret Management:** API keys and sensitive connections managed securely via Google Cloud Secret Manager.
- **SLA & Alerting:** Automated Airflow callbacks (`on_failure_callback`) routing alerts to Slack/PagerDuty for SLA breaches or pipeline failures.