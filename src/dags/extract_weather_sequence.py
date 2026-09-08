from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.operators.dataform import (
    DataformCreateCompilationResultOperator,
    DataformCreateWorkflowInvocationOperator,
)

from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.operators.python import PythonOperator
from airflow import DAG
from datetime import datetime
import requests,json


def preprocess_weather_data(request_date,json_data):
    preprocessed_data = []
    for hourly_data in range(0,24):
        print(hourly_data)
        hourly_preprocessed_data = {
        'requested_date' : request_date,
        'lattitude' : json_data['latitude'],
        'longitude' : json_data['longitude'],
        'elevation' : json_data['elevation'],
        'hour_recorded' : json_data['hourly']['time'][hourly_data],
        'temperature_recorded' : json_data['hourly']['temperature_2m'][hourly_data],
        'generationtime_ms' : json_data["generationtime_ms"],
        'timezone' :  json_data["timezone"],
        'timezone_abbreviation' : json_data["timezone_abbreviation"]}
        preprocessed_data.append(hourly_preprocessed_data)
    return preprocessed_data




def extract_and_store_to_gcs_landing(**context):
    # 1. Fetch data from API
    requested_date = datetime.now().strftime('%Y-%m-%d')
    url = f"https://api.open-meteo.com/v1/forecast?latitude=52.52&longitude=13.41&hourly=temperature_2m&start_date={requested_date}&end_date={requested_date}"

    response = requests.get(url)
    response.raise_for_status()
    json_data = response.json()
    # preprocessed_data = preprocess_weather_data(requested_date,json_data)
    # 2. Define file name and bucket
    bucket_name = "etl-training-bk-dev"
    
    file_name = f"landing/hourly_weather_data_{requested_date}.json"
    
    # 3. Upload using GCSHook
    gcs_hook = GCSHook(gcp_conn_id="google_cloud_default")
    gcs_hook.upload(
        bucket_name=bucket_name,
        object_name=file_name,
        data=json.dumps(json_data),
        mime_type="application/json"
    )



def preprocess_store_to_gcs_raw(**context):
    bucket_name = "etl-training-bk-dev"
    requested_date = datetime.now().strftime('%Y-%m-%d')
    source_file_name = f"landing/hourly_weather_data_{requested_date}.json"
    target_file_name = f"raw/weather_data/hourly_weather_data_{requested_date}.json"
    
    gcs_hook = GCSHook(gcp_conn_id="google_cloud_default")
    file_content = gcs_hook.download(
        bucket_name=bucket_name,
        object_name=source_file_name
    )
    json_data = json.loads(file_content)
    preprocessed_data = preprocess_weather_data(requested_date,json_data)
    newline_delimited_json = '\n'.join(
        json.dumps(record) for record in preprocessed_data
    )

    # 3. Upload using GCSHook
    gcs_hook.upload(
        bucket_name=bucket_name,
        object_name=target_file_name,
        data=newline_delimited_json,
        mime_type="application/json"
    )
    


with DAG(
    dag_id="etl_training-dag",
    start_date=datetime(2026, 1, 1),
    # schedule_interval="@daily",
    catchup=False,
) as dag:
    
    task_extract_and_store_to_gcs_landing = PythonOperator(
        task_id="extract_and_store_to_gcs_landing",
        python_callable=extract_and_store_to_gcs_landing,
    )

    task_preprocess_store_to_gcs_raw = PythonOperator(
        task_id="preprocess_store_to_gcs_raw",
        python_callable=preprocess_store_to_gcs_raw,
    )

    load_to_bigQuery_bronze = GCSToBigQueryOperator(
        task_id="load_to_bigQuery_bronze",
        bucket="etl-training-bk-dev",
        source_objects=["raw/weather_data/*.json"],
        destination_project_dataset_table="etl_training_bq_dev.weather_bronze_table",
        source_format="NEWLINE_DELIMITED_JSON",
        write_disposition="WRITE_TRUNCATE",
        external_table=False,
        autodetect=True,       
        )
    compile_dataform = DataformCreateCompilationResultOperator(
        task_id="compile_dataform",
        project_id="sample-training-001",
        region="us-central1",
        repository_id="etl-training-repository",
        compilation_result={
            "git_commitish": "develop",
        },
    )

    # 2. Invoke the compiled Dataform workflow
    # Pulls the compilation result name dynamically from XCom
    invoke_dataform = DataformCreateWorkflowInvocationOperator(
        task_id="invoke_dataform",
        project_id="sample-training-001",
        region="us-central1",
        repository_id="etl-training-repository",
        workflow_invocation={
            "compilation_result": "{{ task_instance.xcom_pull(task_ids='compile_dataform')['name'] }}",
            "invocation_config": {
            # ADD THIS LINE: Explicitly define the execution service account
            "service_account": "155478623400-compute@developer.gserviceaccount.com",
            }
        },
    )

    task_extract_and_store_to_gcs_landing >> task_preprocess_store_to_gcs_raw >> load_to_bigQuery_bronze >> compile_dataform >> invoke_dataform