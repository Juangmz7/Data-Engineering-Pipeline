from datetime import datetime, timedelta
from typing import List, Type

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from airflow.hooks.base import BaseHook

from BatchProcessing.src.writer.local_parquet_writer import LocalParquetWriter
from BatchProcessing.src.writer.azure_blob_parquet_writer import AzureBlobParquetWriter
from shared.contracts.data_writer import DataWriter

from shared.contracts.pipeline_processor import PipelineProcessor
from BatchProcessing.src.pipeline.batch_pipeline_processor import BatchPipelineProcessor
from BatchProcessing.src.validation_schema.yellow_taxi_trip_validation_schema import YellowTaxiTripValidationSchema
from BatchProcessing.src.validation_schema.yellow_taxi_trip_backup_validation_schema import YellowTaxiTripBackupValidationSchema

from shared.implementations.pipeline_dag_executions import execute_processor, execute_reader, execute_validator


def _execute_writer(
    processor_class: Type[PipelineProcessor],
    source_path: str,
    final_destination: str,
    correlation_id: str
) -> None:
    # Retrieve configuration and secrets from Airflow backend securely
    azure_connection = BaseHook.get_connection('azure_blob_default')

    extras = azure_connection.extra_dejson
    azure_conn_str = extras.get('connection_string') 

    azure_container = Variable.get("azure_blob_container_name", default_var="trip-data-processed")

    local_writer = LocalParquetWriter(correlation_id=correlation_id)
    azure_writer = AzureBlobParquetWriter(
        connection_string=azure_conn_str,
        container_name=azure_container,
        correlation_id=correlation_id
    )

    injected_writers: List[DataWriter] = [local_writer, azure_writer]

    processor: PipelineProcessor = processor_class()
    processor.run_writer(
        source_path=source_path, 
        final_destination=final_destination, 
        correlation_id=correlation_id,
        writers=injected_writers
    )

default_args = {
    'owner': 'data_engineering',
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='trip_data_processing_pipeline',
    default_args=default_args,
    description='Daily orchestration of the Trip Data batch pipeline with DI and Quarantine',
    schedule='@daily',
    start_date=datetime(2026, 4, 1),
    catchup=False,
    tags=['trip_data', 'batch', 'etl'],
) as dag:

    BASE_STAGING_PATH = "/tmp/staging/trip_data/{{ ds }}"
    FINAL_DESTINATION = "processed/{{ ds }}/trip_data.parquet" 
    
    QUARANTINE_PATH_RAW = "/data/quarantine/trip_data/{{ ds }}/{{ run_id }}_raw_invalid.parquet"
    QUARANTINE_PATH_PROCESSED = "/data/quarantine/trip_data/{{ ds }}/{{ run_id }}_processed_invalid.parquet"
    
    DAG_CORRELATION_ID = "{{ run_id }}"

    task_reader = PythonOperator(
        task_id='read_batch_data',
        python_callable=execute_reader,
        op_kwargs={
            'processor_class': BatchPipelineProcessor,
            'correlation_id': DAG_CORRELATION_ID,
            'source_path': '/opt/airflow/BatchProcessing/data/yellow_tripdata_2025-01.parquet',
            'output_path': f"{BASE_STAGING_PATH}/raw.parquet",
        },
    )

    task_validator = PythonOperator(
        task_id='validate_raw_schema',
        python_callable=execute_validator,
        op_kwargs={
            'processor_class': BatchPipelineProcessor,
            'schema_class': YellowTaxiTripValidationSchema,
            'input_path': f"{BASE_STAGING_PATH}/raw.parquet",
            'quarantine_path': QUARANTINE_PATH_RAW,
            'correlation_id': DAG_CORRELATION_ID,
        },
    )

    task_processor = PythonOperator(
        task_id='process_data',
        python_callable=execute_processor,
        op_kwargs={
            'processor_class': BatchPipelineProcessor,
            'input_path': f"{BASE_STAGING_PATH}/raw.parquet",
            'output_path': f"{BASE_STAGING_PATH}/processed.parquet",
            'correlation_id': DAG_CORRELATION_ID,
        },
    )

    task_validator_backup = PythonOperator(
        task_id='validate_processed_schema',
        python_callable=execute_validator,
        op_kwargs={
            'processor_class': BatchPipelineProcessor,
            'schema_class': YellowTaxiTripBackupValidationSchema,
            'input_path': f"{BASE_STAGING_PATH}/processed.parquet",
            'quarantine_path': QUARANTINE_PATH_PROCESSED,
            'correlation_id': DAG_CORRELATION_ID,
        },
    )

    task_writer = PythonOperator(
        task_id='write_data',
        python_callable=_execute_writer,
        op_kwargs={
            'processor_class': BatchPipelineProcessor,
            'source_path': f"{BASE_STAGING_PATH}/processed.parquet",
            'final_destination': FINAL_DESTINATION,
            'correlation_id': DAG_CORRELATION_ID,
        },
    )

    task_reader >> task_validator >> task_processor >> task_validator_backup >> task_writer
