from datetime import datetime, timedelta
from typing import List, Type

from airflow import DAG
from airflow.models import Variable
from airflow.hooks.base import BaseHook
from airflow.operators.python import PythonOperator
from airflow.sensors.filesystem import FileSensor

from RealTimeProcessing.src.pipeline.real_time_pipeline_processor import RealTimePipelineProcessor
from RealTimeProcessing.src.validation_schema.supermarket_sales_validation_schema import SupermarketSalesValidationSchema
from RealTimeProcessing.src.validation_schema.supermarket_sales_backup_validatation import SupermarketSalesBackupValidationSchema
from shared.contracts.pipeline_processor import PipelineProcessor
from shared.contracts.data_writer import DataWriter

from shared.implementations.pipeline_dag_executions import (
    execute_reader,
    execute_validator,
    execute_processor,
)

def _execute_writer(
    processor_class: Type[PipelineProcessor],
    source_path: str,
    final_destination: str,
    correlation_id: str
) -> None:
    # Retrieve configuration and secrets from Airflow backend securely
    azure_connection = BaseHook.get_connection('azure_blob_default')
    azure_conn_str = azure_connection.get_password() 
    azure_container = Variable.get("azure_blob_container_name", default_var="trip-data-processed")

    # TODO: Nil descomenta esto cuando crees los writers e importalos
    # local_writer = LocalCsvWriter(correlation_id=correlation_id)
    # azure_writer = AzureBlobCsvWriter(
    #     connection_string=azure_conn_str,
    #     container_name=azure_container,
    #     correlation_id=correlation_id
    # )

    # injected_writers: List[DataWriter] = [local_writer, azure_writer]

    # processor: PipelineProcessor = processor_class()
    # processor.run_writer(
    #     source_path=source_path, 
    #     final_destination=final_destination, 
    #     correlation_id=correlation_id,
    #     writers=injected_writers
    # )


default_args = {
    'owner': 'data_engineering',
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='supermarket_sales_processing_pipeline',
    default_args=default_args,
    description='Near real-time processing of Supermarket Sales with Quarantine routing',
    schedule_interval='* * * * *',  
    start_date=datetime(2026, 4, 1),
    catchup=False,
    max_active_runs=1,
    tags=['sales', 'near_real_time', 'etl'],
) as dag:

    LANDING_ZONE_FILE = "/data/landing/supermarket_sales/sales_data.csv"
    STAGING_RAW_PATH = "/tmp/staging/supermarket_sales/{{ run_id }}/raw.csv"
    STAGING_PROCESSED_PATH = "/tmp/staging/supermarket_sales/{{ run_id }}/processed.csv"
    FINAL_DESTINATION_PATH = "/data/processed/supermarket_sales/{{ ds }}/{{ run_id }}_final.csv"
    
    QUARANTINE_PATH_RAW = "/data/quarantine/supermarket_sales/{{ ds }}/{{ run_id }}_raw_invalid.csv"
    QUARANTINE_PATH_PROCESSED = "/data/quarantine/supermarket_sales/{{ ds }}/{{ run_id }}_processed_invalid.csv"
    
    DAG_CORRELATION_ID = "{{ run_id }}"

    sensor_incoming_file = FileSensor(
        task_id='wait_for_csv_file',
        filepath=LANDING_ZONE_FILE,
        poke_interval=10,
        timeout=60,
        mode='poke',
    )

    task_reader = PythonOperator(
        task_id='read_csv_data',
        python_callable=execute_reader,
        op_kwargs={
            'processor_class': RealTimePipelineProcessor,
            'source_path': LANDING_ZONE_FILE,
            'output_path': STAGING_RAW_PATH,
            'correlation_id': DAG_CORRELATION_ID,
        },
    )

    task_validator = PythonOperator(
        task_id='validate_raw_schema',
        python_callable=execute_validator,
        op_kwargs={
            'processor_class': RealTimePipelineProcessor,
            'schema_class': SupermarketSalesValidationSchema,
            'input_path': STAGING_RAW_PATH,
            'quarantine_path': QUARANTINE_PATH_RAW,
            'correlation_id': DAG_CORRELATION_ID,
        },
    )

    task_processor = PythonOperator(
        task_id='process_sales_data',
        python_callable=execute_processor,
        op_kwargs={
            'processor_class': RealTimePipelineProcessor,
            'input_path': STAGING_RAW_PATH,
            'output_path': STAGING_PROCESSED_PATH,
            'correlation_id': DAG_CORRELATION_ID,
        },
    )

    task_validator_backup = PythonOperator(
        task_id='validate_processed_schema',
        python_callable=execute_validator,
        op_kwargs={
            'processor_class': RealTimePipelineProcessor,
            'schema_class': SupermarketSalesBackupValidationSchema,
            'input_path': STAGING_PROCESSED_PATH,
            'quarantine_path': QUARANTINE_PATH_PROCESSED,
            'correlation_id': DAG_CORRELATION_ID,
        },
    )

    task_writer = PythonOperator(
        task_id='write_final_data',
        python_callable=_execute_writer,
        op_kwargs={
            'processor_class': RealTimePipelineProcessor,
            'source_path': STAGING_PROCESSED_PATH,
            'final_destination': FINAL_DESTINATION_PATH,
            'correlation_id': DAG_CORRELATION_ID,
        },
    )

    # sensor_incoming_file >> task_reader >> task_validator >> task_processor >> task_validator_backup >> task_writer