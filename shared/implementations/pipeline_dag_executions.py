from typing import Type, List

from airflow.models import Variable
from airflow.hooks.base import BaseHook

from shared.contracts.data_writer import DataWriter
from shared.contracts.pipeline_processor import PipelineProcessor
from BatchProcessing.src.writer.local_parquet_writer import LocalParquetWriter
from BatchProcessing.src.writer.azure_blob_parquet_writer import AzureBlobParquetWriter


def execute_reader(processor_class: Type[PipelineProcessor], execution_date: str, output_path: str) -> str:
    processor: PipelineProcessor = processor_class()
    return processor.run_reader(execution_date, output_path)


def execute_validator(processor_class: Type[PipelineProcessor], input_path: str) -> str:
    processor: PipelineProcessor = processor_class()
    return processor.run_validator(input_path)


def execute_processor(processor_class: Type[PipelineProcessor], input_path: str, output_path: str, correlation_id: str) -> str:
    processor: PipelineProcessor = processor_class()
    return processor.run_processor(input_path, output_path, correlation_id)


def execute_validator_backup(processor_class: Type[PipelineProcessor], input_path: str) -> str:
    processor: PipelineProcessor = processor_class()
    return processor.run_validator_backup(input_path)