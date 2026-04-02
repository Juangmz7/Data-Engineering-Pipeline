import shutil
from typing import Type

from airflow.hooks.base import BaseHook
from anyio import Path

from shared.contracts.pipeline_processor import PipelineProcessor
from shared.contracts.validation_schema import DataFrameSchema


def execute_reader(processor_class: Type[PipelineProcessor], execution_date: str, output_path: str) -> str:
    processor: PipelineProcessor = processor_class()
    return processor.run_reader(execution_date, output_path)


def execute_validator(
    processor_class: Type[PipelineProcessor], 
    schema_class: Type[DataFrameSchema],
    input_path: str, 
    quarantine_path: str, 
    correlation_id: str
) -> str:
    processor: PipelineProcessor = processor_class()
    
    try:
        return processor.run_validator(input_path, schema_class, correlation_id)
    except ValueError as e:
        Path(quarantine_path).parent.mkdir(parents=True, exist_ok=True)
        shutil.move(input_path, quarantine_path)
        
        raise AirflowSkipException(
            f"Validation contract broken. Data safely routed to quarantine: {quarantine_path}"
        ) from e


def execute_processor(processor_class: Type[PipelineProcessor],
                      input_path: str,
                      output_path: str,
                      correlation_id: str) -> str:
    processor: PipelineProcessor = processor_class()
    return processor.run_processor(input_path, output_path, correlation_id)