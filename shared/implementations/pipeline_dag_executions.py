import os
import shutil
from typing import Type

from airflow.exceptions import AirflowSkipException
from shared.contracts.pipeline_processor import PipelineProcessor
from shared.contracts.validation_schema import DataFrameSchema


def execute_reader(processor_class: Type[PipelineProcessor],
                   correlation_id: str,
                   source_path: str,
                   output_path: str
                ) -> str:
    processor: PipelineProcessor = processor_class()
    return processor.run_reader(
        source_path, output_path, correlation_id
    )


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
        target_dir = os.path.dirname(quarantine_path)
        os.makedirs(target_dir, exist_ok=True)
        
        if not os.path.exists(input_path):
            raise FileNotFoundError(f"Input path not found: {input_path}")
            
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