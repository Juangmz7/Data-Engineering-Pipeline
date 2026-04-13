import pandas as pd
from typing import Type

from shared.contracts.data_writer import DataWriter
from shared.contracts.pipeline_processor import PipelineProcessor
from shared.contracts.validation_schema import DataFrameSchema
from BatchProcessing.src.processor.trip_data_processor import TripDataProcessor
from BatchProcessing.src.reader.parquet_reader import ParquetReader
from shared.validator.generic_data_validator import GenericDataValidator

class BatchPipelineProcessor(PipelineProcessor):
    
    def run_reader(self,
                   source_path: str,
                   output_path: str,
                   correlation_id: str
                ) -> str:
        reader = ParquetReader(correlation_id=correlation_id)
        df = reader.read(source_path)
        df.to_parquet(output_path, index=False)
        return output_path

    def run_validator(self, input_path: str, schema_class: Type[DataFrameSchema], correlation_id: str) -> str:
        df = pd.read_parquet(input_path)
        validator = GenericDataValidator(
            correlation_id=correlation_id,
            dataframe_schema=schema_class().get_schema(),
        )
        validator.validate(df)
        return input_path

    def run_processor(self, input_path: str, output_path: str, correlation_id: str) -> str:
        df = pd.read_parquet(input_path)
        
        processor = TripDataProcessor(correlation_id=correlation_id)
        processed_df = processor.process(df)
        
        processed_df.to_parquet(output_path, index=False)
        return output_path
