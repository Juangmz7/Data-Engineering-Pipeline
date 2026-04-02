import pandas as pd
from typing import List

from shared.contracts.data_writer import DataWriter
from shared.contracts.pipeline_processor import PipelineProcessor
from BatchProcessing.src.processor.trip_data_processor import TripDataProcessor
from implementations.composite_writer import CompositeWriter

class BatchPipelineProcessor(PipelineProcessor):
    
    def run_reader(self, execution_date: str, source_path: str, output_path: str) -> str:
        # TODO: Instantiate and invoke the Reader contract.
        return output_path

    def run_validator(self, input_path: str) -> str:
        # TODO: Instantiate and invoke the initial Validator contract.
        return input_path

    def run_processor(self, input_path: str, output_path: str, correlation_id: str) -> str:
        df = pd.read_parquet(input_path)
        
        processor = TripDataProcessor(correlation_id=correlation_id)
        processed_df = processor.process(df)
        
        processed_df.to_parquet(output_path, index=False)
        return output_path