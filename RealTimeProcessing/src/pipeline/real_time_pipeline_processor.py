import pandas as pd
from typing import Type

from RealTimeProcessing.src.reader.csv_reader import CsvReader
from shared.contracts.pipeline_processor import PipelineProcessor
from shared.validator.generic_data_validator import GenericDataValidator
from shared.contracts.validation_schema import DataFrameSchema


class RealTimePipelineProcessor(PipelineProcessor):
    
    def run_reader(self, output_path: str, source_path: str, correlation_id: str) -> str:
        reader = CsvReader(correlation_id=correlation_id)
        df = reader.read(source_path=source_path)
        
        df.to_csv(output_path, index=False)
        return output_path

    def run_validator(
        self, 
        input_path: str, 
        schema_class: Type[DataFrameSchema], 
        correlation_id: str
    ) -> str:
        df = pd.read_csv(input_path)
        
        # Instantiate the injected schema dynamically
        schema = schema_class().get_schema()
        
        validator = GenericDataValidator(
            correlation_id=correlation_id, 
            dataframe_schema=schema
        )
        
        # The generic validator will raise a ValueError if validation fails.
        validator.validate(df)
        
        return input_path

    def run_processor(self, input_path: str, output_path: str, correlation_id: str) -> str:
        # TODO: Instantiate and invoke the Processor contract.
        return output_path