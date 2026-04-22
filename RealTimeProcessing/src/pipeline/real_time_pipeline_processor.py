import pandas as pd
from typing import Type
from pathlib import Path

from RealTimeProcessing.src.reader.csv_reader import CsvReader
from RealTimeProcessing.src.processor.supermarket_sales_data_processor import (
    SupermarketSalesDataProcessor,
)
from shared.contracts.pipeline_processor import PipelineProcessor
from shared.validator.generic_data_validator import GenericDataValidator
from shared.contracts.validation_schema import DataFrameSchema


class RealTimePipelineProcessor(PipelineProcessor):
    
    def run_reader(self, source_path: str, output_path: str, correlation_id: str) -> str:
        reader = CsvReader(correlation_id=correlation_id)
        df = reader.read(source_path=source_path)
        output_file = Path(output_path)
        output_file.parent.mkdir(parents=True, exist_ok=True)
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
        df = pd.read_csv(input_path)

        # The raw CSV stores these fields as strings, but the processor expects datetime accessors.
        df["Date"] = pd.to_datetime(df["Date"], errors="raise")
        df["Time"] = pd.to_datetime(df["Time"], errors="raise")

        processor = SupermarketSalesDataProcessor(correlation_id=correlation_id)
        processed_df = processor.process(df)

        self.create_output_dir(output_path)

        processed_df.to_csv(output_path, index=False)
        return output_path
