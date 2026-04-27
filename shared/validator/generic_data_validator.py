import pandas as pd
import pandera as pa
from pandera.errors import SchemaError

from shared.contracts.data_validator import DataValidator
from shared.util.id_generator import IdGenerator
from shared.util.pipeline_log_formatter import get_pipeline_logger

class GenericDataValidator(DataValidator):
    def __init__(self, correlation_id: str, dataframe_schema: pa.DataFrameSchema) -> None:
        self._correlation_id = correlation_id
        self._local_id = IdGenerator.generate()
        self._logger = get_pipeline_logger(
            class_name=self.__class__.__name__,
            correlation_id=self._correlation_id,
            local_id=self._local_id
        )
        
        self._schema = dataframe_schema
        self._logger.info("SupermarketSalesValidator initialized with Pandera schema.")

    def validate(self, df: pd.DataFrame) -> bool:
        self._logger.info(f"Starting validation for DataFrame with {len(df)} rows.")
        
        try:
            self._schema.validate(df, lazy=True)
            self._logger.info("DataFrame successfully passed schema validation.")   
            return True         
        except SchemaError as e:
            error_msg = f"Data validation failed on column '{e.schema.name}': {e.failure_cases}"
            self._logger.error(error_msg)
            raise ValueError(error_msg) from e
            
        except pa.errors.SchemaErrors as e:
            error_msg = f"Multiple validation failures detected. Summary: {e.failure_cases}"
            self._logger.error(error_msg)
            raise ValueError("Data validation failed due to multiple schema violations. Check logs for details.") from e
            
        except Exception as e:
            self._logger.critical(f"Unexpected error during data validation: {e}", exc_info=True)
            raise