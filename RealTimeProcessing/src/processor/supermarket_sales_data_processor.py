from pathlib import Path
from typing import List
import numpy as np
import pandas as pd

    
from shared.util.id_generator import IdGenerator
from shared.util.pipeline_log_formatter import get_pipeline_logger

from shared.contracts.data_processor import DataProcessor


class SupermarketSalesDataProcessor(DataProcessor):
    def __init__(self, correlation_id: str) -> None:
        self._correlation_id = correlation_id
        self._local_id = IdGenerator.generate()
        self._logger = get_pipeline_logger(
            class_name=self.__class__.__name__,
            correlation_id=self._correlation_id,
            local_id=self._local_id
        )
        
        self._optional_columns: List[str] = [
            'Gender', 'Rating'
        ]

        # Define boundaries for time of day categorization
        self._time_bins = [-1, 5, 11, 17, 23]
        self._time_labels = ['Night', 'Morning', 'Afternoon', 'Evening']

        self._logger.info("SupermarketSalesDataProcessor initialized.")

    def process(self, df: pd.DataFrame) -> pd.DataFrame:
        self._logger.info(f"Starting data frame processing ")

        processed_df = self._apply_business_logic(df)
        
        self._logger.info("Execution completed successfully.")
        return processed_df
    
    def _apply_business_logic(self, df: pd.DataFrame) -> pd.DataFrame:
        self._logger.info("Applying business logic transformations.")
        
        try:
            df = self._drop_unnecessary_columns(df)
            df = self._remove_duplicates(df)
            df = self._calculate_total_sale(df)
            df = self._categorize_time_of_day(df)
            df = self._calculate_day_of_the_week(df)

        except KeyError as e:
            self._logger.error(f"Validation contract broken. Mandatory column missing: {e}")
            raise RuntimeError(f"Data validation failed prior to processing. Missing column: {e}") from e
        except TypeError as e:
            self._logger.error(f"Validation contract broken. Invalid data type encountered: {e}")
            raise RuntimeError(f"Data validation failed prior to processing. Invalid data type: {e}") from e
        except Exception as e:
            self._logger.critical(f"Unexpected error during business logic execution: {e}")
            raise RuntimeError(f"Unexpected error in Processor: {e}") from e
        
        return df
    
    def _drop_unnecessary_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        self._logger.info("Dropping optional columns.")
        return df.drop(columns=self._optional_columns, errors='ignore')
    

    def _calculate_total_sale(self, df: pd.DataFrame) -> pd.DataFrame:
        df["Total Sale"] = df["Unit price"] * df["Quantity"]
        return df

    def _categorize_time_of_day(self, df: pd.DataFrame) -> pd.DataFrame:
        invoice_hour = df['Time'].dt.hour
        df['Time of the day'] = pd.cut(
            invoice_hour, 
            bins=self._time_bins, 
            labels=self._time_labels, 
            ordered=False
        )
        return df
    
    def _calculate_day_of_the_week(self, df: pd.DataFrame) -> pd.DataFrame:
        df["Day of the week"] = df["Date"].dt.day_name()
        return df

    def _remove_duplicates(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.drop_duplicates()
        return df
