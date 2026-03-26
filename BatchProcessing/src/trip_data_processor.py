from pathlib import Path
from typing import List
import numpy as np
import pandas as pd

from util.id_generator import IdGenerator
from util.pipeline_log_formatter import get_pipeline_logger


class TripDataProcessor:
    def __init__(self, correlation_id: str) -> None:
        self._correlation_id = correlation_id
        self._local_id = IdGenerator.generate()
        self._logger = get_pipeline_logger(
            class_name=self.__class__.__name__,
            correlation_id=self._correlation_id,
            local_id=self._local_id
        )
        
        self._optional_columns: List[str] = [
            'tip_amount', 'tolls_amount', 'extra', 'airport_fee',
            'congestion_surcharge', 'cbd_congestion_fee'
        ]

        # Define boundaries for time of day categorization
        self._time_bins = [-1, 5, 11, 17, 23]
        self._time_labels = ['Night', 'Morning', 'Afternoon', 'Evening']

        self._logger.info("TripDataProcessor initialized.")

    def execute(self, input_directory: str, output_directory: str, filename: str) -> None:
        self._logger.info(f"Starting execution for file: {filename}")
        
        input_path = Path(input_directory) / filename
        output_path = Path(output_directory) / filename

        df = self._read_data(input_path)
        processed_df = self._apply_business_logic(df)
        self._write_data(processed_df, output_path)
        
        self._logger.info("Execution completed successfully.")

    def _read_data(self, file_path: Path) -> pd.DataFrame:
        self._logger.info(f"Reading data from: {file_path}")
        try:
            return pd.read_parquet(file_path, engine='pyarrow')
        except Exception as e:
            self._logger.error(f"Failed to read input file {file_path}: {e}")
            raise IOError(f"Input data read error: {e}") from e

    def _apply_business_logic(self, df: pd.DataFrame) -> pd.DataFrame:
        self._logger.info("Applying business logic transformations.")
        
        try:
            df = self._process_optional_columns(df)
            df = self._drop_unnecessary_columns(df)
            
            df = self._calculate_trip_duration(df)
            df = self._extract_temporal_features(df)
            df = self._categorize_time_of_day(df)
            
            df = self._calculate_average_speed(df)
            df = self._calculate_revenue_per_mile(df)
            
            df = self._categorize_trip_distance(df)
            df = self._categorize_fare_amount(df)

        except KeyError as e:
            self._logger.error(f"Validation contract broken. Mandatory column missing: {e}")
            raise RuntimeError(f"Data validation failed prior to processing. Missing column: {e}") from e
        except TypeError as e:
            self._logger.error(f"Validation contract broken. Invalid data type encountered: {e}")
            raise RuntimeError(f"Data validation failed prior to processing. Invalid data type: {e}") from e
        except Exception as e:
            self._logger.error(f"Unexpected error during business logic execution: {e}")
            raise RuntimeError(f"Unexpected error in Processor: {e}") from e
        
        return df

    def _process_optional_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        self._logger.info("Processing optional columns if present.")
        
        for col in self._optional_columns:
            if col in df.columns:
                if pd.api.types.is_numeric_dtype(df[col]):
                    df[col] = df[col].fillna(0.0)
                else:
                    df[col] = df[col].fillna('UNKNOWN')
                    
        return df
    
    def _drop_unnecessary_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        self._logger.info("Dropping requested columns (VendorID, store_and_fwd_flag, RatecodeID).")
        columns_to_drop = ['VendorID', 'store_and_fwd_flag', 'RatecodeID']
        return df.drop(columns=columns_to_drop, errors='ignore')

    def _calculate_trip_duration(self, df: pd.DataFrame) -> pd.DataFrame:
        df['trip_duration_minutes'] = (
            df['tpep_dropoff_datetime'] - df['tpep_pickup_datetime']
        ).dt.total_seconds() / 60.0
        return df

    def _extract_temporal_features(self, df: pd.DataFrame) -> pd.DataFrame:
        df['pickup_year'] = df['tpep_pickup_datetime'].dt.year
        df['pickup_month'] = df['tpep_pickup_datetime'].dt.month
        return df

    def _categorize_time_of_day(self, df: pd.DataFrame) -> pd.DataFrame:
        pickup_hour = df['tpep_pickup_datetime'].dt.hour
        df['trip_time_of_day'] = pd.cut(
            pickup_hour, 
            bins=self._time_bins, 
            labels=self._time_labels, 
            ordered=False
        )
        return df

    def _calculate_average_speed(self, df: pd.DataFrame) -> pd.DataFrame:
        df['average_speed_mph'] = np.where(
            df['trip_duration_minutes'] > 0,
            df['trip_distance'] / (df['trip_duration_minutes'] / 60.0),
            0.0
        )
        return df

    def _calculate_revenue_per_mile(self, df: pd.DataFrame) -> pd.DataFrame:
        df['revenue_per_mile'] = np.where(
            df['trip_distance'] > 0,
            df['total_amount'] / df['trip_distance'],
            0.0
        )
        return df

    def _categorize_trip_distance(self, df: pd.DataFrame) -> pd.DataFrame:
        distance_conditions = [
            (df['trip_distance'] < 2.0),
            (df['trip_distance'] >= 2.0) & (df['trip_distance'] <= 10.0),
            (df['trip_distance'] > 10.0)
        ]
        distance_choices = ['Short', 'Medium', 'Long']
        df['trip_distance_category'] = np.select(distance_conditions, distance_choices, default='Unknown')
        return df

    def _categorize_fare_amount(self, df: pd.DataFrame) -> pd.DataFrame:
        fare_conditions = [
            (df['fare_amount'] < 20.0),
            (df['fare_amount'] >= 20.0) & (df['fare_amount'] <= 50.0),
            (df['fare_amount'] > 50.0)
        ]
        fare_choices = ['Low', 'Medium', 'High']
        df['fare_category'] = np.select(fare_conditions, fare_choices, default='Unknown')
        return df
    
    def _write_data(self, df: pd.DataFrame, file_path: Path) -> None:
        try:
            file_path.parent.mkdir(parents=True, exist_ok=True)
            df.to_parquet(file_path, index=False, engine='pyarrow')
        except Exception as e:
            self._logger.error(f"Failed to write output file {file_path}: {e}")
            raise IOError(f"Output data write error: {e}") from e