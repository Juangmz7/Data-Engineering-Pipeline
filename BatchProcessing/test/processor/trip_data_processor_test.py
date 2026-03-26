from anyio import Path
import pytest
import pandas as pd
import numpy as np
import sys  
from unittest.mock import patch

root_path = Path(__file__).parent.parent
sys.path.append(str(root_path))

from processor.trip_data_processor import TripDataProcessor 


@pytest.fixture
def mock_dependencies():
    """Mocks external utilities to prevent side effects during testing."""
    with patch("trip_data_processor.IdGenerator.generate", return_value="test-local-id"), \
         patch("trip_data_processor.get_pipeline_logger") as mock_logger:
        yield mock_logger


@pytest.fixture
def processor(mock_dependencies):
    """Provides a fresh instance of TripDataProcessor."""
    return TripDataProcessor(correlation_id="test-correlation-id")


@pytest.fixture
def sample_dataframe():
    """Provides a controlled, deterministic DataFrame for testing business logic."""
    data = {
        'VendorID': [1, 2, 1],
        'tpep_pickup_datetime': pd.to_datetime([
            '2026-03-26 04:00:00',  # Night
            '2026-03-26 10:00:00',  # Morning
            '2026-03-26 16:00:00'   # Afternoon
        ]),
        'tpep_dropoff_datetime': pd.to_datetime([
            '2026-03-26 04:30:00',  # 30 mins
            '2026-03-26 10:00:00',  # 0 mins (edge case testing division by zero)
            '2026-03-26 17:00:00'   # 60 mins
        ]),
        'trip_distance': [1.5, 5.0, 15.0],    # Short, Medium, Long
        'fare_amount': [15.0, 35.0, 60.0],    # Low, Medium, High
        'total_amount': [20.0, 40.0, 70.0],
        'store_and_fwd_flag': ['N', 'Y', 'N'],
        'tip_amount': [5.0, np.nan, 10.0],    # Optional column with missing value
        # Deliberately omitting 'airport_fee' to test missing optional column handling
    }
    return pd.DataFrame(data)


class TestTripDataProcessor:

    # Optional & Unnecessary Columns 

    def test_process_optional_columns_fills_missing_numeric_with_zero(self, processor, sample_dataframe):
        # Arrange
        assert pd.isna(sample_dataframe.loc[1, 'tip_amount'])
        
        # Act
        result_df = processor._process_optional_columns(sample_dataframe.copy())
        
        # Assert
        assert result_df.loc[1, 'tip_amount'] == 0.0
        assert 'airport_fee' not in result_df.columns  # Unprovided optional column shouldn't be created

    def test_drop_unnecessary_columns_removes_correct_columns(self, processor, sample_dataframe):
        # Act
        result_df = processor._drop_unnecessary_columns(sample_dataframe.copy())
        
        # Assert
        assert 'VendorID' not in result_df.columns
        assert 'store_and_fwd_flag' not in result_df.columns
        assert 'tpep_pickup_datetime' in result_df.columns  # Mandatory columns remain intact


    def test_calculate_trip_duration_computes_correct_minutes(self, processor, sample_dataframe):
        # Act
        result_df = processor._calculate_trip_duration(sample_dataframe.copy())
        
        # Assert
        expected_durations = [30.0, 0.0, 60.0]
        np.testing.assert_array_almost_equal(result_df['trip_duration_minutes'].values, expected_durations)

    def test_extract_temporal_features_extracts_year_and_month(self, processor, sample_dataframe):
        # Act
        result_df = processor._extract_temporal_features(sample_dataframe.copy())
        
        # Assert
        assert all(result_df['pickup_year'] == 2026)
        assert all(result_df['pickup_month'] == 3)

    def test_categorize_time_of_day_applies_correct_labels(self, processor, sample_dataframe):
        # Act
        result_df = processor._categorize_time_of_day(sample_dataframe.copy())
        
        # Assert
        expected_labels = ['Night', 'Morning', 'Afternoon']
        assert list(result_df['trip_time_of_day']) == expected_labels

    
    # Testing Performance Metrics & Edge Cases

    def test_calculate_average_speed_handles_zero_duration(self, processor, sample_dataframe):
        # Arrange
        df = processor._calculate_trip_duration(sample_dataframe.copy())
        
        # Act
        result_df = processor._calculate_average_speed(df)
        
        # Assert
        # Index 0: 1.5 miles / 0.5 hours = 3.0 mph
        # Index 1: 5.0 miles / 0.0 hours = 0.0 mph (Handled by np.where, preventing ZeroDivisionError)
        # Index 2: 15.0 miles / 1.0 hours = 15.0 mph
        expected_speeds = [3.0, 0.0, 15.0]
        np.testing.assert_array_almost_equal(result_df['average_speed_mph'].values, expected_speeds)

    def test_calculate_revenue_per_mile_handles_zero_distance(self, processor, sample_dataframe):
        # Arrange
        df = sample_dataframe.copy()
        df.loc[1, 'trip_distance'] = 0.0  # Force division by zero scenario
        
        # Act
        result_df = processor._calculate_revenue_per_mile(df)
        
        # Assert
        assert result_df.loc[1, 'revenue_per_mile'] == 0.0
        assert result_df.loc[0, 'revenue_per_mile'] == pytest.approx(13.333, rel=1e-3)

   
    #  Testing Categorizations

    def test_categorize_trip_distance_assigns_correct_buckets(self, processor, sample_dataframe):
        # Act
        result_df = processor._categorize_trip_distance(sample_dataframe.copy())
        
        # Assert
        expected_categories = ['Short', 'Medium', 'Long']
        assert list(result_df['trip_distance_category']) == expected_categories

    def test_categorize_fare_amount_assigns_correct_buckets(self, processor, sample_dataframe):
        # Act
        result_df = processor._categorize_fare_amount(sample_dataframe.copy())
        
        # Assert
        expected_categories = ['Low', 'Medium', 'High']
        assert list(result_df['fare_category']) == expected_categories


    # Testing Overall Execution & Error Handling

    def test_apply_business_logic_raises_runtime_error_on_missing_mandatory_column(self, processor, sample_dataframe):
        # Arrange: Break the schema contract by dropping a mandatory column
        bad_df = sample_dataframe.drop(columns=['tpep_pickup_datetime'])
        
        # Act & Assert
        with pytest.raises(RuntimeError, match="Data validation failed prior to processing. Missing column"):
            processor._apply_business_logic(bad_df)

    def test_apply_business_logic_raises_runtime_error_on_type_error(self, processor, sample_dataframe):
        # Arrange: Break the type contract
        bad_df = sample_dataframe.copy()
        bad_df['tpep_pickup_datetime'] = 'Invalid String' 
        
        # Act & Assert
        with pytest.raises(RuntimeError, match="Data validation failed prior to processing. Invalid data type"):
            processor._apply_business_logic(bad_df)

    def test_execute_orchestrates_processing_successfully(self, processor, sample_dataframe):
        # Act
        result_df = processor.execute(sample_dataframe.copy())
        
        # Assert
        # Verify that all transformations were applied by checking for newly engineered features
        assert 'trip_duration_minutes' in result_df.columns
        assert 'trip_time_of_day' in result_df.columns
        assert 'average_speed_mph' in result_df.columns
        assert 'trip_distance_category' in result_df.columns
        
        # Verify that drop constraints were applied
        assert 'VendorID' not in result_df.columns