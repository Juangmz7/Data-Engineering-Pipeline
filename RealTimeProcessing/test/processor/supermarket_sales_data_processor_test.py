from anyio import Path
import pytest
import pandas as pd
import sys
from unittest.mock import patch

root_path = Path(__file__).parent.parent
sys.path.append(str(root_path))

from RealTimeProcessing.src.processor.supermarket_sales_data_processor import SupermarketSalesDataProcessor


@pytest.fixture
def mock_dependencies():
    """Mocks external utilities to prevent side effects during testing."""
    with patch(
        "RealTimeProcessing.src.processor.supermarket_sales_data_processor.IdGenerator.generate",
        return_value="test-local-id",
    ), patch(
        "RealTimeProcessing.src.processor.supermarket_sales_data_processor.get_pipeline_logger"
    ) as mock_logger:
        yield mock_logger


@pytest.fixture
def processor(mock_dependencies):
    """Provides a fresh instance of SupermarketSalesDataProcessor."""
    return SupermarketSalesDataProcessor(correlation_id="test-correlation-id")


@pytest.fixture
def sample_dataframe():
    """Provides a controlled, deterministic DataFrame for testing business logic."""
    data = {
        "Invoice ID": ["750-67-8428", "226-31-3081", "226-31-3081"],
        "Branch": ["A", "B", "B"],
        "Customer type": ["Member", "Normal", "Normal"],
        "Product line": ["Health and beauty", "Electronic accessories", "Electronic accessories"],
        "Unit price": [10.0, 20.0, 20.0],
        "Quantity": [2, 3, 3],
        "cogs": [20.0, 60.0, 60.0],
        "Date": pd.to_datetime(["2026-03-23", "2026-03-24", "2026-03-24"]),
        "Time": pd.to_datetime(["2026-03-23 04:15:00", "2026-03-24 10:30:00", "2026-03-24 10:30:00"]),
        "Payment": ["Cash", "Credit card", "Credit card"],
        "Gender": ["Female", "Male", "Male"],
        "Rating": [9.1, 7.5, 7.5],
    }
    return pd.DataFrame(data)


class TestSupermarketSalesDataProcessor:
    def test_drop_unnecessary_columns_removes_optional_columns(self, processor, sample_dataframe):
        result_df = processor._drop_unnecessary_columns(sample_dataframe.copy())

        assert "Gender" not in result_df.columns
        assert "Rating" not in result_df.columns
        assert "Invoice ID" in result_df.columns

    def test_remove_duplicates_drops_repeated_rows(self, processor, sample_dataframe):
        result_df = processor._remove_duplicates(sample_dataframe.copy())

        assert len(result_df) == 2

    def test_calculate_total_sale_computes_expected_values(self, processor, sample_dataframe):
        result_df = processor._calculate_total_sale(sample_dataframe.copy())

        expected_totals = [20.0, 60.0, 60.0]
        assert list(result_df["Total Sale"]) == expected_totals

    def test_categorize_time_of_day_applies_correct_labels(self, processor, sample_dataframe):
        result_df = processor._categorize_time_of_day(sample_dataframe.copy())

        expected_labels = ["Night", "Morning", "Morning"]
        assert list(result_df["Time of the day"]) == expected_labels

    def test_calculate_day_of_the_week_extracts_day_names(self, processor, sample_dataframe):
        result_df = processor._calculate_day_of_the_week(sample_dataframe.copy())

        expected_days = ["Monday", "Tuesday", "Tuesday"]
        assert list(result_df["Day of the week"]) == expected_days

    def test_apply_business_logic_raises_runtime_error_on_missing_mandatory_column(
        self, processor, sample_dataframe
    ):
        bad_df = sample_dataframe.drop(columns=["Date"])

        with pytest.raises(RuntimeError, match="Data validation failed prior to processing. Missing column"):
            processor._apply_business_logic(bad_df)

    def test_apply_business_logic_raises_runtime_error_on_invalid_time_type(
        self, processor, sample_dataframe
    ):
        bad_df = sample_dataframe.copy()
        bad_df["Time"] = "Invalid String"

        with pytest.raises(RuntimeError, match="Unexpected error in Processor"):
            processor._apply_business_logic(bad_df)

    def test_process_orchestrates_processing_successfully(self, processor, sample_dataframe):
        result_df = processor.process(sample_dataframe.copy())

        assert "Total Sale" in result_df.columns
        assert "Time of the day" in result_df.columns
        assert "Day of the week" in result_df.columns
        assert "Gender" not in result_df.columns
        assert "Rating" not in result_df.columns
        assert len(result_df) == 2
