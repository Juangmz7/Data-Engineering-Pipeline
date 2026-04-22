from unittest.mock import patch

import pandas as pd
import pytest

from RealTimeProcessing.src.pipeline.real_time_pipeline_processor import RealTimePipelineProcessor


@pytest.fixture
def mock_processor_dependencies():
    with patch(
        "RealTimeProcessing.src.processor.supermarket_sales_data_processor.IdGenerator.generate",
        return_value="test-local-id",
    ), patch(
        "RealTimeProcessing.src.processor.supermarket_sales_data_processor.get_pipeline_logger"
    ):
        yield


def test_run_processor_writes_processed_csv(tmp_path, mock_processor_dependencies):
    input_path = tmp_path / "raw.csv"
    output_path = tmp_path / "staging" / "processed.csv"

    raw_df = pd.DataFrame(
        {
            "Invoice ID": ["750-67-8428", "226-31-3081"],
            "Branch": ["A", "B"],
            "City": ["Yangon", "Naypyitaw"],
            "Customer type": ["Member", "Normal"],
            "Gender": ["Female", "Male"],
            "Product line": ["Health and beauty", "Electronic accessories"],
            "Unit price": [10.0, 20.0],
            "Quantity": [2, 3],
            "Tax 5%": [1.0, 3.0],
            "Sales": [21.0, 63.0],
            "Date": ["2026-03-23", "2026-03-24"],
            "Time": ["04:15:00 AM", "10:30:00 AM"],
            "Payment": ["Cash", "Credit card"],
            "cogs": [20.0, 60.0],
            "gross margin percentage": [4.76, 4.76],
            "gross income": [1.0, 3.0],
            "Rating": [9.1, 7.5],
        }
    )
    raw_df.to_csv(input_path, index=False)

    pipeline = RealTimePipelineProcessor()

    result = pipeline.run_processor(
        input_path=str(input_path),
        output_path=str(output_path),
        correlation_id="test-correlation-id",
    )

    assert result == str(output_path)
    assert output_path.exists()

    processed_df = pd.read_csv(output_path)

    assert "Total Sale" in processed_df.columns
    assert "Time of the day" in processed_df.columns
    assert "Day of the week" in processed_df.columns
    assert "Gender" not in processed_df.columns
    assert "Rating" not in processed_df.columns
