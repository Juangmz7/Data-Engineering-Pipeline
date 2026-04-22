import pandas as pd
import pandera as pa
import pytest
from pandera.errors import SchemaError

from RealTimeProcessing.src.validation_schema.supermarket_sales_backup_validatation import (
    SupermarketSalesBackupValidationSchema,
)


@pytest.fixture
def schema_provider():
    return SupermarketSalesBackupValidationSchema()


@pytest.fixture
def valid_processed_dataframe():
    return pd.DataFrame(
        {
            "Invoice ID": ["INV-001", "INV-002"],
            "Branch": ["Alex", "Giza"],
            "City": ["Yangon", "Naypyitaw"],
            "Customer type": ["Member", "Normal"],
            "Product line": ["Health and beauty", "Electronic accessories"],
            "Unit price": [10.0, 20.0],
            "Quantity": [2, 3],
            "Tax 5%": [1.0, 3.0],
            "Sales": [21.0, 63.0],
            "Date": ["2026-03-23", "2026-03-24"],
            "Time": ["2026-03-23 04:15:00", "2026-03-24 10:30:00"],
            "Payment": ["Cash", "Credit card"],
            "cogs": [20.0, 60.0],
            "gross margin percentage": [4.76, 4.76],
            "gross income": [1.0, 3.0],
            "Total Sale": [20.0, 60.0],
            "Time of the day": ["Night", "Morning"],
            "Day of the week": ["Monday", "Tuesday"],
        }
    )


class TestSupermarketSalesBackupValidationSchema:

    def test_get_schema_returns_pandera_dataframe_schema(self, schema_provider):
        schema = schema_provider.get_schema()
        assert isinstance(schema, pa.DataFrameSchema)

    def test_valid_processed_dataframe_passes_validation(self, schema_provider, valid_processed_dataframe):
        schema = schema_provider.get_schema()

        validated_df = schema.validate(valid_processed_dataframe)

        assert isinstance(validated_df, pd.DataFrame)
        assert "Total Sale" in validated_df.columns
        assert "Time of the day" in validated_df.columns
        assert "Day of the week" in validated_df.columns

    def test_rejects_invalid_total_sale_formula(self, schema_provider, valid_processed_dataframe):
        schema = schema_provider.get_schema()
        df = valid_processed_dataframe.copy()
        df.loc[0, "Total Sale"] = 999.0

        with pytest.raises(SchemaError, match="Total Sale must match Unit price \\* Quantity"):
            schema.validate(df)
