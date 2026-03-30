import pytest
import pandas as pd
import pandera as pa
from pandera.errors import SchemaError

from RealTimeProcessing.src.validation_schema.supermarket_sales_validation_schema import SupermarketSalesValidationSchema


@pytest.fixture
def schema_provider():
    """Provides a fresh instance of the validation schema."""
    return SupermarketSalesValidationSchema()

@pytest.fixture
def valid_dataframe():
    """
    Provides a strictly valid DataFrame meeting all schema requirements.
    This serves as the baseline for all mutation tests.
    """
    return pd.DataFrame({
        "Invoice ID": ["INV-001", "INV-002"],
        "Branch": ["A", "B"],
        "Customer type": ["Member", "Normal"],
        "Product line": ["Health and beauty", "Electronic accessories"],
        "Unit price": [15.5, 99.99],
        "Quantity": [5, 1],
        "cogs": [77.5, 99.99],
        "Date": ["2026-03-30", "2026-03-31"],  # Will be coerced to DateTime
        "Time": ["10:29:00 AM", "01:15:30 PM"],
        "Payment": ["Cash", "Credit card"],
        # Optional fields
        "Gender": ["Female", "Male"],
        "Rating": [9.5, 8.0]
    })

class TestSupermarketSalesValidationSchema:

    def test_get_schema_returns_pandera_dataframe_schema(self, schema_provider):
        """Verifies that the factory method returns the correct Pandera object."""
        schema = schema_provider.get_schema()
        assert isinstance(schema, pa.DataFrameSchema)

    def test_valid_dataframe_passes_validation(self, schema_provider, valid_dataframe):
        """Tests the happy path where a fully compliant DataFrame passes."""
        schema = schema_provider.get_schema()
        # Pandera returns the validated DataFrame on success
        validated_df = schema.validate(valid_dataframe)
        
        assert isinstance(validated_df, pd.DataFrame)
        assert len(validated_df) == 2

    def test_strict_mode_rejects_extra_columns(self, schema_provider, valid_dataframe):
        """Ensures strict=True blocks DataFrames with undeclared columns."""
        schema = schema_provider.get_schema()
        df = valid_dataframe.copy()
        df["City"] = ["New York", "Los Angeles"]  # Undeclared column
        
        with pytest.raises(SchemaError, match="column 'City' not in dataframe_schema"):
            schema.validate(df)

    def test_rejects_missing_mandatory_columns(self, schema_provider, valid_dataframe):
        """Ensures required=True is enforced for mandatory columns."""
        schema = schema_provider.get_schema()
        df = valid_dataframe.drop(columns=["Invoice ID"])
        
        with pytest.raises(SchemaError, match="column 'Invoice ID' not in dataframe"):
            schema.validate(df)

    def test_allows_missing_optional_columns(self, schema_provider, valid_dataframe):
        """Verifies that optional columns can be safely omitted."""
        schema = schema_provider.get_schema()
        df = valid_dataframe.drop(columns=["Gender", "Rating"])
        
        validated_df = schema.validate(df)
        assert "Gender" not in validated_df.columns

    def test_rejects_empty_strings(self, schema_provider, valid_dataframe):
        """Verifies Check.str_length(min_value=1) catches empty strings."""
        schema = schema_provider.get_schema()
        df = valid_dataframe.copy()
        df.loc[0, "Product line"] = ""
        
        with pytest.raises(SchemaError):
            schema.validate(df)

    def test_rejects_invalid_categorical_values(self, schema_provider, valid_dataframe):
        """Verifies Check.isin() enforces allowed categorical values."""
        schema = schema_provider.get_schema()
        df = valid_dataframe.copy()
        df.loc[0, "Payment"] = "Bitcoin"  # Not in allowed list
        
        with pytest.raises(SchemaError):
            schema.validate(df)

    def test_rejects_out_of_range_numerics(self, schema_provider, valid_dataframe):
        """Verifies Check.in_range() protects against business logic violations and overflows."""
        schema = schema_provider.get_schema()
        
        # Test lower bound violation
        df_low = valid_dataframe.copy()
        df_low.loc[0, "Quantity"] = 0
        with pytest.raises(SchemaError):
            schema.validate(df_low)
            
        # Test upper bound violation
        df_high = valid_dataframe.copy()
        df_high.loc[0, "Quantity"] = 15000
        with pytest.raises(SchemaError):
            schema.validate(df_high)

    def test_rejects_invalid_time_format(self, schema_provider, valid_dataframe):
        """Verifies the regex strictly enforces the HH:MM:SS AM/PM format."""
        schema = schema_provider.get_schema()
        
        invalid_times = [
            "14:30:00",       # 24-hour military format (missing AM/PM)
            "10:29 AM",       # Missing seconds
            "13:00:00 PM",    # Hour > 12
            "10:65:00 AM",    # Minute > 59
            "10:29:00AM "     # Trailing space
        ]
        
        for invalid_time in invalid_times:
            df = valid_dataframe.copy()
            df.loc[0, "Time"] = invalid_time
            
            with pytest.raises(SchemaError):
                schema.validate(df)

    def test_date_column_coerces_to_datetime(self, schema_provider, valid_dataframe):
        """Verifies that coerce=True successfully converts valid date strings to DateTime objects."""
        schema = schema_provider.get_schema()
        
        # Check initial state in the dummy dataframe (it's a string)
        assert pd.api.types.is_string_dtype(valid_dataframe["Date"])
        
        validated_df = schema.validate(valid_dataframe)
        
        # Check final state after Pandera validation (should be datetime64)
        assert pd.api.types.is_datetime64_any_dtype(validated_df["Date"])