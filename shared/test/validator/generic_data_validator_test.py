import pytest
import pandas as pd
import pandera as pa
from pandera.errors import SchemaError, SchemaErrors
from unittest.mock import patch, MagicMock

from shared.validator.generic_data_validator import GenericDataValidator



@pytest.fixture
def mock_logger():
    """Provides a mocked pipeline logger to verify observability calls."""
    with patch("shared.validator.generic_data_validator.get_pipeline_logger") as mock:
        yield mock.return_value


@pytest.fixture
def mock_id_gen():
    """Mocks the ID generator for consistent test metadata."""
    with patch("shared.validator.generic_data_validator.IdGenerator.generate", return_value="test-local-uuid"):
        yield


@pytest.fixture
def mock_schema():
    """
    Provides a mocked Pandera schema. 
    This isolates the test from actual Pandera validation logic.
    """
    return MagicMock(spec=pa.DataFrameSchema)


@pytest.fixture
def validator(mock_logger, mock_id_gen, mock_schema):
    """Provides a fresh instance of GenericDataValidator with injected mocked dependencies."""
    return GenericDataValidator(
        correlation_id="test-correlation-id", 
        dataframe_schema=mock_schema
    )


@pytest.fixture
def dummy_dataframe():
    """Provides a lightweight dummy DataFrame to pass through the validator."""
    return pd.DataFrame({"col1": [1, 2, 3]})


class TestGenericDataValidator:

    def test_validate_success(self, validator, mock_schema, mock_logger, dummy_dataframe):
        """Verifies the happy path where validation passes without raising exceptions."""
        # Act
        result = validator.validate(dummy_dataframe)
        
        # Assert
        assert result is True
        mock_schema.validate.assert_called_once_with(dummy_dataframe, lazy=True)
        
        mock_logger.info.assert_any_call(f"Starting validation for DataFrame with {len(dummy_dataframe)} rows.")
        mock_logger.info.assert_any_call("DataFrame successfully passed schema validation.")

    def test_validate_maps_single_schema_error_to_value_error(self, validator, mock_schema, mock_logger, dummy_dataframe):
        """Ensures a single SchemaError from Pandera is properly logged and wrapped into a ValueError."""
        # Arrange
        mock_column = MagicMock()
        mock_column.name = "Price"

        simulated_error = SchemaError(
            schema=mock_column,
            data=dummy_dataframe,
            message="Mocked error message"
        )
        # Inject the property separately to satisfy the constructor signature
        simulated_error.failure_cases = "Mocked failure case"

        mock_schema.validate.side_effect = simulated_error

        # Act & Assert
        with pytest.raises(ValueError, match="Data validation failed on column 'Price'"):
            validator.validate(dummy_dataframe)

        mock_logger.error.assert_called_once()
        args, _ = mock_logger.error.call_args
        assert "Data validation failed on column 'Price'" in args[0]
        assert "Mocked failure case" in args[0]

    def test_validate_maps_multiple_schema_errors_to_value_error(self, validator, mock_schema, mock_logger, dummy_dataframe):
        """
        Ensures multiple schema violations (triggered by lazy=True)
        are properly logged and wrapped into a standard ValueError.
        """
        # Arrange
        simulated_errors = SchemaErrors.__new__(SchemaErrors)
        simulated_errors.failure_cases = "Multiple mocked failures"

        mock_schema.validate.side_effect = simulated_errors

        # Act & Assert
        with pytest.raises(ValueError, match="Data validation failed due to multiple schema violations"):
            validator.validate(dummy_dataframe)

        mock_logger.error.assert_called_once()
        args, _ = mock_logger.error.call_args
        assert "Multiple validation failures detected" in args[0]
        assert "Multiple mocked failures" in args[0]

    def test_validate_handles_unexpected_exceptions_as_critical(self, validator, mock_schema, mock_logger, dummy_dataframe):
        """Ensures unforeseen runtime errors trigger a CRITICAL log with stack trace."""
        # Arrange
        mock_schema.validate.side_effect = Exception("Unexpected Memory Limit Exceeded")
        
        # Act & Assert
        with pytest.raises(Exception, match="Unexpected Memory Limit Exceeded"):
            validator.validate(dummy_dataframe)
            
        mock_logger.critical.assert_called_once()
        args, kwargs = mock_logger.critical.call_args
        assert "Unexpected error during data validation" in args[0]
        assert "Unexpected Memory Limit Exceeded" in args[0]
        assert kwargs.get("exc_info") is True