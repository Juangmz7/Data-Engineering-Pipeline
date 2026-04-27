import pytest
import pandas as pd
from unittest.mock import patch, MagicMock
from pathlib import Path

from BatchProcessing.src.reader.parquet_reader import ParquetReader


@pytest.fixture
def mock_logger():
    """Provides a mocked pipeline logger to verify observability calls."""
    with patch("BatchProcessing.src.reader.parquet_reader.get_pipeline_logger") as mock:
        yield mock.return_value


@pytest.fixture
def mock_id_gen():
    """Mocks the ID generator for consistent test metadata."""
    with patch("BatchProcessing.src.reader.parquet_reader.IdGenerator.generate", return_value="test-local-uuid"):
        yield


@pytest.fixture
def reader(mock_logger, mock_id_gen):
    """Provides a fresh instance of ParquetReader for each test."""
    return ParquetReader(correlation_id="test-correlation-id")


class TestParquetReader:

    def test_read_success(self, reader, mock_logger):
        """Tests a successful Parquet read scenario, verifying integration with pandas."""
        # Arrange
        source_file = "data/valid_data.parquet"
        expected_df = pd.DataFrame({"col1": [1, 2], "col2": ["A", "B"]})

        with patch("BatchProcessing.src.reader.parquet_reader.Path") as mock_path_cls, \
             patch("BatchProcessing.src.reader.parquet_reader.pd.read_parquet") as mock_read_parquet:

            mock_src = MagicMock(spec=Path)
            mock_src.exists.return_value = True
            mock_src.__str__.return_value = source_file
            mock_path_cls.return_value = mock_src

            mock_read_parquet.return_value = expected_df

            # Act
            result_df = reader.read(source_file)

            # Assert
            mock_read_parquet.assert_called_once_with(
                mock_src, 
                engine="pyarrow", 
                dtype_backend="pyarrow"
            )
            pd.testing.assert_frame_equal(result_df, expected_df)

            # Verify observability metrics
            mock_logger.info.assert_any_call(
                f"Successfully read 2 rows and 2 columns from {source_file}."
            )

    def test_read_raises_file_not_found_when_source_missing(self, reader, mock_logger):
        """Ensures FileNotFoundError is raised and logged before invoking pandas."""
        source_file = "data/missing_data.parquet"

        with patch("BatchProcessing.src.reader.parquet_reader.Path") as mock_path_cls:
            mock_src = MagicMock(spec=Path)
            mock_src.exists.return_value = False
            mock_src.__str__.return_value = source_file
            mock_path_cls.return_value = mock_src

            # Act & Assert
            with pytest.raises(FileNotFoundError, match="Source file not found"):
                reader.read(source_file)

            mock_logger.error.assert_any_call(f"Source file not found: {source_file}")

    def test_read_handles_io_error(self, reader, mock_logger):
        """Verifies that native Python IO errors (e.g., permissions) are logged and re-raised."""
        source_file = "data/locked_data.parquet"

        with patch("BatchProcessing.src.reader.parquet_reader.Path") as mock_path_cls, \
            patch("BatchProcessing.src.reader.parquet_reader.pd.read_parquet", side_effect=IOError("Permission denied")):

            mock_src = MagicMock(spec=Path)
            mock_src.exists.return_value = True
            mock_src.__str__.return_value = source_file
            mock_path_cls.return_value = mock_src

            # Act & Assert
            with pytest.raises(IOError, match="Permission denied"):
                reader.read(source_file)

            mock_logger.error.assert_any_call(
                f"I/O error while reading file {source_file}: Permission denied"
            )

    def test_read_handles_empty_dataframe(self, reader, mock_logger):
        """
        Ensures that empty Parquet content is correctly mapped
        to a standard Python ValueError to maintain abstraction layers.
        """
        source_file = "data/empty.parquet"

        with patch("BatchProcessing.src.reader.parquet_reader.Path") as mock_path_cls, \
             patch("BatchProcessing.src.reader.parquet_reader.pd.read_parquet", return_value=pd.DataFrame()):

            mock_src = MagicMock(spec=Path)
            mock_src.exists.return_value = True
            mock_src.__str__.return_value = source_file
            mock_path_cls.return_value = mock_src

            # Act & Assert
            with pytest.raises(ValueError, match="Cannot process an empty Parquet file"):
                reader.read(source_file)

            mock_logger.error.assert_any_call(f"The Parquet file is empty: {source_file}")

    def test_read_handles_unexpected_exceptions(self, reader, mock_logger):
        """Ensures unforeseen runtime errors trigger a CRITICAL log with stack trace."""
        source_file = "data/any.parquet"

        with patch("BatchProcessing.src.reader.parquet_reader.Path") as mock_path_cls:
            mock_src = MagicMock(spec=Path)
            mock_src.exists.return_value = True
            mock_src.__str__.return_value = source_file
            mock_path_cls.return_value = mock_src

            # Force an unexpected failure inside the try block before pandas is even called
            # by mocking the logger to throw an exception
            reader._logger.info = MagicMock(side_effect=MemoryError("Out of memory"))

            # Act & Assert
            with pytest.raises(MemoryError):
                reader.read(source_file)

            mock_logger.critical.assert_called_once()
            args, kwargs = mock_logger.critical.call_args
            assert "Unexpected system or runtime error during Parquet read" in args[0]
            assert kwargs.get("exc_info") is True
