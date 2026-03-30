import pytest
import shutil
from unittest.mock import patch, MagicMock
from pathlib import Path

from BatchProcessing.src.writer.local_parquet_writer import LocalParquetWriter

@pytest.fixture
def mock_logger():
    """Provides a mocked pipeline logger to verify log calls."""
    with patch("local_parquet_writer.get_pipeline_logger") as mock:
        yield mock.return_value

@pytest.fixture
def mock_id_gen():
    """Mocks the ID generator to return a predictable local ID."""
    with patch("local_parquet_writer.IdGenerator.generate", return_value="test-local-uuid"):
        yield

@pytest.fixture
def writer(mock_logger, mock_id_gen):
    return LocalParquetWriter(correlation_id="test-correlation-id")

class TestLocalParquetWriter:

    def test_write_success_with_directory_creation(self, writer, mock_logger):
        """Tests successful copy and directory creation."""
        source = "source/data.parquet"
        destination = "target/subdir/data.parquet"

        with patch("local_parquet_writer.Path") as mock_path_cls, \
             patch("shutil.copy2") as mock_copy:
            
            mock_src = MagicMock(spec=Path)
            mock_src.exists.return_value = True
            
            mock_dst = MagicMock(spec=Path)
            mock_dst.parent.exists.return_value = False 
            
            mock_path_cls.side_effect = [mock_src, mock_dst]

            writer.write(source, destination)

            # Assert directory creation logic
            mock_dst.parent.mkdir.assert_called_once_with(parents=True, exist_ok=True)
            mock_copy.assert_called_once_with(mock_src, mock_dst)
            mock_logger.info.assert_any_call("Data successfully written to local disk.")

    def test_write_raises_file_not_found_when_source_missing(self, writer, mock_logger):
        """Ensures FileNotFoundError is raised and logged."""
        with patch("local_parquet_writer.Path") as mock_path_cls:
            mock_src = MagicMock(spec=Path)
            mock_src.exists.return_value = False
            mock_path_cls.return_value = mock_src
            
            with pytest.raises(FileNotFoundError):
                writer.write("missing.parquet", "dest.parquet")
            
            # Verify error was logged
            assert mock_logger.error.called

    def test_write_handles_permission_error(self, writer, mock_logger):
        """Verifies logging for PermissionError using the specific message pattern."""
        with patch("local_parquet_writer.Path") as mock_path_cls, \
             patch("shutil.copy2", side_effect=PermissionError("Access denied")):
            
            mock_src = MagicMock(spec=Path)
            mock_src.exists.return_value = True
            mock_path_cls.return_value = mock_src
            
            with pytest.raises(PermissionError):
                writer.write("src.parquet", "dst.parquet")
            
            # Verify specific error message
            args, _ = mock_logger.error.call_args
            assert "Permission denied writing to" in args[0]
            assert "Access denied" in args[0]

    def test_write_handles_same_file_error(self, writer, mock_logger):
        """Verifies logging for shutil.SameFileError using the specific message pattern."""
        with patch("local_parquet_writer.Path") as mock_path_cls, \
             patch("shutil.copy2", side_effect=shutil.SameFileError("Identical paths")):
            
            mock_src = MagicMock(spec=Path)
            mock_src.exists.return_value = True
            mock_path_cls.return_value = mock_src
            
            with pytest.raises(shutil.SameFileError):
                writer.write("src.parquet", "src.parquet")
            
            args, _ = mock_logger.error.call_args
            assert "Source and destination paths are identical" in args[0]
            assert "Identical paths" in args[0]

    def test_write_handles_os_error(self, writer, mock_logger):
        """Verifies logging for OSError using the specific message pattern."""
        with patch("local_parquet_writer.Path") as mock_path_cls, \
             patch("shutil.copy2", side_effect=OSError("No space left")):
            
            mock_src = MagicMock(spec=Path)
            mock_src.exists.return_value = True
            mock_path_cls.return_value = mock_src
            
            with pytest.raises(OSError):
                writer.write("src.parquet", "dst.parquet")
            
            args, _ = mock_logger.error.call_args
            assert "OS error during local copy to" in args[0]
            assert "No space left" in args[0]

    def test_write_handles_unexpected_exceptions(self, writer, mock_logger):
        """Verifies that unforeseen errors trigger a critical log with specific message."""
        with patch("local_parquet_writer.Path", side_effect=Exception("Unexpected crash")):
            with pytest.raises(Exception):
                writer.write("src.parquet", "dst.parquet")
            
            mock_logger.critical.assert_called_once()
            args, kwargs = mock_logger.critical.call_args
            assert "Unexpected runtime error during local copy" in args[0]
            assert "Unexpected crash" in args[0]
            assert kwargs.get("exc_info") is True