import pytest
from unittest.mock import patch, MagicMock

from implementations.composite_writer import CompositeWriter

@pytest.fixture
def mock_logger():
    """Provides a mocked pipeline logger to verify orchestration logs."""
    with patch("shared.util.pipeline_log_formatter.get_pipeline_logger") as mock:
        yield mock.return_value

@pytest.fixture
def mock_id_gen():
    """Mocks the ID generator for a consistent local ID during testing."""
    with patch("shared.util.id_generator.IdGenerator.generate", return_value="test-composite-uuid"):
        yield

class TestCompositeParquetWriter:

    def test_initialization_logs_writer_count(self, mock_logger, mock_id_gen):
        """Verifies that the composite writer correctly logs the number of injected writers."""
        # Arrange
        mock_writers = [MagicMock(), MagicMock()]
        
        # Act
        CompositeWriter(writers=mock_writers, correlation_id="test-corr")
        
        # Assert
        mock_logger.info.assert_any_call("CompositeParquetWriter initialized with 2 underlying writers.")

    def test_write_delegates_to_all_writers(self, mock_logger, mock_id_gen):
        """Ensures that the write call is propagated to every writer in the collection."""
        # Arrange
        writer_a = MagicMock()
        writer_b = MagicMock()
        composite = CompositeWriter(writers=[writer_a, writer_b], correlation_id="test-corr")
        
        source = "source.parquet"
        destination = "destination.parquet"
        
        # Act
        composite.write(source, destination)
        
        # Assert
        # Check that both writers received the exact same parameters
        writer_a.write.assert_called_once_with(source, destination)
        writer_b.write.assert_called_once_with(source, destination)
        
        # Verify orchestration logs
        mock_logger.info.assert_any_call(f"Starting composite write operation from {source} to destination: {destination}")
        mock_logger.info.assert_any_call("Composite write operation completed successfully across all writers.")

    def test_write_propagates_exception_and_stops_execution(self, mock_logger, mock_id_gen):
        """
        Verifies that if an underlying writer fails, the exception propagates 
        and subsequent writers are not executed (Fail-Fast behavior).
        """
        # Arrange
        writer_success = MagicMock()
        writer_fail = MagicMock()
        writer_unreachable = MagicMock()
        
        # Mocking a failure in the second writer
        writer_fail.write.side_effect = RuntimeError("Storage connection failed")
        
        composite = CompositeWriter(
            writers=[writer_success, writer_fail, writer_unreachable], 
            correlation_id="test-corr"
        )
        
        # Act & Assert
        with pytest.raises(RuntimeError, match="Storage connection failed"):
            composite.write("src", "dst")
            
        # Verify execution flow
        writer_success.write.assert_called_once()
        writer_fail.write.assert_called_once()
        # The third writer should never be called due to the exception in the second one
        writer_unreachable.write.assert_not_called()

    def test_write_handles_empty_writer_list(self, mock_logger, mock_id_gen):
        """Ensures the composite handles an empty writer list gracefully without errors."""
        # Arrange
        composite = CompositeWriter(writers=[], correlation_id="test-corr")
        
        # Act
        composite.write("src", "dst")
        
        # Assert
        mock_logger.info.assert_any_call("Composite write operation completed successfully across all writers.")