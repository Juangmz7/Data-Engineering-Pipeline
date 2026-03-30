import pytest
from unittest.mock import patch, MagicMock, mock_open
from azure.core.exceptions import AzureError
from pathlib import Path

from BatchProcessing.src.writer.azure_blob_parquet_writer import AzureBlobParquetWriter

@pytest.fixture
def mock_logger():
    """Provides a mocked pipeline logger. Patch path must match the implementation module."""
    with patch("azure_blob_parquet_writer.get_pipeline_logger") as mock:
        yield mock.return_value

@pytest.fixture
def mock_id_gen():
    """Mocks the ID generator within the specific writer module."""
    with patch("azure_blob_parquet_writer.IdGenerator.generate", return_value="test-local-uuid"):
        yield

@pytest.fixture
def writer(mock_logger, mock_id_gen):
    """
    Provides an instance of AzureBlobParquetWriter with a mocked Azure client.
    We patch the BlobServiceClient inside the target module.
    """
    with patch("azure_blob_parquet_writer.BlobServiceClient.from_connection_string") as mock_factory:
        conn_str = "DefaultEndpointsProtocol=https;AccountName=test;AccountKey=key;EndpointSuffix=core.windows.net"
        instance = AzureBlobParquetWriter(conn_str, "test-container", "test-correlation-id")
        # Store the mock client for assertion purposes in tests
        instance._mock_client = mock_factory.return_value
        return instance

class TestAzureBlobParquetWriter:

    def test_write_success(self, writer, mock_logger):
        """Tests a successful upload scenario, verifying I/O and Azure SDK calls."""
        source = "local_dir/data.parquet"
        destination = "cloud_dir/data.parquet"
        mock_data = b"parquet_binary_content"

        # Patching Path inside the module where it is used
        with patch("azure_blob_parquet_writer.Path") as mock_path_cls:
            mock_path_cls.return_value.exists.return_value = True
            
            with patch("builtins.open", mock_open(read_data=mock_data)) as m_open:
                writer.write(source, destination)

                # Verify interaction with the Azure Blob Storage SDK
                writer._mock_client.get_blob_client.assert_called_once_with(
                    container="test-container", 
                    blob=destination
                )
                
                blob_client = writer._mock_client.get_blob_client.return_value
                blob_client.upload_blob.assert_called_once()
                
                m_open.assert_called_once()
                mock_logger.info.assert_any_call("Data successfully uploaded to Azure Blob Storage.")

    def test_write_raises_file_not_found_when_source_missing(self, writer, mock_logger):
        """Ensures FileNotFoundError is raised and logged when the local source is missing."""
        with patch("azure_blob_parquet_writer.Path") as mock_path_cls:
            mock_path_cls.return_value.exists.return_value = False
            
            with pytest.raises(FileNotFoundError, match="Source file not found"):
                writer.write("non_existent.parquet", "remote.parquet")


    def test_write_handles_io_error_during_read(self, writer, mock_logger):
        source_str = "source.parquet"
        
        with patch("azure_blob_parquet_writer.Path") as mock_path_cls:
            mock_instance = mock_path_cls.return_value
            mock_instance.exists.return_value = True
            mock_instance.__str__.return_value = source_str 
            
            with patch("builtins.open", side_effect=IOError("Permission denied")):
                with pytest.raises(IOError):
                    writer.write(source_str, "dest.parquet")
                
                # Ahora la comparación sí coincidirá
                mock_logger.error.assert_any_call(
                    f"Local file system error reading {source_str}: Permission denied"
                )

    def test_write_handles_azure_sdk_error(self, writer, mock_logger):
        """Verifies that Azure service-related errors are properly logged and re-raised."""
        with patch("azure_blob_parquet_writer.Path") as mock_path_cls:
            mock_path_cls.return_value.exists.return_value = True
            
            blob_client = writer._mock_client.get_blob_client.return_value
            blob_client.upload_blob.side_effect = AzureError("Authentication failed")
            
            with patch("builtins.open", mock_open(read_data=b"data")):
                with pytest.raises(AzureError):
                    writer.write("source.parquet", "dest.parquet")
                
                mock_logger.error.assert_any_call(
                    "Azure SDK error during upload to dest.parquet: Authentication failed"
                )

    def test_write_handles_unexpected_exceptions(self, writer, mock_logger):
        """Ensures unforeseen runtime errors trigger a CRITICAL log with stack trace."""
        with patch("azure_blob_parquet_writer.Path") as mock_path_cls:
            mock_path_cls.return_value.exists.return_value = True
            
            writer._mock_client.get_blob_client.side_effect = Exception("Connection reset")
            
            with pytest.raises(Exception):
                writer.write("source.parquet", "dest.parquet")
            
            mock_logger.critical.assert_called_once()
            args, kwargs = mock_logger.critical.call_args
            assert "Unexpected system or runtime error" in args[0]
            assert kwargs.get("exc_info") is True