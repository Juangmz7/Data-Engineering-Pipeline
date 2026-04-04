import pytest
from unittest.mock import mock_open, patch

from azure.core.exceptions import AzureError

from RealTimeProcessing.src.writer.azure_blob_csv_writer import AzureBlobCsvWriter


@pytest.fixture
def mock_logger():
    with patch("RealTimeProcessing.src.writer.azure_blob_csv_writer.get_pipeline_logger") as mock:
        yield mock.return_value


@pytest.fixture
def mock_id_gen():
    with patch(
        "RealTimeProcessing.src.writer.azure_blob_csv_writer.IdGenerator.generate",
        return_value="test-local-uuid",
    ):
        yield


@pytest.fixture
def writer(mock_logger, mock_id_gen):
    with patch(
        "RealTimeProcessing.src.writer.azure_blob_csv_writer.BlobServiceClient.from_connection_string"
    ) as mock_factory:
        instance = AzureBlobCsvWriter(
            "DefaultEndpointsProtocol=https;AccountName=test;AccountKey=key;EndpointSuffix=core.windows.net",
            "test-container",
            "test-correlation-id",
        )
        instance._mock_client = mock_factory.return_value
        return instance


class TestAzureBlobCsvWriter:
    def test_write_success(self, writer, mock_logger):
        source = "local_dir/data.csv"
        destination = "cloud_dir/data.csv"

        with patch("RealTimeProcessing.src.writer.azure_blob_csv_writer.Path") as mock_path_cls:
            mock_src = mock_path_cls.return_value
            mock_src.exists.return_value = True

            with patch("builtins.open", mock_open(read_data=b"csv_content")) as mocked_open:
                writer.write(source, destination)

        writer._mock_client.get_blob_client.assert_called_once_with(
            container="test-container",
            blob=destination,
        )
        writer._mock_client.get_blob_client.return_value.upload_blob.assert_called_once()
        mocked_open.assert_called_once_with(mock_src, "rb")
        mock_logger.info.assert_any_call("Data successfully uploaded to Azure Blob Storage.")

    def test_write_raises_file_not_found_when_source_missing(self, writer, mock_logger):
        with patch("RealTimeProcessing.src.writer.azure_blob_csv_writer.Path") as mock_path_cls:
            mock_src = mock_path_cls.return_value
            mock_src.exists.return_value = False

            with pytest.raises(FileNotFoundError, match="Source file not found"):
                writer.write("missing.csv", "remote.csv")

        mock_logger.error.assert_any_call(f"File not found: {mock_src}")

    def test_write_handles_io_error_during_read(self, writer, mock_logger):
        with patch("RealTimeProcessing.src.writer.azure_blob_csv_writer.Path") as mock_path_cls:
            mock_src = mock_path_cls.return_value
            mock_src.exists.return_value = True

            with patch("builtins.open", side_effect=IOError("Permission denied")):
                with pytest.raises(IOError):
                    writer.write("source.csv", "dest.csv")

        mock_logger.error.assert_any_call(f"Local file system error reading {mock_src}: Permission denied")

    def test_write_handles_azure_sdk_error(self, writer, mock_logger):
        with patch("RealTimeProcessing.src.writer.azure_blob_csv_writer.Path") as mock_path_cls:
            mock_src = mock_path_cls.return_value
            mock_src.exists.return_value = True
            writer._mock_client.get_blob_client.return_value.upload_blob.side_effect = AzureError(
                "Authentication failed"
            )

            with patch("builtins.open", mock_open(read_data=b"data")):
                with pytest.raises(AzureError):
                    writer.write("source.csv", "dest.csv")

        mock_logger.error.assert_any_call(
            "Azure SDK error during upload to dest.csv: Authentication failed"
        )

    def test_write_handles_unexpected_exceptions(self, writer, mock_logger):
        with patch("RealTimeProcessing.src.writer.azure_blob_csv_writer.Path") as mock_path_cls:
            mock_path_cls.return_value.exists.return_value = True
            writer._mock_client.get_blob_client.side_effect = Exception("Connection reset")

            with pytest.raises(Exception, match="Connection reset"):
                writer.write("source.csv", "dest.csv")

        mock_logger.critical.assert_called_once()
        args, kwargs = mock_logger.critical.call_args
        assert "Unexpected system or runtime error during Azure upload: Connection reset" == args[0]
        assert kwargs.get("exc_info") is True
