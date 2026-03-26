from pathlib import Path

from shared.contracts.data_writer import DataWriter
from util.id_generator import IdGenerator
from util.pipeline_log_formatter import get_pipeline_logger

from azure.storage.blob import BlobServiceClient
from azure.core.exceptions import AzureError

class AzureBlobParquetWriter(DataWriter):
    def __init__(self, connection_string: str, container_name: str, correlation_id: str) -> None:
        self._correlation_id = correlation_id
        self._local_id = IdGenerator.generate()
        self._logger = get_pipeline_logger(
            class_name=self.__class__.__name__,
            correlation_id=self._correlation_id,
            local_id=self._local_id
        )
        
        self._container_name = container_name
        self._blob_service_client = BlobServiceClient.from_connection_string(connection_string)
        self._logger.info(f"AzureBlobParquetWriter initialized for container: {self._container_name}")

    def write(self, source_path: str, destination: str) -> None:
        try:
            src = Path(source_path)
            self._logger.info(f"Uploading data from {src} to Azure Blob: {destination}")
            
            if not src.exists():
                raise FileNotFoundError(f"Source file not found: {src}")
            
            blob_client = self._blob_service_client.get_blob_client(
                container=self._container_name, 
                blob=destination
            )
            with open(src, "rb") as data:
                blob_client.upload_blob(data, overwrite=True)

            self._logger.info("Data successfully uploaded to Azure Blob Storage.")
        
        except IOError as e:
            # Handles local disk issues during the read operation
            self._logger.error(f"Local file system error reading {src}: {e}")
            raise

        except FileNotFoundError as e:
            self._logger.error(f"File not found: {src}")
            raise
            
        except AzureError as e:
            # Handles network, authentication, or service issues from Azure
            self._logger.error(f"Azure SDK error during upload to {destination}: {e}")
            raise

        except Exception as e:
            self._logger.critical(f"Unexpected system or runtime error during Azure upload: {e}", exc_info=True)
            raise