from abc import ABC, abstractmethod
from pathlib import Path
from io import BytesIO
import pandas as pd
from azure.storage.blob import BlobServiceClient

from util.id_generator import IdGenerator
from util.pipeline_log_formatter import get_pipeline_logger


class ParquetWriter(ABC):
    @abstractmethod
    def write(self, df: pd.DataFrame, destination: str) -> None:
        """
        Writes a DataFrame to a destination in Parquet format.
        """
        pass