from pathlib import Path
import pandas as pd

from shared.contracts.data_reader import DataReader
from shared.util.id_generator import IdGenerator
from shared.util.pipeline_log_formatter import get_pipeline_logger

class CsvReader(DataReader):
    def __init__(self, correlation_id: str) -> None:
        self._correlation_id = correlation_id
        self._local_id = IdGenerator.generate()
        self._logger = get_pipeline_logger(
            class_name=self.__class__.__name__,
            correlation_id=self._correlation_id,
            local_id=self._local_id
        )
        self._logger.info("CsvReader initialized.")

    def read(self, source_path: str) -> pd.DataFrame:
        try:
            src = Path(source_path)
            self._logger.info(f"Attempting to read CSV file from: {src}")
            
            if not src.exists():
                error_msg = f"Source file not found: {src}"
                self._logger.error(error_msg)
                raise FileNotFoundError(error_msg)
            
            df = pd.read_csv(src)
            self._logger.info(f"Successfully read {len(df)} rows and {len(df.columns)} columns from {src}.")
            
            return df

        except FileNotFoundError:
            # Re-raise immediately as it's already logged
            raise
            
        except IOError as e:
            self._logger.error(f"I/O error while reading file {source_path}: {e}")
            raise
            
        except pd.errors.EmptyDataError as e:
            self._logger.error(f"The CSV file is empty: {source_path}")
            raise ValueError(f"Cannot process an empty CSV file: {source_path}") from e
            
        except pd.errors.ParserError as e:
            self._logger.error(f"Failed to parse CSV file due to malformed data: {source_path}")
            raise ValueError(f"Malformed CSV data in file: {source_path}") from e
            
        except Exception as e:
            self._logger.critical(f"Unexpected system or runtime error during CSV read: {e}", exc_info=True)
            raise