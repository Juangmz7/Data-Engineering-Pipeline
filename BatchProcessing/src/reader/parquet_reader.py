from pathlib import Path

import pandas as pd

from shared.contracts.data_reader import DataReader
from shared.util.id_generator import IdGenerator
from shared.util.pipeline_log_formatter import get_pipeline_logger


class ParquetReader(DataReader):
    def __init__(self, correlation_id: str) -> None:
        self._correlation_id = correlation_id
        self._local_id = IdGenerator.generate()
        self._logger = get_pipeline_logger(
            class_name=self.__class__.__name__,
            correlation_id=self._correlation_id,
            local_id=self._local_id
        )
        self._logger.info("ParquetReader initialized.")

    def read(self, source_path: str) -> pd.DataFrame:
        try:
            src = Path(source_path)
            self._logger.info(f"Attempting to read Parquet file from: {src}")

            if not src.exists():
                error_msg = f"Source file not found: {src}"
                self._logger.error(error_msg)
                raise FileNotFoundError(error_msg)

            df = pd.read_parquet(src)
            self._logger.info(f"Successfully read {len(df)} rows and {len(df.columns)} columns from {src}.")

            if df.empty:
                self._logger.error(f"The Parquet file is empty: {source_path}")
                raise ValueError(f"Cannot process an empty Parquet file: {source_path}")

            return df

        except FileNotFoundError:
            raise

        except IOError as e:
            self._logger.error(f"I/O error while reading file {source_path}: {e}")
            raise

        except ValueError:
            raise

        except Exception as e:
            self._logger.critical(f"Unexpected system or runtime error during Parquet read: {e}", exc_info=True)
            raise
