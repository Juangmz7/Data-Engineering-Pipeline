import shutil
from zipfile import Path

from parquet_writer import ParquetWriter
from util.id_generator import IdGenerator
from util.pipeline_log_formatter import get_pipeline_logger


class LocalParquetWriter(ParquetWriter):
    def __init__(self, correlation_id: str) -> None:
        self._correlation_id = correlation_id
        self._local_id = IdGenerator.generate()
        self._logger = get_pipeline_logger(
            class_name=self.__class__.__name__,
            correlation_id=self._correlation_id,
            local_id=self._local_id
        )
        self._logger.info("LocalParquetWriter initialized.")

    def write(self, source_path: str, destination: str) -> None:
        src = Path(source_path)
        dst = Path(destination)
        self._logger.info(f"Copying data from {src} to local path: {dst}")
        
        if not src.exists():
            self._logger.error(f"Source file not found: {src}")
            raise FileNotFoundError(f"Source file not found: {src}")
        
        try:
            self._ensure_directory_exists(dst.parent)
            shutil.copy2(src, dst)
            self._logger.info("Data successfully written to local disk.")
            
        except PermissionError as e:
            self._logger.error(f"Permission denied writing to {dst}: {e}")
            raise
            
        except shutil.SameFileError as e:
            self._logger.error(f"Source and destination paths are identical: {e}")
            raise
            
        except OSError as e:
            self._logger.error(f"OS error during local copy to {dst}: {e}")
            raise
            
        except Exception as e:
            self._logger.critical(f"Unexpected runtime error during local copy: {e}", exc_info=True)
            raise
            
    def _ensure_directory_exists(self, directory_path: Path) -> None:
        if not directory_path.exists():
            self._logger.info(f"Creating missing directory structure: {directory_path}")
            directory_path.mkdir(parents=True, exist_ok=True)
