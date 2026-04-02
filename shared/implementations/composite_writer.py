from shared.util.id_generator import IdGenerator
from shared.util.pipeline_log_formatter import get_pipeline_logger
from shared.contracts.data_writer import DataWriter

"""
    Writer implementation that delegates the write operation to multiple underlying Writer instances.
"""
class CompositeWriter(DataWriter):
    def __init__(self, writers: list[DataWriter], correlation_id: str) -> None:
        self._writers = writers
        self._correlation_id = correlation_id
        self._local_id = IdGenerator.generate()
        self._logger = get_pipeline_logger(
            class_name=self.__class__.__name__,
            correlation_id=self._correlation_id,
            local_id=self._local_id
        )
        self._logger.info(f"CompositeWriter initialized with {len(self._writers)} underlying writers.")

    def write(self, source_path: str, destination: str) -> None:
        self._logger.info(f"Starting composite write operation from {source_path} to destination: {destination}")
        
        for writer in self._writers:
            writer.write(source_path, destination)
                
        self._logger.info("Composite write operation completed successfully across all writers.")