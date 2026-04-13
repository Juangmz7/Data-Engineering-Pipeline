from abc import ABC, abstractmethod
import os
from pathlib import Path
from typing import List

from shared.contracts.data_writer import DataWriter
from shared.implementations.composite_writer import CompositeWriter

class PipelineProcessor(ABC):
    
    @abstractmethod
    def run_reader(self,
                   source_path: str,
                   output_path: str,
                   correlation_id: str
                ) -> str:
        pass

    @abstractmethod
    def run_validator(self, input_path: str, correlation_id: str) -> str:
        pass

    @abstractmethod
    def run_processor(self, input_path: str, output_path: str, correlation_id: str) -> str:
        pass

    def create_output_dir(self, output_path: str) -> None:
        if not os.path.exists(output_path): 
            output_path_dir = Path(output_path).parent
            output_path_dir.mkdir(parents=True, exist_ok=True)

    def run_writer(self, 
                   source_path: str, 
                   final_destination: str, 
                   correlation_id: str,
                   writers: List[DataWriter]) -> None:
        
        composite_writer = CompositeWriter(
            writers=writers, 
            correlation_id=correlation_id
        )
        
        # execution of the composite pattern
        composite_writer.write(source_path=source_path, destination=final_destination)