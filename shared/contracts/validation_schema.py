from abc import ABC, abstractmethod
import pandera as pa

class DataFrameSchema(ABC):

    @abstractmethod
    def get_schema(self) -> pa.DataFrameSchema:
        """
          Returns the predefined schema for validation.
        """
        pass
        