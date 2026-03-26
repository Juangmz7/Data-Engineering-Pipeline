from abc import ABC, abstractmethod
import pandas as pd

class DataWriter(ABC):
    
    @abstractmethod
    def write(self, df: pd.DataFrame, destination: str) -> None:
        """
        Writes a DataFrame to a destination format.
        """
        pass