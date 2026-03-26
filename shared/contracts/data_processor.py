from abc import ABC, abstractmethod

import pandas as pd

class DataProcessor(ABC):

    @abstractmethod
    def process(self, df: pd.DataFrame) -> pd.DataFrame:
        """Processes a DataFrame and returns the processed DataFrame.
        """
        pass
        