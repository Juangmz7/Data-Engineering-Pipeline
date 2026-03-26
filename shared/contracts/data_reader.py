from abc import ABC, abstractmethod

import pandas as pd

class DataReader(ABC):

    @abstractmethod
    def read(self, source: str) -> pd.DataFrame:
        """
          Reads data from a source and returns a DataFrame.
        """
        pass
        