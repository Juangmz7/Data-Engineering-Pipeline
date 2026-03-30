from abc import ABC, abstractmethod

import pandas as pd
import pandera as pa


class DataValidator(ABC):

    @abstractmethod
    def validate(self, df: pd.DataFrame, dataset_schema: pa.DataFrameSchema) -> bool:
        """
          Validates the DataFrame against the predefined schema.
        """
        pass
        