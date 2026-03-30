import pandera as pa
from pandera import Column, Check

from validation_schema import DataFrameSchema

class SupermarketSalesBackupValidationSchema(DataFrameSchema):

    def get_schema(self) -> pa.DataFrameSchema:
        return pa.DataFrameSchema(
            {
                # ToDo Validation rules for the supermarket sales dataset after processing and transformation
            },
            strict=True 
        )
        
        
        