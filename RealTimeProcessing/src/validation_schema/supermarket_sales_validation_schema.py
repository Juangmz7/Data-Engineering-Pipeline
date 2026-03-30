import pandera as pa
from pandera import Column, Check

from validation_schema import DataFrameSchema

class SupermarketSalesValidationSchema(DataFrameSchema):

    def get_schema(self) -> pa.DataFrameSchema:
        return pa.DataFrameSchema(
            {
                # Mandatory fields
                "Invoice ID": Column(str, Check.str_length(min_value=1), required=True),
                "Branch": Column(str, Check.str_length(min_value=1), required=True),
                "Customer type": Column(str, Check.isin(["Member", "Normal"]), required=True),
                "Product line": Column(str, Check.str_length(min_value=1), required=True),
                
                "Unit price": Column(float, Check.in_range(0.0, 100000.0), required=True),
                "Quantity": Column(int, Check.in_range(1, 10000), required=True),
                "cogs": Column(float, Check.in_range(0.0, 100000.0), required=True),
                
                "Date": Column(pa.DateTime, coerce=True, required=True),
                
                # Regex validation to ensure time strictly follows HH:MM:SS AM/PM format
                "Time": Column(str, Check.str_matches(r"^(0?[1-9]|1[0-2]):[0-5]\d:[0-5]\d\s?(AM|PM|am|pm)$"), required=True),
                
                "Payment": Column(str, Check.isin(["Cash", "Credit card", "Ewallet"]), required=True),
                
                # Optional fields
                "Gender": Column(str, Check.isin(["Female", "Male"]), required=False),
                "Rating": Column(float, Check.in_range(1.0, 10.0), required=False)
            },
            strict=True 
        )
        
        
        